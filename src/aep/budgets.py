"""Shared integer accounting and the only enabled paid-call gateway.

Lock order is run -> accounting/operation records. Provider calls happen after
dispatch commits. Settlement is independent of worker liveness and run state.
"""

import hashlib
from uuid import uuid4

from psycopg.types.json import Jsonb

from .contracts import validate_message
from .db import connect
from .execution import Conflict, LeaseLost, NotFound, canonical, event
from .providers import FakeProvider, OutcomeUnknown, quote_call, usage_cost


def accounting_evidence(receipt):
    """Keep immutable accounting evidence without retaining generated payloads."""
    return {**{key: value for key, value in receipt.items() if key != "result"},
            "evidence_version": "usage-v2", "result_digest": hashlib.sha256(canonical(receipt.get("result"))).hexdigest()}


class BudgetDenied(Exception):
    code = "budget_exhausted"

    def __init__(self, requested, available, breached=False):
        self.details = {"code": "budget_frozen" if breached else self.code,
                        "requested_micro_usd": requested, "available_micro_usd": max(0, available)}
        self.code = self.details["code"]
        super().__init__(self.code)


class OperationPending(Exception):
    def __init__(self, operation_id):
        self.operation_id = str(operation_id)
        super().__init__("operation_pending_reconciliation")


class ProviderFailure(Exception):
    def __init__(self, reason):
        self.code = reason or "provider_failure"
        super().__init__(self.code)


def account_snapshot(conn, run_id):
    return conn.execute("""SELECT limit_micro_usd,settled_micro_usd,reserved_micro_usd,
        limit_micro_usd-settled_micro_usd-reserved_micro_usd AS available_micro_usd,breached
        FROM budget_accounts WHERE run_id=%s""", (run_id,)).fetchone()


class Budgets:
    def __init__(self, engine):
        self.engine = engine
        self.dsn = engine.settings.database_url
        self.provider = FakeProvider(engine)

    def reserve(self, claim, spec):
        request, quote = quote_call(spec)
        return self._reserve_prepared(claim, request, quote)

    def _reserve_prepared(self, claim, request, quote):
        """Trusted adapter entry point. Agent/model text cannot supply a quote."""
        digest = hashlib.sha256(canonical({"request": request, "quote": quote})).hexdigest()
        with connect(self.dsn) as conn:
            self.engine._lease(conn, claim)
            previous = conn.execute("SELECT * FROM operations WHERE run_id=%s AND task_id=%s AND call_key=%s",
                                    (claim["run_id"], claim["task_id"], request["call_key"])).fetchone()
            if previous:
                if previous["request_hash"] != digest:
                    raise Conflict("Logical call key reused with different request or price")
                if previous["state"] == "RESERVED":
                    conn.execute("UPDATE operations SET attempt_id=%s,updated_at=clock_timestamp() WHERE id=%s",
                                 (claim["attempt_id"], previous["id"]))
                    previous["attempt_id"] = claim["attempt_id"]
                return previous
            bound = quote["bound_micro_usd"]
            admitted = conn.execute("""UPDATE budget_accounts SET reserved_micro_usd=reserved_micro_usd+%s
                WHERE run_id=%s AND NOT breached AND settled_micro_usd+reserved_micro_usd+%s<=limit_micro_usd
                RETURNING run_id""", (bound, claim["run_id"], bound)).fetchone()
            if not admitted:
                account = account_snapshot(conn, claim["run_id"])
                raise BudgetDenied(bound, account["available_micro_usd"], account["breached"])
            operation_id = uuid4()
            operation = conn.execute("""INSERT INTO operations
                (id,run_id,task_id,attempt_id,call_key,request_hash,request,quote,state)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'RESERVED') RETURNING *""",
                (operation_id, claim["run_id"], claim["task_id"], claim["attempt_id"], request["call_key"], digest, Jsonb(request), Jsonb(quote))).fetchone()
            conn.execute("INSERT INTO budget_reservations(operation_id,run_id,amount_micro_usd,state) VALUES (%s,%s,%s,'HELD')",
                         (operation_id, claim["run_id"], bound))
            event(conn, claim["run_id"], "budget.reserved", actor=claim["worker_id"], task_id=claim["task_id"],
                  attempt_id=claim["attempt_id"], operation_id=str(operation_id), amount_micro_usd=bound)
            return operation

    def dispatch(self, claim, operation_id):
        with connect(self.dsn) as conn:
            self.engine._lease(conn, claim)
            operation = conn.execute("""SELECT * FROM operations WHERE id=%s AND run_id=%s AND task_id=%s""",
                                     (operation_id, claim["run_id"], claim["task_id"])).fetchone()
            if not operation:
                raise NotFound("Operation not found")
            if operation["state"] != "RESERVED":
                raise Conflict("Operation has already been dispatched or closed")
            if operation["request"]["provider"] == "openai-responses-v1" and not self.engine.settings.enable_openai:
                from .model_provider import ModelCallError
                raise ModelCallError("openai_backend_disabled")
            account = account_snapshot(conn, claim["run_id"])
            if account["breached"]:
                raise BudgetDenied(operation["quote"]["bound_micro_usd"], account["available_micro_usd"], True)
            token = uuid4()
            conn.execute("""UPDATE operations SET state='DISPATCHED',attempt_id=%s,dispatch_token=%s,
                updated_at=clock_timestamp() WHERE id=%s""", (claim["attempt_id"], token, operation_id))
            event(conn, claim["run_id"], "operation.dispatched", actor=claim["worker_id"], task_id=claim["task_id"],
                  attempt_id=claim["attempt_id"], operation_id=str(operation_id))
            return token

    def call(self, claim, spec):
        operation = self.reserve(claim, spec)
        return self.execute_reserved(claim, operation)

    def _provider_for(self, operation):
        if operation["request"]["provider"] == "fake-v1":
            return self.provider
        from .model_provider import ModelProvider
        return ModelProvider(self.engine)

    def execute_reserved(self, claim, operation):
        if operation["state"] == "SUCCEEDED":
            return self._call_result(operation)
        if operation["state"] in {"FAILED", "CANCELLED"}:
            raise ProviderFailure(operation["error"])
        if operation["state"] in {"DISPATCHED", "UNKNOWN"}:
            raise OperationPending(operation["id"])
        token = self.dispatch(claim, operation["id"])
        try:
            receipt = self._provider_for(operation).execute(operation["id"], token, claim)
        except OutcomeUnknown:
            self.mark_unknown(operation["id"])
            raise OperationPending(operation["id"]) from None
        except LeaseLost:
            self.mark_unknown(operation["id"])
            raise
        operation = self.settle(operation["id"], receipt)
        if operation["state"] != "SUCCEEDED":
            raise ProviderFailure(operation["error"])
        return self._call_result(operation)

    @staticmethod
    def _call_result(operation):
        return {"operation_id": str(operation["id"]), "provider": operation["quote"]["provider"],
                "price_version": operation["quote"]["price_version"], **operation["result"]}

    def _lock_operation(self, conn, operation_id, run_id=None, owner=None):
        operation = conn.execute("SELECT * FROM operations WHERE id=%s", (operation_id,)).fetchone()
        if not operation or (run_id is not None and str(operation["run_id"]) != str(run_id)):
            raise NotFound("Operation not found")
        run = conn.execute("SELECT * FROM runs WHERE id=%s FOR UPDATE", (operation["run_id"],)).fetchone()
        if owner is not None and run["owner_id"] != owner:
            raise NotFound("Operation not found")
        operation = conn.execute("SELECT * FROM operations WHERE id=%s FOR UPDATE", (operation_id,)).fetchone()
        return run, operation

    def mark_unknown(self, operation_id):
        with connect(self.dsn) as conn:
            run, operation = self._lock_operation(conn, operation_id)
            if operation["state"] == "DISPATCHED":
                conn.execute("UPDATE operations SET state='UNKNOWN',updated_at=clock_timestamp() WHERE id=%s", (operation_id,))
                event(conn, run["id"], "operation.unknown", operation_id=str(operation_id), task_id=operation["task_id"])

    def settle(self, operation_id, receipt):
        """Internal trusted-adapter entry point; verify durable provider evidence."""
        with connect(self.dsn) as conn:
            run, operation = self._lock_operation(conn, operation_id)
            evidence = self._provider_for(operation).evidence(conn, operation)
            if evidence != receipt:
                raise Conflict("Settlement requires matching authoritative provider evidence")
            return self._settle_locked(conn, run, operation, receipt)

    def _settle_locked(self, conn, run, operation, receipt):
        if operation["state"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            if operation["result"] != receipt:
                raise Conflict("Conflicting settlement for a closed operation")
            return operation
        if operation["state"] not in {"DISPATCHED", "UNKNOWN"}:
            raise Conflict("Cannot settle an undispatched operation")
        if receipt["request_hash"] != operation["request_hash"] or receipt["outcome"] not in {"SUCCEEDED", "FAILED", "NOT_ACCEPTED"}:
            raise Conflict("Provider evidence does not match the operation")
        for field in ("cost_micro_usd", "input_tokens", "output_tokens"):
            if type(receipt[field]) is not int or receipt[field] < 0:
                raise ValueError("Provider usage must be nonnegative integers")
        quote = operation["quote"]
        actual = receipt["cost_micro_usd"]
        cached = receipt.get("cached_input_tokens", 0)
        if type(cached) is not int or not 0 <= cached <= receipt["input_tokens"]:
            raise ValueError("Invalid cached input usage")
        expected = usage_cost(quote, receipt["input_tokens"], receipt["output_tokens"], cached)
        if actual != expected or (receipt["outcome"] == "NOT_ACCEPTED" and actual):
            raise ValueError("Inconsistent provider usage")
        breached = (actual > quote["bound_micro_usd"] or receipt["input_tokens"] > quote["max_input_tokens"]
                    or receipt["output_tokens"] > quote["max_output_tokens"])
        reservation = conn.execute("SELECT * FROM budget_reservations WHERE operation_id=%s", (operation["id"],)).fetchone()
        if reservation["state"] != "HELD":
            raise Conflict("Reservation already closed")
        account = account_snapshot(conn, run["id"])
        new_total = account["settled_micro_usd"] + actual + account["reserved_micro_usd"] - reservation["amount_micro_usd"]
        breached = breached or new_total > account["limit_micro_usd"]
        conn.execute("""UPDATE budget_accounts SET settled_micro_usd=settled_micro_usd+%s,
            reserved_micro_usd=reserved_micro_usd-%s,breached=breached OR %s WHERE run_id=%s""",
            (actual, reservation["amount_micro_usd"], breached, run["id"]))
        reservation_state = "RELEASED" if receipt["outcome"] == "NOT_ACCEPTED" else "SETTLED"
        conn.execute("UPDATE budget_reservations SET state=%s,finished_at=clock_timestamp() WHERE operation_id=%s",
                     (reservation_state, operation["id"]))
        state = "SUCCEEDED" if receipt["outcome"] == "SUCCEEDED" and not breached else "FAILED"
        error = "provider_bound_breached" if breached else (None if state == "SUCCEEDED" else "provider_not_accepted" if receipt["outcome"] == "NOT_ACCEPTED" else "provider_failure")
        result = conn.execute("""UPDATE operations SET state=%s,result=%s,error=%s,updated_at=clock_timestamp()
            WHERE id=%s RETURNING *""", (state, Jsonb(receipt), error, operation["id"])).fetchone()
        conn.execute("""INSERT INTO usage_ledger(run_id,operation_id,entry_key,kind,amount_micro_usd,reason,evidence)
            VALUES (%s,%s,%s,'CHARGE',%s,%s,%s)""", (run["id"], operation["id"], "charge:" + str(operation["id"]),
            actual, "provider_receipt", Jsonb(accounting_evidence(receipt))))
        event(conn, run["id"], "budget.settled", operation_id=str(operation["id"]), task_id=operation["task_id"],
              actual_micro_usd=actual, released_micro_usd=max(0, reservation["amount_micro_usd"] - actual))
        if breached:
            conn.execute("UPDATE runs SET failure=COALESCE(failure,'provider_bound_breached') WHERE id=%s AND state='RUNNING'", (run["id"],))
            event(conn, run["id"], "budget.breached", operation_id=str(operation["id"]))
        return result

    def reconcile_operation(self, operation_id, run_id=None, owner=None):
        # External lookup must never run inside a transaction holding a run lock.
        with connect(self.dsn) as conn:
            snapshot = conn.execute("SELECT o.*,r.owner_id FROM operations o JOIN runs r ON r.id=o.run_id WHERE o.id=%s", (operation_id,)).fetchone()
            if (not snapshot or (run_id is not None and str(snapshot["run_id"]) != str(run_id))
                    or (owner is not None and snapshot["owner_id"] != owner)):
                raise NotFound("Operation not found")
        if snapshot["request"]["provider"] != "fake-v1" and snapshot["state"] in {"DISPATCHED", "UNKNOWN"}:
            self._provider_for(snapshot).refresh(snapshot)
        with connect(self.dsn) as conn:
            run, operation = self._lock_operation(conn, operation_id, run_id, owner)
            conn.execute("UPDATE operations SET last_checked_at=clock_timestamp() WHERE id=%s", (operation_id,))
            if operation["state"] == "RESERVED":
                task = conn.execute("SELECT state FROM tasks WHERE run_id=%s AND id=%s", (run["id"], operation["task_id"])).fetchone()
                expired = conn.execute("SELECT deadline_at<=clock_timestamp() AS expired FROM runs WHERE id=%s", (run["id"],)).fetchone()["expired"]
                if run["state"] != "RUNNING" or run["failure"] or expired or task["state"] in {"FAILED", "CANCELLED", "SUCCEEDED"}:
                    return self._release_locked(conn, run, operation)
                return operation
            if operation["state"] not in {"DISPATCHED", "UNKNOWN"}:
                return operation
            receipt = self._provider_for(operation).evidence(conn, operation)
            if receipt is not None:
                return self._settle_locked(conn, run, operation, receipt)
            # A live dispatcher may be between its commit and provider acceptance.
            # Do not revoke its authority merely because a receipt is not present.
            return operation

    def _release_locked(self, conn, run, operation):
        amount = conn.execute("SELECT amount_micro_usd FROM budget_reservations WHERE operation_id=%s", (operation["id"],)).fetchone()["amount_micro_usd"]
        conn.execute("UPDATE budget_accounts SET reserved_micro_usd=reserved_micro_usd-%s WHERE run_id=%s", (amount, run["id"]))
        conn.execute("UPDATE budget_reservations SET state='RELEASED',finished_at=clock_timestamp() WHERE operation_id=%s", (operation["id"],))
        result = conn.execute("""UPDATE operations SET state='CANCELLED',error='undispatched_reservation_released',
            updated_at=clock_timestamp() WHERE id=%s RETURNING *""", (operation["id"],)).fetchone()
        event(conn, run["id"], "budget.released", operation_id=str(operation["id"]), amount_micro_usd=amount)
        return result

    def reconcile_pending(self, limit=100):
        with connect(self.dsn) as conn:
            rows = conn.execute("""SELECT id FROM operations WHERE state IN ('RESERVED','DISPATCHED','UNKNOWN')
                ORDER BY last_checked_at ASC NULLS FIRST,sequence LIMIT %s""", (limit,)).fetchall()
        for row in rows:
            self.reconcile_operation(row["id"])

    def summary(self, run_id, owner="local"):
        with connect(self.dsn) as conn:
            self._owned(conn, run_id, owner)
            return account_snapshot(conn, run_id)

    @staticmethod
    def _owned(conn, run_id, owner):
        if not conn.execute("SELECT 1 FROM runs WHERE id=%s AND owner_id=%s", (run_id, owner)).fetchone():
            raise NotFound("Run not found")

    def history(self, run_id, kind="operations", after=0, limit=100, owner="local"):
        with connect(self.dsn) as conn:
            self._owned(conn, run_id, owner)
            if kind == "operations":
                return conn.execute("""SELECT id,sequence,task_id,attempt_id,call_key,state,request_hash,quote,
                    result,error,created_at,updated_at FROM operations WHERE run_id=%s AND sequence>%s
                    ORDER BY sequence LIMIT %s""", (run_id, after, limit)).fetchall()
            if kind == "ledger":
                return conn.execute("SELECT * FROM usage_ledger WHERE run_id=%s AND sequence>%s ORDER BY sequence LIMIT %s", (run_id, after, limit)).fetchall()
            raise ValueError("Unknown budget history kind")

    def adjust(self, run_id, operation_id, adjustment, owner="local"):
        """Explicit owner correction, never available to an agent handler."""
        validate_message(adjustment, "adjustment")
        with connect(self.dsn) as conn:
            run, operation = self._lock_operation(conn, operation_id, run_id, owner)
            key = "adjust:" + adjustment["adjustment_key"]
            existing = conn.execute("SELECT * FROM usage_ledger WHERE run_id=%s AND entry_key=%s", (run_id, key)).fetchone()
            if existing:
                if str(existing["operation_id"]) != str(operation_id) or existing["amount_micro_usd"] != adjustment["delta_micro_usd"] or existing["reason"] != adjustment["reason"]:
                    raise Conflict("Adjustment key reused with different data")
                return existing
            charged = conn.execute("SELECT COALESCE(sum(amount_micro_usd),0) AS n FROM usage_ledger WHERE operation_id=%s", (operation_id,)).fetchone()["n"]
            delta = adjustment["delta_micro_usd"]
            if operation["state"] not in {"SUCCEEDED", "FAILED"} or operation["result"] is None:
                raise Conflict("Only settled operations can be adjusted")
            if charged + delta < 0:
                raise ValueError("Adjustment would make the operation's total charge negative")
            account = account_snapshot(conn, run_id)
            breach = account["settled_micro_usd"] + delta + account["reserved_micro_usd"] > account["limit_micro_usd"]
            conn.execute("""UPDATE budget_accounts SET settled_micro_usd=settled_micro_usd+%s,
                breached=breached OR %s WHERE run_id=%s""", (delta, breach, run_id))
            entry = conn.execute("""INSERT INTO usage_ledger(run_id,operation_id,entry_key,kind,amount_micro_usd,reason,evidence)
                VALUES (%s,%s,%s,'ADJUSTMENT',%s,%s,%s) RETURNING *""",
                (run_id, operation_id, key, delta, adjustment["reason"], Jsonb({"actor": owner}))).fetchone()
            event(conn, run_id, "budget.adjusted", actor=owner, operation_id=str(operation_id), amount_micro_usd=delta,
                  adjustment_key=adjustment["adjustment_key"], breached=breach)
            return entry
