"""A bounded, durable fake provider. No network requests or actual charges.

For this adapter only, one UTF-8 byte is one input token. All rates and output
limits are enforced by the provider, making its upper bound trustworthy.
"""

from psycopg.types.json import Jsonb

from .contracts import validate_message
from .db import connect


class OutcomeUnknown(Exception):
    pass


def quote_call(spec):
    validate_message(spec, "paidCall")
    request = dict(spec, mode=spec.get("mode", "success"),
                   reconcile_delay_seconds=spec.get("reconcile_delay_seconds", 5))
    if len(request["input"].encode("utf-8")) > request["max_input_tokens"]:
        raise ValueError("Input exceeds the provider input-token limit")
    if request["output_tokens"] > request["max_output_tokens"]:
        raise ValueError("Output exceeds the provider output-token limit")
    bound = request["max_input_tokens"] * 2 + request["max_output_tokens"] * 5
    if bound > request["max_cost_micro_usd"]:
        raise ValueError("Price bound exceeds the per-call cost limit")
    quote = {"provider": "fake-v1", "price_version": "fake-usd-v1", "currency": "USD",
             "input_rate_micro_usd": 2, "output_rate_micro_usd": 5, "bound_micro_usd": bound,
             "max_input_tokens": request["max_input_tokens"], "max_output_tokens": request["max_output_tokens"]}
    return request, quote


class FakeProvider:
    def __init__(self, engine):
        self.engine = engine

    def execute(self, operation_id, dispatch_token, claim):
        """Acceptance and the receipt commit atomically in the fake provider store.

        Run locking and state checks also fence a delayed, unaccepted fake request.
        Real providers will need their own idempotency and reconciliation protocol.
        """
        from .execution import Conflict, LeaseLost
        with connect(self.engine.settings.database_url) as conn:
            self.engine._lease(conn, claim)
            operation = conn.execute("SELECT * FROM operations WHERE id=%s AND run_id=%s AND task_id=%s",
                                     (operation_id, claim["run_id"], claim["task_id"])).fetchone()
            if not operation or str(operation["dispatch_token"]) != str(dispatch_token):
                raise Conflict("Invalid provider dispatch token")
            previous = conn.execute("SELECT receipt FROM fake_provider_receipts WHERE operation_id=%s", (operation_id,)).fetchone()
            if previous:
                return previous["receipt"]
            if operation["state"] != "DISPATCHED" or str(operation["attempt_id"]) != claim["attempt_id"]:
                raise LeaseLost("Provider dispatch no longer authorized")
            request, quote = operation["request"], operation["quote"]
            _, enforced_quote = quote_call(request)
            if quote != enforced_quote:
                raise Conflict("Provider price or bounds differ from the admitted quote")
            if request["mode"] == "timeout_before_accept":
                raise OutcomeUnknown("No provider response")
            output_tokens = 0 if request["mode"] == "known_failure" else request["output_tokens"]
            input_tokens = len(request["input"].encode("utf-8"))
            receipt = {"provider_request_id": "fake-" + str(operation_id), "request_hash": operation["request_hash"],
                       "outcome": "FAILED" if request["mode"] == "known_failure" else "SUCCEEDED",
                       "input_tokens": input_tokens, "output_tokens": output_tokens,
                       "cost_micro_usd": input_tokens * quote["input_rate_micro_usd"] + output_tokens * quote["output_rate_micro_usd"],
                       "result": {"fixture": "bounded fake provider", "text": "synthetic provider output"}}
            delay = request["reconcile_delay_seconds"] if request["mode"] == "timeout_after_accept" else 0
            conn.execute("""INSERT INTO fake_provider_receipts(operation_id,request_hash,receipt,available_at)
                VALUES (%s,%s,%s,clock_timestamp()+(%s * interval '1 second'))""",
                (operation_id, operation["request_hash"], Jsonb(receipt), delay))
        if request["mode"] == "timeout_after_accept":
            raise OutcomeUnknown("Accepted request; response lost")
        return receipt

    @staticmethod
    def evidence(conn, operation):
        """SQL-only lookup, called under the run lock. Never perform network I/O here."""
        evidence = conn.execute("""SELECT receipt,available_at<=clock_timestamp() AS ready
            FROM fake_provider_receipts WHERE operation_id=%s""", (operation["id"],)).fetchone()
        if evidence:
            return evidence["receipt"] if evidence["ready"] else None
        # The fake adapter can close an unaccepted request once dispatch authority
        # is gone. Its acceptance gate uses this same lock/state protocol, so no
        # late fake request can be accepted after this definitive no-charge result.
        active = conn.execute("""SELECT 1 FROM attempts a JOIN runs r ON r.id=a.run_id JOIN tasks t
            ON t.run_id=a.run_id AND t.id=a.task_id WHERE a.id=%s AND a.state='ACTIVE'
            AND a.lease_expires_at>clock_timestamp() AND r.state='RUNNING' AND r.failure IS NULL
            AND r.deadline_at>clock_timestamp() AND t.state='RUNNING'
            AND t.fencing_token=a.fencing_token
            AND t.first_started_at+((t.spec->>'timeout_seconds')::int * interval '1 second')>clock_timestamp()""",
            (operation["attempt_id"],)).fetchone()
        if active:
            return None
        receipt = {"provider_request_id": "fake-" + str(operation["id"]), "request_hash": operation["request_hash"],
                   "outcome": "NOT_ACCEPTED", "input_tokens": 0, "output_tokens": 0, "cost_micro_usd": 0,
                   "result": {"reason": "dispatch authority closed before provider acceptance"}}
        conn.execute("""INSERT INTO fake_provider_receipts(operation_id,request_hash,receipt,available_at)
            VALUES (%s,%s,%s,clock_timestamp())""", (operation["id"], operation["request_hash"], Jsonb(receipt)))
        return receipt
