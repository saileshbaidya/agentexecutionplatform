"""Versioned evidence/recovery evaluations using real durable execution.

Faults are harness-only operations in a private worker pool. Public submissions
cannot select faults or join that pool. Recorded is the default; live is opt-in.
"""

from dataclasses import replace
import hashlib
import json
from importlib.metadata import version
from pathlib import Path
import platform
import re
import threading
import time
from uuid import uuid4

from psycopg.types.json import Jsonb

from .agents import demo, execute_agent
from .budgets import BudgetDenied, OperationPending, ProviderFailure
from .context import Contexts, RECIPE_VERSION, digest
from .contracts import SCHEMA_PATH, StrictValidator
from .db import connect
from .execution import Conflict, Execution, Gone, NotFound, event
from .worker import run_claim, run_worker

EVALUATOR_VERSION = "evidence-recovery-v1"
FAULTS = ["none", "lease_expiry", "after_model_receipt", "context_corruption", "cancel_after_claim"]
CASE_SCHEMA = {"type": "object", "additionalProperties": False,
               "required": ["case_id", "fault", "budget_micro_usd", "expected_state", "min_findings", "unresolved_conflicts", "expected_statements", "max_operations"],
               "properties": {
                   "case_id": {"type": "string", "pattern": "^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,47}$"},
                   "fault": {"enum": FAULTS}, "budget_micro_usd": {"type": "integer", "minimum": 0, "maximum": 10000000},
                   "expected_state": {"enum": ["SUCCEEDED", "FAILED", "CANCELLED"]},
                   "expected_failure": {"type": ["string", "null"], "maxLength": 100},
                   "min_findings": {"type": "integer", "minimum": 0, "maximum": 100},
                   "unresolved_conflicts": {"type": "integer", "minimum": 0, "maximum": 100},
                   "expected_statements": {"type": "array", "maxItems": 30, "items": {"type": "string", "maxLength": 500}},
                   "max_operations": {"type": "integer", "minimum": 0, "maximum": 50}}}
DATASET_SCHEMA = {"type": "object", "additionalProperties": False,
                  "required": ["schema_version", "dataset_id", "version", "fixture_digest", "cases"],
                  "properties": {"schema_version": {"const": "1"},
                      "dataset_id": {"type": "string", "pattern": "^[a-zA-Z0-9_.-]{1,100}$"},
                      "version": {"type": "string", "minLength": 1, "maxLength": 100},
                      "fixture_digest": {"type": "string", "pattern": "^[a-f0-9]{64}$"},
                      "cases": {"type": "array", "minItems": 1, "maxItems": 20, "items": CASE_SCHEMA}}}


def validate_dataset(dataset):
    errors = list(StrictValidator(DATASET_SCHEMA).iter_errors(dataset))
    if errors:
        raise ValueError("Invalid evaluation dataset: " + errors[0].message)
    if len({c["case_id"] for c in dataset["cases"]}) != len(dataset["cases"]):
        raise ValueError("Duplicate evaluation case ID")
    if dataset["fixture_digest"] != digest(demo()):
        raise ValueError("Dataset fixture digest does not match packaged evidence/responses")


def load_dataset(path):
    raw = Path(path).read_bytes()
    if len(raw) > 256 * 1024:
        raise ValueError("Evaluation dataset exceeds 256 KiB")
    dataset = json.loads(raw)
    validate_dataset(dataset)
    return dataset


def runtime_fingerprint():
    directory = Path(__file__).parent
    files = sorted(directory.glob("*.py")) + sorted((directory / "migrations").glob("*.sql"))
    return digest({**{str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
                   "contracts": hashlib.sha256(SCHEMA_PATH.read_bytes()).hexdigest()})


def environment():
    return {"python": platform.python_version(), "packages": {name: version(name) for name in ("fastapi", "httpx", "psycopg", "jsonschema")}}


class Evaluations:
    def __init__(self, settings):
        self.settings = settings
        self.dsn = settings.database_url

    def start(self, dataset, key, label, live=False, timeout_seconds=120, owner="local"):
        validate_dataset(dataset)
        if not isinstance(key, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", key) or not isinstance(label, str) or not 1 <= len(label) <= 100:
            raise ValueError("Invalid evaluation key or label")
        if type(timeout_seconds) is not int or not 10 <= timeout_seconds <= 600:
            raise ValueError("Evaluation timeout must be between 10 and 600 seconds")
        if live and not self.settings.enable_openai:
            raise ValueError("Live evaluation requires an enabled model deployment")
        configuration = {"backend": "openai" if live else "recorded", "runtime_digest": runtime_fingerprint(),
                         "environment": environment(),
                         "context_recipe": RECIPE_VERSION, "fixture_digest": digest(demo()), "timeout_seconds": timeout_seconds,
                         "model": self.settings.openai_model if live else "synthetic-agent-demo-v1",
                         "pricing": {"version": self.settings.model_price_version, "input": self.settings.model_input_rate,
                                     "cached": self.settings.model_cached_input_rate, "output": self.settings.model_output_rate} if live else "recorded-usd-v1"}
        with connect(self.dsn) as conn:
            conn.execute("""INSERT INTO evaluation_batches
                (id,owner_id,evaluation_key,dataset_digest,dataset,configuration,label,evaluator_version,state)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'RUNNING') ON CONFLICT(owner_id,evaluation_key) DO NOTHING""",
                (uuid4(), owner, key, digest(dataset), Jsonb(dataset), Jsonb(configuration), label, EVALUATOR_VERSION))
            batch = conn.execute("SELECT * FROM evaluation_batches WHERE owner_id=%s AND evaluation_key=%s FOR UPDATE", (owner, key)).fetchone()
            if batch["dataset_digest"] != digest(dataset) or batch["configuration"] != configuration or batch["label"] != label:
                raise Conflict("Evaluation key already used with another dataset, version, label, or configuration")
            for case in dataset["cases"]:
                conn.execute("INSERT INTO evaluation_cases(batch_id,case_id) VALUES (%s,%s) ON CONFLICT DO NOTHING", (batch["id"], case["case_id"]))
            return str(batch["id"])

    def run(self, batch_id, owner="local"):
        # Session lock, not a long database transaction. A process crash releases
        # it automatically; rerunning the same key resumes stored cases/runs.
        with connect(self.dsn) as session:
            session.autocommit = True
            lock_key = int.from_bytes(hashlib.sha256(str(batch_id).encode()).digest()[:8], "big", signed=True)
            if not session.execute("SELECT pg_try_advisory_lock(%s) AS locked", (lock_key,)).fetchone()["locked"]:
                raise Conflict("Evaluation is already running")
            try:
                batch = session.execute("SELECT * FROM evaluation_batches WHERE id=%s AND owner_id=%s", (batch_id, owner)).fetchone()
                if not batch:
                    raise NotFound("Evaluation not found")
                if batch["configuration"]["runtime_digest"] != runtime_fingerprint():
                    raise Conflict("Resume requires the same runtime version; create a new evaluation key")
                if batch["configuration"]["environment"] != environment():
                    raise Conflict("Resume requires the same Python and dependency versions")
                if batch["configuration"]["backend"] == "openai" and (not self.settings.enable_openai or
                        batch["configuration"]["model"] != self.settings.openai_model or
                        batch["configuration"]["pricing"] != {"version": self.settings.model_price_version, "input": self.settings.model_input_rate,
                                                               "cached": self.settings.model_cached_input_rate, "output": self.settings.model_output_rate}):
                    raise Conflict("Resume requires the frozen model and price configuration")
                settings = replace(self.settings, worker_pool="eval-" + str(batch["id"]).replace("-", ""),
                                   lease_seconds=max(60, self.settings.lease_seconds), worker_maintenance=False, inline_reconciliation=True)
                engine = Execution(settings)
                for case in batch["dataset"]["cases"]:
                    saved = session.execute("SELECT * FROM evaluation_cases WHERE batch_id=%s AND case_id=%s", (batch_id, case["case_id"])).fetchone()
                    if saved["metrics"] is not None:
                        continue
                    fixture = demo()
                    run_id, _ = engine.submit({"schema_version": "1", "idempotency_key": "eval-" + str(batch["id"]).replace("-", "") + "-" + case["case_id"],
                        "request": fixture["request"], "documents": fixture["documents"], "agent": {"backend": batch["configuration"]["backend"]},
                        "budget_micro_usd": case["budget_micro_usd"], "timeout_seconds": batch["configuration"]["timeout_seconds"]}, owner)
                    session.execute("UPDATE evaluation_cases SET run_id=%s WHERE batch_id=%s AND case_id=%s", (run_id, batch_id, case["case_id"]))
                    deadline = time.monotonic() + batch["configuration"]["timeout_seconds"]
                    while True:
                        engine.maintain()
                        status = engine.status(run_id, owner)
                        if status["state"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                            break
                        if time.monotonic() >= deadline:
                            engine.cancel(run_id, owner)
                            raise TimeoutError("Evaluation case timed out; cancellation requested. Resume using the same key.")
                        saved = session.execute("SELECT injection_done FROM evaluation_cases WHERE batch_id=%s AND case_id=%s", (batch_id, case["case_id"])).fetchone()
                        if case["fault"] != "none" and not saved["injection_done"]:
                            claim = engine.claim("evaluation-injector")
                            if claim:
                                self._inject(engine, batch_id, case, claim, owner)
                        else:
                            run_worker(settings, once=True)
                        time.sleep(min(0.05, settings.poll_seconds))
                    metrics = self.score(engine, run_id, case, owner)
                    done = session.execute("SELECT injection_done FROM evaluation_cases WHERE batch_id=%s AND case_id=%s", (batch_id, case["case_id"])).fetchone()["injection_done"]
                    metrics["checks"]["fault_observed"] = case["fault"] == "none" or done
                    metrics["passed"] = all(metrics["checks"].values())
                    session.execute("UPDATE evaluation_cases SET metrics=%s WHERE batch_id=%s AND case_id=%s", (Jsonb(metrics), batch_id, case["case_id"]))
                session.execute("UPDATE evaluation_batches SET state='SUCCEEDED',finished_at=clock_timestamp() WHERE id=%s", (batch_id,))
            finally:
                session.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
        return self.report(batch_id, owner)

    def _inject(self, engine, batch_id, case, claim, owner):
        fault = case["fault"]
        if fault == "after_model_receipt":
            try:
                execute_agent(engine, claim)
            except OperationPending as pending:
                engine.wait_for_operation(claim, pending.operation_id)
                return
            except (ValueError, BudgetDenied, ProviderFailure):
                engine.fail(claim, reason="evaluation_generation_failed", retryable=False)
                return
        if fault == "context_corruption":
            Contexts(engine).assemble(claim)
        with connect(self.dsn) as conn:
            engine._lease(conn, claim)
            conn.execute("UPDATE evaluation_cases SET injection_done=true WHERE batch_id=%s AND case_id=%s", (batch_id, case["case_id"]))
            if fault in {"lease_expiry", "after_model_receipt"}:
                conn.execute("UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (claim["attempt_id"],))
            if fault == "context_corruption":
                conn.execute("UPDATE context_snapshots SET context=jsonb_set(context,'{request}','\"injected corruption\"') WHERE run_id=%s AND task_id=%s", (claim["run_id"], claim["task_id"]))
            event(conn, claim["run_id"], "evaluation.fault_injected", actor="evaluation-harness", task_id=claim["task_id"], fault=fault, batch_id=str(batch_id))
        if fault == "cancel_after_claim":
            engine.cancel(claim["run_id"], owner)
            engine.acknowledge_stop(claim)
        if fault == "context_corruption":
            run_claim(engine, claim, threading.Event())

    def score(self, engine, run_id, case, owner="local"):
        run = engine.status(run_id, owner)
        if run["payload_purged_at"]:
            raise Gone("Cannot evaluate purged run payloads")
        findings, conflicts, coverage, replay_ok = [], [], True, True
        if run["state"] == "SUCCEEDED":
            _, raw = engine.artifact(run_id, run["final_artifact_id"], owner)
            artifact = json.loads(raw)
            findings = artifact.get("findings", [])
            conflicts = artifact.get("review", {}).get("conflicts", [])
            for finding in findings:
                if not finding["sources"]:
                    coverage = False
                for source in finding["sources"]:
                    try:
                        metadata, content = engine.artifact(run_id, source["artifact"]["artifact_id"], owner)
                        prefix, start, end = source["locator"].split(":")
                        coverage &= (prefix == "chars" and metadata["sha256"] == source["artifact"]["sha256"]
                                     and content.decode()[int(start):int(end)] == source["quote"])
                    except (KeyError, ValueError, NotFound, OSError):
                        coverage = False
            try:
                Contexts(engine).replay(run_id, owner)
            except (ValueError, Conflict, KeyError, OSError):
                replay_ok = False
        with connect(self.dsn) as conn:
            operations = conn.execute("SELECT * FROM operations WHERE run_id=%s", (run_id,)).fetchall()
            snapshots = conn.execute("SELECT task_id,context_digest,manifest_digest FROM context_snapshots WHERE run_id=%s ORDER BY task_id", (run_id,)).fetchall()
        checks = {"state": run["state"] == case["expected_state"], "findings": len(findings) >= case["min_findings"],
                  "failure_reason": "expected_failure" not in case or run["failure"] == case["expected_failure"],
                  "conflicts": sum(c["status"] == "unresolved" for c in conflicts) == case["unresolved_conflicts"],
                  "expected_statements": all(s in {f["statement"] for f in findings} for s in case["expected_statements"]),
                  "citation_integrity": bool(coverage), "offline_replay": replay_ok,
                  "operation_limit": len(operations) <= case["max_operations"], "no_unsettled_budget": run["budget"]["reserved_micro_usd"] == 0}
        return {"checks": checks, "state": run["state"], "failure": run["failure"], "findings": len(findings),
                "unresolved_conflicts": sum(c["status"] == "unresolved" for c in conflicts), "operations": len(operations),
                "attempts": len(run["attempts"]), "cost_micro_usd": run["budget"]["settled_micro_usd"],
                "input_tokens": sum((o["result"] or {}).get("input_tokens", 0) for o in operations),
                "output_tokens": sum((o["result"] or {}).get("output_tokens", 0) for o in operations),
                "duration_seconds": (run["updated_at"] - run["created_at"]).total_seconds(), "contexts": snapshots}

    def report(self, batch_id, owner="local"):
        with connect(self.dsn) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            batch = conn.execute("SELECT * FROM evaluation_batches WHERE id=%s AND owner_id=%s", (batch_id, owner)).fetchone()
            if not batch:
                raise NotFound("Evaluation not found")
            batch["cases"] = conn.execute("SELECT case_id,run_id,injection_done,metrics FROM evaluation_cases WHERE batch_id=%s ORDER BY case_id", (batch_id,)).fetchall()
        batch["passed"] = batch["state"] == "SUCCEEDED" and all(c["metrics"] and c["metrics"]["passed"] for c in batch["cases"])
        batch["total_cost_micro_usd"] = sum((c["metrics"] or {}).get("cost_micro_usd", 0) for c in batch["cases"])
        return batch

    def compare(self, baseline_id, candidate_id, owner="local"):
        baseline, candidate = self.report(baseline_id, owner), self.report(candidate_id, owner)
        if (baseline["state"] != "SUCCEEDED" or candidate["state"] != "SUCCEEDED" or
                baseline["dataset_digest"] != candidate["dataset_digest"] or baseline["evaluator_version"] != candidate["evaluator_version"]):
            raise Conflict("Comparison requires completed evaluations of the same dataset and evaluator version")
        previous = {c["case_id"]: c["metrics"] for c in baseline["cases"]}
        rows = []
        for case in candidate["cases"]:
            old, new = previous[case["case_id"]], case["metrics"]
            rows.append({"case_id": case["case_id"], "baseline_passed": old["passed"], "candidate_passed": new["passed"],
                         "regression": old["passed"] and not new["passed"],
                         **{key + "_delta": new[key] - old[key] for key in ("cost_micro_usd", "input_tokens", "output_tokens", "duration_seconds", "attempts")}})
        return {"baseline_id": str(baseline_id), "candidate_id": str(candidate_id), "cases": rows,
                "regressions": sum(row["regression"] for row in rows),
                "baseline_configuration": baseline["configuration"], "candidate_configuration": candidate["configuration"]}
