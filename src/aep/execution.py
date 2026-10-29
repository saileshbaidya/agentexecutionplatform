"""PostgreSQL execution state. Lock the run before mutating any child records.

Run locks serialize short metadata transactions, never handler execution.
Different runs and claimed tasks may execute concurrently.
"""

from datetime import timedelta
import hashlib
import json
import random
from uuid import uuid4

from psycopg.types.json import Jsonb

from .artifacts import LocalArtifacts
from .contracts import validate_message, validate_plan
from .db import connect
from .planner import HANDLERS, make_plan


class Conflict(Exception):
    pass


class NotFound(Exception):
    pass


class LeaseLost(Exception):
    pass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def event(conn, run_id, kind, actor="scheduler", task_id=None, attempt_id=None, **data):
    sequence = conn.execute("""UPDATE runs SET event_sequence=event_sequence+1,
        updated_at=clock_timestamp() WHERE id=%s RETURNING event_sequence""", (run_id,)).fetchone()["event_sequence"]
    conn.execute("""INSERT INTO events(run_id,sequence,kind,actor,task_id,attempt_id,data)
        VALUES (%s,%s,%s,%s,%s,%s,%s)""",
        (run_id, sequence, kind, actor, task_id, attempt_id, Jsonb(data)))


class Execution:
    def __init__(self, settings):
        self.settings = settings
        self.store = LocalArtifacts(settings.artifact_root)

    def submit(self, submission, owner="local"):
        validate_message(submission, "submission")
        # Artifact ingestion/authorization is not exposed yet. Do not silently ignore input.
        if submission.get("input_artifacts"):
            raise ValueError("Input artifacts are not supported by the deterministic planner")
        normalized = dict(submission, input_artifacts=submission.get("input_artifacts", []))
        digest = hashlib.sha256(canonical(normalized)).hexdigest()
        with connect(self.settings.database_url) as conn:
            created = conn.execute("""INSERT INTO runs
                (id,owner_id,idempotency_key,input_hash,submission,state,deadline_at)
                VALUES (%s,%s,%s,%s,%s,'SUBMITTED',clock_timestamp()+(%s * interval '1 second'))
                ON CONFLICT(owner_id,idempotency_key) DO NOTHING RETURNING id""",
                (uuid4(), owner, submission["idempotency_key"], digest, Jsonb(normalized), submission["timeout_seconds"])).fetchone()
            run = conn.execute("SELECT * FROM runs WHERE owner_id=%s AND idempotency_key=%s FOR UPDATE",
                               (owner, submission["idempotency_key"])).fetchone()
            if run["input_hash"] != digest:
                raise Conflict("Idempotency key already used with different input")
            if created:
                event(conn, run["id"], "run.submitted", actor=owner)
            return str(run["id"]), bool(created)

    def status(self, run_id, owner="local"):
        with connect(self.settings.database_url) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            run = conn.execute("""SELECT id,state,failure,final_artifact_id,created_at,updated_at,
                deadline_at,event_sequence,plan FROM runs WHERE id=%s AND owner_id=%s""", (run_id, owner)).fetchone()
            if not run:
                raise NotFound("Run not found")
            run["tasks"] = conn.execute("""SELECT id,state,attempt_count,fencing_token,next_run_at,result
                FROM tasks WHERE run_id=%s ORDER BY id""", (run_id,)).fetchall()
            run["attempts"] = conn.execute("""SELECT id,task_id,number,worker_id,state,started_at,
                lease_expires_at,finished_at,error FROM attempts WHERE run_id=%s ORDER BY started_at,id""", (run_id,)).fetchall()
            return run

    def events(self, run_id, after=0, limit=100, owner="local"):
        with connect(self.settings.database_url) as conn:
            if not conn.execute("SELECT 1 FROM runs WHERE id=%s AND owner_id=%s", (run_id, owner)).fetchone():
                raise NotFound("Run not found")
            return conn.execute("""SELECT sequence,kind,actor,task_id,attempt_id,data,created_at FROM events
                WHERE run_id=%s AND sequence>%s ORDER BY sequence LIMIT %s""", (run_id, after, limit)).fetchall()

    def artifact(self, run_id, artifact_id, owner="local"):
        with connect(self.settings.database_url) as conn:
            metadata = conn.execute("""SELECT a.* FROM artifacts a JOIN runs r ON r.id=a.run_id
                WHERE a.run_id=%s AND a.id=%s AND r.owner_id=%s""", (run_id, artifact_id, owner)).fetchone()
            if not metadata:
                raise NotFound("Artifact not found")
        return metadata, self.store.read(metadata)

    def maintain(self):
        """Recover submitted runs and expired work. Safe for multiple schedulers."""
        with connect(self.settings.database_url) as conn:
            ids = conn.execute("SELECT id FROM runs WHERE state IN ('SUBMITTED','RUNNING','CANCELLING') ORDER BY created_at").fetchall()
        for row in ids:
            with connect(self.settings.database_url) as conn:
                run = conn.execute("SELECT * FROM runs WHERE id=%s FOR UPDATE SKIP LOCKED", (row["id"],)).fetchone()
                if not run:
                    continue
                if run["state"] == "SUBMITTED":
                    self._plan(conn, run)
                self._advance(conn, run["id"])

    def _plan(self, conn, run):
        # This planner is pure, bounded, and free; admission fits in one transaction.
        # No model/network call may be added inside this transaction.
        if run["deadline_at"] <= conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]:
            return
        conn.execute("UPDATE runs SET state='PLANNING' WHERE id=%s", (run["id"],))
        event(conn, run["id"], "run.planning", planner="deterministic-v1")
        try:
            plan = make_plan()
            validate_plan(plan)
            if any(t["handler"] not in HANDLERS for t in plan["tasks"]):
                raise ValueError("Unknown handler")
        except Exception as exc:
            reason = "invalid_plan" if isinstance(exc, ValueError) else "planner_error"
            conn.execute("UPDATE runs SET state='FAILED',failure=%s WHERE id=%s", (reason, run["id"]))
            event(conn, run["id"], "run.failed", reason=reason)
            return
        for task in plan["tasks"]:
            state = "BLOCKED" if task["depends_on"] else "READY"
            conn.execute("INSERT INTO tasks(run_id,id,spec,state) VALUES (%s,%s,%s,%s)",
                         (run["id"], task["task_id"], Jsonb(task), state))
            event(conn, run["id"], "task." + state.lower(), task_id=task["task_id"])
        for task in plan["tasks"]:
            for dependency in task["depends_on"]:
                conn.execute("INSERT INTO dependencies VALUES (%s,%s,%s)", (run["id"], task["task_id"], dependency))
        conn.execute("UPDATE runs SET state='RUNNING',plan=%s WHERE id=%s", (Jsonb(plan), run["id"]))
        event(conn, run["id"], "run.running", plan_id=plan["plan_id"], revision=1)

    def _retry_or_fail(self, conn, run_id, task, reason, now):
        policy = task["spec"]["retry"]
        expired = task["first_started_at"] is not None and now >= task["first_started_at"] + timedelta(seconds=task["spec"]["timeout_seconds"])
        if task["attempt_count"] < policy["max_attempts"] and not expired:
            cap = min(policy["max_backoff_seconds"], policy["initial_backoff_seconds"] * 2 ** (task["attempt_count"] - 1))
            next_run = now + timedelta(seconds=random.uniform(0, cap))
            conn.execute("UPDATE tasks SET state='RETRY_WAIT',next_run_at=%s WHERE run_id=%s AND id=%s", (next_run, run_id, task["id"]))
            event(conn, run_id, "task.retry_wait", task_id=task["id"], reason=reason, next_run_at=next_run.isoformat())
        else:
            conn.execute("UPDATE tasks SET state='FAILED' WHERE run_id=%s AND id=%s", (run_id, task["id"]))
            conn.execute("UPDATE runs SET failure=COALESCE(failure,%s) WHERE id=%s", (reason, run_id))
            event(conn, run_id, "task.failed", task_id=task["id"], reason=reason)

    def _advance(self, conn, run_id):
        run = conn.execute("SELECT * FROM runs WHERE id=%s", (run_id,)).fetchone()
        if run["state"] not in {"SUBMITTED", "RUNNING", "CANCELLING"}:
            return
        now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
        if run["deadline_at"] <= now and run["state"] != "CANCELLING" and not run["failure"]:
            conn.execute("UPDATE runs SET state='CANCELLING',failure='deadline_exceeded' WHERE id=%s", (run_id,))
            event(conn, run_id, "run.cancelling", reason="deadline_exceeded")
            run["state"] = "CANCELLING"
        tasks = conn.execute("SELECT * FROM tasks WHERE run_id=%s ORDER BY id", (run_id,)).fetchall()
        for task in tasks:
            if task["state"] != "RUNNING":
                continue
            attempt = conn.execute("SELECT * FROM attempts WHERE run_id=%s AND task_id=%s AND state='ACTIVE'", (run_id, task["id"])).fetchone()
            timed_out = now >= task["first_started_at"] + timedelta(seconds=task["spec"]["timeout_seconds"])
            if attempt["lease_expires_at"] > now and not timed_out:
                continue
            draining = run["state"] == "CANCELLING" or run["failure"]
            attempt_state = "CANCELLED" if draining else "EXPIRED"
            conn.execute("UPDATE attempts SET state=%s,finished_at=%s,error=%s WHERE id=%s",
                         (attempt_state, now, "task_timeout" if timed_out else "lease_expired", attempt["id"]))
            event(conn, run_id, "attempt." + attempt_state.lower(), task_id=task["id"], attempt_id=attempt["id"])
            if draining:
                conn.execute("UPDATE tasks SET state='CANCELLED' WHERE run_id=%s AND id=%s", (run_id, task["id"]))
                event(conn, run_id, "task.cancelled", task_id=task["id"])
            else:
                self._retry_or_fail(conn, run_id, task, "task_timeout" if timed_out else "lease_expired", now)
        # Re-read failure, since retry exhaustion above may have started fail-fast drain.
        run = conn.execute("SELECT * FROM runs WHERE id=%s", (run_id,)).fetchone()
        if run["failure"] or run["state"] == "CANCELLING":
            cancelled = conn.execute("""UPDATE tasks SET state='CANCELLED' WHERE run_id=%s
                AND state IN ('BLOCKED','READY','RETRY_WAIT') RETURNING id""", (run_id,)).fetchall()
            for task in cancelled:
                event(conn, run_id, "task.cancelled", task_id=task["id"])
            active = conn.execute("SELECT 1 FROM attempts WHERE run_id=%s AND state='ACTIVE' LIMIT 1", (run_id,)).fetchone()
            if not active:
                terminal = "CANCELLED" if run["state"] == "CANCELLING" else "FAILED"
                conn.execute("UPDATE runs SET state=%s WHERE id=%s", (terminal, run_id))
                event(conn, run_id, "run." + terminal.lower(), reason=run["failure"])
            return
        # Retry deadlines cover the entire task lifetime, not just live attempts.
        waiting = conn.execute("SELECT * FROM tasks WHERE run_id=%s AND state='RETRY_WAIT'", (run_id,)).fetchall()
        for task in waiting:
            if now >= task["first_started_at"] + timedelta(seconds=task["spec"]["timeout_seconds"]):
                self._retry_or_fail(conn, run_id, task, "task_timeout", now)
                return self._advance(conn, run_id)
        ready = conn.execute("""UPDATE tasks t SET state='READY' WHERE t.run_id=%s AND (
            (t.state='RETRY_WAIT' AND t.next_run_at<=%s) OR
            (t.state='BLOCKED' AND NOT EXISTS (
                SELECT 1 FROM dependencies d JOIN tasks parent ON parent.run_id=d.run_id AND parent.id=d.dependency_id
                WHERE d.run_id=t.run_id AND d.task_id=t.id AND parent.state<>'SUCCEEDED')))
            RETURNING id""", (run_id, now)).fetchall()
        for task in ready:
            event(conn, run_id, "task.ready", task_id=task["id"])
        if tasks and not conn.execute("SELECT 1 FROM tasks WHERE run_id=%s AND state<>'SUCCEEDED' LIMIT 1", (run_id,)).fetchone():
            final = conn.execute("""SELECT a.id FROM artifacts a JOIN tasks t ON t.run_id=a.run_id AND t.id=a.task_id
                WHERE a.run_id=%s AND t.spec->>'kind'='report_writer' ORDER BY a.created_at DESC LIMIT 1""", (run_id,)).fetchone()
            if not final:
                raise RuntimeError("Successful writer has no artifact")
            conn.execute("UPDATE runs SET state='SUCCEEDED',final_artifact_id=%s WHERE id=%s", (final["id"], run_id))
            event(conn, run_id, "run.succeeded", artifact_id=str(final["id"]))

    def claim(self, worker_id):
        with connect(self.settings.database_url) as conn:
            run = conn.execute("""SELECT r.* FROM runs r WHERE r.state='RUNNING' AND r.failure IS NULL
                AND EXISTS(SELECT 1 FROM tasks t WHERE t.run_id=r.id AND t.state='READY')
                ORDER BY r.created_at FOR UPDATE OF r SKIP LOCKED LIMIT 1""").fetchone()
            if not run:
                return None
            self._advance(conn, run["id"])
            current = conn.execute("SELECT state,failure FROM runs WHERE id=%s", (run["id"],)).fetchone()
            if current["state"] != "RUNNING" or current["failure"]:
                return None
            task = conn.execute("SELECT * FROM tasks WHERE run_id=%s AND state='READY' ORDER BY id LIMIT 1", (run["id"],)).fetchone()
            if not task:
                return None
            # READY after a retry must also respect the task's lifetime deadline.
            now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
            if task["first_started_at"] and now >= task["first_started_at"] + timedelta(seconds=task["spec"]["timeout_seconds"]):
                self._retry_or_fail(conn, run["id"], task, "task_timeout", now)
                self._advance(conn, run["id"])
                return None
            task = conn.execute("""UPDATE tasks SET state='RUNNING',attempt_count=attempt_count+1,
                fencing_token=fencing_token+1,first_started_at=COALESCE(first_started_at,clock_timestamp())
                WHERE run_id=%s AND id=%s RETURNING *""", (run["id"], task["id"])).fetchone()
            attempt_id = uuid4()
            conn.execute("""INSERT INTO attempts(id,run_id,task_id,number,fencing_token,worker_id,state,lease_expires_at)
                VALUES (%s,%s,%s,%s,%s,%s,'ACTIVE',clock_timestamp()+(%s * interval '1 second'))""",
                (attempt_id, run["id"], task["id"], task["attempt_count"], task["fencing_token"], worker_id, self.settings.lease_seconds))
            event(conn, run["id"], "attempt.claimed", actor=worker_id, task_id=task["id"], attempt_id=attempt_id, fencing_token=task["fencing_token"])
            inputs = conn.execute("""SELECT p.id,p.result FROM dependencies d JOIN tasks p
                ON p.run_id=d.run_id AND p.id=d.dependency_id WHERE d.run_id=%s AND d.task_id=%s ORDER BY p.id""",
                (run["id"], task["id"])).fetchall()
            return {"run_id": str(run["id"]), "task_id": task["id"], "attempt_id": str(attempt_id),
                    "fencing_token": task["fencing_token"], "worker_id": worker_id,
                    "spec": task["spec"], "inputs": {p["id"]: p["result"] for p in inputs},
                    "request": run["submission"]["request"]}

    def _lease(self, conn, claim):
        run = conn.execute("SELECT * FROM runs WHERE id=%s FOR UPDATE", (claim["run_id"],)).fetchone()
        lease = conn.execute("""SELECT a.*,t.spec,t.first_started_at FROM attempts a JOIN tasks t
            ON t.run_id=a.run_id AND t.id=a.task_id WHERE a.id=%s AND a.run_id=%s AND a.task_id=%s
            AND a.worker_id=%s AND a.fencing_token=%s AND t.fencing_token=a.fencing_token
            AND t.state='RUNNING' AND a.state='ACTIVE' AND a.lease_expires_at>clock_timestamp()
            AND t.first_started_at + ((t.spec->>'timeout_seconds')::int * interval '1 second')>clock_timestamp()""",
            (claim["attempt_id"], claim["run_id"], claim["task_id"], claim["worker_id"], claim["fencing_token"])).fetchone()
        if not run or run["state"] != "RUNNING" or run["failure"] or not lease:
            raise LeaseLost("Attempt no longer owns an active lease")
        if run["deadline_at"] <= conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]:
            raise LeaseLost("Run deadline exceeded")
        return lease

    def heartbeat(self, claim):
        with connect(self.settings.database_url) as conn:
            self._lease(conn, claim)
            conn.execute("UPDATE attempts SET lease_expires_at=clock_timestamp()+(%s * interval '1 second') WHERE id=%s",
                         (self.settings.lease_seconds, claim["attempt_id"]))

    def complete(self, claim, output):
        # Publish bytes first. Failed commits may leave invisible orphan files.
        metadata = self.store.put(claim["run_id"], canonical(output))
        artifact_id = uuid4()
        result = {"schema_version": "1", "task_id": claim["task_id"], "attempt_id": claim["attempt_id"],
                  "artifacts": [{"artifact_id": str(artifact_id), "sha256": metadata["sha256"]}],
                  "findings": output.get("findings", [])}
        for fact in output.get("claims", []):
            result["findings"].append({"claim_id": fact["claim_id"], "statement": fact["statement"],
                "sources": [{"artifact": result["artifacts"][0], "locator": "claims/" + fact["claim_id"]}]})
        if "review" in output:
            result["review"] = output["review"]
        validate_message(result, "taskResult")
        with connect(self.settings.database_url) as conn:
            self._lease(conn, claim)
            conn.execute("""INSERT INTO artifacts(id,run_id,task_id,attempt_id,sha256,storage_key,media_type,size_bytes)
                VALUES (%s,%s,%s,%s,%s,%s,'application/json',%s)""",
                (artifact_id, claim["run_id"], claim["task_id"], claim["attempt_id"], metadata["sha256"], metadata["storage_key"], metadata["size_bytes"]))
            # Validate again after metadata writes, before accepting the outcome.
            self._lease(conn, claim)
            conn.execute("UPDATE attempts SET state='SUCCEEDED',finished_at=clock_timestamp() WHERE id=%s", (claim["attempt_id"],))
            conn.execute("UPDATE tasks SET state='SUCCEEDED',result=%s WHERE run_id=%s AND id=%s", (Jsonb(result), claim["run_id"], claim["task_id"]))
            event(conn, claim["run_id"], "attempt.succeeded", actor=claim["worker_id"], task_id=claim["task_id"], attempt_id=claim["attempt_id"])
            event(conn, claim["run_id"], "task.succeeded", task_id=claim["task_id"], artifact_id=str(artifact_id))
            self._advance(conn, claim["run_id"])
        return result

    def fail(self, claim, reason="handler_error", retryable=True):
        with connect(self.settings.database_url) as conn:
            self._lease(conn, claim)
            conn.execute("UPDATE attempts SET state='FAILED',finished_at=clock_timestamp(),error=%s WHERE id=%s", (reason, claim["attempt_id"]))
            event(conn, claim["run_id"], "attempt.failed", actor=claim["worker_id"], task_id=claim["task_id"], attempt_id=claim["attempt_id"], reason=reason)
            task = conn.execute("SELECT * FROM tasks WHERE run_id=%s AND id=%s", (claim["run_id"], claim["task_id"])).fetchone()
            if not retryable:
                task["spec"]["retry"]["max_attempts"] = task["attempt_count"]
            now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
            self._retry_or_fail(conn, claim["run_id"], task, reason, now)
            self._advance(conn, claim["run_id"])
