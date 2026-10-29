"""Real PostgreSQL tests. Each test owns a fresh, randomly named schema.

Set TEST_DATABASE_URL explicitly; an unset URL skips this integration module.
No existing application tables are truncated or dropped.
"""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from fastapi.testclient import TestClient

from aep.api import create_app
from aep.config import Settings
from aep.db import connect, migrate
from aep.execution import Conflict, Execution, LeaseLost, NotFound
from aep.execution import event as append_event
from aep.handlers import execute
from aep.worker import run_worker


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL to run PostgreSQL integration tests")
class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.admin_dsn = os.environ["TEST_DATABASE_URL"]
        self.schema = "aep_test_" + uuid4().hex
        with psycopg.connect(self.admin_dsn) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
        self.dsn = make_conninfo(self.admin_dsn, options=f"-csearch_path={self.schema}")
        self.temporary = tempfile.TemporaryDirectory()
        self.settings = Settings(self.dsn, Path(self.temporary.name), "test-key", lease_seconds=3, poll_seconds=0.05)
        migrate(self.dsn)
        self.engine = Execution(self.settings)

    def tearDown(self):
        self.temporary.cleanup()
        if not self.schema.startswith("aep_test_") or len(self.schema) != 41:
            raise RuntimeError("Refusing to drop unexpected schema")
        with psycopg.connect(self.admin_dsn) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    def submission(self, **changes):
        value = {"schema_version": "1", "idempotency_key": "test-request", "request": "Compare the synthetic evidence",
                 "budget_micro_usd": 0, "timeout_seconds": 600}
        value.update(changes)
        return value

    def start(self):
        run_id, _ = self.engine.submit(self.submission())
        self.engine.maintain()
        return run_id

    def expire(self, claim):
        with connect(self.dsn) as conn:
            conn.execute("UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (claim["attempt_id"],))

    def ready_retries(self):
        with connect(self.dsn) as conn:
            conn.execute("UPDATE tasks SET next_run_at=clock_timestamp()-interval '1 second' WHERE state='RETRY_WAIT'")
        self.engine.maintain()

    def test_migrations_idempotent_and_checksummed(self):
        migrate(self.dsn)
        with connect(self.dsn) as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM schema_migrations").fetchone()["n"], 1)
            conn.execute("UPDATE schema_migrations SET sha256=%s", ("0" * 64,))
        with self.assertRaisesRegex(RuntimeError, "changed"):
            migrate(self.dsn)

    def test_concurrent_submission_is_idempotent(self):
        barrier = threading.Barrier(4)
        def submit(_):
            barrier.wait()
            return self.engine.submit(self.submission())
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(submit, range(4)))
        self.assertEqual(len({row[0] for row in results}), 1)
        self.assertEqual(sum(row[1] for row in results), 1)
        self.assertEqual(len(self.engine.events(results[0][0])), 1)
        with self.assertRaises(Conflict):
            self.engine.submit(self.submission(request="different input"))

    def test_invalid_submission_persists_nothing(self):
        with self.assertRaises(ValueError):
            self.engine.submit(self.submission(budget_micro_usd=-1))
        with self.assertRaises(ValueError):
            self.engine.submit(self.submission(input_artifacts=[{"artifact_id": "other", "sha256": "a" * 64}]))
        with connect(self.dsn) as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM runs").fetchone()["n"], 0)

    def test_planning_failure_is_durable_and_atomic(self):
        run_id, _ = self.engine.submit(self.submission())
        with patch("aep.execution.make_plan", return_value={}):
            self.engine.maintain()
        status = self.engine.status(run_id)
        self.assertEqual(status["state"], "FAILED")
        self.assertEqual(status["failure"], "invalid_plan")
        self.assertEqual(status["tasks"], [])

    def test_unexpected_planner_failure_is_terminal(self):
        run_id, _ = self.engine.submit(self.submission())
        with patch("aep.execution.make_plan", side_effect=RuntimeError("injected planner fault")):
            self.engine.maintain()
        self.assertEqual(self.engine.status(run_id)["state"], "FAILED")
        self.assertEqual(self.engine.status(run_id)["failure"], "planner_error")

    def test_crash_during_plan_admission_leaves_no_partial_graph(self):
        run_id, _ = self.engine.submit(self.submission())
        def interrupted_event(conn, run_id, kind, **kwargs):
            if kwargs.get("task_id") == "review":
                raise RuntimeError("injected admission interruption")
            return append_event(conn, run_id, kind, **kwargs)
        with patch("aep.execution.event", side_effect=interrupted_event):
            with self.assertRaises(RuntimeError):
                self.engine.maintain()
        status = self.engine.status(run_id)
        self.assertEqual(status["state"], "SUBMITTED")
        self.assertEqual(status["tasks"], [])
        self.assertEqual(len(self.engine.events(run_id)), 1)
        self.engine.maintain()
        self.assertEqual(self.engine.status(run_id)["state"], "RUNNING")

    def test_database_errors_are_service_unavailable(self):
        with TestClient(create_app(self.settings)) as client:
            with patch("aep.api.connect", side_effect=psycopg.OperationalError("private connection detail")):
                response = client.get("/health/ready")
                self.assertEqual(response.status_code, 503)
                self.assertNotIn("private", response.text)
            with patch("aep.execution.connect", side_effect=psycopg.OperationalError("private connection detail")):
                response = client.post("/runs", json=self.submission(), headers={"X-API-Key": "test-key"})
                self.assertEqual(response.status_code, 503)
                self.assertNotIn("private", response.text)

    def test_full_workflow_survives_service_recreation(self):
        run_id = self.start()
        self.assertEqual(self.engine.status(run_id)["state"], "RUNNING")
        for _ in range(4):
            run_worker(self.settings, once=True)
            self.engine = Execution(self.settings)
        status = self.engine.status(run_id)
        self.assertEqual(status["state"], "SUCCEEDED")
        self.assertEqual([task["state"] for task in status["tasks"]], ["SUCCEEDED"] * 4)
        metadata, raw = self.engine.artifact(run_id, status["final_artifact_id"])
        report = json.loads(raw)
        self.assertIn("Unresolved conflict", report["report_markdown"])
        self.assertEqual(report["review"]["conflicts"][0]["status"], "unresolved")
        for finding in report["findings"]:
            source = finding["sources"][0]["artifact"]
            source_metadata, _ = self.engine.artifact(run_id, source["artifact_id"])
            self.assertEqual(source_metadata["sha256"], source["sha256"])
        events = self.engine.events(run_id)
        self.assertEqual([e["sequence"] for e in events], list(range(1, len(events) + 1)))
        self.assertEqual(events[-1]["kind"], "run.succeeded")
        self.assertEqual(metadata["media_type"], "application/json")
        with self.assertRaises(NotFound):
            self.engine.artifact(run_id, status["final_artifact_id"], owner="foreign")

    def test_concurrent_claims_are_unique_and_dependencies_wait(self):
        run_id = self.start()
        with ThreadPoolExecutor(max_workers=4) as pool:
            claims = list(pool.map(self.engine.claim, ["worker-a", "worker-b", "worker-c", "worker-d"]))
        claims = [c for c in claims if c]
        while len(claims) < 2:
            claims.append(self.engine.claim("extra-worker"))
        self.assertEqual({c["task_id"] for c in claims}, {"research-a", "research-b"})
        self.assertIsNone(self.engine.claim("no-work"))
        self.engine.complete(claims[0], execute(claims[0]))
        self.assertIsNone(self.engine.claim("still-waiting"))
        self.engine.complete(claims[1], execute(claims[1]))
        self.assertEqual(self.engine.claim("reviewer")["task_id"], "review")
        self.assertEqual(self.engine.status(run_id)["state"], "RUNNING")

    def test_stale_lease_and_wrong_credentials_cannot_commit(self):
        run_id = self.start()
        old = self.engine.claim("old-worker")
        for changed in (dict(old, worker_id="wrong"), dict(old, fencing_token=99)):
            with self.assertRaises(LeaseLost):
                self.engine.heartbeat(changed)
        self.expire(old)
        with self.assertRaises(LeaseLost):
            self.engine.heartbeat(old)
        with self.assertRaises(LeaseLost):
            self.engine.complete(old, execute(old))
        self.engine.maintain()
        self.ready_retries()
        new = self.engine.claim("new-worker")
        self.assertEqual(new["task_id"], old["task_id"])
        self.assertGreater(new["fencing_token"], old["fencing_token"])
        self.engine.complete(new, execute(new))
        with self.assertRaises(LeaseLost):
            self.engine.complete(old, execute(old))
        attempts = self.engine.status(run_id)["attempts"]
        self.assertEqual([a["state"] for a in attempts], ["EXPIRED", "SUCCEEDED"])

    def test_heartbeat_preserves_work_beyond_original_lease(self):
        self.start()
        run_worker(replace(self.settings, lease_seconds=1, fake_delay_seconds=1.5), once=True)
        with connect(self.dsn) as conn:
            attempt = conn.execute("SELECT * FROM attempts").fetchone()
        self.assertEqual(attempt["state"], "SUCCEEDED")
        self.assertGreater((attempt["finished_at"] - attempt["started_at"]).total_seconds(), 1)

    def test_exhaustion_fails_run_and_cancels_downstream(self):
        run_id = self.start()
        for _ in range(3):
            claim = self.engine.claim("failing-worker")
            self.engine.fail(claim)
            self.ready_retries()
        status = self.engine.status(run_id)
        self.assertEqual(status["state"], "FAILED")
        self.assertEqual(next(t for t in status["tasks"] if t["id"] == "report")["state"], "CANCELLED")
        self.assertIsNone(self.engine.claim("late-worker"))

    def test_failure_drains_active_sibling_before_terminalizing_run(self):
        run_id = self.start()
        failing = self.engine.claim("failing-worker")
        sibling = self.engine.claim("sibling-worker")
        self.engine.fail(failing, reason="permanent_failure", retryable=False)
        self.assertEqual(self.engine.status(run_id)["state"], "RUNNING")
        self.assertIsNone(self.engine.claim("no-new-work"))
        with self.assertRaises(LeaseLost):
            self.engine.heartbeat(sibling)
        self.expire(sibling)
        self.engine.maintain()
        status = self.engine.status(run_id)
        self.assertEqual(status["state"], "FAILED")
        self.assertEqual([a["state"] for a in status["attempts"]], ["FAILED", "CANCELLED"])

    def test_deadline_prevents_late_success_and_drains_active_work(self):
        run_id = self.start()
        claim = self.engine.claim("worker")
        with connect(self.dsn) as conn:
            conn.execute("UPDATE runs SET deadline_at=clock_timestamp()-interval '1 second'")
        with self.assertRaises(LeaseLost):
            self.engine.complete(claim, execute(claim))
        self.engine.maintain()
        self.assertEqual(self.engine.status(run_id)["state"], "CANCELLING")
        self.expire(claim)
        self.engine.maintain()
        status = self.engine.status(run_id)
        self.assertEqual(status["state"], "CANCELLED")
        self.assertIsNone(status["final_artifact_id"])

    def test_retry_timing_is_durable(self):
        run_id = self.start()
        claim = self.engine.claim("worker")
        self.engine.fail(claim)
        before = next(t for t in self.engine.status(run_id)["tasks"] if t["id"] == claim["task_id"])
        after = next(t for t in Execution(self.settings).status(run_id)["tasks"] if t["id"] == claim["task_id"])
        self.assertEqual(before["state"], "RETRY_WAIT")
        self.assertEqual(before["next_run_at"], after["next_run_at"])
        self.assertEqual(after["attempt_count"], 1)

    def test_deadline_cancels_unplanned_run(self):
        run_id, _ = self.engine.submit(self.submission())
        with connect(self.dsn) as conn:
            conn.execute("UPDATE runs SET deadline_at=clock_timestamp()-interval '1 second'")
        self.engine.maintain()
        status = self.engine.status(run_id)
        self.assertEqual(status["state"], "CANCELLED")
        self.assertEqual(status["tasks"], [])

    def test_task_timeout_during_retry_fails_without_extra_attempt(self):
        run_id = self.start()
        claim = self.engine.claim("worker")
        self.engine.fail(claim)
        with connect(self.dsn) as conn:
            conn.execute("UPDATE tasks SET first_started_at=clock_timestamp()-interval '1 day' WHERE id=%s", (claim["task_id"],))
        self.engine.maintain()
        self.assertEqual(self.engine.status(run_id)["state"], "FAILED")
        self.assertEqual(len(self.engine.status(run_id)["attempts"]), 1)

    def test_completion_rolls_back_on_transaction_failure(self):
        run_id = self.start()
        claim = self.engine.claim("worker")
        with patch("aep.execution.event", side_effect=RuntimeError("injected transaction fault")):
            with self.assertRaises(RuntimeError):
                self.engine.complete(claim, execute(claim))
        status = self.engine.status(run_id)
        self.assertEqual(status["attempts"][0]["state"], "ACTIVE")
        with connect(self.dsn) as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM artifacts").fetchone()["n"], 0)
        self.engine.complete(claim, execute(claim))

    def test_api_auth_validation_idempotency_and_artifact_access(self):
        with TestClient(create_app(self.settings)) as client:
            self.assertEqual(client.get("/health/ready").status_code, 200)
            self.assertEqual(client.post("/runs", json=self.submission()).status_code, 401)
            headers = {"X-API-Key": "test-key"}
            self.assertEqual(client.post("/runs", json=[], headers=headers).status_code, 422)
            self.assertEqual(client.post("/runs", content=b"x" * 262145, headers=headers).status_code, 413)
            first = client.post("/runs", json=self.submission(), headers=headers)
            self.assertEqual(first.status_code, 201)
            self.assertEqual(client.post("/runs", json=self.submission(), headers=headers).status_code, 200)
            self.assertEqual(client.post("/runs", json=self.submission(request="changed"), headers=headers).status_code, 409)
            run_id = first.json()["run_id"]
            self.assertEqual(client.get(f"/runs/{run_id}", headers=headers).json()["state"], "SUBMITTED")
            self.assertEqual(client.get(f"/runs/{uuid4()}", headers=headers).status_code, 404)
            self.assertEqual(client.get("/runs/not-a-uuid", headers=headers).status_code, 422)
            for _ in range(4):
                run_worker(self.settings, once=True)
            final = client.get(f"/runs/{run_id}", headers=headers).json()["final_artifact_id"]
            self.assertEqual(client.get(f"/runs/{run_id}/artifacts/{final}").status_code, 401)
            self.assertEqual(client.get(f"/runs/{run_id}/artifacts/{final}", headers=headers).status_code, 200)
            foreign, _ = self.engine.submit(self.submission(idempotency_key="other-run"))
            self.assertEqual(client.get(f"/runs/{foreign}/artifacts/{final}", headers=headers).status_code, 404)
            first_page = client.get(f"/runs/{run_id}/events?limit=2", headers=headers).json()
            next_page = client.get(f"/runs/{run_id}/events?after={first_page['next_after']}&limit=2", headers=headers).json()
            self.assertEqual(next_page["events"][0]["sequence"], 3)

    def test_killed_worker_is_recovered_by_new_process(self):
        run_id, _ = self.engine.submit(self.submission())
        env = dict(os.environ, PYTHONPATH=str(ROOT / "src"), DATABASE_URL=self.dsn,
                   ARTIFACT_ROOT=str(self.settings.artifact_root), AEP_API_KEY="test-key",
                   AEP_LEASE_SECONDS="2", AEP_POLL_SECONDS="0.05", AEP_FAKE_DELAY_SECONDS="30")
        process = subprocess.Popen([sys.executable, "-m", "aep", "worker", "--once", "--worker-id", "kill-target"],
                                   env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                attempts = self.engine.status(run_id)["attempts"]
                if attempts:
                    break
                if process.poll() is not None:
                    self.fail("Worker exited before claiming work")
                time.sleep(0.05)
            else:
                self.fail("Worker did not claim before timeout")
            process.kill()
            process.wait(timeout=5)
            time.sleep(2.2)
            self.engine.maintain()
            self.ready_retries()
            env["AEP_FAKE_DELAY_SECONDS"] = "0"
            for _ in range(4):
                result = subprocess.run([sys.executable, "-m", "aep", "worker", "--once", "--worker-id", "replacement"],
                                        env=env, capture_output=True, text=True, timeout=20)
                self.assertEqual(result.returncode, 0, result.stderr)
            status = self.engine.status(run_id)
            self.assertEqual(status["state"], "SUCCEEDED")
            self.assertEqual(status["attempts"][0]["state"], "EXPIRED")
            self.assertEqual(status["attempts"][1]["number"], 2)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
