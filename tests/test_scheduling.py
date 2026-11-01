"""DAG, cancellation, admission, and multiple-process race checks."""

from concurrent.futures import ThreadPoolExecutor
import copy
from dataclasses import replace
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from fastapi.testclient import TestClient
from aep.api import create_app
from aep.db import connect
from aep.execution import Conflict, Execution, LeaseLost, NotFound
from aep.handlers import execute
from aep.planner import make_plan
from aep.scheduler import run_scheduler
from aep.worker import run_worker
from db_support import PostgresFixture


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL to run PostgreSQL integration tests")
class SchedulingTests(PostgresFixture, unittest.TestCase):
    def test_invalid_custom_graphs_are_rejected_before_persistence(self):
        mutations = {
            "cycle": lambda p: p["tasks"][0].update(depends_on=["report"]),
            "unknown dependency": lambda p: p["tasks"][0].update(depends_on=["missing"]),
            "duplicate ID": lambda p: p["tasks"].append(copy.deepcopy(p["tasks"][0])),
            "unknown handler": lambda p: p["tasks"][0].update(handler="untrusted-code"),
            "kind mismatch": lambda p: p["tasks"][0].update(handler="fake-writer-v1"),
            "unauthorized capability": lambda p: p["tasks"][0].update(capabilities=["network.any"]),
            "unreviewed research": lambda p: p["tasks"][2].update(depends_on=["research-a"]),
        }
        with TestClient(create_app(self.settings)) as client:
            for name, mutate in mutations.items():
                with self.subTest(name=name):
                    plan = make_plan()
                    mutate(plan)
                    response = client.post("/runs", json=self.submission(plan=plan), headers={"X-API-Key": "test-key"})
                    self.assertEqual(response.status_code, 422, response.text)
        with connect(self.dsn) as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM runs").fetchone()["n"], 0)

    def test_custom_diamond_and_transitive_inputs_have_durable_snapshots(self):
        plan = make_plan()
        seed = dict(copy.deepcopy(plan["tasks"][0]), task_id="seed", kind="utility", handler="fake-utility-v1", capabilities=[])
        gather = dict(copy.deepcopy(seed), task_id="gather", depends_on=["research-a", "research-b"])
        for research in plan["tasks"][:2]:
            research["depends_on"] = ["seed"]
        plan["tasks"][2].update(task_id="evidence-check", depends_on=["gather"])
        plan["tasks"][3].update(task_id="final-writer", depends_on=["evidence-check"])
        plan["tasks"].extend([seed, gather])
        run_id, _ = self.engine.submit(self.submission(plan=plan))
        original = copy.deepcopy(plan)
        for _ in range(6):
            run_worker(self.settings, once=True)
        status = self.engine.status(run_id)
        self.assertEqual(status["state"], "SUCCEEDED")
        self.assertEqual(plan, original)
        review_attempt = next(a for a in status["attempts"] if a["task_id"] == "evidence-check")
        inputs = review_attempt["input_snapshot"]["inputs"]
        self.assertEqual(set(inputs), {"seed", "research-a", "research-b", "gather"})
        report = next(t for t in status["tasks"] if t["id"] == "final-writer")["result"]
        self.assertEqual(len(report["findings"]), 2)
        self.assertEqual(report["review"]["conflicts"][0]["status"], "unresolved")

    def test_per_run_cap_cannot_be_oversubscribed_by_racing_claimers(self):
        run_id, _ = self.engine.submit(self.submission(max_concurrency=1))
        self.engine.maintain()
        barrier = threading.Barrier(8)
        def claim(index):
            barrier.wait()
            return self.engine.claim(f"worker-{index}")
        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = [result for result in pool.map(claim, range(8)) if result]
        self.assertEqual(len(claims), 1)
        self.assertIsNone(self.engine.claim("ninth-worker"))
        self.assertEqual(self.engine.status(run_id)["max_concurrency"], 1)
        self.engine.complete(claims[0], execute(claims[0]))
        self.assertEqual(self.engine.claim("next-worker")["task_id"], "research-b")

    def test_maximum_length_task_ids_produce_valid_distinct_findings(self):
        plan = make_plan()
        rename = {task["task_id"]: str(index) + "x" * 127 for index, task in enumerate(plan["tasks"])}
        for task in plan["tasks"]:
            task["task_id"] = rename[task["task_id"]]
            task["depends_on"] = [rename[key] for key in task["depends_on"]]
        run_id, _ = self.engine.submit(self.submission(plan=plan))
        for _ in range(4):
            run_worker(self.settings, once=True)
        status = self.engine.status(run_id)
        self.assertEqual(status["state"], "SUCCEEDED")
        findings = next(t for t in status["tasks"] if t["id"] == rename["report"])["result"]["findings"]
        self.assertEqual(len({finding["claim_id"] for finding in findings}), 2)

    def test_saturated_run_does_not_block_other_runs_and_claims_rotate(self):
        runs = [self.engine.submit(self.submission(idempotency_key=f"run-{i}", max_concurrency=1))[0] for i in range(3)]
        self.engine.maintain()
        claims = [self.engine.claim(f"worker-{i}") for i in range(3)]
        self.assertEqual([c["run_id"] for c in claims], runs)
        self.assertIsNone(self.engine.claim("all-capped"))
        self.engine.complete(claims[1], execute(claims[1]))
        self.assertEqual(self.engine.claim("freed-slot")["run_id"], runs[1])

    def test_deployment_cap_is_validated(self):
        for limit in (0, 33, self.settings.run_concurrency_limit + 1):
            with self.subTest(limit=limit):
                with self.assertRaises(ValueError):
                    self.engine.submit(self.submission(max_concurrency=limit))

    def test_parallel_scheduler_passes_admit_only_one_graph(self):
        run_id, _ = self.engine.submit(self.submission())
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: self.engine.maintain(), range(4)))
        status = self.engine.status(run_id)
        self.assertEqual(len(status["tasks"]), 4)
        self.assertEqual(sum(e["kind"] == "run.planning" for e in self.engine.events(run_id)), 1)

    def test_scheduler_times_out_running_task_even_with_a_live_lease(self):
        run_id = self.start()
        claim = self.engine.claim("busy-worker")
        with connect(self.dsn) as conn:
            conn.execute("UPDATE tasks SET first_started_at=clock_timestamp()-interval '1 day' WHERE id=%s", (claim["task_id"],))
        run_scheduler(self.settings, once=True)
        status = self.engine.status(run_id)
        self.assertEqual(status["state"], "FAILED")
        self.assertEqual(status["failure"], "task_timeout")
        self.assertEqual(status["attempts"][0]["state"], "FAILED")
        with self.assertRaises(LeaseLost):
            self.engine.complete(claim, execute(claim))

    def test_scheduler_times_out_ready_retry_without_waiting_for_a_worker(self):
        run_id = self.start()
        claim = self.engine.claim("worker")
        self.engine.fail(claim)
        self.ready_retries()
        with connect(self.dsn) as conn:
            conn.execute("UPDATE tasks SET first_started_at=clock_timestamp()-interval '1 day' WHERE id=%s", (claim["task_id"],))
        run_scheduler(self.settings, once=True)
        self.assertEqual(self.engine.status(run_id)["state"], "FAILED")
        self.assertEqual(len(self.engine.status(run_id)["attempts"]), 1)

    def test_cancel_before_planning_is_idempotent_and_owned(self):
        run_id, _ = self.engine.submit(self.submission())
        with self.assertRaises(NotFound):
            self.engine.cancel(run_id, owner="foreign")
        self.assertEqual(self.engine.cancel(run_id)["state"], "CANCELLED")
        events = self.engine.events(run_id)
        self.assertEqual(self.engine.cancel(run_id)["state"], "CANCELLED")
        self.assertEqual(self.engine.events(run_id), events)
        self.engine.maintain()
        status = self.engine.status(run_id)
        self.assertEqual(status["tasks"], [])
        self.assertEqual(status["cancel_requested_by"], "local")
        self.assertEqual(status["cancel_reason"], "user_requested")
        self.assertIsNotNone(status["cancel_requested_at"])

    def test_cancel_drains_active_attempts_and_blocks_late_completion(self):
        run_id = self.start()
        first = self.engine.claim("one")
        second = self.engine.claim("two")
        self.assertEqual(self.engine.cancel(run_id)["state"], "CANCELLING")
        self.assertEqual(self.engine.cancel(run_id)["state"], "CANCELLING")
        self.assertIsNone(self.engine.claim("three"))
        with self.assertRaises(LeaseLost):
            self.engine.complete(first, execute(first))
        self.assertFalse(self.engine.acknowledge_stop(dict(first, fencing_token=999)))
        self.assertTrue(self.engine.acknowledge_stop(first))
        self.assertEqual(self.engine.status(run_id)["state"], "CANCELLING")
        self.assertTrue(self.engine.acknowledge_stop(second))
        self.assertFalse(self.engine.acknowledge_stop(second))
        status = self.engine.status(run_id)
        self.assertEqual(status["state"], "CANCELLED")
        self.assertEqual({t["state"] for t in status["tasks"]}, {"CANCELLED"})
        self.assertEqual(sum(e["kind"] == "run.cancelling" for e in self.engine.events(run_id)), 1)

    def test_cancelled_retry_and_dead_worker_are_not_rescheduled(self):
        run_id = self.start()
        claim = self.engine.claim("worker")
        self.engine.fail(claim)
        self.engine.cancel(run_id)
        self.ready_retries()
        self.assertIsNone(self.engine.claim("late-worker"))
        self.assertEqual(self.engine.status(run_id)["state"], "CANCELLED")
        self.assertEqual(len(self.engine.status(run_id)["attempts"]), 1)

    def test_cancel_recovers_if_the_active_worker_never_acknowledges(self):
        run_id = self.start()
        claim = self.engine.claim("dead-worker")
        self.engine.cancel(run_id)
        self.expire(claim)
        run_scheduler(self.settings, once=True)
        status = self.engine.status(run_id)
        self.assertEqual(status["state"], "CANCELLED")
        self.assertEqual(status["attempts"][0]["state"], "CANCELLED")

    def test_worker_cooperatively_acknowledges_cancellation(self):
        run_id = self.start()
        settings = replace(self.settings, lease_seconds=2, fake_delay_seconds=30, worker_maintenance=False)
        stop = threading.Event()
        thread = threading.Thread(target=run_worker, args=(settings,), kwargs={"once": True, "stop": stop})
        thread.start()
        try:
            deadline = time.monotonic() + 10
            while not self.engine.status(run_id)["attempts"]:
                if time.monotonic() > deadline:
                    self.fail("Worker did not claim")
                time.sleep(0.02)
            self.engine.cancel(run_id)
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(self.engine.status(run_id)["state"], "CANCELLED")
            self.assertEqual(self.engine.status(run_id)["attempts"][0]["error"], "user_requested")
        finally:
            stop.set()
            thread.join(timeout=5)

    def test_completion_and_cancellation_race_has_one_terminal_outcome(self):
        run_id = self.start()
        for _ in range(3):
            claim = self.engine.claim("finisher")
            self.engine.complete(claim, execute(claim))
        writer = self.engine.claim("writer")
        barrier = threading.Barrier(2)
        def complete():
            barrier.wait()
            try:
                return self.engine.complete(writer, execute(writer))
            except LeaseLost:
                return None
        def cancel():
            barrier.wait()
            try:
                return self.engine.cancel(run_id)
            except Conflict:
                return None
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(complete), pool.submit(cancel)]
            for future in futures:
                future.result(timeout=10)
        self.engine.acknowledge_stop(writer)
        status = self.engine.status(run_id)
        self.assertIn(status["state"], {"SUCCEEDED", "CANCELLED"})
        terminal = [e for e in self.engine.events(run_id) if e["kind"] in {"run.succeeded", "run.cancelled"}]
        self.assertEqual(len(terminal), 1)
        self.assertEqual(bool(status["final_artifact_id"]), status["state"] == "SUCCEEDED")

    def test_cancel_does_not_override_a_committed_failure(self):
        run_id = self.start()
        failed = self.engine.claim("failed")
        sibling = self.engine.claim("sibling")
        self.engine.fail(failed, reason="permanent_failure", retryable=False)
        with self.assertRaises(Conflict):
            self.engine.cancel(run_id)
        self.engine.acknowledge_stop(sibling)
        self.assertEqual(self.engine.status(run_id)["state"], "FAILED")
        self.assertEqual(self.engine.status(run_id)["failure"], "permanent_failure")

    def test_cancel_api_auth_status_codes_and_terminal_conflict(self):
        run_id = self.start()
        claim = self.engine.claim("active")
        with TestClient(create_app(self.settings)) as client:
            path = f"/runs/{run_id}/cancel"
            headers = {"X-API-Key": "test-key"}
            self.assertEqual(client.post(path).status_code, 401)
            self.assertEqual(client.post(path, headers=headers).status_code, 202)
            self.assertEqual(client.post(path, headers=headers).status_code, 202)
            self.engine.acknowledge_stop(claim)
            self.assertEqual(client.post(path, headers=headers).status_code, 200)
            self.assertEqual(client.post(f"/runs/{uuid4()}/cancel", headers=headers).status_code, 404)
            self.assertEqual(client.post("/runs/invalid/cancel", headers=headers).status_code, 422)
            other, _ = self.engine.submit(self.submission(idempotency_key="successful"))
            for _ in range(4):
                run_worker(self.settings, once=True)
            self.assertEqual(client.post(f"/runs/{other}/cancel", headers=headers).status_code, 409)

    def test_multiple_worker_processes_overlap_roots_but_wait_at_fan_in(self):
        run_id = self.start()
        env = dict(os.environ, PYTHONPATH=str(ROOT / "src"), DATABASE_URL=self.dsn,
                   ARTIFACT_ROOT=str(self.settings.artifact_root), AEP_API_KEY="test-key",
                   AEP_LEASE_SECONDS="3", AEP_POLL_SECONDS="0.05", AEP_FAKE_DELAY_SECONDS="2",
                   AEP_WORKER_MAINTENANCE="false")
        workers = [subprocess.Popen([sys.executable, "-m", "aep", "worker", "--worker-id", f"parallel-{i}"],
                                   env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) for i in range(2)]
        peak = 0
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                status = self.engine.status(run_id)
                peak = max(peak, sum(t["state"] == "RUNNING" for t in status["tasks"]))
                if status["state"] in {"SUCCEEDED", "FAILED"}:
                    break
                self.assertTrue(all(w.poll() is None for w in workers))
                time.sleep(0.05)
            self.assertEqual(status["state"], "SUCCEEDED")
            self.assertEqual(peak, 2)
            attempts = {a["task_id"]: a for a in status["attempts"]}
            self.assertEqual(len(attempts), 4)
            roots = [attempts[key] for key in ("research-a", "research-b")]
            self.assertNotEqual(roots[0]["worker_id"], roots[1]["worker_id"])
            self.assertLess(max(a["started_at"] for a in roots), min(a["finished_at"] for a in roots))
            self.assertGreaterEqual(attempts["review"]["started_at"], max(a["finished_at"] for a in roots))
            self.assertGreaterEqual(attempts["report"]["started_at"], attempts["review"]["finished_at"])
        finally:
            for worker in workers:
                if worker.poll() is None:
                    worker.terminate()
                worker.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
