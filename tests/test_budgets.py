"""Real PostgreSQL accounting races and fake-provider recovery checks."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import psycopg
from psycopg.types.json import Jsonb
from fastapi.testclient import TestClient
from aep.api import create_app
from aep.budgets import BudgetDenied, Budgets, OperationPending, ProviderFailure
from aep.db import connect
from aep.execution import Conflict, Execution, LeaseLost
from aep.handlers import execute
from aep.planner import make_plan
from aep.worker import run_worker
from db_support import PostgresFixture


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL to run PostgreSQL integration tests")
class BudgetTests(PostgresFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.budgets = Budgets(self.engine)

    @staticmethod
    def spec(**changes):
        value = {"call_key": "primary", "provider": "fake-v1", "input": "abcdefghij", "max_input_tokens": 10,
                 "max_output_tokens": 8, "output_tokens": 4, "max_cost_micro_usd": 60}
        value.update(changes)
        return value

    def start_budget(self, amount=100, spec=None, both=False):
        plan = make_plan()
        if spec:
            plan["tasks"][0]["paid_call"] = spec
            if both:
                plan["tasks"][1]["paid_call"] = spec
        run_id, _ = self.engine.submit(self.submission(budget_micro_usd=amount, plan=plan))
        self.engine.maintain()
        return run_id

    def accepted(self, claim, spec=None):
        operation = self.budgets.reserve(claim, spec or self.spec())
        token = self.budgets.dispatch(claim, operation["id"])
        receipt = self.budgets.provider.execute(operation["id"], token, claim)
        return operation, receipt

    def ready_evidence(self, operation_id):
        with connect(self.dsn) as conn:
            conn.execute("UPDATE fake_provider_receipts SET available_at=clock_timestamp()-interval '1 second' WHERE operation_id=%s", (operation_id,))

    def assert_account(self, run_id, settled, reserved):
        summary = self.budgets.summary(run_id)
        self.assertEqual(summary["settled_micro_usd"], settled)
        self.assertEqual(summary["reserved_micro_usd"], reserved)
        with connect(self.dsn) as conn:
            ledger = conn.execute("SELECT COALESCE(sum(amount_micro_usd),0) AS n FROM usage_ledger WHERE run_id=%s", (run_id,)).fetchone()["n"]
            holds = conn.execute("SELECT COALESCE(sum(amount_micro_usd),0) AS n FROM budget_reservations WHERE run_id=%s AND state='HELD'", (run_id,)).fetchone()["n"]
        self.assertEqual(ledger, settled)
        self.assertEqual(holds, reserved)
        return summary

    def test_racing_reservations_cannot_oversubscribe_shared_budget(self):
        run_id = self.start_budget()
        claims = [self.engine.claim("one"), self.engine.claim("two")]
        barrier = threading.Barrier(2)
        def reserve(claim):
            barrier.wait()
            try:
                return self.budgets.reserve(claim, self.spec())
            except BudgetDenied as denied:
                return denied
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(reserve, claims))
        denied = [value for value in results if isinstance(value, BudgetDenied)]
        self.assertEqual(len(denied), 1)
        self.assertEqual(denied[0].details, {"code": "budget_exhausted", "requested_micro_usd": 60, "available_micro_usd": 40})
        self.assert_account(run_id, 0, 60)
        self.assertEqual(len(self.budgets.history(run_id)), 1)

    def test_reservation_replay_and_changed_call_key_payload(self):
        run_id = self.start_budget()
        claim = self.engine.claim("worker")
        first = self.budgets.reserve(claim, self.spec())
        second = self.budgets.reserve(claim, self.spec())
        self.assertEqual(first["id"], second["id"])
        with self.assertRaises(Conflict):
            self.budgets.reserve(claim, self.spec(input="different!"))
        self.assert_account(run_id, 0, 60)

    def test_unused_reservation_becomes_available_to_another_agent(self):
        run_id = self.start_budget()
        first, second = self.engine.claim("one"), self.engine.claim("two")
        self.budgets.call(first, self.spec())
        operation = self.budgets.reserve(second, self.spec())
        self.assert_account(run_id, 40, 60)
        self.budgets.call(second, self.spec())
        self.assert_account(run_id, 80, 0)
        self.assertEqual(self.budgets.history(run_id)[1]["id"], operation["id"])

    def test_reconciler_and_worker_settlement_race_charges_once(self):
        run_id = self.start_budget()
        operation, receipt = self.accepted(self.engine.claim("worker"))
        barrier = threading.Barrier(2)
        def settle(_):
            barrier.wait()
            return self.budgets.settle(operation["id"], receipt)
        def reconcile(_):
            barrier.wait()
            return self.budgets.reconcile_operation(operation["id"])
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(settle, 0), pool.submit(reconcile, 0)]
            for future in futures:
                self.assertEqual(future.result(timeout=10)["state"], "SUCCEEDED")
        self.assert_account(run_id, 40, 0)
        self.assertEqual(len(self.budgets.history(run_id, kind="ledger")), 1)

    def test_per_call_limits_and_strict_provider_allowlist(self):
        for spec in (self.spec(max_input_tokens=9), self.spec(output_tokens=9), self.spec(max_cost_micro_usd=59),
                     self.spec(provider="unbounded-provider"), self.spec(output_tokens=True),
                     self.spec(input="é", max_input_tokens=1)):
            with self.subTest(spec=spec):
                with self.assertRaises(ValueError):
                    self.start_budget(spec=spec)
        with connect(self.dsn) as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM runs").fetchone()["n"], 0)

    def test_provider_acceptance_and_settlement_are_idempotent(self):
        run_id = self.start_budget()
        claim = self.engine.claim("worker")
        operation = self.budgets.reserve(claim, self.spec())
        token = self.budgets.dispatch(claim, operation["id"])
        with self.assertRaises(Conflict):
            self.budgets.dispatch(claim, operation["id"])
        with ThreadPoolExecutor(max_workers=2) as pool:
            receipts = list(pool.map(lambda _: self.budgets.provider.execute(operation["id"], token, claim), range(2)))
        self.assertEqual(receipts[0], receipts[1])
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: self.budgets.settle(operation["id"], receipts[0]), range(4)))
        self.assertEqual(self.assert_account(run_id, 40, 0)["available_micro_usd"], 60)
        self.assertEqual(len(self.budgets.history(run_id, kind="ledger")), 1)
        replay = self.budgets.call(claim, self.spec())
        self.assertEqual(replay["operation_id"], str(operation["id"]))
        self.assert_account(run_id, 40, 0)

    def test_forged_or_changed_usage_is_not_accepted(self):
        run_id = self.start_budget()
        claim = self.engine.claim("worker")
        operation, receipt = self.accepted(claim)
        with self.assertRaises(Conflict):
            self.budgets.settle(operation["id"], dict(receipt, cost_micro_usd=0))
        self.assert_account(run_id, 0, 60)
        self.budgets.settle(operation["id"], receipt)
        with self.assertRaises(Conflict):
            self.budgets.settle(operation["id"], dict(receipt, output_tokens=3))
        self.assert_account(run_id, 40, 0)

    def test_settlement_transaction_failure_preserves_reservation(self):
        run_id = self.start_budget()
        operation, receipt = self.accepted(self.engine.claim("worker"))
        with patch("aep.budgets.event", side_effect=RuntimeError("injected accounting crash")):
            with self.assertRaises(RuntimeError):
                self.budgets.settle(operation["id"], receipt)
        self.assert_account(run_id, 0, 60)
        self.budgets.settle(operation["id"], receipt)
        self.assert_account(run_id, 40, 0)

    def test_live_dispatcher_is_not_revoked_by_receipt_polling(self):
        run_id = self.start_budget()
        claim = self.engine.claim("worker")
        operation = self.budgets.reserve(claim, self.spec())
        token = self.budgets.dispatch(claim, operation["id"])
        self.assertEqual(self.budgets.reconcile_operation(operation["id"])["state"], "DISPATCHED")
        receipt = self.budgets.provider.execute(operation["id"], token, claim)
        self.budgets.settle(operation["id"], receipt)
        self.assert_account(run_id, 40, 0)

    def test_reserved_call_survives_worker_loss_without_double_reserving(self):
        run_id = self.start_budget()
        old = self.engine.claim("old")
        operation = self.budgets.reserve(old, self.spec())
        self.expire(old)
        self.engine.maintain()
        self.assert_account(run_id, 0, 60)
        self.ready_retries()
        new = self.engine.claim("replacement")
        result = self.budgets.call(new, self.spec())
        self.assertEqual(result["operation_id"], str(operation["id"]))
        self.assert_account(run_id, 40, 0)
        with self.assertRaises(LeaseLost):
            self.budgets.dispatch(old, operation["id"])

    def test_undispatched_reservation_releases_after_cancellation_once(self):
        run_id = self.start_budget()
        claim = self.engine.claim("worker")
        operation = self.budgets.reserve(claim, self.spec())
        self.engine.cancel(run_id)
        self.budgets.reconcile_operation(operation["id"])
        self.budgets.reconcile_operation(operation["id"])
        self.assert_account(run_id, 0, 0)
        self.assertEqual(self.budgets.history(run_id)[0]["state"], "CANCELLED")
        with self.assertRaises(LeaseLost):
            self.budgets.dispatch(claim, operation["id"])

    def test_dispatch_without_acceptance_requires_proof_before_release(self):
        run_id = self.start_budget()
        old = self.engine.claim("old")
        operation = self.budgets.reserve(old, self.spec())
        token = self.budgets.dispatch(old, operation["id"])
        self.budgets.mark_unknown(operation["id"])
        self.budgets.reconcile_operation(operation["id"])
        self.assert_account(run_id, 0, 60)
        self.expire(old)
        self.engine.maintain()
        self.assert_account(run_id, 0, 0)
        self.assertEqual(self.budgets.history(run_id)[0]["error"], "provider_not_accepted")
        with self.assertRaises(LeaseLost):
            self.budgets.provider.execute(operation["id"], token, old)

    def test_unknown_charge_parks_then_resumes_from_cached_result(self):
        spec = self.spec(mode="timeout_after_accept", reconcile_delay_seconds=3600)
        run_id = self.start_budget(spec=spec)
        claim = self.engine.claim("worker")
        with self.assertRaises(OperationPending) as pending:
            self.budgets.call(claim, spec)
        operation_id = pending.exception.operation_id
        self.engine.wait_for_operation(claim, operation_id)
        self.engine.maintain()
        self.assertEqual(self.engine.status(run_id)["attempts"][0]["state"], "RECONCILIATION")
        self.assertEqual(len(self.engine.status(run_id)["attempts"]), 1)
        self.assert_account(run_id, 0, 60)
        self.ready_evidence(operation_id)
        self.engine.maintain()
        self.ready_retries()
        replacement = self.engine.claim("replacement")
        result = self.budgets.call(replacement, spec)
        self.assertEqual(result["operation_id"], operation_id)
        self.engine.complete(replacement, execute(replacement))
        self.assert_account(run_id, 40, 0)
        with connect(self.dsn) as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM fake_provider_receipts").fetchone()["n"], 1)

    def test_cancellation_preserves_unknown_liabilities_and_reconciles_terminal_run(self):
        spec = self.spec(mode="timeout_after_accept", reconcile_delay_seconds=3600)
        run_id = self.start_budget(spec=spec)
        claim = self.engine.claim("worker")
        with self.assertRaises(OperationPending) as pending:
            self.budgets.call(claim, spec)
        operation_id = pending.exception.operation_id
        self.engine.wait_for_operation(claim, operation_id)
        self.assertEqual(self.engine.cancel(run_id)["state"], "CANCELLED")
        self.engine.maintain()
        self.assert_account(run_id, 0, 60)
        self.ready_evidence(operation_id)
        self.engine.maintain()
        self.assert_account(run_id, 40, 0)
        self.assertEqual(self.engine.status(run_id)["state"], "CANCELLED")
        self.assertIsNone(self.engine.status(run_id)["final_artifact_id"])

    def test_task_timeout_does_not_erase_an_uncertain_charge(self):
        spec = self.spec(mode="timeout_after_accept", reconcile_delay_seconds=3600)
        run_id = self.start_budget(spec=spec)
        claim = self.engine.claim("worker")
        with self.assertRaises(OperationPending) as pending:
            self.budgets.call(claim, spec)
        self.engine.wait_for_operation(claim, pending.exception.operation_id)
        with connect(self.dsn) as conn:
            conn.execute("UPDATE tasks SET first_started_at=clock_timestamp()-interval '1 day' WHERE id=%s", (claim["task_id"],))
        self.engine.maintain()
        self.assertEqual(self.engine.status(run_id)["state"], "FAILED")
        self.assert_account(run_id, 0, 60)

    def test_known_provider_failure_is_charged_and_not_automatically_repeated(self):
        spec = self.spec(mode="known_failure")
        run_id = self.start_budget(spec=spec)
        claim = self.engine.claim("worker")
        for _ in range(2):
            with self.assertRaises(ProviderFailure):
                self.budgets.call(claim, spec)
        self.assert_account(run_id, 20, 0)
        self.assertEqual(len(self.budgets.history(run_id)), 1)

    def test_timeout_before_acceptance_reconciles_to_definitive_zero_charge(self):
        spec = self.spec(mode="timeout_before_accept")
        run_id = self.start_budget(spec=spec)
        claim = self.engine.claim("worker")
        with self.assertRaises(OperationPending) as pending:
            self.budgets.call(claim, spec)
        self.assert_account(run_id, 0, 60)
        self.engine.wait_for_operation(claim, pending.exception.operation_id)
        self.engine.maintain()
        self.assert_account(run_id, 0, 0)
        self.ready_retries()
        replacement = self.engine.claim("replacement")
        with self.assertRaises(ProviderFailure):
            self.budgets.call(replacement, spec)
        self.assertEqual(self.budgets.history(run_id)[0]["result"]["outcome"], "NOT_ACCEPTED")

    def test_worker_reports_budget_exhaustion_without_dispatch(self):
        run_id = self.start_budget(amount=59, spec=self.spec())
        run_worker(self.settings, once=True)
        self.assertEqual(self.engine.status(run_id)["failure"], "budget_exhausted")
        self.assert_account(run_id, 0, 0)
        self.assertEqual(self.budgets.history(run_id), [])

    def test_paid_workflow_and_free_workflow_remain_compatible(self):
        run_id = self.start_budget(amount=120, spec=self.spec(), both=True)
        for _ in range(4):
            run_worker(self.settings, once=True)
        self.assertEqual(self.engine.status(run_id)["state"], "SUCCEEDED")
        self.assert_account(run_id, 80, 0)
        self.assertEqual(len(self.budgets.history(run_id)), 2)

    def test_unsettled_operation_blocks_task_completion(self):
        self.start_budget()
        claim = self.engine.claim("worker")
        self.budgets.reserve(claim, self.spec())
        with self.assertRaises(Conflict):
            self.engine.complete(claim, execute(claim))

    def test_adjustment_is_audited_idempotent_and_cannot_over_refund(self):
        run_id = self.start_budget()
        operation, receipt = self.accepted(self.engine.claim("worker"))
        self.budgets.settle(operation["id"], receipt)
        credit = {"adjustment_key": "refund-1", "delta_micro_usd": -10, "reason": "Verified provider credit"}
        first = self.budgets.adjust(run_id, operation["id"], credit)
        second = self.budgets.adjust(run_id, operation["id"], credit)
        self.assertEqual(first["sequence"], second["sequence"])
        self.assert_account(run_id, 30, 0)
        with self.assertRaises(Conflict):
            self.budgets.adjust(run_id, operation["id"], dict(credit, delta_micro_usd=-11))
        with self.assertRaises(ValueError):
            self.budgets.adjust(run_id, operation["id"], dict(credit, adjustment_key="over-refund", delta_micro_usd=-31))
        for statement in ("UPDATE usage_ledger SET reason='changed'", "DELETE FROM usage_ledger", "TRUNCATE usage_ledger"):
            with self.subTest(statement=statement):
                with self.assertRaises(psycopg.errors.RaiseException):
                    with connect(self.dsn) as conn:
                        conn.execute(statement)

    def test_unexpected_additional_charge_freezes_new_admission(self):
        run_id = self.start_budget()
        operation, receipt = self.accepted(self.engine.claim("worker"))
        self.budgets.settle(operation["id"], receipt)
        other = self.engine.claim("other")
        self.budgets.adjust(run_id, operation["id"], {"adjustment_key": "correction", "delta_micro_usd": 100, "reason": "Provider correction"})
        self.assertTrue(self.assert_account(run_id, 140, 0)["breached"])
        with self.assertRaises(BudgetDenied) as denied:
            self.budgets.reserve(other, self.spec())
        self.assertEqual(denied.exception.code, "budget_frozen")

    def test_provider_bound_breach_is_recorded_without_hiding_actual_usage(self):
        run_id = self.start_budget()
        operation, receipt = self.accepted(self.engine.claim("worker"))
        bad = dict(receipt, output_tokens=9, cost_micro_usd=65)
        with connect(self.dsn) as conn:
            conn.execute("UPDATE fake_provider_receipts SET receipt=%s WHERE operation_id=%s", (Jsonb(bad), operation["id"]))
        result = self.budgets.settle(operation["id"], bad)
        self.assertEqual(result["error"], "provider_bound_breached")
        self.assertTrue(self.assert_account(run_id, 65, 0)["breached"])
        self.engine.maintain()
        self.assertEqual(self.engine.status(run_id)["failure"], "provider_bound_breached")

    def test_frozen_account_also_blocks_an_undispatched_existing_reservation(self):
        run_id = self.start_budget()
        first, second = self.engine.claim("one"), self.engine.claim("two")
        result = self.budgets.call(first, self.spec())
        pending = self.budgets.reserve(second, self.spec())
        self.budgets.adjust(run_id, result["operation_id"], {"adjustment_key": "late-bill", "delta_micro_usd": 10, "reason": "Late provider charge"})
        with self.assertRaises(BudgetDenied):
            self.budgets.dispatch(second, pending["id"])
        self.assert_account(run_id, 50, 60)

    def test_budget_api_auth_pagination_reconciliation_and_adjustments(self):
        run_id = self.start_budget()
        operation, receipt = self.accepted(self.engine.claim("worker"))
        self.budgets.settle(operation["id"], receipt)
        headers = {"X-API-Key": "test-key"}
        with TestClient(create_app(self.settings)) as client:
            path = f"/runs/{run_id}"
            self.assertEqual(client.get(path + "/budget").status_code, 401)
            self.assertEqual(client.get(path + "/budget", headers=headers).json()["settled_micro_usd"], 40)
            ops = client.get(path + "/operations?limit=1", headers=headers).json()
            self.assertEqual(len(ops["operations"]), 1)
            self.assertEqual(client.get(path + f"/operations?after={ops['next_after']}", headers=headers).json()["operations"], [])
            self.assertEqual(client.get(path + "/budget/ledger", headers=headers).json()["entries"][0]["amount_micro_usd"], 40)
            reconcile = path + f"/operations/{operation['id']}/reconcile"
            self.assertEqual(client.post(reconcile).status_code, 401)
            self.assertEqual(client.post(reconcile, headers=headers).status_code, 200)
            adjustment = path + f"/operations/{operation['id']}/adjustments"
            credit = {"adjustment_key": "credit", "delta_micro_usd": -5, "reason": "Verified credit"}
            self.assertEqual(client.post(adjustment, json=credit, headers=headers).status_code, 200)
            self.assertEqual(client.post(adjustment, json={}, headers=headers).status_code, 422)
            self.assertEqual(client.post(adjustment, content="broken", headers=headers).status_code, 422)
            self.assertEqual(client.get(f"/runs/{uuid4()}/budget", headers=headers).status_code, 404)
            foreign, _ = self.engine.submit(self.submission(idempotency_key="other"))
            self.assertEqual(client.post(f"/runs/{foreign}/operations/{operation['id']}/reconcile", headers=headers).status_code, 404)
        self.assert_account(run_id, 35, 0)

    def test_killed_process_after_provider_acceptance_does_not_repeat_charge(self):
        self.settings = replace(self.settings, lease_seconds=15)
        self.engine, self.budgets = Execution(self.settings), Budgets(Execution(self.settings))
        run_id = self.start_budget()
        claim = self.engine.claim("kill-target")
        input_path = Path(self.temporary.name) / "claim.json"
        marker = Path(self.temporary.name) / "accepted.json"
        input_path.write_text(json.dumps({"claim": claim, "spec": self.spec()}), encoding="utf-8")
        code = """
import json, os, time
from pathlib import Path
from aep.config import Settings
from aep.execution import Execution
from aep.budgets import Budgets
data = json.loads(Path(os.environ['CLAIM_PATH']).read_text(encoding='utf-8'))
budget = Budgets(Execution(Settings.from_env()))
operation = budget.reserve(data['claim'], data['spec'])
token = budget.dispatch(data['claim'], operation['id'])
receipt = budget.provider.execute(operation['id'], token, data['claim'])
marker = Path(os.environ['MARKER_PATH'])
temporary = marker.with_suffix('.tmp')
temporary.write_text(json.dumps({'id': str(operation['id'])}), encoding='utf-8')
temporary.replace(marker)
time.sleep(60)
"""
        env = dict(os.environ, PYTHONPATH=str(ROOT / "src"), DATABASE_URL=self.dsn,
                   ARTIFACT_ROOT=str(self.settings.artifact_root), AEP_API_KEY="test-key", AEP_LEASE_SECONDS="15",
                   CLAIM_PATH=str(input_path), MARKER_PATH=str(marker))
        process = subprocess.Popen([sys.executable, "-c", code], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 15
            while not marker.exists():
                if process.poll() is not None or time.monotonic() >= deadline:
                    self.fail("Child did not reach provider acceptance")
                time.sleep(0.05)
            process.kill()
            process.wait(timeout=5)
            self.assert_account(run_id, 0, 60)
            self.expire(claim)
            self.engine.maintain()
            self.ready_retries()
            replacement = self.engine.claim("replacement")
            result = self.budgets.call(replacement, self.spec())
            self.assertEqual(result["operation_id"], json.loads(marker.read_text())["id"])
            self.assert_account(run_id, 40, 0)
            self.assertEqual(len(self.budgets.history(run_id, kind="ledger")), 1)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
