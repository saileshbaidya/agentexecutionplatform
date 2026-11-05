"""Versioned dataset admission, isolated fault injection, and comparisons."""

from dataclasses import replace
import copy
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb
from aep.agents import demo
from aep.api import create_app
from aep.context import digest
from aep.db import connect
from aep.evaluation import Evaluations, load_dataset, validate_dataset
from aep.execution import Conflict, Execution, NotFound
from aep.retention import Retention
from db_support import PostgresFixture


def dataset(one=False):
    value = load_dataset(ROOT / "evaluations/evidence-v1.json")
    if one:
        value["cases"] = value["cases"][:1]
    return value


class DatasetTests(unittest.TestCase):
    def test_versioned_fixture_and_strict_dataset_schema(self):
        value = dataset()
        self.assertEqual(value["fixture_digest"], digest(demo()))
        validate_dataset(value)
        for change in [lambda d: d.update(fixture_digest="0" * 64),
                       lambda d: d["cases"].append(d["cases"][0]),
                       lambda d: d["cases"][0].update(fault="arbitrary-code"),
                       lambda d: d["cases"][0].update(budget_micro_usd=1.0),
                       lambda d: d.update(secret="not-allowed")]:
            bad = copy.deepcopy(value)
            change(bad)
            with self.assertRaises(ValueError):
                validate_dataset(bad)


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL")
class EvaluationTests(PostgresFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.evaluator = Evaluations(self.settings)

    def run_batch(self, value=None, label="baseline"):
        batch_id = self.evaluator.start(value or dataset(one=True), uuid4().hex, label)
        return self.evaluator.run(batch_id)

    def test_all_recorded_recovery_cases_pass_without_external_calls(self):
        with patch("aep.model_provider.make_client", side_effect=AssertionError("External calls forbidden")):
            report = self.run_batch(dataset())
        self.assertTrue(report["passed"], [(c["case_id"], c["metrics"]) for c in report["cases"]])
        cases = {c["case_id"]: c for c in report["cases"]}
        self.assertEqual(len(cases), 6)
        recovered = cases["receipt-before-worker-crash"]["metrics"]
        self.assertEqual(recovered["operations"], 5)
        self.assertEqual(recovered["attempts"], 6)
        self.assertEqual(recovered["cost_micro_usd"], cases["contradictory-evidence"]["metrics"]["cost_micro_usd"])
        self.assertEqual(cases["tampered-context"]["metrics"]["failure"], "context_integrity_error")
        self.assertEqual(cases["tampered-context"]["metrics"]["operations"], 0)
        self.assertEqual(cases["cancelled-worker"]["metrics"]["state"], "CANCELLED")
        for case in report["cases"]:
            self.assertTrue(case["metrics"]["checks"]["fault_observed"])

    def test_completed_evaluation_resumes_without_new_runs_or_spend(self):
        value = dataset(one=True)
        batch_id = self.evaluator.start(value, "stable", "baseline")
        first = self.evaluator.run(batch_id)
        self.assertEqual(batch_id, self.evaluator.start(value, "stable", "baseline"))
        with patch("aep.evaluation.run_worker", side_effect=AssertionError("No repeat execution")):
            second = self.evaluator.run(batch_id)
        self.assertEqual(first["cases"], second["cases"])
        self.assertEqual(first["total_cost_micro_usd"], second["total_cost_micro_usd"])

    def test_changed_dataset_configuration_or_runtime_rejects_resume(self):
        value = dataset(one=True)
        batch_id = self.evaluator.start(value, "stable", "baseline")
        changed = copy.deepcopy(value)
        changed["version"] = "2"
        with self.assertRaises(Conflict):
            self.evaluator.start(changed, "stable", "baseline")
        with self.assertRaises(Conflict):
            self.evaluator.start(value, "stable", "different-label")
        with patch("aep.evaluation.runtime_fingerprint", return_value="0" * 64):
            with self.assertRaises(Conflict):
                self.evaluator.run(batch_id)
        with self.assertRaises(ValueError):
            self.evaluator.start(value, "live", "paid", live=True)

    def test_worker_pool_isolation_and_public_admission(self):
        fixture = demo()
        private = Execution(replace(self.settings, worker_pool="eval-private"))
        run_id, _ = private.submit(self.submission(request=fixture["request"], documents=fixture["documents"],
                                                 agent={"backend": "recorded"}, budget_micro_usd=1000000))
        private.maintain()
        self.assertIsNone(self.engine.claim("ordinary-worker"))
        self.assertEqual(private.claim("private-worker")["run_id"], run_id)
        with self.assertRaises(ValueError):
            self.engine.submit(self.submission(execution_pool="eval-private"))
        with self.assertRaises(ValueError):
            self.engine.submit(self.submission(fault="context_corruption"))

    def test_compare_same_dataset_and_reject_incompatible_or_unfinished_batches(self):
        baseline = self.run_batch()
        candidate = self.run_batch(label="candidate")
        comparison = self.evaluator.compare(baseline["id"], candidate["id"])
        self.assertEqual(comparison["regressions"], 0)
        self.assertEqual(comparison["cases"][0]["cost_micro_usd_delta"], 0)
        with connect(self.dsn) as conn:
            metrics = candidate["cases"][0]["metrics"]
            metrics["passed"] = False
            conn.execute("UPDATE evaluation_cases SET metrics=%s WHERE batch_id=%s", (Jsonb(metrics), candidate["id"]))
        self.assertEqual(self.evaluator.compare(baseline["id"], candidate["id"])["regressions"], 1)
        pending = self.evaluator.start(dataset(one=True), "pending", "pending")
        with self.assertRaises(Conflict):
            self.evaluator.compare(baseline["id"], pending)
        changed = dataset(one=True)
        changed["version"] = "2"
        other = self.run_batch(changed)
        with self.assertRaises(Conflict):
            self.evaluator.compare(baseline["id"], other["id"])

    def test_expected_quality_failure_is_reported_instead_of_marked_passed(self):
        value = dataset(one=True)
        value["cases"][0]["expected_statements"] = ["An unsupported invented statement"]
        report = self.run_batch(value)
        self.assertFalse(report["passed"])
        self.assertFalse(report["cases"][0]["metrics"]["checks"]["expected_statements"])

    def test_report_owner_scope_auth_and_retention_interlock(self):
        report = self.run_batch()
        with self.assertRaises(NotFound):
            self.evaluator.report(report["id"], owner="foreign")
        with TestClient(create_app(self.settings)) as client:
            path = f"/evaluations/{report['id']}"
            self.assertEqual(client.get(path).status_code, 401)
            self.assertTrue(client.get(path, headers={"X-API-Key": "test-key"}).json()["passed"])
        run_id = report["cases"][0]["run_id"]
        with connect(self.dsn) as conn:
            conn.execute("UPDATE evaluation_batches SET state='RUNNING' WHERE id=%s", (report["id"],))
        with self.assertRaises(Conflict):
            Retention(self.engine).purge(run_id)
        with connect(self.dsn) as conn:
            conn.execute("UPDATE evaluation_batches SET state='SUCCEEDED' WHERE id=%s", (report["id"],))
        Retention(self.engine).purge(run_id)
        self.assertEqual(self.evaluator.report(report["id"])["cases"][0]["metrics"], report["cases"][0]["metrics"])


if __name__ == "__main__":
    unittest.main()
