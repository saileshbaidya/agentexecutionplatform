"""Context bounds, provenance replay, and restartable payload retention."""

from dataclasses import replace
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb
from aep.agents import AgentOutputError, demo, execute_agent, normalize, PROMPTS
from aep.api import create_app
from aep.budgets import Budgets
from aep.context import Contexts, digest, select_context
from aep.db import connect
from aep.execution import Conflict, Execution, Gone, NotFound
from aep.retention import Retention
from aep.worker import run_worker
from db_support import PostgresFixture


def raw_context(text="evidence", findings=None):
    return {"request": "Compare", "task_id": "research-1", "findings": findings or [],
            "documents": [{"document_id": "source", "title": "Source", "text": text,
                           "artifact": {"artifact_id": "source-id", "sha256": hashlib.sha256(text.encode()).hexdigest()}}]}


class SelectionTests(unittest.TestCase):
    def test_context_is_bounded_deterministic_and_preserves_original_ranges(self):
        raw = raw_context("évidence " * 4000)
        original = copy.deepcopy(raw)
        context, manifest = select_context(raw, "research", 2048)
        self.assertEqual(raw, original)
        self.assertLessEqual(manifest["selected_bytes"], 2048)
        source = manifest["sources"][0]
        start, end = source["selected_range"]
        self.assertEqual(context["documents"][0]["text"], raw["documents"][0]["text"][start:end])
        self.assertEqual(source["artifact"], raw["documents"][0]["artifact"])
        self.assertEqual(source["omitted_ranges"], [[end, len(raw["documents"][0]["text"])]])
        self.assertEqual((context, manifest), select_context(raw, "research", 2048))

    def test_whole_findings_are_selected_and_omissions_are_recorded(self):
        findings = [{"claim_id": f"claim-{n}", "statement": "x" * 1000, "sources": []} for n in range(5)]
        context, manifest = select_context(raw_context(findings=findings), "review", 2048)
        self.assertLess(len(context["findings"]), 5)
        self.assertTrue(manifest["omitted_claim_ids"])
        self.assertTrue(all(f in findings for f in context["findings"]))

    def test_required_request_and_conflicts_are_never_silently_truncated(self):
        raw = raw_context()
        raw["request"] = "x" * 3000
        with self.assertRaisesRegex(AgentOutputError, "required_fields"):
            select_context(raw, "planner", 2048)
        raw = raw_context(findings=[{"claim_id": "a", "statement": "x" * 3000, "sources": []}])
        raw["review"] = {"unsupported_claim_ids": ["a"], "conflicts": []}
        with self.assertRaisesRegex(AgentOutputError, "required_claims"):
            select_context(raw, "writer", 2048)

    def test_research_can_only_cite_the_selected_source_text(self):
        context, _ = select_context(raw_context("prefix " * 1000 + "secret ending"), "research", 2048)
        value = {"findings": [{"statement": "A prefix exists", "source_document_id": "source", "quote": "prefix"}], "limitations": []}
        result = normalize("research", value, context)
        self.assertEqual(result["findings"][0]["sources"][0]["locator"], "chars:0:6")
        value["findings"][0]["quote"] = "secret ending"
        with self.assertRaises(AgentOutputError):
            normalize("research", value, context)


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL")
class ContextTests(PostgresFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.settings = replace(self.settings, lease_seconds=30)
        self.engine = Execution(self.settings)
        self.contexts = Contexts(self.engine)

    def submit_agent(self):
        fixture = demo()
        run_id, _ = self.engine.submit(self.submission(idempotency_key=uuid4().hex, request=fixture["request"],
            agent={"backend": "recorded"}, documents=fixture["documents"], budget_micro_usd=1000000))
        self.engine.maintain()
        return run_id

    def finish(self, run_id):
        for _ in range(10):
            run_worker(self.settings, once=True)
            state = self.engine.status(run_id)["state"]
            if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                self.assertEqual(state, "SUCCEEDED", self.engine.status(run_id)["failure"])
                return
        self.fail("Agent workflow did not finish")

    def test_snapshots_are_reused_across_attempts_and_ignore_forged_claim_inputs(self):
        run_id = self.submit_agent()
        claim = self.engine.claim("first")
        claim["request"] = "forged"
        first = self.contexts.assemble(claim)
        self.assertEqual(first["context"]["request"], demo()["request"])
        self.expire(claim)
        self.engine.maintain()
        self.ready_retries()
        second = self.contexts.assemble(self.engine.claim("second"))
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["context_digest"], second["context_digest"])
        self.assertEqual(sum(e["kind"] == "context.assembled" for e in self.engine.events(run_id)), 1)

    def test_context_corruption_is_detected_before_spend(self):
        run_id = self.submit_agent()
        claim = self.engine.claim("worker")
        self.contexts.assemble(claim)
        with connect(self.dsn) as conn:
            conn.execute("UPDATE context_snapshots SET context=%s WHERE run_id=%s", (Jsonb({"tampered": True}), run_id))
        with self.assertRaisesRegex(AgentOutputError, "context_integrity_error"):
            execute_agent(self.engine, claim)
        self.assertEqual(Budgets(self.engine).history(run_id), [])

    def test_changed_prompt_cannot_reuse_a_snapshot(self):
        self.submit_agent()
        claim = self.engine.claim("worker")
        self.contexts.assemble(claim)
        with patch.dict(PROMPTS, planner="Changed instructions"):
            with self.assertRaisesRegex(AgentOutputError, "prompt_version_changed"):
                self.contexts.assemble(claim)

    def test_provenance_replay_and_api_authentication(self):
        run_id = self.submit_agent()
        self.finish(run_id)
        provenance = self.contexts.provenance(run_id)
        self.assertEqual(len(provenance["contexts"]), 5)
        with patch("aep.model_provider.ModelGateway.call", side_effect=AssertionError("Network forbidden")):
            replay = self.contexts.replay(run_id)
        self.assertEqual(len(replay["replayed_tasks"]), 5)
        self.assertEqual(replay["provider_calls"], 0)
        with self.assertRaises(NotFound):
            self.contexts.provenance(run_id, owner="foreign")
        with TestClient(create_app(self.settings)) as client:
            headers = {"X-API-Key": "test-key"}
            self.assertEqual(client.get(f"/runs/{run_id}/contexts/planner").status_code, 401)
            self.assertEqual(client.get(f"/runs/{run_id}/contexts/planner", headers=headers).status_code, 200)
            self.assertEqual(client.get(f"/runs/{run_id}/provenance", headers=headers).status_code, 200)
            self.assertEqual(client.post(f"/runs/{run_id}/replay", headers=headers).status_code, 200)

    def test_changed_operation_breaks_replay(self):
        run_id = self.submit_agent()
        self.finish(run_id)
        with connect(self.dsn) as conn:
            conn.execute("UPDATE operations SET request_hash=%s WHERE run_id=%s AND task_id='planner'", ("0" * 64, run_id))
        with self.assertRaises(Conflict):
            self.contexts.replay(run_id)

    def test_agent_cannot_commit_forged_context_provenance(self):
        self.submit_agent()
        claim = self.engine.claim("worker")
        output = execute_agent(self.engine, claim)
        output["agent_output"]["context"]["digest"] = "0" * 64
        with self.assertRaises(Conflict):
            self.engine.complete(claim, output)

    def test_new_financial_evidence_keeps_usage_and_result_hash_without_text(self):
        run_id = self.submit_agent()
        self.finish(run_id)
        evidence = Budgets(self.engine).history(run_id, kind="ledger")[0]["evidence"]
        self.assertEqual(evidence["evidence_version"], "usage-v2")
        self.assertEqual(len(evidence["result_digest"]), 64)
        self.assertNotIn("result", evidence)
        self.assertIn("input_tokens", evidence)
        self.assertEqual(len(self.contexts.replay(run_id)["replayed_tasks"]), 5)

    def test_retention_dry_run_and_purge_preserve_audit_and_financial_evidence(self):
        run_id = self.submit_agent()
        self.finish(run_id)
        before = Budgets(self.engine).history(run_id, kind="ledger")
        retention = Retention(self.engine)
        self.assertIn(run_id, retention.run(days=0)["eligible_run_ids"])
        self.assertIsNotNone(self.contexts.get(run_id, "planner")["context"])
        result = retention.purge(run_id)
        self.assertEqual(result["state"], "DONE")
        self.assertEqual(before, Budgets(self.engine).history(run_id, kind="ledger"))
        self.assertEqual(len(self.contexts.provenance(run_id)["contexts"]), 5)
        with self.assertRaises(Gone):
            self.contexts.get(run_id, "planner")
        with self.assertRaises(Gone):
            self.contexts.replay(run_id)
        status = self.engine.status(run_id)
        with self.assertRaises(Gone):
            self.engine.artifact(run_id, status["final_artifact_id"])
        operation = Budgets(self.engine).history(run_id)[0]
        self.assertEqual(Budgets(self.engine).reconcile_operation(operation["id"])["state"], "SUCCEEDED")
        with TestClient(create_app(self.settings)) as client:
            self.assertEqual(client.get(f"/runs/{run_id}/contexts/planner", headers={"X-API-Key": "test-key"}).status_code, 410)
        with connect(self.dsn) as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM context_snapshots WHERE run_id=%s AND context IS NOT NULL", (run_id,)).fetchone()["n"], 0)
            self.assertEqual(conn.execute("SELECT input_snapshot FROM attempts WHERE run_id=%s LIMIT 1", (run_id,)).fetchone()["input_snapshot"], {})

    def test_retention_file_cleanup_resumes_after_failure(self):
        run_id = self.submit_agent()
        self.finish(run_id)
        retention = Retention(self.engine)
        with patch.object(self.engine.store, "delete", side_effect=OSError("Injected disk failure")):
            self.assertEqual(retention.purge(run_id)["state"], "PENDING")
        self.assertEqual(retention.purge(run_id)["state"], "DONE")
        self.assertEqual(retention.cleanup(run_id)["state"], "DONE")

    def test_retention_denies_active_runs_foreign_owners_and_unknown_liabilities(self):
        run_id = self.submit_agent()
        retention = Retention(self.engine)
        with self.assertRaises(Conflict):
            retention.purge(run_id)
        with self.assertRaises(NotFound):
            retention.purge(run_id, owner="foreign")
        claim = self.engine.claim("worker")
        budgets = Budgets(self.engine)
        op = budgets.reserve(claim, {"call_key": "test", "provider": "fake-v1", "input": "a", "max_input_tokens": 1,
                                    "max_output_tokens": 1, "output_tokens": 1, "max_cost_micro_usd": 7})
        budgets.dispatch(claim, op["id"])
        budgets.mark_unknown(op["id"])
        self.engine.cancel(run_id)
        self.engine.acknowledge_stop(claim)
        with self.assertRaises(Conflict):
            retention.purge(run_id)

    def test_artifact_retention_rejects_other_runs_and_path_traversal(self):
        run_id = self.submit_agent()
        for key in ["../outside", str(uuid4()) + "/" + "a" * 64, run_id + "/../../outside"]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.engine.store.delete(run_id, key)


if __name__ == "__main__":
    unittest.main()
