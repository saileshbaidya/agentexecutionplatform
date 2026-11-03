"""Agent protocols, recorded execution, and mocked OpenAI wire/recovery tests."""

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

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import httpx
from fastapi.testclient import TestClient
from aep.agents import AgentOutputError, build_profile, demo, execute_agent, normalize, render_report
from aep.api import create_app
from aep.budgets import Budgets
from aep.config import Settings
from aep.db import connect
from aep.execution import Execution
from aep.model_provider import ModelProvider
from aep.planner import make_agent_plan
from aep.providers import usage_cost
from aep.worker import run_worker
from db_support import PostgresFixture


def source_context():
    documents = []
    for d in demo()["documents"]:
        documents.append({**d, "artifact": {"artifact_id": str(uuid4()), "sha256": hashlib.sha256(d["text"].encode()).hexdigest()}})
    return {"task_id": "research-1", "documents": documents, "findings": [], "expected_task_ids": ["research-1", "research-2"]}


class AgentProtocolTests(unittest.TestCase):
    def test_quote_checks_and_owned_source_provenance(self):
        context = source_context()
        value = demo()["responses"]["research-1"]
        result = normalize("research", value, context)
        source = result["findings"][0]["sources"][0]
        self.assertEqual(source["artifact"], context["documents"][0]["artifact"])
        self.assertEqual(source["quote"], context["documents"][0]["text"])
        self.assertEqual(source["locator"], "chars:0:" + str(len(source["quote"])))
        for change in ({"quote": "Invented evidence"}, {"source_document_id": "foreign-document"}, {"quote": ""}):
            with self.subTest(change=change):
                bad = copy.deepcopy(value)
                bad["findings"][0].update(change)
                with self.assertRaises(AgentOutputError):
                    normalize("research", bad, context)

    def test_planner_is_bounded_and_cannot_omit_sources(self):
        context = source_context()
        plan = demo()["responses"]["planner"]
        normalize("planner", plan, context)
        for mutate in (lambda p: p["branches"].pop(),
                       lambda p: p["branches"][0].update(document_ids=["foreign"]),
                       lambda p: p["branches"][1].update(document_ids=["north-audit"])):
            changed = copy.deepcopy(plan)
            mutate(changed)
            with self.assertRaises(AgentOutputError):
                normalize("planner", changed, context)

    def test_review_rejects_unknown_claims_and_writer_preserves_conflicts(self):
        context = source_context()
        context["findings"] = [{"claim_id": "one", "statement": "Ten", "sources": []}, {"claim_id": "two", "statement": "Twelve", "sources": []}]
        review = {"unsupported_claim_ids": [], "conflicts": [{"claim_ids": ["one", "two"], "status": "unresolved", "explanation": "Different figures."}]}
        normalize("review", {"review": review}, context)
        with self.assertRaises(AgentOutputError):
            normalize("review", {"review": dict(review, unsupported_claim_ids=["foreign"])}, context)
        context["review"] = review
        writer = {"title": "Capacity", "summary": "Evidence differs", "sections": [{"heading": "Comparison", "text": "Uncertain", "claim_ids": ["one", "two"]}], "acknowledged_conflict_indices": [0]}
        normalize("writer", writer, context)
        with self.assertRaises(AgentOutputError):
            normalize("writer", dict(writer, acknowledged_conflict_indices=[]), context)
        context["review"]["unsupported_claim_ids"] = ["two"]
        with self.assertRaises(AgentOutputError):
            normalize("writer", writer, context)

    def test_report_renderer_carries_uncertainty_and_escapes_untrusted_markup(self):
        context = source_context()
        context["findings"] = normalize("research", demo()["responses"]["research-1"], context)["findings"]
        context["review"] = {"unsupported_claim_ids": [], "conflicts": [{"claim_ids": ["one", "two"], "status": "unresolved", "explanation": "Still uncertain."}]}
        value = {"title": "<script>bad</script>", "summary": "[bad](https://evil.invalid)", "sections": []}
        report = render_report(value, context, "run")
        self.assertNotIn("<script>", report)
        self.assertIn("Unresolved conflict", report)
        self.assertIn("Still uncertain.", report)
        self.assertIn("/runs/run/artifacts/", report)

    def test_prices_round_up_without_float_arithmetic(self):
        quote = {"rate_denominator": 1000000, "input_rate_micro_usd": 400000,
                 "cached_input_rate_micro_usd": 100000, "output_rate_micro_usd": 1600000}
        self.assertEqual(usage_cost(quote, 3, 2, 1), 5)

    def test_model_configuration_never_guesses_a_price_or_exposes_key(self):
        base = Settings("postgresql://unused", ROOT / "artifacts", "api-key")
        with self.assertRaises(ValueError):
            replace(base, enable_openai=True)
        configured = replace(base, enable_openai=True, openai_api_key="sk-test-secret", openai_model="test-snapshot",
                             model_input_rate=1000000, model_output_rate=2000000, model_price_version="test-v1")
        self.assertNotIn("sk-test-secret", repr(configured))
        self.assertIsNone(configured.model_cached_input_rate)
        profile = build_profile({"agent": {"backend": "openai"}, "documents": demo()["documents"]}, configured)
        self.assertEqual(profile["cached_input_rate"], configured.model_input_rate)
        with self.assertRaises(ValueError):
            replace(configured, openai_api_key="secret\nheader")


class ResponsesServer:
    """In-memory wire fixtures; no external requests and no real model charges."""
    def __init__(self, queued=False, lost=False, bad_quote=False, reject=False, count=100, always_bad=False):
        self.queued, self.lost, self.bad_quote, self.reject, self.count = queued, lost, bad_quote, reject, count
        self.creates, self.requests, self.responses = [], [], {}
        self.callback = None
        self.get_status = 200
        self.always_bad = always_bad

    def handle(self, request):
        self.requests.append(request)
        if request.url.path == "/v1/responses/input_tokens":
            return httpx.Response(200, json={"input_tokens": self.count, "object": "response.input_tokens"})
        if request.method == "GET":
            if self.get_status != 200:
                return httpx.Response(self.get_status, json={"error": "not available"})
            return httpx.Response(200, json=self.responses[request.url.path.rsplit("/", 1)[-1]])
        body = json.loads(request.content)
        self.creates.append(body)
        if self.callback:
            self.callback(body)
        if self.reject:
            return httpx.Response(400, json={"error": {"code": "invalid_request"}})
        payload = json.loads(body["input"])
        context = payload["context"]
        role = body["text"]["format"]["name"].removesuffix("_v1")
        key = context["task_id"] if role == "research" else role
        data = copy.deepcopy(demo()["responses"][key])
        if self.bad_quote and role == "research" and ("repair" not in payload or self.always_bad):
            data["findings"][0]["quote"] = "Unsupported quote"
        text = json.dumps(data)
        for index, finding in enumerate(context["findings"], 1):
            text = text.replace(f"$CLAIM_{index}", finding["claim_id"])
        response_id = "resp_mock_" + str(len(self.creates))
        raw = {"id": response_id, "model": body["model"], "metadata": body["metadata"], "status": "completed",
               "usage": {"input_tokens": 100, "output_tokens": len(text), "input_tokens_details": {"cached_tokens": 25}},
               "output": [{"type": "message", "content": [{"type": "output_text", "text": text}]}]}
        self.responses[response_id] = raw
        if self.lost:
            raise httpx.ReadTimeout("simulated response loss", request=request)
        if self.queued:
            return httpx.Response(200, json=dict(raw, status="queued", usage=None, output=[]))
        return httpx.Response(200, json=raw)

    def client(self, settings):
        return httpx.Client(transport=httpx.MockTransport(self.handle), timeout=1, follow_redirects=False)


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL to run PostgreSQL integration tests")
class AgentExecutionTests(PostgresFixture, unittest.TestCase):
    def submit_agent(self, backend="recorded", **changes):
        fixture = demo()
        submission = self.submission(request=fixture["request"], documents=fixture["documents"],
                                     agent={"backend": backend}, budget_micro_usd=1000000)
        submission.update(changes)
        return self.engine.submit(submission)[0]

    def enable_mock(self):
        self.settings = replace(self.settings, enable_openai=True, openai_api_key="sk-test-secret",
            openai_model="test-model-2026-01-01", model_price_version="test-price-v1",
            model_input_rate=1000000, model_cached_input_rate=500000, model_output_rate=2000000)
        self.engine = Execution(self.settings)

    def finish(self, run_id, limit=30):
        with patch("aep.execution.random.uniform", return_value=0):
            for _ in range(limit):
                run_worker(self.settings, once=True)
                self.engine.maintain()
                status = self.engine.status(run_id)
                if status["state"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                    return status
        self.fail("Workflow did not reach a terminal state")

    def test_recorded_agent_pipeline_and_source_api(self):
        run_id = self.submit_agent()
        status = self.finish(run_id)
        self.assertEqual(status["state"], "SUCCEEDED", status["failure"])
        self.assertEqual(len(status["tasks"]), 5)
        self.assertEqual(len(Budgets(self.engine).history(run_id)), 5)
        self.assertGreater(status["budget"]["settled_micro_usd"], 0)
        self.assertEqual(status["budget"]["reserved_micro_usd"], 0)
        _, raw = self.engine.artifact(run_id, status["final_artifact_id"])
        report = json.loads(raw)
        self.assertIn("Unresolved conflict", report["report_markdown"])
        self.assertEqual(report["agent_output"]["prompt_version"], "writer-v1")
        with TestClient(create_app(self.settings)) as client:
            headers = {"X-API-Key": "test-key"}
            self.assertEqual(client.get("/agents").status_code, 401)
            self.assertEqual(len(client.get("/agents", headers=headers).json()["agents"]), 4)
            for source in status["documents"]:
                response = client.get(f"/runs/{run_id}/artifacts/{source['artifact_id']}", headers=headers)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(hashlib.sha256(response.content).hexdigest(), source["sha256"])
            other, _ = self.engine.submit(self.submission(idempotency_key="other"))
            self.assertEqual(client.get(f"/runs/{other}/artifacts/{status['documents'][0]['artifact_id']}", headers=headers).status_code, 404)

    def test_invalid_submission_modes_and_server_owned_profiles(self):
        fixture = demo()
        for changed in ({"agent": {"backend": "openai"}}, {"documents": []},
                        {"documents": [fixture["documents"][0], fixture["documents"][0]]},
                        {"request": "Unmatched recorded request"}, {"agent": {"backend": "recorded", "research_branches": 3}}):
            with self.subTest(changed=changed):
                with self.assertRaises(ValueError):
                    self.submit_agent(**changed)
        profile = build_profile({"agent": {"backend": "recorded"}, "documents": fixture["documents"], "request": fixture["request"]}, self.settings)
        with self.assertRaises(ValueError):
            self.engine.submit(self.submission(plan=make_agent_plan(profile)))

    def test_source_and_model_profile_replay_are_durable(self):
        run_id = self.submit_agent()
        self.assertEqual(self.submit_agent(), run_id)
        self.assertEqual(len(self.engine.status(run_id)["documents"]), 2)
        self.engine.maintain()
        claim = self.engine.claim("worker")
        snapshot = self.engine.status(run_id)["attempts"][0]["input_snapshot"]
        self.assertEqual(claim["documents"], snapshot["documents"])
        self.assertEqual(snapshot["spec"]["agent_profile"]["model"], "synthetic-agent-demo-v1")

    def test_corrupted_source_fails_before_any_model_spend(self):
        run_id = self.submit_agent()
        with connect(self.dsn) as conn:
            conn.execute("UPDATE source_documents SET content='corrupted' WHERE run_id=%s AND document_id='north-audit'", (run_id,))
        status = self.finish(run_id)
        self.assertEqual(status["failure"], "source_integrity_error")
        self.assertEqual(Budgets(self.engine).history(run_id), [])

    def test_openai_payloads_and_usage_through_the_shared_budget(self):
        self.enable_mock()
        server = ResponsesServer()
        with patch("aep.model_provider.make_client", side_effect=server.client):
            run_id = self.submit_agent("openai")
            status = self.finish(run_id)
        self.assertEqual(status["state"], "SUCCEEDED", status["failure"])
        self.assertEqual(len(server.creates), 5)
        for body in server.creates:
            self.assertTrue(body["background"])
            self.assertTrue(body["store"])
            self.assertEqual(body["truncation"], "disabled")
            self.assertEqual(body["service_tier"], "default")
            self.assertEqual(body["max_output_tokens"], 2048)
            self.assertTrue(body["text"]["format"]["strict"])
            self.assertNotIn("tools", body)
        self.assertNotIn("sk-test-secret", json.dumps(status, default=str))
        ledger = Budgets(self.engine).history(run_id, kind="ledger")
        self.assertEqual(sum(e["amount_micro_usd"] for e in ledger), status["budget"]["settled_micro_usd"])

    def test_background_polling_resumes_without_creating_again(self):
        self.enable_mock()
        server = ResponsesServer(queued=True)
        with patch("aep.model_provider.make_client", side_effect=server.client):
            run_id = self.submit_agent("openai")
            status = self.finish(run_id)
        self.assertEqual(status["state"], "SUCCEEDED", status["failure"])
        self.assertEqual(len(server.creates), 5)
        self.assertEqual(len(status["attempts"]), 10)
        self.assertEqual(len(Budgets(self.engine).history(run_id, kind="ledger")), 5)

    def test_bad_quote_has_one_bounded_paid_repair_per_research_stage(self):
        self.enable_mock()
        server = ResponsesServer(bad_quote=True)
        with patch("aep.model_provider.make_client", side_effect=server.client):
            run_id = self.submit_agent("openai")
            status = self.finish(run_id)
        self.assertEqual(status["state"], "SUCCEEDED", status["failure"])
        self.assertEqual(len(server.creates), 7)
        keys = [o["call_key"] for o in Budgets(self.engine).history(run_id)]
        self.assertEqual(keys.count("research-v1.step-1"), 2)

    def test_background_repair_reuses_both_logical_calls_after_resume(self):
        self.enable_mock()
        server = ResponsesServer(queued=True, bad_quote=True)
        with patch("aep.model_provider.make_client", side_effect=server.client):
            run_id = self.submit_agent("openai")
            status = self.finish(run_id)
        self.assertEqual(status["state"], "SUCCEEDED", status["failure"])
        self.assertEqual(len(server.creates), 7)
        self.assertEqual(len(Budgets(self.engine).history(run_id, kind="ledger")), 7)

    def test_invalid_output_stops_after_one_repair(self):
        self.enable_mock()
        server = ResponsesServer(bad_quote=True, always_bad=True)
        with patch("aep.model_provider.make_client", side_effect=server.client):
            run_id = self.submit_agent("openai")
            status = self.finish(run_id)
        self.assertEqual(status["state"], "FAILED")
        self.assertEqual(status["failure"], "agent_output_invalid_after_repair")
        self.assertEqual(len(server.creates), 3)  # planner + original research + repair

    def test_budget_denial_prevents_additional_generation(self):
        self.enable_mock()
        server = ResponsesServer()
        with patch("aep.model_provider.make_client", side_effect=server.client):
            run_id = self.submit_agent("openai", budget_micro_usd=20100)
            status = self.finish(run_id)
        self.assertEqual(status["failure"], "budget_exhausted")
        self.assertEqual(len(server.creates), 1)

    def test_scheduler_deadlines_do_not_wait_for_external_polling(self):
        settings = replace(self.settings, inline_reconciliation=False)
        engine = Execution(settings)
        run_id = self.submit_agent()
        with connect(self.dsn) as conn:
            conn.execute("UPDATE runs SET deadline_at=clock_timestamp()-interval '1 second' WHERE id=%s", (run_id,))
        with patch("aep.budgets.Budgets.reconcile_pending", side_effect=AssertionError("Scheduler attempted provider polling")):
            engine.maintain()
        self.assertEqual(engine.status(run_id)["state"], "CANCELLED")

    def test_lost_create_without_id_holds_funds_and_verified_attachment_recovers(self):
        self.enable_mock()
        server = ResponsesServer(lost=True)
        with patch("aep.model_provider.make_client", side_effect=server.client):
            run_id = self.submit_agent("openai")
            run_worker(self.settings, once=True)
            self.engine.maintain()
            status = self.engine.status(run_id)
            self.assertEqual(status["attempts"][0]["state"], "RECONCILIATION")
            self.assertGreater(status["budget"]["reserved_micro_usd"], 0)
            operation = Budgets(self.engine).history(run_id)[0]
            self.assertEqual(len(server.creates), 1)
            with TestClient(create_app(self.settings)) as client:
                path = f"/runs/{run_id}/operations/{operation['id']}/response"
                self.assertEqual(client.post(path, json={"response_id": "resp_mock_1"}).status_code, 401)
                attached = client.post(path, json={"response_id": "resp_mock_1"}, headers={"X-API-Key": "test-key"})
                self.assertEqual(attached.status_code, 200, attached.text)
            server.lost = False
            status = self.finish(run_id)
        self.assertEqual(status["state"], "SUCCEEDED", status["failure"])
        self.assertEqual(len(server.creates), 5)
        self.assertEqual(len(Budgets(self.engine).history(run_id)), 5)

    def test_failed_polling_or_missing_usage_never_releases_liability(self):
        self.enable_mock()
        server = ResponsesServer(queued=True)
        with patch("aep.model_provider.make_client", side_effect=server.client):
            run_id = self.submit_agent("openai")
            run_worker(self.settings, once=True)
            held = self.engine.status(run_id)["budget"]["reserved_micro_usd"]
            server.get_status = 404
            self.engine.maintain()
            self.assertEqual(self.engine.status(run_id)["budget"]["reserved_micro_usd"], held)
            server.get_status = 200
            server.responses["resp_mock_1"]["usage"] = None
            self.engine.maintain()
            self.assertEqual(self.engine.status(run_id)["budget"]["reserved_micro_usd"], held)
            self.engine.cancel(run_id)
            self.engine.maintain()
            self.assertEqual(self.engine.status(run_id)["state"], "CANCELLED")
            self.assertEqual(self.engine.status(run_id)["budget"]["reserved_micro_usd"], held)

    def test_generation_is_outside_run_lock_and_late_charge_is_accounted(self):
        self.enable_mock()
        server = ResponsesServer()
        run_id = self.submit_agent("openai")
        server.callback = lambda body: self.engine.cancel(run_id)
        with patch("aep.model_provider.make_client", side_effect=server.client):
            run_worker(self.settings, once=True)
        status = self.engine.status(run_id)
        self.assertEqual(status["state"], "CANCELLED")
        self.assertGreater(status["budget"]["settled_micro_usd"], 0)
        self.assertEqual(status["budget"]["reserved_micro_usd"], 0)
        self.assertIsNone(status["final_artifact_id"])

    def test_context_preflight_rejects_before_reserving_or_generating(self):
        self.enable_mock()
        server = ResponsesServer(count=64001)
        with patch("aep.model_provider.make_client", side_effect=server.client):
            run_id = self.submit_agent("openai")
            status = self.finish(run_id)
        self.assertEqual(status["failure"], "agent_context_limit_exceeded")
        self.assertEqual(server.creates, [])
        self.assertEqual(Budgets(self.engine).history(run_id), [])

    def test_provider_rejection_is_zero_charge_but_not_a_generation_retry(self):
        self.enable_mock()
        server = ResponsesServer(reject=True)
        with patch("aep.model_provider.make_client", side_effect=server.client):
            run_id = self.submit_agent("openai")
            status = self.finish(run_id)
        self.assertEqual(status["state"], "FAILED")
        self.assertEqual(len(server.creates), 1)
        self.assertEqual(status["budget"]["settled_micro_usd"], 0)
        self.assertEqual(status["budget"]["reserved_micro_usd"], 0)

    def test_disabled_backend_does_not_turn_an_unsent_reservation_into_unknown_spend(self):
        self.enable_mock()
        server = ResponsesServer()
        run_id = self.submit_agent("openai")
        self.engine.maintain()
        claim = self.engine.claim("original")
        with patch("aep.model_provider.make_client", side_effect=server.client):
            with patch("aep.budgets.Budgets.dispatch", side_effect=RuntimeError("crash before dispatch")):
                with self.assertRaises(RuntimeError):
                    execute_agent(self.engine, claim)
        self.expire(claim)
        self.engine.maintain()
        self.ready_retries()
        self.settings = replace(self.settings, enable_openai=False)
        self.engine = Execution(self.settings)
        run_worker(self.settings, once=True)
        self.engine.maintain()
        status = self.engine.status(run_id)
        self.assertEqual(status["failure"], "openai_backend_disabled")
        self.assertEqual(status["budget"]["reserved_micro_usd"], 0)
        self.assertEqual(Budgets(self.engine).history(run_id)[0]["state"], "CANCELLED")
        self.assertEqual(server.creates, [])


if __name__ == "__main__":
    unittest.main()
