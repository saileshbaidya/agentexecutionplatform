"""Policy, approval, lease, and gateway recovery security boundaries."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import os
from pathlib import Path
import sys
import threading
import unittest
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

from aep.api import create_app
from aep.db import connect
from aep.execution import Execution, Conflict, LeaseLost, NotFound
from aep.planner import make_plan, validate_executable_plan
from aep.tool_policy import ToolDenied, validate_call
from aep.tool_sandbox import DockerSandbox
from aep.tools import ToolGateway, ToolPending
from aep.tool_gateway import run_gateway
from aep.worker import run_worker
from db_support import PostgresFixture

IMAGE = "sha256:" + "a" * 64
APPROVAL_KEY = "test-human-approval-secret-012345"


def plan(tool="text.stats", args=None):
    value = make_plan()
    task = value["tasks"][0]
    task.update(kind="utility", handler="tool-v1", capabilities=[tool],
                tool_call={"call_key": "tool-step-0", "tool": tool,
                           "arguments": args if args is not None else {"text": "hello world"}})
    return value


class PolicyTests(unittest.TestCase):
    def test_no_arbitrary_tools_arguments_or_executor_selection(self):
        for call in [
            {"call_key": "x", "tool": "shell", "arguments": {}},
            {"call_key": "x", "tool": "text.stats", "arguments": {"text": "ok", "url": "https://evil"}},
            {"call_key": "x", "tool": "python.run", "arguments": {"code": "print(1)", "stdin": "", "mount": "/"}},
            {"call_key": "x", "tool": "text.stats", "arguments": {"text": "a" * 20001}},
        ]:
            with self.subTest(call=call["tool"]), self.assertRaises(ValueError):
                validate_call(call)

    def test_tool_call_cannot_hide_on_another_handler(self):
        value = plan()
        value["tasks"][0]["handler"] = "fake-utility-v1"
        with self.assertRaises(ValueError):
            validate_executable_plan(value)

    def test_docker_command_has_mandatory_boundaries(self):
        command = DockerSandbox().command({"id": uuid4(), "action": {"image": IMAGE}})
        for flag in ["--network=none", "--read-only", "--user=65534:65534", "--cap-drop=ALL",
                     "--security-opt=no-new-privileges=true", "--memory=128m", "--memory-swap=128m",
                     "--cpus=0.5", "--pids-limit=32", "--pull=never", "--log-driver=none"]:
            self.assertIn(flag, command)
        self.assertFalse(any(flag in command for flag in ["--privileged", "--volume", "-v", "--env", "-e"]))


class FakeSandbox:
    def __init__(self):
        self.calls = []
        self.cleaned = []

    def execute(self, row, active):
        assert active()
        self.calls.append(str(row["id"]))
        return {"characters": 11, "words": 2, "lines": 1}, None

    def cleanup(self, action_id):
        self.cleaned.append(str(action_id))
        return True


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL")
class ToolTests(PostgresFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.settings = replace(self.settings, tool_capabilities=("text.stats", "python.run"),
                                tool_image=IMAGE, tool_approval_key=APPROVAL_KEY)
        self.engine = Execution(self.settings)
        self.gateway = ToolGateway(self.engine)

    def start_tool(self, tool="text.stats", args=None, capability=True):
        value = plan(tool, args)
        if not capability:
            value["tasks"][0]["capabilities"] = []
        run_id, _ = self.engine.submit(self.submission(plan=value))
        self.engine.maintain()
        claim = self.engine.claim("tool-worker")
        self.assertEqual(claim["task_id"], "research-a")
        return run_id, claim

    def propose(self, tool="text.stats", args=None):
        run_id, claim = self.start_tool(tool, args)
        with self.assertRaises(ToolPending) as pending:
            self.gateway.call(claim)
        self.gateway.park(claim, pending.exception.action_id)
        return run_id, claim, self.gateway.history(run_id)[0]

    def approve(self, run_id, action, **changes):
        body = {"action_digest": action["action_digest"], "decision": "approve", "ttl_seconds": 60}
        body.update(changes)
        return self.gateway.decide(run_id, action["id"], body)

    def test_end_to_end_tool_graph_and_cached_result(self):
        run_id, claim, action = self.propose()
        sandbox = FakeSandbox()
        run_gateway(self.settings, once=True, sandbox=sandbox)
        self.engine.maintain()
        resumed = self.engine.claim("new-worker")
        output = self.gateway.call(resumed)
        self.assertEqual(output["tool_result"]["words"], 2)
        self.engine.complete(resumed, output)
        for _ in range(4):
            run_worker(self.settings, once=True)
        self.assertEqual(self.engine.status(run_id)["state"], "SUCCEEDED")
        self.assertEqual(len(sandbox.calls), 1)
        self.assertEqual(self.gateway.history(run_id)[0]["state"], "SUCCEEDED")
        self.assertEqual(self.engine.status(run_id)["budget"]["settled_micro_usd"], 0)

    def test_task_capability_denied_and_audited_even_if_claim_forged(self):
        run_id, claim = self.start_tool(capability=False)
        claim["spec"]["capabilities"] = ["text.stats"]
        with self.assertRaises(ToolDenied):
            self.gateway.call(claim)
        self.assertEqual(self.gateway.history(run_id), [])
        self.assertIn("tool.denied", [e["kind"] for e in self.engine.events(run_id)])

    def test_deployment_default_denies(self):
        run_id, claim = self.start_tool()
        gateway = ToolGateway(Execution(replace(self.settings, tool_capabilities=())))
        with self.assertRaises(ToolDenied):
            gateway.call(claim)

    def test_worker_denial_is_a_permanent_policy_failure(self):
        value = plan()
        value["tasks"][0]["capabilities"] = []
        run_id, _ = self.engine.submit(self.submission(plan=value))
        self.engine.maintain()
        run_worker(self.settings, once=True)
        self.assertEqual(self.engine.status(run_id)["failure"], "tool_denied")
        self.assertEqual(self.gateway.history(run_id), [])

    def test_stale_worker_cannot_propose_or_park(self):
        run_id, claim = self.start_tool()
        self.expire(claim)
        with self.assertRaises(LeaseLost):
            self.gateway.call(claim)
        with self.assertRaises(LeaseLost):
            self.gateway.park(claim, uuid4())

    def test_worker_cannot_complete_without_authoritative_tool_receipt(self):
        run_id, claim = self.start_tool("python.run", {"code": "print(1)", "stdin": ""})
        with self.assertRaises(Conflict):
            self.engine.complete(claim, {"tool_result": {"stdout": "forged"}})
        self.assertEqual(self.gateway.history(run_id), [])

    def test_worker_cannot_change_tool_result(self):
        run_id, claim, action = self.propose()
        run_gateway(self.settings, once=True, sandbox=FakeSandbox())
        self.engine.maintain()
        resumed = self.engine.claim("result-worker")
        output = self.gateway.call(resumed)
        output["tool_result"]["words"] = 999
        with self.assertRaises(Conflict):
            self.engine.complete(resumed, output)

    def test_approval_wait_releases_lease_and_does_not_poll_retry(self):
        run_id, claim, action = self.propose("python.run", {"code": "print(1)", "stdin": ""})
        self.expire(claim)
        for _ in range(3):
            self.engine.maintain()
        status = self.engine.status(run_id)
        task = next(t for t in status["tasks"] if t["id"] == claim["task_id"])
        self.assertEqual(task["state"], "TOOL_WAIT")
        self.assertEqual(task["attempt_count"], 1)
        self.assertEqual(status["attempts"][0]["state"], "TOOL_WAIT")
        self.assertIsNone(self.gateway.dispatch(action["id"]))

    def test_only_separate_credential_can_approve(self):
        run_id, claim, action = self.propose("python.run", {"code": "print(1)", "stdin": ""})
        path = f"/runs/{run_id}/tool-actions/{action['id']}/approval"
        body = {"action_digest": action["action_digest"], "decision": "approve", "ttl_seconds": 60}
        with TestClient(create_app(self.settings)) as client:
            self.assertEqual(client.post(path, json=body).status_code, 401)
            self.assertEqual(client.post(path, json=body, headers={"X-API-Key": "test-key"}).status_code, 403)
            headers = {"X-API-Key": "test-key", "X-Approval-Key": APPROVAL_KEY}
            self.assertEqual(client.post(path, json=body, headers=headers).status_code, 200)
            self.assertEqual(client.post(path, json=dict(body, decision=[]), headers=headers).status_code, 422)
            self.assertEqual(client.get(f"/runs/{run_id}/tool-actions").status_code, 401)

    def test_digest_mismatch_and_foreign_ownership_denied(self):
        run_id, claim, action = self.propose("python.run", {"code": "print(1)", "stdin": ""})
        with self.assertRaises(Conflict):
            self.approve(run_id, action, action_digest="0" * 64)
        with self.assertRaises(NotFound):
            self.gateway.decide(run_id, action["id"], {"action_digest": action["action_digest"], "decision": "approve", "ttl_seconds": 60}, owner="foreign")
        with self.assertRaises(NotFound):
            self.gateway.history(run_id, owner="foreign")
        with self.assertRaises(NotFound):
            self.approve(run_id, dict(action, id=uuid4()))

    def test_approval_consumed_once_under_concurrent_dispatch(self):
        run_id, claim, action = self.propose("python.run", {"code": "print(1)", "stdin": ""})
        approval = self.approve(run_id, action)
        replay = self.approve(run_id, action, ttl_seconds=300)
        self.assertEqual(approval["expires_at"], replay["expires_at"])
        with ThreadPoolExecutor(max_workers=4) as pool:
            rows = list(pool.map(lambda _: self.gateway.dispatch(action["id"]), range(4)))
        self.assertEqual(sum(row is not None for row in rows), 1)
        self.assertIsNotNone(self.gateway.history(run_id)[0]["consumed_at"])
        self.assertIsNone(self.gateway.dispatch(action["id"]))

    def test_changed_arguments_and_policy_cannot_use_approval(self):
        run_id, claim, action = self.propose("python.run", {"code": "print(1)", "stdin": ""})
        self.approve(run_id, action)
        with connect(self.dsn) as conn:
            spec = claim["spec"]
            spec["tool_call"]["arguments"]["code"] = "print(2)"
            conn.execute("UPDATE tasks SET spec=%s WHERE run_id=%s AND id=%s", (Jsonb(spec), run_id, claim["task_id"]))
        self.assertIsNone(self.gateway.dispatch(action["id"]))
        self.assertEqual(self.gateway.history(run_id)[0]["state"], "DENIED")

    def test_policy_revocation_and_image_change_denied_at_dispatch(self):
        run_id, claim, action = self.propose()
        revoked = ToolGateway(Execution(replace(self.settings, tool_image="sha256:" + "b" * 64)))
        self.assertIsNone(revoked.dispatch(action["id"]))
        self.assertEqual(self.gateway.history(run_id)[0]["state"], "DENIED")

    def test_expired_approval_is_not_refreshed_by_replay(self):
        run_id, claim, action = self.propose("python.run", {"code": "print(1)", "stdin": ""})
        self.approve(run_id, action)
        with connect(self.dsn) as conn:
            conn.execute("UPDATE tool_approvals SET expires_at=clock_timestamp()-interval '1 second' WHERE action_id=%s", (action["id"],))
        replay = self.approve(run_id, action)
        self.assertIsNone(self.gateway.dispatch(action["id"]))
        self.assertEqual(self.gateway.history(run_id)[0]["state"], "DENIED")

    def test_deny_and_expiry_wake_worker_without_dispatch(self):
        run_id, claim, action = self.propose("python.run", {"code": "print(1)", "stdin": ""})
        self.approve(run_id, action, decision="deny")
        self.engine.maintain()
        resumed = self.engine.claim("resumed")
        with self.assertRaises(ToolDenied):
            self.gateway.call(resumed)
        self.assertIsNone(self.gateway.dispatch(action["id"]))

    def test_cancellation_closes_pending_approval(self):
        run_id, claim, action = self.propose("python.run", {"code": "print(1)", "stdin": ""})
        self.engine.cancel(run_id)
        with self.assertRaises(Conflict):
            self.approve(run_id, action)
        self.assertIsNone(self.gateway.dispatch(action["id"]))
        self.assertEqual(self.gateway.history(run_id)[0]["state"], "CANCELLED")

    def test_cancel_during_execution_discards_late_result(self):
        run_id, claim, action = self.propose()
        running = self.gateway.dispatch(action["id"])
        self.engine.cancel(run_id)
        self.assertFalse(self.gateway.active(running))
        self.gateway.finish(running, {"private": "late output"})
        row = self.gateway.history(run_id)[0]
        self.assertEqual(row["state"], "CANCELLED")
        self.assertIsNone(row["result"])

    def test_gateway_crash_becomes_unknown_without_redispatch(self):
        run_id, claim, action = self.propose()
        self.gateway.dispatch(action["id"])
        with connect(self.dsn) as conn:
            conn.execute("UPDATE tool_actions SET dispatched_at=clock_timestamp()-interval '1 minute' WHERE id=%s", (action["id"],))
        sandbox = FakeSandbox()
        run_gateway(self.settings, once=True, sandbox=sandbox)
        self.assertEqual(self.gateway.history(run_id)[0]["state"], "UNKNOWN")
        self.assertEqual(sandbox.calls, [])
        self.assertEqual(sandbox.cleaned, [str(action["id"])])
        self.assertIsNone(self.gateway.dispatch(action["id"]))

    def test_task_deadline_expires_pending_approval(self):
        run_id, claim, action = self.propose("python.run", {"code": "print(1)", "stdin": ""})
        with connect(self.dsn) as conn:
            conn.execute("UPDATE tasks SET first_started_at=clock_timestamp()-interval '1 hour' WHERE run_id=%s AND id=%s", (run_id, claim["task_id"]))
        self.engine.maintain()
        self.assertEqual(self.gateway.history(run_id)[0]["state"], "CANCELLED")
        self.assertEqual(next(t for t in self.engine.status(run_id)["tasks"] if t["id"] == claim["task_id"])["state"], "FAILED")

    def test_action_expiry_is_terminal_and_does_not_dispatch(self):
        run_id, claim, action = self.propose("python.run", {"code": "print(1)", "stdin": ""})
        with connect(self.dsn) as conn:
            conn.execute("UPDATE tool_actions SET expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (action["id"],))
        self.engine.maintain()
        self.assertEqual(self.gateway.history(run_id)[0]["state"], "EXPIRED")
        with self.assertRaises(Conflict):
            self.approve(run_id, action)

    def test_capability_revoked_after_proposal_prevents_dispatch(self):
        run_id, claim, action = self.propose()
        revoked = ToolGateway(Execution(replace(self.settings, tool_capabilities=())))
        self.assertIsNone(revoked.dispatch(action["id"]))
        self.assertEqual(self.gateway.history(run_id)[0]["state"], "DENIED")

    def test_missing_approval_secret_fails_closed(self):
        run_id, claim, action = self.propose("python.run", {"code": "print(1)", "stdin": ""})
        with TestClient(create_app(replace(self.settings, tool_approval_key=""))) as client:
            response = client.post(f"/runs/{run_id}/tool-actions/{action['id']}/approval",
                                   headers={"X-API-Key": "test-key", "X-Approval-Key": APPROVAL_KEY},
                                   json={"action_digest": action["action_digest"], "decision": "approve", "ttl_seconds": 60})
            self.assertEqual(response.status_code, 403)

    @unittest.skipUnless(os.environ.get("AEP_TEST_TOOL_IMAGE"), "Set AEP_TEST_TOOL_IMAGE for real approved gateway execution")
    def test_real_approved_python_action_through_api_gateway_and_resume(self):
        self.settings = replace(self.settings, tool_image=os.environ["AEP_TEST_TOOL_IMAGE"])
        self.engine = Execution(self.settings)
        self.gateway = ToolGateway(self.engine)
        run_id, claim, action = self.propose("python.run", {"code": "print('reviewed result')", "stdin": ""})
        with TestClient(create_app(self.settings)) as client:
            response = client.post(f"/runs/{run_id}/tool-actions/{action['id']}/approval",
                                   headers={"X-API-Key": "test-key", "X-Approval-Key": APPROVAL_KEY},
                                   json={"action_digest": action["action_digest"], "decision": "approve", "ttl_seconds": 60})
            self.assertEqual(response.status_code, 200, response.text)
        run_gateway(self.settings, once=True)
        self.engine.maintain()
        resumed = self.engine.claim("real-result-worker")
        output = self.gateway.call(resumed)
        self.assertEqual(output["tool_result"], {"stdout": "reviewed result\n", "exit_code": 0})
        self.engine.complete(resumed, output)
        self.assertIsNotNone(self.gateway.history(run_id)[0]["consumed_at"])


if __name__ == "__main__":
    unittest.main()
