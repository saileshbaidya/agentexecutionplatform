"""Submit an isolated text tool task and verify the durable result through HTTP."""

import json
import os
from pathlib import Path
import sys
import time
from uuid import uuid4

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from aep.planner import make_plan


def main():
    plan = make_plan()
    plan["tasks"][0].update(kind="utility", handler="tool-v1", capabilities=["text.stats"],
                            tool_call={"call_key": "stats-v1", "tool": "text.stats", "arguments": {"text": "hello secure tools"}})
    headers = {"X-API-Key": os.environ.get("AEP_API_KEY", "local-development-only")}
    with httpx.Client(base_url=os.environ.get("AEP_API_URL", "http://127.0.0.1:8000"), headers=headers, timeout=10) as client:
        response = client.get("/tools")
        response.raise_for_status()
        if not any(t["name"] == "text.stats" and t["enabled"] for t in response.json()["tools"]):
            raise SystemExit("Enable secure tools using compose.tools.yaml; see docs/milestone-5.md")
        response = client.post("/runs", json={"schema_version": "1", "idempotency_key": "tools-" + uuid4().hex,
                              "request": "Exercise isolated tool execution", "budget_micro_usd": 0,
                              "timeout_seconds": 120, "plan": plan})
        response.raise_for_status()
        run_id = response.json()["run_id"]
        deadline = time.monotonic() + 120
        while True:
            response = client.get(f"/runs/{run_id}")
            response.raise_for_status()
            run = response.json()
            if run["state"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            if time.monotonic() >= deadline:
                client.post(f"/runs/{run_id}/cancel").raise_for_status()
                raise SystemExit("Tool smoke timed out; cancellation requested")
            time.sleep(0.3)
        if run["state"] != "SUCCEEDED":
            raise SystemExit(f"Tool smoke failed: {run['failure']}")
        response = client.get(f"/runs/{run_id}/tool-actions")
        response.raise_for_status()
        actions = response.json()["actions"]
        assert len(actions) == 1 and actions[0]["state"] == "SUCCEEDED", actions
        assert actions[0]["result"] == {"characters": 18, "words": 3, "lines": 1}, actions
        assert actions[0]["action"]["limits"]["network"] == "none"
        task = next(t for t in run["tasks"] if t["id"] == "research-a")
        assert task["result"]["tool_output"]["action_digest"] == actions[0]["action_digest"]
        print(json.dumps({"run_id": run_id, "state": run["state"], "action_id": actions[0]["id"],
                          "tool": "text.stats", "isolated_result": actions[0]["result"], "action_digest_verified": True}))


if __name__ == "__main__":
    main()
