"""Verify saved context, provenance, and offline replay through the deployed API."""

import json
import os
from pathlib import Path
import sys
import time
from uuid import uuid4

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from aep.context import digest


def main():
    submission = json.loads((Path(__file__).resolve().parents[1] / "examples/agent-submission.json").read_text())
    submission["idempotency_key"] = "context-" + uuid4().hex
    headers = {"X-API-Key": os.environ.get("AEP_API_KEY", "local-development-only")}
    with httpx.Client(base_url=os.environ.get("AEP_API_URL", "http://127.0.0.1:8000"), headers=headers, timeout=15) as client:
        response = client.post("/runs", json=submission)
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
                raise SystemExit("Context smoke timed out; cancellation requested")
            time.sleep(0.3)
        assert run["state"] == "SUCCEEDED", run["failure"]
        response = client.get(f"/runs/{run_id}/provenance")
        response.raise_for_status()
        provenance = response.json()
        assert len(provenance["contexts"]) == 5
        for task in run["tasks"]:
            response = client.get(f"/runs/{run_id}/contexts/{task['id']}")
            response.raise_for_status()
            snapshot = response.json()
            assert digest(snapshot["context"]) == snapshot["context_digest"]
            assert digest(snapshot["manifest"]) == snapshot["manifest_digest"]
            assert snapshot["manifest"]["selected_bytes"] <= snapshot["manifest"]["byte_limit"]
            assert task["result"]["agent_output"]["context"]["digest"] == snapshot["context_digest"]
        response = client.post(f"/runs/{run_id}/replay")
        response.raise_for_status()
        replay = response.json()
        assert len(replay["replayed_tasks"]) == 5
        assert replay["provider_calls"] == 0 and replay["tool_calls"] == 0
        print(json.dumps({"run_id": run_id, "state": run["state"], "verified_contexts": 5,
                          "offline_replay": True, "replay_provider_calls": 0}))


if __name__ == "__main__":
    main()
