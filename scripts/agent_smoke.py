"""Run the recorded agent demonstration; --live explicitly opts into real calls."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--submission", type=Path, default=Path(__file__).resolve().parents[1] / "examples/agent-submission.json")
    parser.add_argument("--live", action="store_true", help="Explicitly use the deployment's real OpenAI backend and submitted budget")
    parser.add_argument("--timeout", type=float, default=180)
    args = parser.parse_args()
    key = os.environ.get("AEP_API_KEY")
    if not key:
        parser.error("Set AEP_API_KEY")

    def request(path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = Request(args.url.rstrip("/") + path, data=data,
                      headers={"X-API-Key": key, "Content-Type": "application/json"})
        with urlopen(req, timeout=15) as response:
            return response.read(), response.headers

    def read(path, body=None):
        return json.loads(request(path, body)[0])

    read("/health/ready")
    if args.live and not read("/agents")["backends"]["openai"]:
        parser.error("The server's OpenAI backend is disabled")
    submission = json.loads(args.submission.read_text(encoding="utf-8"))
    submission["idempotency_key"] = "agent-smoke-" + uuid4().hex
    submission["agent"]["backend"] = "openai" if args.live else "recorded"
    created = read("/runs", submission)
    path = "/runs/" + created["run_id"]
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        run = read(path)
        if run["state"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(0.2)
    else:
        try:
            read(path + "/cancel", {})
        except HTTPError as exc:
            if exc.code != 409:
                raise
        raise RuntimeError("Agent smoke timed out; requested cancellation for " + path)
    if run["state"] != "SUCCEEDED":
        raise RuntimeError(f"Agent run {path} ended {run['state']}: {run['failure']}")
    raw, headers = request(path + "/artifacts/" + run["final_artifact_id"])
    if headers.get("ETag") != '"' + hashlib.sha256(raw).hexdigest() + '"':
        raise RuntimeError("Report checksum mismatch")
    artifact = json.loads(raw)
    for finding in artifact["findings"]:
        for source in finding["sources"]:
            content, _ = request(path + "/artifacts/" + source["artifact"]["artifact_id"])
            if hashlib.sha256(content).hexdigest() != source["artifact"]["sha256"] or source["quote"] not in content.decode():
                raise RuntimeError("Citation does not match an owned source artifact")
    if not args.live and "Unresolved conflict" not in artifact["report_markdown"]:
        raise RuntimeError("The recorded report omitted its unresolved conflict")
    ledger = read(path + "/budget/ledger?limit=1000")["entries"]
    if run["budget"]["reserved_micro_usd"] or sum(e["amount_micro_usd"] for e in ledger) != run["budget"]["settled_micro_usd"]:
        raise RuntimeError("Agent accounting mismatch")
    print(json.dumps({"run_id": run["id"], "state": run["state"], "backend": submission["agent"]["backend"],
                      "tasks": len(run["tasks"]), "operations": len(read(path + "/operations")["operations"]),
                      "settled_micro_usd": run["budget"]["settled_micro_usd"], "citations_verified": True,
                      "final_artifact_id": run["final_artifact_id"]}))


if __name__ == "__main__":
    main()
