"""Exercise a running deployment over HTTP using only the Python standard library."""

import argparse
import hashlib
import json
import os
import time
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--check-parallelism", action="store_true", help="Require two workers and overlapping research attempts")
    parser.add_argument("--check-cancellation", action="store_true", help="Cancel an active run; use a nonzero fake handler delay")
    args = parser.parse_args()
    key = os.environ.get("AEP_API_KEY")
    if not key:
        parser.error("Set AEP_API_KEY to the deployment API key")

    def request(path, body=None):
        payload = json.dumps(body).encode() if body is not None else None
        req = Request(args.url.rstrip("/") + path, data=payload,
                      headers={"X-API-Key": key, "Content-Type": "application/json"})
        with urlopen(req, timeout=10) as response:
            raw = response.read()
            return response.status, json.loads(raw), response.headers, raw

    request("/health/ready")
    submission = {"schema_version": "1", "idempotency_key": "smoke-" + uuid4().hex,
                  "request": "Compare the synthetic evidence", "budget_micro_usd": 0, "timeout_seconds": 120}
    code, created, _, _ = request("/runs", submission)
    if code != 201:
        raise RuntimeError("Expected a newly created run")
    replay_code, replay, _, _ = request("/runs", submission)
    if replay_code != 200 or replay["run_id"] != created["run_id"]:
        raise RuntimeError("Submission replay was not idempotent")
    try:
        request("/runs", dict(submission, request="changed input"))
    except HTTPError as exc:
        if exc.code != 409:
            raise
    else:
        raise RuntimeError("Conflicting submission was accepted")

    path = "/runs/" + created["run_id"]
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        _, run, _, _ = request(path)
        if run["state"] in {"FAILED", "CANCELLED"}:
            raise RuntimeError(f"Run {run['state']}: {run['failure']}")
        if run["state"] == "SUCCEEDED":
            break
        time.sleep(0.2)
    else:
        raise RuntimeError("Workflow did not finish before the smoke-test timeout")

    _, artifact, headers, raw = request(path + "/artifacts/" + run["final_artifact_id"])
    if headers.get("ETag") != '"' + hashlib.sha256(raw).hexdigest() + '"':
        raise RuntimeError("Artifact hash does not match its ETag")
    if "Unresolved conflict" not in artifact["report_markdown"]:
        raise RuntimeError("Report omitted the fixture contradiction")
    _, history, _, _ = request(path + "/events?limit=1000")
    if history["events"][-1]["kind"] != "run.succeeded":
        raise RuntimeError("Missing terminal event")
    if args.check_parallelism:
        roots = [a for a in run["attempts"] if a["task_id"] in {"research-a", "research-b"} and a["state"] == "SUCCEEDED"]
        if len(roots) != 2 or max(a["started_at"] for a in roots) >= min(a["finished_at"] for a in roots):
            raise RuntimeError("Research tasks did not overlap; start two workers with a nonzero fake delay")
        review = next(a for a in run["attempts"] if a["task_id"] == "review" and a["state"] == "SUCCEEDED")
        if review["started_at"] < max(a["finished_at"] for a in roots):
            raise RuntimeError("Evidence review ran before its dependencies finished")
    if args.check_cancellation:
        _, cancellation, _, _ = request("/runs", dict(submission, idempotency_key="cancel-" + uuid4().hex))
        cancel_path = "/runs/" + cancellation["run_id"]
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            _, active, _, _ = request(cancel_path)
            if any(t["state"] == "RUNNING" for t in active["tasks"]):
                break
            if active["state"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                raise RuntimeError("Run finished before cancellation; use a nonzero fake delay")
            time.sleep(0.05)
        else:
            raise RuntimeError("No active task appeared for cancellation")
        request(cancel_path + "/cancel", {})
        while time.monotonic() < deadline:
            _, stopped, _, _ = request(cancel_path)
            if stopped["state"] == "CANCELLED":
                break
            time.sleep(0.1)
        else:
            raise RuntimeError("Cancellation did not finish before timeout")
        if stopped["final_artifact_id"] or any(a["started_at"] > stopped["cancel_requested_at"] for a in stopped["attempts"]):
            raise RuntimeError("Cancelled run published a final artifact or admitted new work")
    print(json.dumps({"run_id": run["id"], "state": run["state"],
                      "attempts": len(run["attempts"]), "events": len(history["events"]),
                      "parallelism_checked": args.check_parallelism, "cancellation_checked": args.check_cancellation,
                      "artifact_sha256": hashlib.sha256(raw).hexdigest()}))


if __name__ == "__main__":
    main()
