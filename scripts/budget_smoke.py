"""Exercise shared budgets against a running API using only synthetic charges."""

import argparse
import copy
import json
import os
from pathlib import Path
import time
from urllib.request import Request, urlopen
from uuid import uuid4


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--timeout", type=float, default=90)
    args = parser.parse_args()
    key = os.environ.get("AEP_API_KEY")
    if not key:
        parser.error("Set AEP_API_KEY")
    plan = json.loads((Path(__file__).resolve().parents[1] / "examples/paid-research-plan.json").read_text())

    def request(path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = Request(args.url.rstrip("/") + path, data=data,
                      headers={"X-API-Key": key, "Content-Type": "application/json"})
        with urlopen(req, timeout=10) as response:
            return json.loads(response.read())

    def wait(path, predicate):
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            value = request(path)
            if predicate(value):
                return value
            time.sleep(0.1)
        raise RuntimeError("Budget smoke test timed out at " + path)

    def submit(selected_plan, budget=120):
        created = request("/runs", {"schema_version": "1", "idempotency_key": "budget-smoke-" + uuid4().hex,
            "request": "Exercise synthetic paid operations", "budget_micro_usd": budget,
            "timeout_seconds": 120, "plan": selected_plan})
        return "/runs/" + created["run_id"]

    def terminal(path, expected):
        value = wait(path, lambda r: r["state"] in {"SUCCEEDED", "FAILED", "CANCELLED"})
        if value["state"] != expected:
            raise RuntimeError(f"Expected {expected}, got {value['state']}: {value['failure']}")
        return value

    request("/health/ready")
    paid_path = submit(plan)
    terminal(paid_path, "SUCCEEDED")
    operations = request(paid_path + "/operations")["operations"]
    for operation in operations:
        request(paid_path + f"/operations/{operation['id']}/reconcile", {})
    account = request(paid_path + "/budget")
    if account["settled_micro_usd"] != 80 or account["reserved_micro_usd"] != 0 or len(operations) != 2:
        raise RuntimeError("Paid workflow accounting mismatch")
    if len(request(paid_path + "/budget/ledger")["entries"]) != 2:
        raise RuntimeError("Duplicate or missing charges")

    unknown_plan = copy.deepcopy(plan)
    unknown_plan["tasks"][0]["paid_call"].update(mode="timeout_after_accept", reconcile_delay_seconds=3)
    unknown_plan["tasks"][1].pop("paid_call")
    unknown_path = submit(unknown_plan)
    recovered = terminal(unknown_path, "SUCCEEDED")
    if recovered["budget"]["settled_micro_usd"] != 40 or len(request(unknown_path + "/operations")["operations"]) != 1:
        raise RuntimeError("Uncertain operation was charged more than once")
    if sum(a["task_id"] == "research-a" for a in recovered["attempts"]) != 2:
        raise RuntimeError("Uncertain operation did not resume through reconciliation")

    cancel_plan = copy.deepcopy(unknown_plan)
    cancel_plan["tasks"][0]["paid_call"]["reconcile_delay_seconds"] = 10
    cancel_path = submit(cancel_plan)
    wait(cancel_path, lambda r: any(a["state"] == "RECONCILIATION" for a in r["attempts"]))
    request(cancel_path + "/cancel", {})
    terminal(cancel_path, "CANCELLED")
    if request(cancel_path + "/budget")["reserved_micro_usd"] != 60:
        raise RuntimeError("Cancellation prematurely released an uncertain liability")
    closed = wait(cancel_path + "/budget", lambda b: b["reserved_micro_usd"] == 0)
    if closed["settled_micro_usd"] != 40 or request(cancel_path)["state"] != "CANCELLED":
        raise RuntimeError("Terminal-run reconciliation failed")

    denied_path = submit(plan, budget=59)
    denied = terminal(denied_path, "FAILED")
    if denied["failure"] != "budget_exhausted" or request(denied_path + "/operations")["operations"]:
        raise RuntimeError("Insufficient funds did not prevent dispatch")
    print(json.dumps({"provider": "fake-v1", "paid_run": paid_path, "settled_micro_usd": 80,
                      "unknown_recovered_once": True, "cancelled_liability_settled": True,
                      "denied_before_dispatch": True}))


if __name__ == "__main__":
    main()
