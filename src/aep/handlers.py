"""Trusted deterministic fixtures; no network, subprocesses, or paid operations."""

import hashlib


def execute(claim):
    handler = claim["spec"]["handler"]
    if handler == "fake-research-v1":
        value = 10 if claim["task_id"] == "research-a" else 12
        return {"fixture": claim["task_id"], "claims": [{
            "claim_id": "claim-" + hashlib.sha256(claim["task_id"].encode()).hexdigest(),
            "statement": f"The fixture measured {value} units.",
        }]}
    # Inputs include all ancestors, so a diamond can deliver the same claim by
    # multiple paths. Keep a single copy keyed by the run-scoped claim ID.
    findings = list({finding["claim_id"]: finding for result in claim["inputs"].values()
                     for finding in result["findings"]}.values())
    if handler == "fake-utility-v1":
        return {"fixture": "utility", "findings": findings}
    if handler == "fake-review-v1":
        conflicts = []
        if len({finding["statement"] for finding in findings}) > 1:
            conflicts.append({
            "claim_ids": [finding["claim_id"] for finding in findings],
            "status": "unresolved",
            "explanation": "Synthetic fixture claims disagree; no evidence resolves the discrepancy.",
            })
        return {"review": {"unsupported_claim_ids": [], "conflicts": conflicts}}
    if handler == "fake-writer-v1":
        review = next(result["review"] for result in claim["inputs"].values() if "review" in result)
        lines = ["# Deterministic execution report", "", "This report demonstrates execution using synthetic fixtures.", ""]
        for finding in findings:
            source = finding["sources"][0]["artifact"]["artifact_id"]
            lines.append(f"- {finding['statement']} [claim: {finding['claim_id']}; artifact: {source}]")
        if review["conflicts"]:
            lines.extend(["", "Unresolved conflict: " + review["conflicts"][0]["explanation"]])
        return {"report_markdown": "\n".join(lines), "review": review, "findings": findings}
    raise ValueError("Handler not registered")
