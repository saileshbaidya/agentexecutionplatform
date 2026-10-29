"""Trusted deterministic fixtures; no network, subprocesses, or paid operations."""


def execute(claim):
    handler = claim["spec"]["handler"]
    if handler == "fake-research-v1":
        value = 10 if claim["task_id"] == "research-a" else 12
        return {"fixture": claim["task_id"], "claims": [{
            "claim_id": claim["task_id"] + "-claim",
            "statement": f"The fixture measured {value} units.",
        }]}
    findings = [finding for result in claim["inputs"].values() for finding in result["findings"]]
    if handler == "fake-review-v1":
        return {"review": {"unsupported_claim_ids": [], "conflicts": [{
            "claim_ids": [finding["claim_id"] for finding in findings],
            "status": "unresolved",
            "explanation": "Synthetic fixtures report 10 and 12 units; no evidence resolves the discrepancy.",
        }]}}
    if handler == "fake-writer-v1":
        review = claim["inputs"]["review"]["review"]
        lines = ["# Deterministic execution report", "", "This report demonstrates execution using synthetic fixtures.", ""]
        for finding in findings:
            source = finding["sources"][0]["artifact"]["artifact_id"]
            lines.append(f"- {finding['statement']} [claim: {finding['claim_id']}; artifact: {source}]")
        lines.extend(["", "Unresolved conflict: fixtures disagree between 10 and 12 units."])
        return {"report_markdown": "\n".join(lines), "review": review, "findings": findings}
    raise ValueError("Handler not registered")
