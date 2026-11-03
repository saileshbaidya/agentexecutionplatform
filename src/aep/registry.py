"""Trusted versioned agent definitions. No model-generated executable code."""

AGENTS = {
    "agent-planner-v1": {"kind": "utility", "role": "planner", "prompt_version": "planner-v1"},
    "agent-research-v1": {"kind": "research", "role": "research", "prompt_version": "research-v1"},
    "agent-review-v1": {"kind": "evidence_review", "role": "review", "prompt_version": "review-v1"},
    "agent-writer-v1": {"kind": "report_writer", "role": "writer", "prompt_version": "writer-v1"},
}
