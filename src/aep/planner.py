"""A fixed, free planner for exercising execution, not interpreting requests."""


from .contracts import validate_plan
from .providers import quote_call
from .registry import AGENTS


HANDLERS = {
    "fake-research-v1": ("research", {"fixture.read"}),
    "fake-review-v1": ("evidence_review", set()),
    "fake-writer-v1": ("report_writer", set()),
    "fake-utility-v1": ("utility", set()),
    "tool-v1": ("utility", {"text.stats", "python.run"}),
}


def validate_executable_plan(plan, allow_agents=False):
    """Validate the graph and the deliberately restricted milestone 2 registry."""
    validate_plan(plan)
    for task in plan["tasks"]:
        if task["handler"] == "tool-v1":
            from .tool_policy import validate_call
            if "tool_call" not in task or "paid_call" in task:
                raise ValueError("Tool tasks require one tool_call and no paid_call")
            validate_call(task["tool_call"])
        elif "tool_call" in task:
            raise ValueError("Only the tool handler may request tools")
        if task["handler"] in AGENTS:
            if not allow_agents or task["kind"] != AGENTS[task["handler"]]["kind"]:
                raise ValueError("Agent tasks must be generated through the agent submission mode")
            if "agent_profile" not in task or "paid_call" in task or task["capabilities"] or task["input_artifacts"]:
                raise ValueError("Invalid agent task configuration")
            continue
        if "agent_profile" in task:
            raise ValueError("Agent profiles are server-owned")
        registered = HANDLERS.get(task["handler"])
        if not registered or task["kind"] != registered[0]:
            raise ValueError(f"Unknown handler or kind mismatch for {task['task_id']}")
        if set(task["capabilities"]) - registered[1]:
            raise ValueError(f"Unsupported capabilities for {task['task_id']}")
        if task["input_artifacts"]:
            raise ValueError("External input artifact ingestion is not implemented")
        if "paid_call" in task:
            quote_call(task["paid_call"])


def make_plan():
    def task(task_id, kind, handler, dependencies):
        return {
            "task_id": task_id, "kind": kind, "handler": handler,
            "instructions": "Run the deterministic execution fixture.",
            "depends_on": dependencies, "input_artifacts": [],
            "capabilities": ["fixture.read"] if kind == "research" else [],
            "timeout_seconds": 120, "context_token_limit": 4000,
            "retry": {"max_attempts": 3, "initial_backoff_seconds": 1, "max_backoff_seconds": 10},
        }
    return {"schema_version": "1", "plan_id": "deterministic-v1", "revision": 1, "tasks": [
        task("research-a", "research", "fake-research-v1", []),
        task("research-b", "research", "fake-research-v1", []),
        task("review", "evidence_review", "fake-review-v1", ["research-a", "research-b"]),
        task("report", "report_writer", "fake-writer-v1", ["research-a", "research-b", "review"]),
    ]}


def make_agent_plan(profile):
    """Bounded template topology; the leased planner supplies branch objectives.

    All model work, including planning, happens on workers outside transactions.
    Dynamic graph mutation remains outside the version 1 contract.
    """
    def task(task_id, handler, deps):
        return {"task_id": task_id, "kind": AGENTS[handler]["kind"], "handler": handler,
                "instructions": "Execute the versioned agent protocol over supplied evidence.",
                "depends_on": deps, "input_artifacts": [], "capabilities": [], "timeout_seconds": 600,
                "context_token_limit": profile["max_input_tokens"], "agent_profile": profile,
                "retry": {"max_attempts": 6, "initial_backoff_seconds": 1, "max_backoff_seconds": 10}}
    branches = [f"research-{index + 1}" for index in range(profile["research_branches"])]
    return {"schema_version": "1", "plan_id": "agent-dag-v1", "revision": 1, "tasks": [
        task("planner", "agent-planner-v1", []),
        *[task(branch, "agent-research-v1", ["planner"]) for branch in branches],
        task("review", "agent-review-v1", branches),
        task("report", "agent-writer-v1", ["review"]),
    ]}
