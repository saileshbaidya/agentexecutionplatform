"""A fixed, free planner for exercising execution, not interpreting requests."""


from .contracts import validate_plan


HANDLERS = {
    "fake-research-v1": ("research", {"fixture.read"}),
    "fake-review-v1": ("evidence_review", set()),
    "fake-writer-v1": ("report_writer", set()),
    "fake-utility-v1": ("utility", set()),
}


def validate_executable_plan(plan):
    """Validate the graph and the deliberately restricted milestone 2 registry."""
    validate_plan(plan)
    for task in plan["tasks"]:
        registered = HANDLERS.get(task["handler"])
        if not registered or task["kind"] != registered[0]:
            raise ValueError(f"Unknown handler or kind mismatch for {task['task_id']}")
        if set(task["capabilities"]) - registered[1]:
            raise ValueError(f"Unsupported capabilities for {task['task_id']}")
        if task["input_artifacts"]:
            raise ValueError("External input artifact ingestion is not implemented")


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
