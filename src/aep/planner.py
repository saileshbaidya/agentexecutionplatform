"""A fixed, free planner for exercising execution, not interpreting requests."""


HANDLERS = {"fake-research-v1", "fake-review-v1", "fake-writer-v1"}


def make_plan():
    def task(task_id, kind, handler, dependencies):
        return {
            "task_id": task_id, "kind": kind, "handler": handler,
            "instructions": "Run the deterministic milestone 1 fixture.",
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
