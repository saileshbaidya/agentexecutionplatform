"""Schema and semantic validation; no execution or provider side effects."""

import json
from pathlib import Path

from jsonschema import Draft202012Validator


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "contracts/v1/platform.schema.json"


def validate_plan(plan: dict) -> None:
    """Raise ValueError for schema or DAG violations. Do not mutate the plan."""
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    errors = sorted(Draft202012Validator(schema).iter_errors(plan), key=lambda e: str(e.path))
    if errors:
        raise ValueError(f"Invalid plan schema: {errors[0].message}")

    tasks = {task["task_id"]: task for task in plan["tasks"]}
    if len(tasks) != len(plan["tasks"]):
        raise ValueError("Duplicate task IDs")
    for task_id, task in tasks.items():
        deps = set(task["depends_on"])
        if task_id in deps:
            raise ValueError(f"Self dependency: {task_id}")
        if deps - tasks.keys():
            raise ValueError(f"Unknown dependencies for {task_id}")
        retry = task["retry"]
        if retry["initial_backoff_seconds"] > retry["max_backoff_seconds"]:
            raise ValueError(f"Invalid retry backoff for {task_id}")

    ancestors = {}
    pending = set(tasks)
    while pending:
        ready = {key for key in pending if set(tasks[key]["depends_on"]) <= ancestors.keys()}
        if not ready:
            raise ValueError("Dependency cycle")
        for key in ready:
            deps = set(tasks[key]["depends_on"])
            ancestors[key] = deps.union(*(ancestors[dep] for dep in deps))
        pending -= ready

    non_sinks = {dep for task in tasks.values() for dep in task["depends_on"]}
    sinks = tasks.keys() - non_sinks
    writers = {key for key, task in tasks.items() if task["kind"] == "report_writer"}
    reviewers = {key for key, task in tasks.items() if task["kind"] == "evidence_review"}
    if len(sinks) != 1 or sinks != writers:
        raise ValueError("Plan requires exactly one report writer as its only sink")
    if len(reviewers) != 1:
        raise ValueError("Plan requires exactly one evidence reviewer")
    writer = next(iter(writers))
    reviewer = next(iter(reviewers))
    if reviewer not in tasks[writer]["depends_on"]:
        raise ValueError("Report writer must directly depend on evidence review")
    researchers = {key for key, task in tasks.items() if task["kind"] == "research"}
    if not researchers <= ancestors[reviewer]:
        raise ValueError("Evidence review must follow all research tasks")
