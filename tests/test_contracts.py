import copy
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from aep.contracts import SCHEMA_PATH, validate_message, validate_plan
from aep.planner import validate_executable_plan
from jsonschema import Draft202012Validator


class PlanContractTests(unittest.TestCase):
    def setUp(self):
        self.plan = json.loads((ROOT / "examples/research-plan.json").read_text())

    def test_schema_is_valid(self):
        Draft202012Validator.check_schema(json.loads(SCHEMA_PATH.read_text()))

    def test_example_is_valid_and_unchanged(self):
        original = copy.deepcopy(self.plan)
        validate_plan(self.plan)
        self.assertEqual(original, self.plan)

    def test_paid_example_is_valid_for_the_registered_provider(self):
        plan = json.loads((ROOT / "examples/paid-research-plan.json").read_text())
        validate_executable_plan(plan)

    def test_currency_and_usage_reject_float_coercion(self):
        message = {"schema_version": "1", "idempotency_key": "strict", "request": "test", "budget_micro_usd": 1.0, "timeout_seconds": 60}
        with self.assertRaises(ValueError):
            validate_message(message, "submission")
        plan = json.loads((ROOT / "examples/paid-research-plan.json").read_text())
        for field, value in (("max_cost_micro_usd", 60.0), ("output_tokens", 4.0)):
            with self.subTest(field=field):
                changed = copy.deepcopy(plan)
                changed["tasks"][0]["paid_call"][field] = value
                with self.assertRaises(ValueError):
                    validate_executable_plan(changed)

    def test_submission_and_result_contracts(self):
        schema = json.loads(SCHEMA_PATH.read_text())
        messages = {
            "submission": {"schema_version": "1", "idempotency_key": "request-1", "request": "Compare fixture evidence", "budget_micro_usd": 100000, "timeout_seconds": 600},
            "taskResult": {"schema_version": "1", "task_id": "review", "attempt_id": "attempt-1", "artifacts": [{"artifact_id": "review-output", "sha256": "a" * 64}], "findings": [], "review": {"conflicts": [], "unsupported_claim_ids": []}},
        }
        for name, message in messages.items():
            with self.subTest(contract=name):
                selected = dict(schema, **{"$ref": f"#/$defs/{name}"})
                validator = Draft202012Validator(selected)
                validator.validate(message)
                invalid = dict(message, unexpected=True)
                self.assertFalse(validator.is_valid(invalid))
        selected = dict(schema, **{"$ref": "#/$defs/submission"})
        invalid = dict(messages["submission"], budget_micro_usd=-1)
        self.assertFalse(Draft202012Validator(selected).is_valid(invalid))

    def test_invalid_graphs_are_rejected(self):
        mutations = {
            "unknown": lambda p: p["tasks"][0]["depends_on"].append("missing"),
            "self": lambda p: p["tasks"][0]["depends_on"].append("research-a"),
            "cycle": lambda p: p["tasks"][0]["depends_on"].append("report"),
            "duplicate": lambda p: p["tasks"].append(copy.deepcopy(p["tasks"][0])),
            "multiple sinks": lambda p: p["tasks"].append(dict(p["tasks"][0], task_id="orphan")),
            "wrong sink": lambda p: p["tasks"][-1].update(kind="utility"),
            "no review": lambda p: p["tasks"][2].update(kind="utility"),
            "review bypass": lambda p: p["tasks"][-1].update(depends_on=["research-a", "research-b"]),
            "unreviewed research": lambda p: p["tasks"][2].update(depends_on=["research-a"]),
            "retry bounds": lambda p: p["tasks"][0]["retry"].update(initial_backoff_seconds=100),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                plan = copy.deepcopy(self.plan)
                mutate(plan)
                with self.assertRaises(ValueError):
                    validate_plan(plan)

    def test_schema_rejects_bad_fields(self):
        for field, value in [("revision", 2), ("schema_version", "2"), ("unexpected", True), ("tasks", [])]:
            with self.subTest(field=field):
                self.plan[field] = value
                with self.assertRaises(ValueError):
                    validate_plan(self.plan)
                self.setUp()


if __name__ == "__main__":
    unittest.main()
