"""Server-owned tool policy. No action may select its sandbox or privileges."""

import re

from .contracts import validate_message

POLICY_VERSION = "isolated-tools-v1"
LIMITS = {"cpus": "0.5", "memory": "128m", "pids": 32, "seconds": 10,
          "output_bytes": 65536, "tmpfs_bytes": 16777216, "network": "none"}
TOOLS = {"text.stats": {"approval": False}, "python.run": {"approval": True}}
IMAGE = re.compile(r"^sha256:[a-f0-9]{64}$")


class ToolDenied(ValueError):
    pass


def validate_call(call):
    validate_message(call, "toolCall")
    args = call["arguments"]
    if call["tool"] == "text.stats":
        valid = set(args) == {"text"} and isinstance(args["text"], str) and len(args["text"].encode("utf-8")) <= 20000
    else:
        valid = (set(args) == {"code", "stdin"} and isinstance(args["code"], str)
                 and isinstance(args["stdin"], str) and 0 < len(args["code"].encode("utf-8")) <= 12000
                 and len(args["stdin"].encode("utf-8")) <= 20000)
    if not valid:
        raise ToolDenied("Invalid tool arguments")


def authorize(settings, spec, call):
    validate_call(call)
    if call["tool"] not in settings.tool_capabilities or call["tool"] not in spec["capabilities"]:
        raise ToolDenied("Tool capability denied")
    if not IMAGE.fullmatch(settings.tool_image):
        raise ToolDenied("An immutable sandbox image ID is required")


def snapshot(settings, owner, run_id, task_id, call):
    return {"owner": owner, "run_id": str(run_id), "task_id": task_id, **call,
            "policy_version": POLICY_VERSION, "image": settings.tool_image, "limits": LIMITS,
            "requires_approval": TOOLS[call["tool"]]["approval"]}
