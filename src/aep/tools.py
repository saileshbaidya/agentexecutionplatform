"""Durable tool admission and exact-action human approval.

Workers propose actions; only the separate gateway dispatches containers.
Run locks serialize metadata, never container execution or approval waits.
"""

import hashlib
from datetime import timedelta
from uuid import uuid4

from psycopg.types.json import Jsonb

from .db import connect
from .execution import Conflict, NotFound, canonical, event
from .tool_policy import ToolDenied, authorize, snapshot

TERMINAL = {"SUCCEEDED", "FAILED", "DENIED", "EXPIRED", "CANCELLED", "UNKNOWN"}


class ToolPending(Exception):
    def __init__(self, action_id):
        self.action_id = action_id


class ToolGateway:
    def __init__(self, engine):
        self.engine = engine
        self.settings = engine.settings

    def call(self, claim):
        denied = None
        with connect(self.settings.database_url) as conn:
            lease = self.engine._lease(conn, claim)
            spec = lease["spec"]  # Never trust a worker-supplied spec or capability list.
            call = spec.get("tool_call")
            if spec["handler"] != "tool-v1" or not call:
                denied = "No authorized tool call on task"
            else:
                try:
                    authorize(self.settings, spec, call)
                except ToolDenied as exc:
                    denied = str(exc)
            if denied:
                event(conn, claim["run_id"], "tool.denied", task_id=claim["task_id"], reason=denied)
            else:
                run = conn.execute("SELECT * FROM runs WHERE id=%s", (claim["run_id"],)).fetchone()
                action = snapshot(self.settings, run["owner_id"], claim["run_id"], claim["task_id"], call)
                digest = hashlib.sha256(canonical(action)).hexdigest()
                row = conn.execute("SELECT * FROM tool_actions WHERE run_id=%s AND task_id=%s AND call_key=%s",
                                   (claim["run_id"], claim["task_id"], call["call_key"])).fetchone()
                if row and row["action_digest"] != digest:
                    raise Conflict("Tool call key already bound to another action or policy")
                if not row:
                    row = conn.execute("""INSERT INTO tool_actions
                        (id,run_id,task_id,call_key,action_digest,action,state,expires_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,LEAST(%s,%s,clock_timestamp()+interval '5 minutes')) RETURNING *""",
                        (uuid4(), claim["run_id"], claim["task_id"], call["call_key"], digest, Jsonb(action),
                         "PENDING" if action["requires_approval"] else "READY", run["deadline_at"],
                         lease["first_started_at"] + timedelta(seconds=spec["timeout_seconds"]))).fetchone()
                    event(conn, claim["run_id"], "tool.proposed", task_id=claim["task_id"], action_id=str(row["id"]),
                          action_digest=digest, requires_approval=action["requires_approval"])
                if row["state"] == "SUCCEEDED":
                    return {"tool_action_id": str(row["id"]), "action_digest": digest, "tool_result": row["result"]}
                if row["state"] in TERMINAL:
                    denied = "Tool action " + row["state"].lower()
        if denied:
            raise ToolDenied(denied)
        raise ToolPending(str(row["id"]))

    def park(self, claim, action_id):
        with connect(self.settings.database_url) as conn:
            self.engine._lease(conn, claim)
            if not conn.execute("SELECT 1 FROM tool_actions WHERE id=%s AND run_id=%s AND task_id=%s",
                                (action_id, claim["run_id"], claim["task_id"])).fetchone():
                raise NotFound("Tool action not found")
            conn.execute("UPDATE attempts SET state='TOOL_WAIT',finished_at=clock_timestamp() WHERE id=%s", (claim["attempt_id"],))
            conn.execute("UPDATE tasks SET state='TOOL_WAIT' WHERE run_id=%s AND id=%s", (claim["run_id"], claim["task_id"]))
            event(conn, claim["run_id"], "task.tool_wait", task_id=claim["task_id"], action_id=str(action_id))

    def history(self, run_id, owner="local", after=0, limit=100):
        with connect(self.settings.database_url) as conn:
            if not conn.execute("SELECT 1 FROM runs WHERE id=%s AND owner_id=%s", (run_id, owner)).fetchone():
                raise NotFound("Run not found")
            return conn.execute("""SELECT a.*,p.decision,p.approver,p.expires_at AS approval_expires_at,p.consumed_at
                FROM tool_actions a LEFT JOIN tool_approvals p ON p.action_id=a.id
                WHERE a.run_id=%s AND a.sequence>%s ORDER BY a.sequence LIMIT %s""", (run_id, after, limit)).fetchall()

    def decide(self, run_id, action_id, body, owner="local"):
        if (not isinstance(body, dict) or set(body) != {"action_digest", "decision", "ttl_seconds"}
                or not isinstance(body["decision"], str) or body["decision"] not in {"approve", "deny"} or type(body["ttl_seconds"]) is not int
                or not 1 <= body["ttl_seconds"] <= 300 or not isinstance(body["action_digest"], str)):
            raise ValueError("Expected action_digest, approve/deny decision, and ttl_seconds between 1 and 300")
        with connect(self.settings.database_url) as conn:
            run = conn.execute("SELECT * FROM runs WHERE id=%s AND owner_id=%s FOR UPDATE", (run_id, owner)).fetchone()
            if not run:
                raise NotFound("Run not found")
            action = conn.execute("SELECT * FROM tool_actions WHERE id=%s AND run_id=%s", (action_id, run_id)).fetchone()
            if not action:
                raise NotFound("Tool action not found")
            if action["action_digest"] != body["action_digest"]:
                raise Conflict("Approval digest does not match reviewed action")
            now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
            task = conn.execute("SELECT * FROM tasks WHERE run_id=%s AND id=%s", (run_id, action["task_id"])).fetchone()
            if (run["state"] != "RUNNING" or run["failure"] or run["deadline_at"] <= now
                    or action["expires_at"] <= now or task["state"] not in {"RUNNING", "TOOL_WAIT"}):
                raise Conflict("Action is no longer eligible for approval")
            previous = conn.execute("SELECT * FROM tool_approvals WHERE action_id=%s", (action_id,)).fetchone()
            if previous:
                if previous["decision"] != body["decision"]:
                    raise Conflict("Approval decision is immutable")
                return previous  # Replay never refreshes expiry or consumption.
            if action["state"] != "PENDING":
                raise Conflict("Action is not awaiting approval")
            approval = conn.execute("""INSERT INTO tool_approvals(action_id,action_digest,decision,approver,expires_at)
                VALUES (%s,%s,%s,%s,LEAST(%s,clock_timestamp()+(%s * interval '1 second'))) RETURNING *""",
                (action_id, body["action_digest"], body["decision"], owner, action["expires_at"], body["ttl_seconds"])).fetchone()
            conn.execute("UPDATE tool_actions SET state=%s WHERE id=%s",
                         ("READY" if body["decision"] == "approve" else "DENIED", action_id))
            event(conn, run_id, "tool.approval", actor="human:" + owner, task_id=action["task_id"],
                  action_id=str(action_id), action_digest=body["action_digest"], decision=body["decision"])
            return approval

    def dispatch(self, action_id):
        with connect(self.settings.database_url) as conn:
            candidate = conn.execute("SELECT run_id FROM tool_actions WHERE id=%s", (action_id,)).fetchone()
            if not candidate:
                return None
            run = conn.execute("SELECT * FROM runs WHERE id=%s FOR UPDATE", (candidate["run_id"],)).fetchone()
            row = conn.execute("SELECT * FROM tool_actions WHERE id=%s", (action_id,)).fetchone()
            if row["state"] != "READY":
                return None
            now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
            task = conn.execute("SELECT * FROM tasks WHERE run_id=%s AND id=%s", (run["id"], row["task_id"])).fetchone()
            if task["state"] == "RUNNING":
                return None  # Worker must park and relinquish its lease first.
            running = conn.execute("SELECT count(*) AS n FROM tool_actions WHERE run_id=%s AND state='RUNNING'", (run["id"],)).fetchone()["n"]
            if running >= run["max_concurrency"]:
                return None
            error = None
            try:
                spec = task["spec"]
                authorize(self.settings, spec, spec["tool_call"])
                expected = snapshot(self.settings, run["owner_id"], run["id"], task["id"], spec["tool_call"])
                if canonical(expected) != canonical(row["action"]) or hashlib.sha256(canonical(expected)).hexdigest() != row["action_digest"]:
                    error = "Tool action or policy changed"
            except (ValueError, KeyError):
                error = "Tool capability denied"
            if run["state"] != "RUNNING" or run["failure"] or task["state"] != "TOOL_WAIT":
                error = "Run or task stopped"
            if row["expires_at"] <= now or run["deadline_at"] <= now:
                error = "Tool action expired"
            if row["action"]["requires_approval"]:
                approval = conn.execute("SELECT * FROM tool_approvals WHERE action_id=%s", (action_id,)).fetchone()
                if (not approval or approval["decision"] != "approve" or approval["action_digest"] != row["action_digest"]
                        or approval["expires_at"] <= now or approval["consumed_at"]):
                    error = "Exact-action approval required or expired"
            if error:
                conn.execute("UPDATE tool_actions SET state='DENIED',error=%s,finished_at=clock_timestamp() WHERE id=%s", (error, action_id))
                event(conn, run["id"], "tool.denied", task_id=task["id"], action_id=str(action_id), reason=error)
                return None
            token = uuid4()
            if row["action"]["requires_approval"]:
                conn.execute("UPDATE tool_approvals SET consumed_at=clock_timestamp() WHERE action_id=%s", (action_id,))
            conn.execute("UPDATE tool_actions SET state='RUNNING',cleanup_required=true,dispatched_at=clock_timestamp(),dispatch_token=%s WHERE id=%s", (token, action_id))
            event(conn, run["id"], "tool.dispatched", task_id=task["id"], action_id=str(action_id), action_digest=row["action_digest"])
            return dict(row, state="RUNNING", dispatch_token=token)

    def active(self, row):
        with connect(self.settings.database_url) as conn:
            return bool(conn.execute("""SELECT 1 FROM tool_actions a JOIN runs r ON r.id=a.run_id
                JOIN tasks t ON t.run_id=a.run_id AND t.id=a.task_id
                WHERE a.id=%s AND a.dispatch_token=%s AND a.state='RUNNING'
                AND r.state='RUNNING' AND r.failure IS NULL AND r.deadline_at>clock_timestamp()
                AND t.state='TOOL_WAIT' AND a.expires_at>clock_timestamp()""", (row["id"], row["dispatch_token"])).fetchone())

    def finish(self, row, result, error=None):
        with connect(self.settings.database_url) as conn:
            run = conn.execute("SELECT * FROM runs WHERE id=%s FOR UPDATE", (row["run_id"],)).fetchone()
            current = conn.execute("SELECT * FROM tool_actions WHERE id=%s", (row["id"],)).fetchone()
            if current["state"] != "RUNNING" or current["dispatch_token"] != row["dispatch_token"]:
                return
            now = conn.execute("SELECT clock_timestamp() AS now").fetchone()["now"]
            state = "FAILED" if error else "SUCCEEDED"
            if run["state"] != "RUNNING" or run["failure"] or run["deadline_at"] <= now or current["expires_at"] <= now:
                state, result, error = "CANCELLED", None, "Action stopped before result publication"
            conn.execute("UPDATE tool_actions SET state=%s,result=%s,error=%s,finished_at=clock_timestamp() WHERE id=%s",
                         (state, Jsonb(result), error, row["id"]))
            event(conn, row["run_id"], "tool." + state.lower(), task_id=row["task_id"], action_id=str(row["id"]))
