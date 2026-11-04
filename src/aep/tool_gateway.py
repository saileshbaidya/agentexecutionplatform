"""Separate trusted dispatcher. Only this process needs the Docker socket."""

import logging
import subprocess
import threading

import psycopg

from .db import connect
from .execution import Execution
from .tools import ToolGateway
from .tool_sandbox import DockerSandbox

log = logging.getLogger(__name__)


def run_gateway(settings, stop=None, once=False, sandbox=None):
    stop = stop or threading.Event()
    gateway = ToolGateway(Execution(settings))
    sandbox = sandbox or DockerSandbox()
    while not stop.is_set():
        try:
            with connect(settings.database_url) as conn:
                rows = conn.execute("""SELECT id,run_id,state FROM tool_actions
                    WHERE state='READY' OR (state='RUNNING' AND
                    dispatched_at < clock_timestamp()-interval '30 seconds') OR
                    (cleanup_required AND state NOT IN ('PENDING','READY','RUNNING')) ORDER BY sequence LIMIT 20""").fetchall()
            for row in rows:
                if stop.is_set():
                    break
                if row["state"] not in {"READY", "RUNNING"}:
                    if sandbox.cleanup(row["id"]):
                        with connect(settings.database_url) as conn:
                            conn.execute("SELECT id FROM runs WHERE id=%s FOR UPDATE", (row["run_id"],))
                            conn.execute("UPDATE tool_actions SET cleanup_required=false WHERE id=%s", (row["id"],))
                    continue
                if row["state"] == "RUNNING":
                    # A crash after dispatch is never permission to repeat code.
                    if not sandbox.cleanup(row["id"]):
                        continue  # Keep waiting until the old container is proven removed.
                    with connect(settings.database_url) as conn:
                        conn.execute("SELECT id FROM runs WHERE id=%s FOR UPDATE", (row["run_id"],))
                        changed = conn.execute("""UPDATE tool_actions SET state='UNKNOWN',error='gateway_interrupted',
                            finished_at=clock_timestamp(),cleanup_required=false WHERE id=%s AND state='RUNNING'
                            AND dispatched_at < clock_timestamp()-interval '30 seconds' RETURNING task_id""", (row["id"],)).fetchone()
                        if changed:
                            from .execution import event
                            event(conn, row["run_id"], "tool.unknown", task_id=changed["task_id"], action_id=str(row["id"]))
                    continue
                action = gateway.dispatch(row["id"])
                if action:
                    result, error = sandbox.execute(action, lambda: not stop.is_set() and gateway.active(action))
                    gateway.finish(action, result, error)
        except (psycopg.Error, OSError, TimeoutError, subprocess.SubprocessError):
            log.warning("tool_gateway.unavailable")
        if once:
            return
        stop.wait(settings.poll_seconds)
