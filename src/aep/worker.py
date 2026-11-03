"""A restartable worker with independent heartbeat transactions."""

import logging
import threading
import time
from uuid import uuid4

import psycopg

from .execution import Execution, LeaseLost
from .handlers import execute
from .budgets import BudgetDenied, Budgets, OperationPending, ProviderFailure
from .agents import AgentOutputError, execute_agent
from .model_provider import ModelCallError
from .registry import AGENTS


log = logging.getLogger(__name__)


def run_claim(engine, claim, stop):
    finished = threading.Event()
    lost = threading.Event()

    def heartbeat():
        while not finished.wait(engine.settings.lease_seconds / 3):
            try:
                engine.heartbeat(claim)
            except (LeaseLost, psycopg.Error):
                lost.set()
                return

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    try:
        log.info("attempt.claimed run=%s task=%s attempt=%s token=%s", claim["run_id"], claim["task_id"], claim["attempt_id"], claim["fencing_token"])
        deadline = time.monotonic() + engine.settings.fake_delay_seconds
        while time.monotonic() < deadline:
            if stop.wait(min(0.1, max(0, deadline - time.monotonic()))) or lost.is_set():
                raise LeaseLost("Worker stopped or lease lost")
        if lost.is_set() or stop.is_set():
            raise LeaseLost("Worker stopped or lease lost")
        paid_result = None
        if "paid_call" in claim["spec"]:
            paid_result = Budgets(engine).call(claim, claim["spec"]["paid_call"])
        output = (execute_agent(engine, claim, should_stop=lambda: lost.is_set() or stop.is_set())
                  if claim["spec"]["handler"] in AGENTS else execute(claim))
        if paid_result is not None:
            output["paid_operation"] = paid_result
        if lost.is_set() or stop.is_set():
            raise LeaseLost("Worker stopped or lease lost")
        engine.complete(claim, output)
        log.info("attempt.succeeded run=%s task=%s attempt=%s", claim["run_id"], claim["task_id"], claim["attempt_id"])
    except OperationPending as pending:
        log.info("attempt.waiting_for_operation run=%s operation=%s", claim["run_id"], pending.operation_id)
        try:
            engine.wait_for_operation(claim, pending.operation_id)
        except (LeaseLost, psycopg.Error):
            log.warning("attempt.wait_not_committed attempt=%s", claim["attempt_id"])
    except (BudgetDenied, ProviderFailure) as denied:
        log.info("attempt.paid_call_denied run=%s reason=%s", claim["run_id"], denied.code)
        try:
            engine.fail(claim, reason=denied.code, retryable=False)
        except (LeaseLost, psycopg.Error):
            log.warning("attempt.failure_not_committed attempt=%s", claim["attempt_id"])
    except LeaseLost:
        log.info("attempt.abandoned run=%s attempt=%s", claim["run_id"], claim["attempt_id"])
        try:
            engine.acknowledge_stop(claim)
        except psycopg.Error:
            log.warning("attempt.stop_not_committed attempt=%s", claim["attempt_id"])
    except psycopg.Error:
        # A commit can have an unknown outcome. Never record failure on a new
        # connection as if the old commit were known to have failed.
        log.warning("attempt.database_error run=%s attempt=%s", claim["run_id"], claim["attempt_id"])
    except (AgentOutputError, ModelCallError) as invalid:
        log.warning("attempt.agent_error run=%s reason=%s", claim["run_id"], str(invalid))
        try:
            engine.fail(claim, reason=str(invalid), retryable=False)
        except (LeaseLost, psycopg.Error):
            log.warning("attempt.failure_not_committed attempt=%s", claim["attempt_id"])
    except ValueError:
        log.warning("attempt.invalid_output run=%s attempt=%s", claim["run_id"], claim["attempt_id"])
        try:
            engine.fail(claim, reason="invalid_output", retryable=False)
        except (LeaseLost, psycopg.Error):
            log.warning("attempt.failure_not_committed attempt=%s", claim["attempt_id"])
    except Exception:
        log.exception("attempt.handler_error run=%s attempt=%s", claim["run_id"], claim["attempt_id"])
        try:
            engine.fail(claim)
        except (LeaseLost, psycopg.Error):
            log.warning("attempt.failure_not_committed attempt=%s", claim["attempt_id"])
    finally:
        finished.set()
        thread.join(timeout=6)


def run_worker(settings, stop=None, once=False, worker_id=None):
    stop = stop or threading.Event()
    engine = Execution(settings)
    worker_id = worker_id or str(uuid4())
    while not stop.is_set():
        try:
            if settings.worker_maintenance:
                engine.maintain()
            claim = engine.claim(worker_id)
            if claim:
                run_claim(engine, claim, stop)
        except psycopg.Error:
            log.warning("worker.database_unavailable worker=%s", worker_id)
        if once:
            return
        stop.wait(settings.poll_seconds)
