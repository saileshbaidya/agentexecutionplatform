"""A restartable worker with independent heartbeat transactions."""

import logging
import threading
import time
from uuid import uuid4

import psycopg

from .execution import Execution, LeaseLost
from .handlers import execute


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
        output = execute(claim)
        if lost.is_set() or stop.is_set():
            raise LeaseLost("Worker stopped or lease lost")
        engine.complete(claim, output)
        log.info("attempt.succeeded run=%s task=%s attempt=%s", claim["run_id"], claim["task_id"], claim["attempt_id"])
    except LeaseLost:
        log.info("attempt.abandoned run=%s attempt=%s", claim["run_id"], claim["attempt_id"])
    except psycopg.Error:
        # A commit can have an unknown outcome. Never record failure on a new
        # connection as if the old commit were known to have failed.
        log.warning("attempt.database_error run=%s attempt=%s", claim["run_id"], claim["attempt_id"])
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
            engine.maintain()
            claim = engine.claim(worker_id)
            if claim:
                run_claim(engine, claim, stop)
        except psycopg.Error:
            log.warning("worker.database_unavailable worker=%s", worker_id)
        if once:
            return
        stop.wait(settings.poll_seconds)
