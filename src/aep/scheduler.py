"""Restartable maintenance loop independent of worker execution."""

import logging
import threading

import psycopg

from .execution import Execution


log = logging.getLogger(__name__)


def run_scheduler(settings, stop=None, once=False):
    stop = stop or threading.Event()
    engine = Execution(settings)
    while not stop.is_set():
        try:
            engine.maintain()
        except psycopg.Error:
            log.warning("scheduler.database_unavailable")
        if once:
            return
        stop.wait(settings.poll_seconds)
