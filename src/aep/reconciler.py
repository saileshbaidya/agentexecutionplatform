"""Accounting/provider polling independent of scheduling deadlines."""

import logging
import threading

import psycopg

from .budgets import Budgets
from .execution import Execution


log = logging.getLogger(__name__)


def run_reconciler(settings, stop=None, once=False):
    stop = stop or threading.Event()
    budgets = Budgets(Execution(settings))
    while not stop.is_set():
        try:
            budgets.reconcile_pending(limit=20)
        except psycopg.Error:
            log.warning("reconciler.database_unavailable")
        if once:
            return
        stop.wait(settings.poll_seconds)
