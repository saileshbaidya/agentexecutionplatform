"""Isolated PostgreSQL fixtures shared by integration suites."""

import os
from pathlib import Path
import tempfile
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from aep.config import Settings
from aep.db import connect, migrate
from aep.execution import Execution


class PostgresFixture:
    def setUp(self):
        self.admin_dsn = os.environ["TEST_DATABASE_URL"]
        self.schema = "aep_test_" + uuid4().hex
        with psycopg.connect(self.admin_dsn) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
        self.dsn = make_conninfo(self.admin_dsn, options=f"-csearch_path={self.schema}")
        self.temporary = tempfile.TemporaryDirectory()
        self.settings = Settings(self.dsn, Path(self.temporary.name), "test-key", lease_seconds=3, poll_seconds=0.05)
        migrate(self.dsn)
        self.engine = Execution(self.settings)

    def tearDown(self):
        self.temporary.cleanup()
        if not self.schema.startswith("aep_test_") or len(self.schema) != 41:
            raise RuntimeError("Refusing to drop unexpected schema")
        with psycopg.connect(self.admin_dsn) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    def submission(self, **changes):
        value = {"schema_version": "1", "idempotency_key": "test-request", "request": "Compare the synthetic evidence",
                 "budget_micro_usd": 0, "timeout_seconds": 600}
        value.update(changes)
        return value

    def start(self):
        run_id, _ = self.engine.submit(self.submission())
        self.engine.maintain()
        return run_id

    def expire(self, claim):
        with connect(self.dsn) as conn:
            conn.execute("UPDATE attempts SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=%s", (claim["attempt_id"],))

    def ready_retries(self):
        with connect(self.dsn) as conn:
            conn.execute("UPDATE tasks SET next_run_at=clock_timestamp()-interval '1 second' WHERE state='RETRY_WAIT'")
        self.engine.maintain()
