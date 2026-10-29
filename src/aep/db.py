"""Short transactions and serialized, checksummed schema migrations."""

import hashlib
from pathlib import Path

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row


def connect(dsn):
    options = conninfo_to_dict(dsn).get("options", "")
    return psycopg.connect(dsn, row_factory=dict_row, connect_timeout=5,
                          options=options + " -c statement_timeout=15000 -c lock_timeout=5000")


def migrate(dsn):
    with connect(dsn) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(741992001)")
        conn.execute("""CREATE TABLE IF NOT EXISTS schema_migrations (
            name text PRIMARY KEY, sha256 char(64) NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT clock_timestamp())""")
        for path in sorted((Path(__file__).parent / "migrations").glob("*.sql")):
            content = path.read_bytes()
            digest = hashlib.sha256(content).hexdigest()
            previous = conn.execute("SELECT sha256 FROM schema_migrations WHERE name=%s", (path.name,)).fetchone()
            if previous:
                if previous["sha256"] != digest:
                    raise RuntimeError(f"Applied migration changed: {path.name}")
                continue
            conn.execute(content.decode("utf-8"))
            conn.execute("INSERT INTO schema_migrations(name,sha256) VALUES (%s,%s)", (path.name, digest))
