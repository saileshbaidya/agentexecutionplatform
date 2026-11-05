"""Explicit terminal-run payload retention with restartable file cleanup.

Financial ledger evidence, approval decisions, manifests, hashes, and evaluation
datasets remain audit records. This is not comprehensive personal-data erasure.
"""

from psycopg.types.json import Jsonb

from .db import connect
from .execution import Conflict, NotFound, event

TOMBSTONE = {"retained": "audit_metadata_only"}


class Retention:
    def __init__(self, engine):
        self.engine = engine
        self.dsn = engine.settings.database_url

    def candidates(self, days=30, owner="local", limit=100):
        if type(days) is not int or not 0 <= days <= 36500 or not 1 <= limit <= 100:
            raise ValueError("Invalid retention age or limit")
        with connect(self.dsn) as conn:
            return conn.execute("""SELECT id,state,updated_at FROM runs r WHERE owner_id=%s
                AND payload_purged_at IS NULL AND state IN ('SUCCEEDED','FAILED','CANCELLED')
                AND updated_at<=clock_timestamp()-(%s * interval '1 day')
                AND NOT EXISTS(SELECT 1 FROM operations o WHERE o.run_id=r.id AND o.state IN ('RESERVED','DISPATCHED','UNKNOWN'))
                AND NOT EXISTS(SELECT 1 FROM tool_actions a WHERE a.run_id=r.id AND (a.state IN ('PENDING','READY','RUNNING') OR a.cleanup_required))
                AND NOT EXISTS(SELECT 1 FROM evaluation_cases c JOIN evaluation_batches b ON b.id=c.batch_id
                    WHERE c.run_id=r.id AND (c.metrics IS NULL OR b.state='RUNNING'))
                ORDER BY updated_at,id LIMIT %s""", (owner, days, limit)).fetchall()

    def purge(self, run_id, owner="local"):
        with connect(self.dsn) as conn:
            run = conn.execute("SELECT * FROM runs WHERE id=%s AND owner_id=%s FOR UPDATE", (run_id, owner)).fetchone()
            if not run:
                raise NotFound("Run not found")
            if not run["payload_purged_at"]:
                pending = conn.execute("""SELECT 1 FROM operations WHERE run_id=%s AND state IN ('RESERVED','DISPATCHED','UNKNOWN')
                    UNION ALL SELECT 1 FROM tool_actions WHERE run_id=%s AND (state IN ('PENDING','READY','RUNNING') OR cleanup_required)
                    UNION ALL SELECT 1 FROM evaluation_cases c JOIN evaluation_batches b ON b.id=c.batch_id
                    WHERE c.run_id=%s AND (c.metrics IS NULL OR b.state='RUNNING') LIMIT 1""", (run_id, run_id, run_id)).fetchone()
                if run["state"] not in {"SUCCEEDED", "FAILED", "CANCELLED"} or pending:
                    raise Conflict("Retention requires terminal execution, settled operations, completed evaluations, and tool cleanup")
                keys = sorted({a["storage_key"] for a in conn.execute("SELECT storage_key FROM artifacts WHERE run_id=%s", (run_id,)).fetchall()})
                conn.execute("INSERT INTO retention_jobs(run_id,storage_keys,state) VALUES (%s,%s,'PENDING')", (run_id, Jsonb(keys)))
                conn.execute("UPDATE runs SET payload_purged_at=clock_timestamp(),submission=%s,plan=NULL WHERE id=%s", (Jsonb(TOMBSTONE), run_id))
                conn.execute("UPDATE attempts SET input_snapshot='{}' WHERE run_id=%s", (run_id,))
                conn.execute("UPDATE tasks SET result=NULL,spec=jsonb_build_object('handler',spec->'handler','kind',spec->'kind') WHERE run_id=%s", (run_id,))
                conn.execute("UPDATE source_documents SET content='',title='[retained metadata]' WHERE run_id=%s", (run_id,))
                conn.execute("UPDATE context_snapshots SET context=NULL WHERE run_id=%s", (run_id,))
                conn.execute("UPDATE operations SET request=jsonb_build_object('provider',request->'provider','retained','audit_metadata_only'),result=CASE WHEN result IS NULL THEN NULL ELSE result-'result' END WHERE run_id=%s", (run_id,))
                conn.execute("UPDATE model_receipts m SET raw_response=%s,receipt=receipt-'result' FROM operations o WHERE o.id=m.operation_id AND o.run_id=%s", (Jsonb(TOMBSTONE), run_id))
                conn.execute("UPDATE fake_provider_receipts f SET receipt=receipt-'result' FROM operations o WHERE o.id=f.operation_id AND o.run_id=%s", (run_id,))
                conn.execute("UPDATE tool_actions SET action=%s,result=NULL WHERE run_id=%s", (Jsonb(TOMBSTONE), run_id))
                event(conn, run_id, "retention.payloads_purged", actor="retention", artifact_files=len(keys))
        return self.cleanup(run_id, owner)

    def cleanup(self, run_id, owner="local"):
        with connect(self.dsn) as conn:
            job = conn.execute("SELECT j.* FROM retention_jobs j JOIN runs r ON r.id=j.run_id WHERE j.run_id=%s AND r.owner_id=%s", (run_id, owner)).fetchone()
            if not job:
                raise NotFound("Retention job not found")
        if job["state"] == "DONE":
            return {"run_id": str(run_id), "state": "DONE"}
        try:
            for key in job["storage_keys"]:
                self.engine.store.delete(run_id, key)
        except (OSError, ValueError):
            with connect(self.dsn) as conn:
                conn.execute("UPDATE retention_jobs SET error='artifact_cleanup_pending' WHERE run_id=%s", (run_id,))
            return {"run_id": str(run_id), "state": "PENDING"}
        with connect(self.dsn) as conn:
            conn.execute("SELECT id FROM runs WHERE id=%s FOR UPDATE", (run_id,))
            conn.execute("UPDATE retention_jobs SET state='DONE',error=NULL,finished_at=clock_timestamp() WHERE run_id=%s", (run_id,))
        return {"run_id": str(run_id), "state": "DONE"}

    def run(self, days=30, apply=False, owner="local"):
        rows = self.candidates(days, owner)
        result = {"dry_run": not apply, "eligible_run_ids": [str(r["id"]) for r in rows], "jobs": []}
        if apply:
            with connect(self.dsn) as conn:
                pending = conn.execute("SELECT j.run_id FROM retention_jobs j JOIN runs r ON r.id=j.run_id WHERE r.owner_id=%s AND j.state='PENDING' LIMIT 100", (owner,)).fetchall()
            result["jobs"].extend(self.cleanup(row["run_id"], owner) for row in pending)
            for row in rows:
                try:
                    result["jobs"].append(self.purge(row["id"], owner))
                except Conflict:
                    result["jobs"].append({"run_id": str(row["id"]), "state": "SKIPPED"})
        return result
