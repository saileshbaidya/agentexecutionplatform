"""Deterministic bounded evidence contexts, immutable snapshots, and offline replay."""

import copy
import hashlib
import json
from uuid import uuid4

from psycopg.types.json import Jsonb

from .db import connect
from .execution import Conflict, Gone, NotFound, canonical, event

RECIPE_VERSION = "evidence-prefix-v1"


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def select_context(raw, role, limit):
    """Select whole findings and source prefixes; never synthesize source text.

    This byte bound is independent of model tokens. The provider's exact token
    preflight remains authoritative for the complete prompt and repair payload.
    """
    from .agents import AgentOutputError
    context = copy.deepcopy(raw)
    documents = sorted(context.pop("documents"), key=lambda d: d["document_id"])
    findings = sorted(context.pop("findings"), key=lambda f: f["claim_id"])
    context["documents"] = [dict(d, text="", source_range=[0, 0]) for d in documents]
    context["findings"] = []
    mandatory = set()
    if role == "writer":
        review = context["review"]
        mandatory.update(review["unsupported_claim_ids"])
        mandatory.update(cid for c in review["conflicts"] for cid in c["claim_ids"])
        if not mandatory <= {f["claim_id"] for f in findings}:
            raise AgentOutputError("context_review_provenance_error")
    if len(canonical(context)) > limit:
        raise AgentOutputError("context_required_fields_exceed_limit")
    for finding in [f for f in findings if f["claim_id"] in mandatory]:
        context["findings"].append(finding)
    if len(canonical(context)) > limit:
        raise AgentOutputError("context_required_claims_exceed_limit")
    for finding in [f for f in findings if f["claim_id"] not in mandatory]:
        candidate = dict(context, findings=context["findings"] + [finding])
        if len(canonical(candidate)) <= limit - min(4096, limit // 4):
            context = candidate
    # Round-robin allocation gives each source an equal prefix allowance.
    remaining = max(0, limit - len(canonical(context)) - 128 * len(documents))
    for index, original in enumerate(documents):
        allowance = remaining // (len(documents) - index)
        low, high = 0, len(original["text"])
        while low < high:
            mid = (low + high + 1) // 2
            if len(canonical(original["text"][:mid])) <= allowance:
                low = mid
            else:
                high = mid - 1
        selected = context["documents"][index]
        selected.update(text=original["text"][:low], source_range=[0, low])
        remaining -= len(canonical(selected["text"]))
    if len(canonical(context)) > limit:
        raise AgentOutputError("context_limit_exceeded")
    selected_ids = [f["claim_id"] for f in context["findings"]]
    manifest = {"recipe_version": RECIPE_VERSION, "budget_unit": "canonical_utf8_bytes", "byte_limit": limit,
                "selected_bytes": len(canonical(context)), "selected_claim_ids": selected_ids,
                "omitted_claim_ids": [f["claim_id"] for f in findings if f["claim_id"] not in selected_ids],
                "sources": [{"document_id": d["document_id"], "artifact": d["artifact"],
                             "selected_range": s["source_range"], "original_characters": len(d["text"]),
                             "excerpt_digest": hashlib.sha256(s["text"].encode()).hexdigest(),
                             "omitted_ranges": [[len(s["text"]), len(d["text"])]] if len(s["text"]) < len(d["text"]) else []}
                            for d, s in zip(documents, context["documents"])]}
    return context, manifest


def verify_snapshot(row):
    from .agents import AgentOutputError
    if row["context"] is None:
        raise Gone("Context payload was removed by retention")
    if digest(row["context"]) != row["context_digest"] or digest(row["manifest"]) != row["manifest_digest"]:
        raise AgentOutputError("context_integrity_error")
    return row


class Contexts:
    def __init__(self, engine):
        self.engine = engine
        self.dsn = engine.settings.database_url

    def assemble(self, claim):
        from .agents import BOUNDARY, PROMPTS, context_for, output_schema, AgentOutputError
        from .registry import AGENTS
        with connect(self.dsn) as conn:
            lease = self.engine._lease(conn, claim)
            trusted = dict(claim, **lease["input_snapshot"])
            definition = AGENTS[trusted["spec"]["handler"]]
            prompt_digest = digest({"instructions": PROMPTS[definition["role"]] + " " + BOUNDARY,
                                    "schema": output_schema(definition["role"])})
            identity = digest({key: trusted[key] for key in ("spec", "inputs", "documents", "request")})
            old = conn.execute("SELECT * FROM context_snapshots WHERE run_id=%s AND task_id=%s", (claim["run_id"], claim["task_id"])).fetchone()
            if old:
                if old["input_digest"] != identity:
                    raise AgentOutputError("context_inputs_changed")
                verify_snapshot(old)
                if old["manifest"]["prompt_digest"] != prompt_digest:
                    raise AgentOutputError("context_prompt_version_changed")
                return verify_snapshot(old)
            definition = AGENTS[trusted["spec"]["handler"]]
            raw = context_for(trusted)
            selected, manifest = select_context(raw, definition["role"], trusted["spec"]["agent_profile"].get("context_byte_limit", 48000))
            all_ids = {f["claim_id"] for result in trusted["inputs"].values() for f in result["findings"]}
            manifest["omitted_claim_ids"] = sorted(all_ids - set(manifest["selected_claim_ids"]))
            manifest.update(request_digest=hashlib.sha256(trusted["request"].encode()).hexdigest(),
                            origin_attempt_id=claim["attempt_id"],
                            excluded_by_assignment=sorted(d["document_id"] for d in trusted["documents"] if d["document_id"] not in {s["document_id"] for s in raw["documents"]}),
                            profile_digest=digest(trusted["spec"]["agent_profile"]),
                            prompt_version=definition["prompt_version"],
                            prompt_digest=prompt_digest,
                            upstream_artifacts=[a for key in sorted(trusted["inputs"]) for a in trusted["inputs"][key]["artifacts"]])
            row = conn.execute("""INSERT INTO context_snapshots
                (id,run_id,task_id,recipe_version,input_digest,context_digest,manifest_digest,context,manifest)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
                (uuid4(), claim["run_id"], claim["task_id"], RECIPE_VERSION, identity, digest(selected), digest(manifest),
                 Jsonb(selected), Jsonb(manifest))).fetchone()
            event(conn, claim["run_id"], "context.assembled", task_id=claim["task_id"], snapshot_id=str(row["id"]),
                  context_digest=row["context_digest"], manifest_digest=row["manifest_digest"],
                  selected_bytes=manifest["selected_bytes"], omitted_claims=len(manifest["omitted_claim_ids"]))
            return row

    def get(self, run_id, task_id, owner="local"):
        with connect(self.dsn) as conn:
            if not conn.execute("SELECT 1 FROM runs WHERE id=%s AND owner_id=%s", (run_id, owner)).fetchone():
                raise NotFound("Run not found")
            row = conn.execute("SELECT * FROM context_snapshots WHERE run_id=%s AND task_id=%s", (run_id, task_id)).fetchone()
            if not row:
                raise NotFound("Context not found")
            return verify_snapshot(row)

    def provenance(self, run_id, owner="local"):
        with connect(self.dsn) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            run = conn.execute("SELECT id,input_hash,agent_profile,payload_purged_at FROM runs WHERE id=%s AND owner_id=%s", (run_id, owner)).fetchone()
            if not run:
                raise NotFound("Run not found")
            rows = conn.execute("SELECT * FROM context_snapshots WHERE run_id=%s ORDER BY task_id", (run_id,)).fetchall()
            for row in rows:
                if row["context"] is not None:
                    verify_snapshot(row)
                elif digest(row["manifest"]) != row["manifest_digest"]:
                    raise Conflict("Provenance manifest integrity error")
                del row["context"]
            run["contexts"] = rows
            run["artifacts"] = conn.execute("SELECT id,task_id,attempt_id,sha256,size_bytes FROM artifacts WHERE run_id=%s ORDER BY task_id,id", (run_id,)).fetchall()
            run["operations"] = conn.execute("SELECT id,task_id,call_key,request_hash,state,quote FROM operations WHERE run_id=%s ORDER BY sequence", (run_id,)).fetchall()
            return run

    def replay(self, run_id, owner="local"):
        """Validate saved outputs against saved inputs; no provider/tool dispatch."""
        from .agents import normalize, render_report, AgentOutputError
        from .registry import AGENTS
        from .budgets import accounting_evidence
        with connect(self.dsn) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            run = conn.execute("SELECT * FROM runs WHERE id=%s AND owner_id=%s", (run_id, owner)).fetchone()
            if not run:
                raise NotFound("Run not found")
            if run["payload_purged_at"]:
                raise Gone("Run payloads were removed by retention")
            if run["state"] != "SUCCEEDED" or not run["agent_profile"]:
                raise Conflict("Replay requires a successful agent run")
            documents = conn.execute("SELECT * FROM source_documents WHERE run_id=%s", (run_id,)).fetchall()
            for document in documents:
                if hashlib.sha256(document["content"].encode()).hexdigest() != document["sha256"]:
                    raise AgentOutputError("source_integrity_error")
            tasks = conn.execute("SELECT * FROM tasks WHERE run_id=%s ORDER BY id", (run_id,)).fetchall()
            checked = []
            for task in tasks:
                if task["spec"]["handler"] not in AGENTS:
                    continue
                snapshot = conn.execute("SELECT * FROM context_snapshots WHERE run_id=%s AND task_id=%s", (run_id, task["id"])).fetchone()
                if not snapshot:
                    raise Conflict("Missing context snapshot")
                verify_snapshot(snapshot)
                output = task["result"]["agent_output"]
                if not output["operation_ids"]:
                    raise Conflict("Missing model operation IDs")
                if output["context"]["digest"] != snapshot["context_digest"] or str(output["context"]["snapshot_id"]) != str(snapshot["id"]):
                    raise Conflict("Output context identity mismatch")
                try:
                    for operation_id in output["operation_ids"]:
                        operation = conn.execute("SELECT * FROM operations WHERE id=%s AND run_id=%s AND task_id=%s",
                                                 (operation_id, run_id, task["id"])).fetchone()
                        if not operation:
                            raise Conflict("Missing model operation")
                        receipt = conn.execute("SELECT receipt FROM model_receipts WHERE operation_id=%s", (operation_id,)).fetchone()
                        evidence = conn.execute("SELECT evidence FROM usage_ledger WHERE operation_id=%s AND kind='CHARGE'", (operation_id,)).fetchone()
                        body = operation["request"]["body"]
                        if (operation["state"] != "SUCCEEDED" or not receipt or not evidence or
                                operation["result"] != receipt["receipt"] or
                                evidence["evidence"] not in [operation["result"], accounting_evidence(operation["result"])] or
                                digest({"request": operation["request"], "quote": operation["quote"]}) != operation["request_hash"] or
                                digest({"instructions": body["instructions"], "schema": body["text"]["format"]["schema"]}) != snapshot["manifest"]["prompt_digest"] or
                                json.loads(body["input"])["context"] != snapshot["context"]):
                            raise Conflict("Model operation provenance mismatch")
                    role = AGENTS[task["spec"]["handler"]]["role"]
                    value = normalize(role, json.loads(operation["result"]["result"]["text"]), snapshot["context"])
                except (KeyError, TypeError, ValueError) as exc:
                    raise Conflict("Saved model data cannot be replayed") from exc
                if value != output["data"]:
                    raise Conflict("Replayed output differs from committed output")
                for artifact in task["result"]["artifacts"]:
                    metadata = conn.execute("SELECT * FROM artifacts WHERE id=%s AND run_id=%s", (artifact["artifact_id"], run_id)).fetchone()
                    if not metadata or metadata["sha256"] != artifact["sha256"]:
                        raise Conflict("Missing output artifact")
                    content = json.loads(self.engine.store.read(metadata))
                    if content["agent_output"] != output:
                        raise Conflict("Artifact output provenance mismatch")
                    if role == "writer" and (content["report_markdown"] != render_report(value, snapshot["context"], run_id)
                                             or content["findings"] != snapshot["context"]["findings"] or content["review"] != snapshot["context"]["review"]):
                        raise Conflict("Report does not match replayed evidence")
                checked.append(task["id"])
            return {"run_id": str(run_id), "replayed_tasks": checked, "provider_calls": 0, "tool_calls": 0}
