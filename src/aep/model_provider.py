"""Budgeted recorded/OpenAI Responses adapters, without automatic POST retries.

Only the fixed OpenAI HTTPS origin is enabled. Unknown creates without a response
ID remain liabilities; they are never inferred to be free or silently repeated.
"""

import hashlib
import json
import re

import httpx
from psycopg.types.json import Jsonb

from .db import connect
from .execution import Conflict, LeaseLost, NotFound, canonical
from .providers import OutcomeUnknown, usage_cost


ORIGIN = "https://api.openai.com/v1"
RESPONSE_ID = re.compile(r"^resp_[A-Za-z0-9_-]{1,200}$")


class ModelCallError(ValueError):
    pass


def make_client(settings):
    return httpx.Client(timeout=settings.model_timeout_seconds, follow_redirects=False,
                        headers={"Authorization": "Bearer " + settings.openai_api_key, "Content-Type": "application/json"})


class ModelGateway:
    def __init__(self, engine):
        self.engine = engine

    def call(self, claim, call_key, role, prompt_version, payload, profile):
        from .agents import BOUNDARY, PROMPTS, output_schema
        from .budgets import Budgets
        body = {"model": profile["model"], "instructions": PROMPTS[role] + " " + BOUNDARY,
                "input": canonical(payload).decode(),
                "text": {"format": {"type": "json_schema", "name": role + "_v1", "strict": True, "schema": output_schema(role)}}}
        seed = {"body": body, "profile": profile, "prompt_version": prompt_version, "call_key": call_key}
        seed_hash = hashlib.sha256(canonical(seed)).hexdigest()
        budgets = Budgets(self.engine)
        # Cached requests avoid even repeating preflight. Attempt IDs and worker
        # IDs are intentionally absent from logical request identity.
        with connect(self.engine.settings.database_url) as conn:
            self.engine._lease(conn, claim)
            previous = conn.execute("SELECT * FROM operations WHERE run_id=%s AND task_id=%s AND call_key=%s",
                                    (claim["run_id"], claim["task_id"], call_key)).fetchone()
            if previous and previous["request"].get("seed_hash") != seed_hash:
                raise Conflict("Agent call key reused with changed prompt, context, or model profile")
        if previous:
            return budgets.execute_reserved(claim, previous)
        if profile["backend"] == "openai":
            if not self.engine.settings.enable_openai:
                raise ModelCallError("openai_backend_disabled")
            try:
                with make_client(self.engine.settings) as client:
                    response = client.post(ORIGIN + "/responses/input_tokens", json=body)
                    response.raise_for_status()
                    count = response.json()["input_tokens"]
            except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
                raise ModelCallError("model_preflight_unavailable") from exc
        else:
            count = len(canonical(body))
        if type(count) is not int or count < 0 or count > profile["max_input_tokens"]:
            raise ModelCallError("agent_context_limit_exceeded")
        provider = "openai-responses-v1" if profile["backend"] == "openai" else "recorded-model-v1"
        quote = {"provider": provider, "model": profile["model"], "price_version": profile["price_version"], "currency": "USD",
                 "rate_denominator": 1000000, "input_rate_micro_usd": profile["input_rate"],
                 "cached_input_rate_micro_usd": profile["cached_input_rate"], "output_rate_micro_usd": profile["output_rate"],
                 "max_input_tokens": profile["max_input_tokens"], "max_output_tokens": profile["max_output_tokens"]}
        quote["bound_micro_usd"] = usage_cost(quote, quote["max_input_tokens"], quote["max_output_tokens"])
        if quote["bound_micro_usd"] > profile["max_call_cost_micro_usd"]:
            raise ModelCallError("model_call_cost_limit_exceeded")
        request = {"provider": provider, "call_key": call_key, "seed_hash": seed_hash, "body": body,
                   "counted_input_tokens": count, "role": role, "prompt_version": prompt_version,
                   "max_output_tokens": profile["max_output_tokens"], "task_id": claim["task_id"]}
        operation = budgets._reserve_prepared(claim, request, quote)
        return budgets.execute_reserved(claim, operation)


class ModelProvider:
    def __init__(self, engine):
        self.engine = engine
        self.dsn = engine.settings.database_url

    @staticmethod
    def evidence(conn, operation):
        found = conn.execute("SELECT receipt FROM model_receipts WHERE operation_id=%s", (operation["id"],)).fetchone()
        return found["receipt"] if found else None

    def execute(self, operation_id, dispatch_token, claim):
        with connect(self.dsn) as conn:
            self.engine._lease(conn, claim)
            operation = conn.execute("SELECT * FROM operations WHERE id=%s AND run_id=%s AND task_id=%s",
                                     (operation_id, claim["run_id"], claim["task_id"])).fetchone()
            if (not operation or operation["state"] != "DISPATCHED" or str(operation["dispatch_token"]) != str(dispatch_token)
                    or str(operation["attempt_id"]) != claim["attempt_id"]):
                raise LeaseLost("Model dispatch no longer authorized")
        if operation["request"]["provider"] == "recorded-model-v1":
            raw = self._recorded(operation)
            return self._record(operation, raw)
        if not self.engine.settings.enable_openai:
            raise OutcomeUnknown("OpenAI backend is disabled")
        body = dict(operation["request"]["body"], background=True, store=True, truncation="disabled", service_tier="default",
                    max_output_tokens=operation["request"]["max_output_tokens"],
                    metadata={"operation_id": str(operation_id), "request_hash": operation["request_hash"]})
        try:
            with make_client(self.engine.settings) as client:
                response = client.post(ORIGIN + "/responses", json=body)
        except httpx.HTTPError:
            raise OutcomeUnknown("Model response not received") from None
        if response.status_code in {400, 401, 403, 404, 422}:
            receipt = {"provider_request_id": response.headers.get("x-request-id", "rejected-" + str(operation_id)),
                       "request_hash": operation["request_hash"], "outcome": "NOT_ACCEPTED", "input_tokens": 0,
                       "output_tokens": 0, "cached_input_tokens": 0, "cost_micro_usd": 0,
                       "result": {"text": "", "status": "rejected", "http_status": response.status_code}}
            return self._save(operation, receipt, {"http_status": response.status_code})
        if response.status_code != 200:
            raise OutcomeUnknown("Model response outcome is uncertain")
        try:
            raw = response.json()
            response_id = raw["id"]
            self._bind(operation, response_id)
            receipt = self._record(operation, raw)
        except (ValueError, KeyError, TypeError):
            raise OutcomeUnknown("Unusable model response") from None
        if receipt is None:
            raise OutcomeUnknown("Model response is still running or usage is unavailable")
        return receipt

    def _bind(self, operation, response_id):
        if not isinstance(response_id, str) or not RESPONSE_ID.fullmatch(response_id):
            raise ValueError("Invalid provider response ID")
        with connect(self.dsn) as conn:
            conn.execute("SELECT id FROM runs WHERE id=%s FOR UPDATE", (operation["run_id"],))
            current = conn.execute("SELECT provider_response_id FROM operations WHERE id=%s", (operation["id"],)).fetchone()
            if current["provider_response_id"] not in {None, response_id}:
                raise Conflict("Operation already associated with another provider response")
            conn.execute("UPDATE operations SET provider_response_id=%s WHERE id=%s", (response_id, operation["id"]))

    def _record(self, operation, raw):
        if not isinstance(raw, dict) or not isinstance(raw.get("metadata", {}), dict):
            raise ValueError("Invalid response object")
        if raw.get("status") in {"queued", "in_progress"}:
            return None
        if raw.get("status") not in {"completed", "failed", "cancelled", "incomplete"}:
            raise ValueError("Unknown response status")
        if (raw.get("metadata", {}).get("operation_id") != str(operation["id"])
                or raw.get("metadata", {}).get("request_hash") != operation["request_hash"]
                or raw.get("model") != operation["request"]["body"]["model"]):
            raise ValueError("Response identity or model mismatch")
        usage = raw.get("usage")
        if not isinstance(usage, dict):
            return None  # No assertion of zero charge without usage evidence.
        input_tokens, output_tokens = usage.get("input_tokens"), usage.get("output_tokens")
        cached = (usage.get("input_tokens_details") or {}).get("cached_tokens", 0)
        if any(type(n) is not int or n < 0 for n in (input_tokens, output_tokens, cached)) or cached > input_tokens:
            raise ValueError("Invalid usage evidence")
        chunks, refused = [], False
        for item in raw.get("output", []):
            if item.get("type") not in {"message", "reasoning"}:
                refused = True  # No tool execution is allowed in this adapter.
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    chunks.append(content.get("text", ""))
                elif content.get("type") == "refusal":
                    refused = True
        text = "".join(chunks)
        if len(text.encode()) > 200000:
            text, refused = "", True
            raw = {key: raw[key] for key in ("id", "status", "model", "metadata", "usage")}
            raw["output_omitted"] = True
        receipt = {"provider_request_id": raw["id"], "request_hash": operation["request_hash"],
                   "outcome": "SUCCEEDED" if raw["status"] == "completed" and not refused else "FAILED",
                   "input_tokens": input_tokens, "output_tokens": output_tokens, "cached_input_tokens": cached,
                   "cost_micro_usd": usage_cost(operation["quote"], input_tokens, output_tokens, cached),
                   "result": {"text": text, "status": raw["status"], "model": raw["model"]}}
        return self._save(operation, receipt, raw)

    def _save(self, operation, receipt, raw):
        with connect(self.dsn) as conn:
            conn.execute("SELECT id FROM runs WHERE id=%s FOR UPDATE", (operation["run_id"],))
            conn.execute("""INSERT INTO model_receipts(operation_id,request_hash,receipt,raw_response)
                VALUES (%s,%s,%s,%s) ON CONFLICT(operation_id) DO NOTHING""",
                (operation["id"], operation["request_hash"], Jsonb(receipt), Jsonb(raw)))
            return self.evidence(conn, operation)

    def refresh(self, operation):
        with connect(self.dsn) as conn:
            if self.evidence(conn, operation):
                return
        if operation["request"]["provider"] == "recorded-model-v1":
            self._record(operation, self._recorded(operation))
            return
        if not operation["provider_response_id"]:
            return
        if not self.engine.settings.enable_openai:
            return
        try:
            with make_client(self.engine.settings) as client:
                response = client.get(ORIGIN + "/responses/" + operation["provider_response_id"],
                                      timeout=min(5, self.engine.settings.model_timeout_seconds))
            if response.status_code == 200:
                raw = response.json()
                if raw.get("id") == operation["provider_response_id"]:
                    self._record(operation, raw)
        except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError):
            return  # A failed GET/404 is not evidence of zero cost.

    def attach_response(self, run_id, operation_id, response_id, owner="local"):
        if not isinstance(response_id, str) or not RESPONSE_ID.fullmatch(response_id):
            raise ValueError("Invalid response ID")
        with connect(self.dsn) as conn:
            operation = conn.execute("SELECT o.* FROM operations o JOIN runs r ON r.id=o.run_id WHERE o.id=%s AND o.run_id=%s AND r.owner_id=%s",
                                     (operation_id, run_id, owner)).fetchone()
            if not operation:
                raise NotFound("Operation not found")
        if operation["request"]["provider"] != "openai-responses-v1" or not self.engine.settings.enable_openai:
            raise Conflict("Response attachment requires an enabled OpenAI operation")
        if operation["state"] not in {"DISPATCHED", "UNKNOWN"}:
            if operation["provider_response_id"] == response_id:
                return
            raise Conflict("Only uncertain dispatched operations can be associated with a response")
        try:
            with make_client(self.engine.settings) as client:
                response = client.get(ORIGIN + "/responses/" + response_id)
                response.raise_for_status()
                raw = response.json()
        except (httpx.HTTPError, ValueError):
            raise ValueError("Could not verify response") from None
        if (raw.get("id") != response_id or raw.get("metadata", {}).get("operation_id") != str(operation_id)
                or raw.get("metadata", {}).get("request_hash") != operation["request_hash"]
                or raw.get("model") != operation["request"]["body"]["model"]):
            raise Conflict("Response metadata does not match this operation")
        self._bind(operation, response_id)
        self._record(operation, raw)

    @staticmethod
    def _recorded(operation):
        from .agents import demo
        request = operation["request"]
        payload = json.loads(request["body"]["input"])
        context = payload["context"]
        key = request["task_id"] if request["role"] == "research" else request["role"]
        data = demo()["responses"][key]
        text = json.dumps(data, ensure_ascii=False)
        for index, finding in enumerate(context["findings"], 1):
            text = text.replace(f"$CLAIM_{index}", finding["claim_id"])
        count = len(text.encode())
        truncated = count > request["max_output_tokens"]
        return {"id": "resp_recorded_" + str(operation["id"]).replace("-", ""), "status": "incomplete" if truncated else "completed",
                "model": request["body"]["model"], "metadata": {"operation_id": str(operation["id"]), "request_hash": operation["request_hash"]},
                "usage": {"input_tokens": request["counted_input_tokens"], "output_tokens": min(count, request["max_output_tokens"]),
                          "input_tokens_details": {"cached_tokens": 0}},
                "output": [{"type": "message", "content": [{"type": "output_text", "text": "" if truncated else text}]}]}
