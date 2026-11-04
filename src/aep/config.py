"""Explicit environment configuration; Compose, not Python, reads .env."""

from dataclasses import dataclass, field
import math
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    database_url: str
    artifact_root: Path
    api_key: str
    lease_seconds: int = 15
    poll_seconds: float = 1.0
    fake_delay_seconds: float = 0.0
    run_concurrency_limit: int = 4
    worker_maintenance: bool = True
    enable_openai: bool = False
    openai_api_key: str = field(default="", repr=False)
    openai_model: str = ""
    model_price_version: str = ""
    model_input_rate: int = 0
    model_cached_input_rate: int | None = None
    model_output_rate: int = 0
    model_timeout_seconds: float = 30.0
    inline_reconciliation: bool = True
    tool_capabilities: tuple[str, ...] = ()
    tool_image: str = ""
    tool_approval_key: str = field(default="", repr=False)

    def __post_init__(self):
        if not self.api_key or not self.database_url:
            raise ValueError("DATABASE_URL and AEP_API_KEY must be set")
        if set(self.tool_capabilities) - {"text.stats", "python.run"}:
            raise ValueError("Unknown deployment tool capability")
        if self.tool_approval_key and (len(self.tool_approval_key) < 24 or self.tool_approval_key == self.api_key):
            raise ValueError("Tool approval key must be distinct and at least 24 characters")
        if self.tool_capabilities:
            import re
            if not re.fullmatch(r"sha256:[a-f0-9]{64}", self.tool_image):
                raise ValueError("Enabled tools require an immutable sandbox image ID")
        if self.openai_api_key and any(ord(c) < 33 or ord(c) > 126 for c in self.openai_api_key):
            raise ValueError("Invalid OpenAI API key format")
        if not 1 <= self.run_concurrency_limit <= 32:
            raise ValueError("Run concurrency limit must be between 1 and 32")
        if (self.lease_seconds < 1 or not math.isfinite(self.poll_seconds)
                or self.poll_seconds <= 0 or not math.isfinite(self.fake_delay_seconds)
                or self.fake_delay_seconds < 0):
            raise ValueError("Invalid worker timing settings")
        if not math.isfinite(self.model_timeout_seconds) or not 1 <= self.model_timeout_seconds <= 120:
            raise ValueError("Model HTTP timeout must be between 1 and 120 seconds")
        cached_rate = self.model_input_rate if self.model_cached_input_rate is None else self.model_cached_input_rate
        if any(type(rate) is not int or not 0 <= rate <= 9000000000000000 for rate in
               (self.model_input_rate, cached_rate, self.model_output_rate)):
            raise ValueError("Model rates must be nonnegative integer micro-USD per million tokens")
        if cached_rate > self.model_input_rate:
            raise ValueError("Cached input rate cannot exceed the uncached input rate")
        if self.enable_openai and (not self.openai_api_key or not self.openai_model or not self.model_price_version
                                  or self.model_input_rate <= 0 or self.model_output_rate <= 0):
            raise ValueError("Enabled OpenAI requires a key, model, price version, and positive input/output rates")

    @classmethod
    def from_env(cls):
        maintenance = os.environ.get("AEP_WORKER_MAINTENANCE", "true").lower()
        if maintenance not in {"true", "false"}:
            raise ValueError("AEP_WORKER_MAINTENANCE must be true or false")
        enable_openai = os.environ.get("AEP_ENABLE_OPENAI", "false").lower()
        if enable_openai not in {"true", "false"}:
            raise ValueError("AEP_ENABLE_OPENAI must be true or false")
        reconciliation = os.environ.get("AEP_INLINE_RECONCILIATION", "true").lower()
        if reconciliation not in {"true", "false"}:
            raise ValueError("AEP_INLINE_RECONCILIATION must be true or false")
        return cls(
            database_url=os.environ.get("DATABASE_URL", ""),
            artifact_root=Path(os.environ.get("ARTIFACT_ROOT", "artifacts")).resolve(),
            api_key=os.environ.get("AEP_API_KEY", ""),
            lease_seconds=int(os.environ.get("AEP_LEASE_SECONDS", "15")),
            poll_seconds=float(os.environ.get("AEP_POLL_SECONDS", "1")),
            fake_delay_seconds=float(os.environ.get("AEP_FAKE_DELAY_SECONDS", "0")),
            run_concurrency_limit=int(os.environ.get("AEP_RUN_CONCURRENCY_LIMIT", "4")),
            worker_maintenance=maintenance == "true",
            enable_openai=enable_openai == "true",
            openai_api_key=os.environ.get("OPENAI_API_KEY", ""),
            openai_model=os.environ.get("AEP_OPENAI_MODEL", ""),
            model_price_version=os.environ.get("AEP_MODEL_PRICE_VERSION", ""),
            model_input_rate=int(os.environ.get("AEP_MODEL_INPUT_RATE", "0")),
            model_cached_input_rate=int(os.environ["AEP_MODEL_CACHED_INPUT_RATE"]) if os.environ.get("AEP_MODEL_CACHED_INPUT_RATE") else None,
            model_output_rate=int(os.environ.get("AEP_MODEL_OUTPUT_RATE", "0")),
            model_timeout_seconds=float(os.environ.get("AEP_MODEL_HTTP_TIMEOUT", "30")),
            inline_reconciliation=reconciliation == "true",
            tool_capabilities=tuple(filter(None, os.environ.get("AEP_TOOL_CAPABILITIES", "").split(","))),
            tool_image=os.environ.get("AEP_TOOL_IMAGE", ""),
            tool_approval_key=os.environ.get("AEP_TOOL_APPROVAL_KEY", ""),
        )
