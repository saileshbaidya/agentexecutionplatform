"""Explicit environment configuration; Compose, not Python, reads .env."""

from dataclasses import dataclass
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

    def __post_init__(self):
        if not self.api_key or not self.database_url:
            raise ValueError("DATABASE_URL and AEP_API_KEY must be set")
        if (self.lease_seconds < 1 or not math.isfinite(self.poll_seconds)
                or self.poll_seconds <= 0 or not math.isfinite(self.fake_delay_seconds)
                or self.fake_delay_seconds < 0):
            raise ValueError("Invalid worker timing settings")

    @classmethod
    def from_env(cls):
        return cls(
            database_url=os.environ.get("DATABASE_URL", ""),
            artifact_root=Path(os.environ.get("ARTIFACT_ROOT", "artifacts")).resolve(),
            api_key=os.environ.get("AEP_API_KEY", ""),
            lease_seconds=int(os.environ.get("AEP_LEASE_SECONDS", "15")),
            poll_seconds=float(os.environ.get("AEP_POLL_SECONDS", "1")),
            fake_delay_seconds=float(os.environ.get("AEP_FAKE_DELAY_SECONDS", "0")),
        )
