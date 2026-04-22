from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from pydantic import BaseModel, BeforeValidator, Field
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Bucket(BaseModel):
    name: str
    lo_bps: int
    hi_bps: int | None  # None = no upper bound

    def contains(self, bps: int) -> bool:
        if bps < self.lo_bps:
            return False
        if self.hi_bps is not None and bps >= self.hi_bps:
            return False
        return True


def _parse_buckets(raw: object) -> dict[str, Bucket]:
    if isinstance(raw, dict):
        return raw  # already parsed (e.g. default)
    if not isinstance(raw, str):
        raise TypeError(f"BUCKETS must be a string, got {type(raw).__name__}")
    out: dict[str, Bucket] = {}
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts = chunk.split(":")
        if len(parts) != 3:
            raise ValueError(f"bucket entry must be name:lo:hi, got {chunk!r}")
        name, lo, hi = parts
        name = name.strip()
        lo_i = int(lo)
        hi_i: int | None = int(hi) if hi.strip() else None
        if hi_i is not None and hi_i <= lo_i:
            raise ValueError(f"bucket {name!r}: hi ({hi_i}) must exceed lo ({lo_i})")
        out[name] = Bucket(name=name, lo_bps=lo_i, hi_bps=hi_i)
    if not out:
        raise ValueError("BUCKETS parsed to empty dict")
    return out


# NoDecode disables pydantic-settings' default "complex field" JSON-
# decode pass for env values, so our `BeforeValidator` sees the raw
# string `low:300:500,...` from .env / $BUCKETS instead of a json error.
BucketsField = Annotated[
    dict[str, Bucket], NoDecode, BeforeValidator(_parse_buckets),
]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"

    state_dir: str = "/state"
    ingest_token: str = "change-me"
    max_ingest_bytes: int = 32 * 1024 * 1024

    buckets: BucketsField = Field(default="low:300:500,mid:500:800,high:800:1500,moon:1500:")

    ema_alpha: float = 0.2
    ema_warmup_samples: int = 5
    timings_retention_rows: int = 10_000

    sse_ring_buffer_size: int = 1024
    sse_heartbeat_seconds: int = 15
    snapshot_cache_seconds: int = 2

    # copied from bookeeETL_opusTests/arbitrage/schema.py — keep in sync
    stale_seconds_live: int = 45
    stale_seconds_upcoming: int = 600

    schema_version: int = 1


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
