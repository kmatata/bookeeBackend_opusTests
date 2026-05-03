from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from .broadcaster import Broadcaster
from .bucket_state import BucketState
from .config import get_settings
from .ops_db import get_ops_db
from .snapshot import SnapshotCache
from .state_paths import ensure_dirs


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_dirs()
    get_ops_db()  # initialises ops.db schema on boot

    s = get_settings()
    app.state.bucket_state = BucketState(s.buckets)
    app.state.broadcaster = Broadcaster(
        bucket_names=list(s.buckets.keys()),
        ring_size=s.sse_ring_buffer_size,
    )
    # Broadcaster for the matches snapshot refresh stream (client-side arb).
    app.state.matches_broadcaster = Broadcaster(
        bucket_names=["matches"],
        ring_size=s.sse_ring_buffer_size,
    )
    app.state.snapshot_cache = SnapshotCache(
        ttl_seconds=float(s.snapshot_cache_seconds),
    )
    # Monotone counter: ingest handler rejects any X-Scanner-Run-Id ≤ this.
    app.state.last_scanner_run_id = 0
    app.state.last_matches_received_at = None
    yield
