from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, HTTPException, Request, status

from ..config import get_settings
from ..state_paths import arbitrage_db_path

router = APIRouter()


@router.get("/arbs/{bucket}")
async def arbs(bucket: str, request: Request) -> list[dict]:
    """Return the current deduped contents of a bucket as JSON.

    Convenience endpoint for curl inspection and non-browser consumers.
    The response shape mirrors what each SSE `upsert` event carries.
    """
    settings = get_settings()
    if bucket not in settings.buckets:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown bucket: {bucket}",
        )
    return request.app.state.bucket_state.snapshot(bucket)


@router.get("/meta")
async def meta() -> dict:
    """Protocol constants that clients need at startup.

    `buckets` maps bucket name → [lo_bps, hi_bps | null].
    `stale_seconds_*` mirror the ETL schema so clients compute
    `status` locally with the same math.
    """
    s = get_settings()
    return {
        "schema_version": s.schema_version,
        "buckets": {
            name: [b.lo_bps, b.hi_bps]
            for name, b in s.buckets.items()
        },
        "stale_seconds_live": s.stale_seconds_live,
        "stale_seconds_upcoming": s.stale_seconds_upcoming,
        "sse_heartbeat_seconds": s.sse_heartbeat_seconds,
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
    }


@router.get("/buckets")
async def buckets(request: Request) -> dict:
    """Live count of deduped opportunities per bucket."""
    return request.app.state.bucket_state.counts()


@router.get("/metrics")
async def metrics() -> dict:
    """EMA receive-timing stats + last-10 rows + 5s-hypothesis block.

    The `hypothesis_5s` block validates whether the observed inter-push
    cadence matches the expected ~5-second scanner cycle.
    """
    from ..ops_db import get_ops_db
    db = get_ops_db()
    ema_row = db.snapshot_ema()
    last_10 = db.snapshot_last(10)

    arb_mtime: str | None = None
    p = arbitrage_db_path()
    if p.exists():
        arb_mtime = dt.datetime.fromtimestamp(
            p.stat().st_mtime, dt.UTC,
        ).isoformat()

    samples = int(ema_row.get("samples", 0))
    inter_ema = ema_row.get("inter_arrival_ms_ema")
    last_gap = ema_row.get("last_gap_ms")

    return {
        "samples": samples,
        "alpha": ema_row.get("alpha"),
        "updated_at": ema_row.get("updated_at"),
        "state_db_mtime": arb_mtime,
        "ema": {
            "upload_ms": ema_row.get("upload_ms_ema"),
            "populate_ms": ema_row.get("populate_ms_ema"),
            "reconcile_ms": ema_row.get("reconcile_ms_ema"),
            "e2e_ms": ema_row.get("e2e_ms_ema"),
        },
        "last_10": last_10,
        "hypothesis_5s": {
            "expected_cadence_ms": 5000,
            "observed_inter_arrival_ms_ema": inter_ema,
            "last_gap_ms": last_gap,
        },
    }
