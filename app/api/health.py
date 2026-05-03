from __future__ import annotations

import asyncio
import datetime as dt

from fastapi import APIRouter, Request
from pydantic import BaseModel

from ..ops_db import get_ops_db
from ..state_paths import arbitrage_db_path, matches_db_path

router = APIRouter()


class Health(BaseModel):
    ok: bool
    state_db_mtime: str | None
    last_run_id: int | None
    samples: int
    matches_db_mtime: str | None
    last_matches_received_at: str | None


@router.get("/health", response_model=Health)
async def health(request: Request) -> Health:
    p = arbitrage_db_path()
    mtime = None
    if p.exists():
        mtime = dt.datetime.fromtimestamp(p.stat().st_mtime, dt.UTC).isoformat()

    mp = matches_db_path()
    matches_mtime = None
    if mp.exists():
        matches_mtime = dt.datetime.fromtimestamp(mp.stat().st_mtime, dt.UTC).isoformat()

    ema = await asyncio.to_thread(get_ops_db().snapshot_ema)
    samples = int(ema.get("samples", 0))
    last_run_id = getattr(request.app.state, "last_scanner_run_id", None) or None
    last_matches = getattr(request.app.state, "last_matches_received_at", None)
    last_matches_str = None
    if last_matches is not None:
        last_matches_str = dt.datetime.fromtimestamp(last_matches, dt.UTC).isoformat()

    return Health(
        ok=True,
        state_db_mtime=mtime,
        last_run_id=last_run_id,
        samples=samples,
        matches_db_mtime=matches_mtime,
        last_matches_received_at=last_matches_str,
    )
