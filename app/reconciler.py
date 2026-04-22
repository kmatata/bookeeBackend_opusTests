from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

import aiosqlite

from .broadcaster import Broadcaster
from .bucket_state import BucketState
from .dedup import dedup
from .state_paths import arbitrage_db_path


@dataclass
class ReconcileResult:
    reconcile_ms: float
    scanner_run_id: int | None
    rows_read: int
    deduped: int
    total_upserts: int
    total_deletes: int


async def run_once(
    *,
    scanner_run_id: int | None,
    bucket_state: BucketState,
    broadcaster: Broadcaster,
) -> ReconcileResult:
    """Read ACTIVE rows from the freshly-swapped /state/arbitrage.db,
    dedup, diff against the in-memory bucket state, and publish the
    per-bucket delta events.

    Called from the ingest pipeline under its asyncio.Lock, so this
    function runs serially with no concurrent writers mutating the
    bucket state.
    """
    t0 = time.perf_counter()

    rows = await _read_active(arbitrage_db_path())
    deduped = dedup(rows)
    deltas = bucket_state.reconcile(deduped)

    total_up = 0
    total_del = 0
    for delta in deltas:
        for row in delta.upserts:
            broadcaster.publish(delta.bucket, "upsert", row)
            total_up += 1
        for opp_id in delta.deletes:
            broadcaster.publish(
                delta.bucket, "delete", {"opportunity_id": opp_id},
            )
            total_del += 1

    reconcile_ms = (time.perf_counter() - t0) * 1000.0
    return ReconcileResult(
        reconcile_ms=reconcile_ms,
        scanner_run_id=scanner_run_id,
        rows_read=len(rows),
        deduped=len(deduped),
        total_upserts=total_up,
        total_deletes=total_del,
    )


async def _read_active(db_path: Path) -> list[dict]:
    """Open the swapped-in file read-only and return its ACTIVE rows
    with the `legs_json` aggregate parsed into a real list.

    Opens fresh each call because each atomic swap replaces the inode
    underneath — holding the connection open would keep reading the
    previous file. The file is small (<1 MB) so open cost is in the
    hundreds of microseconds.
    """
    if not db_path.exists():
        return []

    uri = f"file:{db_path}?mode=ro"
    async with aiosqlite.connect(uri, uri=True) as conn:
        conn.row_factory = aiosqlite.Row
        # Defence: if the DB somehow arrived without arb_view (schema
        # drift), return empty rather than 500 the whole ingest.
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='view' AND name='arb_view'"
        )
        if not await cur.fetchone():
            return []

        cur = await conn.execute(
            "SELECT * FROM arb_view WHERE status = 'ACTIVE'"
        )
        rows = await cur.fetchall()

    out: list[dict] = []
    for r in rows:
        d = dict(r)
        legs_json = d.pop("legs_json", None) or "[]"
        legs = json.loads(legs_json)
        # `LEFT JOIN` yields a single null-filled leg when there are no
        # real legs; filter those out.
        d["legs"] = [l for l in legs if l.get("bookmaker") is not None]
        out.append(d)
    return out
