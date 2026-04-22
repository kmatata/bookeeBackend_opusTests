from __future__ import annotations

import asyncio
import gzip
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from .arbitrage_schema import ARBITRAGE_SCHEMA_SQL
from .bucket_state import BucketState
from .state_paths import arbitrage_db_path, snapshots_dir


@dataclass
class SnapshotArtifact:
    bucket: str
    cursor: int
    body: bytes  # gzip-compressed SQLite file
    row_count: int
    built_at: float  # monotonic timestamp


class SnapshotCache:
    """Per-bucket TTL cache around `_build_snapshot`.

    Rationale: clients boot by hitting `/snapshot/{bucket}` before
    opening the SSE stream, so a crowd of fresh subscribers would
    otherwise VACUUM INTO N times in parallel for the same cursor.
    Cache serves identical bytes while `cursor` is unchanged and the
    TTL hasn't expired; per-bucket `asyncio.Lock` coalesces concurrent
    misses so only one build runs at a time.
    """

    def __init__(self, ttl_seconds: float) -> None:
        self._ttl = ttl_seconds
        self._cache: dict[str, SnapshotArtifact] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    async def get(
        self,
        *,
        bucket: str,
        cursor: int,
        bucket_state: BucketState,
    ) -> SnapshotArtifact:
        hit = self._lookup_fresh(bucket, cursor)
        if hit is not None:
            return hit
        lock = self._locks.setdefault(bucket, asyncio.Lock())
        async with lock:
            hit = self._lookup_fresh(bucket, cursor)
            if hit is not None:
                return hit
            rows = bucket_state.snapshot(bucket)
            artifact = await asyncio.to_thread(
                _build_snapshot, bucket=bucket, cursor=cursor, rows=rows,
            )
            self._cache[bucket] = artifact
            return artifact

    def _lookup_fresh(self, bucket: str, cursor: int) -> SnapshotArtifact | None:
        hit = self._cache.get(bucket)
        if hit is None:
            return None
        if hit.cursor != cursor:
            return None
        if time.monotonic() - hit.built_at > self._ttl:
            return None
        return hit


def _build_snapshot(
    *, bucket: str, cursor: int, rows: list[dict],
) -> SnapshotArtifact:
    """Produce a gzipped, bucket-filtered SQLite file.

    Path preference: when `/state/arbitrage.db` exists, replicate its
    `sqlite_master` DDL (picks up any ETL-side schema extensions we
    haven't mirrored yet) and copy the bucket's opportunity/leg rows
    via ATTACH. When it doesn't, fall back to the bundled schema —
    the response is still a valid, openable SQLite file with zero
    rows, which is exactly what a boot-time client expects when the
    backend hasn't yet received a push.
    """
    ids = [int(r["opportunity_id"]) for r in rows]
    source = arbitrage_db_path()

    work = snapshots_dir()
    work.mkdir(parents=True, exist_ok=True)
    # `monotonic_ns` ensures unique filenames even if two builds for
    # the same bucket land in the same microsecond.
    nonce = f"{os.getpid()}-{time.monotonic_ns()}"
    build_path = work / f".build-{bucket}-{nonce}.db"
    out_path = work / f".vacuum-{bucket}-{nonce}.db"
    build_path.unlink(missing_ok=True)
    out_path.unlink(missing_ok=True)

    try:
        # `uri=True` lets us ATTACH the source with `?mode=ro` below.
        conn = sqlite3.connect(f"file:{build_path}", uri=True)
        try:
            if source.exists():
                _replicate_schema(conn, source)
                if ids:
                    _copy_rows(conn, source, ids)
            else:
                conn.executescript(ARBITRAGE_SCHEMA_SQL)
            conn.commit()
            # `VACUUM INTO` produces a single-file, WAL-free, page-
            # aligned DB — perfect for clients to deserialize directly.
            # The target path is not parameterisable, so SQL-quote it.
            quoted = str(out_path).replace("'", "''")
            conn.execute(f"VACUUM INTO '{quoted}'")
        finally:
            conn.close()
        raw = out_path.read_bytes()
    finally:
        build_path.unlink(missing_ok=True)
        out_path.unlink(missing_ok=True)

    # Level 3 trades a few percent of compression for meaningful
    # encode-time savings on these small DBs; body is served gzipped.
    body = gzip.compress(raw, compresslevel=3)
    return SnapshotArtifact(
        bucket=bucket, cursor=cursor, body=body,
        row_count=len(ids), built_at=time.monotonic(),
    )


def _replicate_schema(conn: sqlite3.Connection, source: Path) -> None:
    """Copy every DDL statement from the source DB's `sqlite_master`.

    Ordered tables → indexes → views so view bodies can reference
    their base tables without forward declarations.
    """
    src = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    try:
        ddls = src.execute(
            "SELECT sql FROM sqlite_master "
            "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' "
            "ORDER BY CASE type "
            "  WHEN 'table' THEN 0 "
            "  WHEN 'index' THEN 1 "
            "  WHEN 'view' THEN 2 "
            "  ELSE 3 END, rootpage"
        ).fetchall()
    finally:
        src.close()
    for (ddl,) in ddls:
        conn.execute(ddl)


def _copy_rows(
    conn: sqlite3.Connection, source: Path, ids: list[int],
) -> None:
    """Bulk-copy the bucket's opportunity + leg rows and the full
    `arb_runs` table (small, keeps FK-consistent for clients that
    enable `PRAGMA foreign_keys=ON`)."""
    placeholders = ",".join("?" * len(ids))
    conn.execute("ATTACH DATABASE ? AS src", (f"file:{source}?mode=ro",))
    try:
        conn.execute("INSERT INTO main.arb_runs SELECT * FROM src.arb_runs")
        conn.execute(
            f"INSERT INTO main.arb_opportunities "
            f"SELECT * FROM src.arb_opportunities WHERE id IN ({placeholders})",
            ids,
        )
        conn.execute(
            f"INSERT INTO main.arb_legs "
            f"SELECT * FROM src.arb_legs WHERE opportunity_id IN ({placeholders})",
            ids,
        )
    finally:
        # python sqlite3 opens an implicit transaction on the first
        # write, holding SHARED/RESERVED locks on `src` until commit.
        # DETACH refuses while those locks are live — commit first.
        conn.commit()
        conn.execute("DETACH DATABASE src")
