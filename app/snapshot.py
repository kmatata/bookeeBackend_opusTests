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


def _disk_path(bucket: str, cursor: int) -> Path:
    return snapshots_dir() / f"{bucket}-{cursor}.db.gz"


class SnapshotCache:
    """Per-bucket TTL cache around `_build_snapshot`.

    Memory layer: serves identical bytes while `cursor` is unchanged
    and the TTL hasn't expired — no disk read on hot path.

    Disk layer: on a memory miss the cache checks for a previously
    written `snapshots/{bucket}-{cursor}.db.gz` before running a full
    VACUUM INTO. This survives process restarts so a restarted backend
    serving many cold clients doesn't rebuild from scratch.

    Per-bucket `asyncio.Lock` coalesces concurrent misses to one build.
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
        hit = self._lookup_mem(bucket, cursor)
        if hit is not None:
            return hit
        lock = self._locks.setdefault(bucket, asyncio.Lock())
        async with lock:
            hit = self._lookup_mem(bucket, cursor)
            if hit is not None:
                return hit
            rows = bucket_state.snapshot(bucket)
            artifact = await asyncio.to_thread(
                _build_snapshot, bucket=bucket, cursor=cursor, rows=rows,
            )
            self._cache[bucket] = artifact
            return artifact

    def _lookup_mem(self, bucket: str, cursor: int) -> SnapshotArtifact | None:
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

    Disk-cache check: if `snapshots/{bucket}-{cursor}.db.gz` already
    exists from a previous build (e.g. before a restart), load it
    directly — no VACUUM INTO needed. Stale files (different cursor)
    are pruned after a successful build.

    Source preference: when `/state/arbitrage.db` exists, replicate its
    `sqlite_master` DDL (picks up any ETL-side schema extensions) and
    copy the bucket's rows via ATTACH. Falls back to the bundled schema
    when no push has arrived yet — still a valid, openable SQLite file.
    """
    ids = [int(r["opportunity_id"]) for r in rows]
    disk = _disk_path(bucket, cursor)

    # Disk hit — reuse bytes from a prior build.
    if disk.exists():
        body = disk.read_bytes()
        return SnapshotArtifact(
            bucket=bucket, cursor=cursor, body=body,
            row_count=len(ids), built_at=time.monotonic(),
        )

    source = arbitrage_db_path()
    work = snapshots_dir()
    work.mkdir(parents=True, exist_ok=True)

    nonce = f"{os.getpid()}-{time.monotonic_ns()}"
    build_path = work / f".build-{bucket}-{nonce}.db"
    out_path = work / f".vacuum-{bucket}-{nonce}.db"
    build_path.unlink(missing_ok=True)
    out_path.unlink(missing_ok=True)

    try:
        conn = sqlite3.connect(f"file:{build_path}", uri=True)
        try:
            if source.exists():
                _replicate_schema(conn, source)
                if ids:
                    _copy_rows(conn, source, ids)
            else:
                conn.executescript(ARBITRAGE_SCHEMA_SQL)
            conn.commit()
            quoted = str(out_path).replace("'", "''")
            conn.execute(f"VACUUM INTO '{quoted}'")
        finally:
            conn.close()
        raw = out_path.read_bytes()
    finally:
        build_path.unlink(missing_ok=True)
        out_path.unlink(missing_ok=True)

    body = gzip.compress(raw, compresslevel=3)

    # Write to disk atomically so a concurrent reader never sees a
    # partial file.  Use a temp name then os.replace().
    tmp = work / f".gz-{bucket}-{nonce}"
    tmp.write_bytes(body)
    os.replace(tmp, disk)

    # Prune stale snapshots for this bucket (different cursor).
    for stale in work.glob(f"{bucket}-*.db.gz"):
        if stale != disk:
            stale.unlink(missing_ok=True)

    return SnapshotArtifact(
        bucket=bucket, cursor=cursor, body=body,
        row_count=len(ids), built_at=time.monotonic(),
    )


def _replicate_schema(conn: sqlite3.Connection, source: Path) -> None:
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
        conn.commit()
        conn.execute("DETACH DATABASE src")
