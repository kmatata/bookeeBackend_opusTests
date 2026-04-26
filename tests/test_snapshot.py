from __future__ import annotations

import asyncio
import gzip
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient
from httpx import ASGITransport

from app import app
from app.bucket_state import BucketState
from app.config import get_settings
from app.snapshot import SnapshotCache, _build_snapshot
from app.state_paths import arbitrage_db_path

from .conftest import build_arbitrage_db


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def _put(client: TestClient, body: bytes, run_id: int = 1) -> object:
    return client.put(
        "/ingest/arbitrage",
        content=body,
        headers={
            "Content-Type": "application/vnd.sqlite3",
            "X-Ingest-Token": "test-token",
            "X-Populate-Duration-Ms": "10.0",
            "X-Scanner-Run-Id": str(run_id),
        },
    )


def _seeded_db_bytes(tmp_path: Path, opps: list[dict], legs: list[dict]) -> bytes:
    p = tmp_path / "src.db"
    build_arbitrage_db(p, opportunities=opps, legs=legs)
    data = p.read_bytes()
    p.unlink()
    return data


# ----- _build_snapshot direct unit tests -----


def test_build_snapshot_no_source_produces_valid_empty_db(_isolated_state: Path):
    """Boot-time client (backend hasn't received anything yet) must
    still get an openable SQLite file — empty, but schema-complete."""
    artifact = _build_snapshot(bucket="low", cursor=0, rows=[])
    raw = gzip.decompress(artifact.body)
    assert raw[:16] == b"SQLite format 3\x00"
    assert artifact.row_count == 0
    assert artifact.cursor == 0

    # Make sure the emitted file has the expected tables/view.
    tmp = _isolated_state / "emitted.db"
    tmp.write_bytes(raw)
    con = sqlite3.connect(tmp)
    try:
        tables = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        views = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='view'"
        )}
        (n,) = con.execute("SELECT COUNT(*) FROM arb_opportunities").fetchone()
    finally:
        con.close()
    assert "arb_opportunities" in tables
    assert "arb_legs" in tables
    assert "arb_runs" in tables
    assert "arb_view" in views
    assert n == 0


def test_build_snapshot_copies_only_selected_ids(
    _isolated_state: Path, tmp_path: Path,
):
    """Three opps span two buckets; snapshot for the `low` bucket
    must contain only the opp whose bps falls in [300,500)."""
    opps = [
        {"id": 10, "group_id": 1, "profit_margin_bps": 400},  # low
        {"id": 20, "group_id": 2, "profit_margin_bps": 700},  # mid
        {"id": 30, "group_id": 3, "profit_margin_bps": 1200}, # high
    ]
    legs = [
        {"opportunity_id": 10, "leg_index": 0, "bookmaker": "A"},
        {"opportunity_id": 20, "leg_index": 0, "bookmaker": "B"},
        {"opportunity_id": 30, "leg_index": 0, "bookmaker": "C"},
    ]
    arbitrage_db_path().write_bytes(_seeded_db_bytes(tmp_path, opps, legs))

    artifact = _build_snapshot(
        bucket="low", cursor=5,
        rows=[{"opportunity_id": 10, "profit_margin_bps": 400}],
    )
    raw = gzip.decompress(artifact.body)
    out = _isolated_state / "low.db"
    out.write_bytes(raw)
    con = sqlite3.connect(out)
    try:
        ids = [r[0] for r in con.execute(
            "SELECT id FROM arb_opportunities ORDER BY id"
        )]
        leg_ids = [r[0] for r in con.execute(
            "SELECT opportunity_id FROM arb_legs ORDER BY opportunity_id"
        )]
    finally:
        con.close()
    assert ids == [10]
    assert leg_ids == [10]
    assert artifact.row_count == 1
    assert artifact.cursor == 5


# ----- SnapshotCache behaviour -----


async def test_cache_returns_same_bytes_for_same_cursor():
    bstate = BucketState(get_settings().buckets)
    cache = SnapshotCache(ttl_seconds=5.0)
    a = await cache.get(bucket="low", cursor=7, bucket_state=bstate)
    b = await cache.get(bucket="low", cursor=7, bucket_state=bstate)
    assert a is b


async def test_cache_rebuilds_on_cursor_change():
    bstate = BucketState(get_settings().buckets)
    cache = SnapshotCache(ttl_seconds=5.0)
    a = await cache.get(bucket="low", cursor=1, bucket_state=bstate)
    b = await cache.get(bucket="low", cursor=2, bucket_state=bstate)
    assert a is not b
    assert a.cursor == 1 and b.cursor == 2


async def test_cache_coalesces_concurrent_misses():
    """Ten concurrent gets on the same bucket must trigger exactly one
    build — otherwise a boot-time fanout would re-run VACUUM INTO per
    subscriber."""
    bstate = BucketState(get_settings().buckets)
    builds = 0
    original = _build_snapshot

    def counting_build(**kw):
        nonlocal builds
        builds += 1
        return original(**kw)

    import app.snapshot as snap_mod
    snap_mod._build_snapshot = counting_build
    try:
        cache = SnapshotCache(ttl_seconds=5.0)
        results = await asyncio.gather(*[
            cache.get(bucket="mid", cursor=3, bucket_state=bstate)
            for _ in range(10)
        ])
    finally:
        snap_mod._build_snapshot = original

    assert builds == 1
    assert all(r is results[0] for r in results)


# ----- HTTP endpoint -----


async def test_snapshot_unknown_bucket_404(app_with_state):
    transport = ASGITransport(app=app_with_state)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver",
    ) as c:
        r = await c.get("/snapshot/nope")
    assert r.status_code == 404


async def test_snapshot_empty_bucket_returns_valid_db(app_with_state, tmp_path):
    transport = ASGITransport(app=app_with_state)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver",
    ) as c:
        r = await c.get("/snapshot/low")
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/vnd.sqlite3"
    assert r.headers["x-cursor"] == "0"
    assert r.headers["x-row-count"] == "0"
    assert r.headers["cache-control"] == "no-store"
    assert r.headers["etag"] == '"0"'

    # httpx auto-decompresses gzip because of Content-Encoding: gzip,
    # so r.content is the raw SQLite bytes.
    out = tmp_path / "empty.db"
    out.write_bytes(r.content)
    con = sqlite3.connect(out)
    try:
        (n,) = con.execute("SELECT COUNT(*) FROM arb_opportunities").fetchone()
    finally:
        con.close()
    assert n == 0


def _fresh_times() -> dict:
    """Produce `start_time` + `last_seen_at` that land in the
    'ACTIVE' branch of `arb_view` regardless of wall-clock drift
    between test authoring and test execution."""
    now = datetime.now(timezone.utc)
    return {
        "start_time": (now + timedelta(days=2)).isoformat(),
        "last_seen_at": now.isoformat(),
        "oldest_odd_updated_at": now.isoformat(),
        "latest_odd_updated_at": now.isoformat(),
        "first_seen_at": now.isoformat(),
    }


def test_snapshot_after_ingest_contains_bucket_rows(
    client, _isolated_state: Path, tmp_path: Path,
):
    # Seed the source with opps spanning low + mid so we can verify the
    # bucket filter.
    t = _fresh_times()
    opps = [
        {"id": 1, "group_id": 100, "profit_margin_bps": 350, **t},  # low
        {"id": 2, "group_id": 200, "profit_margin_bps": 600, **t},  # mid
        {"id": 3, "group_id": 300, "profit_margin_bps": 450, **t},  # low
    ]
    legs = [
        {"opportunity_id": 1, "leg_index": 0, "bookmaker": "a"},
        {"opportunity_id": 1, "leg_index": 1, "bookmaker": "b"},
        {"opportunity_id": 2, "leg_index": 0, "bookmaker": "c"},
        {"opportunity_id": 2, "leg_index": 1, "bookmaker": "d"},
        {"opportunity_id": 3, "leg_index": 0, "bookmaker": "e"},
        {"opportunity_id": 3, "leg_index": 1, "bookmaker": "f"},
    ]
    db_bytes = _seeded_db_bytes(tmp_path, opps, legs)

    r = _put(client, db_bytes)
    assert r.status_code == 204, r.text

    r = client.get("/snapshot/low")
    assert r.status_code == 200
    # TestClient transparently decompresses gzip; r.content is raw SQLite
    out = _isolated_state / "low.db"
    out.write_bytes(r.content)
    con = sqlite3.connect(out)
    try:
        ids = sorted(
            r[0] for r in con.execute("SELECT id FROM arb_opportunities")
        )
        leg_count = con.execute(
            "SELECT COUNT(*) FROM arb_legs"
        ).fetchone()[0]
    finally:
        con.close()
    assert ids == [1, 3]
    assert leg_count == 4
    assert int(r.headers["x-row-count"]) == 2
    # Cursor must be ≥ 1 — each reconcile emits at least one upsert per
    # row in the bucket on first sight.
    assert int(r.headers["x-cursor"]) >= 2


def test_snapshot_cursor_matches_broadcaster(client, tmp_path: Path):
    """`X-Cursor` from /snapshot must equal the broadcaster's current
    cursor immediately after the response — the client uses this as
    the stream resume point with zero gap."""
    t = _fresh_times()
    opps = [{"id": 1, "group_id": 100, "profit_margin_bps": 400, **t}]
    legs = [{"opportunity_id": 1, "leg_index": 0, "bookmaker": "a"}]
    db_bytes = _seeded_db_bytes(tmp_path, opps, legs)
    r = _put(client, db_bytes)
    assert r.status_code == 204

    r = client.get("/snapshot/low")
    assert r.status_code == 200
    cursor = int(r.headers["x-cursor"])
    assert cursor == app.state.broadcaster.current_cursor("low")
