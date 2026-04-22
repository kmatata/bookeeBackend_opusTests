from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.config import get_settings
from app.ops_db import reset_instance as reset_ops_db

from ._schema import ARBITRAGE_SCHEMA_SQL


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Each test gets its own STATE_DIR + INGEST_TOKEN; caches cleared."""
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    monkeypatch.setenv("INGEST_TOKEN", "test-token")
    monkeypatch.setenv("MAX_INGEST_BYTES", str(16 * 1024 * 1024))
    # Fast SSE heartbeat so stream tests don't wait on the real 15s
    # disconnect-detection window.
    monkeypatch.setenv("SSE_HEARTBEAT_SECONDS", "1")
    get_settings.cache_clear()
    reset_ops_db()
    (tmp_path / "snapshots").mkdir(exist_ok=True)
    yield tmp_path
    get_settings.cache_clear()
    reset_ops_db()


def build_arbitrage_db(
    path: Path,
    *,
    opportunities: list[dict] | None = None,
    legs: list[dict] | None = None,
    run: dict | None = None,
) -> None:
    """Create a fully-formed arbitrage.db at `path` with the real schema
    and views, optionally seeded with opportunities + legs.
    """
    if path.exists():
        path.unlink()
    con = sqlite3.connect(path)
    try:
        con.executescript(ARBITRAGE_SCHEMA_SQL)
        r = run or {
            "started_at": "2026-04-21T06:00:00+00:00",
            "ended_at":   "2026-04-21T06:00:01+00:00",
            "source_type": "upcoming",
            "market_type": "three_way",
            "groups_scanned": 0,
            "opportunities_found": 0,
            "threshold": 1.0,
            "min_confidence": "HIGH",
            "stake": 1000.0,
        }
        con.execute(
            """INSERT INTO arb_runs (started_at, ended_at, source_type,
               market_type, groups_scanned, opportunities_found, threshold,
               min_confidence, stake)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (r["started_at"], r["ended_at"], r["source_type"], r["market_type"],
             r["groups_scanned"], r["opportunities_found"], r["threshold"],
             r["min_confidence"], r["stake"]),
        )
        for opp in (opportunities or []):
            con.execute(
                """INSERT INTO arb_opportunities
                   (id, group_id, source_type, market_type, target_date,
                    start_time, canonical_home, canonical_away, competition,
                    country, confidence, n_legs, leg_signature,
                    inverse_odds_sum, profit_margin_bps, total_stake,
                    guaranteed_return, guaranteed_profit,
                    oldest_odd_updated_at, latest_odd_updated_at,
                    first_seen_at, last_seen_at, last_run_id)
                   VALUES
                   (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (opp["id"], opp["group_id"], opp.get("source_type", "upcoming"),
                 opp.get("market_type", "three_way"),
                 opp.get("target_date", "2026-04-25"),
                 opp.get("start_time", "2026-04-25T18:00:00+03:00"),
                 opp.get("canonical_home", "home"),
                 opp.get("canonical_away", "away"),
                 opp.get("competition"), opp.get("country"),
                 opp.get("confidence", "HIGH"),
                 opp.get("n_legs", 2),
                 opp.get("leg_signature", f"sig-{opp['id']}"),
                 opp.get("inverse_odds_sum", 0.95),
                 opp["profit_margin_bps"],
                 opp.get("total_stake", 1000.0),
                 opp.get("guaranteed_return", 1050.0),
                 opp.get("guaranteed_profit", 50.0),
                 opp.get("oldest_odd_updated_at", "2026-04-21T05:59:00+00:00"),
                 opp.get("latest_odd_updated_at", "2026-04-21T06:00:00+00:00"),
                 opp.get("first_seen_at", "2026-04-21T05:59:00+00:00"),
                 opp.get("last_seen_at", "2026-04-21T06:00:00+00:00"),
                 1),
            )
        for leg in (legs or []):
            con.execute(
                """INSERT INTO arb_legs
                   (opportunity_id, leg_index, bookmaker, outcome, odd, stake,
                    expected_return, event_id, bookmaker_event_id, fetch_url,
                    event_active, odd_updated_at, event_updated_at)
                   VALUES
                   (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (leg["opportunity_id"], leg["leg_index"], leg["bookmaker"],
                 leg.get("outcome", "1"), leg.get("odd", 2.5),
                 leg.get("stake", 500.0),
                 leg.get("expected_return", 1250.0),
                 leg.get("event_id", 1),
                 leg.get("bookmaker_event_id"),
                 leg.get("fetch_url"),
                 leg.get("event_active", "ACTIVE"),
                 leg.get("odd_updated_at", "2026-04-21T05:59:00+00:00"),
                 leg.get("event_updated_at")),
            )
        con.commit()
    finally:
        con.close()


def _make_sqlite_bytes() -> bytes:
    path = Path("/tmp/_bookee_test_src.db")
    if path.exists():
        path.unlink()
    build_arbitrage_db(path)
    data = path.read_bytes()
    path.unlink()
    return data


@pytest.fixture
def sqlite_bytes() -> bytes:
    return _make_sqlite_bytes()


@pytest.fixture
def app_with_state():
    """Install fresh BucketState + Broadcaster on app.state without
    requiring ASGI lifespan (httpx.ASGITransport does not run it).
    """
    from app import app
    from app.broadcaster import Broadcaster
    from app.bucket_state import BucketState
    from app.config import get_settings
    from app.snapshot import SnapshotCache

    s = get_settings()
    app.state.bucket_state = BucketState(s.buckets)
    app.state.broadcaster = Broadcaster(
        bucket_names=list(s.buckets.keys()),
        ring_size=s.sse_ring_buffer_size,
    )
    app.state.snapshot_cache = SnapshotCache(
        ttl_seconds=float(s.snapshot_cache_seconds),
    )
    app.state.last_scanner_run_id = 0
    yield app
