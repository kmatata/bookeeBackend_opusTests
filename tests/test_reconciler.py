from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from app.broadcaster import Broadcaster
from app.bucket_state import BucketState
from app.config import get_settings
from app.reconciler import run_once
from app.state_paths import arbitrage_db_path

from .conftest import build_arbitrage_db


def _fresh_state(path: Path, opportunities, legs) -> None:
    build_arbitrage_db(path, opportunities=opportunities, legs=legs)


def _active_opp(
    *, opp_id: int, group_id: int, bps: int,
    start_offset_hours: int = 48,
    last_seen_offset_seconds: int = -1,
) -> dict:
    """Returns an opp that the view's status CASE will classify ACTIVE:
    start_time >> now (no EXPIRED), last_seen_at ~= now (no STALE)."""
    now = dt.datetime.now(dt.UTC)
    start = (now + dt.timedelta(hours=start_offset_hours)).isoformat()
    last_seen = (now + dt.timedelta(seconds=last_seen_offset_seconds)).isoformat()
    return {
        "id": opp_id, "group_id": group_id, "profit_margin_bps": bps,
        "start_time": start, "last_seen_at": last_seen,
        "oldest_odd_updated_at": last_seen, "latest_odd_updated_at": last_seen,
        "first_seen_at": last_seen,
        "leg_signature": f"sig-{opp_id}",
    }


def _leg(opp_id: int, idx: int, bookmaker: str, odd: float = 2.5) -> dict:
    return {
        "opportunity_id": opp_id, "leg_index": idx,
        "bookmaker": bookmaker, "odd": odd,
    }


@pytest.fixture
def bc():
    return Broadcaster(bucket_names=["low", "mid", "high", "moon"], ring_size=64)


@pytest.fixture
def bs():
    return BucketState(get_settings().buckets)


async def test_run_once_noop_on_empty_db(bs, bc):
    path = arbitrage_db_path()
    build_arbitrage_db(path)  # schema only, no rows
    res = await run_once(scanner_run_id=1, bucket_state=bs, broadcaster=bc)
    assert res.rows_read == 0
    assert res.total_upserts == res.total_deletes == 0


async def test_run_once_publishes_upserts_and_dedups(bs, bc):
    """Real group_4262 fixture — 443 bps opp wins over 428 bps; only one
    lands in the 'low' bucket, both into the same dedup class."""
    path = arbitrage_db_path()
    _fresh_state(path,
        opportunities=[
            _active_opp(opp_id=1, group_id=4262, bps=428),
            _active_opp(opp_id=2, group_id=4262, bps=443),
        ],
        legs=[
            _leg(1, 0, "betika"), _leg(1, 1, "odi"), _leg(1, 2, "sport_pesa"),
            _leg(2, 0, "betika"), _leg(2, 1, "odi"), _leg(2, 2, "sport_pesa"),
        ],
    )
    res = await run_once(scanner_run_id=1, bucket_state=bs, broadcaster=bc)
    assert res.rows_read == 2
    assert res.deduped == 1
    assert res.total_upserts == 1
    assert res.total_deletes == 0
    assert bs.counts()["low"] == 1
    winners = bs.snapshot("low")
    assert winners[0]["opportunity_id"] == 2
    assert winners[0]["profit_margin_bps"] == 443
    assert bc.current_cursor("low") == 1


async def test_run_once_emits_delete_when_row_disappears(bs, bc):
    path = arbitrage_db_path()
    _fresh_state(path,
        opportunities=[_active_opp(opp_id=1, group_id=1, bps=400)],
        legs=[_leg(1, 0, "a"), _leg(1, 1, "b")],
    )
    r1 = await run_once(scanner_run_id=1, bucket_state=bs, broadcaster=bc)
    assert r1.total_upserts == 1

    _fresh_state(path, opportunities=[], legs=[])
    r2 = await run_once(scanner_run_id=2, bucket_state=bs, broadcaster=bc)
    assert r2.total_upserts == 0
    assert r2.total_deletes == 1
    assert bs.counts()["low"] == 0
    assert bc.current_cursor("low") == 2


async def test_run_once_bucket_migration_deletes_and_upserts(bs, bc):
    path = arbitrage_db_path()
    _fresh_state(path,
        opportunities=[_active_opp(opp_id=1, group_id=1, bps=400)],
        legs=[_leg(1, 0, "a"), _leg(1, 1, "b")],
    )
    await run_once(scanner_run_id=1, bucket_state=bs, broadcaster=bc)
    assert bs.counts()["low"] == 1

    _fresh_state(path,
        opportunities=[_active_opp(opp_id=1, group_id=1, bps=900)],
        legs=[_leg(1, 0, "a"), _leg(1, 1, "b")],
    )
    r = await run_once(scanner_run_id=2, bucket_state=bs, broadcaster=bc)
    assert r.total_deletes == 1  # low drops opp 1
    assert r.total_upserts == 1  # high gets opp 1
    assert bs.counts()["low"] == 0
    assert bs.counts()["high"] == 1


async def test_run_once_skips_expired_rows(bs, bc):
    """start_time in the past → EXPIRED → excluded by the view filter."""
    path = arbitrage_db_path()
    _fresh_state(path,
        opportunities=[_active_opp(opp_id=1, group_id=1, bps=400,
                                   start_offset_hours=-4)],  # 4h in past
        legs=[_leg(1, 0, "a"), _leg(1, 1, "b")],
    )
    res = await run_once(scanner_run_id=1, bucket_state=bs, broadcaster=bc)
    assert res.rows_read == 0
    assert res.total_upserts == 0
