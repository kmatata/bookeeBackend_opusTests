from __future__ import annotations

import pytest

from app.bucket_state import BucketState, bucket_for, partition
from app.config import Bucket


@pytest.fixture
def buckets() -> dict[str, Bucket]:
    return {
        "low":  Bucket(name="low",  lo_bps=300,  hi_bps=500),
        "mid":  Bucket(name="mid",  lo_bps=500,  hi_bps=800),
        "high": Bucket(name="high", lo_bps=800,  hi_bps=1500),
        "moon": Bucket(name="moon", lo_bps=1500, hi_bps=None),
    }


def _row(opp_id, bps, *, group_id=1, last_seen="2026-04-21T09:00:00+03:00",
         legs=None):
    return {
        "opportunity_id": opp_id,
        "group_id": group_id,
        "profit_margin_bps": bps,
        "source_type": "upcoming",
        "market_type": "three_way",
        "start_time": "2026-04-22T18:00:00+03:00",
        "canonical_home": "home",
        "canonical_away": "away",
        "last_seen_at": last_seen,
        "legs": legs or [
            {"leg_index": 0, "bookmaker": "a", "outcome": "1", "odd": 2.1,
             "stake": 100.0, "event_active": "ACTIVE",
             "odd_updated_at": "2026-04-21T08:59:00+03:00"},
            {"leg_index": 1, "bookmaker": "b", "outcome": "2", "odd": 3.2,
             "stake": 100.0, "event_active": "ACTIVE",
             "odd_updated_at": "2026-04-21T08:59:00+03:00"},
        ],
    }


def test_bucket_for_ranges(buckets):
    assert bucket_for(299, buckets) is None           # below floor
    assert bucket_for(300, buckets) == "low"          # inclusive low
    assert bucket_for(499, buckets) == "low"
    assert bucket_for(500, buckets) == "mid"          # inclusive on boundary
    assert bucket_for(799, buckets) == "mid"
    assert bucket_for(800, buckets) == "high"
    assert bucket_for(1499, buckets) == "high"
    assert bucket_for(1500, buckets) == "moon"
    assert bucket_for(9999, buckets) == "moon"


def test_partition_drops_sub_threshold(buckets):
    rows = [_row(1, 250), _row(2, 350), _row(3, 900)]
    p = partition(rows, buckets)
    assert [r["opportunity_id"] for r in p["low"]] == [2]
    assert [r["opportunity_id"] for r in p["high"]] == [3]
    assert p["mid"] == []
    assert p["moon"] == []


def test_reconcile_first_pass_emits_all_as_upserts(buckets):
    st = BucketState(buckets)
    deltas = st.reconcile([_row(1, 400), _row(2, 600), _row(3, 1600)])
    by_bucket = {d.bucket: d for d in deltas}
    assert [r["opportunity_id"] for r in by_bucket["low"].upserts] == [1]
    assert [r["opportunity_id"] for r in by_bucket["mid"].upserts] == [2]
    assert [r["opportunity_id"] for r in by_bucket["moon"].upserts] == [3]
    assert all(d.deletes == [] for d in deltas)


def test_reconcile_unchanged_row_emits_nothing(buckets):
    st = BucketState(buckets)
    rows = [_row(1, 400), _row(2, 600)]
    st.reconcile(rows)
    deltas = st.reconcile(rows)
    assert all(d.is_empty() for d in deltas)


def test_reconcile_changed_row_emits_upsert(buckets):
    st = BucketState(buckets)
    st.reconcile([_row(1, 400)])
    changed = _row(1, 400, last_seen="2026-04-21T10:00:00+03:00")
    deltas = {d.bucket: d for d in st.reconcile([changed])}
    assert len(deltas["low"].upserts) == 1
    assert deltas["low"].deletes == []


def test_reconcile_gone_row_emits_delete(buckets):
    st = BucketState(buckets)
    st.reconcile([_row(1, 400), _row(2, 450)])
    deltas = {d.bucket: d for d in st.reconcile([_row(2, 450)])}
    assert deltas["low"].deletes == [1]
    assert deltas["low"].upserts == []


def test_reconcile_bucket_migration_deletes_from_old_and_upserts_in_new(buckets):
    """A row whose bps climbs into a higher bucket must be dropped from
    its previous bucket and appear as an upsert in the new one."""
    st = BucketState(buckets)
    st.reconcile([_row(1, 400)])  # → low
    deltas = {d.bucket: d for d in st.reconcile([_row(1, 900)])}  # → high
    assert deltas["low"].deletes == [1]
    assert deltas["low"].upserts == []
    assert [r["opportunity_id"] for r in deltas["high"].upserts] == [1]
    assert deltas["high"].deletes == []


def test_reconcile_row_falling_below_threshold_is_pure_delete(buckets):
    st = BucketState(buckets)
    st.reconcile([_row(1, 400)])
    deltas = {d.bucket: d for d in st.reconcile([_row(1, 250)])}
    assert deltas["low"].deletes == [1]
    for name in ("mid", "high", "moon"):
        assert deltas[name].is_empty()


def test_counts_and_snapshot_after_reconcile(buckets):
    st = BucketState(buckets)
    st.reconcile([_row(1, 400), _row(2, 700), _row(3, 700)])
    assert st.counts() == {"low": 1, "mid": 2, "high": 0, "moon": 0}
    mid = st.snapshot("mid")
    assert sorted(r["opportunity_id"] for r in mid) == [2, 3]
