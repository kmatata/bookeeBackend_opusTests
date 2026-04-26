"""Smoke tests for /meta, /buckets, /arbs/{bucket}, /metrics, /health."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import app
from app.config import get_settings

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
            "X-Populate-Duration-Ms": "12.5",
            "X-Scanner-Run-Id": str(run_id),
        },
    )


def _seeded_bytes(tmp_path: Path, opps: list[dict], legs: list[dict]) -> bytes:
    now = datetime.now(timezone.utc)
    fresh = {
        "start_time": (now + timedelta(days=2)).isoformat(),
        "last_seen_at": now.isoformat(),
        "oldest_odd_updated_at": now.isoformat(),
        "latest_odd_updated_at": now.isoformat(),
        "first_seen_at": now.isoformat(),
    }
    opps = [{**fresh, **o} for o in opps]
    p = tmp_path / "src.db"
    build_arbitrage_db(p, opportunities=opps, legs=legs)
    data = p.read_bytes()
    p.unlink()
    return data


# ----- /meta -----

def test_meta_shape(client):
    r = client.get("/meta")
    assert r.status_code == 200
    body = r.json()
    s = get_settings()
    assert body["schema_version"] == s.schema_version
    assert set(body["buckets"].keys()) == set(s.buckets.keys())
    # Each value is [lo, hi|null]
    for name, bounds in body["buckets"].items():
        assert len(bounds) == 2
        assert bounds[0] == s.buckets[name].lo_bps
    assert body["stale_seconds_live"] == s.stale_seconds_live
    assert body["stale_seconds_upcoming"] == s.stale_seconds_upcoming


# ----- /buckets -----

def test_buckets_empty_on_fresh_boot(client):
    r = client.get("/buckets")
    assert r.status_code == 200
    body = r.json()
    s = get_settings()
    assert set(body.keys()) == set(s.buckets.keys())
    assert all(v == 0 for v in body.values())


def test_buckets_reflects_ingest(client, tmp_path):
    db_bytes = _seeded_bytes(
        tmp_path,
        opps=[
            {"id": 1, "group_id": 1, "profit_margin_bps": 400},
            {"id": 2, "group_id": 2, "profit_margin_bps": 600},
        ],
        legs=[
            {"opportunity_id": 1, "leg_index": 0, "bookmaker": "a"},
            {"opportunity_id": 2, "leg_index": 0, "bookmaker": "b"},
        ],
    )
    r = _put(client, db_bytes)
    assert r.status_code == 204

    r = client.get("/buckets")
    body = r.json()
    assert body["low"] == 1
    assert body["mid"] == 1
    assert body.get("high", 0) == 0


# ----- /arbs/{bucket} -----

def test_arbs_unknown_bucket_404(client):
    r = client.get("/arbs/nope")
    assert r.status_code == 404


def test_arbs_empty_bucket_returns_empty_list(client):
    r = client.get("/arbs/low")
    assert r.status_code == 200
    assert r.json() == []


def test_arbs_returns_bucket_rows(client, tmp_path):
    db_bytes = _seeded_bytes(
        tmp_path,
        opps=[
            {"id": 10, "group_id": 1, "profit_margin_bps": 350},
            {"id": 20, "group_id": 2, "profit_margin_bps": 900},
        ],
        legs=[
            {"opportunity_id": 10, "leg_index": 0, "bookmaker": "a"},
            {"opportunity_id": 20, "leg_index": 0, "bookmaker": "b"},
        ],
    )
    assert _put(client, db_bytes).status_code == 204

    r = client.get("/arbs/low")
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["opportunity_id"] == 10

    r = client.get("/arbs/high")
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["opportunity_id"] == 20


# ----- /metrics -----

def test_metrics_shape_on_fresh_boot(client):
    r = client.get("/metrics")
    assert r.status_code == 200
    body = r.json()
    assert body["samples"] == 0
    assert "ema" in body
    assert "hypothesis_5s" in body
    assert body["hypothesis_5s"]["expected_cadence_ms"] == 5000


def test_metrics_reflects_ingest(client, tmp_path):
    db_bytes = _seeded_bytes(
        tmp_path,
        opps=[{"id": 1, "group_id": 1, "profit_margin_bps": 400}],
        legs=[{"opportunity_id": 1, "leg_index": 0, "bookmaker": "a"}],
    )
    assert _put(client, db_bytes).status_code == 204

    r = client.get("/metrics")
    body = r.json()
    assert body["samples"] == 1
    assert body["ema"]["upload_ms"] is not None
    assert body["ema"]["e2e_ms"] is not None
    assert len(body["last_10"]) == 1


# ----- /health -----

def test_health_samples_grows_with_ingest(client, tmp_path):
    assert client.get("/health").json()["samples"] == 0

    db_bytes = _seeded_bytes(
        tmp_path,
        opps=[{"id": 1, "group_id": 1, "profit_margin_bps": 400}],
        legs=[{"opportunity_id": 1, "leg_index": 0, "bookmaker": "a"}],
    )
    assert _put(client, db_bytes).status_code == 204
    assert client.get("/health").json()["samples"] == 1
