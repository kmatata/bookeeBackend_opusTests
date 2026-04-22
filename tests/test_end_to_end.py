"""End-to-end smoke: simulate a sequence of ETL pushes and verify that
every layer of the stack (ingest → reconcile → broadcast → SSE → snapshot
→ metrics) is wired up and responding correctly.

SSE framing / replay is tested in isolation in test_stream.py and
test_broadcaster.py; here we verify the ring buffer via the broadcaster's
ring rather than opening a real HTTP SSE stream, which would block the
TestClient indefinitely (EventSourceResponse keeps connections alive).
"""
from __future__ import annotations

import json
import sqlite3
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


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _future_iso(days: int = 2) -> str:
    return (datetime.now(timezone.utc) + timedelta(days=days)).isoformat()


def _build_db(tmp_path: Path, opps: list[dict], legs: list[dict]) -> bytes:
    fresh = {
        "start_time": _future_iso(),
        "last_seen_at": _now_iso(),
        "oldest_odd_updated_at": _now_iso(),
        "latest_odd_updated_at": _now_iso(),
        "first_seen_at": _now_iso(),
    }
    enriched = [{**fresh, **o} for o in opps]
    p = tmp_path / "src.db"
    build_arbitrage_db(p, opportunities=enriched, legs=legs)
    data = p.read_bytes()
    p.unlink()
    return data


def _put(client: TestClient, body: bytes, run_id: int) -> object:
    return client.put(
        "/ingest/arbitrage",
        content=body,
        headers={
            "Content-Type": "application/vnd.sqlite3",
            "X-Ingest-Token": "test-token",
            "X-Populate-Duration-Ms": "22.0",
            "X-Scanner-Run-Id": str(run_id),
        },
    )


def test_end_to_end_boot_then_ingest(client, tmp_path):
    """/health → /meta → PUT ingest → /snapshot → /arbs → /buckets."""
    # Boot checks
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["ok"] is True
    assert r.json()["samples"] == 0

    r = client.get("/meta")
    assert r.status_code == 200
    body = r.json()
    assert "buckets" in body
    s = get_settings()
    assert set(body["buckets"].keys()) == set(s.buckets.keys())

    # ETL push #1: 2 low-bucket opps
    db_bytes = _build_db(
        tmp_path,
        opps=[
            {"id": 1, "group_id": 100, "profit_margin_bps": 350},
            {"id": 2, "group_id": 200, "profit_margin_bps": 420},
        ],
        legs=[
            {"opportunity_id": 1, "leg_index": 0, "bookmaker": "a"},
            {"opportunity_id": 1, "leg_index": 1, "bookmaker": "b"},
            {"opportunity_id": 2, "leg_index": 0, "bookmaker": "c"},
            {"opportunity_id": 2, "leg_index": 1, "bookmaker": "d"},
        ],
    )
    r = _put(client, db_bytes, run_id=1)
    assert r.status_code == 204, r.text
    assert float(r.headers["X-Upload-Ms"]) >= 0.0
    assert float(r.headers["X-Reconcile-Ms"]) >= 0.0
    assert float(r.headers["X-E2E-Ms"]) >= 0.0
    assert int(r.headers["X-Samples"]) == 1

    # /buckets reflects the reconciled state
    r = client.get("/buckets")
    assert r.json()["low"] == 2

    # /arbs/low returns both rows with legs
    r = client.get("/arbs/low")
    rows = r.json()
    assert len(rows) == 2
    ids = {row["opportunity_id"] for row in rows}
    assert ids == {1, 2}
    assert all(len(row["legs"]) == 2 for row in rows)

    # /snapshot/low produces a valid SQLite with both rows + legs
    r = client.get("/snapshot/low")
    assert r.status_code == 200
    snap_cursor = int(r.headers["X-Cursor"])
    assert snap_cursor >= 2
    assert int(r.headers["X-Row-Count"]) == 2
    out = tmp_path / "low.db"
    out.write_bytes(r.content)
    con = sqlite3.connect(out)
    try:
        db_ids = sorted(
            i for (i,) in con.execute("SELECT id FROM arb_opportunities")
        )
        leg_count = con.execute(
            "SELECT COUNT(*) FROM arb_legs"
        ).fetchone()[0]
    finally:
        con.close()
    assert db_ids == [1, 2]
    assert leg_count == 4

    # Broadcaster ring reflects the two upserts
    bc = app.state.broadcaster
    ring_events = bc.replay_since("low", last_id=0)
    assert len(ring_events) == 2
    assert all(e.event == "upsert" for e in ring_events)
    ring_opp_ids = {e.data["opportunity_id"] for e in ring_events}
    assert ring_opp_ids == {1, 2}


def test_end_to_end_delete_on_second_push(client, tmp_path):
    """Second ETL push that drops opp #1 must emit a delete event to the
    broadcaster ring and update /buckets + /arbs accordingly."""
    # Push #1: opps 1 + 2
    db1 = _build_db(
        tmp_path,
        opps=[
            {"id": 1, "group_id": 1, "profit_margin_bps": 400},
            {"id": 2, "group_id": 2, "profit_margin_bps": 410},
        ],
        legs=[
            {"opportunity_id": 1, "leg_index": 0, "bookmaker": "a"},
            {"opportunity_id": 2, "leg_index": 0, "bookmaker": "b"},
        ],
    )
    assert _put(client, db1, run_id=1).status_code == 204

    # Push #2: only opp 2 remains
    db2 = _build_db(
        tmp_path,
        opps=[{"id": 2, "group_id": 2, "profit_margin_bps": 410}],
        legs=[{"opportunity_id": 2, "leg_index": 0, "bookmaker": "b"}],
    )
    assert _put(client, db2, run_id=2).status_code == 204

    # Ring: upsert 1, upsert 2 (push #1) + delete 1 (push #2)
    bc = app.state.broadcaster
    ring_events = bc.replay_since("low", last_id=0)
    event_types = [e.event for e in ring_events]
    assert "delete" in event_types
    delete_events = [e for e in ring_events if e.event == "delete"]
    assert delete_events[0].data["opportunity_id"] == 1

    # /buckets now shows 1 for low
    assert client.get("/buckets").json()["low"] == 1

    # /arbs/low has only opp 2
    rows = client.get("/arbs/low").json()
    assert len(rows) == 1
    assert rows[0]["opportunity_id"] == 2


def test_end_to_end_metrics_after_two_pushes(client, tmp_path):
    """Two pushes → metrics.samples == 2 and EMA + hypothesis populated."""
    for run_id in (1, 2):
        db = _build_db(
            tmp_path,
            opps=[{"id": run_id, "group_id": run_id, "profit_margin_bps": 350}],
            legs=[{"opportunity_id": run_id, "leg_index": 0, "bookmaker": "a"}],
        )
        assert _put(client, db, run_id=run_id).status_code == 204

    r = client.get("/metrics")
    body = r.json()
    assert body["samples"] == 2
    assert body["ema"]["upload_ms"] is not None
    assert body["ema"]["e2e_ms"] is not None
    assert len(body["last_10"]) == 2
    # After two pushes there should be a gap measurement
    assert body["hypothesis_5s"]["last_gap_ms"] is not None


def test_end_to_end_stale_run_id_rejected(client, tmp_path):
    """A push with the same scanner-run-id as an already-applied push
    must be rejected with 409 Conflict."""
    db = _build_db(
        tmp_path,
        opps=[{"id": 1, "group_id": 1, "profit_margin_bps": 400}],
        legs=[{"opportunity_id": 1, "leg_index": 0, "bookmaker": "a"}],
    )
    assert _put(client, db, run_id=5).status_code == 204
    r = _put(client, db, run_id=5)  # same run_id again
    assert r.status_code == 409


def test_end_to_end_unknown_routes_404(client):
    assert client.get("/stream/no_such_bucket").status_code == 404
    assert client.get("/snapshot/no_such_bucket").status_code == 404
    assert client.get("/arbs/no_such_bucket").status_code == 404


def test_end_to_end_health_samples_tracks_pushes(client, tmp_path):
    """health.samples must match the number of successful pushes."""
    assert client.get("/health").json()["samples"] == 0

    for run_id in (1, 2, 3):
        db = _build_db(
            tmp_path,
            opps=[{"id": run_id, "group_id": run_id, "profit_margin_bps": 320}],
            legs=[{"opportunity_id": run_id, "leg_index": 0, "bookmaker": "a"}],
        )
        assert _put(client, db, run_id=run_id).status_code == 204

    assert client.get("/health").json()["samples"] == 3
