from __future__ import annotations

import gzip
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import app
from app.state_paths import arbitrage_db_path, ops_db_path


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def _put(client: TestClient, body: bytes, **headers) -> object:
    base = {
        "Content-Type": "application/vnd.sqlite3",
        "X-Ingest-Token": "test-token",
        "X-Populate-Duration-Ms": "87.4",
        "X-Scanner-Run-Id": "1",
    }
    base.update(headers)
    return client.put("/ingest/arbitrage", content=body, headers=base)


def test_ingest_happy_path(client, sqlite_bytes, _isolated_state: Path):
    r = _put(client, sqlite_bytes)
    assert r.status_code == 204, r.text
    # atomic swap landed the file
    assert arbitrage_db_path().exists()
    assert arbitrage_db_path().read_bytes() == sqlite_bytes
    # response headers carry timings
    assert float(r.headers["X-Upload-Ms"]) >= 0.0
    assert float(r.headers["X-Reconcile-Ms"]) >= 0.0
    assert int(r.headers["X-Samples"]) == 1
    assert int(r.headers["X-Bytes-Received"]) == len(sqlite_bytes)
    # ops.db has the row
    con = sqlite3.connect(ops_db_path())
    (n,) = con.execute("SELECT COUNT(*) FROM receive_timings").fetchone()
    assert n == 1
    (samples, upload_ema) = con.execute(
        "SELECT samples, upload_ms_ema FROM receive_ema WHERE id=1"
    ).fetchone()
    con.close()
    assert samples == 1
    assert upload_ema is not None


def test_ingest_rejects_bad_token(client, sqlite_bytes):
    r = _put(client, sqlite_bytes, **{"X-Ingest-Token": "wrong"})
    assert r.status_code == 401


def test_ingest_rejects_missing_token(client, sqlite_bytes):
    r = client.put(
        "/ingest/arbitrage",
        content=sqlite_bytes,
        headers={"Content-Type": "application/vnd.sqlite3"},
    )
    assert r.status_code == 401


def test_ingest_rejects_non_sqlite_body(client):
    r = _put(client, b"<html>not a db</html>" + b"\x00" * 64)
    assert r.status_code == 400


def test_ingest_rejects_oversize_body(client, monkeypatch):
    # squeeze the cap, clear cache so Settings re-reads it
    from app.config import get_settings
    monkeypatch.setenv("MAX_INGEST_BYTES", "128")
    get_settings.cache_clear()
    payload = b"SQLite format 3\x00" + b"\x00" * 2048
    r = _put(client, payload)
    assert r.status_code == 413


def test_ingest_accepts_gzip(client, sqlite_bytes):
    gz = gzip.compress(sqlite_bytes)
    r = _put(client, gz, **{"Content-Encoding": "gzip"})
    assert r.status_code == 204, r.text
    assert arbitrage_db_path().read_bytes() == sqlite_bytes


def test_ingest_updates_inter_arrival_on_second_push(client, sqlite_bytes):
    r1 = _put(client, sqlite_bytes)
    assert r1.status_code == 204
    r2 = _put(client, sqlite_bytes, **{"X-Scanner-Run-Id": "2"})
    assert r2.status_code == 204
    assert int(r2.headers["X-Samples"]) == 2
    assert r2.headers.get("X-Inter-Arrival-Ms-Ema", "") != ""
