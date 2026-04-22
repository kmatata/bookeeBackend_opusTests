from __future__ import annotations

import datetime as dt
import sqlite3
import threading
from pathlib import Path
from typing import Any

from .config import get_settings
from .state_paths import ops_db_path
from .timing import compute_ema

_SCHEMA = """
CREATE TABLE IF NOT EXISTS receive_timings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    received_at     TEXT NOT NULL,
    bytes_received  INTEGER NOT NULL,
    upload_ms       REAL NOT NULL,
    populate_ms     REAL,
    reconcile_ms    REAL NOT NULL,
    e2e_ms          REAL NOT NULL,
    scanner_run_id  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_receive_timings_recent ON receive_timings(received_at);

CREATE TABLE IF NOT EXISTS receive_ema (
    id                    INTEGER PRIMARY KEY CHECK (id = 1),
    alpha                 REAL NOT NULL DEFAULT 0.2,
    upload_ms_ema         REAL,
    populate_ms_ema       REAL,
    reconcile_ms_ema      REAL,
    e2e_ms_ema            REAL,
    inter_arrival_ms_ema  REAL,
    last_received_at      TEXT,
    last_gap_ms           REAL,
    samples               INTEGER NOT NULL DEFAULT 0,
    updated_at            TEXT NOT NULL
);
INSERT OR IGNORE INTO receive_ema (id, updated_at) VALUES (1, datetime('now'));
"""


def _iso_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


class OpsDB:
    """Synchronous SQLite writer for operational metrics (receive timings +
    EMA state). Called from an asyncio.to_thread wrapper so the event loop
    is never blocked.

    A single RLock guards writes because sqlite3 connections aren't
    thread-safe; in practice the ingest pipeline already serialises pushes
    with an asyncio.Lock so contention is zero.
    """

    def __init__(
        self,
        path: Path,
        *,
        alpha: float,
        warmup: int,
        retention_rows: int,
    ) -> None:
        self.path = path
        self.alpha = alpha
        self.warmup = warmup
        self.retention_rows = retention_rows
        self._lock = threading.RLock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        c = sqlite3.connect(str(self.path), isolation_level=None, timeout=30.0)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        return c

    def _init_schema(self) -> None:
        with self._lock, self._connect() as c:
            c.executescript(_SCHEMA)
            c.execute(
                "UPDATE receive_ema SET alpha = ? WHERE id = 1 AND alpha != ?",
                (self.alpha, self.alpha),
            )

    def record(
        self,
        *,
        bytes_received: int,
        upload_ms: float,
        populate_ms: float | None,
        reconcile_ms: float,
        e2e_ms: float,
        scanner_run_id: int | None,
    ) -> dict[str, Any]:
        now = _iso_now()
        with self._lock, self._connect() as c:
            c.execute("BEGIN IMMEDIATE")
            try:
                c.execute(
                    """
                    INSERT INTO receive_timings
                      (received_at, bytes_received, upload_ms, populate_ms,
                       reconcile_ms, e2e_ms, scanner_run_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (now, bytes_received, upload_ms, populate_ms,
                     reconcile_ms, e2e_ms, scanner_run_id),
                )

                row = c.execute("SELECT * FROM receive_ema WHERE id = 1").fetchone()
                samples = int(row["samples"]) + 1

                last_gap_ms: float | None = None
                if row["last_received_at"]:
                    try:
                        prev = dt.datetime.fromisoformat(row["last_received_at"])
                        last_gap_ms = (
                            dt.datetime.fromisoformat(now) - prev
                        ).total_seconds() * 1000.0
                    except ValueError:
                        last_gap_ms = None

                new_upload = compute_ema(
                    row["upload_ms_ema"], upload_ms,
                    alpha=self.alpha, samples=samples, warmup=self.warmup,
                )
                new_populate = (
                    compute_ema(
                        row["populate_ms_ema"], populate_ms,
                        alpha=self.alpha, samples=samples, warmup=self.warmup,
                    )
                    if populate_ms is not None else row["populate_ms_ema"]
                )
                new_reconcile = compute_ema(
                    row["reconcile_ms_ema"], reconcile_ms,
                    alpha=self.alpha, samples=samples, warmup=self.warmup,
                )
                new_e2e = compute_ema(
                    row["e2e_ms_ema"], e2e_ms,
                    alpha=self.alpha, samples=samples, warmup=self.warmup,
                )
                new_inter_arrival = (
                    compute_ema(
                        row["inter_arrival_ms_ema"], last_gap_ms,
                        alpha=self.alpha, samples=samples, warmup=self.warmup,
                    )
                    if last_gap_ms is not None else row["inter_arrival_ms_ema"]
                )

                c.execute(
                    """
                    UPDATE receive_ema SET
                      samples              = ?,
                      upload_ms_ema        = ?,
                      populate_ms_ema      = ?,
                      reconcile_ms_ema     = ?,
                      e2e_ms_ema           = ?,
                      inter_arrival_ms_ema = ?,
                      last_received_at     = ?,
                      last_gap_ms          = ?,
                      updated_at           = ?
                    WHERE id = 1
                    """,
                    (samples, new_upload, new_populate, new_reconcile, new_e2e,
                     new_inter_arrival, now, last_gap_ms, now),
                )

                # retention trim: keep only the most-recent N rows
                c.execute(
                    """
                    DELETE FROM receive_timings
                    WHERE id < COALESCE(
                      (SELECT id FROM receive_timings
                       ORDER BY id DESC LIMIT 1 OFFSET ?),
                      0
                    )
                    """,
                    (self.retention_rows,),
                )
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise

            return self._snapshot_ema_row(c)

    def snapshot_ema(self) -> dict[str, Any]:
        with self._lock, self._connect() as c:
            return self._snapshot_ema_row(c)

    def snapshot_last(self, n: int) -> list[dict[str, Any]]:
        with self._lock, self._connect() as c:
            rows = c.execute(
                "SELECT * FROM receive_timings ORDER BY id DESC LIMIT ?",
                (n,),
            ).fetchall()
            return [dict(r) for r in rows]

    @staticmethod
    def _snapshot_ema_row(c: sqlite3.Connection) -> dict[str, Any]:
        r = c.execute("SELECT * FROM receive_ema WHERE id = 1").fetchone()
        return dict(r) if r else {}


_instance: OpsDB | None = None


def get_ops_db() -> OpsDB:
    global _instance
    if _instance is None:
        s = get_settings()
        _instance = OpsDB(
            path=ops_db_path(),
            alpha=s.ema_alpha,
            warmup=s.ema_warmup_samples,
            retention_rows=s.timings_retention_rows,
        )
    return _instance


def reset_instance() -> None:
    """Test helper: drop the singleton so the next get_ops_db() rebuilds
    against the current env/settings."""
    global _instance
    _instance = None
