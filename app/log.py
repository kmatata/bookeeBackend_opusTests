from __future__ import annotations

import json
import logging
import sqlite3
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

# Standard LogRecord attributes — everything else is caller-supplied context.
_STDLIB_ATTRS = frozenset({
    "name", "msg", "args", "created", "filename", "funcName", "levelname",
    "levelno", "lineno", "module", "msecs", "pathname", "process",
    "processName", "relativeCreated", "thread", "threadName", "exc_info",
    "exc_text", "stack_info", "message", "taskName",
})


class JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON object on one line."""

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        ts = dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{record.msecs:03.0f}Z"
        doc: dict = {
            "ts": ts,
            "level": record.levelname,
            "event": record.message,
            "logger": record.name,
        }
        for k, v in record.__dict__.items():
            if k not in _STDLIB_ATTRS:
                doc[k] = v
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str)


class SqliteLogHandler(logging.Handler):
    """Append structured log records to a SQLite table in logs.db.

    Uses WAL mode + a threading.Lock so concurrent callers (uvicorn worker
    threads, asyncio callbacks) can write safely.  The caller must set a
    JsonFormatter on this handler before any records are emitted.
    """

    _DDL = """
        CREATE TABLE IF NOT EXISTS logs (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            ts     TEXT    NOT NULL,
            level  TEXT    NOT NULL,
            event  TEXT    NOT NULL,
            logger TEXT,
            data   TEXT
        );
        CREATE INDEX IF NOT EXISTS logs_ts ON logs(ts);
    """

    def __init__(self, db_path: Path) -> None:
        super().__init__()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.executescript(self._DDL)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.commit()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            doc = json.loads(self.format(record))
            ts = doc.pop("ts", "")
            level = doc.pop("level", "")
            event = doc.pop("event", "")
            logger_name = doc.pop("logger", "")
            data = json.dumps(doc, default=str) if doc else None
            with self._lock:
                self._conn.execute(
                    "INSERT INTO logs(ts, level, event, logger, data)"
                    " VALUES(?,?,?,?,?)",
                    (ts, level, event, logger_name, data),
                )
                self._conn.commit()
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.close()
            except Exception:
                pass
        super().close()


def setup_logging(*, state_dir: Path, level: str = "INFO") -> None:
    """Configure the root logger with a JSON stream handler (stdout) and a
    SQLite handler (state_dir/logs.db).  Call this once before uvicorn.run().
    Passing log_config=None to uvicorn lets uvicorn loggers propagate here.
    """
    numeric = getattr(logging, level.upper(), logging.INFO)
    fmt = JsonFormatter()

    stream_h = logging.StreamHandler(sys.stdout)
    stream_h.setFormatter(fmt)

    sqlite_h = SqliteLogHandler(state_dir / "logs.db")
    sqlite_h.setFormatter(fmt)

    root = logging.getLogger()
    # Remove any handlers that may have been added by earlier basicConfig calls.
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(numeric)
    root.addHandler(stream_h)
    root.addHandler(sqlite_h)
