from __future__ import annotations

from pathlib import Path

from .config import get_settings


def state_dir() -> Path:
    return Path(get_settings().state_dir)


def arbitrage_db_path() -> Path:
    return state_dir() / "arbitrage.db"


def arbitrage_db_incoming_path() -> Path:
    return state_dir() / "arbitrage.db.incoming"


def matches_db_path() -> Path:
    return state_dir() / "matches.db"


def matches_db_incoming_path() -> Path:
    return state_dir() / "matches.db.incoming"


def ops_db_path() -> Path:
    return state_dir() / "ops.db"


def snapshots_dir() -> Path:
    return state_dir() / "snapshots"


def ensure_dirs() -> None:
    state_dir().mkdir(parents=True, exist_ok=True)
    snapshots_dir().mkdir(parents=True, exist_ok=True)
