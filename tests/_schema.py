"""Re-export of `app.arbitrage_schema.ARBITRAGE_SCHEMA_SQL` so test
fixtures and the runtime snapshot builder stay in lockstep."""

from __future__ import annotations

from app.arbitrage_schema import ARBITRAGE_SCHEMA_SQL

__all__ = ["ARBITRAGE_SCHEMA_SQL"]
