from __future__ import annotations

from typing import Any, Iterable


def dedup_key(row: dict[str, Any]) -> tuple[int, tuple[str, ...]]:
    """Collapse duplicates per (fixture, bookmaker-set).

    Two arb opportunities on the same `group_id` touching the same set
    of bookmakers are mutually exclusive — you can't place both legs of
    one and both legs of another on the same books at the same time, so
    the client only needs the best one.
    """
    legs = row.get("legs") or []
    bookmakers = tuple(sorted(str(leg["bookmaker"]) for leg in legs))
    return (int(row["group_id"]), bookmakers)


def _beats(candidate: dict[str, Any], current: dict[str, Any]) -> bool:
    """Tiebreak order: profit_margin_bps DESC, last_seen_at DESC, opportunity_id ASC.

    ISO-8601 timestamps sort correctly lexicographically, so string
    comparison is fine.
    """
    cand_bps = int(candidate["profit_margin_bps"])
    cur_bps = int(current["profit_margin_bps"])
    if cand_bps != cur_bps:
        return cand_bps > cur_bps

    cand_ts = str(candidate.get("last_seen_at") or "")
    cur_ts = str(current.get("last_seen_at") or "")
    if cand_ts != cur_ts:
        return cand_ts > cur_ts

    return int(candidate["opportunity_id"]) < int(current["opportunity_id"])


def dedup(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return the winners only, in input order of first appearance."""
    best: dict[tuple[int, tuple[str, ...]], dict[str, Any]] = {}
    for row in rows:
        k = dedup_key(row)
        cur = best.get(k)
        if cur is None or _beats(row, cur):
            best[k] = row
    return list(best.values())
