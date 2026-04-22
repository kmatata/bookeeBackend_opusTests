from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from .config import Bucket


def bucket_for(bps: int, buckets: dict[str, Bucket]) -> str | None:
    """Return the bucket name this bps falls in, or None if below the
    lowest floor (sub-threshold rows are dropped from fan-out entirely)."""
    for name, b in buckets.items():
        if b.contains(bps):
            return name
    return None


def partition(
    rows: Iterable[dict[str, Any]],
    buckets: dict[str, Bucket],
) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {name: [] for name in buckets}
    for row in rows:
        name = bucket_for(int(row["profit_margin_bps"]), buckets)
        if name is not None:
            out[name].append(row)
    return out


def _legs_fingerprint(legs: list[dict[str, Any]] | None) -> tuple:
    if not legs:
        return ()
    return tuple(
        (
            int(leg.get("leg_index", 0)),
            str(leg["bookmaker"]),
            str(leg.get("outcome", "")),
            float(leg.get("odd", 0.0)),
            float(leg.get("stake", 0.0)),
            str(leg.get("event_active") or ""),
            str(leg.get("odd_updated_at") or ""),
        )
        for leg in legs
    )


def fingerprint(row: dict[str, Any]) -> int:
    """Stable hash over the material row state; used to decide whether
    an upsert delta is worth emitting."""
    return hash((
        int(row["opportunity_id"]),
        int(row["group_id"]),
        int(row["profit_margin_bps"]),
        str(row.get("source_type") or ""),
        str(row.get("market_type") or ""),
        str(row.get("start_time") or ""),
        str(row.get("canonical_home") or ""),
        str(row.get("canonical_away") or ""),
        str(row.get("last_seen_at") or ""),
        _legs_fingerprint(row.get("legs")),
    ))


@dataclass
class BucketDelta:
    bucket: str
    upserts: list[dict[str, Any]] = field(default_factory=list)
    deletes: list[int] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not self.upserts and not self.deletes


class BucketState:
    """In-memory snapshot of what each bucket currently contains.

    Call `reconcile(new_rows)` with the deduped live set; it returns
    one BucketDelta per bucket and atomically swaps the internal state
    so the next call diffs against the just-applied snapshot.
    """

    def __init__(self, buckets: dict[str, Bucket]) -> None:
        self._buckets = buckets
        # bucket -> opportunity_id -> (fingerprint, row)
        self._current: dict[str, dict[int, tuple[int, dict[str, Any]]]] = {
            name: {} for name in buckets
        }

    def reconcile(
        self, new_rows: Iterable[dict[str, Any]],
    ) -> list[BucketDelta]:
        partitioned = partition(new_rows, self._buckets)
        deltas: list[BucketDelta] = []

        for name in self._buckets:
            prior = self._current[name]
            incoming: dict[int, tuple[int, dict[str, Any]]] = {}
            delta = BucketDelta(bucket=name)

            for row in partitioned.get(name, []):
                opp_id = int(row["opportunity_id"])
                fp = fingerprint(row)
                incoming[opp_id] = (fp, row)
                prev = prior.get(opp_id)
                if prev is None or prev[0] != fp:
                    delta.upserts.append(row)

            # Rows in prior but not in incoming have either been deleted,
            # moved to another bucket, or fallen below threshold. In all
            # three cases this bucket's subscribers must drop them.
            for opp_id in prior:
                if opp_id not in incoming:
                    delta.deletes.append(opp_id)

            self._current[name] = incoming
            deltas.append(delta)

        return deltas

    def snapshot(self, bucket: str) -> list[dict[str, Any]]:
        return [row for _, row in self._current[bucket].values()]

    def counts(self) -> dict[str, int]:
        return {name: len(m) for name, m in self._current.items()}
