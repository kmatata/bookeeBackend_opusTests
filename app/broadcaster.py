from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import Any, Iterable


@dataclass(frozen=True, slots=True)
class Event:
    id: int
    event: str  # "upsert" | "delete" | "snapshot_stale"
    bucket: str
    data: dict[str, Any]


class Broadcaster:
    """Per-bucket pub/sub with a bounded ring buffer.

    - Each bucket has a monotonic event id that never rewinds.
    - The ring buffer keeps the last `ring_size` events so reconnecting
      subscribers can replay from `Last-Event-ID`.
    - Slow subscribers whose queue fills up are dropped silently; their
      next reconnect will emit `snapshot_stale` because the id gap will
      have exceeded the ring.
    """

    def __init__(
        self,
        *,
        bucket_names: Iterable[str],
        ring_size: int,
        subscriber_queue_size: int = 256,
    ) -> None:
        self._ring_size = ring_size
        self._sub_queue_size = subscriber_queue_size
        self._next_id: dict[str, int] = {b: 1 for b in bucket_names}
        self._ring: dict[str, deque[Event]] = {
            b: deque(maxlen=ring_size) for b in bucket_names
        }
        self._subs: dict[str, set[asyncio.Queue[Event]]] = {
            b: set() for b in bucket_names
        }

    # ---- publish ----

    def publish(self, bucket: str, event: str, data: dict[str, Any]) -> Event:
        if bucket not in self._next_id:
            raise KeyError(f"unknown bucket: {bucket}")
        eid = self._next_id[bucket]
        self._next_id[bucket] += 1
        evt = Event(id=eid, event=event, bucket=bucket, data=data)
        self._ring[bucket].append(evt)
        for q in list(self._subs[bucket]):
            try:
                q.put_nowait(evt)
            except asyncio.QueueFull:
                # Drop this subscriber — their stream will emit
                # snapshot_stale on reconnect (ring gap detection).
                self._subs[bucket].discard(q)
        return evt

    # ---- query ----

    def current_cursor(self, bucket: str) -> int:
        """Last id published (0 if the bucket has never published)."""
        return self._next_id[bucket] - 1

    def oldest_event_id(self, bucket: str) -> int | None:
        r = self._ring[bucket]
        return r[0].id if r else None

    def replay_since(self, bucket: str, last_id: int) -> list[Event]:
        return [e for e in self._ring[bucket] if e.id > last_id]

    def has_gap(self, bucket: str, last_id: int) -> bool:
        """True iff the ring can't cover the gap between `last_id` and now.

        `last_id == 0` means "I have nothing" — only counts as a gap if
        the ring's oldest event isn't id 1 (so events 1..N-1 have been
        evicted).
        """
        oldest = self.oldest_event_id(bucket)
        if oldest is None:
            return False
        return oldest > last_id + 1

    # ---- subscribe ----

    def subscribe(self, bucket: str) -> asyncio.Queue[Event]:
        q: asyncio.Queue[Event] = asyncio.Queue(maxsize=self._sub_queue_size)
        self._subs[bucket].add(q)
        return q

    def unsubscribe(self, bucket: str, q: asyncio.Queue[Event]) -> None:
        self._subs[bucket].discard(q)

    def subscriber_count(self, bucket: str) -> int:
        return len(self._subs[bucket])
