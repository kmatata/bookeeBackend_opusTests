from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Awaitable, Callable

import orjson

from .broadcaster import Broadcaster

_log = logging.getLogger(__name__)


def parse_last_id(last_event_id: str | None, cursor: int) -> int:
    """Resolve the subscriber's resume point.

    Per the SSE spec the `Last-Event-ID` header wins over anything in
    the URL; we fall back to `?cursor=N` so clients that can't set
    headers (simple `new EventSource(...)` + explicit resume) still
    work. 0 means "I have nothing, send me everything in the ring".
    """
    if last_event_id:
        try:
            return int(last_event_id.strip())
        except ValueError:
            pass
    return max(0, int(cursor))


def sse_payload(event: str, event_id: int, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "event": event,
        "id": str(event_id),
        "data": orjson.dumps(data).decode(),
    }


def sse_stale(next_cursor: int, bucket: str) -> dict[str, Any]:
    """`snapshot_stale`: the ring can't cover the client's gap, so the
    client must re-fetch `/snapshot/{bucket}` and reconnect with a
    fresh cursor."""
    return sse_payload(
        "snapshot_stale",
        next_cursor,
        {"reason": "cursor_gap", "recommended": f"/snapshot/{bucket}"},
    )


async def stream_events(
    *,
    broadcaster: Broadcaster,
    bucket: str,
    last_id: int,
    heartbeat_seconds: float,
    is_disconnected: Callable[[], Awaitable[bool]] | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Subscribe-replay-follow loop for one bucket.

    Subscribe is issued BEFORE snapshotting the ring so no published
    event slips through the gap between the two operations; any event
    that lands in both `replay` and the live queue is suppressed by
    `replayed_ids`. Heartbeat timeout lets the loop re-check
    `is_disconnected` promptly when clients hang up.
    """
    q = broadcaster.subscribe(bucket)
    _log.info(
        "sse_connect",
        extra={
            "bucket": bucket,
            "cursor": last_id,
            "subscribers": broadcaster.subscriber_count(bucket),
        },
    )
    events_sent = 0
    try:
        if broadcaster.has_gap(bucket, last_id):
            yield sse_stale(broadcaster.current_cursor(bucket), bucket)
            events_sent += 1
            return

        replayed_ids: set[int] = set()
        for evt in broadcaster.replay_since(bucket, last_id):
            yield sse_payload(evt.event, evt.id, evt.data)
            events_sent += 1
            replayed_ids.add(evt.id)

        while True:
            if is_disconnected is not None and await is_disconnected():
                return
            try:
                evt = await asyncio.wait_for(q.get(), timeout=heartbeat_seconds)
            except asyncio.TimeoutError:
                continue
            if evt.id in replayed_ids:
                continue
            yield sse_payload(evt.event, evt.id, evt.data)
            events_sent += 1
    finally:
        broadcaster.unsubscribe(bucket, q)
        _log.info(
            "sse_disconnect",
            extra={"bucket": bucket, "events_sent": events_sent},
        )
