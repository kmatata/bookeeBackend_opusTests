from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Query, Request, status
from sse_starlette.sse import EventSourceResponse

from ..config import get_settings
from ..sse import parse_last_id, stream_events

router = APIRouter()


@router.get("/stream/matches")
async def stream_matches(
    request: Request,
    cursor: int = Query(default=0),
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
):
    """SSE stream for matched-odds snapshot refreshes.

    The backend does NOT compute row-level deltas for matches — it
    simply broadcasts a ``refresh`` event every time a new ``matches.db``
    is ingested.  Clients react by re-fetching ``/snapshot/matches``.
    This keeps the backend simple while still giving near-real-time
    updates (typical ingest cadence is 5 s for live, 600 s for upcoming).
    """
    settings = get_settings()
    broadcaster = getattr(request.app.state, "matches_broadcaster", None)
    if broadcaster is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="matches broadcaster not initialised",
        )

    last_id = parse_last_id(last_event_id, cursor)
    gen = stream_events(
        broadcaster=broadcaster,
        bucket="matches",
        last_id=last_id,
        heartbeat_seconds=settings.sse_heartbeat_seconds,
        is_disconnected=request.is_disconnected,
    )
    return EventSourceResponse(gen, ping=settings.sse_heartbeat_seconds)
