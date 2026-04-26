from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Query, Request, status
from sse_starlette.sse import EventSourceResponse

from ..config import get_settings
from ..sse import parse_last_id, stream_events

router = APIRouter()


@router.get("/stream/{bucket}")
async def stream(
    bucket: str,
    request: Request,
    cursor: int = Query(default=0),
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
):
    settings = get_settings()
    if bucket not in settings.buckets:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown bucket: {bucket}",
        )

    last_id = parse_last_id(last_event_id, cursor)
    gen = stream_events(
        broadcaster=request.app.state.broadcaster,
        bucket=bucket,
        last_id=last_id,
        heartbeat_seconds=settings.sse_heartbeat_seconds,
        is_disconnected=request.is_disconnected,
    )
    return EventSourceResponse(gen, ping=settings.sse_heartbeat_seconds)
