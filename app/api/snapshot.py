from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response, status

from ..config import get_settings

router = APIRouter()


@router.get("/snapshot/{bucket}")
async def snapshot(bucket: str, request: Request) -> Response:
    settings = get_settings()
    if bucket not in settings.buckets:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"unknown bucket: {bucket}",
        )

    broadcaster = request.app.state.broadcaster
    bucket_state = request.app.state.bucket_state
    cache = request.app.state.snapshot_cache

    # Sample the cursor BEFORE building so the snapshot + the stream
    # cursor it advertises are quote-by-quote aligned: a client that
    # opens `/stream/{bucket}?cursor=<X-Cursor>` after receiving this
    # response will either replay events strictly after the snapshot
    # or see `snapshot_stale` if it reconnects far later.
    cursor = broadcaster.current_cursor(bucket)
    artifact = await cache.get(
        bucket=bucket, cursor=cursor, bucket_state=bucket_state,
    )

    return Response(
        content=artifact.body,
        media_type="application/vnd.sqlite3",
        headers={
            # Body is already gzip — tell intermediaries not to re-encode.
            "Content-Encoding": "gzip",
            "ETag": f'"{artifact.cursor}"',
            "X-Cursor": str(artifact.cursor),
            "X-Row-Count": str(artifact.row_count),
            "Cache-Control": "no-store",
        },
    )
