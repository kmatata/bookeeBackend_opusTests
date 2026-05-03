from __future__ import annotations

import asyncio
import logging
import time

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status

from ..auth import verify_ingest_token
from ..config import get_settings
from ..ingest import atomic_swap, get_ingest_lock, stream_to_temp
from ..ops_db import get_ops_db
from ..state_paths import matches_db_incoming_path, matches_db_path

router = APIRouter()
_log = logging.getLogger(__name__)


def _fmt(ms: float | None) -> str:
    return f"{ms:.3f}" if ms is not None else ""


@router.put(
    "/ingest/matches",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(verify_ingest_token)],
)
async def ingest_matches(
    request: Request,
    content_encoding: str | None = Header(default=None),
) -> Response:
    """Receive a matches.db snapshot from the ETL matcher.

    Unlike the arbitrage ingest, this does NOT run a reconciler — the
    backend simply stores the file and broadcasts a ``refresh`` event on
    the matches SSE stream so clients know to re-fetch the snapshot.
    """
    settings = get_settings()
    incoming = matches_db_incoming_path()
    target = matches_db_path()

    t_upload_start = time.perf_counter()

    async with get_ingest_lock():
        bytes_written = await stream_to_temp(
            request.stream(),
            target=incoming,
            max_bytes=settings.max_ingest_bytes,
            content_encoding=content_encoding,
        )
        upload_ms = (time.perf_counter() - t_upload_start) * 1000.0

        try:
            atomic_swap(incoming, target)
        except OSError as e:
            if incoming.exists():
                incoming.unlink(missing_ok=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"atomic swap failed: {e}",
            ) from e

        # Notify any listening clients that a new snapshot is available.
        broadcaster = getattr(request.app.state, "matches_broadcaster", None)
        if broadcaster is not None:
            broadcaster.publish("matches", "refresh", {"received_at": time.time()})

        ema = await asyncio.to_thread(
            get_ops_db().record,
            bytes_received=bytes_written,
            upload_ms=upload_ms,
            populate_ms=None,
            reconcile_ms=0.0,
            e2e_ms=upload_ms,
            scanner_run_id=None,
        )

        request.app.state.last_matches_received_at = time.time()

        _log.info(
            "matches_push_received",
            extra={
                "bytes": bytes_written,
                "upload_ms": round(upload_ms, 3),
            },
        )

    headers = {
        "X-Upload-Ms": _fmt(upload_ms),
        "X-Upload-Ms-Ema": _fmt(ema.get("upload_ms_ema")),
        "X-Samples": str(ema.get("samples", 0)),
        "X-Bytes-Received": str(bytes_written),
    }
    return Response(status_code=status.HTTP_204_NO_CONTENT, headers=headers)
