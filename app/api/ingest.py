from __future__ import annotations

import asyncio
import logging
import time

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status

from ..auth import verify_ingest_token
from ..config import get_settings
from ..ingest import atomic_swap, get_ingest_lock, stream_to_temp
from ..ops_db import get_ops_db
from ..reconciler import run_once
from ..state_paths import arbitrage_db_incoming_path, arbitrage_db_path

router = APIRouter()
_log = logging.getLogger(__name__)


def _fmt(ms: float | None) -> str:
    return f"{ms:.3f}" if ms is not None else ""


@router.put(
    "/ingest/arbitrage",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(verify_ingest_token)],
)
async def ingest_arbitrage(
    request: Request,
    x_populate_duration_ms: float | None = Header(default=None),
    x_scanner_run_id: int | None = Header(default=None),
    content_encoding: str | None = Header(default=None),
) -> Response:
    settings = get_settings()
    incoming = arbitrage_db_incoming_path()
    target = arbitrage_db_path()

    t_upload_start = time.perf_counter()

    async with get_ingest_lock():
        # Idempotency guard: reject pushes that can't be newer than what
        # we already applied. The check is inside the lock so concurrent
        # pushes see a consistent view of `last_scanner_run_id`.
        if x_scanner_run_id is not None:
            last = getattr(request.app.state, "last_scanner_run_id", 0)
            if x_scanner_run_id <= last:
                _log.warning(
                    "push_rejected_duplicate",
                    extra={"run_id": x_scanner_run_id, "last_applied": last},
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=(
                        f"run_id {x_scanner_run_id} already applied "
                        f"(last={last})"
                    ),
                )

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

        rec = await run_once(
            scanner_run_id=x_scanner_run_id,
            bucket_state=request.app.state.bucket_state,
            broadcaster=request.app.state.broadcaster,
        )

        e2e_ms = upload_ms + (x_populate_duration_ms or 0.0) + rec.reconcile_ms

        ema = await asyncio.to_thread(
            get_ops_db().record,
            bytes_received=bytes_written,
            upload_ms=upload_ms,
            populate_ms=x_populate_duration_ms,
            reconcile_ms=rec.reconcile_ms,
            e2e_ms=e2e_ms,
            scanner_run_id=x_scanner_run_id,
        )

        if x_scanner_run_id is not None:
            request.app.state.last_scanner_run_id = x_scanner_run_id

        _log.info(
            "push_received",
            extra={
                "run_id": x_scanner_run_id,
                "bytes": bytes_written,
                "upload_ms": round(upload_ms, 3),
                "reconcile_ms": round(rec.reconcile_ms, 3),
                "e2e_ms": round(e2e_ms, 3),
                "bucket_counts": request.app.state.bucket_state.counts(),
            },
        )

    headers = {
        "X-Upload-Ms": _fmt(upload_ms),
        "X-Reconcile-Ms": _fmt(rec.reconcile_ms),
        "X-Populate-Ms": _fmt(x_populate_duration_ms),
        "X-E2E-Ms": _fmt(e2e_ms),
        "X-Upload-Ms-Ema": _fmt(ema.get("upload_ms_ema")),
        "X-Reconcile-Ms-Ema": _fmt(ema.get("reconcile_ms_ema")),
        "X-E2E-Ms-Ema": _fmt(ema.get("e2e_ms_ema")),
        "X-Inter-Arrival-Ms-Ema": _fmt(ema.get("inter_arrival_ms_ema")),
        "X-Samples": str(ema.get("samples", 0)),
        "X-Bytes-Received": str(bytes_written),
    }
    return Response(status_code=status.HTTP_204_NO_CONTENT, headers=headers)
