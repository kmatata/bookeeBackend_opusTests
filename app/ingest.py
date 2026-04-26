from __future__ import annotations

import asyncio
import logging
import os
import zlib
from pathlib import Path
from typing import AsyncIterator

from fastapi import HTTPException, status

_SQLITE_MAGIC = b"SQLite format 3\x00"
_GZIP_MAGIC = b"\x1f\x8b"

_lock = asyncio.Lock()
_log = logging.getLogger(__name__)


def get_ingest_lock() -> asyncio.Lock:
    """Module-level lock serialising the full ingest pipeline (receive →
    swap → reconcile). Single in-flight push at a time."""
    return _lock


async def stream_to_temp(
    chunks: AsyncIterator[bytes],
    *,
    target: Path,
    max_bytes: int,
    content_encoding: str | None,
) -> int:
    """Stream an async chunk iterator into a temp file. Returns total bytes
    written (post-decompression if the body was gzipped). Raises 413 if the
    stream exceeds max_bytes, 400 if the final bytes aren't a SQLite file.
    """
    if target.exists():
        target.unlink()

    is_gzip = (content_encoding or "").lower().strip() == "gzip"
    # wbits=31 → accept gzip + zlib headers; see zlib docs.
    decomp = zlib.decompressobj(31) if is_gzip else None
    total = 0

    def _write(fh, data: bytes) -> int:
        nonlocal total
        total += len(data)
        if total > max_bytes:
            _log.warning(
                "body_too_large",
                extra={"bytes_received": total, "limit": max_bytes},
            )
            raise HTTPException(
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                detail=f"body exceeds {max_bytes} bytes",
            )
        fh.write(data)
        return len(data)

    try:
        with open(target, "wb") as fh:
            async for chunk in chunks:
                if not chunk:
                    continue
                if decomp is not None:
                    out = decomp.decompress(chunk)
                    if out:
                        _write(fh, out)
                else:
                    _write(fh, chunk)
            if decomp is not None:
                tail = decomp.flush()
                if tail:
                    _write(fh, tail)
            fh.flush()
            os.fsync(fh.fileno())
    except HTTPException:
        if target.exists():
            target.unlink()
        raise
    except Exception:
        if target.exists():
            target.unlink()
        raise

    # Magic-byte validation: must be a real SQLite file (not, e.g., an HTML
    # error page the ETL accidentally PUT).
    with open(target, "rb") as fh:
        head = fh.read(16)
    if head != _SQLITE_MAGIC:
        target.unlink(missing_ok=True)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="body is not a SQLite database",
        )

    return total


def atomic_swap(incoming: Path, target: Path) -> None:
    """Atomic rename on same filesystem; opens + fsyncs the directory so the
    rename survives a crash."""
    os.replace(incoming, target)
    dir_fd = os.open(str(target.parent), os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def looks_gzipped(first_bytes: bytes) -> bool:
    return first_bytes[:2] == _GZIP_MAGIC
