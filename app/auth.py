from __future__ import annotations

import hmac

from fastapi import Header, HTTPException, status

from .config import get_settings


async def verify_ingest_token(
    x_ingest_token: str | None = Header(default=None),
) -> None:
    expected = get_settings().ingest_token
    if not x_ingest_token or not hmac.compare_digest(x_ingest_token, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid ingest token",
        )
