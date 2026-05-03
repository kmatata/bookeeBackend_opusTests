from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Response, status

from ..state_paths import matches_db_path

router = APIRouter()


@router.get("/snapshot/matches")
async def snapshot_matches(request: Request) -> Response:
    """Return the latest matches.db as a binary SQLite snapshot.

    The client downloads this file, deserialises it into a local SQLite
    instance, and runs the arbitrage scanner locally.
    """
    target = matches_db_path()
    if not target.exists():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="matches snapshot not yet available",
        )

    body = target.read_bytes()
    return Response(
        content=body,
        media_type="application/vnd.sqlite3",
        headers={
            "Cache-Control": "no-store",
            "X-Row-Count": str(_count_rows(target)),
        },
    )


def _count_rows(db_path) -> int:
    import sqlite3

    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=5.0)
        cur = conn.execute(
            "SELECT (SELECT COUNT(*) FROM matched_three_way_odds) "
            "     + (SELECT COUNT(*) FROM matched_btts_odds)"
        )
        row = cur.fetchone()
        conn.close()
        return int(row[0]) if row else 0
    except Exception:
        return 0
