from fastapi import APIRouter

from . import (
    arbs,
    health,
    ingest,
    matches_ingest,
    matches_snapshot,
    matches_stream,
    snapshot,
    stream,
)

router = APIRouter()
router.include_router(health.router)
router.include_router(ingest.router)
router.include_router(matches_ingest.router)
router.include_router(matches_snapshot.router)
router.include_router(matches_stream.router)
router.include_router(snapshot.router)
router.include_router(stream.router)
router.include_router(arbs.router)
