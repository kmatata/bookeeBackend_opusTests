from fastapi import APIRouter

from . import arbs, health, ingest, snapshot, stream

router = APIRouter()
router.include_router(health.router)
router.include_router(ingest.router)
router.include_router(snapshot.router)
router.include_router(stream.router)
router.include_router(arbs.router)
