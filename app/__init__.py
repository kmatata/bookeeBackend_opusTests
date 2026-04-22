from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .api import router
from .lifespan import lifespan

app = FastAPI(title="bookee-backend", lifespan=lifespan)
app.include_router(router)


class _COEPMiddleware:
    """Inject COOP/COEP headers on /app/* responses only.

    SharedArrayBuffer (required by sqlite-wasm threads) is gated behind
    these two headers.  Applying them site-wide would break cross-origin
    subresources on API responses, so we scope them to the static mount.

    Raw ASGI class (not BaseHTTPMiddleware) so streaming SSE responses
    on /stream/* are never buffered by this layer.
    """

    def __init__(self, asgi_app) -> None:
        self._app = asgi_app

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http" or not scope["path"].startswith("/app"):
            await self._app(scope, receive, send)
            return

        async def _send(message: dict) -> None:
            if message["type"] == "http.response.start":
                hdrs = list(message.get("headers", []))
                hdrs.append((b"cross-origin-embedder-policy", b"require-corp"))
                hdrs.append((b"cross-origin-opener-policy", b"same-origin"))
                message = {**message, "headers": hdrs}
            await send(message)

        await self._app(scope, receive, _send)


app.add_middleware(_COEPMiddleware)

# Serve the built Svelte web client at /app.
# Skipped when /client doesn't exist (pre-client-build) so all existing
# API routes continue to work unchanged.
_client_dir = Path("/client")
if _client_dir.exists():
    app.mount("/app", StaticFiles(directory=_client_dir, html=True), name="web")
