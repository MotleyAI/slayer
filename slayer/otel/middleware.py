"""The REST request scope: one ``request`` probe per HTTP request outside the MCP mount."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, MutableMapping
from typing import Any

import slayer.core.probes as probes

Scope = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[MutableMapping[str, Any]]]
Send = Callable[[MutableMapping[str, Any]], Awaitable[None]]


class RequestMiddleware:
    """Opens ``request`` with ``surface=rest``; sets ``operation`` from the matched route template."""

    def __init__(self, app: Callable[[Scope, Receive, Send], Awaitable[None]], *, mounted: str) -> None:
        self.app = app
        self.mounted = mounted

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = scope.get("path", "")
        if scope["type"] != "http" or path == self.mounted or path.startswith(f"{self.mounted}/"):
            await self.app(scope, receive, send)
            return
        with probes.scope(name="request", attributes={"surface": "rest"}) as request:
            try:
                await self.app(scope, receive, send)
            finally:
                request.set({"operation": getattr(scope.get("route"), "path", "other")})
