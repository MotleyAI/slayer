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
            if not request.active:
                await self.app(scope, receive, send)
                return
            statuses: list[int] = []

            async def observed(message: MutableMapping[str, Any]) -> None:
                if message["type"] == "http.response.start":
                    statuses.append(int(message["status"]))
                await send(message)

            try:
                await self.app(scope, receive, observed)
            finally:
                request.set({"operation": getattr(scope.get("route"), "path", "other")})
                if statuses:
                    request.set(_status_attributes(statuses[0]))


def _status_attributes(status: int) -> dict[str, probes.AttributeValue]:
    """The response status; a 5xx answer fails the request, as the OTel HTTP server conventions say."""
    return {"http.response.status_code": status, **({"error.type": str(status)} if status >= 500 else {})}
