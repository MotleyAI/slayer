"""Local HTTP server standing in for the telemetry endpoint (stdlib only)."""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class CapturedRequest(BaseModel):
    path: str
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body)


class CaptureBehaviour(BaseModel):
    """Mutable response behaviour; ``stall_first`` delays only the first request."""

    model_config = ConfigDict(validate_assignment=True)

    status: int = 200
    delay: float = 0.0
    stall_first: float = 0.0


class CaptureServer:
    """Records every POST; ``behaviour`` sets status and delays."""

    def __init__(self) -> None:
        self.behaviour = CaptureBehaviour()
        self.requests: list[CapturedRequest] = []
        self.arrivals = 0
        self._lock = threading.Lock()
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler_class())
        self._server.daemon_threads = True
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}/i/v0/e/"

    def start(self) -> "CaptureServer":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()

    def wait_for(self, *, count: int, timeout: float = 10.0) -> list[CapturedRequest]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if len(self.requests) >= count:
                    return list(self.requests)
            time.sleep(0.02)
        with self._lock:
            return list(self.requests)

    def wait_for_arrival(self, *, timeout: float = 10.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if self.arrivals:
                    return True
            time.sleep(0.02)
        return False

    def _handler_class(self) -> type[BaseHTTPRequestHandler]:
        server = self

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 — http.server naming
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length)
                with server._lock:
                    server.arrivals += 1
                    first = server.arrivals == 1
                delay = server.behaviour.delay + (server.behaviour.stall_first if first else 0.0)
                if delay:
                    time.sleep(delay)
                with server._lock:
                    server.requests.append(CapturedRequest(
                        path=self.path, headers=dict(self.headers.items()), body=body,
                    ))
                self.send_response(server.behaviour.status)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 — http.server signature
                return None

        return _Handler


class RefusedEndpoint(BaseModel):
    """A URL nothing listens on."""

    url: str = Field(default="http://127.0.0.1:9/i/v0/e/")
