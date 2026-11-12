"""Local HTTP server standing in for the usage-reporting endpoint (stdlib only)."""

from __future__ import annotations

import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from slayer.usage_reporting.sender import HTTP_TIMEOUT

# Outlasts one whole send, which may itself take up to HTTP_TIMEOUT on a loaded runner.
_WAIT = 3 * HTTP_TIMEOUT


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


class _Server(ThreadingHTTPServer):
    def handle_error(self, request: Any, client_address: Any) -> None:
        # A client that gave up on a delayed reply is expected; the traceback would land in a captured CLI stderr.
        if not isinstance(sys.exc_info()[1], ConnectionError):
            super().handle_error(request, client_address)


class CaptureServer:
    """Records every POST; ``behaviour`` sets status and delays."""

    def __init__(self) -> None:
        self.behaviour = CaptureBehaviour()
        self.requests: list[CapturedRequest] = []
        self.arrivals = 0
        self.hold = threading.Event()  # cleared: arrived requests wait unrecorded until set
        self.hold.set()
        self._lock = threading.Lock()
        self._server = _Server(("127.0.0.1", 0), self._handler_class())
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
        self.hold.set()
        self._server.shutdown()
        self._server.server_close()

    def wait_for(self, *, count: int, timeout: float = _WAIT) -> list[CapturedRequest]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if len(self.requests) >= count:
                    return list(self.requests)
            time.sleep(0.02)
        with self._lock:
            return list(self.requests)

    def wait_for_arrival(self, *, timeout: float = _WAIT) -> bool:
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
                server.hold.wait()
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
