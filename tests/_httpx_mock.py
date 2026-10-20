"""Route every ``httpx.Client`` / ``httpx.AsyncClient`` through a recording ``MockTransport``."""

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from pydantic import BaseModel, ConfigDict


class RecordedRequest(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    method: str
    raw_path: str
    query: dict[str, str]
    headers: httpx.Headers
    body: Any
    timeout: dict[str, float | None]


class HttpxRecorder:
    def __init__(self, respond: Callable[[httpx.Request], httpx.Response]) -> None:
        self.requests: list[RecordedRequest] = []
        self._respond = respond

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(RecordedRequest(
            method=request.method,
            raw_path=request.url.raw_path.decode().split("?", 1)[0],
            query=dict(request.url.params),
            headers=request.headers,
            body=json.loads(request.content) if request.content else None,
            timeout=request.extensions.get("timeout", {}),
        ))
        return self._respond(request)

    @property
    def last(self) -> RecordedRequest:
        return self.requests[-1]


def install(
    *, monkeypatch: pytest.MonkeyPatch, respond: Callable[[httpx.Request], httpx.Response],
) -> HttpxRecorder:
    recorder = HttpxRecorder(respond)
    transport = httpx.MockTransport(recorder.handle)
    real_async, real_sync = httpx.AsyncClient, httpx.Client

    def make_async(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real_async(*args, **{**kwargs, "transport": transport})

    def make_sync(*args: Any, **kwargs: Any) -> httpx.Client:
        return real_sync(*args, **{**kwargs, "transport": transport})

    monkeypatch.setattr(httpx, "AsyncClient", make_async)
    monkeypatch.setattr(httpx, "Client", make_sync)
    return recorder


def json_response(body: Any, status: int = 200) -> Callable[[httpx.Request], httpx.Response]:
    return lambda _request: httpx.Response(status, json=body)
