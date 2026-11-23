"""Shared helpers for the deprecated-input tests: warning capture, payload filters, and an orders→customers engine.

The engine is the dev1840 graph: ``sum(customers.spend)`` by ``orders.status`` associates by default, fails under
``to_many_handling="error"`` and repeats one total under ``"broadcast"``, so each mode is observable.
"""

from __future__ import annotations

import json
import os
import tempfile
import warnings
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel, ValidationError

from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.core.deprecation import DeprecatedInputFutureWarning
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.core.warnings import DeprecatedInputWarning
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev1840_fixtures import _seed_sqlite, dev1840_datasets
from tests._engine_helpers import build_exec_engine

SAVED = "by_status"
CM = {"metric": "sum(customers.spend)", "alias": "cm"}
CLEAN = {"population": "orders", "metrics": ["count(orders.*)"]}
CROSS = {"population": "orders", "dimensions": ["orders.status"], "metrics": [CM]}


@contextmanager
def recorded() -> Generator[list[warnings.WarningMessage]]:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield caught


def deprecations(caught: list[warnings.WarningMessage]) -> list[warnings.WarningMessage]:
    """The recorded deprecated-input conversions."""
    return [w for w in caught if isinstance(w.message, DeprecatedInputFutureWarning)]


def payload_of(w: warnings.WarningMessage) -> DeprecatedInputWarning:
    assert isinstance(w.message, DeprecatedInputFutureWarning)
    return w.message.payload


def _kind(w: Any) -> str | None:
    return w.get("kind") if isinstance(w, dict) else getattr(w, "kind", None)


def deprecated_in(entries: list[Any] | None) -> list[Any]:
    """The ``deprecated_input`` entries of a warnings list (payload objects or their JSON dicts)."""
    return [w for w in entries or [] if _kind(w) == "deprecated_input"]


def errors_in(exc: BaseException) -> list[BaseException]:
    """``exc`` and, for a pydantic ``ValidationError``, the errors its validators raised."""
    out: list[BaseException] = [exc]
    if isinstance(exc, ValidationError):
        out += [e for err in exc.errors() if isinstance(e := (err.get("ctx") or {}).get("error"), BaseException)]
    return out


def strict(body: dict[str, Any], value: Any = True) -> dict[str, Any]:
    return {**body, "strict": value}


def mode(body: dict[str, Any], value: str) -> dict[str, Any]:
    return {**body, "to_many_handling": value}


def mcp_payload(text: str) -> dict[str, Any]:
    """The MCP ``query`` tool's JSON reply as ``{"data", "warnings"?, ...}``."""
    parsed = json.loads(text)
    return {"data": parsed} if isinstance(parsed, list) else parsed


def saved_stage() -> SlayerQuery:
    return SlayerQuery.model_validate(
        {"population": "orders", "dimensions": ["orders.status"], "metrics": [{"metric": "count(orders.*)", "alias": "n"}]},
    )


@asynccontextmanager
async def orders_engine() -> AsyncGenerator[SlayerQueryEngine]:
    """The dev1840 engine plus a saved query-backed dataset ``by_status``."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "data.sqlite")
        _seed_sqlite(db_path)
        engine = await build_exec_engine(db_path, dialect="sqlite", datasets=dev1840_datasets())
        try:
            await engine.save_dataset(SlayerDataset(name=SAVED, source_queries=[saved_stage()]))
            yield engine
        finally:
            engine.close()


def rows(data: list[dict[str, Any]]) -> list[tuple]:
    """Order-independent comparable rows."""
    return sorted(tuple(sorted((k, json.dumps(v, sort_keys=True)) for k, v in r.items())) for r in data)


def dumped(payload: BaseModel) -> dict[str, Any]:
    return payload.model_dump(mode="json")


def http_client(engine: SlayerQueryEngine, monkeypatch: pytest.MonkeyPatch) -> SlayerClient:
    """A remote ``SlayerClient`` whose sync and async transports post to an in-process app."""
    server = TestClient(create_app(storage=engine.storage))

    def via_app(*, method: str, path: str, json: dict | None = None, params: dict | None = None) -> Any:
        resp = server.request(method, path, json=json, params=params)
        resp.raise_for_status()
        return resp.json()

    async def via_app_async(*, method: str, path: str, json: dict | None = None, params: dict | None = None) -> Any:  # NOSONAR(S7503) — matches the awaited transport
        return via_app(method=method, path=path, json=json, params=params)

    client = SlayerClient(url="http://testserver")
    monkeypatch.setattr(client, "_request_sync", via_app)
    monkeypatch.setattr(client, "_request", via_app_async)
    return client
