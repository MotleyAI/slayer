"""Shared helpers for the deprecated-input tests: warning capture, payload filters, and an orders→customers engine.

The engine is the dev1840 graph: ``sum(customers.spend)`` by ``orders.status`` broadcasts by default, fails under
``to_many_handling="error"`` and splits per cell under ``"associate"``, so each mode is observable.
"""

from __future__ import annotations

import json
import os
import tempfile
import warnings
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from typing import Any

from pydantic import BaseModel, ValidationError

from slayer.core.deprecation import DeprecatedInputFutureWarning
from slayer.core.models import SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.warnings import DeprecatedInputWarning
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev1840_fixtures import _seed_sqlite, dev1840_models
from tests._engine_helpers import build_exec_engine

SAVED = "by_status"
CM = {"formula": "sum(customers.spend)", "name": "cm"}
CLEAN = {"source_model": "orders", "measures": ["count(*)"]}
CROSS = {"source_model": "orders", "dimensions": ["status"], "measures": [CM]}


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
        {"source_model": "orders", "dimensions": ["status"], "measures": [{"formula": "count(*)", "name": "n"}]},
    )


@asynccontextmanager
async def orders_engine() -> AsyncGenerator[SlayerQueryEngine]:
    """The dev1840 engine plus a saved query-backed model ``by_status``."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "data.sqlite")
        _seed_sqlite(db_path)
        engine = await build_exec_engine(db_path, dialect="sqlite", models=dev1840_models())
        try:
            await engine.save_model(SlayerModel(name=SAVED, source_queries=[saved_stage()]))
            yield engine
        finally:
            engine.close()


def rows(data: list[dict[str, Any]]) -> list[tuple]:
    """Order-independent comparable rows."""
    return sorted(tuple(sorted((k, json.dumps(v, sort_keys=True)) for k, v in r.items())) for r in data)


def dumped(payload: BaseModel) -> dict[str, Any]:
    return payload.model_dump(mode="json")
