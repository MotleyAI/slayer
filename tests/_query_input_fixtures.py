"""Shared helpers for the query-input tests: unknown-key refusals, spelling conflicts and the ``shop`` saved query."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from slayer.core.errors import SpellingConflictError, UnknownKeyError
from slayer.storage.base import StorageBackend
from tests._engine_helpers import owned_query_engine
from tests._ossie_shapes_fixtures import shop_engine_store
from tests._ossie_vocabulary_fixtures import fresh

STRAY = "stray"
BASE: dict[str, Any] = {"population": "orders", "metrics": ["count(orders.*)"]}
TD: dict[str, Any] = {"field": "orders.ordered_at", "granularity": "month"}
#: Two of the shop's five order months (2024-01, 2024-06).
RANGE = ["2024-01-01", "2024-06-30"]
SAVED = "saved_counts"


def _validation_errors(exc: BaseException) -> list[tuple[BaseException, tuple]]:
    """Every validator-raised error in ``exc`` (or its causes) with its Pydantic location."""
    out: list[tuple[BaseException, tuple]] = []
    seen: set[int] = set()
    node: BaseException | None = exc
    while node is not None and id(node) not in seen:
        seen.add(id(node))
        out.append((node, ()))
        if isinstance(node, ValidationError):
            for err in node.errors():
                inner = (err.get("ctx") or {}).get("error")
                if isinstance(inner, BaseException):
                    out.append((inner, tuple(err["loc"])))
        node = node.__cause__
    return out


def refusal(exc: BaseException) -> tuple[UnknownKeyError, tuple]:
    """The one ``UnknownKeyError`` in ``exc`` and its location."""
    found = {id(e): (e, loc) for e, loc in _validation_errors(exc) if isinstance(e, UnknownKeyError)}
    assert len(found) == 1, f"expected one UnknownKeyError in {exc!r}"
    [(err, loc)] = found.values()
    assert isinstance(err, UnknownKeyError)
    return err, loc


def conflict(exc: BaseException) -> SpellingConflictError:
    """The one ``SpellingConflictError`` in ``exc``."""
    found = {id(e): e for e, _ in _validation_errors(exc) if isinstance(e, SpellingConflictError)}
    assert len(found) == 1, f"expected one SpellingConflictError in {exc!r}"
    [err] = found.values()
    assert isinstance(err, SpellingConflictError)
    return err


def advertised_keys(model: type[BaseModel]) -> set[str]:
    """The keys ``model``'s input schema advertises."""
    return set(model.model_json_schema(mode="validation").get("properties", {}))


def hidden_keys(model: type[BaseModel]) -> set[str]:
    """``model``'s fields its input schema hides."""
    return set(model.model_fields) - advertised_keys(model)


def assert_lists_keys(text: str, model: type[BaseModel]) -> None:
    """``text`` lists every key ``model`` advertises and none it hides."""
    for key in advertised_keys(model):
        assert f"`{key}`" in text, (key, text)
    for key in hidden_keys(model):
        assert f"`{key}`" not in text, (key, text)


async def shop_with_saved(tmp_path: Path) -> StorageBackend:
    """The ``shop`` store plus the saved query ``SAVED`` (``count(orders.*)`` over orders)."""
    storage = await shop_engine_store(tmp_path)
    async with owned_query_engine(storage=storage) as engine:
        await engine.create_dataset_from_query(query=fresh(BASE), name=SAVED)
    return storage
