"""The pre-2.0 vocabulary: its rename tables, the per-document conversion of fresh input (one deprecation per old
spelling per document), and the silent conversion of stored documents."""

from __future__ import annotations

from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, TypeVar

from slayer.core.deprecation import deprecated_input
from slayer.core.errors import SpellingConflictError
from slayer.core.warnings import DeprecatedInputWarning

__all__ = [
    "AGGREGATION_KEYS", "DATASET_KEYS", "EXTENSION_KEYS", "FIELD_KEYS", "FIELD_REF_KEYS", "METRIC_KEYS",
    "ORDER_ITEM_KEYS", "QUERY_KEYS", "REFINEMENT_KEYS", "RELATIONSHIP_KEYS", "TIME_DIMENSION_KEYS",
    "SpellingConflictError", "Spellings", "converted_document", "current_spellings", "one_document",
    "renamed_input", "stored_dataset", "stored_query",
]

DATASET_KEYS = {"columns": "fields", "measures": "metrics", "joins": "relationships"}
FIELD_KEYS = {"type": "datatype"}
METRIC_KEYS = {"formula": "expression", "type": "datatype"}
AGGREGATION_KEYS = {"formula": "expression"}
RELATIONSHIP_KEYS = {"target_model": "to"}
EXTENSION_KEYS = {"measures": "metrics", "joins": "relationships"}
QUERY_KEYS = {"source_model": "population", "measures": "metrics", "filters": "filter"}
REFINEMENT_KEYS = {"measures": "metrics", "filters": "filter"}
TIME_DIMENSION_KEYS = {"dimension": "field", "column": "field"}
ORDER_ITEM_KEYS = {"column": "field", "raw_formula": "raw_expression"}
FIELD_REF_KEYS = {"model": "dataset"}

_T = TypeVar("_T")


class Spellings:
    """The pre-2.0 spellings one input document converted; a stored document converts silently."""

    def __init__(self, *, stored: bool) -> None:
        self.stored = stored
        self._seen: dict[tuple[str, str], None] = {}
        self._reported: dict[tuple[str, str], DeprecatedInputWarning] = {}

    def rename(self, data: dict[str, Any], table: Mapping[str, str]) -> dict[str, Any]:
        """``data`` with each key of ``table`` spelled its replacement; both spellings at once are refused."""
        for old, new in table.items():
            if old not in data:
                continue
            if new in data:
                raise SpellingConflictError(old=old, new=new)
            data = {(new if k == old else k): v for k, v in data.items()}
            if not self.stored:
                self._seen[(old, new)] = None
        return data

    def report(self) -> tuple[DeprecatedInputWarning, ...]:
        """One deprecation per spelling seen so far, each emitted once however often this is called."""
        for old, new in self._seen:
            if (old, new) not in self._reported:
                self._reported[(old, new)] = deprecated_input(old, new, removal="3.0")
        return tuple(self._reported.values())


_DOCUMENT: ContextVar[Spellings | None] = ContextVar("slayer_vocabulary_document", default=None)


def current_spellings() -> Spellings | None:
    """The spellings of the document being read, if any."""
    return _DOCUMENT.get()


@contextmanager
def converted_document(*, stored: bool) -> Generator[tuple[Spellings, bool]]:
    """The enclosing document's spellings, else a new document's; yields ``(spellings, owned)``."""
    shared = _DOCUMENT.get()
    if shared is not None:
        yield shared, False
        return
    token = _DOCUMENT.set(Spellings(stored=stored))
    try:
        yield _DOCUMENT.get() or Spellings(stored=stored), True
    finally:
        _DOCUMENT.reset(token)


@contextmanager
def one_document() -> Generator[None]:
    """Read the fresh input within the block (e.g. a list of stages) as one document."""
    with converted_document(stored=False):
        yield


def renamed_input(data: Any, handler: Callable[[Any], _T], table: Mapping[str, str], *, stored: bool) -> _T:
    """``handler`` over ``data`` with its pre-2.0 keys of ``table`` respelled, within the enclosing document."""
    with converted_document(stored=stored) as (spellings, owned):
        if isinstance(data, dict):
            data = spellings.rename(data, table)
        out = handler(data)
        if owned:
            spellings.report()
        return out


def _members(data: dict[str, Any], key: str, convert: Callable[[Any], Any]) -> dict[str, Any]:
    items = data.get(key)
    return {**data, key: [convert(item) for item in items]} if isinstance(items, list) else data


def _renamed(data: Any, table: Mapping[str, str], spellings: Spellings) -> Any:
    return spellings.rename(data, table) if isinstance(data, dict) else data


def _field_ref(value: Any, spellings: Spellings) -> Any:
    return _renamed(value, FIELD_REF_KEYS, spellings) if isinstance(value, dict) and "name" in value else value


def _dataset(data: dict[str, Any], spellings: Spellings) -> dict[str, Any]:
    data = spellings.rename(data, DATASET_KEYS)
    data = _members(data, "fields", lambda f: _renamed(f, FIELD_KEYS, spellings))
    data = _members(data, "metrics", lambda m: _renamed(m, METRIC_KEYS, spellings))
    data = _members(data, "aggregations", lambda a: _renamed(a, AGGREGATION_KEYS, spellings))
    return _members(data, "relationships", lambda r: _renamed(r, RELATIONSHIP_KEYS, spellings))


def _time_dimension(data: Any, spellings: Spellings) -> Any:
    data = _renamed(data, TIME_DIMENSION_KEYS, spellings)
    return {**data, "field": _field_ref(data["field"], spellings)} if isinstance(data, dict) and "field" in data else data


def _order_item(data: Any, spellings: Spellings) -> Any:
    data = _renamed(data, ORDER_ITEM_KEYS, spellings)
    return {**data, "field": _field_ref(data["field"], spellings)} if isinstance(data, dict) and "field" in data else data


def _extension(data: Any, spellings: Spellings) -> Any:
    if not isinstance(data, dict):
        return data
    data = spellings.rename(data, EXTENSION_KEYS)
    data = _members(data, "metrics", lambda m: _renamed(m, METRIC_KEYS, spellings))
    return _members(data, "relationships", lambda r: _renamed(r, RELATIONSHIP_KEYS, spellings))


def _query(data: dict[str, Any], spellings: Spellings) -> dict[str, Any]:
    data = spellings.rename(data, QUERY_KEYS)
    data = _members(data, "metrics", lambda m: _renamed(m, METRIC_KEYS, spellings))
    data = _members(data, "dimensions", lambda d: _field_ref(d, spellings))
    data = _members(data, "time_dimensions", lambda t: _time_dimension(t, spellings))
    data = _members(data, "order", lambda o: _order_item(o, spellings))
    data = _members(data, "extensions", lambda e: _extension(e, spellings))
    population = data.get("population")
    if isinstance(population, dict) and "source_name" not in population:
        data = {**data, "population": _dataset(population, spellings)}
    elif isinstance(population, dict):
        data = {**data, "population": _extension(population, spellings)}
    return data


def stored_dataset(data: dict[str, Any]) -> dict[str, Any]:
    """A stored dataset document's own keys and members in the Ossie vocabulary (its queries convert on their own)."""
    return _dataset(data, Spellings(stored=True))


def stored_query(data: dict[str, Any]) -> dict[str, Any]:
    """A stored query document in the Ossie vocabulary, entries, extensions and an inline dataset included."""
    return _query(data, Spellings(stored=True))
