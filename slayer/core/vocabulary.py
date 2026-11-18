"""The pre-2.0 vocabulary: its rename tables, the per-document conversion of fresh input (one deprecation per old
spelling per document), the silent conversion of stored documents, and the deprecated Python twins of old names."""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, TypeVar, cast

from pydantic import BaseModel

from slayer.core import deprecation
from slayer.core.errors import SpellingConflictError

if TYPE_CHECKING:
    from slayer.core.warnings import DeprecatedInputWarning

__all__ = [
    "AGGREGATION_KEYS", "COVERAGE_KEYS", "DATASET_KEYS", "ENTITY_KINDS", "EXTENSION_KEYS", "FIELD_KEYS", "FIELD_REF_KEYS",
    "METRIC_KEYS", "ORDER_ITEM_KEYS", "QUERY_KEYS", "RECOMMENDATION_KEYS", "REFINEMENT_KEYS", "RELATIONSHIP_KEYS",
    "RESPONSE_ATTRIBUTE_KEYS", "SOURCE_ORIGIN_KEYS", "TIME_DIMENSION_KEYS", "WARNING_KEYS",
    "SpellingConflictError", "Spellings", "attribute_used", "bridge_renamed_hooks", "converted_document",
    "current_spellings", "deprecated_spelling", "method_twin", "name_twins", "one_document", "renamed_arguments",
    "renamed_input", "renamed_name", "respelled_token", "same_ignoring_deprecations", "stored_dataset", "stored_query",
]

DATASET_KEYS = {"columns": "fields", "measures": "metrics", "joins": "relationships"}
FIELD_KEYS = {"type": "datatype"}
METRIC_KEYS = {"formula": "expression", "type": "datatype"}
AGGREGATION_KEYS = {"formula": "expression"}
RELATIONSHIP_KEYS = {"target_model": "to"}
EXTENSION_KEYS = {"measures": "metrics", "joins": "relationships"}
QUERY_KEYS = {"source_model": "population", "measures": "metrics", "fields": "metrics", "filters": "filter"}
REFINEMENT_KEYS = {"measures": "metrics", "filters": "filter"}
TIME_DIMENSION_KEYS = {"dimension": "field", "column": "field"}
ORDER_ITEM_KEYS = {"column": "field", "raw_formula": "raw_expression"}
FIELD_REF_KEYS = {"model": "dataset"}
SOURCE_ORIGIN_KEYS = {"agg_column_names": "agg_field_names"}
RESPONSE_ATTRIBUTE_KEYS = {"measures": "metrics"}
RECOMMENDATION_KEYS = {"root_model": "population"}
COVERAGE_KEYS = {"model_name": "dataset_name"}
#: Entity kinds as ``inspect`` and search name them.
ENTITY_KINDS = {"model": "dataset", "column": "field", "measure": "metric"}
#: A warning payload's pre-2.0 keys, by its ``kind``.
WARNING_KEYS: dict[str, dict[str, str]] = {
    "associated": {"measure": "metric"},
    "broadcast": {"measure": "metric"},
    "broken_dependent": {"model": "dataset", "column": "field"},
    "degenerate_reaggregation": {"measure": "metric"},
    "extension_field_inlined": {"column": "field"},
    "library_name_shadow": {"model": "dataset"},
    "name_collision": {"model": "dataset"},
    "narrowed_access": {"model": "dataset"},
    "semi_join_pushed": {"measure": "metric"},
    "spine_relationship_dropped": {"model": "dataset", "join": "relationship"},
    "unrelated_dataset": {"measures": "metrics"},
    "whole_periods_non_nesting": {"column": "field"},
}

_T = TypeVar("_T")
_F = TypeVar("_F", bound=Callable[..., Any])


def _deprecation(old: str, new: str) -> DeprecatedInputWarning:
    """The one tagged site of every pre-2.0 vocabulary spelling and name."""
    return deprecation.deprecated_input(old, new, removal="3.0")


def same_ignoring_deprecations(a: BaseModel, b: BaseModel) -> bool:
    """``a == b`` (same type) disregarding the pre-2.0 spellings each was read from."""
    def private(model: BaseModel) -> dict[str, Any]:
        return {k: v for k, v in (model.__pydantic_private__ or {}).items() if k != "_deprecated_inputs"}

    return a.__dict__ == b.__dict__ and private(a) == private(b) and a.__pydantic_extra__ == b.__pydantic_extra__


def deprecated_spelling(old: str, new: str) -> DeprecatedInputWarning:
    """Report reading the pre-2.0 spelling ``old`` as ``new`` (a surface token, label or reference)."""
    return _deprecation(old, new)


def respelled_token(token: str, table: Mapping[str, str], reported: list[DeprecatedInputWarning]) -> str:
    """``token``'s replacement in ``table`` (its deprecation appended to ``reported``), else ``token``."""
    if token not in table:
        return token
    reported.append(_deprecation(token, table[token]))
    return table[token]


def renamed_arguments(kwargs: Mapping[str, Any], *, renames: Mapping[str, str]) -> dict[str, Any]:
    """``kwargs`` with each keyword of ``renames`` spelled its replacement; both spellings at once are refused."""
    out = dict(kwargs)
    for old, new in renames.items():
        if old in out:
            if new in out:
                raise SpellingConflictError(old=old, new=new)
            out[new] = out.pop(old)
    return out


def _with_keywords(fn: _F, renames: Mapping[str, str]) -> _F:
    """``fn`` (sync or async) called with its keywords of ``renames`` respelled."""
    if inspect.iscoroutinefunction(fn):
        @functools.wraps(fn)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            return await fn(*args, **renamed_arguments(kwargs, renames=renames))

        return cast(_F, async_wrapper)

    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return fn(*args, **renamed_arguments(kwargs, renames=renames))

    return cast(_F, wrapper)


def method_twin(new: str, *, arguments: Mapping[str, str] | None = None) -> Callable[[_F], _F]:
    """A declared pre-2.0 method stub as one deprecation, then ``self.<new>`` with its old keywords respelled."""
    renames = dict(arguments or {})

    def decorate(stub: _F) -> _F:
        @functools.wraps(stub)
        def twin(self: Any, *args: Any, **kwargs: Any) -> Any:
            kwargs = renamed_arguments(kwargs, renames=renames)
            _deprecation(f"{stub.__name__}()", f"{new}()")
            return getattr(self, new)(*args, **kwargs)

        return cast(_F, twin)

    return decorate


def attribute_used(owner: object, *, old: str, new: str) -> None:
    """Report a read or write of the pre-2.0 attribute ``old`` of ``owner``."""
    _deprecation(f"{type(owner).__name__}.{old}", f"{type(owner).__name__}.{new}")


def renamed_name(
    name: str, *, namespace: Mapping[str, Any], renames: Mapping[str, str],
    arguments: Mapping[str, Mapping[str, str]] | None = None,
) -> Any:
    """A module's replacement of its pre-2.0 class or function ``name``, with one deprecation; a function's old
    keywords of ``arguments[name]`` are respelled."""
    module = namespace["__name__"]
    if name not in renames:
        raise AttributeError(f"module {module!r} has no attribute {name!r}")
    _deprecation(f"{module}.{name}", f"{module}.{renames[name]}")
    target = namespace[renames[name]]
    keywords = (arguments or {}).get(name)
    return _with_keywords(target, keywords) if keywords else target


def name_twins(
    namespace: Mapping[str, Any], renames: Mapping[str, str], *,
    arguments: Mapping[str, Mapping[str, str]] | None = None,
) -> Callable[[str], Any]:
    """A module ``__getattr__`` resolving each pre-2.0 name of ``renames`` (``renamed_name``)."""
    return functools.partial(renamed_name, namespace=namespace, renames=renames, arguments=arguments)


def bridge_renamed_hooks(
    cls: type, renames: Mapping[str, str], *, arguments: Mapping[str, Mapping[str, str]] | None = None,
) -> None:
    """Route each new hook name to ``cls``'s own override of its pre-2.0 name, with one deprecation per class;
    ``arguments`` maps a new hook's keywords to the override's (new → old)."""
    bridged = [(old, new) for old, new in renames.items() if old in cls.__dict__ and new not in cls.__dict__]
    for old, new in bridged:
        hook = cls.__dict__[old]
        keywords = (arguments or {}).get(new)
        setattr(cls, new, _with_keywords(hook, keywords) if keywords else hook)
    if bridged:
        _deprecation(f"{cls.__name__} overriding {', '.join(old for old, _ in bridged)}",
                     ", ".join(new for _, new in bridged))


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
                self._reported[(old, new)] = _deprecation(old, new)
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
    if isinstance(population, dict) and "version" in population:
        pass  # a versioned inline dataset converts through its own migration
    elif isinstance(population, dict) and "source_name" not in population:
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
