"""The pre-2.0 vocabulary: its rename tables, the per-document conversion of fresh input (one deprecation per old
spelling per document), the silent conversion of stored documents, and the deprecated Python twins of old names."""

from __future__ import annotations

import functools
import inspect
import re
from collections.abc import Callable, Generator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any, TypeGuard, TypeVar, cast

from pydantic import BaseModel

from slayer.core import deprecation
from slayer.core.enums import PRE_2_0_DATATYPES, DataType, pre_2_0_datatype, stored_datatype
from slayer.core.errors import RelationshipKeyError, SpellingConflictError
from slayer.core.names import NAME_PATTERN, quoted_name

if TYPE_CHECKING:
    from slayer.core.warnings import DeprecatedInputWarning

__all__ = [
    "AGGREGATION_KEYS", "COVERAGE_KEYS", "DATASET_KEYS", "DIMENSION_EXPRESSION_KEYS", "ENTITY_KINDS", "EXTENSION_KEYS",
    "FIELD_KEYS", "FIELD_REF_KEYS", "METRIC_KEYS", "ORDER_ITEM_KEYS", "QUERY_KEYS", "QUERY_METRIC_KEYS",
    "RECOMMENDATION_KEYS", "REFINEMENT_KEYS", "RELATIONSHIP_KEYS", "RESPONSE_ATTRIBUTE_KEYS", "SOURCE_ORIGIN_KEYS",
    "TIME_DIMENSION_KEYS", "WARNING_KEYS",
    "SpellingConflictError", "Spellings", "aggregation", "attribute_used", "bridge_renamed_hooks", "camel_healed",
    "converted_document", "converted_input", "current_spellings", "dataset", "deprecated_spelling", "dimension_entry",
    "extension", "extension_keys", "field", "input_object", "method_twin", "metric", "name_twins", "one_document", "order_item", "query",
    "query_metric", "refinement",
    "relationship", "renamed_arguments", "renamed_input", "renamed_name", "respelled_token", "same_ignoring_deprecations",
    "stored_dataset", "stored_query", "time_dimension",
]

DATASET_KEYS = {"columns": "fields", "measures": "metrics", "joins": "relationships"}
FIELD_KEYS = {"type": "datatype"}
METRIC_KEYS = {"formula": "expression", "type": "datatype"}
AGGREGATION_KEYS = {"formula": "expression"}
RELATIONSHIP_KEYS = {"target_model": "to"}
EXTENSION_KEYS = {"source_name": "dataset", "measures": "metrics", "joins": "relationships"}
QUERY_KEYS = {"source_model": "population", "measures": "metrics", "fields": "metrics", "filters": "filter"}
REFINEMENT_KEYS = {"measures": "metrics", "filters": "filter"}
DIMENSION_EXPRESSION_KEYS = {"expression": "field", "name": "alias"}
QUERY_METRIC_KEYS = {"formula": "metric", "name": "alias", "type": "datatype"}
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
_M = TypeVar("_M", bound=type[BaseModel])


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
        #: Paths of the unknown keys a stored document dropped.
        self.dropped: list[str] = []

    def note(self, old: str, new: str) -> None:
        """Record converting the pre-2.0 form ``old`` to ``new`` (silent in a stored document)."""
        if not self.stored:
            self._seen[(old, new)] = None

    def rename(self, data: dict[str, Any], table: Mapping[str, str], *, record: bool = True) -> dict[str, Any]:
        """``data`` with each key of ``table`` spelled its replacement (each recorded unless ``record`` is off); both
        spellings at once are refused."""
        for old, new in table.items():
            if old not in data:
                continue
            if new in data:
                raise SpellingConflictError(old=old, new=new)
            data = {(new if k == old else k): v for k, v in data.items()}
            if record and not self.stored:
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
    """The enclosing document's spellings, else a new document's; yields ``(spellings, owned)``. A stored document
    nested in fresh input is a document of its own."""
    shared = _DOCUMENT.get()
    if shared is not None and (shared.stored or not stored):
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


def converted_input(data: Any, handler: Callable[[Any], _T], convert: Callable[[Any, Spellings], Any], *,
                    stored: bool) -> _T:
    """``handler`` over ``data`` converted by ``convert``, within the enclosing document."""
    with converted_document(stored=stored) as (spellings, owned):
        out = handler(convert(data, spellings))
        if owned:
            spellings.report()
        return out


#: Each query input object's pre-2.0 key table, by kind.
_INPUT_RENAMES: dict[str, Mapping[str, str]] = {
    "query": QUERY_KEYS, "refinement": REFINEMENT_KEYS, "dimension": DIMENSION_EXPRESSION_KEYS,
    "metric": QUERY_METRIC_KEYS, "time_dimension": TIME_DIMENSION_KEYS, "order_item": ORDER_ITEM_KEYS,
    "extension": EXTENSION_KEYS,
}
#: Each query input object's fields, by kind, registered by its model (``input_object``).
_INPUT_FIELDS: dict[str, frozenset[str]] = {}
_CAMEL = re.compile(r"[a-z][a-z0-9]*(?:[A-Z][a-z0-9]*)+")


def input_object(kind: str) -> Callable[[_M], _M]:
    """Register a query input model's fields as the keys a camelCase key of ``kind`` heals to."""
    def register(model: _M) -> _M:
        _INPUT_FIELDS[kind] = frozenset(model.model_fields)
        return model

    return register


def _snake(key: Any) -> Any:
    if not isinstance(key, str) or not _CAMEL.fullmatch(key):
        return key
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", key).lower()


def camel_healed(data: dict[str, Any], kind: str) -> dict[str, Any]:
    """``data`` with each camelCase key whose snake_case form ``kind`` accepts spelled that way; two keys reaching one
    canonical key are refused."""
    renames = _INPUT_RENAMES[kind]
    accepted = _INPUT_FIELDS.get(kind, frozenset()) | renames.keys()
    healed = {k: s for k in data if (s := _snake(k)) != k and s in accepted}
    if not healed:
        return data
    written: dict[str, str] = {}
    for key in data:
        spelled = healed.get(key, key)
        final = renames.get(spelled, spelled)
        prior = written.setdefault(final, key)
        if prior != key and (key in healed or prior in healed):
            old, new = (key, prior) if key in healed else (prior, key)
            raise SpellingConflictError(old=old, new=new, spelling="camelCase spelling")
    return {healed.get(k, k): v for k, v in data.items()}


def extension_dataset(extension: Any) -> Any:
    """A raw extension's target, spelled ``dataset`` or (pre-2.0) ``source_name``; ``None`` when neither."""
    if not isinstance(extension, dict):
        return None
    return extension.get("dataset", extension.get("source_name"))


def is_extension_object(source: Any) -> TypeGuard[dict[str, Any]]:
    """Whether a raw population is an extension object (its target spelled either way)."""
    return isinstance(source, dict) and ("dataset" in source or "source_name" in source)


def _members(data: dict[str, Any], key: str, convert: Callable[[Any], Any]) -> dict[str, Any]:
    items = data.get(key)
    return {**data, key: [convert(item) for item in items]} if isinstance(items, list) else data


def _renamed(data: Any, table: Mapping[str, str], spellings: Spellings) -> Any:
    return spellings.rename(data, table) if isinstance(data, dict) else data


def _datatype_replacement(new: DataType) -> str:
    """How a pre-2.0 datatype value's replacement reads: its two values when the database type splits it."""
    if new in (DataType.DECIMAL, DataType.FLOAT):
        return "Float (Decimal for an exact numeric database type)"
    if new in (DataType.DATETIME_TZ, DataType.DATETIME):
        return "DateTime (DateTimeTz for a zoned database type)"
    return new.value


def _datatype(data: dict[str, Any], key: str, spellings: Spellings, *, db_type: Any = None) -> dict[str, Any]:
    """``data`` with a pre-2.0 value under ``key`` as its Ossie value (dropped for a pseudo-type)."""
    value = data.get(key)
    if not isinstance(value, str) or value not in PRE_2_0_DATATYPES:
        return data
    new = pre_2_0_datatype(value, db_type=db_type if isinstance(db_type, str) else None)
    if new is None:
        return {k: v for k, v in data.items() if k != key}
    spellings.note(value, _datatype_replacement(new))
    return {**data, key: new.value}


def field(data: Any, spellings: Spellings) -> Any:
    """A dataset field in the Ossie vocabulary; its deprecated ``sql`` is converted by the field itself."""
    if not isinstance(data, dict):
        return data
    data = spellings.rename(data, FIELD_KEYS)
    if data.get("sql") is not None:
        if data.get("expression") is not None:
            raise SpellingConflictError(old="sql", new="expression")
        spellings.note("sql", "expression")
    data = _datatype(data, "datatype", spellings, db_type=data.get("db_type"))
    return _datatype(data, "element_type", spellings)


def metric(data: Any, spellings: Spellings) -> Any:
    """A dataset or extension metric in the Ossie vocabulary."""
    if not isinstance(data, dict):
        return data
    return _datatype(spellings.rename(data, METRIC_KEYS), "datatype", spellings)


def aggregation(data: Any, spellings: Spellings) -> Any:
    return _renamed(data, AGGREGATION_KEYS, spellings)


def relationship(data: Any, spellings: Spellings) -> Any:
    """A relationship in the Ossie vocabulary: ``join_pairs`` become parallel ``from_columns`` / ``to_columns``."""
    if not isinstance(data, dict):
        return data
    data = spellings.rename(data, RELATIONSHIP_KEYS)
    if "join_pairs" not in data:
        return data
    for new in ("from_columns", "to_columns"):
        if new in data:
            raise SpellingConflictError(old="join_pairs", new=new)
    pairs = data["join_pairs"]
    if not isinstance(pairs, list) or not all(isinstance(p, (list, tuple)) and len(p) == 2 for p in pairs):
        raise RelationshipKeyError(summary=f"join_pairs must be [[source_field, target_field], ...], got {pairs!r}")
    spellings.note("join_pairs", "from_columns and to_columns")
    rest = {k: v for k, v in data.items() if k != "join_pairs"}
    return {**rest, "from_columns": [p[0] for p in pairs], "to_columns": [p[1] for p in pairs]}


def _dataset(data: dict[str, Any], spellings: Spellings) -> dict[str, Any]:
    data = spellings.rename(data, DATASET_KEYS)
    data = _members(data, "fields", lambda f: field(f, spellings))
    data = _members(data, "metrics", lambda m: metric(m, spellings))
    data = _members(data, "aggregations", lambda a: aggregation(a, spellings))
    return _members(data, "relationships", lambda r: relationship(r, spellings))


def dataset(data: Any, spellings: Spellings) -> Any:
    """A dataset and its members in the Ossie vocabulary (its queries convert on their own)."""
    return _dataset(data, spellings) if isinstance(data, dict) else data


def _reference_text(data: dict[str, Any]) -> Any:
    """The dotted text of a pre-2.0 field-reference object (``name``, ``model``, ``quoted``); only a ``name``-only
    object may carry an expression, any other names a field by valid names."""
    name, model = data.get("name"), data.get("model")
    if not isinstance(name, str) or not (model is None or isinstance(model, str)):
        return name
    parts = [*model.split("."), *name.split(".")] if model else name.split(".")
    quoted = data.get("quoted")
    flags = quoted if isinstance(quoted, list) and len(quoted) == len(parts) else [False] * len(parts)
    if set(data) != {"name"}:
        bad = next((p for p, q in zip(parts, flags) if not q and not NAME_PATTERN.match(p)), None)
        if bad is not None:
            raise ValueError(f"Invalid name '{bad}': must contain only letters, digits, and underscores, and start "
                             f"with a letter or underscore")
    return ".".join(quoted_name(p) if q else p for p, q in zip(parts, flags))


def _reference_object(value: Any) -> bool:
    return isinstance(value, dict) and isinstance(value.get("name"), str)


def _entry_form(item: dict[str, Any], table: Mapping[str, str], spellings: Spellings) -> dict[str, Any]:
    """``item`` with ``table``'s keys renamed, reported as one deprecation by the form's leading key."""
    old = next((old for old in table if old in item), None)
    if old is not None:
        spellings.note(old, table[old])
    return spellings.rename(item, table, record=False)


def query_metric(item: Any, spellings: Spellings) -> Any:
    """A query metric entry in the Ossie form; the pre-release ``{expression, name}`` is left to be refused."""
    if not isinstance(item, dict):
        return item
    item = camel_healed(item, "metric")
    if "expression" in item:
        return item
    item = _entry_form(item, {"formula": "metric", "name": "alias"}, spellings)
    return _datatype(spellings.rename(item, {"type": "datatype"}), "datatype", spellings)


def dimension_entry(item: Any, spellings: Spellings) -> Any:
    """A query dimension entry in the Ossie form: ``{name, model}`` and ``{expression, name}`` become ``field`` /
    ``alias``; the pre-release ``{name, dataset}`` is left to be refused."""
    if not isinstance(item, dict):
        return item
    item = camel_healed(item, "dimension")
    if "field" in item:
        for old in ("expression", "model"):
            if old in item:
                raise SpellingConflictError(old=old, new="field")
        return item
    if "expression" in item:
        return _entry_form(item, DIMENSION_EXPRESSION_KEYS, spellings)
    if not _reference_object(item):
        return item
    spellings.note("model" if "model" in item else "name", "field")
    rest = {k: v for k, v in item.items() if k not in ("name", "model", "quoted")}
    return {"field": _reference_text(item), **rest}


def _field_text(data: Any, table: Mapping[str, str], spellings: Spellings, *, labelled: bool) -> Any:
    """``data`` with ``table``'s keys renamed to ``field``, a pre-2.0 reference object under one becoming text (its
    ``label`` kept when ``labelled``)."""
    if not isinstance(data, dict):
        return data
    olds = [old for old in table if old in data and table[old] == "field"]
    data = spellings.rename(data, table)
    if olds and _reference_object(data.get("field")):
        if labelled and "label" in data["field"] and "label" not in data:
            data = {**data, "label": data["field"]["label"]}
        data = {**data, "field": _reference_text(data["field"])}
    return data


def _healed(data: Any, kind: str) -> Any:
    return camel_healed(data, kind) if isinstance(data, dict) else data


def time_dimension(data: Any, spellings: Spellings) -> Any:
    return _field_text(_healed(data, "time_dimension"), TIME_DIMENSION_KEYS, spellings, labelled=True)


def order_item(data: Any, spellings: Spellings) -> Any:
    return _field_text(_healed(data, "order_item"), ORDER_ITEM_KEYS, spellings, labelled=False)


def extension_keys(data: Any, spellings: Spellings) -> Any:
    """An extension's own keys in the Ossie vocabulary (its members convert on their own)."""
    return _renamed(_healed(data, "extension"), EXTENSION_KEYS, spellings)


def extension(data: Any, spellings: Spellings) -> Any:
    if not isinstance(data, dict):
        return data
    data = extension_keys(data, spellings)
    data = _members(data, "metrics", lambda m: metric(m, spellings))
    return _members(data, "relationships", lambda r: relationship(r, spellings))


def _query(data: dict[str, Any], spellings: Spellings) -> dict[str, Any]:
    data = spellings.rename(camel_healed(data, "query"), QUERY_KEYS)
    data = _members(data, "metrics", lambda m: query_metric(m, spellings))
    data = _members(data, "dimensions", lambda d: dimension_entry(d, spellings))
    data = _members(data, "time_dimensions", lambda t: time_dimension(t, spellings))
    data = _members(data, "order", lambda o: order_item(o, spellings))
    data = _members(data, "extensions", lambda e: extension(e, spellings))
    population = data.get("population")
    if isinstance(population, dict) and "version" in population:
        pass  # a versioned inline dataset converts through its own migration
    elif isinstance(population, dict) and "source_name" not in population and "dataset" not in population:
        data = {**data, "population": _dataset(population, spellings)}
    elif isinstance(population, dict):
        data = {**data, "population": extension(population, spellings)}
    return data


def refinement(data: Any, spellings: Spellings) -> Any:
    """A query refinement in the Ossie vocabulary."""
    if not isinstance(data, dict):
        return data
    data = spellings.rename(camel_healed(data, "refinement"), REFINEMENT_KEYS)
    data = _members(data, "metrics", lambda m: query_metric(m, spellings))
    data = _members(data, "dimensions", lambda d: dimension_entry(d, spellings))
    data = _members(data, "time_dimensions", lambda t: time_dimension(t, spellings))
    return _members(data, "order", lambda o: order_item(o, spellings))


def query(data: Any, spellings: Spellings) -> Any:
    return _query(data, spellings) if isinstance(data, dict) else data


def _stash(data: Any) -> Any:
    """A stored field or metric with the importer's retired ``meta["osi_datatype"]`` folded into ``datatype`` when it
    names a value of the stored type's kind, then dropped."""
    if not isinstance(data, dict) or not isinstance(data.get("meta"), dict) or "osi_datatype" not in data["meta"]:
        return data
    meta = {k: v for k, v in data["meta"].items() if k != "osi_datatype"}
    stashed = stored_datatype(data["meta"]["osi_datatype"])
    stored = stored_datatype(data.get("datatype"))
    out = {**data, "meta": meta or None}
    if stashed is not None and (stored is None or stashed.kind is stored.kind):
        out["datatype"] = stashed.value
    return out


def stored_dataset(data: dict[str, Any]) -> dict[str, Any]:
    """A stored dataset document's own keys and members in the Ossie vocabulary (its queries convert on their own)."""
    spellings = Spellings(stored=True)
    data = spellings.rename(data, DATASET_KEYS)
    data = _members(data, "fields", lambda f: _stash(_renamed(f, FIELD_KEYS, spellings)))
    data = _members(data, "metrics", lambda m: _stash(_renamed(m, METRIC_KEYS, spellings)))
    return _dataset(data, spellings)


def stored_query(data: dict[str, Any]) -> dict[str, Any]:
    """A stored query document in the Ossie vocabulary, entries, extensions and an inline dataset included."""
    return _query(data, Spellings(stored=True))
