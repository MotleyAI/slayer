"""Typed identity primitives for the resolution pipeline.

Identity is structural: two expression occurrences with the same key intern to
the same slot. Keys carry only what's needed to decide "are these the same slot?".
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal
from enum import IntEnum
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    FrozenSet,
    Sequence,
    TypeGuard,
    AbstractSet,
    Callable,
    ClassVar,
    Iterable,
    Iterator,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Self,
    Tuple,
    TypeVar,
    Union,
    cast,
    get_args,
)

from typing_extensions import TypeIs
from pydantic import BaseModel, ConfigDict, Field, field_validator

from slayer.core.enums import (
    AggregationValueClass,
    DataType,
    DatePart,
    INTEGER_AGGREGATIONS,
    LIST_AGGREGATIONS,
    NUMERIC_DATATYPES,
    NUMERIC_ONLY_AGGREGATIONS,
    RANKED_AGGREGATIONS,
    RANK_FAMILY_TRANSFORMS,
    SUB_DAY_GRANULARITIES,
    TEMPORAL_DATATYPES,
    TimeGranularity,
    TypeKind,
    classify_aggregation,
    kind_of,
)
from slayer.core.format import NumberFormat
from slayer.core.granularity import CustomGranularity, Granularity, bucket_type
from slayer.core.spine_names import TIME_SPINE_DATASET
from slayer.core.time_points import parse_temporal_value


# Regroup-consumed aggregates are substituted by a FieldKey with this leaf
# prefix; a source dataset may not declare a real field with it during regroup.
REGROUP_LEAF_PREFIX = "__regroup__"

#: A path's first hop naming an aggregation's frame dataset: reached by no join from the reading dataset (reserved).
ANCHOR_TOKEN_PREFIX = "__slayer_anchor__"


def anchor_token(name: str) -> str:
    """The anchor hop to dataset ``name``."""
    return ANCHOR_TOKEN_PREFIX + name


def token_anchor(token: str) -> Optional[str]:
    """The dataset an anchor hop ``token`` names, else ``None``."""
    return token[len(ANCHOR_TOKEN_PREFIX):] if token.startswith(ANCHOR_TOKEN_PREFIX) else None


#: A virtual spine hop's token while a declared name shadows its own (reserved: never a declared name).
SPINE_TOKEN = "__slayer_spine__"


def display_token(token: str) -> str:
    """A path token as a reference spells it: an anchor hop by its dataset, the spine hop ``time_spine``."""
    return TIME_SPINE_DATASET if token == SPINE_TOKEN else token_anchor(token) or token


class ScalarSpec(BaseModel, frozen=True):
    """One allowlisted Mode-B scalar: accepted argument counts (``max_args=None``
    is variadic), whether its call is literally the same SQL function, its fixed
    result type (``None``: one of its value arguments' types), whether it reads
    every argument as a number, and the argument counts at which Ossie Layer 1 defines
    it with SLayer's meaning (``ossie_arg_counts``; a variadic one's least, larger
    counts included)."""

    min_args: int
    max_args: Optional[int] = None
    sql_passthrough: bool = True
    result: Optional[DataType] = None
    numeric: bool = False
    ossie_arg_counts: frozenset[int] = frozenset()

    def ossie_admits(self, argc: int) -> bool:
        """Whether Ossie defines this function at ``argc`` arguments."""
        if self.max_args is None and self.ossie_arg_counts:
            return argc >= min(self.ossie_arg_counts)
        return argc in self.ossie_arg_counts


def _spec(
    low: int, high: Optional[int], *, sql: bool = True, result: Optional[DataType] = None, numeric: bool = False,
    ossie: tuple[int, ...] = (),
) -> ScalarSpec:
    return ScalarSpec(min_args=low, max_args=high, sql_passthrough=sql, result=result, numeric=numeric,
                      ossie_arg_counts=frozenset(ossie))


_TEXT, _BOOL, _INT, _DOUBLE = DataType.STRING, DataType.BOOLEAN, DataType.INTEGER, DataType.FLOAT


def _library(
    low: int, high: Optional[int], *, result: Optional[DataType] = None, numeric: bool = False,
    ossie: tuple[int, ...] = (),
) -> ScalarSpec:
    """A library function: the same-named SQL function, lowered per dialect to its one meaning."""
    return _spec(low, high, result=result, numeric=numeric, ossie=ossie)


# The one canonical Mode-B scalar allowlist (C12); every other scalar set derives
# from it. Arity is enforced because sqlglot silently mis-handles wrong-arity calls.
SCALAR_SPECS: Mapping[str, ScalarSpec] = MappingProxyType({
    "nullif": _spec(2, 2, ossie=(2,)), "coalesce": _spec(1, None, ossie=(2,)), "ifnull": _spec(2, 2, ossie=(2,)),
    "ln": _spec(1, 1, numeric=True, ossie=(1,)), "log10": _spec(1, 1, numeric=True, ossie=(1,)),
    "log2": _spec(1, 1, numeric=True),
    "log": _spec(1, 2, numeric=True, ossie=(2,)), "exp": _spec(1, 1, numeric=True, ossie=(1,)),
    "sqrt": _spec(1, 1, numeric=True, ossie=(1,)),
    "pow": _spec(2, 2, numeric=True), "power": _spec(2, 2, numeric=True, ossie=(2,)),
    "abs": _spec(1, 1, numeric=True, ossie=(1,)), "floor": _spec(1, 1, numeric=True, ossie=(1,)),
    "ceil": _spec(1, 1, numeric=True, ossie=(1,)),
    "round": _spec(1, 2, numeric=True, ossie=(2,)), "ceiling": _spec(1, 1, numeric=True, ossie=(1,)),
    "sign": _spec(1, 1, numeric=True, ossie=(1,)),
    "trunc": _spec(1, 1, numeric=True),  # 2-arg form silently drops digits on SQLite
    "mod": _spec(2, 2, numeric=True, ossie=(2,)),
    # Scalar min/max, NULLs ignored; min two args because 1-arg MAX/MIN parse as the aggregate on SQLite.
    "greatest": _library(2, None, ossie=(2,)), "least": _library(2, None, ossie=(2,)),
    "lower": _spec(1, 1, result=_TEXT, ossie=(1,)), "upper": _spec(1, 1, result=_TEXT, ossie=(1,)),
    "trim": _spec(1, 1, result=_TEXT, ossie=(1,)),
    "length": _spec(1, 1, result=_INT, ossie=(1,)),
    # Trims take the string only; 2-arg strip-set form deferred (DEV-1793).
    "ltrim": _spec(1, 1, result=_TEXT, ossie=(1,)), "rtrim": _spec(1, 1, result=_TEXT, ossie=(1,)),
    "replace": _spec(3, 3, result=_TEXT, ossie=(3,)), "substr": _spec(2, 3, result=_TEXT),
    "substring": _spec(2, 3, result=_TEXT, ossie=(3,)), "instr": _spec(2, 2, result=_INT),
    "concat": _spec(1, None, result=_TEXT, ossie=(2,)),
    "left": _library(2, 2, result=_TEXT, ossie=(2,)), "right": _library(2, 2, result=_TEXT, ossie=(2,)),
    "starts_with": _library(2, 2, result=_BOOL), "ends_with": _library(2, 2, result=_BOOL),
    "contains": _library(2, 2, result=_BOOL, ossie=(2,)),
    "contains_any": _library(2, 2, result=_BOOL), "contains_all": _library(2, 2, result=_BOOL),
    "lpad": _library(2, 3, result=_TEXT), "rpad": _library(2, 3, result=_TEXT),
    "reverse": _library(1, 1, result=_TEXT), "repeat": _library(2, 2, result=_TEXT),
    "split_part": _library(3, 3, result=_TEXT, ossie=(3,)), "concat_ws": _library(2, None, result=_TEXT),
    "chr": _library(1, 1, result=_TEXT), "ascii": _library(1, 1, result=_INT),
    "div": _library(2, 2, numeric=True), "pi": _library(0, 0, result=_DOUBLE, ossie=(0,)),
    **{name: _library(1, 1, result=_DOUBLE, numeric=True, ossie=(1,))
       for name in ("sin", "cos", "tan", "asin", "acos", "atan")},
    "atan2": _library(2, 2, result=_DOUBLE, numeric=True, ossie=(2,)),
    "cbrt": _library(1, 1, result=_DOUBLE, numeric=True),
    "regexp_like": _library(2, 3, result=_BOOL, ossie=(2,)),
    "regexp_extract": _library(2, 3, result=_TEXT, ossie=(2,)),
    "regexp_replace": _library(3, 4, result=_TEXT, ossie=(3,)), "json_value": _library(2, 2, result=_TEXT),
    "like": _spec(2, 2, sql=False, result=_BOOL),  # emits the LIKE operator
    "ilike": _spec(2, 2, sql=False, result=_BOOL),  # case-insensitive LIKE on every dialect
    "iif": _spec(3, 3, sql=False),  # emits CASE WHEN
    "date_part": _spec(2, 2, sql=False, result=_INT, ossie=(2,)),
    "date_diff": _spec(3, 3, sql=False, result=_INT),
    "date_add": _spec(3, 3, sql=False),
    "interval": _spec(2, 2, sql=False),  # parse-time spelling of date_add
    "current_date": _spec(0, 0, sql=False, ossie=(0,)),
    "now": _spec(0, 0, sql=False),
})

SCALAR_FUNCTIONS: frozenset[str] = frozenset(SCALAR_SPECS)
# List functions: over a list first argument (``contains`` / ``length`` also take TEXT).
LIST_FUNCTIONS: frozenset[str] = frozenset({"contains", "contains_any", "contains_all", "length"})
LIST_ONLY_FUNCTIONS: frozenset[str] = frozenset({"contains_any", "contains_all"})
# Names the parser rewrites into a library call or a time bucket; reserved like the functions.
SCALAR_SPELLINGS: frozenset[str] = frozenset({
    "strpos", "iff", "cardinality", "array_length", "array_contains", "datediff", "dateadd", "current_timestamp",
    "date_trunc", "json_extract", "json_extract_scalar", "json_extract_path_text",
    "jsonb_extract_path_text",
})
RESERVED_SCALAR_NAMES: frozenset[str] = SCALAR_FUNCTIONS | SCALAR_SPELLINGS
TEXT_RESULT_SCALARS: frozenset[str] = frozenset(n for n, s in SCALAR_SPECS.items() if s.result is DataType.STRING)
BOOLEAN_RESULT_SCALARS: frozenset[str] = frozenset(n for n, s in SCALAR_SPECS.items() if s.result is DataType.BOOLEAN)
# Scalars that read every argument as a number.
NUMERIC_ARG_SCALARS: frozenset[str] = frozenset(n for n, s in SCALAR_SPECS.items() if s.numeric)
SCALAR_FUNCTION_ARITY: dict[str, tuple[int, Optional[int]]] = {
    name: (spec.min_args, spec.max_args) for name, spec in SCALAR_SPECS.items()
}
# Names whose Mode-B call is the same SQL function (OSI carries these verbatim).
SCALAR_PASSTHROUGH: frozenset[str] = frozenset(
    name for name, spec in SCALAR_SPECS.items() if spec.sql_passthrough
)
# Scalars whose value depends on when the query runs.
CLOCK_FUNCTIONS: frozenset[str] = frozenset({"current_date", "now"})
# Date scalars taking a closed unit word: (position of the unit argument, its vocabulary).
DATE_UNIT_ARGS: Mapping[str, tuple[int, type[DatePart] | type[TimeGranularity]]] = MappingProxyType({
    "date_part": (0, DatePart),
    "date_diff": (0, TimeGranularity),
    "date_add": (2, TimeGranularity),
    "interval": (1, TimeGranularity),
})
# Date scalars' DATE/TIMESTAMP operand positions.
DATE_OPERAND_ARGS: Mapping[str, tuple[int, ...]] = MappingProxyType({
    "date_part": (1,), "date_diff": (1, 2), "date_add": (0,),
})
DATE_ADD_COUNT_ARG = 1
if not CLOCK_FUNCTIONS | set(DATE_UNIT_ARGS) <= SCALAR_FUNCTIONS:  # pragma: no cover
    raise RuntimeError("derived scalar sets must be subsets of SCALAR_SPECS")


def check_scalar_arity(*, name: str, argc: int) -> Optional[str]:
    """Return an error message when ``name`` cannot take ``argc`` arguments."""
    bounds = SCALAR_FUNCTION_ARITY.get(name)
    if bounds is None:
        return None
    low, high = bounds
    if low <= argc and (high is None or argc <= high):
        return None
    if low == high:
        expected = f"{low}"
    elif high is None:
        expected = f"{low} or more"
    else:
        expected = f"{low} to {high}"
    plural = "" if low == high == 1 else "s"
    return (
        f"Scalar function {name!r} takes {expected} argument{plural}; "
        f"got {argc}."
    )


class Phase(IntEnum):
    """Resolution phase of a ValueKey (P8); filters/arithmetic take the max phase."""

    ROW = 0
    AGGREGATE = 1
    POST = 2


Scalar = Union[Decimal, str, bool, None, datetime, date]


def normalize_scalar(value):
    """Canonicalize a raw scalar before keying.

    bool checked before int (bool is-a int); int/float become Decimal (float via
    str, so it lands on the displayed decimal form); dates pass through. TypeError
    for anything else.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (Decimal, date)):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, str):
        return value
    raise TypeError(
        f"Cannot normalize scalar of type {type(value).__name__!r}: "
        f"only int/float/Decimal/str/bool/None/date/datetime are accepted (got {value!r})."
    )


class _FrozenKey(BaseModel, frozen=True):
    """Common config for the typed-key family: frozen (hashable, immutable).

    Every kind overrides the total-traversal protocol: ``children()`` yields the
    directly embedded value keys (scalars are never children); ``map_children``
    rebuilds one level with ``fn`` applied at each ``children()`` position,
    returning ``self`` when no child changed identity (``is``).
    """

    def children(self) -> Tuple["ValueKey", ...]:
        raise NotImplementedError(
            f"{type(self).__name__} must override children(): every value-key "
            f"kind implements the total-traversal protocol."
        )

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "_FrozenKey":
        raise NotImplementedError(
            f"{type(self).__name__} must override map_children(): every "
            f"value-key kind implements the total-traversal protocol."
        )


class _ChildMapper:
    """``map_children`` helper: applies ``fn`` to keys, passes scalars through,
    and records whether any result changed identity."""

    def __init__(self, fn: Callable[["ValueKey"], "ValueKey"]) -> None:
        self._fn = fn
        self.changed = False

    def __call__(self, value):
        if not isinstance(value, _FrozenKey):
            return value
        new = self._fn(cast("ValueKey", value))  # every concrete _FrozenKey is a ValueKey member
        if new is not value:
            self.changed = True
        return new


class _LeafKey(_FrozenKey, frozen=True):
    """Traversal leaf: no embedded keys."""

    def children(self) -> Tuple["ValueKey", ...]:
        return ()

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> Self:
        return self


def _typed_leaf(v):
    """Wrap a scalar leaf in a (type_tag, value) pair so hash/eq don't conflate
    numerically-equal values of different types (True == 1 == Decimal("1")).
    """
    if isinstance(v, bool):
        return ("__bool__", v)
    if v is None:
        return ("__none__", None)
    if isinstance(v, Decimal):
        return ("__num__", v)
    if isinstance(v, str):
        return ("__str__", v)
    if isinstance(v, datetime):
        return ("__timestamp__", v)
    if isinstance(v, date):
        return ("__date__", v)
    return ("__key__", v)


def _typed_args(args):
    return tuple(_typed_leaf(a) for a in args)


def _typed_kwargs(kwargs):
    return tuple((k, _typed_leaf(v)) for k, v in kwargs)


class FieldKey(_LeafKey, frozen=True):
    """Row-level reference to a base field on a dataset.

    ``path`` is the relationship walk from the query's source dataset to the terminal
    dataset — empty for local refs, non-empty for joined refs; ``leaf`` is the
    field name. Local and cross-dataset refs share this shape (P3).
    """

    path: Tuple[str, ...] = ()
    leaf: str
    #: A query reference anchored off its reading dataset: the anchor's name and canonical route (Principle 1).
    anchor: Optional[Tuple[str, ...]] = None

    @property
    def phase(self) -> Phase:
        return Phase.ROW


class FieldSqlKey(_LeafKey, frozen=True):
    """Reference to a derived field (whose ``DatasetField.sql`` is set).

    The expansion AST is recovered from the dataset at binding time — the key only
    carries identity. ``path`` works like ``FieldKey.path``.
    """

    path: Tuple[str, ...] = ()
    dataset: str
    field_name: str
    anchor: Optional[Tuple[str, ...]] = None

    @property
    def phase(self) -> Phase:
        return Phase.ROW


class SqlFragmentKey(_FrozenKey, frozen=True):
    """A bound aggregation-parameter expression: canonical Mode-A ``template`` whose
    ``{r<i>}`` placeholders stand for ``refs[i]`` (absolute field keys). Phase ROW."""

    template: str
    refs: Tuple[FieldKey | FieldSqlKey, ...] = ()

    @property
    def phase(self) -> Phase:
        return Phase.ROW

    def children(self) -> Tuple["ValueKey", ...]:
        return self.refs

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "SqlFragmentKey":
        m = _ChildMapper(fn)
        refs = tuple(m(r) for r in self.refs)
        return self.model_copy(update={"refs": refs}) if m.changed else self


class TimeTruncKey(_FrozenKey, frozen=True):
    """A row-level value truncated to a granularity, keyed by (source, granularity).

    ``source`` is any row-level key, a field being the one-leaf case.
    ``granularity`` is resolved: built-in, or a datasource definition.
    """

    source: "ValueKey"
    granularity: Granularity

    @property
    def phase(self) -> Phase:
        return Phase.ROW

    def children(self) -> Tuple["ValueKey", ...]:
        return (self.source,)

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "TimeTruncKey":
        m = _ChildMapper(fn)
        source = m(self.source)
        return self.model_copy(update={"source": source}) if m.changed else self


def field_leaf(col: FieldKey | FieldSqlKey) -> str:
    """Leaf field name of a ``TimeTruncKey.source`` regardless of kind."""
    return getattr(col, "leaf", None) or getattr(col, "field_name")


def field_path(col: FieldKey | FieldSqlKey) -> Tuple[str, ...]:
    """Relationship path of a ``TimeTruncKey.source`` regardless of kind."""
    return col.path


class StarKey(_LeafKey, frozen=True):
    """Sentinel source for ``count(*)`` aggregations.

    ``path`` is empty for the local star and non-empty for a cross-dataset star
    (``count(customers.*)`` → ``path=("customers",)``), mirroring ``FieldKey.path``.
    """

    path: Tuple[str, ...] = ()

    @property
    def phase(self) -> Phase:
        return Phase.ROW


class LiteralKey(_LeafKey, frozen=True):
    """Identity for a literal value inside an expression tree.

    Scalar normalization happens at the call site via ``normalize_scalar`` so
    equality is type-stable (``LiteralKey(Decimal(1))`` and ``LiteralKey(True)``
    are distinct). Phase ROW.
    """

    value: Scalar = None

    @property
    def phase(self) -> Phase:
        return Phase.ROW

    def __hash__(self) -> int:
        return hash(("LiteralKey", _typed_leaf(self.value)))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LiteralKey):
            return NotImplemented
        return _typed_leaf(self.value) == _typed_leaf(other.value)


# ---------------------------------------------------------------------------
# Aggregate / Transform / Arithmetic / ScalarCall
# ---------------------------------------------------------------------------


# Beyond field / star sources, an aggregate may take a row-level
# same-dataset EXPRESSION source (``sum(amount - cost)``) — the bound tree reuses
# the existing row-level composites, so hash/equality/serialization come from
# the canonical tree and formatting variants intern to one key.
# A re-aggregation's source resolves entirely to attached values, so
# the source may itself be an ``AggregateKey`` (or a composite of them — carried
# by the Arithmetic/ScalarCall operands, which already admit any ValueKey).
AggregateSource = Union[
    FieldKey, FieldSqlKey, StarKey, TimeTruncKey,
    "ArithmeticKey", "ScalarCallKey", "LiteralKey", "AggregateKey", "TransformKey",
    "InKey", "TimePointCmpKey", "CastKey", "ListSetKey", "WindowKey",
]
# Positional and kwarg arg values share one union: `last(amount, created_at)` binds a
# ranking field, `weighted_avg(weight=qty * 2)` a row-level expression,
# `weighted_avg(weight=count(id, partition_by=…))` an aggregate, and
# `weighted_avg(weight=rank(sum(amount, partition_by=…), direction='desc'))` a grained transform —
# all via `_bind_agg_arg`.
_AggregateArgValue = Union[
    FieldKey, FieldSqlKey, SqlFragmentKey, "AggregateKey", "TransformKey",
    "ScalarCallKey", "ArithmeticKey", "CastKey", "InKey", TimeTruncKey, "LiteralKey", "WindowKey",
    Decimal, str, bool, None,
]
_AggregateKwargValue = _AggregateArgValue


def _sort_kwargs_tuple(v):
    """Validator helper: canonicalize a kwargs tuple to sorted order by key."""
    if v is None:
        return ()
    return tuple(sorted(v, key=lambda kv: kv[0]))


class AggregateKey(_FrozenKey, frozen=True):
    """Identity for an aggregation slot (P3).

    Local and cross-dataset aggregates share this shape: ``source.path`` empty for
    local, non-empty for joined. ``args``/``kwargs`` carry parameters (numeric
    scalars pre-normalized to Decimal; identifier kwargs arrive as
    ``FieldKey``/``FieldSqlKey``; kwargs canonicalized to sorted order). A
    ``DatasetField.filter`` rides its source ``FieldSqlKey``, not the
    aggregate.

    ``locus`` names where a cross-dataset aggregate is evaluated:
    ``"target"`` (default) rooted at the target, one value per target row-group;
    ``"host"`` rooted at the host, one value per host group (needed by the
    order wrap). It participates in identity — the two are different
    values (global vs per-group).
    """

    source: AggregateSource
    agg: str
    args: Tuple[_AggregateArgValue, ...] = ()
    kwargs: Tuple[Tuple[str, _AggregateKwargValue], ...] = ()
    locus: Literal["target", "host"] = "target"
    # None = grain inherited from context; Grain.EMPTY = explicitly scalar.
    partition_keys: Optional["Grain"] = None
    # The ``FILTER (WHERE …)`` condition; a built-in lowers it as an input mask, any other aggregation as its
    # producer's row restriction.
    filter: Optional["ValueKey"] = None
    # The built-in aggregation's semantics (its owner declares no expression for ``agg``); not part of identity.
    builtin: bool = Field(default=True, repr=False)

    @field_validator("kwargs", mode="before")
    @classmethod
    def _canonicalize_kwargs(cls, v):
        return _sort_kwargs_tuple(v)

    @property
    def phase(self) -> Phase:
        return Phase.AGGREGATE

    def children(self) -> Tuple["ValueKey", ...]:
        embedded: List["ValueKey"] = [
            c
            for c in (self.source, *self.args, *(v for _, v in self.kwargs))
            if isinstance(c, _FrozenKey)
        ]
        if self.partition_keys is not None:
            embedded.extend(self.partition_keys)
        if self.filter is not None:
            embedded.append(self.filter)
        return tuple(embedded)

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "AggregateKey":
        m = _ChildMapper(fn)
        update = {
            "source": m(self.source),
            "args": tuple(m(a) for a in self.args),
            "kwargs": tuple((k, m(v)) for k, v in self.kwargs),
            "partition_keys": (
                None if self.partition_keys is None
                else Grain.of(m(p) for p in self.partition_keys)
            ),
            "filter": None if self.filter is None else m(self.filter),
        }
        return self.model_copy(update=update) if m.changed else self

    def __hash__(self) -> int:
        return hash((
            "AggregateKey",
            self.source,
            self.agg,
            _typed_args(self.args),
            _typed_kwargs(self.kwargs),
            self.locus,
            self.partition_keys,
            self.filter,
        ))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AggregateKey):
            return NotImplemented
        return (
            self.source == other.source
            and self.agg == other.agg
            and _typed_args(self.args) == _typed_args(other.args)
            and _typed_kwargs(self.kwargs) == _typed_kwargs(other.kwargs)
            and self.locus == other.locus
            and self.partition_keys == other.partition_keys
            and self.filter == other.filter
        )


# Rerooting is type-preserving — a FieldKey in, a FieldKey out.
_RerootableT = TypeVar("_RerootableT")


def _map_path_ref(ref, *, map_path):
    """Apply ``map_path`` to a single embedded reference's relationship ``path``.

    Per-leaf half of the path-map visitor: ``FieldKey``/``FieldSqlKey``/
    ``StarKey`` carry a ``path``; other fields ride along via ``model_copy``. A
    scalar (no ``path``), or a ``path`` left untouched, short-circuits the copy.
    """
    path = getattr(ref, "path", None)
    if path is None:
        return ref
    path = tuple(path)
    new_path = tuple(map_path(path))
    if new_path == path:
        return ref
    return ref.model_copy(update={"path": new_path})


def reroot_aggregate_key(
    key: "AggregateKey", *, target_path: Tuple[str, ...],
) -> "AggregateKey":
    """Re-anchor a cross-dataset ``AggregateKey`` into its target's local scope.

    A thin alias for :func:`reroot_value_key`; a ``DatasetField.filter`` rides its
    source ``FieldSqlKey``, recovered from the dataset at expansion.
    """
    return reroot_value_key(key, target_path=target_path)


class TransformKey(_FrozenKey, frozen=True):
    """Identity for a transform slot (window / temporal operator over a value).

    ``input`` is the operated-on value. ``partition_keys`` is order-independent;
    ``time_key`` is the sort dimension for time-ordered transforms.
    """

    op: str
    input: "ValueKey"
    args: Tuple[Scalar, ...] = ()
    # A ``time_shift`` unit resolves to its datasource definition when custom.
    kwargs: Tuple[Tuple[str, Scalar | CustomGranularity], ...] = ()
    partition_keys: "Grain" = Field(default_factory=lambda: Grain.EMPTY)
    time_key: Optional["ValueKey"] = None

    @field_validator("kwargs", mode="before")
    @classmethod
    def _canonicalize_kwargs(cls, v):
        return _sort_kwargs_tuple(v)

    @property
    def phase(self) -> Phase:
        return Phase.POST

    def children(self) -> Tuple["ValueKey", ...]:
        # args/kwargs are scalar-only, never children.
        embedded = [self.input, *self.partition_keys]
        if self.time_key is not None:
            embedded.append(self.time_key)
        return tuple(embedded)

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "TransformKey":
        m = _ChildMapper(fn)
        update = {
            "input": m(self.input),
            "partition_keys": Grain.of(m(p) for p in self.partition_keys),
            "time_key": None if self.time_key is None else m(self.time_key),
        }
        return self.model_copy(update=update) if m.changed else self

    def __hash__(self) -> int:
        return hash((
            "TransformKey",
            self.op,
            self.input,
            _typed_args(self.args),
            _typed_kwargs(self.kwargs),
            self.partition_keys,
            self.time_key,
        ))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TransformKey):
            return NotImplemented
        return (
            self.op == other.op
            and self.input == other.input
            and _typed_args(self.args) == _typed_args(other.args)
            and _typed_kwargs(self.kwargs) == _typed_kwargs(other.kwargs)
            and self.partition_keys == other.partition_keys
            and self.time_key == other.time_key
        )


class ArithmeticKey(_FrozenKey, frozen=True):
    """Identity for an arithmetic / comparison / boolean expression.

    ``op`` is the operator symbol. Operand order matters (non-commutative ops,
    fixed LHS/RHS, deterministic emission). Phase is the max of operand phases (P8).
    """

    op: str
    operands: Tuple["ValueKey", ...]

    @property
    def phase(self) -> Phase:
        return max((o.phase for o in self.operands), default=Phase.ROW)

    def children(self) -> Tuple["ValueKey", ...]:
        return self.operands

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "ArithmeticKey":
        m = _ChildMapper(fn)
        operands = tuple(m(o) for o in self.operands)
        return (
            self.model_copy(update={"operands": operands})
            if m.changed else self
        )


_ScalarCallArg = Union["ValueKey", Decimal, str, bool, None, datetime, date]


def _arg_phase(arg) -> Optional[Phase]:
    """Return ``arg.phase`` for ValueKey args, ``None`` for pure scalars."""
    return getattr(arg, "phase", None)


class ScalarCallKey(_FrozenKey, frozen=True):
    """Identity for a closed-allowlist scalar function call (C12).

    ``name`` must be in ``SCALAR_FUNCTIONS``; the key does not validate this (the
    binder rejects unknown names). Phase is the max of arg phases, ROW floor.
    """

    name: str
    args: Tuple[_ScalarCallArg, ...] = ()

    @property
    def phase(self) -> Phase:
        phases = [p for a in self.args if (p := _arg_phase(a)) is not None]
        return max(phases) if phases else Phase.ROW

    def children(self) -> Tuple["ValueKey", ...]:
        return tuple(a for a in self.args if isinstance(a, _FrozenKey))

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "ScalarCallKey":
        m = _ChildMapper(fn)
        args = tuple(m(a) for a in self.args)
        return self.model_copy(update={"args": args}) if m.changed else self

    def __hash__(self) -> int:
        return hash(("ScalarCallKey", self.name, _typed_args(self.args)))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ScalarCallKey):
            return NotImplemented
        return (
            self.name == other.name
            and _typed_args(self.args) == _typed_args(other.args)
        )


class WindowFrameSpec(BaseModel, frozen=True):
    """A window's ``ROWS`` / ``RANGE`` frame: each bound a kind (``unbounded preceding``, ``preceding``, ``current row``,
    ``following``, ``unbounded following``) and its whole-number offset; no ``end`` for a single start bound."""

    unit: str
    start: Tuple[str, Optional[Decimal]]
    end: Optional[Tuple[str, Optional[Decimal]]] = None


class WindowKey(_FrozenKey, frozen=True):
    """``function(args) [FILTER (WHERE filter)] OVER (PARTITION BY … ORDER BY … frame)``.

    Row-level (phase ROW) when no operand is aggregate-valued: a windowed column of the dataset at ``anchor``, the route
    from the reading dataset. Measure-level (phase POST) otherwise: a transform over its operand's cells. Each order
    item carries its resolved NULL placement."""

    function: str
    args: Tuple["ValueKey", ...] = ()
    filter: Optional["ValueKey"] = None
    partition: Tuple["ValueKey", ...] = ()
    order: Tuple["ValueKey", ...] = ()
    descending: Tuple[bool, ...] = ()
    nulls_first: Tuple[bool, ...] = ()
    frame: Optional[WindowFrameSpec] = None
    anchor: Tuple[str, ...] = ()

    @property
    def phase(self) -> Phase:
        return Phase.ROW if all(c.phase is Phase.ROW for c in self.children()) else Phase.POST

    @property
    def row_level(self) -> bool:
        return self.phase is Phase.ROW

    def children(self) -> Tuple["ValueKey", ...]:
        return (*self.args, *(() if self.filter is None else (self.filter,)), *self.partition, *self.order)

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "WindowKey":
        m = _ChildMapper(fn)
        update = {
            "args": tuple(m(a) for a in self.args),
            "filter": None if self.filter is None else m(self.filter),
            "partition": tuple(m(p) for p in self.partition),
            "order": tuple(m(o) for o in self.order),
        }
        return self.model_copy(update=update) if m.changed else self


#: The ``TransformLayer`` op of a measure-level window.
WINDOW_OP = "window"
#: The transform kinds: SLayer's transforms and a measure-level window, each a value over its operand's cells.
TRANSFORM_KINDS = (TransformKey, WindowKey)


def is_transform(key: object) -> TypeIs["TransformKey | WindowKey"]:
    """Whether ``key`` is a transform: a ``TransformKey``, or a window over aggregate-valued operands."""
    return isinstance(key, TransformKey) or (isinstance(key, WindowKey) and not key.row_level)


def transform_operands(key: "TransformKey | WindowKey") -> Tuple["ValueKey", ...]:
    """The values a transform reads in its cells: a transform's input, a window's arguments, filter and keys."""
    return (key.input,) if isinstance(key, TransformKey) else key.children()


_CAST_TYPE_TOKEN_RE = re.compile(r"\[\]|\(([^()]*)\)|[A-Z_][A-Z0-9_]*")


class CastTypeParts(BaseModel):
    """The parts of a canonical cast type text (``TIMESTAMP(6) WITH TIME ZONE``, ``DECIMAL(10,2)[]``)."""

    model_config = ConfigDict(frozen=True)

    words: Tuple[str, ...]
    sizes: Tuple[str, ...] = ()
    array_rank: int = 0

    @property
    def unsigned(self) -> bool:
        return "UNSIGNED" in self.words

    @property
    def zone(self) -> Optional[bool]:
        """``True`` for a zoned type (``WITH [LOCAL] TIME ZONE``), ``False`` for ``WITHOUT TIME ZONE``, else ``None``."""
        if "ZONE" not in self.words:
            return None
        return "WITHOUT" not in self.words


def cast_type_parts(type_text: str) -> CastTypeParts:
    words: list[str] = []
    sizes: Tuple[str, ...] = ()
    array_rank = 0
    for token in _CAST_TYPE_TOKEN_RE.finditer(type_text.upper()):
        if token[0] == "[]":
            array_rank += 1
        elif token[1] is not None:
            sizes = tuple(p.strip() for p in token[1].split(",") if p.strip())
        else:
            words.append(token[0])
    return CastTypeParts(words=tuple(words), sizes=sizes, array_rank=array_rank)


class CastKey(_FrozenKey, frozen=True):
    """Identity for ``CAST(source AS type_text)`` (``try_``: ``TRY_CAST``) — the author's cast;
    ``data_type`` is SLayer's type for the target, ``element_type`` its elements' for an array target;
    ``numeric_source``: what the cast reads its source as — ``INT`` (integer text), ``DOUBLE`` (another number),
    else ``None`` (text)."""

    source: "ValueKey"
    type_text: str
    data_type: DataType
    try_: bool = False
    element_type: Optional[DataType] = None
    numeric_source: Optional[DataType] = None

    @property
    def phase(self) -> Phase:
        return self.source.phase

    def children(self) -> Tuple["ValueKey", ...]:
        return (self.source,)

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "CastKey":
        m = _ChildMapper(fn)
        source = m(self.source)
        return self.model_copy(update={"source": source}) if m.changed else self


TimePointOp = Literal["=", "!=", "<", "<=", ">", ">=", "in", "not in"]
MIRRORED_TIME_POINT_OP: Dict[TimePointOp, TimePointOp] = {"=": "=", "!=": "!=", "<": ">", "<=": ">=", ">": "<", ">=": "<="}


class TimePointCmpKey(_FrozenKey, frozen=True):
    """Transient ``operand <op> '<point>'`` against a time-point string, resolved away by the checker before planning.

    ``op`` is operand-left; a literal-left spelling is stored mirrored with ``literal_on_left`` set. A date-only
    ``point`` is its whole day under ``in`` / ``not in`` or when ``whole_day`` (a ``date_range`` bound), else midnight.
    """

    op: TimePointOp
    operand: "ValueKey"
    point: str
    literal_on_left: bool = False
    whole_day: bool = False

    @property
    def reads_whole_day(self) -> bool:
        return self.whole_day or self.op in ("in", "not in")

    @property
    def phase(self) -> Phase:
        return self.operand.phase

    def children(self) -> Tuple["ValueKey", ...]:
        return (self.operand,)

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "TimePointCmpKey":
        m = _ChildMapper(fn)
        operand = m(self.operand)
        return self.model_copy(update={"operand": operand}) if m.changed else self


class InKey(_FrozenKey, frozen=True):
    """Typed identity for a ``col IN (lit, …)`` / ``NOT IN`` predicate.

    A field LHS and a fixed tuple of ``LiteralKey``
    RHS operands (LiteralKey so equality is type-stable). ``negated`` flips IN vs
    NOT IN. Phase is the max of child phases (P8); the renderer emits
    ``exp.In`` (wrapped in ``exp.Not``).
    """

    field: "ValueKey"
    values: Tuple[LiteralKey, ...]
    negated: bool = False

    @field_validator("values")
    @classmethod
    def _reject_empty_values(
        cls, v: Tuple[LiteralKey, ...],
    ) -> Tuple[LiteralKey, ...]:
        # Defense in depth: direct construction can bypass the parser's empty-RHS
        # check and reach the generator, which would emit invalid ``col IN ()``.
        if not v:
            raise ValueError(
                "InKey requires a non-empty ``values`` tuple; ``col IN "
                "()`` is invalid SQL across every supported dialect.",
            )
        return v

    @property
    def phase(self) -> Phase:
        return max(c.phase for c in self.children())

    def children(self) -> Tuple["ValueKey", ...]:
        return (self.field, *self.values)

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "InKey":
        m = _ChildMapper(fn)
        update = {
            "field": m(self.field),
            "values": tuple(m(v) for v in self.values),
        }
        return self.model_copy(update=update) if m.changed else self


class ArrayLiteralKey(_FrozenKey, frozen=True):
    """Identity for a literal list ``['a', 'b']``; ``[]`` takes its element type from the other operand."""

    values: Tuple[LiteralKey, ...] = ()

    @property
    def phase(self) -> Phase:
        return Phase.ROW

    def children(self) -> Tuple["ValueKey", ...]:
        return self.values

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "ArrayLiteralKey":
        m = _ChildMapper(fn)
        values = tuple(m(v) for v in self.values)
        return self.model_copy(update={"values": values}) if m.changed else self


class ListSetKey(_FrozenKey, frozen=True):
    """A list's element set — its distinct non-NULL elements, sorted — the list's value wherever equality reads it."""

    source: "ValueKey"

    @property
    def phase(self) -> Phase:
        return self.source.phase

    def children(self) -> Tuple["ValueKey", ...]:
        return (self.source,)

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "ListSetKey":
        m = _ChildMapper(fn)
        source = m(self.source)
        return list_set_key(source) if m.changed else self


def list_set_key(source: "ValueKey") -> "ListSetKey":
    """``ListSetKey(source)``, idempotent: the set of a set is itself."""
    return source if isinstance(source, ListSetKey) else ListSetKey(source=source)


ValueKey = Union[
    FieldKey,
    FieldSqlKey,
    TimeTruncKey,
    StarKey,
    LiteralKey,
    ArrayLiteralKey,
    AggregateKey,
    TransformKey,
    ArithmeticKey,
    ScalarCallKey,
    TimePointCmpKey,
    InKey,
    SqlFragmentKey,
    CastKey,
    ListSetKey,
    WindowKey,
]


class Grain(BaseModel):
    """The grain of an aggregate — its dimension-key set — as a first-class value type.

    Grains form a lattice under inclusion: ``union`` is the join, a coarser grain
    is a subgrain of a finer one, and broadcast is the coarse->fine coercion.
    Set-like dunders carry the mechanical set algebra; the named predicates spell
    out the lattice reading. Comparisons and ``union`` accept a ``Grain`` only;
    ``__or__`` / ``__sub__`` also accept any ``AbstractSet[ValueKey]``; ``__eq__``
    is ``Grain``-only so mixed-representation drift fails loudly.
    """

    model_config = ConfigDict(frozen=True)

    keys: frozenset[ValueKey] = frozenset()

    EMPTY: ClassVar["Grain"]

    @classmethod
    def of(cls, keys: Iterable[ValueKey]) -> "Grain":
        return cls(keys=frozenset(keys))

    @property
    def is_empty(self) -> bool:
        return not self.keys

    def union(self, other: "Grain") -> "Grain":
        """The join: the grain containing every key of both."""
        return Grain(keys=self.keys | other.keys)

    def is_subgrain_of(self, other: "Grain") -> bool:
        """``self`` is coarser than or equal to ``other`` (reflexive)."""
        return self.keys <= other.keys

    def is_strict_subgrain_of(self, other: "Grain") -> bool:
        """``self`` is strictly coarser than ``other`` (irreflexive)."""
        return self.keys < other.keys

    def broadcasts_into(self, finer: "Grain") -> bool:
        """A coarse value coerces up to ``finer`` iff ``self`` is a subgrain of it;
        the reverse needs a second-order aggregation, never a broadcast."""
        return self.is_subgrain_of(finer)

    def __contains__(self, key: object) -> bool:
        return key in self.keys

    def __iter__(self) -> Iterator[ValueKey]:  # type: ignore[override]
        return iter(self.keys)

    def __len__(self) -> int:
        return len(self.keys)

    def __bool__(self) -> bool:
        return bool(self.keys)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Grain):
            return self.keys == other.keys
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.keys)

    def __le__(self, other: object) -> bool:
        if isinstance(other, Grain):
            return self.keys <= other.keys
        return NotImplemented

    def __lt__(self, other: object) -> bool:
        if isinstance(other, Grain):
            return self.keys < other.keys
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, Grain):
            return self.keys >= other.keys
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, Grain):
            return self.keys > other.keys
        return NotImplemented

    def __or__(self, other: object) -> "Grain":
        if isinstance(other, Grain):
            return Grain(keys=self.keys | other.keys)
        if isinstance(other, AbstractSet):
            return Grain(keys=self.keys | frozenset(other))
        return NotImplemented

    def __sub__(self, other: object) -> "Grain":
        if isinstance(other, Grain):
            return Grain(keys=self.keys - other.keys)
        if isinstance(other, AbstractSet):
            return Grain(keys=self.keys - frozenset(other))
        return NotImplemented


# Resolve the recursive forward references on the keys that take ValueKey.
TransformKey.model_rebuild()
ArithmeticKey.model_rebuild()
ScalarCallKey.model_rebuild()
TimePointCmpKey.model_rebuild()
InKey.model_rebuild()
CastKey.model_rebuild()
ListSetKey.model_rebuild()
WindowKey.model_rebuild()
TimeTruncKey.model_rebuild()
# AggregateKey.source forward-references the expression composites.
AggregateKey.model_rebuild()
Grain.model_rebuild()
Grain.EMPTY = Grain(keys=frozenset())


VALUE_KEY_TYPES: Tuple[type, ...] = get_args(ValueKey)
# Key fields spelled only when set, so a key without one keeps its historical spelling.
OPTIONAL_KEY_FIELDS = frozenset({"filter", "anchor"})
#: Key fields outside identity (never spelled into a name).
NON_IDENTITY_KEY_FIELDS = frozenset({"builtin"})


class KindPolicy(BaseModel):
    """Consumer-named per-kind policy flags; membership is a conscious
    classification asserted by tests, not derived from structure.

    ``slot_composite``: an operator over child keys, rendered inline from them
    and staged by them."""

    model_config = ConfigDict(frozen=True)

    slottable: bool = False
    slot_composite: bool = False
    materialised_order: bool = False


KIND_POLICY: dict[type, KindPolicy] = {
    FieldKey: KindPolicy(slottable=True),
    FieldSqlKey: KindPolicy(slottable=True),
    TimeTruncKey: KindPolicy(slottable=True),
    StarKey: KindPolicy(),
    LiteralKey: KindPolicy(),
    ArrayLiteralKey: KindPolicy(),
    AggregateKey: KindPolicy(slottable=True, materialised_order=True),
    TransformKey: KindPolicy(slottable=True, materialised_order=True),
    ArithmeticKey: KindPolicy(slot_composite=True, materialised_order=True),
    ScalarCallKey: KindPolicy(slot_composite=True, materialised_order=True),
    TimePointCmpKey: KindPolicy(),
    InKey: KindPolicy(slot_composite=True, materialised_order=True),
    SqlFragmentKey: KindPolicy(),
    CastKey: KindPolicy(slot_composite=True, materialised_order=True),
    ListSetKey: KindPolicy(slot_composite=True, materialised_order=True),
    WindowKey: KindPolicy(slottable=True, materialised_order=True),
}
SLOT_COMPOSITE_KINDS: Tuple[type, ...] = tuple(k for k, p in KIND_POLICY.items() if p.slot_composite)


def _map_value_key(key: _RerootableT, *, map_path) -> _RerootableT:
    """Rewrite every embedded relationship ``path`` in ``key`` through ``map_path``.

    Total & fail-closed behind :func:`reroot_value_key` / :func:`prepend_value_key`:
    path-carrying leaves map here, every other kind routes through
    ``map_children`` (a protocol-less kind raises).
    """
    # Scalars ride through untouched (ScalarCallKey args, AggregateKey kwargs).
    if key is None or isinstance(key, (Decimal, str, bool, int, float)):
        return key
    if isinstance(key, (FieldKey, FieldSqlKey, StarKey)):
        return cast(_RerootableT, _map_path_ref(key, map_path=map_path))
    if not isinstance(key, _FrozenKey):
        raise TypeError(
            f"the value-key path visitor has no case for {type(key).__name__!r}: "
            f"only value keys and scalars are mappable."
        )
    return cast(
        _RerootableT,
        key.map_children(lambda c: _map_value_key(c, map_path=map_path)),
    )


def walk_value_keys(key: ValueKey):
    """Yield every ``ValueKey`` reachable from ``key``, including ``key`` —
    total via the traversal protocol (a protocol-less kind raises)."""
    yield key
    for child in key.children():
        yield from walk_value_keys(child)


def _inner_partition_grain(k: ValueKey) -> Grain:
    """Union of the outermost aggregates' partition grains; an aggregate is opaque."""
    if isinstance(k, AggregateKey):
        return Grain.of(k.partition_keys or frozenset())
    grain = Grain.EMPTY
    for c in k.children():
        grain = grain | _inner_partition_grain(c)
    return grain


def regroup_root_grain(root: ValueKey) -> Grain:
    """Producer grain of a row-attach root: a transform evaluates at the set-union
    of its inner aggregates' partition grains; a bare aggregate at its own grain."""
    if isinstance(root, TransformKey):
        return _inner_partition_grain(root.input)
    if is_transform(root):
        return _inner_partition_grain(root)
    return Grain.of(getattr(root, "partition_keys", None) or frozenset())


def effective_root_grain(
    agg: ValueKey,
    *,
    projected_dim_keys: List[ValueKey],
    projected_td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> Tuple[Grain, bool]:
    """A combined-root's producer grain and windowedness.

    An explicitly-partitioned aggregate keeps ``regroup_root_grain``. A bare
    windowed / first-last root takes the FULL projected grain (a windowed root's
    bucket enters via ``window_td_key``, so it is excluded here)."""
    windowed = window_kwarg_of(agg) is not None
    if getattr(agg, "partition_keys", None) is not None or is_transform(agg):
        grain = regroup_root_grain(agg)
        # A transform with no grained inner (e.g. rank(region, direction='desc')) is the degenerate
        # query-grain identity (Axiom 11.1); its operand cells are the query grain.
        if is_transform(agg) and grain.is_empty:
            return Grain.of([*projected_dim_keys, *projected_td_keys]), False
        # A transform over a window= inner gains the active bucket in its union grain.
        if (
            not windowed and is_transform(agg) and active_bucket is not None
            and any(window_kwarg_of(k) is not None for k in walk_value_keys(agg))
        ):
            return grain | {active_bucket}, True
        return grain, windowed
    if windowed:
        grain = Grain.of(projected_dim_keys) | (
            Grain.of(projected_td_keys)
            - ({active_bucket} if active_bucket else frozenset())
        )
    else:
        grain = Grain.of([*projected_dim_keys, *projected_td_keys])
    return grain, windowed


def constituent_grain(
    c: ValueKey, *,
    projected_dim_keys: List[ValueKey],
    projected_td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> Grain:
    """Grain of an attached constituent (Axiom 2.3) — aggregate or grained
    transform: its effective root grain, folding the active bucket back in when a
    windowed inner pulls it in."""
    grain, windowed = effective_root_grain(
        agg=c, projected_dim_keys=projected_dim_keys,
        projected_td_keys=projected_td_keys, active_bucket=active_bucket,
    )
    if windowed and active_bucket is not None:
        return grain | {active_bucket}
    return grain


def transform_operand_grain(
    input: ValueKey, *, query_grain: Grain, active_bucket: Optional[ValueKey],
) -> Grain:
    """A transform input's operand grain (Axioms 11.1, 11.5), recursive.
    Without an aggregate over rows, row leaves sit at the query grain."""
    if any(isinstance(k, AggregateKey) and not is_reaggregation_key(k) for k in walk_value_keys(input)):
        return _operand_grain(input, query_grain=query_grain, active_bucket=active_bucket, leaf_grain=None)
    grain = _operand_grain(input, query_grain=query_grain, active_bucket=active_bucket, leaf_grain=query_grain)
    # Literal-only: nothing grained, so the query-grain identity (Axiom 11.1).
    grained = any(isinstance(k, (TransformKey, FieldKey, FieldSqlKey, TimeTruncKey)) or is_transform(k)
                  for k in walk_value_keys(input))
    return query_grain if grain.is_empty and not grained else grain


def window_operand_grain(key: "WindowKey", *, query_grain: Grain, active_bucket: Optional[ValueKey]) -> Grain:
    """A measure-level window's operand grain: that of its aggregate-valued operands (the query grain else)."""
    grains = [transform_operand_grain(c, query_grain=query_grain, active_bucket=active_bucket)
              for c in key.children() if c.phase is not Phase.ROW]
    out = grains[0] if grains else query_grain
    for g in grains[1:]:
        out = out | g
    return out


def value_grain(k: ValueKey, *, query_grain: Grain, active_bucket: Optional[ValueKey]) -> Grain:
    """The grain ``k``'s value varies at: an aggregate's (a windowed one's with the active bucket), a transform's
    operand grain, a row leaf itself; a compound the union of its parts'."""
    return _operand_grain(k, query_grain=query_grain, active_bucket=active_bucket, leaf_grain=None)


def _operand_grain(
    k: ValueKey, *, query_grain: Grain, active_bucket: Optional[ValueKey], leaf_grain: Optional[Grain],
) -> Grain:
    if isinstance(k, AggregateKey):
        grain = query_grain if k.partition_keys is None else k.partition_keys
        if window_kwarg_of(k) is not None and active_bucket is not None:
            return grain | {active_bucket}
        return grain
    if isinstance(k, TransformKey):
        return transform_operand_grain(k.input, query_grain=query_grain, active_bucket=active_bucket)
    if isinstance(k, WindowKey) and is_transform(k):
        return window_operand_grain(k, query_grain=query_grain, active_bucket=active_bucket)
    if isinstance(k, (FieldKey, FieldSqlKey, TimeTruncKey)):
        return Grain.of([k]) if leaf_grain is None else leaf_grain
    grain = Grain.EMPTY
    for c in k.children():
        grain = grain | _operand_grain(c, query_grain=query_grain, active_bucket=active_bucket, leaf_grain=leaf_grain)
    return grain


def attached_parameter_grain(
    key: ValueKey, *,
    projected_dim_keys: List[ValueKey],
    projected_td_keys: List[ValueKey],
    active_bucket: Optional[ValueKey],
) -> Optional[Grain]:
    """The grain at which an attached aggregation parameter is typed (Axiom 11.4 /
    2.3) — ``None`` when it is determined by construction. An ``AggregateKey`` types
    at its ``partition_keys`` (``None`` = ungrained, at the enclosing grain and
    determined). A ``TransformKey`` types at its result grain. Both home- and
    operand-determination checks resolve the parameter to this grain first, so
    neither predicate needs a transform arm (D5)."""
    if isinstance(key, AggregateKey):
        return key.partition_keys
    if is_transform(key):
        return constituent_grain(
            c=key, projected_dim_keys=projected_dim_keys,
            projected_td_keys=projected_td_keys, active_bucket=active_bucket,
        )
    return None


def reroot_value_key(
    key: _RerootableT, *, target_path: Tuple[str, ...],
) -> _RerootableT:
    """Re-anchor every embedded reference from the query root into ``target_path``'s
    local scope (STRIP direction), inverse of :func:`prepend_value_key`.

    Prefix-strip-with-residual per position: a ``path`` starting with
    ``target_path`` drops that prefix and keeps the residual hops; any other
    ``path`` or scalar is unchanged. ``target_path == ()`` is the identity.
    """
    target_path = tuple(target_path)
    if not target_path:
        return key

    def _strip(path: Tuple[str, ...]) -> Tuple[str, ...]:
        path = tuple(path)
        if path[: len(target_path)] == target_path:
            return path[len(target_path):]
        return path

    return _map_value_key(key, map_path=_strip)


def prepend_value_key(
    key: _RerootableT, *, host_path: Tuple[str, ...],
) -> _RerootableT:
    """Prefix every embedded relationship ``path`` with ``host_path`` (PREPEND direction),
    re-anchoring a target-local bound tree into the host's coordinate system.

    Inverse of ``reroot_value_key(key, target_path=host_path)``. ``host_path == ()``
    is the identity.
    """
    host_path = tuple(host_path)
    if not host_path:
        return key

    def _prepend(path: Tuple[str, ...]) -> Tuple[str, ...]:
        return host_path + tuple(path)

    return _map_value_key(key, map_path=_prepend)


def is_framed_path(path: Tuple[str, ...]) -> bool:
    """``path`` starts with an anchor hop."""
    return bool(path) and token_anchor(path[0]) is not None


def _framed_leaves(key: _RerootableT, *, token: str) -> _RerootableT:
    """``key``'s row leaves prefixed with ``token`` (an already-framed one kept); attached values are opaque."""
    if not isinstance(key, _FrozenKey) or isinstance(key, (AggregateKey, TransformKey)) or is_transform(key):
        return key
    if isinstance(key, (FieldKey, FieldSqlKey, StarKey)):
        return key if is_framed_path(key.path) else cast(_RerootableT, key.model_copy(update={"path": (token, *key.path)}))
    return cast(_RerootableT, key.map_children(lambda c: _framed_leaves(c, token=token)))


def _unanchored_leaves(key: _RerootableT) -> _RerootableT:
    """``key``'s row leaves without their anchored spelling (read from one frame, their path is their identity);
    attached values are opaque."""
    if not isinstance(key, _FrozenKey) or isinstance(key, (AggregateKey, TransformKey)) or is_transform(key):
        return key
    if isinstance(key, (FieldKey, FieldSqlKey)):
        return key if key.anchor is None else cast(_RerootableT, key.model_copy(update={"anchor": None}))
    return cast(_RerootableT, key.map_children(_unanchored_leaves))


def interior_unanchored(key: AggregateKey) -> AggregateKey:
    """``key`` with its interior's references keyed by their path from its frame alone; a pick over cells ranks by
    a grain key, no interior one."""
    return key.model_copy(update={
        "source": _unanchored_leaves(key.source),
        "args": key.args if is_ranked_over_cells(key) else tuple(_unanchored_leaves(a) for a in key.args),
        "kwargs": tuple((k, _unanchored_leaves(v)) for k, v in key.kwargs),
        "filter": None if key.filter is None else _unanchored_leaves(key.filter),
    })


def framed_aggregate(key: AggregateKey, *, frame: str) -> AggregateKey:
    """``key`` with its interior (source, parameters, ``FILTER``) read from dataset ``frame`` (semantics Axiom 2.6);
    its grain and nested attached values keep their own coordinates."""
    token = anchor_token(frame)
    return key.model_copy(update={
        "source": _framed_leaves(key.source, token=token),
        "args": key.args if is_ranked_over_cells(key) else tuple(_framed_leaves(a, token=token) for a in key.args),
        "kwargs": tuple((k, _framed_leaves(v, token=token)) for k, v in key.kwargs),
        "filter": None if key.filter is None else _framed_leaves(key.filter, token=token),
    })


def compose_frame(
    key: _RerootableT, *, frame: str, host_path: Callable[[], Tuple[str, ...]],
) -> _RerootableT:
    """A value bound on dataset ``frame`` read by another dataset: every aggregation's interior framed at ``frame``,
    every other reference prefixed with ``host_path()`` (the reader's route to ``frame``, asked only when needed)."""
    if not isinstance(key, _FrozenKey):
        return key
    if isinstance(key, AggregateKey):
        framed = framed_aggregate(key, frame=frame)
        return cast(_RerootableT, framed.model_copy(update={
            "source": _compose_nested(framed.source, frame=frame, host_path=host_path),
            # A pick over cells ranks by a grain key of its operand.
            "args": tuple(
                _composed_grain(a, frame=frame, host_path=host_path) if is_ranked_over_cells(key)
                else _compose_nested(a, frame=frame, host_path=host_path) for a in framed.args
            ),
            "kwargs": tuple((k, _compose_nested(v, frame=frame, host_path=host_path)) for k, v in framed.kwargs),
            "partition_keys": None if key.partition_keys is None else Grain.of(
                _composed_grain(p, frame=frame, host_path=host_path) for p in key.partition_keys),
        }))
    if isinstance(key, TransformKey):
        return cast(_RerootableT, key.model_copy(update={
            "input": compose_frame(key.input, frame=frame, host_path=host_path),
            "partition_keys": Grain.of(_composed_grain(p, frame=frame, host_path=host_path) for p in key.partition_keys),
            "time_key": None if key.time_key is None else _composed_grain(key.time_key, frame=frame, host_path=host_path),
        }))
    if isinstance(key, (FieldKey, FieldSqlKey, StarKey)):
        return _composed_grain(key, frame=frame, host_path=host_path)
    return cast(_RerootableT, key.map_children(lambda c: compose_frame(c, frame=frame, host_path=host_path)))


def _compose_nested(key: _RerootableT, *, frame: str, host_path: Callable[[], Tuple[str, ...]]) -> _RerootableT:
    """Inside a framed interior: a nested attached value is composed in turn; row leaves are already framed."""
    if not isinstance(key, _FrozenKey) or isinstance(key, (FieldKey, FieldSqlKey, StarKey)):
        return key
    if isinstance(key, (AggregateKey, TransformKey)) or is_transform(key):
        return compose_frame(key, frame=frame, host_path=host_path)
    return cast(_RerootableT, key.map_children(lambda c: _compose_nested(c, frame=frame, host_path=host_path)))


def _composed_grain(key: _RerootableT, *, frame: str, host_path: Callable[[], Tuple[str, ...]]) -> _RerootableT:
    """A grain reference bound on ``frame``, read through the reader's route to it, keeping its anchored spelling."""
    def leaf(k: ValueKey) -> ValueKey:
        if isinstance(k, (FieldKey, FieldSqlKey)):
            anchor = k.anchor if k.anchor is not None else (frame, *k.path)
            return k.model_copy(update={"path": (*host_path(), *k.path), "anchor": anchor})
        if isinstance(k, StarKey):
            return k.model_copy(update={"path": (*host_path(), *k.path)})
        if isinstance(k, (AggregateKey, TransformKey)) or is_transform(k):
            return compose_frame(k, frame=frame, host_path=host_path)
        return k.map_children(leaf)

    return cast(_RerootableT, leaf(cast(ValueKey, key)))


def substitute_value_keys(
    key: _RerootableT, mapping: Mapping["ValueKey", "ValueKey"],
) -> _RerootableT:
    """Replace whole sub-keys named in ``mapping`` by identity, structurally.

    Pre-order match-before-recurse: a key equal to a ``mapping`` entry is
    replaced atomically (children never traversed, replacements never
    re-substituted); everything else routes through ``map_children`` (a
    protocol-less kind raises). ``TimeTruncKey.source`` IS traversed.
    """
    # Scalars ride through untouched (ScalarCallKey args, AggregateKey kwargs).
    if key is None or isinstance(key, (Decimal, str, bool, int, float)):
        return key
    if not isinstance(key, _FrozenKey):
        raise TypeError(
            f"substitute_value_keys has no case for {type(key).__name__!r}: "
            f"only value keys and scalars are substitutable."
        )
    if key in mapping:
        return cast(_RerootableT, mapping[cast("ValueKey", key)])
    return cast(
        _RerootableT,
        key.map_children(lambda c: substitute_value_keys(c, mapping)),
    )


# Conditional branch typing — Postgres CASE semantics.
_NUMERIC_TYPES = NUMERIC_DATATYPES


def join_conditional_branch_types(
    a: Optional[DataType], b: Optional[DataType],
) -> Optional[DataType]:
    """Result type of a conditional whose branches are ``a`` / ``b``.

    ``None`` marks a NULL-literal branch, absorbed by the other. Identical types
    pass through; a numeric mix widens to ``DOUBLE``, a DATE/TIMESTAMP mix to
    ``TIMESTAMP``; any other mix is a plan-time error (matching what Postgres rejects).
    """
    if a is None:
        return b
    if b is None:
        return a
    joined = _joined_known_types(a, b)
    if joined is None:
        raise ValueError(
            f"CASE/iif branches have incompatible types {a.value} and {b.value}: "
            f"branches must share a type (numeric types widen to DOUBLE). Cast one "
            f"branch so both match."
        )
    return joined


def _joined_known_types(a: DataType, b: DataType) -> Optional[DataType]:
    """The shared type of two known branch types, ``None`` when they cannot share one."""
    if a == b:
        return a
    if a in _NUMERIC_TYPES and b in _NUMERIC_TYPES:
        return DataType.FLOAT
    if {a, b} <= _NUMERIC_TYPES | {DataType.BOOLEAN}:
        # A boolean beside a number is its integer.
        return b if a is DataType.BOOLEAN else a
    if a in TEMPORAL_TYPES and b in TEMPORAL_TYPES:
        return DataType.DATETIME
    return None


TEMPORAL_TYPES = TEMPORAL_DATATYPES
# Aggregations whose value is one of their inputs, so they keep a temporal type.
_TEMPORAL_PRESERVING_AGGS = frozenset({"min", "max", "first", "last"})
# Scalars returning one of their value arguments: name → value-argument positions (None = all).
_VALUE_ARG_SCALARS: Mapping[str, Optional[Tuple[int, ...]]] = MappingProxyType({
    "coalesce": None, "ifnull": None, "nullif": None, "greatest": None, "least": None,
    "iif": (1, 2),
})


def value_arg_positions(name: str, argc: int) -> Tuple[int, ...]:
    """Argument positions a value-returning scalar (``coalesce``, ``iif``, …) may return; empty otherwise."""
    if name not in _VALUE_ARG_SCALARS:
        return ()
    positions = _VALUE_ARG_SCALARS[name]
    return tuple(range(argc)) if positions is None else tuple(i for i in positions if i < argc)


def type_date_values(key: object) -> Any:
    """``key`` with ISO string literals in date-value position — itself, or a conditional's
    value arguments, recursively — typed as ``date`` / ``datetime`` literals."""
    if isinstance(key, LiteralKey) and isinstance(key.value, str):
        value = parse_temporal_value(key.value)
        return key if value is None else LiteralKey(value=value)
    if isinstance(key, ScalarCallKey):
        positions = value_arg_positions(key.name, len(key.args))
        args = tuple(type_date_values(a) if i in positions else a for i, a in enumerate(key.args))
        return key.model_copy(update={"args": args}) if args != key.args else key
    return key


def _is_null_arg(arg: object) -> bool:
    return arg is None or (isinstance(arg, LiteralKey) and arg.value is None)


# The declared type of a field leaf (``FieldKey`` / ``FieldSqlKey``), ``None`` when unknown.
FieldTypeFn = Callable[[Any], Optional[DataType]]


#: Window functions whose value is an integer, a fraction, or one of their first argument's values.
_WINDOW_INTEGER = frozenset({"row_number", "rank", "dense_rank", "ntile", "count", "count_distinct"})
_WINDOW_FRACTION = frozenset({
    "percent_rank", "cume_dist", "avg", "stddev", "stddev_pop", "stddev_samp", "variance", "var_pop", "var_samp",
    "median", "percentile_cont",
})
_WINDOW_ARGUMENT = frozenset({"lag", "lead", "first_value", "last_value", "nth_value", "min", "max", "sum"})


def _window_argument(key: object) -> Optional["ValueKey"]:
    """A window's first argument when its value is one of that argument's (or their sum), else ``None``."""
    if isinstance(key, WindowKey) and key.function in _WINDOW_ARGUMENT and key.args:
        return key.args[0]
    return None


def temporal_type(key: object, *, field_type: FieldTypeFn) -> Optional[DataType]:
    """DATE / TIMESTAMP when ``key`` is provably temporal, else ``None``; field leaves type via ``field_type``."""
    if isinstance(key, (FieldKey, FieldSqlKey)):
        t = field_type(key)
        return t if t in TEMPORAL_TYPES else None
    if isinstance(key, TimeTruncKey):
        source = temporal_type(key.source, field_type=field_type)
        return None if source is None else bucket_type(source, key.granularity)
    if isinstance(key, (LiteralKey, date)):
        return _literal_temporal_type(key.value if isinstance(key, LiteralKey) else key)
    if isinstance(key, (AggregateKey, TransformKey)):
        return _derived_temporal_type(key, field_type=field_type)
    if isinstance(key, WindowKey):
        argument = _window_argument(key)
        return None if argument is None or key.function == "sum" else temporal_type(argument, field_type=field_type)
    if isinstance(key, ScalarCallKey):
        return _scalar_temporal_type(key, field_type=field_type)
    if isinstance(key, CastKey):
        return key.data_type if key.data_type in TEMPORAL_TYPES else None
    return None


def _derived_temporal_type(key: AggregateKey | TransformKey, *, field_type: FieldTypeFn) -> Optional[DataType]:
    """An aggregate's or transform's type when it keeps its input's (``max``, ``lag``, …), else ``None``."""
    if isinstance(key, AggregateKey):
        keeps = key.agg.lower() in _TEMPORAL_PRESERVING_AGGS
        return temporal_type(key.source, field_type=field_type) if keeps else None
    return temporal_type(key.input, field_type=field_type) if key.op in _VALUE_PRESERVING_TRANSFORMS else None


def _literal_temporal_type(value: object) -> Optional[DataType]:
    if isinstance(value, datetime):
        return DataType.DATETIME
    return DataType.DATE if isinstance(value, date) else None


_CLOCK_TYPES: Mapping[str, DataType] = MappingProxyType({
    "now": DataType.DATETIME, "current_date": DataType.DATE,
})


def _scalar_temporal_type(key: ScalarCallKey, *, field_type: FieldTypeFn) -> Optional[DataType]:
    if key.name in _CLOCK_TYPES:
        return _CLOCK_TYPES[key.name]
    if key.name == "date_add" and len(key.args) == 3:
        base = temporal_type(key.args[0], field_type=field_type)
        return None if base is None else date_add_type(base, TimeGranularity(unit_word(key.args[2])))
    values = [key.args[i] for i in value_arg_positions(key.name, len(key.args))]
    types = [temporal_type(a, field_type=field_type) for a in values if not _is_null_arg(a)]
    if not types or None in types:
        return None
    # NULLIF returns its first argument (or NULL); the second is only compared.
    if key.name == "nullif" and not _is_null_arg(key.args[0]):
        return types[0]
    stamps = {t for t in types if kind_of(t) is TypeKind.TIMESTAMP}
    if not stamps:
        return DataType.DATE
    return stamps.pop() if len(stamps) == 1 else DataType.DATETIME


# Aggregations whose value is one of their (boolean) inputs.
_BOOLEAN_PRESERVING_AGGS = frozenset({"min", "max", "first", "last"})
# Scalars returning one of their arguments, keyed to the returnable positions (None = all).
_BOOLEAN_BRANCH_SCALARS: Mapping[str, Optional[Tuple[int, ...]]] = MappingProxyType({
    "coalesce": None, "ifnull": None, "greatest": None, "least": None, "nullif": (0,), "iif": (1, 2),
})


def boolean_valued(key: object, *, field_type: FieldTypeFn) -> bool:
    """Whether ``key``'s value is certainly boolean; field leaves type via ``field_type``."""
    if isinstance(key, (FieldKey, FieldSqlKey)):
        return field_type(key) is DataType.BOOLEAN
    if isinstance(key, LiteralKey):
        return isinstance(key.value, bool)
    if is_boolean_shaped(cast("ValueKey", key)):
        return True
    if isinstance(key, AggregateKey):
        return key.agg.lower() in _BOOLEAN_PRESERVING_AGGS and boolean_valued(key.source, field_type=field_type)
    if isinstance(key, WindowKey):
        argument = _window_argument(key)
        return argument is not None and key.function != "sum" and boolean_valued(argument, field_type=field_type)
    if isinstance(key, CastKey):
        return key.data_type is DataType.BOOLEAN
    if isinstance(key, ScalarCallKey):
        return _scalar_boolean_valued(key, field_type=field_type)
    return False


def _scalar_boolean_valued(key: ScalarCallKey, *, field_type: FieldTypeFn) -> bool:
    if key.name in BOOLEAN_RESULT_SCALARS:
        return True
    if key.name not in _BOOLEAN_BRANCH_SCALARS:
        return False
    positions = _BOOLEAN_BRANCH_SCALARS[key.name]
    returned = key.args if positions is None else [key.args[i] for i in positions if i < len(key.args)]
    branches = [a for a in returned if not _is_null_arg(a)]
    return bool(branches) and all(boolean_valued(a, field_type=field_type) for a in branches)


# Integer-preserving operators and scalars, for typing an expression built from booleans.
_INT_OPS = frozenset({"+", "-", "*", "%"})
_INT_SCALARS = frozenset({"abs", "sign", "round", "floor", "ceil", "ceiling", "trunc", "mod"})
_ARITHMETIC_OPS = _INT_OPS | {"/"}
_NULL_TEST_OPS = frozenset({"is", "is not"})


def _literal_value(arg: object) -> object:
    return arg.value if isinstance(arg, LiteralKey) else arg


def _is_number_literal(arg: object) -> bool:
    value = _literal_value(arg)
    return isinstance(value, (int, float, Decimal)) and not isinstance(value, bool)


def _is_int_literal(arg: object) -> bool:
    if not _is_number_literal(arg):
        return False
    value = Decimal(str(_literal_value(arg)))  # ``% 1`` traps past 28 digits
    return value.is_finite() and value == value.to_integral_value()


# Scalars whose result is always a number.
_NUMBER_RESULT_SCALARS = NUMERIC_ARG_SCALARS | frozenset(
    n for n, s in SCALAR_SPECS.items() if s.result in _NUMERIC_TYPES
)
# Transforms whose result is always a number.
_NUMBER_RESULT_TRANSFORMS = frozenset({*RANK_FAMILY_TRANSFORMS, "consecutive_periods"})


def numeric_valued(key: object, *, field_type: FieldTypeFn) -> bool:
    """Whether ``key``'s value is certainly a number (a boolean reads as one only beside a number)."""
    if _is_number_literal(key):
        return True
    if isinstance(key, (FieldKey, FieldSqlKey)):
        return field_type(key) in _NUMERIC_TYPES
    if isinstance(key, ArithmeticKey):
        return key.op in _ARITHMETIC_OPS
    if isinstance(key, AggregateKey):
        agg = key.agg.lower()
        if agg in INTEGER_AGGREGATIONS or agg in NUMERIC_ONLY_AGGREGATIONS:
            return True
        return agg in _BOOLEAN_PRESERVING_AGGS and numeric_valued(key.source, field_type=field_type)
    if isinstance(key, TransformKey):
        return key.op in _NUMBER_RESULT_TRANSFORMS or numeric_valued(key.input, field_type=field_type)
    if isinstance(key, WindowKey):
        argument = _window_argument(key)
        return argument is None or numeric_valued(argument, field_type=field_type)
    if isinstance(key, CastKey):
        return key.data_type in _NUMERIC_TYPES
    if isinstance(key, ScalarCallKey):
        return _scalar_numeric_valued(key, field_type=field_type)
    return False


def _scalar_numeric_valued(key: ScalarCallKey, *, field_type: FieldTypeFn) -> bool:
    if key.name in _NUMBER_RESULT_SCALARS:
        return True
    branches = [key.args[i] for i in value_arg_positions(key.name, len(key.args)) if not _is_null_arg(key.args[i])]
    numbers = [numeric_valued(a, field_type=field_type) for a in branches]
    return any(numbers) and all(n or boolean_valued(a, field_type=field_type) for a, n in zip(branches, numbers))


_INTEGER_RESULT_AGGS = frozenset({"count", "count_distinct", "count_distinct_approx"})


def integer_valued(key: object, *, field_type: FieldTypeFn) -> bool:
    """Whether ``key``'s value is certainly an integer (booleans read as 0 / 1 count)."""
    if _is_int_literal(key) or boolean_valued(key, field_type=field_type):
        return True
    if isinstance(key, (FieldKey, FieldSqlKey)):
        return field_type(key) is DataType.INTEGER
    if isinstance(key, CastKey):
        return key.data_type is DataType.INTEGER
    if isinstance(key, ArithmeticKey):
        return key.op in _INT_OPS and all(integer_valued(o, field_type=field_type) for o in key.operands)
    if isinstance(key, AggregateKey):
        return _aggregate_integer_valued(key, field_type=field_type)
    if isinstance(key, ScalarCallKey):
        return _scalar_integer_valued(key, field_type=field_type)
    if isinstance(key, WindowKey):
        argument = _window_argument(key)
        if argument is None:
            return key.function in _WINDOW_INTEGER
        return integer_valued(argument, field_type=field_type)
    return False


def reads_no_rows(key: ValueKey) -> bool:
    """A constant: every leaf a literal or a call on literals only (``pi()``), no aggregate or transform."""
    return all(
        not isinstance(k, (AggregateKey, TransformKey)) and not is_transform(k)
        and (bool(k.children()) or isinstance(k, (LiteralKey, ScalarCallKey)))
        for k in walk_value_keys(key)
    )


def integer_text_valued(key: ValueKey, *, field_type: FieldTypeFn) -> bool:
    """Whether ``key`` certainly prints as integer text: an integer, not a bare boolean, with every literal plain digits."""
    if not integer_valued(key, field_type=field_type) or boolean_valued(key, field_type=field_type):
        return False
    return not any(_not_plain_digits(a) for k in walk_value_keys(key) for a in _literals_of(k))


def _literals_of(key: ValueKey) -> Iterator[object]:
    if isinstance(key, LiteralKey):
        yield key.value
    elif isinstance(key, ScalarCallKey):
        yield from (a for a in key.args if not isinstance(a, _FrozenKey))


def _not_plain_digits(value: object) -> bool:
    """A number literal written with a fraction or an exponent (``2.0``, ``1e3``)."""
    return isinstance(value, Decimal) and value.as_tuple().exponent != 0


def _aggregate_integer_valued(key: AggregateKey, *, field_type: FieldTypeFn) -> bool:
    agg = key.agg.lower()
    return agg in _INTEGER_RESULT_AGGS or (
        agg in ("sum", "min", "max", "first", "last") and integer_valued(key.source, field_type=field_type)
    )


def _scalar_integer_valued(key: ScalarCallKey, *, field_type: FieldTypeFn) -> bool:
    spec = SCALAR_SPECS.get(key.name)
    if spec is not None and spec.result is DataType.INTEGER:
        return True
    if key.name not in _INT_SCALARS and key.name not in _VALUE_ARG_SCALARS:
        return False
    if key.name == "round" and len(key.args) > 1:
        return False
    values = [a for a in key.args if not _is_null_arg(a)]
    return bool(values) and all(integer_valued(a, field_type=field_type) for a in values)


def _boolean_positions(
    children: Sequence[object], positions: Iterable[int], *, field_type: FieldTypeFn,
) -> FrozenSet[int]:
    return frozenset(i for i in positions if i < len(children) and boolean_valued(children[i], field_type=field_type))


def _arithmetic_numeric_booleans(key: ArithmeticKey, *, field_type: FieldTypeFn) -> FrozenSet[int]:
    everywhere = range(len(key.operands))
    if key.op in _ARITHMETIC_OPS:
        return _boolean_positions(key.operands, everywhere, field_type=field_type)
    if key.op not in PREDICATE_COMPARISON_OPS or key.op in _NULL_TEST_OPS or len(key.operands) != 2:
        return frozenset()
    found = _boolean_positions(key.operands, everywhere, field_type=field_type)
    other = [o for i, o in enumerate(key.operands) if i not in found]
    return found if len(found) == 1 and numeric_valued(other[0], field_type=field_type) else frozenset()


def _scalar_numeric_booleans(key: ScalarCallKey, *, field_type: FieldTypeFn) -> FrozenSet[int]:
    if key.name in NUMERIC_ARG_SCALARS:
        return _boolean_positions(key.args, range(len(key.args)), field_type=field_type)
    if key.name in _BOOLEAN_BRANCH_SCALARS and not boolean_valued(key, field_type=field_type):
        positions = _BOOLEAN_BRANCH_SCALARS[key.name]
        return _boolean_positions(
            key.args, range(len(key.args)) if positions is None else positions, field_type=field_type,
        )
    return frozenset()


def numeric_boolean_positions(key: object, *, field_type: FieldTypeFn) -> FrozenSet[int]:
    """Child positions of ``key`` (operands, arguments, an IN field at 0) holding a boolean read as a number."""
    if isinstance(key, ArithmeticKey):
        return _arithmetic_numeric_booleans(key, field_type=field_type)
    if isinstance(key, InKey):
        numbers = any(_is_number_literal(v) for v in key.values)
        return frozenset({0}) if numbers and boolean_valued(key.field, field_type=field_type) else frozenset()
    if isinstance(key, ScalarCallKey):
        return _scalar_numeric_booleans(key, field_type=field_type)
    return frozenset()


_TEMPORAL_PAIR_OPS = frozenset({"==", "=", "!=", "<>", "<", "<=", ">", ">=", "is distinct from", "is not distinct from"})


def _date_beside_timestamp(types: Sequence[Optional[DataType]]) -> Optional[int]:
    """The DATE's position among two types that are a DATE and a timestamp, else ``None``."""
    kinds = [kind_of(t) for t in types]
    if len(kinds) != 2 or set(kinds) != {TypeKind.DATE, TypeKind.TIMESTAMP}:
        return None
    return kinds.index(TypeKind.DATE)


def temporal_pair(key: object, *, field_type: FieldTypeFn) -> Optional[Tuple[DataType, DataType]]:
    """The operand types of a comparison between two non-literal temporal values, else ``None``."""
    if not isinstance(key, ArithmeticKey) or key.op.lower() not in _TEMPORAL_PAIR_OPS or len(key.operands) != 2:
        return None
    if any(isinstance(o, LiteralKey) for o in key.operands):
        return None
    left, right = (temporal_type(o, field_type=field_type) for o in key.operands)
    return None if left is None or right is None else (left, right)


def mixed_nullif(key: object, *, field_type: FieldTypeFn) -> Optional[int]:
    """The DATE argument's position of a ``nullif`` comparing a DATE with a timestamp, else ``None``."""
    if not isinstance(key, ScalarCallKey) or key.name != "nullif" or len(key.args) != 2:
        return None
    return _date_beside_timestamp([temporal_type(a, field_type=field_type) for a in key.args])


def date_promotion_positions(key: object, *, field_type: FieldTypeFn) -> FrozenSet[int]:
    """Child positions of ``key`` holding a DATE read as its midnight timestamp: a comparison's DATE beside a
    timestamp, and a timestamp-typed conditional's DATE value arguments."""
    pair = temporal_pair(key, field_type=field_type)
    if pair is not None:
        position = _date_beside_timestamp(pair)
        return frozenset() if position is None else frozenset({position})
    if (isinstance(key, ScalarCallKey) and key.name in CONDITIONAL_SCALARS
            and kind_of(value_type(key, field_type=field_type)) is TypeKind.TIMESTAMP):
        return frozenset(
            i for i in value_arg_positions(key.name, len(key.args))
            if temporal_type(key.args[i], field_type=field_type) is DataType.DATE
        )
    return frozenset()


def _boolean_int_expression(key: object, *, field_type: FieldTypeFn) -> bool:
    """An integer built only from booleans and integer literals (``coalesce(flag, 0)``, ``(a > 1) + 1``)."""
    if isinstance(key, ArithmeticKey) and key.op in _INT_OPS:
        children = key.operands
    elif isinstance(key, ScalarCallKey) and key.name in _INT_SCALARS:
        children = key.args
    elif isinstance(key, ScalarCallKey) and key.name in _BOOLEAN_BRANCH_SCALARS:
        positions = _BOOLEAN_BRANCH_SCALARS[key.name]
        children = key.args if positions is None else [key.args[i] for i in positions if i < len(key.args)]
    else:
        return False
    values = [c for c in children if not _is_null_arg(c)]
    leaves_ok = all(
        _is_int_literal(c) or boolean_valued(c, field_type=field_type)
        or _boolean_int_expression(c, field_type=field_type)
        for c in values
    )
    return leaves_ok and any(not _is_number_literal(c) for c in values)


def aggregation_source_type(source: object, *, field_type: FieldTypeFn) -> Optional[DataType]:
    """The type an aggregation reads from ``source``: BOOLEAN when boolean-valued, INT when built from booleans and
    integer literals, ARRAY for an element set, a field's, cast's or conditional's type, else ``None``."""
    if isinstance(source, ListSetKey):
        return DataType.ARRAY
    if boolean_valued(source, field_type=field_type):
        return DataType.BOOLEAN
    if _boolean_int_expression(source, field_type=field_type):
        return DataType.INTEGER
    if isinstance(source, (FieldKey, FieldSqlKey, CastKey)) or (
            isinstance(source, ScalarCallKey) and source.name in CONDITIONAL_SCALARS):
        return value_type(source, field_type=field_type)
    return None


# The element type of a list-typed field leaf (``None`` when the leaf is not a list or unknown).
FieldElementFn = Callable[[Any], Optional[DataType]]
# Transforms whose value is one of their operand's values.
_VALUE_PRESERVING_TRANSFORMS = frozenset({"lag", "lead", "time_shift"})


def _returned_args(key: ScalarCallKey) -> List[object]:
    """The non-NULL arguments a value-returning scalar may return."""
    returned = [key.args[i] for i in value_arg_positions(key.name, len(key.args))]
    if key.name == "nullif":
        returned = returned[:1]
    return [a for a in returned if not _is_null_arg(a)]


# Value-returning conditionals: every argument they may return shares one type.
CONDITIONAL_SCALARS = frozenset({"iif", "coalesce", "ifnull", "greatest", "least"})


def _fold_branch_types(
    key: ScalarCallKey, *, field_type: FieldTypeFn, aggregate_type: Optional[AggregateTypeFn] = None,
) -> Tuple[Optional[DataType], Optional[Tuple[DataType, DataType]]]:
    """``(joined type, None)`` over a conditional's returned arguments, else ``(None, the first pair that cannot
    join)``; an untyped or UNKNOWN argument is absorbed."""
    joined: Optional[DataType] = None
    for arg in _returned_args(key):
        t = value_type(arg, field_type=field_type, aggregate_type=aggregate_type)
        if t is None or t.is_opaque:
            continue
        if joined is not None:
            shared = _joined_known_types(joined, t)
            if shared is None:
                return None, (joined, t)
            t = shared
        joined = t
    return joined, None


def conditional_branch_conflict(
    key: ScalarCallKey, *, field_type: FieldTypeFn,
) -> Optional[Tuple[DataType, DataType]]:
    """The first two branch types of a value-returning conditional that cannot share a type, else ``None``."""
    if key.name not in CONDITIONAL_SCALARS:
        return None
    return _fold_branch_types(key, field_type=field_type)[1]


def list_valued(key: object, *, field_type: FieldTypeFn) -> bool:
    """Whether ``key``'s value is certainly a list."""
    if isinstance(key, (FieldKey, FieldSqlKey)):
        return field_type(key) is DataType.ARRAY
    if isinstance(key, (ArrayLiteralKey, ListSetKey)):
        return True
    if isinstance(key, AggregateKey):
        return key.builtin and key.agg.lower() in LIST_AGGREGATIONS
    if isinstance(key, TransformKey):
        return key.op in _VALUE_PRESERVING_TRANSFORMS and list_valued(key.input, field_type=field_type)
    if isinstance(key, CastKey):
        return key.data_type is DataType.ARRAY
    if isinstance(key, ScalarCallKey) and key.name in _VALUE_ARG_SCALARS:
        returned = _returned_args(key)
        return bool(returned) and all(list_valued(a, field_type=field_type) for a in returned)
    return False


def is_list_call(key: ScalarCallKey, *, field_type: FieldTypeFn) -> bool:
    """A list-function call over a list (``contains`` / ``length`` over TEXT keep their string meaning)."""
    if key.name in LIST_ONLY_FUNCTIONS:
        return True
    return key.name in LIST_FUNCTIONS and bool(key.args) and list_valued(key.args[0], field_type=field_type)


def elements_as_numbers(values: "ValueKey", *, text: bool) -> CastKey:
    """``values``' elements read one at a time as numbers (``text``: TEXT elements, through ``TRY_CAST``)."""
    return CastKey(source=values, type_text="DOUBLE", data_type=DataType.FLOAT, try_=text)


def numbers_read_list(key: ScalarCallKey, *, field_type: FieldTypeFn) -> Optional[CastKey]:
    """The second list of a ``contains_any`` / ``contains_all`` built by ``elements_as_numbers``, else ``None``."""
    other = key.args[1] if key.name in LIST_ONLY_FUNCTIONS and len(key.args) == 2 else None
    if isinstance(other, CastKey) and other.data_type is DataType.FLOAT and list_valued(
            other.source, field_type=field_type):
        return other
    return None


def literal_type(value: object) -> Optional[DataType]:
    """The type of a scalar literal value (``None`` for NULL)."""
    if isinstance(value, bool):
        return DataType.BOOLEAN
    if isinstance(value, Decimal):
        return DataType.INTEGER if value == value.to_integral_value() else DataType.FLOAT
    if isinstance(value, str):
        return DataType.STRING
    if isinstance(value, datetime):
        return DataType.DATETIME
    if isinstance(value, date):
        return DataType.DATE
    return None


# An expression aggregation's declared or inferred result type, ``None`` when its expression cannot be typed.
AggregateTypeFn = Callable[["AggregateKey"], Optional[DataType]]


def aggregation_result_type(
    *, metric_name: Optional[str], aggregation: str, source_type: Optional[DataType],
) -> Tuple[Optional[DataType], Optional[DataType]]:
    """A built-in aggregation's ``(type, element type)``: COUNT → INT, the float classes → DOUBLE, PRESERVING →
    ``source_type``, LIST → a list of ``source_type``."""
    cls = classify_aggregation(metric_name=metric_name, aggregation=aggregation, source_type=source_type)
    if cls is AggregationValueClass.COUNT:
        return DataType.INTEGER, None
    if cls is AggregationValueClass.LIST:
        return DataType.ARRAY, source_type or DataType.OPAQUE
    if cls is AggregationValueClass.PRESERVING:
        return source_type, None
    return DataType.FLOAT, None


def _aggregate_value_type(
    key: AggregateKey, *, field_type: FieldTypeFn, aggregate_type: Optional[AggregateTypeFn],
) -> Optional[DataType]:
    """An expression aggregation: declared → inferred → its input's type (INT over ``*``); a built-in by its class."""
    if not key.builtin and aggregate_type is not None:
        declared = aggregate_type(key)
        if declared is not None:
            return declared
    star = isinstance(key.source, StarKey)
    source_type = None if star else (
        aggregation_source_type(key.source, field_type=field_type)
        or value_type(key.source, field_type=field_type, aggregate_type=aggregate_type)
    )
    if not key.builtin:
        return DataType.INTEGER if star else source_type
    agg = key.agg.lower()
    return aggregation_result_type(metric_name="*" if star else None, aggregation=agg, source_type=source_type)[0]


def _transform_value_type(
    key: TransformKey, *, field_type: FieldTypeFn, aggregate_type: Optional[AggregateTypeFn],
) -> Optional[DataType]:
    if key.op in _NUMBER_RESULT_TRANSFORMS and key.op != "percent_rank":
        return DataType.INTEGER
    if key.op in ("change_pct", "percent_rank"):
        return DataType.FLOAT
    inner = value_type(key.input, field_type=field_type, aggregate_type=aggregate_type)
    if key.op == "cumsum":
        return aggregation_result_type(metric_name=None, aggregation="sum", source_type=inner)[0]
    if key.op == "change":
        return _arithmetic_result_type("-", [inner, inner])
    return inner


def _arithmetic_result_type(op: str, types: Sequence[Optional[DataType]]) -> Optional[DataType]:
    """``+ - * %`` over integers (booleans read as theirs) is INT, over exact numbers with a DECIMAL among them
    DECIMAL; any other number mix (or ``/``) DOUBLE."""
    if any(t is None for t in types) or op not in _INT_OPS:
        return DataType.FLOAT
    if all(t in (DataType.INTEGER, DataType.BOOLEAN) for t in types):
        return DataType.INTEGER
    if DataType.DECIMAL in types and all(t in _EXACT_NUMBER_TYPES for t in types):
        return DataType.DECIMAL
    return DataType.FLOAT


_EXACT_NUMBER_TYPES = frozenset({DataType.INTEGER, DataType.BOOLEAN, DataType.DECIMAL})


def value_type(
    key: object, *, field_type: FieldTypeFn, aggregate_type: Optional[AggregateTypeFn] = None,
) -> Optional[DataType]:
    """``key``'s type; ``None`` only for NULL or an unresolvable field leaf."""
    if isinstance(key, LiteralKey):
        return literal_type(key.value)
    if isinstance(key, (FieldKey, FieldSqlKey)):
        return field_type(key)
    if isinstance(key, CastKey):
        return None if kind_of(key.data_type) is TypeKind.UNKNOWN else key.data_type
    if isinstance(key, AggregateKey):
        return _aggregate_value_type(key, field_type=field_type, aggregate_type=aggregate_type)
    if isinstance(key, TransformKey):
        return _transform_value_type(key, field_type=field_type, aggregate_type=aggregate_type)
    if isinstance(key, WindowKey):
        return _window_value_type(key, field_type=field_type, aggregate_type=aggregate_type)
    return _row_value_type(key, field_type=field_type, aggregate_type=aggregate_type)


def _window_value_type(
    key: WindowKey, *, field_type: FieldTypeFn, aggregate_type: Optional[AggregateTypeFn],
) -> Optional[DataType]:
    """An integer rank or count, a fraction, or the type of the argument it returns (or sums)."""
    if key.function in _WINDOW_INTEGER:
        return DataType.INTEGER
    if key.function in _WINDOW_FRACTION:
        return DataType.FLOAT
    argument = _window_argument(key)
    return None if argument is None else value_type(argument, field_type=field_type, aggregate_type=aggregate_type)


def _row_value_type(
    key: object, *, field_type: FieldTypeFn, aggregate_type: Optional[AggregateTypeFn],
) -> Optional[DataType]:
    if list_valued(key, field_type=field_type):
        return DataType.ARRAY
    if isinstance(key, ScalarCallKey) and key.name in CONDITIONAL_SCALARS:
        return _fold_branch_types(key, field_type=field_type, aggregate_type=aggregate_type)[0]
    if isinstance(key, ScalarCallKey) and key.name == "nullif" and key.args:
        # Its first argument or NULL: that argument's resolved type (an opaque one kept).
        return value_type(key.args[0], field_type=field_type, aggregate_type=aggregate_type)
    if boolean_valued(key, field_type=field_type):
        return DataType.BOOLEAN
    if isinstance(key, ArithmeticKey) and key.op in _ARITHMETIC_OPS:
        # A NULL literal operand is absorbed; all-NULL is NULL-only (``None``).
        operands = [o for o in key.operands if not _is_null_arg(o)]
        if not operands:
            return None
        return _arithmetic_result_type(key.op, [
            value_type(o, field_type=field_type, aggregate_type=aggregate_type) for o in operands])
    return _computed_value_type(key, field_type=field_type, aggregate_type=aggregate_type)


def _computed_value_type(
    key: object, *, field_type: FieldTypeFn, aggregate_type: Optional[AggregateTypeFn],
) -> Optional[DataType]:
    temporal = temporal_type(key, field_type=field_type)
    if temporal is not None:
        return temporal
    if isinstance(key, ScalarCallKey) and key.name in _INT_SCALARS and key.args and (
            SCALAR_SPECS.get(key.name) is None or SCALAR_SPECS[key.name].result is None):
        return _int_scalar_type(key, field_type=field_type, aggregate_type=aggregate_type)
    if integer_valued(key, field_type=field_type):
        return DataType.INTEGER
    if numeric_valued(key, field_type=field_type):
        return DataType.FLOAT
    if isinstance(key, ScalarCallKey):
        return _scalar_value_type(key, field_type=field_type, aggregate_type=aggregate_type)
    if isinstance(key, TimeTruncKey):
        return value_type(key.source, field_type=field_type, aggregate_type=aggregate_type)
    return None


def _int_scalar_type(
    key: ScalarCallKey, *, field_type: FieldTypeFn, aggregate_type: Optional[AggregateTypeFn],
) -> Optional[DataType]:
    """``abs`` / ``round`` / … from its operands' resolved types: ``mod`` as ``%``; otherwise a DECIMAL first operand
    keeps DECIMAL, integers (or booleans) stay INT unless ``round`` takes digits, any other number is DOUBLE;
    all-NULL is ``None``."""
    types = [value_type(a, field_type=field_type, aggregate_type=aggregate_type)
             for a in key.args if not _is_null_arg(a)]
    if not types:
        return None
    if key.name == "mod":
        return _arithmetic_result_type("%", types)
    if types[0] is DataType.DECIMAL:
        return DataType.DECIMAL
    digits = key.name == "round" and len(key.args) > 1
    if not digits and all(t in (DataType.INTEGER, DataType.BOOLEAN) for t in types):
        return DataType.INTEGER
    return DataType.FLOAT


def _scalar_value_type(
    key: ScalarCallKey, *, field_type: FieldTypeFn, aggregate_type: Optional[AggregateTypeFn] = None,
) -> Optional[DataType]:
    spec = SCALAR_SPECS.get(key.name)
    if spec is not None and spec.result is not None:
        return spec.result
    types = {value_type(a, field_type=field_type, aggregate_type=aggregate_type) for a in _returned_args(key)}
    return types.pop() if len(types) == 1 else None


def list_element_type(
    key: object, *, field_type: FieldTypeFn, field_element: FieldElementFn,
) -> Optional[DataType]:
    """The element type of a list-valued ``key`` (``None`` when unknown, e.g. ``[]``)."""
    if isinstance(key, (FieldKey, FieldSqlKey)):
        return field_element(key)
    if isinstance(key, ArrayLiteralKey):
        types = {literal_type(v.value) for v in key.values}
        return types.pop() if len(types) == 1 else None
    if isinstance(key, AggregateKey):
        return value_type(key.source, field_type=field_type) if list_valued(key, field_type=field_type) else None
    if isinstance(key, TransformKey):
        return list_element_type(key.input, field_type=field_type, field_element=field_element)
    if isinstance(key, ListSetKey):
        return list_element_type(key.source, field_type=field_type, field_element=field_element)
    if isinstance(key, CastKey):
        return key.element_type
    if isinstance(key, ScalarCallKey):
        types = {list_element_type(a, field_type=field_type, field_element=field_element)
                 for a in _returned_args(key)} - {None}
        return types.pop() if len(types) == 1 else None
    return None


def unit_word(arg: object) -> str:
    """The lower-cased unit/part word of a date scalar's unit argument."""
    value = arg.value if isinstance(arg, LiteralKey) else arg
    if not isinstance(value, str):
        raise TypeError(f"A date unit argument must be a string literal, got {value!r}.")
    return value


def date_add_type(base: DataType, unit: TimeGranularity) -> DataType:
    """``date_add``'s result type: a DATE stays a DATE for day-or-coarser units."""
    return DataType.DATE if base is DataType.DATE and unit not in SUB_DAY_GRANULARITIES else DataType.DATETIME


def conditional_number_format(
    a: Optional[NumberFormat], b: Optional[NumberFormat],
) -> Optional[NumberFormat]:
    """A conditional carries a number format only when both branches agree."""
    return a if (a is not None and a == b) else None


# ---------------------------------------------------------------------------
# Key classification and rewrites shared by binding, elaboration and compilation
# ---------------------------------------------------------------------------


def window_kwarg_of(key: ValueKey):
    """The ``window=`` kwarg value of an ``AggregateKey``, or ``None``."""
    if isinstance(key, AggregateKey):
        for k, v in key.kwargs:
            if k == "window":
                return v
    return None


def is_kernel_requiring(key: ValueKey) -> TypeGuard[AggregateKey]:
    """A ranked (``first``/``last``) or windowed aggregate — rendered only as its own kernel producer's answer."""
    return isinstance(key, AggregateKey) and (is_ranked(key) or window_kwarg_of(key) is not None)


def is_ranked(key: AggregateKey) -> bool:
    """A ``first`` / ``last`` pick with the built-in ranked semantics."""
    return key.builtin and key.agg in RANKED_AGGREGATIONS


def explicit_ranking_key(key: AggregateKey) -> Optional[ValueKey]:
    """A ranked aggregate's explicit ranking key (its first positional), or ``None``."""
    if not is_ranked(key) or not key.args:
        return None
    arg = key.args[0]
    return cast(ValueKey, arg) if isinstance(arg, VALUE_KEY_TYPES) else None


def restricts_own_rows(key: ValueKey) -> TypeGuard[AggregateKey]:
    """A FILTERed non-built-in aggregate: ``filter`` restricts its producer's rows, never its inputs."""
    return isinstance(key, AggregateKey) and key.filter is not None and not key.builtin


# Row-valued aggregation parameters a FILTER masks (literals and attached values are not rows).
_ROW_INPUT_KINDS = (FieldKey, FieldSqlKey, SqlFragmentKey)


def input_masked(key: AggregateKey, *, condition: ValueKey) -> AggregateKey:
    """``key`` with its source (a star as ``1``) and every row-valued kwarg masked to NULL where ``condition`` fails."""
    def _masked(value: ValueKey) -> ScalarCallKey:
        return ScalarCallKey(name="iif", args=(condition, value, LiteralKey(value=None)))

    value = LiteralKey(value=normalize_scalar(1)) if isinstance(key.source, StarKey) else key.source
    return key.model_copy(update={
        "source": _masked(value),
        "kwargs": tuple((k, _masked(v) if isinstance(v, _ROW_INPUT_KINDS) else v) for k, v in key.kwargs),
    })


def is_local_partitioned_agg(k: ValueKey) -> bool:
    """A LOCAL aggregate with an explicit ``partition_by=`` grain."""
    return (
        isinstance(k, AggregateKey)
        and k.partition_keys is not None
        and not source_anchor_path(k.source)
    )


def is_local_combined_regroup_ref(
    k: ValueKey, *, row_agg_set: frozenset = frozenset(),
) -> bool:
    """A LOCAL aggregate attached at the COMBINED SELECT (explicit ``partition_by=``
    or a bare windowed/first/last metric); ``row_agg_set`` aggregates excluded."""
    return (
        isinstance(k, AggregateKey)
        and not source_anchor_path(k.source)
        and k not in row_agg_set
        and (k.partition_keys is not None or is_kernel_requiring(k))
    )


# Null-safe comparisons: never UNKNOWN.
NULL_SAFE_COMPARISON_OPS = frozenset({"is distinct from", "is not distinct from"})
PREDICATE_COMPARISON_OPS = frozenset(
    {"==", "=", "!=", "<>", "<", "<=", ">", ">=", "is", "is not", *NULL_SAFE_COMPARISON_OPS}
)
BOOL_CONNECTIVE_OPS = frozenset({"and", "or", "not"})


def is_boolean_shaped(key: "ValueKey") -> bool:
    """Whether ``key`` renders as a SQL predicate (truth value) rather than a
    numeric/text value: a comparison, a null test, IN, or an ``and`` /
    ``or`` / ``not`` connective."""
    if isinstance(key, ArithmeticKey):
        return (
            key.op in PREDICATE_COMPARISON_OPS
            or key.op in BOOL_CONNECTIVE_OPS
        )
    return isinstance(key, (TimePointCmpKey, InKey))


def split_top_level_and(vk: ValueKey) -> List[ValueKey]:
    """Top-level AND conjuncts; only ``and`` splits (OR/comparisons stay whole)."""
    if isinstance(vk, ArithmeticKey) and vk.op == "and":
        out: List[ValueKey] = []
        for o in vk.operands:
            out.extend(split_top_level_and(o))
        return out
    return [vk]


def rewrite_rank_partition_keys(
    key: ValueKey, *, rewrite_fn: Callable[[AggregateKey | TransformKey], Grain],
) -> ValueKey:
    """Replace every rank-family ``TransformKey``'s / partitioned aggregate's ``partition_keys`` via ``rewrite_fn``; identity-preserving, runs before interning. Post-order; ``rewrite_fn`` receives the pre-rebuild node."""
    rebuilt = key.map_children(
        lambda c: rewrite_rank_partition_keys(key=c, rewrite_fn=rewrite_fn),
    )
    if (
        isinstance(key, TransformKey)
        and key.op in RANK_FAMILY_TRANSFORMS
        and bool(key.partition_keys)
    ) or (isinstance(key, AggregateKey) and bool(key.partition_keys)):
        new_pk = rewrite_fn(key)
        assert isinstance(rebuilt, (TransformKey, AggregateKey))
        if new_pk != rebuilt.partition_keys:
            rebuilt = rebuilt.model_copy(update={"partition_keys": new_pk})
    return rebuilt


def desugar_change(key: TransformKey) -> ArithmeticKey:
    """``change(x)`` → ``x - time_shift(x, periods=-1)``; inner ``x`` is identity-preserving so the registry interns it once."""
    assert key.op == "change", f"desugar_change expected op='change', got {key.op!r}."
    inner = key.input
    shifted = TransformKey(
        op="time_shift",
        input=inner,
        kwargs=(("periods", normalize_scalar(-1)),),
        partition_keys=key.partition_keys,
        time_key=key.time_key,
    )
    return ArithmeticKey(op="-", operands=(inner, shifted))


def desugar_change_pct(key: TransformKey) -> ArithmeticKey:
    """``change_pct(x)`` → ``(x - time_shift(x,-1)) / NULLIF(time_shift(x,-1), 0)``; NULLIF guards a zero prior value."""
    assert key.op == "change_pct", (
        f"desugar_change_pct expected op='change_pct', got {key.op!r}."
    )
    inner = key.input
    shifted = TransformKey(
        op="time_shift",
        input=inner,
        kwargs=(("periods", normalize_scalar(-1)),),
        partition_keys=key.partition_keys,
        time_key=key.time_key,
    )
    numerator = ArithmeticKey(op="-", operands=(inner, shifted))
    guarded_divisor = ScalarCallKey(
        name="nullif", args=(shifted, normalize_scalar(0)),
    )
    return ArithmeticKey(op="/", operands=(numerator, guarded_divisor))


def shift_offset_of(key: TransformKey) -> Tuple[int, Optional[Granularity]]:
    """A ``time_shift`` key's ``(periods, granularity)``; ``periods`` must be an integer."""
    kwargs = dict(key.kwargs)
    periods = kwargs.get("periods")
    if isinstance(periods, Decimal) and periods == periods.to_integral_value():
        periods = int(periods)
    if isinstance(periods, bool) or not isinstance(periods, int):
        raise ValueError(f"time_shift periods must be an integer; got {periods!r}")
    granularity = kwargs.get("granularity")
    if granularity is None or isinstance(granularity, CustomGranularity):
        return periods, granularity
    return periods, TimeGranularity(str(granularity).lower())


def lower_sugar_transforms(key: ValueKey) -> ValueKey:
    """Recursively lower ``change``/``change_pct`` TransformKeys to desugared arithmetic, preserving the inner aggregate's identity. Post-order over ``map_children``."""
    lowered = key.map_children(lower_sugar_transforms)
    if isinstance(lowered, TransformKey):
        if lowered.op == "change":
            return desugar_change(lowered)
        if lowered.op == "change_pct":
            return desugar_change_pct(lowered)
    return lowered

def operand_aggregates(source: ValueKey) -> List[AggregateKey]:
    """The top-level attached aggregates of a re-aggregation source (the direct
    constituents, deduped — a composite may repeat one), not descending through
    a nested aggregate's own source."""
    out: List[AggregateKey] = []

    def _walk(k: ValueKey) -> None:
        if isinstance(k, AggregateKey):
            if k not in out:
                out.append(k)
            return
        for c in k.children():
            _walk(c)

    _walk(source)
    return out


def operand_constituents(source: ValueKey) -> List[ValueKey]:
    """The top-level attached constituents of an aggregation source — nested
    aggregates AND grained transforms (Axiom 2.3), deduped and opaque (neither
    descended past). The generalisation of :func:`operand_aggregates` that treats
    a transform as the typed dataset it is."""
    out: List[ValueKey] = []

    def _walk(k: ValueKey) -> None:
        if isinstance(k, (AggregateKey, TransformKey)) or is_transform(k):
            if k not in out:
                out.append(k)
            return
        for c in k.children():
            _walk(c)

    _walk(source)
    return out


def source_row_leaves(source: ValueKey) -> List[ValueKey]:
    """The top-level ROW-level field leaves of an aggregation source (not
    descending through an attached constituent — a nested aggregate or a grained
    transform — whose own leaves are opaque)."""
    out: List[ValueKey] = []

    def _walk(k: ValueKey) -> None:
        if isinstance(k, (AggregateKey, TransformKey)) or is_transform(k):
            return
        if isinstance(k, (FieldKey, FieldSqlKey, StarKey)):
            out.append(k)
            return
        for c in k.children():
            _walk(c)

    _walk(source)
    return out


def parameter_row_leaves(value) -> List[ValueKey]:
    """The row-level field leaves of an aggregation parameter value; a scalar, a
    marker string or an attached value (aggregate / transform) has none."""
    if not isinstance(value, _FrozenKey) or isinstance(value, (AggregateKey, TransformKey)) or is_transform(value):
        return []
    return source_row_leaves(cast(ValueKey, value))


def source_leaf_paths(source: ValueKey) -> List[Tuple[str, ...]]:
    """Relationship paths of the row-level field/star leaves of an aggregation source.

    Attached constituents — nested aggregates and grained transforms — are opaque
    (they contribute their grain, not a source leaf); a literal-only source yields
    no leaf. The one accessor every "where does this source live" site reads (D1)."""
    out: List[Tuple[str, ...]] = []

    def _walk(k: ValueKey) -> None:
        if isinstance(k, (AggregateKey, TransformKey)) or is_transform(k):
            return
        if isinstance(k, (FieldKey, FieldSqlKey, StarKey)):
            out.append(k.path)
            return
        for c in k.children():
            _walk(c)

    _walk(source)
    return out


def source_anchor_path(source: ValueKey) -> Tuple[str, ...]:
    """Where an aggregation source lives: the longest common prefix of its
    row-level leaves' paths. A bare field/star yields its own path; a
    literal-only or fully-attached source yields ``()`` (the root)."""
    paths = source_leaf_paths(source)
    if not paths:
        return ()
    prefix = paths[0]
    for path in paths[1:]:
        i = 0
        while i < len(prefix) and i < len(path) and prefix[i] == path[i]:
            i += 1
        prefix = prefix[:i]
        if not prefix:
            break
    return prefix


def _grain_transform_inner_aggregates(
    t: "TransformKey | WindowKey", *, query_grain: Grain,
) -> "TransformKey | WindowKey":
    """Grain every ungrained, non-windowed, LOCAL aggregate under ``t.input`` at
    ``query_grain`` — a cross-dataset or windowed or already-explicit inner is left
    untouched. Post-order over ``map_children``."""
    def _grain(node: ValueKey) -> ValueKey:
        rebuilt = node.map_children(_grain)
        if (
            isinstance(rebuilt, AggregateKey)
            and rebuilt.partition_keys is None
            and window_kwarg_of(rebuilt) is None
            and not source_anchor_path(rebuilt.source)
        ):
            return rebuilt.model_copy(update={"partition_keys": query_grain})
        return cast("ValueKey", rebuilt)  # map_children preserves ValueKey-ness

    if isinstance(t, WindowKey):
        return t.map_children(_grain)
    new_input = _grain(t.input)
    return t if new_input is t.input else t.model_copy(update={"input": new_input})


def map_attached_inputs(
    key: AggregateKey, fn: Callable[[ValueKey], ValueKey],
) -> AggregateKey:
    """Apply ``fn`` to each top-level attached constituent of ``key`` across
    source, positional args and keyword-arg values — the same enumeration as
    :func:`attached_inputs` — substituting per position by identity (D2). Partition
    keys are never rewritten. The one law both transform-constituent rewrites run
    through, so the source, positional and keyword parameter positions share it."""
    subs: Dict[ValueKey, ValueKey] = {}
    for inp in (key.source, *key.args, *(v for _, v in key.kwargs)):
        if isinstance(inp, _FrozenKey):
            for c in operand_constituents(inp):
                if c not in subs and (new_c := fn(c)) is not c:
                    subs[c] = new_c
    if not subs:
        return key

    def _sub(v):
        return substitute_value_keys(key=v, mapping=subs) if isinstance(v, _FrozenKey) else v

    return key.model_copy(update={
        "source": _sub(key.source),
        "args": tuple(_sub(a) for a in key.args),
        "kwargs": tuple((k, _sub(v)) for k, v in key.kwargs),
    })


def normalize_transform_constituents(
    key: ValueKey, *, query_grain: Grain,
) -> ValueKey:
    """Explicitly grain the ungrained inner aggregates of a transform constituent
    at ``query_grain`` (D4b, Axiom 11.1): a time-ordered constituent then has an
    axis and a mixed operand shares one canonical grain. Post-order over source,
    args and kwargs (D2); runs before partition-key validation, so the synthesized
    keys face the same attributability / resolution checks as a user-written
    ``partition_by=``."""
    rebuilt = key.map_children(
        lambda c: normalize_transform_constituents(key=c, query_grain=query_grain),
    )
    if not isinstance(rebuilt, AggregateKey):
        return cast("ValueKey", rebuilt)  # map_children preserves ValueKey-ness

    def _grain(c: ValueKey) -> ValueKey:
        if is_transform(c):
            return _grain_transform_inner_aggregates(t=c, query_grain=query_grain)
        return c

    return map_attached_inputs(key=rebuilt, fn=_grain)


def is_ranked_over_cells(key: ValueKey) -> TypeGuard[AggregateKey]:
    """A ``first`` / ``last`` over an aggregate: it ranks its operand's cells (semantics Axiom 2.4)."""
    return isinstance(key, AggregateKey) and is_ranked(key) and is_attached_source(key.source)


def settle_ranked_grains(
    key: ValueKey, *, query_grain: Grain, active_bucket: Optional[ValueKey],
    row_default_key: Callable[["AggregateKey"], Optional[ValueKey]],
) -> ValueKey:
    """Each unwindowed ranked aggregation's grain drops its ranking key on an exact match (semantics Axiom 5): its
    base is ``partition_by=``, else the query's dimensions over rows or the operand's grain over an aggregate. A
    dropped row-form default key (``row_default_key`` of that aggregation, read for its home) becomes explicit.
    Post-order: a nested pick settles first."""
    rebuilt = cast(ValueKey, key.map_children(lambda c: settle_ranked_grains(
        c, query_grain=query_grain, active_bucket=active_bucket, row_default_key=row_default_key,
    )))
    if not isinstance(rebuilt, AggregateKey) or not is_ranked(rebuilt) or window_kwarg_of(rebuilt) is not None:
        return rebuilt
    over_cells = is_attached_source(rebuilt.source)
    if over_cells:
        # The operand keeps the query's grain wherever the pick is consumed (Axiom 5; as a transform's, D4b).
        rebuilt = map_attached_inputs(rebuilt, lambda c: _grained_at(c, query_grain=query_grain))
    base = rebuilt.partition_keys
    if base is None:
        base = transform_operand_grain(
            rebuilt.source, query_grain=query_grain, active_bucket=active_bucket,
        ) if over_cells else query_grain
    rk = explicit_ranking_key(rebuilt) or (None if over_cells else row_default_key(rebuilt))
    if rk is None or rk not in base:
        return rebuilt.model_copy(update={"partition_keys": base}) if over_cells else rebuilt
    return rebuilt.model_copy(update={"args": (rk,), "partition_keys": base - {rk}})


def _grained_at(c: ValueKey, *, query_grain: Grain) -> ValueKey:
    """An ungrained, unwindowed local aggregate grained at ``query_grain``; any other constituent as is."""
    if (
        isinstance(c, AggregateKey) and c.partition_keys is None and window_kwarg_of(c) is None
        and not source_anchor_path(c.source)
    ):
        return c.model_copy(update={"partition_keys": query_grain})
    return c


def attached_inputs(k: ValueKey) -> List[ValueKey]:
    """Deduped top-level attached constituents — aggregates and grained
    transforms — across source, args and kwargs (source first)."""
    if not isinstance(k, AggregateKey):
        return []
    out: List[ValueKey] = []
    for inp in (k.source, *k.args, *(v for _, v in k.kwargs)):
        if isinstance(inp, _FrozenKey):
            for c in operand_constituents(inp):
                if c not in out:
                    out.append(c)
    return out


def is_attached_source(source: ValueKey) -> bool:
    """Attached values only: at least one constituent and no row leaf."""
    return bool(operand_constituents(source)) and not source_row_leaves(source)


def is_reaggregation_key(k: ValueKey) -> TypeGuard[AggregateKey]:
    """Aggregate over an attached source — axiom 6. A source mixing a row leaf
    with an attached value is row grain."""
    return isinstance(k, AggregateKey) and is_attached_source(k.source)


def is_row_attach_root(k: ValueKey) -> TypeGuard[AggregateKey]:
    """Row-grain aggregate carrying attached inputs to broadcast per row: a mixed
    / literal / plain-row source with an attached constituent or parameter.
    Not a re-aggregation, so it never reaches the carrier."""
    return (
        isinstance(k, AggregateKey)
        and not is_reaggregation_key(k)
        and bool(attached_inputs(k))
    )


def attached_operand_keys(vks: Sequence[ValueKey]) -> FrozenSet[ValueKey]:
    """Aggregates AND transforms nested (any depth) in the inputs of any root with
    attached inputs — re-aggregation or row-attach. The lenient partition-key set
    the bind pass needs so a row-attached constituent's or transform parameter's own
    rank-family ``partition_by=`` is not mis-flagged as a combined consumer (D6)."""
    out: set = set()

    def _scan(k: ValueKey) -> None:
        if isinstance(k, AggregateKey) and attached_inputs(k):
            for r in (k.source, *k.args, *(v for _, v in k.kwargs)):
                if isinstance(r, _FrozenKey):
                    out.update(c for c in walk_value_keys(r)
                               if isinstance(c, (AggregateKey, TransformKey)) or is_transform(c))
            return
        for c in k.children():
            _scan(c)

    for vk in vks:
        _scan(vk)
    return frozenset(out)


class ConsumerNode(NamedTuple):
    """One node of a consumer walk with its position context."""

    key: "ValueKey"
    own_pk: bool  # inside an ancestor's partition keys
    attach_pk: bool  # inside an attach-owning aggregate's partition keys
    dim_key: bool  # inside a subtree equal to a query dimension


def walk_consumer_positions(
    key: ValueKey, *, dim_keys: AbstractSet["ValueKey"] = frozenset(),
    own_pk: bool = False, attach_pk: bool = False, dim_key: bool = False,
    opaque: Optional[Callable[["ValueKey"], bool]] = None,
) -> Iterator[ConsumerNode]:
    """Reachable keys for root discovery, pre-order: opaque below an attach-owning
    aggregate's inputs (they belong to its own attach), still walking its partition
    keys — an attach-carrying computed dimension in ``partition_by=`` needs the
    outer attach the grain join is built on. An ``opaque`` key is yielded, not entered."""
    dim_key = dim_key or key in dim_keys
    yield ConsumerNode(key, own_pk, attach_pk, dim_key)
    if opaque is not None and opaque(key):
        return
    if isinstance(key, AggregateKey) and attached_inputs(key):
        for pk in key.partition_keys or ():
            yield from walk_consumer_positions(
                pk, dim_keys=dim_keys, own_pk=True, attach_pk=True, dim_key=dim_key,
                opaque=opaque,
            )
        return
    pks = frozenset(getattr(key, "partition_keys", None) or ())
    for c in key.children():
        yield from walk_consumer_positions(
            c, dim_keys=dim_keys, own_pk=own_pk or c in pks, attach_pk=attach_pk,
            dim_key=dim_key, opaque=opaque,
        )


def walk_consumer_keys(key: ValueKey) -> Iterator["ValueKey"]:
    """The keys of :func:`walk_consumer_positions`."""
    return (n.key for n in walk_consumer_positions(key))


def substitute_consumer_keys(
    key: _RerootableT, mapping: Mapping["ValueKey", "ValueKey"],
    *, opaque: Optional[Callable[["ValueKey"], bool]] = None,
) -> _RerootableT:
    """Replace sub-keys named in ``mapping`` where root discovery looks — the
    substitution law mirroring :func:`walk_consumer_keys`.

    Pre-order match-before-recurse like :func:`substitute_value_keys`, but below an
    attach-owning aggregate the source/args/kwargs are opaque (they belong to its
    own attach) and only ``partition_keys`` are traversed; an unmatched ``opaque``
    key is kept whole. Scalars ride through; identity is preserved when nothing matches.
    """
    if key is None or isinstance(key, (Decimal, str, bool, int, float)):
        return key
    if not isinstance(key, _FrozenKey):
        raise TypeError(
            f"substitute_consumer_keys has no case for {type(key).__name__!r}: "
            f"only value keys and scalars are substitutable."
        )
    if key in mapping:
        return cast(_RerootableT, mapping[cast("ValueKey", key)])
    if opaque is not None and opaque(cast("ValueKey", key)):
        return key
    if isinstance(key, AggregateKey) and attached_inputs(key):
        if key.partition_keys is None:
            return key
        new_pks = Grain.of(
            substitute_consumer_keys(p, mapping, opaque=opaque) for p in key.partition_keys
        )
        if new_pks == key.partition_keys:
            return key
        return cast(
            _RerootableT, key.model_copy(update={"partition_keys": new_pks})
        )
    return cast(
        _RerootableT,
        key.map_children(lambda c: substitute_consumer_keys(c, mapping, opaque=opaque)),
    )
