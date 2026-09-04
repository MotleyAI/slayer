"""Typed identity primitives for the resolution pipeline.

Identity is structural: two expression occurrences with the same key intern to
the same slot. Keys carry only what's needed to decide "are these the same slot?".
"""

from __future__ import annotations

from decimal import Decimal
from enum import IntEnum
from typing import (
    Callable,
    Literal,
    Mapping,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
    get_args,
)

from pydantic import BaseModel, ConfigDict, field_validator

from slayer.core.enums import DataType
from slayer.core.format import NumberFormat


# Regroup-consumed aggregates are substituted by a ColumnKey with this leaf
# prefix; a source model may not declare a real column with it during regroup.
REGROUP_LEAF_PREFIX = "__regroup__"


# Closed scalar-function allowlist (C12): anything outside this set in Mode B
# raises UnknownFunctionError at binding time. Single source of truth.
SCALAR_FUNCTIONS: frozenset[str] = frozenset({
    "nullif", "coalesce", "ifnull",
    "ln", "log10", "log2", "log", "exp", "sqrt", "pow", "power",
    "abs", "floor", "ceil", "ceiling", "round", "sign", "trunc", "mod",
    # Scalar min/max over the arguments (NOT the min:/max: aggregations).
    "greatest", "least",
    "lower", "upper", "trim", "ltrim", "rtrim",
    "replace", "substr", "substring", "instr", "length", "concat",
    "like",  # emits SQL LIKE operator
    "iif",  # emits CASE WHEN (CASE surface rewrites to it at parse time)
})


# Accepted argument counts per allowlisted scalar, as (min, max); max=None means
# variadic. Enforced because sqlglot silently mis-handles wrong-arity calls.
SCALAR_FUNCTION_ARITY: dict[str, tuple[int, Optional[int]]] = {
    "nullif": (2, 2),
    "coalesce": (1, None),
    "ifnull": (2, 2),
    "ln": (1, 1), "log10": (1, 1), "log2": (1, 1), "log": (1, 2),
    "exp": (1, 1), "sqrt": (1, 1),
    "pow": (2, 2), "power": (2, 2),
    "abs": (1, 1), "floor": (1, 1), "ceil": (1, 1), "round": (1, 2),
    "ceiling": (1, 1), "sign": (1, 1),  # ceiling: T-SQL spelling of ceil
    "trunc": (1, 1),  # 2-arg form silently drops digits on SQLite
    "mod": (2, 2),
    # Variadic min two: 1-arg MAX/MIN parse as the aggregate on SQLite.
    "greatest": (2, None), "least": (2, None),
    "lower": (1, 1), "upper": (1, 1), "trim": (1, 1), "length": (1, 1),
    # Trims take the string only; 2-arg strip-set form deferred (DEV-1793).
    "ltrim": (1, 1), "rtrim": (1, 1),
    "replace": (3, 3), "substr": (2, 3), "substring": (2, 3), "instr": (2, 2),
    "concat": (1, None),
    "like": (2, 2),
    "iif": (3, 3),
}

# Checked both ways at import: the table must cover SCALAR_FUNCTIONS exactly.
_arity_missing = SCALAR_FUNCTIONS - set(SCALAR_FUNCTION_ARITY)
_arity_unknown = set(SCALAR_FUNCTION_ARITY) - SCALAR_FUNCTIONS
if _arity_missing or _arity_unknown:  # pragma: no cover — import-time invariant
    raise RuntimeError(
        f"SCALAR_FUNCTION_ARITY disagrees with SCALAR_FUNCTIONS: "
        f"missing={sorted(_arity_missing)}, unknown={sorted(_arity_unknown)}",
    )


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


Scalar = Union[Decimal, str, bool, None]


def normalize_scalar(value):
    """Canonicalize a raw scalar before keying.

    bool checked before int (bool is-a int); int/float become Decimal (float via
    str, so it lands on the displayed decimal form). TypeError for anything else.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, str):
        return value
    raise TypeError(
        f"Cannot normalize scalar of type {type(value).__name__!r}: "
        f"only int/float/Decimal/str/bool/None are accepted (got {value!r})."
    )


class _FrozenKey(BaseModel):
    """Common config for the typed-key family: frozen (hashable, immutable).

    Every kind overrides the total-traversal protocol: ``children()`` yields the
    directly embedded value keys (scalars and the Mode-A-opaque
    ``AggregateKey.column_filter_key`` are never children); ``map_children``
    rebuilds one level with ``fn`` applied at each ``children()`` position,
    returning ``self`` when no child changed identity (``is``).
    """

    model_config = ConfigDict(frozen=True)

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
        new = self._fn(value)
        if new is not value:
            self.changed = True
        return new


class _LeafKey(_FrozenKey):
    """Traversal leaf: no embedded keys."""

    def children(self) -> Tuple["ValueKey", ...]:
        return ()

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "_LeafKey":
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
    return ("__key__", v)


def _typed_args(args):
    return tuple(_typed_leaf(a) for a in args)


def _typed_kwargs(kwargs):
    return tuple((k, _typed_leaf(v)) for k, v in kwargs)


class ColumnKey(_LeafKey):
    """Row-level reference to a base column on a model.

    ``path`` is the join walk from the query's source model to the terminal
    model — empty for local refs, non-empty for joined refs; ``leaf`` is the
    column name. Local and cross-model refs share this shape (P3).
    """

    path: Tuple[str, ...] = ()
    leaf: str

    @property
    def phase(self) -> Phase:
        return Phase.ROW


class ColumnSqlKey(_LeafKey):
    """Reference to a derived column (whose ``Column.sql`` is set).

    The expansion AST is recovered from the model at binding time — the key only
    carries identity. ``path`` works like ``ColumnKey.path``.
    """

    path: Tuple[str, ...] = ()
    model: str
    column_name: str

    @property
    def phase(self) -> Phase:
        return Phase.ROW


class TimeTruncKey(_FrozenKey):
    """Row-level reference to a time-truncated column, keyed by (column, granularity).

    ``column`` is a ``ColumnKey`` (base temporal column) or ``ColumnSqlKey``
    (derived). ``granularity`` is a ``TimeGranularity`` member's string value.
    Different granularities on the same column are distinct slots.
    """

    column: Union["ColumnKey", "ColumnSqlKey"]
    granularity: str

    @property
    def phase(self) -> Phase:
        return Phase.ROW

    def children(self) -> Tuple["ValueKey", ...]:
        return (self.column,)

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "TimeTruncKey":
        m = _ChildMapper(fn)
        column = m(self.column)
        return self.model_copy(update={"column": column}) if m.changed else self


def column_leaf(col: Union["ColumnKey", "ColumnSqlKey"]) -> str:
    """Leaf column name of a ``TimeTruncKey.column`` regardless of kind."""
    return getattr(col, "leaf", None) or getattr(col, "column_name")


def column_path(col: Union["ColumnKey", "ColumnSqlKey"]) -> Tuple[str, ...]:
    """Join path of a ``TimeTruncKey.column`` regardless of kind."""
    return col.path


class StarKey(_LeafKey):
    """Sentinel source for ``*:count`` aggregations.

    ``path`` is empty for the local star and non-empty for a cross-model star
    (``customers.*:count`` → ``path=("customers",)``), mirroring ``ColumnKey.path``.
    """

    path: Tuple[str, ...] = ()

    @property
    def phase(self) -> Phase:
        return Phase.ROW


class LiteralKey(_LeafKey):
    """Identity for a literal value inside an expression tree.

    Scalar normalization happens at the call site via ``normalize_scalar`` so
    equality is type-stable (``LiteralKey(Decimal(1))`` and ``LiteralKey(True)``
    are distinct). Phase ROW.
    """

    value: Union[Decimal, str, bool, None] = None

    @property
    def phase(self) -> Phase:
        return Phase.ROW

    def __hash__(self) -> int:
        return hash(("LiteralKey", _typed_leaf(self.value)))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, LiteralKey):
            return NotImplemented
        return _typed_leaf(self.value) == _typed_leaf(other.value)


class SqlExprKey(_LeafKey):
    """Identity for a Mode-A SQL fragment.

    Used as ``AggregateKey.column_filter_key`` so an attached ``Column.filter``
    joins the aggregate's structural identity. ``canonical_sql`` is
    sqlglot-normalized by the binder. ``referenced_join_paths`` is the set of
    non-anchor join-path prefixes the filter touches (``()`` for same-model);
    the before-validator sorts/dedups it so order doesn't affect identity.
    """

    canonical_sql: str
    referenced_join_paths: Tuple[Tuple[str, ...], ...] = ()

    @field_validator("referenced_join_paths", mode="before")
    @classmethod
    def _canonicalize_referenced_join_paths(cls, v):
        if not v:
            return ()
        return tuple(sorted({tuple(p) for p in v}))

    @property
    def phase(self) -> Phase:
        return Phase.ROW

    def __hash__(self) -> int:
        return hash(("SqlExprKey", self.canonical_sql, self.referenced_join_paths))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SqlExprKey):
            return NotImplemented
        return (
            self.canonical_sql == other.canonical_sql
            and self.referenced_join_paths == other.referenced_join_paths
        )


# ---------------------------------------------------------------------------
# Aggregate / Transform / Arithmetic / ScalarCall
# ---------------------------------------------------------------------------


# DEV-1826: beyond column / star sources, an aggregate may take a row-level
# same-model EXPRESSION source (``sum(amount - cost)``) — the bound tree reuses
# the existing row-level composites, so hash/equality/serialization come from
# the canonical tree and formatting variants intern to one key.
_AggregateSource = Union[
    ColumnKey, ColumnSqlKey, StarKey,
    "ArithmeticKey", "ScalarCallKey", "LiteralKey",
]
# Positional and kwarg arg values share one union: both `last(created_at)` and
# `weighted_avg(weight=qty)` bind identifier columns via `_bind_agg_arg`.
_AggregateArgValue = Union[ColumnKey, ColumnSqlKey, Decimal, str, bool, None]
_AggregateKwargValue = _AggregateArgValue


def _sort_kwargs_tuple(v):
    """Validator helper: canonicalize a kwargs tuple to sorted order by key."""
    if v is None:
        return ()
    return tuple(sorted(v, key=lambda kv: kv[0]))


class AggregateKey(_FrozenKey):
    """Identity for an aggregation slot (P3).

    Local and cross-model aggregates share this shape: ``source.path`` empty for
    local, non-empty for joined. ``args``/``kwargs`` carry parameters (numeric
    scalars pre-normalized to Decimal; identifier kwargs arrive as
    ``ColumnKey``/``ColumnSqlKey``; kwargs canonicalized to sorted order).
    ``column_filter_key`` folds any attached ``Column.filter`` into identity.

    ``grain`` (DEV-1747 D2) names where a cross-model aggregate is evaluated:
    ``"target"`` (default) rooted at the target, one value per target row-group;
    ``"host"`` rooted at the host, one value per host group (needed by the
    DEV-1735 order wrap). It participates in identity — the two are different
    values (global vs per-group).
    """

    source: _AggregateSource
    agg: str
    args: Tuple[_AggregateArgValue, ...] = ()
    kwargs: Tuple[Tuple[str, _AggregateKwargValue], ...] = ()
    column_filter_key: Optional[SqlExprKey] = None
    grain: Literal["target", "host"] = "target"
    partition_keys: Optional[frozenset["ValueKey"]] = None

    @field_validator("kwargs", mode="before")
    @classmethod
    def _canonicalize_kwargs(cls, v):
        return _sort_kwargs_tuple(v)

    @property
    def phase(self) -> Phase:
        return Phase.AGGREGATE

    def children(self) -> Tuple["ValueKey", ...]:
        # column_filter_key is Mode-A opaque — never a child (A1).
        embedded = [
            c
            for c in (self.source, *self.args, *(v for _, v in self.kwargs))
            if isinstance(c, _FrozenKey)
        ]
        if self.partition_keys is not None:
            embedded.extend(self.partition_keys)
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
                else frozenset(m(p) for p in self.partition_keys)
            ),
        }
        return self.model_copy(update=update) if m.changed else self

    def __hash__(self) -> int:
        return hash((
            "AggregateKey",
            self.source,
            self.agg,
            _typed_args(self.args),
            _typed_kwargs(self.kwargs),
            self.column_filter_key,
            self.grain,
            self.partition_keys,
        ))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AggregateKey):
            return NotImplemented
        return (
            self.source == other.source
            and self.agg == other.agg
            and _typed_args(self.args) == _typed_args(other.args)
            and _typed_kwargs(self.kwargs) == _typed_kwargs(other.kwargs)
            and self.column_filter_key == other.column_filter_key
            and self.grain == other.grain
            and self.partition_keys == other.partition_keys
        )


# Rerooting is type-preserving — a ColumnKey in, a ColumnKey out.
_RerootableT = TypeVar("_RerootableT")


def _map_path_ref(ref, *, map_path):
    """Apply ``map_path`` to a single embedded reference's join ``path``.

    Per-leaf half of the path-map visitor: ``ColumnKey``/``ColumnSqlKey``/
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
    """Re-anchor a cross-model ``AggregateKey`` into its target's local scope.

    A thin alias for :func:`reroot_value_key`. ``column_filter_key`` rides
    through unchanged (its paths are anchored at the source column's owning model).
    """
    return reroot_value_key(key, target_path=target_path)


class TransformKey(_FrozenKey):
    """Identity for a transform slot (window / temporal operator over a value).

    ``input`` is the operated-on value. ``partition_keys`` is order-independent;
    ``time_key`` is the sort dimension for time-ordered transforms.
    """

    op: str
    input: "ValueKey"
    args: Tuple[Scalar, ...] = ()
    kwargs: Tuple[Tuple[str, Scalar], ...] = ()
    partition_keys: frozenset["ValueKey"] = frozenset()
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
            "partition_keys": frozenset(m(p) for p in self.partition_keys),
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


class ArithmeticKey(_FrozenKey):
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


_ScalarCallArg = Union["ValueKey", Decimal, str, bool, None]


def _arg_phase(arg) -> Optional[Phase]:
    """Return ``arg.phase`` for ValueKey args, ``None`` for pure scalars."""
    return getattr(arg, "phase", None)


class ScalarCallKey(_FrozenKey):
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


class BetweenKey(_FrozenKey):
    """Typed identity for a ``col BETWEEN low AND high`` predicate.

    The planner uses this to mark where ``BETWEEN`` is the right legacy-parity
    rendering (today only ``TimeDimension.date_range``). User DSL filters never
    produce it — ``col >= a and col <= b`` stays ``ArithmeticKey``. Phase ROW;
    the renderer emits ``exp.Between``.
    """

    column: "ValueKey"
    low: "ValueKey"
    high: "ValueKey"

    @property
    def phase(self) -> Phase:
        return Phase.ROW

    def children(self) -> Tuple["ValueKey", ...]:
        return (self.column, self.low, self.high)

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "BetweenKey":
        m = _ChildMapper(fn)
        update = {
            "column": m(self.column), "low": m(self.low), "high": m(self.high),
        }
        return self.model_copy(update=update) if m.changed else self


class InKey(_FrozenKey):
    """Typed identity for a ``col IN (lit, …)`` / ``NOT IN`` predicate.

    Modelled on ``BetweenKey``: a column LHS and a fixed tuple of ``LiteralKey``
    RHS operands (LiteralKey so equality is type-stable). ``negated`` flips IN vs
    NOT IN. Phase ROW; the renderer emits ``exp.In`` (wrapped in ``exp.Not``).
    """

    column: "ValueKey"
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
        return Phase.ROW

    def children(self) -> Tuple["ValueKey", ...]:
        return (self.column, *self.values)

    def map_children(
        self, fn: Callable[["ValueKey"], "ValueKey"],
    ) -> "InKey":
        m = _ChildMapper(fn)
        update = {
            "column": m(self.column),
            "values": tuple(m(v) for v in self.values),
        }
        return self.model_copy(update=update) if m.changed else self


ValueKey = Union[
    ColumnKey,
    ColumnSqlKey,
    TimeTruncKey,
    StarKey,
    LiteralKey,
    AggregateKey,
    TransformKey,
    ArithmeticKey,
    ScalarCallKey,
    BetweenKey,
    InKey,
]


# Resolve the recursive forward references on the keys that take ValueKey.
TransformKey.model_rebuild()
ArithmeticKey.model_rebuild()
ScalarCallKey.model_rebuild()
BetweenKey.model_rebuild()
InKey.model_rebuild()
TimeTruncKey.model_rebuild()
# AggregateKey.source forward-references the expression composites (DEV-1826).
AggregateKey.model_rebuild()


VALUE_KEY_TYPES: Tuple[type, ...] = get_args(ValueKey)


class KindPolicy(BaseModel):
    """Consumer-named per-kind policy flags; membership is a conscious
    classification asserted by tests, not derived from structure."""

    model_config = ConfigDict(frozen=True)

    slottable: bool = False
    slot_composite: bool = False
    materialised_order: bool = False


KIND_POLICY: dict[type, KindPolicy] = {
    ColumnKey: KindPolicy(slottable=True),
    ColumnSqlKey: KindPolicy(slottable=True),
    TimeTruncKey: KindPolicy(slottable=True),
    StarKey: KindPolicy(),
    LiteralKey: KindPolicy(),
    AggregateKey: KindPolicy(slottable=True, materialised_order=True),
    TransformKey: KindPolicy(slottable=True, materialised_order=True),
    ArithmeticKey: KindPolicy(slot_composite=True, materialised_order=True),
    ScalarCallKey: KindPolicy(slot_composite=True, materialised_order=True),
    BetweenKey: KindPolicy(),
    InKey: KindPolicy(),
}


def _map_sql_expr_key(key: SqlExprKey, *, map_path) -> SqlExprKey:
    """Apply ``map_path`` to a standalone fragment's referenced paths.

    Reconstructed (not ``model_copy``d) so the validator re-sorts/dedups; a path
    mapped to ``()`` is dropped.
    """
    mapped = [np for p in key.referenced_join_paths if (np := tuple(map_path(p)))]
    return SqlExprKey(
        canonical_sql=key.canonical_sql,
        referenced_join_paths=mapped,
    )


def _map_value_key(key: _RerootableT, *, map_path) -> _RerootableT:
    """Rewrite every embedded join ``path`` in ``key`` through ``map_path``.

    Total & fail-closed behind :func:`reroot_value_key` / :func:`prepend_value_key`:
    path-carrying leaves map here, every other kind routes through
    ``map_children`` (a protocol-less kind raises). ``AggregateKey.column_filter_key``
    is copied unchanged (owner-anchored), while a standalone ``SqlExprKey`` is
    root-anchored and does map.
    """
    # Scalars ride through untouched (ScalarCallKey args, AggregateKey kwargs).
    if key is None or isinstance(key, (Decimal, str, bool, int, float)):
        return key
    if isinstance(key, (ColumnKey, ColumnSqlKey, StarKey)):
        return cast(_RerootableT, _map_path_ref(key, map_path=map_path))
    if isinstance(key, SqlExprKey):
        return cast(_RerootableT, _map_sql_expr_key(key, map_path=map_path))
    if not isinstance(key, _FrozenKey):
        raise TypeError(
            f"the value-key path visitor has no case for {type(key).__name__!r}: "
            f"only value keys and scalars are mappable."
        )
    return cast(
        _RerootableT,
        key.map_children(lambda c: _map_value_key(c, map_path=map_path)),
    )


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
    """Prefix every embedded join ``path`` with ``host_path`` (PREPEND direction),
    re-anchoring a target-local bound tree into the host's coordinate system.

    Inverse of ``reroot_value_key(key, target_path=host_path)``. ``host_path == ()``
    is the identity; ``AggregateKey.column_filter_key`` stays owner-anchored.
    """
    host_path = tuple(host_path)
    if not host_path:
        return key

    def _prepend(path: Tuple[str, ...]) -> Tuple[str, ...]:
        return host_path + tuple(path)

    return _map_value_key(key, map_path=_prepend)


def substitute_value_keys(
    key: _RerootableT, mapping: Mapping["ValueKey", "ValueKey"],
) -> _RerootableT:
    """Replace whole sub-keys named in ``mapping`` by identity, structurally.

    Pre-order match-before-recurse: a key equal to a ``mapping`` entry is
    replaced atomically (children never traversed, replacements never
    re-substituted); everything else routes through ``map_children`` (a
    protocol-less kind raises). ``AggregateKey.column_filter_key`` is NOT
    traversed (a Mode-A ``SqlExprKey``); ``TimeTruncKey.column`` IS.
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
        return cast(_RerootableT, mapping[key])
    return cast(
        _RerootableT,
        key.map_children(lambda c: substitute_value_keys(c, mapping)),
    )


# Conditional branch typing (DEV-1740) — Postgres CASE semantics.
_NUMERIC_TYPES = frozenset({DataType.INT, DataType.DOUBLE})


def join_conditional_branch_types(
    a: Optional[DataType], b: Optional[DataType],
) -> Optional[DataType]:
    """Result type of a conditional whose branches are ``a`` / ``b``.

    ``None`` marks a NULL-literal branch, absorbed by the other. Identical types
    pass through; a numeric mix widens to ``DOUBLE``; any other mix is a plan-time
    error (matching what Postgres rejects).
    """
    if a is None:
        return b
    if b is None:
        return a
    if a == b:
        return a
    if a in _NUMERIC_TYPES and b in _NUMERIC_TYPES:
        return DataType.DOUBLE
    raise ValueError(
        f"CASE/iif branches have incompatible types {a.value} and {b.value}: "
        f"branches must share a type (numeric types widen to DOUBLE). Cast one "
        f"branch so both match."
    )


def conditional_number_format(
    a: Optional[NumberFormat], b: Optional[NumberFormat],
) -> Optional[NumberFormat]:
    """A conditional carries a number format only when both branches agree."""
    return a if (a is not None and a == b) else None
