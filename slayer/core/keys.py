"""Stage 1 (DEV-1450) — typed identity primitives for the new resolution
pipeline.

Identity is structural (P2 of the DEV-1450 spec). Two expression occurrences
with the same key intern to the same slot — whether the occurrence is a
declared measure, an inner reference inside a transform, or a filter
predicate.

Rendering state (SQL text, public alias, projection position, hidden-ness)
does not live here. Those decisions belong to the planner and the SQL
generator. The keys carry only what's needed to decide "are these the same
slot?".

Public types: ``ValueKey`` (Union alias), ``Phase`` (IntEnum), ``ColumnKey``,
``ColumnSqlKey``, ``StarKey``, ``SqlExprKey``, ``AggregateKey``,
``TransformKey``, ``ArithmeticKey``, ``ScalarCallKey``. Helpers:
``normalize_scalar``, ``SCALAR_FUNCTIONS``.

These types are dormant in stage 1 — no engine code routes through them.
Stages 7a and 7b wire them up.
"""

from __future__ import annotations

from decimal import Decimal
from enum import IntEnum
from typing import Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, field_validator


# ---------------------------------------------------------------------------
# Closed scalar-function allowlist (C12).
# ---------------------------------------------------------------------------

# Anything outside this set in Mode B raises ``UnknownFunctionError`` at
# binding time. Lives here (not in formula.py) so the keys module is the
# single source of truth for what counts as a structurally-keyed scalar
# call. The binder (stage 7a) imports from here.
SCALAR_FUNCTIONS: frozenset[str] = frozenset({
    # Null handling
    "nullif", "coalesce", "ifnull",
    # Math
    "ln", "log10", "log2", "log", "exp", "sqrt", "pow", "power",
    "abs", "floor", "ceil", "round",
    # String hygiene (was DEV-1378's STRING_HYGIENE_OPS)
    "lower", "upper", "trim", "replace", "substr", "instr", "length", "concat",
})


# ---------------------------------------------------------------------------
# Phase
# ---------------------------------------------------------------------------


class Phase(IntEnum):
    """Resolution phase of a ValueKey (P8).

    Filters and arithmetic compose by taking the maximum phase of their
    operands; the filter's phase then routes it to WHERE (ROW), HAVING
    (AGGREGATE), or post-filter on the outer SELECT (POST).
    """

    ROW = 0
    AGGREGATE = 1
    POST = 2


# ---------------------------------------------------------------------------
# Scalar
# ---------------------------------------------------------------------------

Scalar = Union[Decimal, str, bool, None]


def normalize_scalar(value):
    """Canonicalize a raw scalar before keying.

    - Booleans pass through unchanged (checked BEFORE int because bool
      is-a int in Python).
    - ``None`` passes through unchanged.
    - ``Decimal`` passes through unchanged.
    - ``int`` becomes ``Decimal(value)``.
    - ``float`` becomes ``Decimal(str(value))`` — via ``str`` so floats
      land on their displayed decimal form, not their binary
      approximation (``Decimal(0.5)`` differs from ``Decimal("0.5")``).
    - ``str`` passes through unchanged.

    Raises ``TypeError`` for anything else (lists, dicts, custom objects).
    Caller-side conversion of identifiers to ``ColumnKey`` happens in the
    binder; this helper does not touch ColumnKey.
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


# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class _FrozenKey(BaseModel):
    """Common config for the typed-key family: frozen (hashable, immutable)."""

    model_config = ConfigDict(frozen=True)


def _typed_leaf(v):
    """Return a hash- and equality-friendly representation of a scalar
    leaf that does NOT conflate numerically-equal values of different
    types.

    Python collapses ``True == 1 == Decimal("1")`` (and the same for
    ``False`` / ``0``), so a key built from ``args=(True,)`` would
    intern with one built from ``args=(Decimal("1"),)`` if the
    container's hash/eq blindly delegate to tuple-of-bare-values.
    Wrapping the leaf in a ``(type_tag, value)`` pair at hash/eq time
    restores the type distinction without changing the stored
    representation users see via ``key.args[0]``.

    ``ValueKey`` leaves (ColumnKey, AggregateKey, ...) are themselves
    frozen Pydantic models with value-based equality — they ride in the
    generic ``("__key__", v)`` slot. Every branch returns a uniform
    ``(tag, value)`` pair so callers never have to special-case the
    container shape.
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


# ---------------------------------------------------------------------------
# Row-phase keys
# ---------------------------------------------------------------------------


class ColumnKey(_FrozenKey):
    """Row-level reference to a base column on a model.

    ``path`` is the join walk from the query's source model to the
    terminal model — empty for local refs, non-empty for joined refs
    (``("customers",)``, ``("customers", "regions")``, …). ``leaf`` is
    the column name on the terminal model.

    Local and cross-model references share this shape (P3) — the only
    difference is whether ``path`` is empty. The planner uses
    ``path == ()`` to decide whether to materialize the value in the
    base CTE or in a cross-model sub-query.
    """

    path: Tuple[str, ...] = ()
    leaf: str

    @property
    def phase(self) -> Phase:
        return Phase.ROW


class ColumnSqlKey(_FrozenKey):
    """Reference to a derived column (one whose ``Column.sql`` is set).

    The expansion AST is recovered from the model definition at binding
    time — the key only carries identity. Two references to the same
    derived column on the same model intern to one slot.

    ``path`` is the join walk from the query's source model to the
    model that owns the derived column — empty for local references,
    non-empty for joined ones (``("customers",)``,
    ``("customers", "regions")``, …). Cross-model planners use
    ``path`` the same way they use ``ColumnKey.path``.
    """

    path: Tuple[str, ...] = ()
    model: str
    column_name: str

    @property
    def phase(self) -> Phase:
        return Phase.ROW


class TimeTruncKey(_FrozenKey):
    """Row-level reference to a time-truncated column (DEV-1450 stage 7b.3).

    Identifies a time dimension by ``(column, granularity)``. The
    underlying column is recoverable via ``column`` so date-range filters
    can bind against the raw column independently of the truncation.

    Identity is structural: two ``TimeTruncKey``s with the same
    ``column`` and the same ``granularity`` intern to the same slot;
    different granularities on the same column are distinct slots. This
    lets the ``ValueRegistry`` keep month / day / raw uses of the same
    column as separate materialised values without special-casing.

    ``column`` is a ``ColumnKey`` (base temporal column) or a
    ``ColumnSqlKey`` (DEV-1450 follow-up #4a — a DERIVED temporal column
    whose ``Column.sql`` is set). The SQL generator applies the
    ``DATE_TRUNC`` over the bare identifier (``ColumnKey``) or over the
    expanded derived expression (``ColumnSqlKey``).

    ``granularity`` is the string value of a ``TimeGranularity`` member
    (``"day"`` / ``"month"`` / ...). Stored as ``str`` so the key stays
    a pure-data frozen Pydantic model without an enum import here.
    """

    column: Union["ColumnKey", "ColumnSqlKey"]
    granularity: str

    @property
    def phase(self) -> Phase:
        return Phase.ROW


def column_leaf(col: Union["ColumnKey", "ColumnSqlKey"]) -> str:
    """The leaf column name of a ``TimeTruncKey.column`` regardless of kind.

    ``ColumnKey`` carries ``leaf``; ``ColumnSqlKey`` carries
    ``column_name``. Using this helper everywhere a ``TimeTruncKey``'s
    column is unwrapped avoids ``leaf`` / ``column_name`` drift.
    """
    return getattr(col, "leaf", None) or getattr(col, "column_name")


def column_path(col: Union["ColumnKey", "ColumnSqlKey"]) -> Tuple[str, ...]:
    """The join path of a ``TimeTruncKey.column`` regardless of kind.

    Both ``ColumnKey`` and ``ColumnSqlKey`` carry ``.path``.
    """
    return col.path


class StarKey(_FrozenKey):
    """Sentinel source for ``*:count`` aggregations.

    ``path`` is empty for the local star (``*:count`` over the host) and
    non-empty for a cross-model star (``customers.*:count`` →
    ``path=("customers",)``), mirroring ``ColumnKey.path`` so the
    cross-model planner can route a star aggregate through the join graph
    (P3). Two stars with the same path intern; the default empty path
    keeps the local-star identity bit-identical to before.
    """

    path: Tuple[str, ...] = ()

    @property
    def phase(self) -> Phase:
        return Phase.ROW


class LiteralKey(_FrozenKey):
    """Identity for a literal value inside an expression tree.

    Used wherever an ``ArithmeticKey``, ``TransformKey``, or other
    composite key needs a literal operand (``revenue:sum + 1`` — the
    ``1`` is a ``LiteralKey``). Carries phase ROW so it doesn't
    artificially elevate the phase of expressions it appears in.

    Scalar normalization (int → Decimal, float → Decimal via str)
    happens at the call site via ``normalize_scalar`` so equality
    is type-stable (``LiteralKey(Decimal(1))`` and
    ``LiteralKey(True)`` are distinct).
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


class SqlExprKey(_FrozenKey):
    """Identity for a Mode-A SQL fragment.

    Currently used as ``AggregateKey.column_filter_key`` so a
    ``Column.filter`` wired in at aggregation time becomes part of the
    aggregate's structural identity. Two aggregates over the same column
    differ when their attached ``Column.filter`` differs; same-filter
    ones intern.

    ``canonical_sql`` is a sqlglot-normalized string (the binder is
    responsible for normalization — the key trusts the form it receives).
    """

    canonical_sql: str

    @property
    def phase(self) -> Phase:
        return Phase.ROW


# ---------------------------------------------------------------------------
# Aggregate / Transform / Arithmetic / ScalarCall
# ---------------------------------------------------------------------------


_AggregateSource = Union[ColumnKey, ColumnSqlKey, StarKey]
# Positional and keyword arg values accept the same union — both
# `last(created_at)` (positional ColumnKey time arg) and
# `weighted_avg(weight=qty)` (kwarg ColumnKey) bind to identifier columns
# via `_bind_agg_arg`. Reusing one alias for both keeps the surface tight.
_AggregateArgValue = Union[ColumnKey, ColumnSqlKey, Decimal, str, bool, None]
_AggregateKwargValue = _AggregateArgValue


def _sort_kwargs_tuple(v):
    """Validator helper: canonicalize a kwargs tuple to sorted order by key."""
    if v is None:
        return ()
    return tuple(sorted(v, key=lambda kv: kv[0]))


class AggregateKey(_FrozenKey):
    """Identity for an aggregation slot (P3).

    Local and cross-model aggregates share this shape: ``source.path``
    is empty for local, non-empty for joined. The render strategy
    (base CTE vs cross-model CTE) is decided downstream by the planner.

    ``args`` and ``kwargs`` carry the aggregation's parameters. Numeric
    scalars must already be normalized to ``Decimal`` (use
    ``normalize_scalar``). Identifier kwargs (``weighted_avg(weight=quantity)``)
    arrive as ``ColumnKey`` / ``ColumnSqlKey``. ``kwargs`` is canonicalized
    to sorted-by-key order by the validator so input order does not affect
    identity.

    ``column_filter_key`` is the ``Column.filter`` attached to the
    aggregated column, if any — pulled into the structural key so two
    aggregates with different attached filters do not collide.
    """

    source: _AggregateSource
    agg: str
    args: Tuple[_AggregateArgValue, ...] = ()
    kwargs: Tuple[Tuple[str, _AggregateKwargValue], ...] = ()
    column_filter_key: Optional[SqlExprKey] = None

    @field_validator("kwargs", mode="before")
    @classmethod
    def _canonicalize_kwargs(cls, v):
        return _sort_kwargs_tuple(v)

    @property
    def phase(self) -> Phase:
        return Phase.AGGREGATE

    def __hash__(self) -> int:
        return hash((
            "AggregateKey",
            self.source,
            self.agg,
            _typed_args(self.args),
            _typed_kwargs(self.kwargs),
            self.column_filter_key,
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
        )


class TransformKey(_FrozenKey):
    """Identity for a transform slot (window / temporal operator over a value).

    The ``input`` is the value the transform operates on — typically an
    aggregate or another transform, occasionally a row-level column.

    ``partition_keys`` is a frozenset (order-independent); ``time_key`` is
    addressed separately as the sort dimension for time-ordered transforms.
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

    ``op`` is the operator symbol (``+``, ``-``, ``*``, ``/``, ``<``,
    ``<=``, ``and``, ``or``, …). Operand order matters — subtraction
    and division are non-commutative, comparisons have a fixed LHS/RHS,
    and even commutative ops keep their textual order for deterministic
    SQL emission.

    Phase is the maximum of operand phases (P8).
    """

    op: str
    operands: Tuple["ValueKey", ...]

    @property
    def phase(self) -> Phase:
        return max((o.phase for o in self.operands), default=Phase.ROW)


_ScalarCallArg = Union["ValueKey", Decimal, str, bool, None]


def _arg_phase(arg) -> Optional[Phase]:
    """Return ``arg.phase`` for ValueKey args, ``None`` for pure scalars."""
    return getattr(arg, "phase", None)


class ScalarCallKey(_FrozenKey):
    """Identity for a closed-allowlist scalar function call (C12).

    ``name`` must be a member of ``SCALAR_FUNCTIONS``. The key constructor
    does NOT validate this — the binder rejects unknown names with
    ``UnknownFunctionError``. Keeping validation out of the key keeps
    identity construction cheap on the hot path.

    Phase is the maximum of arg phases over the args that carry a phase
    (i.e., ``ValueKey``s); pure-scalar args contribute the ROW floor.
    """

    name: str
    args: Tuple[_ScalarCallArg, ...] = ()

    @property
    def phase(self) -> Phase:
        phases = [p for a in self.args if (p := _arg_phase(a)) is not None]
        return max(phases) if phases else Phase.ROW

    def __hash__(self) -> int:
        return hash(("ScalarCallKey", self.name, _typed_args(self.args)))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ScalarCallKey):
            return NotImplemented
        return (
            self.name == other.name
            and _typed_args(self.args) == _typed_args(other.args)
        )


# ---------------------------------------------------------------------------
# BetweenKey — DEV-1450 stage 7b.9
# ---------------------------------------------------------------------------


class BetweenKey(_FrozenKey):
    """Typed identity for a ``col BETWEEN low AND high`` predicate.

    Closed-form Mode-A SQL constructs (``BETWEEN``) and equivalent
    Mode-B compound forms (``col >= low and col <= high``) render to
    different SQL text. The planner uses ``BetweenKey`` to mark the
    spots where ``BETWEEN`` is the right legacy-parity rendering — today
    only ``TimeDimension.date_range`` produces them. User-written DSL
    filters never produce ``BetweenKey``: the syntax parser doesn't
    have a ``between`` construct, and a user-written ``col >= a and
    col <= b`` stays as ``ArithmeticKey(and, [GE, LE])`` so its parity
    with the legacy generator (which keeps the AND form verbatim) is
    preserved.

    Phase is always ROW — ``BetweenKey`` predicates filter row-level
    columns. The renderer emits ``exp.Between``.
    """

    column: "ValueKey"
    low: "ValueKey"
    high: "ValueKey"

    @property
    def phase(self) -> Phase:
        return Phase.ROW


# ---------------------------------------------------------------------------
# Union alias + rebuild for forward refs
# ---------------------------------------------------------------------------


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
]


# Resolve the recursive forward references on the keys that take ValueKey.
TransformKey.model_rebuild()
ArithmeticKey.model_rebuild()
ScalarCallKey.model_rebuild()
BetweenKey.model_rebuild()
# TimeTruncKey.column is a Union[ColumnKey, ColumnSqlKey] (DEV-1450 #4a).
TimeTruncKey.model_rebuild()
