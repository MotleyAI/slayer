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
from typing import Literal, Optional, Tuple, TypeVar, Union, cast

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
    "abs", "floor", "ceil", "ceiling", "round", "sign", "trunc", "mod",
    # Scalar min/max over the arguments (NOT the min:/max: aggregations).
    "greatest", "least",
    # String hygiene (was DEV-1378's STRING_HYGIENE_OPS)
    "lower", "upper", "trim", "ltrim", "rtrim",
    "replace", "substr", "substring", "instr", "length", "concat",
    # Pattern match — ``like(value, pattern)`` emits the SQL ``LIKE`` operator
    # (sqlglot ``exp.Like``); see SQLGenerator scalar-call rendering.
    "like",
})


# Accepted argument counts per allowlisted scalar, as ``(min, max)``; ``max=None``
# means variadic. Validated at bind time so a malformed call is a clear SLayer
# error, and again at render time as the fail-closed backstop.
#
# Needed because sqlglot is inconsistent about arity: ``exp.func("ROUND", a, b, c)``
# SILENTLY DROPS the third argument, ``exp.func("LENGTH", a, b)`` emits invalid
# ``LENGTH(a, b)`` for the database to reject, and ``exp.func("LOWER", a, b)``
# raises a raw sqlglot ValueError. None of those is a good answer for a user
# who mistyped a filter.
SCALAR_FUNCTION_ARITY: dict[str, tuple[int, Optional[int]]] = {
    "nullif": (2, 2),
    "coalesce": (1, None),
    "ifnull": (2, 2),
    "ln": (1, 1), "log10": (1, 1), "log2": (1, 1), "log": (1, 2),
    "exp": (1, 1), "sqrt": (1, 1),
    "pow": (2, 2), "power": (2, 2),
    "abs": (1, 1), "floor": (1, 1), "ceil": (1, 1), "round": (1, 2),
    # ``ceiling`` is the T-SQL spelling of ``ceil`` and renders to the same
    # node. Pinned at 1: a 2-arg call silently emits ``CEIL(x, y)``, and a
    # 3-arg one becomes DuckDB's unrelated ``CEIL(x TO z)`` rounding form.
    "ceiling": (1, 1), "sign": (1, 1),
    # 1-arg only: 2-arg ``trunc(x, digits)`` SILENTLY drops the digits on
    # SQLite (emits ``TRUNC(x)``), a wrong answer rather than an error.
    "trunc": (1, 1),
    # ``mod`` is the ``%`` operator, admitted with the two-arg operator shape.
    "mod": (2, 2),
    # Variadic, min two: SQLite's one-arg ``MAX``/``MIN`` parses as the
    # AGGREGATE, not the scalar, and MySQL requires at least two arguments —
    # refuse a one-arg call here rather than let a backend reject it.
    "greatest": (2, None), "least": (2, None),
    "lower": (1, 1), "upper": (1, 1), "trim": (1, 1), "length": (1, 1),
    # The trims take the string only, matching ``trim``. The 2-arg
    # strip-these-characters form is deliberately NOT admitted (deferred to
    # DEV-1793): its second argument is a character SET on most backends but an
    # exact SUBSTRING on MySQL (``TRIM(LEADING remstr FROM str)``) — a silent
    # cross-dialect divergence needing its own ruling.
    "ltrim": (1, 1), "rtrim": (1, 1),
    "replace": (3, 3), "substr": (2, 3), "substring": (2, 3), "instr": (2, 2),
    "concat": (1, None),
    "like": (2, 2),
}

# Not a second allowlist: the table above must cover ``SCALAR_FUNCTIONS``
# exactly. Checked BOTH ways at import — a missing entry would let a wrong-arity
# call through to sqlglot's inconsistent handling, and an entry for a name that
# is not allowlisted would be dead weight that reads as though it were.
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

    DEV-1503 — ``referenced_join_paths`` is the typed SET (semantically;
    stored as an ordered tuple for hashability) of non-anchor join-path
    prefixes the filter touches after derived-ref expansion. Computed
    once at bind time via
    ``slayer.engine.column_filter_paths.compute_column_filter_join_paths``;
    the planner reads it to decide whether a filtered-local measure must
    isolate (the DEV-1503 trigger predicate). ``()`` for same-model
    filters; non-empty for cross-model column filters. The field
    participates in structural identity — two filters with the same
    canonical SQL but different referenced paths would be a bug, so
    folding it into the key catches that invariant violation by
    comparison.

    The ``before``-validator canonicalises the input to a sorted,
    de-duplicated tuple of tuples — so callers can pass any iterable
    (list, set, generator) and order doesn't affect identity (otherwise
    two semantically-equal SqlExprKeys built with paths in different
    order would intern as different keys; CodeRabbit nitpick).
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

    ``grain`` names WHERE the aggregate is evaluated when ``source.path`` is
    non-empty (DEV-1747 D2). ``"target"`` — the default and the meaning of
    every user-declared cross-model aggregate — evaluates it in a CTE rooted at
    the target, one value per target row-group. ``"host"`` evaluates it in a
    CTE rooted at the HOST with the path's joins pulled in, grouped on the
    query grain: one value per HOST group. The DEV-1735 order wrap needs the
    latter, because a target-rooted CTE for a host-grain sort key degenerates
    to a scalar CROSS JOIN and sorts every group by one constant.

    It participates in identity deliberately: a declared
    ``customers.regions.name:max`` and the synthetic host-grain wrap over the
    same column are different values (global vs per-group), so interning them
    onto one slot would silently give the user the wrong one.
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


#: Rerooting is type-preserving — a ``ColumnKey`` in, a ``ColumnKey`` out.
#: Expressing that keeps call sites precisely typed rather than collapsing
#: every rerooted key to the union.
_RerootableT = TypeVar("_RerootableT")


def _reroot_path_ref(ref, *, target_path: Tuple[str, ...]):
    """Re-anchor a single embedded reference from the host coordinate system
    into the target's local scope (DEV-1707).

    Prefix-strip with residual: if ``ref`` carries a join ``path`` that starts
    with ``target_path``, drop that prefix and keep the residual hops
    (``("customers", "regions")`` under target ``("customers",)`` →
    ``("regions",)``; an exact match → ``()``). A ``path`` that does NOT start
    with ``target_path`` — or a value with no ``path`` at all (a scalar
    ``Decimal`` / ``str`` / ``bool`` / ``None``) — is returned unchanged. The
    strip applies uniformly to ``ColumnKey``, ``ColumnSqlKey``, and
    ``StarKey``; the non-``path`` fields (``leaf`` / ``model`` /
    ``column_name``) ride along untouched via ``model_copy``.
    """
    path = getattr(ref, "path", None)
    if path is None:
        return ref
    path = tuple(path)
    if path[: len(target_path)] != tuple(target_path):
        return ref
    residual = path[len(target_path):]
    if residual == path:
        return ref
    return ref.model_copy(update={"path": residual})


def reroot_aggregate_key(
    key: "AggregateKey", *, target_path: Tuple[str, ...],
) -> "AggregateKey":
    """Re-anchor a cross-model ``AggregateKey`` into its target's local scope
    (DEV-1707 / DEV-1703 Stage 3).

    A thin alias for :func:`reroot_value_key`, which applies the same
    prefix-strip rule over the whole ``ValueKey`` union. Two implementations
    would be free to drift into two reroot semantics — the drift §5.4 removes.

    ``column_filter_key`` rides through unchanged: its paths are anchored at
    the OWNING model of the source column, and rerooting changes only how that
    owner is reached. After rerooting a filtered cross-model aggregate the
    source reads local while ``referenced_join_paths`` stays non-empty —
    exactly the filtered-local isolation trigger shape.
    """
    return reroot_value_key(key, target_path=target_path)


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
# InKey — DEV-1475
# ---------------------------------------------------------------------------


class InKey(_FrozenKey):
    """Typed identity for a ``col IN (lit, lit, …)`` / ``NOT IN`` predicate.

    Modelled on ``BetweenKey``: a closed-form SQL predicate with a column
    LHS and a fixed tuple of literal-valued RHS operands. Two ``InKey``s
    with the same column and the same set of values (in the same order)
    intern; ``negated`` flips IN vs NOT IN without doubling the class
    count.

    ``values`` is a tuple of ``LiteralKey`` (not bare scalars) so equality
    rides through ``LiteralKey``'s type-stable ``_typed_leaf`` machinery
    — ``InKey(values=(LiteralKey(True),))`` does not collide with
    ``InKey(values=(LiteralKey(Decimal(1)),))``.

    Phase is always ROW; the renderer emits ``exp.In`` (wrapped in
    ``exp.Not`` when ``negated``).
    """

    column: "ValueKey"
    values: Tuple[LiteralKey, ...]
    negated: bool = False

    @field_validator("values")
    @classmethod
    def _reject_empty_values(
        cls, v: Tuple[LiteralKey, ...],
    ) -> Tuple[LiteralKey, ...]:
        # Defense in depth (Codex review): the parser's ``ast.Compare``
        # branch already rejects empty RHS, but direct construction can
        # bypass it and reach the SQL generator, which would emit
        # ``col IN ()`` — invalid in every supported dialect.
        if not v:
            raise ValueError(
                "InKey requires a non-empty ``values`` tuple; ``col IN "
                "()`` is invalid SQL across every supported dialect.",
            )
        return v

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
    InKey,
]


# Resolve the recursive forward references on the keys that take ValueKey.
TransformKey.model_rebuild()
ArithmeticKey.model_rebuild()
ScalarCallKey.model_rebuild()
BetweenKey.model_rebuild()
InKey.model_rebuild()
# TimeTruncKey.column is a Union[ColumnKey, ColumnSqlKey] (DEV-1450 #4a).
TimeTruncKey.model_rebuild()


# ---------------------------------------------------------------------------
# The total reroot visitor
# ---------------------------------------------------------------------------


def _reroot_sql_expr_key(
    key: SqlExprKey, *, target_path: Tuple[str, ...],
) -> SqlExprKey:
    """Re-anchor a STANDALONE Mode-A fragment's referenced join paths.

    Only correct when the fragment is anchored at the QUERY ROOT. A fragment
    reached as ``AggregateKey.column_filter_key`` is anchored at the owning
    model instead and must NOT come through here — see the note in
    :func:`reroot_value_key`.
    """
    stripped = [
        path[len(target_path):]
        if tuple(path[: len(target_path)]) == target_path else path
        for path in key.referenced_join_paths
    ]
    # Constructed, NOT ``model_copy``: the ``before`` validator is what sorts
    # and de-duplicates ``referenced_join_paths``, and ``model_copy`` skips
    # validators in Pydantic v2. Stripping can produce both — two distinct
    # paths can share a residual, and the residuals need not stay in sorted
    # order — and ``__hash__`` / ``__eq__`` read the tuple directly, so two
    # semantically equal keys would fail to intern (CodeRabbit).
    #
    # An EXACT match strips to ``()``, which is not a join-path prefix at all
    # but the documented "same-model filter" marker, so it is dropped rather
    # than carried as an empty tuple.
    return SqlExprKey(
        canonical_sql=key.canonical_sql,
        referenced_join_paths=[p for p in stripped if p],
    )


def _reroot_partition_keys(partition_keys, *, target_path: Tuple[str, ...]):
    """Reroot an ``AggregateKey.partition_keys`` frozenset, preserving the
    ``None`` (absent) vs empty (grand total) distinction."""
    if partition_keys is None:
        return None
    return frozenset(
        reroot_value_key(p, target_path=target_path) for p in partition_keys
    )


def reroot_value_key(
    key: _RerootableT, *, target_path: Tuple[str, ...],
) -> _RerootableT:
    """Re-anchor every embedded reference in ``key`` from the query root into
    ``target_path``'s local scope.

    The generalisation of :func:`reroot_aggregate_key` over the whole
    ``ValueKey`` union (plus the standalone ``SqlExprKey``). Two properties
    make it safe to reroot a plan structurally instead of via formula text:

    **Total.** Every union member has an explicit case. A kind added to
    ``ValueKey`` later has none, so it lands in the fail-closed arm rather than
    riding through unrerooted.

    **Fail-closed.** An unhandled kind raises ``TypeError``. Returning it
    unchanged would be indistinguishable from "correctly identity", which is
    how a mis-anchored ref reaches the SQL generator looking well-formed.

    The rule is prefix-strip-with-residual, applied per position: a ``path``
    starting with ``target_path`` drops that prefix and keeps the residual
    hops; any other ``path``, and any scalar, is returned unchanged.
    ``target_path == ()`` is the identity — the empty prefix strips zero hops.

    ``AggregateKey.column_filter_key`` is deliberately copied UNCHANGED.
    ``binding._resolve_column_filter_key`` walks ``source.path`` first and only
    then stamps the anchor, so the fragment's paths are expressed relative to
    the model that OWNS the filtered column. Rerooting changes how that owner
    is reached from the query root; it never moves the owner, so those paths
    are invariant. A standalone ``SqlExprKey`` is anchored at the query root
    and therefore does strip — the asymmetry is per position, not per type.
    """
    target_path = tuple(target_path)
    if not target_path:
        return key

    def _recurse(value):
        return reroot_value_key(value, target_path=target_path)

    # Scalars ride through untouched — they appear as ScalarCallKey args and
    # as AggregateKey kwarg values.
    if key is None or isinstance(key, (Decimal, str, bool, int, float)):
        return key

    # --- leaves ---------------------------------------------------------
    if isinstance(key, (ColumnKey, ColumnSqlKey, StarKey)):
        # ``_reroot_path_ref`` also accepts bare scalars, so it cannot carry the
        # type-preserving annotation; the isinstance guard above establishes it.
        return cast(_RerootableT, _reroot_path_ref(key, target_path=target_path))
    if isinstance(key, LiteralKey):
        return key
    if isinstance(key, TimeTruncKey):
        # The path lives on the WRAPPED column, which is why walk_value_keys
        # needs a special case here; the visitor must not inherit that blind
        # spot.
        return key.model_copy(update={"column": _recurse(key.column)})
    if isinstance(key, SqlExprKey):
        return _reroot_sql_expr_key(key, target_path=target_path)

    # --- composites -----------------------------------------------------
    if isinstance(key, AggregateKey):
        return key.model_copy(update={
            "source": _recurse(key.source),
            "args": tuple(_recurse(a) for a in key.args),
            "kwargs": tuple((n, _recurse(v)) for n, v in key.kwargs),
            "partition_keys": _reroot_partition_keys(
                key.partition_keys, target_path=target_path,
            ),
        })
    if isinstance(key, TransformKey):
        # ``args`` / ``kwargs`` are Tuple[Scalar, ...] — type-prohibited from
        # holding a ValueKey, so there is nothing to traverse there.
        return key.model_copy(update={
            "input": _recurse(key.input),
            "partition_keys": frozenset(
                _recurse(p) for p in key.partition_keys
            ),
            "time_key": (
                None if key.time_key is None else _recurse(key.time_key)
            ),
        })
    if isinstance(key, ArithmeticKey):
        return key.model_copy(update={
            "operands": tuple(_recurse(o) for o in key.operands),
        })
    if isinstance(key, ScalarCallKey):
        return key.model_copy(update={
            "args": tuple(_recurse(a) for a in key.args),
        })
    if isinstance(key, BetweenKey):
        return key.model_copy(update={
            "column": _recurse(key.column),
            "low": _recurse(key.low),
            "high": _recurse(key.high),
        })
    if isinstance(key, InKey):
        return key.model_copy(update={
            "column": _recurse(key.column),
            "values": tuple(_recurse(v) for v in key.values),
        })

    raise TypeError(
        f"reroot_value_key has no case for {type(key).__name__!r}. The visitor "
        f"is total over ValueKey by design: add an explicit case rather than "
        f"letting an unrerooted key through, which the SQL generator cannot "
        f"distinguish from a correctly-local one."
    )
