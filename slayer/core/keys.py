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
    """

    model: str
    column_name: str

    @property
    def phase(self) -> Phase:
        return Phase.ROW


class StarKey(_FrozenKey):
    """Sentinel source for ``*:count`` aggregations.

    All instances compare equal — there is no source column to
    distinguish. Used as ``AggregateKey.source`` for the star form.
    """

    @property
    def phase(self) -> Phase:
        return Phase.ROW


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
_AggregateKwargValue = Union[ColumnKey, ColumnSqlKey, Decimal, str, bool, None]


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
    args: Tuple[Scalar, ...] = ()
    kwargs: Tuple[Tuple[str, _AggregateKwargValue], ...] = ()
    column_filter_key: Optional[SqlExprKey] = None

    @field_validator("kwargs", mode="before")
    @classmethod
    def _canonicalize_kwargs(cls, v):
        return _sort_kwargs_tuple(v)

    @property
    def phase(self) -> Phase:
        return Phase.AGGREGATE


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


# ---------------------------------------------------------------------------
# Union alias + rebuild for forward refs
# ---------------------------------------------------------------------------


ValueKey = Union[
    ColumnKey,
    ColumnSqlKey,
    AggregateKey,
    TransformKey,
    ArithmeticKey,
    ScalarCallKey,
]


# Resolve the recursive forward references on the keys that take ValueKey.
TransformKey.model_rebuild()
ArithmeticKey.model_rebuild()
ScalarCallKey.model_rebuild()
