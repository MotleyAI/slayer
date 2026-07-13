"""Session-policy data model for forced-filter RLS (DEV-1578 / DEV-1627).

A ``SessionPolicy`` is immutable, agent-invisible engine state. It is set only
at engine/client init and silently scopes every query by wrapping each
physical-table reference in a filtered subquery (see
``slayer/sql/session_policy.py``).

Two rule kinds exist, selected by the ``kind`` discriminator:

* ``ColumnFilterRule`` (DEV-1578) — "every physical table that has column ``C``
  is filtered to ``C = value`` / ``C IN (...)``".
* ``JoinFilterRule`` (DEV-1627) — for a table that does **not** carry the tenant
  column, reach the column via an explicit, policy-authored join path and scope
  the table with a correlated ``EXISTS`` semi-join.

Under the override model, a table targeted by any ``JoinFilterRule`` is scoped
**only** by its join rule(s); column rules do not touch it. A policy that
contains any ``JoinFilterRule`` must also contain at least one
``ColumnFilterRule`` with ``on_unapplicable="block"`` — the mandatory backstop
that keeps every untargeted table either filtered or fail-closed.
"""

from __future__ import annotations

from typing import Annotated, Literal, Optional, Tuple, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

# A scalar value implies ``=``; a non-empty list/tuple implies ``IN (...)``.
PolicyScalar = Union[str, int, float, bool]
OnUnapplicable = Literal["block", "pass"]


def _coerce_policy_value(v):
    """Coerce a rule ``value``: list/tuple -> tuple (immutable) and rejected
    when empty (a degenerate ``IN`` is never allowed); scalars pass through."""
    if isinstance(v, (list, tuple)):
        if len(v) == 0:
            raise ValueError("policy rule value list/tuple must be non-empty")
        return tuple(v)
    return v


def _require_non_blank(v, info: ValidationInfo):
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"{info.field_name} must be a non-empty string")
    return v


class ColumnFilterRule(BaseModel):
    """Force every physical table that has ``column`` to be filtered.

    ``value`` shape selects the operator: a scalar emits ``column = value``;
    a non-empty list/tuple emits ``column IN (...)`` (an empty collection is
    rejected at validation). ``on_unapplicable`` governs a table that
    **confirms it lacks** ``column``: ``"block"`` (default) fails the whole
    query; ``"pass"`` leaves that table unfiltered for this rule. A table
    whose column presence cannot be confirmed always fails closed,
    regardless of ``on_unapplicable``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["column"] = "column"
    name: Optional[str] = None  # diagnostics / error text only
    column: str
    value: Union[PolicyScalar, Tuple[PolicyScalar, ...]]
    on_unapplicable: OnUnapplicable = "block"

    @field_validator("column")
    @classmethod
    def _non_blank_column(cls, v: str, info: ValidationInfo) -> str:
        return _require_non_blank(v, info)

    @field_validator("value", mode="before")
    @classmethod
    def _coerce_value(cls, v):
        return _coerce_policy_value(v)


class JoinHop(BaseModel):
    """One physical-name join hop: ``from_table.from_column`` ->
    ``to_table.to_column``. Table fields may be schema/catalog-qualified
    (``public.orders``). All four fields must be non-blank.

    Internal only: callers author hops as strings (see
    :func:`_parse_hop`); this is the parsed runtime representation and is not
    part of the public API (excluded from ``__all__``, never serialized).
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    from_table: str
    from_column: str
    to_table: str
    to_column: str

    @field_validator("from_table", "from_column", "to_table", "to_column")
    @classmethod
    def _non_blank(cls, v: str, info: ValidationInfo) -> str:
        return _require_non_blank(v, info)


def _parse_hop(spec: str) -> JoinHop:
    """Parse a hop string ``"from_table.from_column = to_table.to_column"`` into
    an internal :class:`JoinHop`.

    Naive split: exactly one ``=``; each side is split on its **last** dot, so
    the prefix is the (optionally schema/catalog-qualified) table kept verbatim
    and the suffix is the column. Whitespace-tolerant. A column literally
    containing a dot is not expressible (out of scope). Blank parts are rejected
    by :class:`JoinHop`'s own validators. Raises ``ValueError`` on a malformed
    spec (surfaces as a Pydantic ``ValidationError`` from the ``after``
    validator that calls it).
    """
    if not isinstance(spec, str):
        raise ValueError(
            f"join_path hop must be a string, got {type(spec).__name__}"
        )
    sides = spec.split("=")
    if len(sides) != 2:
        raise ValueError(
            f"join_path hop {spec!r} must be "
            "'from_table.from_column = to_table.to_column' (exactly one '=')"
        )

    def _split(side: str) -> tuple[str, str]:
        table, dot, column = side.strip().rpartition(".")
        if not dot:
            raise ValueError(
                f"join_path hop side {side.strip()!r} must be 'table.column' "
                f"(in hop {spec!r})"
            )
        return table.strip(), column.strip()

    from_table, from_column = _split(sides[0])
    to_table, to_column = _split(sides[1])
    return JoinHop(
        from_table=from_table,
        from_column=from_column,
        to_table=to_table,
        to_column=to_column,
    )


def _validate_hop_chain(*, target_table: str, hops: Tuple["JoinHop", ...]) -> None:
    """Assert the parsed ``hops`` form a valid chain: non-empty, the first hop
    starts at ``target_table``, and each hop starts where the previous ended.

    Physical table names compare case-insensitively — unquoted SQL identifiers
    are case-insensitive on every supported backend, and the SQL-layer target
    match is case-insensitive too. Raises ``ValueError`` on any violation.
    Runs both at construction and on every ``parsed_hops`` access, so a rule
    reconstructed via ``model_copy(update=...)`` (which bypasses Pydantic
    validation) can never feed a non-chaining path to SQL generation — it fails
    closed instead.
    """
    if not hops:
        raise ValueError("JoinFilterRule.join_path must be non-empty")
    if hops[0].from_table.casefold() != target_table.casefold():
        raise ValueError(
            "JoinFilterRule.join_path[0].from_table "
            f"({hops[0].from_table!r}) must equal target_table "
            f"({target_table!r})"
        )
    for prev, cur in zip(hops, hops[1:]):
        if cur.from_table.casefold() != prev.to_table.casefold():
            raise ValueError(
                "JoinFilterRule.join_path hops must chain: hop from_table "
                f"{cur.from_table!r} must equal the previous hop's to_table "
                f"{prev.to_table!r}"
            )


class JoinFilterRule(BaseModel):
    """Scope ``target_table`` via an explicit join path to the tenant column.

    ``join_path`` is a non-empty tuple of hop **strings** of the form
    ``"from_table.from_column = to_table.to_column"`` (physical DB names,
    tables optionally schema/catalog-qualified). The first hop starts at
    ``target_table`` and each subsequent hop starts where the previous one
    ended. ``column`` is the tenant column on the **last** hop's ``to_table``;
    ``value`` selects ``=`` (scalar) vs ``IN`` (non-empty list/tuple). The
    rewrite emits a correlated ``EXISTS`` semi-join (cardinality-safe).

    Hops are parsed into internal :class:`JoinHop`s via :attr:`parsed_hops`
    (derived fresh from ``join_path`` on each access — no cache); the public
    ``join_path`` stays a tuple of the original strings and serializes
    symmetrically. There is no ``on_unapplicable``: under the override model the
    rule always applies to its named table, and a bad path fails closed at SQL
    execution.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["join"] = "join"
    name: Optional[str] = None  # diagnostics / error text only
    target_table: str
    join_path: Tuple[str, ...]
    column: str
    value: Union[PolicyScalar, Tuple[PolicyScalar, ...]]

    @property
    def parsed_hops(self) -> Tuple[JoinHop, ...]:
        """The ``join_path`` strings parsed into internal :class:`JoinHop`s and
        chain-validated, derived fresh on each access (never stored/serialized,
        so a ``model_copy`` that swaps ``join_path``/``target_table`` can never
        go stale and can never feed a non-chaining path to SQL generation — it
        re-parses and re-validates, failing closed on a broken copy)."""
        hops = tuple(_parse_hop(spec) for spec in self.join_path)
        _validate_hop_chain(target_table=self.target_table, hops=hops)
        return hops

    @field_validator("target_table", "column")
    @classmethod
    def _non_blank(cls, v: str, info: ValidationInfo) -> str:
        return _require_non_blank(v, info)

    @field_validator("join_path", mode="before")
    @classmethod
    def _coerce_path(cls, v):
        if isinstance(v, str):
            # A bare string would otherwise be iterated into a tuple of single
            # characters — reject it; join_path is a list of hop strings.
            raise ValueError(
                "JoinFilterRule.join_path must be a list of hop strings, not a "
                "single string"
            )
        if isinstance(v, list):
            return tuple(v)
        return v

    @field_validator("value", mode="before")
    @classmethod
    def _coerce_value(cls, v):
        return _coerce_policy_value(v)

    @model_validator(mode="after")
    def _validate_chain(self):
        # Parse the hop strings and validate the chain once at construction
        # (fail fast on malformed / non-chaining input). ``parsed_hops`` does
        # both, so the same guard also protects the SQL-generation path.
        _ = self.parsed_hops
        return self


# The discriminated union over rule kinds. The ``kind`` field is the
# discriminator; ``_coerce_filters`` infers it on kind-less dicts so both the
# join-dict shape and the DEV-1578 kind-less column-dict shape keep working.
DataFilterRule = Annotated[
    Union[ColumnFilterRule, JoinFilterRule], Field(discriminator="kind")
]


def _infer_kind(item):
    """Add a ``kind`` discriminator to a kind-less dict rule: join fields
    present -> ``"join"``, else ``"column"``. Non-dicts / dicts already
    carrying ``kind`` pass through untouched."""
    if isinstance(item, dict) and "kind" not in item:
        if "join_path" in item or "target_table" in item:
            return {**item, "kind": "join"}
        return {**item, "kind": "column"}
    return item


class SessionPolicy(BaseModel):
    """Immutable, engine-global forced-filter configuration.

    ``data_filters`` is a tuple (not a list) so the policy is genuinely
    immutable after init — contents cannot be appended or replaced.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Only the v1 schema is understood. An unknown version must fail closed
    # (raise) rather than be silently interpreted by the v1 rewrite path,
    # since this object defines tenant-scoping behaviour.
    version: Literal[1] = 1
    data_filters: Tuple[DataFilterRule, ...] = ()

    @field_validator("data_filters", mode="before")
    @classmethod
    def _coerce_filters(cls, v):
        if isinstance(v, (list, tuple)):
            return tuple(_infer_kind(item) for item in v)
        return v

    @model_validator(mode="after")
    def _require_block_backstop(self):
        # Under the override model a join-targeted table is emitted
        # unfiltered only when no rule produces a predicate for it. Requiring
        # at least one ``block`` column rule alongside any join rule makes
        # every untargeted table either filtered (has the column) or
        # fail-closed (lacks it) — no silent leak (DEV-1627, decision 5).
        has_join = any(isinstance(r, JoinFilterRule) for r in self.data_filters)
        if not has_join:
            return self
        has_block = any(
            isinstance(r, ColumnFilterRule) and r.on_unapplicable == "block"
            for r in self.data_filters
        )
        if not has_block:
            raise ValueError(
                "A SessionPolicy containing a JoinFilterRule must also contain "
                "at least one ColumnFilterRule with on_unapplicable='block' "
                "(mandatory backstop): every table not covered by a join rule "
                "must fail closed if it lacks the tenant column."
            )
        return self


__all__ = [
    "PolicyScalar",
    "OnUnapplicable",
    "ColumnFilterRule",
    "JoinFilterRule",
    "DataFilterRule",
    "SessionPolicy",
]
