"""Session-policy data model for forced-filter RLS (DEV-1578 / DEV-1627 / DEV-1718).

A ``SessionPolicy`` is immutable, agent-invisible engine state. It is set only
at engine/client init and silently scopes every query by wrapping each
physical-table reference in a filtered subquery (see
``slayer/sql/session_policy.py``).

A policy carries exactly one **required** ``ruleset``, one of two kinds:

* ``ColumnFilterRuleset`` (DEV-1578) — "every physical table that has column
  ``C`` is filtered to ``C = value`` / ``C IN (...)``". ``on_unapplicable``
  governs a table confirmed to lack the column.
* ``JoinFilterRuleset`` (DEV-1718) — a single-anchor model. One ``table`` +
  ``column`` + ``value`` holds the tenant identifier; the anchor table is
  filtered directly, other tables reach the identifier via an explicit
  ``JoinFilterRule`` (correlated ``EXISTS`` semi-join), and a ``whitelist``
  lists tables emitted unfiltered. Any physical table that is not the anchor,
  not a join target, and not whitelisted fails closed.

There is no "empty" ruleset: the no-filtering case is ``policy=None`` at the
engine/client. ``ruleset`` being required means a bare ``SessionPolicy()``
raises rather than silently constructing a no-op.
"""

from __future__ import annotations

from typing import Annotated, Literal, Tuple, Union

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


def _table_parts(table: str) -> Tuple[str, ...]:
    """Split a (possibly schema/catalog-qualified) physical table name into its
    dotted parts, whitespace-trimmed."""
    return tuple(p.strip() for p in table.split("."))


def _table_names_match(a: str, b: str) -> bool:
    """Case-insensitive bare/qualified match between two physical table names,
    for the policy-internal validators (endpoint / anchor / whitelist checks).

    Compares the two names right-to-left over the number of parts they share,
    so a bare name matches any schema/catalog and two qualified names must
    agree on every stated qualifier. Symmetric — either side may be the bare
    one (``customers`` matches ``public.customers`` and vice versa).
    """
    ap, bp = _table_parts(a), _table_parts(b)
    for pa, pb in zip(reversed(ap), reversed(bp)):
        if pa.casefold() != pb.casefold():
            return False
    return True


def _reaches_anchor(endpoint: str, anchor: str) -> bool:
    """Whether a join-path ``endpoint`` reaches the configured ``anchor`` AND is
    qualified **at least as fully** as it.

    The endpoint is emitted verbatim into the enforcement ``EXISTS``, so a
    *qualified* anchor (``public.customers``) reached through a *bare* endpoint
    (``customers``) would silently scope against the default-schema table — the
    exact qualifier the author wrote on the anchor would be dropped. Requiring
    the endpoint to carry every part the anchor states closes that footgun (a
    bare anchor still matches a qualified endpoint — over-qualifying is safe).
    """
    return _table_names_match(endpoint, anchor) and len(
        _table_parts(endpoint)
    ) >= len(_table_parts(anchor))


class ColumnFilterRuleset(BaseModel):
    """Force every physical table that has ``column`` to be filtered.

    ``value`` shape selects the operator: a scalar emits ``column = value``;
    a non-empty list/tuple emits ``column IN (...)`` (an empty collection is
    rejected at validation). ``on_unapplicable`` governs a table that
    **confirms it lacks** ``column``: ``"block"`` (default) fails the whole
    query; ``"pass"`` leaves that table unfiltered. A table whose column
    presence cannot be confirmed always fails closed, regardless of
    ``on_unapplicable``.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["column"] = "column"
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

    Internal only: callers author hops as strings (see :func:`_parse_hop`);
    this is the parsed runtime representation and is not part of the public
    API (excluded from ``__all__``, never serialized).
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


def _validate_hop_chain(*, hops: Tuple["JoinHop", ...]) -> None:
    """Assert the parsed ``hops`` form an internally-consistent chain: non-empty
    and each hop starts where the previous ended.

    Physical table names compare case-insensitively — unquoted SQL identifiers
    are case-insensitive on every supported backend. The endpoint checks (which
    end is the target / master) live on the rule and ruleset validators, since
    either endpoint may be the master (DEV-1718). Raises ``ValueError`` on any
    violation. Runs both at construction and on every ``parsed_hops`` access, so
    a rule reconstructed via ``model_copy(update=...)`` (which bypasses Pydantic
    validation) can never feed a non-chaining path to SQL generation — it fails
    closed instead.
    """
    if not hops:
        raise ValueError("JoinFilterRule.join_path must be non-empty")
    for prev, cur in zip(hops, hops[1:]):
        if cur.from_table.casefold() != prev.to_table.casefold():
            raise ValueError(
                "JoinFilterRule.join_path hops must chain: hop from_table "
                f"{cur.from_table!r} must equal the previous hop's to_table "
                f"{prev.to_table!r}"
            )


def _reverse_hop(hop: JoinHop) -> JoinHop:
    """Swap a hop's endpoints (used to normalize a master-first path)."""
    return JoinHop(
        from_table=hop.to_table,
        from_column=hop.to_column,
        to_table=hop.from_table,
        to_column=hop.from_column,
    )


class JoinFilterRule(BaseModel):
    """Scope ``target_table`` via an explicit join path to the ruleset's anchor.

    ``join_path`` is a non-empty tuple of hop **strings** of the form
    ``"from_table.from_column = to_table.to_column"`` (physical DB names,
    tables optionally schema/catalog-qualified). The path's two endpoints are
    ``target_table`` and the ruleset's anchor ``table``, in **either** written
    order (target-first or master-first). ``column``/``value`` are not on the
    rule — they live on the owning :class:`JoinFilterRuleset`.

    Hops are parsed into internal :class:`JoinHop`s via :attr:`parsed_hops`
    (derived fresh from ``join_path`` on each access — no cache); the public
    ``join_path`` stays a tuple of the original strings and serializes
    symmetrically. A bad path fails closed at SQL execution.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    target_table: str
    join_path: Tuple[str, ...]

    @property
    def parsed_hops(self) -> Tuple[JoinHop, ...]:
        """The ``join_path`` strings parsed into internal :class:`JoinHop`s and
        chain-validated, derived fresh on each access (never stored/serialized,
        so a ``model_copy`` that swaps ``join_path``/``target_table`` can never
        go stale — it re-parses and re-validates, failing closed on a broken
        copy)."""
        hops = tuple(_parse_hop(spec) for spec in self.join_path)
        _validate_hop_chain(hops=hops)
        return hops

    @property
    def _endpoints(self) -> Tuple[str, str]:
        hops = self.parsed_hops
        return (hops[0].from_table, hops[-1].to_table)

    def oriented_hops(self) -> Tuple[JoinHop, ...]:
        """``parsed_hops`` guaranteed **target-first** (first hop's from_table
        matches ``target_table``), reversing the chain (list reversed + each
        hop's endpoints swapped) when the path was authored master-first. So
        the correlated-``EXISTS`` builder always finds the wrapped source table
        at the start and the tenant column on the terminal ``to_table``. Raises
        ``ValueError`` if ``target_table`` is not an endpoint (defensive — the
        construction validator already enforces it)."""
        hops = self.parsed_hops
        start, end = hops[0].from_table, hops[-1].to_table
        if _table_names_match(start, self.target_table):
            return hops
        if _table_names_match(end, self.target_table):
            return tuple(_reverse_hop(h) for h in reversed(hops))
        raise ValueError(
            f"JoinFilterRule.target_table ({self.target_table!r}) is not an "
            f"endpoint of join_path (endpoints {start!r}, {end!r})"
        )

    @field_validator("target_table")
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

    @model_validator(mode="after")
    def _validate_endpoints(self):
        # Parse + chain-validate the hops, then require target_table to be one
        # of the path's two endpoints (either order). The "non-target endpoint
        # == anchor" check lives on the ruleset (which knows the anchor).
        hops = self.parsed_hops
        start, end = hops[0].from_table, hops[-1].to_table
        if not (
            _table_names_match(start, self.target_table)
            or _table_names_match(end, self.target_table)
        ):
            raise ValueError(
                "JoinFilterRule.target_table "
                f"({self.target_table!r}) must be one of the join_path "
                f"endpoints ({start!r}, {end!r})"
            )
        return self


class JoinFilterRuleset(BaseModel):
    """Single-anchor join model (DEV-1718).

    The tenant identifier lives on ONE ``table`` + ``column`` (= ``value``).
    The anchor table is filtered directly; each :class:`JoinFilterRule` target
    is scoped via a correlated ``EXISTS`` semi-join to the anchor; ``whitelist``
    tables are emitted unfiltered; any other physical table fails closed. Fully
    structural — no column introspection.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["join"] = "join"
    table: str
    column: str
    value: Union[PolicyScalar, Tuple[PolicyScalar, ...]]
    joins: Tuple[JoinFilterRule, ...] = ()
    whitelist: Tuple[str, ...] = ()

    @field_validator("table", "column")
    @classmethod
    def _non_blank(cls, v: str, info: ValidationInfo) -> str:
        return _require_non_blank(v, info)

    @field_validator("value", mode="before")
    @classmethod
    def _coerce_value(cls, v):
        return _coerce_policy_value(v)

    @field_validator("joins", "whitelist", mode="before")
    @classmethod
    def _coerce_tuple(cls, v):
        return tuple(v) if isinstance(v, list) else v

    @field_validator("whitelist")
    @classmethod
    def _non_blank_whitelist(cls, v):
        for entry in v:
            if not isinstance(entry, str) or not entry.strip():
                raise ValueError("whitelist entries must be non-empty strings")
        return v

    @model_validator(mode="after")
    def _validate_anchor_reachability(self):
        master = self.table
        seen_targets: list[str] = []
        for rule in self.joins:
            oriented = rule.oriented_hops()  # target-first
            # The non-target endpoint (terminal to_table) must be the anchor,
            # qualified at least as fully (so a qualified anchor is never
            # reached via a bare, wrong-schema endpoint).
            terminal = oriented[-1].to_table
            if not _reaches_anchor(terminal, master):
                raise ValueError(
                    f"JoinFilterRule for target '{rule.target_table}': the "
                    f"join_path must reach the anchor table '{master}' at its "
                    f"non-target endpoint, qualified at least as fully as the "
                    f"anchor (got '{terminal}')."
                )
            # The anchor must appear EXACTLY ONCE in the oriented path, only as
            # the terminal to_table — never as an intermediate hop (Codex #3).
            node_sequence = [oriented[0].from_table] + [h.to_table for h in oriented]
            master_hits = sum(1 for n in node_sequence if _table_names_match(n, master))
            if master_hits != 1:
                raise ValueError(
                    f"JoinFilterRule for target '{rule.target_table}': the "
                    f"anchor table '{master}' must appear exactly once in the "
                    "join path, only as the terminal endpoint (not as an "
                    "intermediate hop)."
                )
            # A join rule may not target the anchor (it is filtered directly).
            if _table_names_match(rule.target_table, master):
                raise ValueError(
                    f"JoinFilterRule may not target the anchor table "
                    f"'{master}' (the anchor is filtered directly)."
                )
            # No two join rules may target the same table (one path per target).
            if any(_table_names_match(rule.target_table, t) for t in seen_targets):
                raise ValueError(
                    f"Duplicate JoinFilterRule target '{rule.target_table}' "
                    "(one path per target)."
                )
            seen_targets.append(rule.target_table)
        # Whitelist may not overlap the anchor or any join target.
        for entry in self.whitelist:
            if _table_names_match(entry, master):
                raise ValueError(
                    f"whitelist entry '{entry}' is the anchor table; the anchor "
                    "is filtered, not passed through."
                )
            if any(_table_names_match(entry, t) for t in seen_targets):
                raise ValueError(
                    f"whitelist entry '{entry}' is also a join target; a table "
                    "cannot be both whitelisted and join-scoped."
                )
        return self


# The discriminated union over ruleset kinds — keyed on the explicit ``kind``
# field. There is no inference: a kind-less dict fails to discriminate.
FilterRuleset = Annotated[
    Union[ColumnFilterRuleset, JoinFilterRuleset], Field(discriminator="kind")
]


class SessionPolicy(BaseModel):
    """Immutable, engine-global forced-filter configuration.

    Carries a single **required** ``ruleset`` — the no-filtering case is
    ``policy=None`` at the engine/client, so a bare ``SessionPolicy()`` raises
    rather than constructing a silent no-op.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Only the v1 schema is understood. An unknown version must fail closed
    # (raise) rather than be silently interpreted by the v1 rewrite path,
    # since this object defines tenant-scoping behaviour.
    version: Literal[1] = 1
    ruleset: FilterRuleset


__all__ = [
    "PolicyScalar",
    "OnUnapplicable",
    "ColumnFilterRuleset",
    "JoinFilterRule",
    "JoinFilterRuleset",
    "FilterRuleset",
    "SessionPolicy",
]
