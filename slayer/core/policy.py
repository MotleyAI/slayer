"""Session-policy data model for forced-filter RLS.

A ``SessionPolicy`` is immutable, agent-invisible engine state carrying exactly
one required ``ruleset``; the no-filtering case is ``policy=None`` at the
engine/client, so a bare ``SessionPolicy()`` raises. The rewrite it drives lives
in ``slayer/sql/session_policy.py``.
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
    """Freeze a list/tuple ``value`` into a tuple; a degenerate empty ``IN`` is rejected."""
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
    """Split a possibly qualified table name into whitespace-trimmed dotted parts."""
    return tuple(p.strip() for p in table.split("."))


def _table_names_match(a: str, b: str) -> bool:
    """Symmetric case-insensitive table match over the qualifier parts both names state,
    so a bare name matches any schema and two qualified names must agree on every part."""
    ap, bp = _table_parts(a), _table_parts(b)
    for pa, pb in zip(reversed(ap), reversed(bp)):
        if pa.casefold() != pb.casefold():
            return False
    return True


def _reaches_anchor(endpoint: str, anchor: str) -> bool:
    """Whether a join-path endpoint reaches ``anchor``, qualified at least as fully as it.

    The endpoint is emitted verbatim, so a bare endpoint reaching a qualified anchor
    would silently scope against the default-schema table instead.
    """
    return _table_names_match(endpoint, anchor) and len(
        _table_parts(endpoint)
    ) >= len(_table_parts(anchor))


class ColumnFilterRuleset(BaseModel):
    """Filter every physical table that has ``column``; a scalar emits ``=``, a tuple ``IN``.

    ``on_unapplicable`` governs a table that confirms it lacks the column. A table whose
    column presence cannot be confirmed always fails closed regardless.
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
    """One parsed join hop: ``from_table.from_column`` -> ``to_table.to_column``.

    Internal only — callers author hops as strings (see :func:`_parse_hop`).
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
    """Parse ``"from_table.from_column = to_table.to_column"`` into a :class:`JoinHop`.

    Each side splits on its last dot, so a column containing a dot is not expressible.
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
    """Assert ``hops`` is non-empty and each hop starts where the previous one ended.

    Runs on every ``parsed_hops`` access, so a ``model_copy`` bypassing validation
    still fails closed rather than reaching SQL generation.
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

    ``join_path`` holds hop strings (``"from_table.from_column = to_table.to_column"``,
    physical names) whose two endpoints are ``target_table`` and the anchor, in either
    written order. The tenant ``column``/``value`` live on the owning ruleset.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    target_table: str
    join_path: Tuple[str, ...]

    @property
    def parsed_hops(self) -> Tuple[JoinHop, ...]:
        """``join_path`` parsed and chain-validated, derived fresh so it can never go stale."""
        hops = tuple(_parse_hop(spec) for spec in self.join_path)
        _validate_hop_chain(hops=hops)
        return hops

    @property
    def _endpoints(self) -> Tuple[str, str]:
        hops = self.parsed_hops
        return (hops[0].from_table, hops[-1].to_table)

    def oriented_hops(self) -> Tuple[JoinHop, ...]:
        """``parsed_hops`` normalized target-first, reversing a path authored anchor-first.

        Lets the EXISTS builder always find the wrapped source at the start and the
        tenant column on the terminal hop.
        """
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
            # Would otherwise be iterated into a tuple of single characters.
            raise ValueError(
                "JoinFilterRule.join_path must be a list of hop strings, not a "
                "single string"
            )
        if isinstance(v, list):
            return tuple(v)
        return v

    @model_validator(mode="after")
    def _validate_endpoints(self):
        # The matching "non-target endpoint == anchor" check lives on the ruleset,
        # which is what knows the anchor.
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


def _validate_join_rule_anchor(
    rule: JoinFilterRule, anchor: str
) -> Tuple[JoinHop, ...]:
    """Validate one join rule against the ruleset ``anchor``, returning target-first hops.

    Single source of truth for the per-rule anchor invariants, shared by ruleset
    construction and the SQL boundary so both enforce the same set. Cross-rule checks
    (duplicate targets, whitelist overlaps) stay on the ruleset validator.
    """
    oriented = rule.oriented_hops()
    terminal = oriented[-1].to_table
    if not _reaches_anchor(terminal, anchor):
        raise ValueError(
            f"JoinFilterRule for target '{rule.target_table}': the join_path "
            f"must reach the anchor table '{anchor}' at its non-target endpoint, "
            f"qualified at least as fully as the anchor (got '{terminal}')."
        )
    # The anchor may not also appear as an intermediate hop.
    node_sequence = [oriented[0].from_table] + [h.to_table for h in oriented]
    if sum(1 for n in node_sequence if _table_names_match(n, anchor)) != 1:
        raise ValueError(
            f"JoinFilterRule for target '{rule.target_table}': the anchor table "
            f"'{anchor}' must appear exactly once in the join path, only as the "
            "terminal endpoint (not as an intermediate hop)."
        )
    if _table_names_match(rule.target_table, anchor):
        raise ValueError(
            f"JoinFilterRule may not target the anchor table '{anchor}' "
            "(the anchor is filtered directly)."
        )
    return oriented


class JoinFilterRuleset(BaseModel):
    """Single-anchor join model: the tenant identifier lives on one ``table`` + ``column``.

    The anchor is filtered directly, each :class:`JoinFilterRule` target is scoped via a
    correlated ``EXISTS`` to it, ``whitelist`` tables are emitted unfiltered, and any
    other physical table fails closed. Fully structural — no column introspection.
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
            _validate_join_rule_anchor(rule, master)
            if any(_table_names_match(rule.target_table, t) for t in seen_targets):
                raise ValueError(
                    f"Duplicate JoinFilterRule target '{rule.target_table}' "
                    "(one path per target)."
                )
            seen_targets.append(rule.target_table)
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


# Keyed on the explicit ``kind`` field — no inference, so a kind-less dict fails
# to discriminate.
FilterRuleset = Annotated[
    Union[ColumnFilterRuleset, JoinFilterRuleset], Field(discriminator="kind")
]


class SessionPolicy(BaseModel):
    """Immutable, engine-global forced-filter configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Pinned so an unknown schema version fails closed rather than being silently
    # interpreted by the v1 rewrite path.
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
