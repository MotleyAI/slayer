"""Session-policy data model for forced-filter RLS (DEV-1578).

A ``SessionPolicy`` is immutable, agent-invisible engine state. It is set only
at engine/client init and silently scopes every query by wrapping each
physical-table reference in a filtered subquery (see
``slayer/sql/session_policy.py``).

The minimalist v1 slice supports exactly one rule kind: ``ColumnFilterRule``
("every table that has column C is filtered to ``C = value`` / ``C IN (...)``").
The ``kind`` discriminator and the reserved (un-enforced) fields are present
now so future rule kinds (e.g. join-path filters) slot in without a schema
break.
"""

from __future__ import annotations

from typing import Literal, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict, field_validator

# A scalar value implies ``=``; a non-empty list/tuple implies ``IN (...)``.
PolicyScalar = Union[str, int, float, bool]
OnUnapplicable = Literal["block", "pass"]


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
    def _non_blank_column(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("ColumnFilterRule.column must be a non-empty string")
        return v

    @field_validator("value", mode="before")
    @classmethod
    def _coerce_value(cls, v):
        # A list/tuple becomes a tuple (immutable) and must be non-empty so
        # the ``IN`` predicate is never degenerate. Scalars pass through.
        if isinstance(v, (list, tuple)):
            if len(v) == 0:
                raise ValueError(
                    "ColumnFilterRule.value list/tuple must be non-empty"
                )
            return tuple(v)
        return v


class SessionPolicy(BaseModel):
    """Immutable, engine-global forced-filter configuration.

    ``data_filters`` is a tuple (not a list) so the policy is genuinely
    immutable after init — contents cannot be appended or replaced. The
    reserved fields are accepted shapes for future rule kinds but are not
    enforced in v1.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    version: int = 1
    data_filters: Tuple[ColumnFilterRule, ...] = ()

    @field_validator("data_filters", mode="before")
    @classmethod
    def _coerce_filters(cls, v):
        if isinstance(v, list):
            return tuple(v)
        return v


# Reserved for future rule kinds (not enforced in v1): join_filters,
# model_access, model_writes. Intentionally omitted from the model until a
# concrete shape is implemented, since ``extra="forbid"`` would otherwise
# need them present. Documented here so the intent is discoverable.
__all__ = [
    "PolicyScalar",
    "OnUnapplicable",
    "ColumnFilterRule",
    "SessionPolicy",
]
