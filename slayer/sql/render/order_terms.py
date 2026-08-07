"""The one ORDER BY term resolver (P-D / P-G).

Four render sites used to turn an ``OrderEntry`` into a sort term, each with
its own idea of what a slot id resolves to. They disagreed on three things:

* **What happens when the slot cannot be resolved.** The transform-chain path
  raised; the other three returned ``None`` or ``continue``d, which drops the
  sort term and returns *unsorted rows* under a wiring bug that produces no
  error anywhere.
* **Null ordering.** Only the base path went through the dialect's
  ``build_ordered``, so the T-SQL pin that suppresses sqlglot's mis-resolving
  ``CASE WHEN … IS NULL`` emulation was missing on the combined and chain
  paths.
* **How the reference is qualified.** A five-way precedence chain over four
  alias maps decided between a bare alias, a ``_base.``-qualified one, a
  CTE-qualified one, and an inline expression — and a projected cross-model
  aggregate came out named by its CTE column while the SELECT projected it
  under the user's alias, so the term resolved only by falling through to an
  input column of the FROM.

The producing scope is not something a renderer should re-derive: the planner
already knows it and names it on the entry (``OrderScope``). So the resolution
is a dict lookup keyed by that scope, with no precedence and no fallback —
each render site fills the scopes it can produce, and a slot that is missing
from its own scope's environment is an error rather than a silent drop.
"""

from __future__ import annotations

from typing import Dict, Optional

from pydantic import BaseModel, ConfigDict, Field
from sqlglot import exp

from slayer.engine.planned import OrderEntry, OrderScope
from slayer.sql.dialects.base import SqlDialect

__all__ = [
    "HOST_BASE_SCOPES",
    "OrderEnv",
    "OrderSlotNotMaterialisedError",
    "resolve_order_term",
]

#: The scopes whose value is a column of the host ``_base`` SELECT. A render
#: site that names ``_base`` columns one way and CTE columns another asks this
#: rather than testing the two members by hand, so adding a third host-base
#: scope cannot leave one site behind.
HOST_BASE_SCOPES = frozenset(
    {OrderScope.HOST_BASE, OrderScope.HOST_BASE_HIDDEN},
)


class OrderSlotNotMaterialisedError(RuntimeError):
    """An ORDER BY entry names a slot its producing scope never materialised.

    Always an internal wiring bug — the plan validated the order term long
    before rendering — so it names the slot and what the scope did carry,
    which is the difference between a five-minute fix and a silent
    wrong-results report.
    """


class OrderEnv(BaseModel):
    """Where each order slot's value can be NAMED, per producing scope.

    One mapping per :class:`OrderScope`, so a render site declares only the
    scopes it actually produces and cannot accidentally satisfy a lookup from
    a neighbouring scope's aliases. Values are sqlglot expressions rather than
    alias strings because an order-only outer composite has no alias at all —
    it renders inline.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    host_base: Dict[str, exp.Expression] = Field(default_factory=dict)
    host_base_hidden: Dict[str, exp.Expression] = Field(default_factory=dict)
    cross_model_cte: Dict[str, exp.Expression] = Field(default_factory=dict)
    windowed_cte: Dict[str, exp.Expression] = Field(default_factory=dict)
    transform_step: Dict[str, exp.Expression] = Field(default_factory=dict)
    outer_composite: Dict[str, exp.Expression] = Field(default_factory=dict)
    #: Owns the null-ordering spelling (P-H). Defaults to the portable base.
    dialect: Optional[SqlDialect] = None

    @classmethod
    def uniform(
        cls,
        refs: Dict[str, exp.Expression],
        *,
        dialect: Optional[SqlDialect] = None,
    ) -> "OrderEnv":
        """An environment where every scope names a value the same way.

        The transform chain's outer wrap is that case: whatever produced a
        value, by the time the chain is wrapped it is one column of the
        wrapped subquery, reachable under one alias. The producing scope is
        spent, so refusing to answer for it would be a lie about the SQL.
        """
        return cls(**{scope.value: dict(refs) for scope in OrderScope},
                   dialect=dialect)


_MISSING_ARMS = sorted(
    scope.value for scope in OrderScope if scope.value not in OrderEnv.model_fields
)
if _MISSING_ARMS:  # pragma: no cover - import-time structural guard
    raise RuntimeError(
        f"OrderEnv has no environment for OrderScope {_MISSING_ARMS} — a scope "
        f"added without its arm would make resolve_order_term raise on every "
        f"query that uses it.",
    )

_DEFAULT_DIALECT = SqlDialect()


def resolve_order_term(*, entry: OrderEntry, env: OrderEnv) -> exp.Ordered:
    """One ``OrderEntry`` → one ``ORDER BY`` term.

    Raises :class:`OrderSlotNotMaterialisedError` when the entry's scope did
    not materialise the slot. Returning an unsorted result instead is the one
    outcome a caller can neither detect nor recover from.
    """
    refs: Dict[str, exp.Expression] = getattr(env, entry.scope.value)
    ref = refs.get(entry.slot_id)
    if ref is None:
        raise OrderSlotNotMaterialisedError(
            f"ORDER BY references slot id={entry.slot_id!r}, which the "
            f"{entry.scope.value} scope did not materialise "
            f"(it carries {sorted(refs)}).",
        )
    dialect = env.dialect or _DEFAULT_DIALECT
    return dialect.build_ordered(
        ref.copy(), descending=entry.direction == "desc", nulls=entry.nulls,
    )
