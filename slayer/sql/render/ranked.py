"""The one ranked (``first`` / ``last``) CTE shape (P-C / P-G).

A ranked aggregate is "the value from the row that sorts first (or last) within
each grain". That is two SELECTs: an inner one that ranks the rows it is allowed
to see, and an outer one that picks rank 1 per group. This module owns that
shape, so every route into it — host-rooted, target-rooted, and the collapsed
sub-plan a re-rooted cross-model CTE emits — produces the same SQL.

Two choices are load-bearing and neither is arbitrary:

**The outer SELECT aggregates; it does not filter.** ``MAX(CASE WHEN rn = 1 THEN
v END) … GROUP BY grain`` rather than ``SELECT v … WHERE rn = 1``. The two agree
on every non-empty grain and disagree on the empty one: over a source with no
rows the aggregate form returns ONE row holding NULL and the filter form returns
none. An empty grain is joined back with a CROSS JOIN, so a zero-row CTE erases
the entire result rather than yielding a NULL measure — and returning one NULL
row is what ``amount:sum`` does over the same empty source.

**The inner SELECT projects a NAMED list, never ``source.*``.** Re-exporting the
source's columns is what made the superseded ranked wrap need a bolted-on
materialiser for crossing values, and it let a physical column named like an
internal rank column capture its reference. A projection boundary (P-B) makes
both unrepresentable: nothing crosses it that the scope did not choose to
publish.

The internal names below are private to one CTE scope. They never appear in a
result key and never collide across CTEs, because each ranked CTE is its own
SELECT.
"""

from __future__ import annotations

from typing import List, Sequence, Tuple

from pydantic import BaseModel, ConfigDict
from sqlglot import exp

__all__ = [
    "RANKED_CTE_PREFIX",
    "RANKED_SOURCE_ALIAS",
    "RANK_COLUMN",
    "RankedGrainProjection",
    "build_rank_column",
    "build_ranked_cte_select",
    "build_ranked_pick",
    "ranked_ordered",
]

#: CTE-name prefix, alongside ``_cm_`` (cross-model) and ``_wm_`` (windowed).
RANKED_CTE_PREFIX = "_rk_"
#: Alias of the inner ranked subquery.
RANKED_SOURCE_ALIAS = "_rk_src"
#: The ``ROW_NUMBER`` column the outer SELECT picks rank 1 from.
RANK_COLUMN = "_rk_rn"


class RankedGrainProjection(BaseModel):
    """One grain member as the ranked CTE sees it.

    ``output_alias`` is the column the CTE PUBLISHES — the same alias the host
    ``_base`` projects that member under, which is what lets the join-back
    compare the two by name. ``inner_ref`` is how the same value is named inside
    the ranked scope, which for a materialised crossing expression is an alias
    rather than the expression itself.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    output_alias: str
    inner_ref: exp.Expression


def build_rank_column(
    *,
    partition_by: Sequence[exp.Expression],
    ranking_time: exp.Ordered,
) -> exp.Expression:
    """``ROW_NUMBER() OVER (PARTITION BY <grain> ORDER BY <time>) AS _rk_rn``.

    ``ranking_time`` arrives already ordered because the DIRECTION is the whole
    difference between ``first`` and ``last``, and the null-ordering policy for a
    window's internal frame is the dialect's own rather than SLayer's
    nulls-last (an emulated ``CASE WHEN … IS NULL`` term inside the frame would
    change which row ranks first).
    """
    window = exp.Window(
        this=exp.RowNumber(),
        partition_by=[p.copy() for p in partition_by],
        order=exp.Order(expressions=[ranking_time.copy()]),
    )
    return window.as_(RANK_COLUMN)


def build_ranked_pick(*, value_ref: exp.Expression) -> exp.Expression:
    """``MAX(CASE WHEN _rk_rn = 1 THEN <value> END)`` — un-cast, un-aliased.

    The declared-type CAST is applied by the caller, OUTSIDE this expression.
    Casting the input instead is a different expression with different overflow
    and rounding behaviour, and the two are indistinguishable to any check that
    searches the SQL for a type name.
    """
    return exp.Max(this=exp.Case(ifs=[exp.If(
        this=exp.EQ(
            this=exp.column(RANK_COLUMN),
            expression=exp.Literal.number("1"),
        ),
        true=value_ref.copy(),
    )]))


def build_ranked_cte_select(
    *,
    inner: exp.Select,
    grain: Sequence[RankedGrainProjection],
    pick: exp.Expression,
    agg_alias: str,
    source_alias: str = RANKED_SOURCE_ALIAS,
) -> Tuple[exp.Select, List[str]]:
    """Wrap ``inner`` as the ranked subquery and pick rank 1 per grain.

    Returns ``(select, grain_output_aliases)``; the aliases are the join-back
    handle, in the same order the caller supplied the grain, so the ``ON``
    clause and the ``GROUP BY`` cannot disagree about what the grain is.

    An EMPTY grain emits no ``GROUP BY`` — the aggregate is scalar over the
    whole ranked set and returns exactly one row, which is the property the
    CROSS JOIN back depends on.
    """
    subquery = exp.Subquery(
        this=inner, alias=exp.TableAlias(this=exp.to_identifier(source_alias)),
    )
    select = exp.Select()
    for member in grain:
        select = select.select(
            member.inner_ref.copy().as_(member.output_alias, quoted=True),
        )
    select = select.select(pick.copy().as_(agg_alias, quoted=True))
    select = select.from_(subquery)
    for member in grain:
        select = select.group_by(member.inner_ref.copy())
    return select, [m.output_alias for m in grain]


def ranked_ordered(
    *, ranking_time: exp.Expression, agg: str, native_nulls_first: bool,
) -> exp.Ordered:
    """The window's ``ORDER BY`` term: ``first`` ranks ascending, ``last``
    descending. The direction is derived from the aggregation rather than
    carried beside it, because it is not a separate decision.
    """
    args: dict = {"this": ranking_time.copy(), "nulls_first": native_nulls_first}
    if agg == "last":
        # ``desc=False`` would emit an explicit ``ASC``; leaving the key off
        # emits the bare column, which is what ascending means.
        args["desc"] = True
    return exp.Ordered(**args)
