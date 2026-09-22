"""``build_ranked_cte_select`` must order the grain deterministically (CR review,
DEV-1832): ``Grain`` is frozenset-backed, so the SELECT / GROUP BY / returned-alias
order — and thus the emitted SQL — must not depend on the caller's iteration order.
"""
from __future__ import annotations

from sqlglot import exp

from slayer.sql.render.ranked import RankedGrainProjection, build_ranked_cte_select


def _member(alias: str) -> RankedGrainProjection:
    return RankedGrainProjection(
        output_alias=alias, inner_ref=exp.column(alias, table="_ranked_src"))


def test_ranked_grain_render_is_order_independent() -> None:
    members = [_member("orders.region"), _member("orders.city"),
               _member("orders.month")]
    inner = exp.Select().select(exp.Star()).from_("t")
    pick = exp.Max(this=exp.column("v"))
    sel_a, aliases_a = build_ranked_cte_select(
        inner=inner.copy(), grain=members, pick=pick, agg_alias="m")
    sel_b, aliases_b = build_ranked_cte_select(
        inner=inner.copy(), grain=list(reversed(members)), pick=pick, agg_alias="m")
    assert sel_a.sql() == sel_b.sql()
    assert aliases_a == aliases_b == sorted(aliases_a)
