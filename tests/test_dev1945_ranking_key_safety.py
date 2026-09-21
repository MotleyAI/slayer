"""DEV-1945: the ranking key of a ``first``/``last`` is a closure-judged input.

Explicit argument or implicitly resolved (temporal row dimension, time dimension's
raw column, model ``default_time_dimension``), host-rooted, target-rooted and
windowed alike: a key crossing a hop that is not provably to-one fails closed with
the existing input-safety messages, naming the aggregate by its canonical alias;
safe keys keep executing, a derived key rendering as its plain expansion with no
added CAST.
"""

from __future__ import annotations

import inspect
import re
import warnings
from typing import Optional

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.keys import AggregateKey, ColumnKey, ColumnSqlKey
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.ranked_planner import resolve_ranking_time_key
from slayer.sql.naming import canonical_aggregate_alias
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1945_fixtures import (
    FIRST_AMOUNT,
    JAN,
    LAST_AMOUNT,
    dev1945_bundle,
    dev1945_models,
    make_engine,
    measure,
    month_td,
    orders_q,
)
from tests._exec_fixture_helpers import month_key

HOST_HOP = "unproven join hop to line_items from orders"
MODES = ["broadcast", "error", "associate"]

# Host-coordinate aggregate keys — the alias every refusal must name (D6).
AMOUNT = ColumnKey(path=(), leaf="amount")
AMOUNT_LAST = AggregateKey(source=AMOUNT, agg="last")
AMOUNT_FIRST = AggregateKey(source=AMOUNT, agg="first")
AMOUNT_LAST_30D = AggregateKey(source=AMOUNT, agg="last", kwargs=(("window", "30d"),))
AMOUNT_LAST_LI_TS = AggregateKey(
    source=AMOUNT, agg="last",
    args=(ColumnSqlKey(path=(), model="orders", column_name="li_ts"),),
)
AMOUNT_LAST_LI_CREATED = AggregateKey(
    source=AMOUNT, agg="last", args=(ColumnKey(path=("line_items",), leaf="created_at"),),
)
LI_QTY_LAST = AggregateKey(source=ColumnKey(path=("line_items",), leaf="qty"), agg="last")


def _alias_of(msg: str) -> str:
    match = re.search(r"[Aa]ggregate '([^']+)'", msg)
    assert match, msg
    return match.group(1)


async def _refused(
    engine: SlayerQueryEngine, *fragments: str, agg_key: AggregateKey, **kw,
) -> str:
    with pytest.raises(ValueError) as ei:
        await engine.execute(orders_q(**kw), dry_run=True)
    msg = str(ei.value)
    for fragment in fragments:
        assert fragment in msg, f"{fragment!r} missing from: {msg}"
    assert _alias_of(msg) == canonical_aggregate_alias(agg_key, profile="stage_formula"), msg
    return msg


def _by_month(resp) -> dict:
    time_key = next(k for k in resp.columns if k.endswith("created_at"))
    return {month_key(r[time_key]): r["orders.l"] for r in resp.data}


def _joined_tables(select: exp.Select) -> set:
    return {
        j.this.alias_or_name for j in (select.args.get("joins") or [])
        if isinstance(j.this, exp.Table)
    }


def _assert_plain_ranking_expr(expr: exp.Expression, *, leaf: str) -> None:
    """The ranking expression is the bare column ``leaf`` — no CAST, no wrapper."""
    assert expr.find(exp.Cast) is None, expr.sql()
    assert isinstance(expr, exp.Column) and expr.name == leaf, expr.sql()


def _ranked_cte_order_expr(sql: str) -> tuple[exp.Expression, exp.Select]:
    """(ORDER BY expression, enclosing SELECT) of the single ranked ``ROW_NUMBER``."""
    tree = sqlglot.parse_one(sql, dialect="sqlite")
    [window] = [w for w in tree.find_all(exp.Window) if isinstance(w.this, exp.RowNumber)]
    [ordered] = window.args["order"].expressions
    select = window.find_ancestor(exp.Select)
    assert select is not None
    return ordered.this, select


def _w_rank_expr(sql: str) -> tuple[exp.Expression, exp.Select]:
    """(expression aliased ``_w_rank``, the ``_src`` SELECT projecting it)."""
    tree = sqlglot.parse_one(sql, dialect="sqlite")
    [alias] = [a for a in tree.find_all(exp.Alias) if a.alias == "_w_rank"]
    select = alias.find_ancestor(exp.Select)
    assert select is not None
    return alias.this, select


# --------------------------------------------------------------------------- #
# 1.1 Fixture — the graph saves with exactly the unproven-hop warnings
# --------------------------------------------------------------------------- #
class TestFixture:
    async def test_models_save_with_only_the_backstop_warning(self, tmp_path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        models = dev1945_models(orders_default="li_ts", line_items_default="sh_ts")
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter("always")
            for model in reversed(models):  # targets first, so each hop is loaded
                await storage.save_model(model)
        arity = [str(w.message) for w in rec if w.category is UserWarning]
        expected = {("li_ts", "line_items"), ("li_flag_ts", "line_items"), ("sh_ts", "shipments")}
        assert len(arity) == len(expected), arity
        assert all("crossing an unproven join hop" in m for m in arity), arity
        named = {
            (column, hop) for column, hop in expected
            if any(f"column {column!r}" in m and f"to {hop!r}" in m for m in arity)
        }
        assert named == expected, arity


# --------------------------------------------------------------------------- #
# 1.2 Implicit model default across an unproven hop
# --------------------------------------------------------------------------- #
class TestImplicitDefaultRefused:
    @pytest.mark.parametrize("mode", MODES)
    @pytest.mark.parametrize("agg_key", [AMOUNT_LAST, AMOUNT_FIRST], ids=["last", "first"])
    async def test_host_default_across_unproven_hop(self, tmp_path, agg_key, mode) -> None:
        engine = await make_engine(str(tmp_path), orders_default="li_ts")
        await _refused(
            engine, "ranks/reads by li_ts", HOST_HOP, agg_key=agg_key,
            measures=[measure(f"amount:{agg_key.agg}")], to_many_handling=mode,
        )

    @pytest.mark.parametrize("mode", MODES)
    async def test_target_default_across_unproven_hop(self, tmp_path, mode) -> None:
        engine = await make_engine(str(tmp_path), line_items_default="sh_ts")
        await _refused(
            engine, "ranks/reads by sh_ts", "not attributable from line_items", "shipments",
            agg_key=LI_QTY_LAST,
            measures=[measure("line_items.qty:last")], to_many_handling=mode,
        )


# --------------------------------------------------------------------------- #
# 1.3 Implicit grain candidates across an unproven hop
# --------------------------------------------------------------------------- #
class TestImplicitGrainKeyRefused:
    @pytest.mark.parametrize("mode", MODES)
    async def test_fanning_temporal_dimension(self, tmp_path, mode) -> None:
        engine = await make_engine(str(tmp_path), orders_default="created_at")
        await _refused(
            engine, "ranks/reads by created_at", HOST_HOP, agg_key=AMOUNT_LAST,
            dimensions=["line_items.created_at"], measures=[measure("amount:last")],
            to_many_handling=mode,
        )

    async def test_fanning_temporal_dimension_under_window(self, tmp_path) -> None:
        engine = await make_engine(str(tmp_path), orders_default="created_at")
        await _refused(
            engine, "ranks/reads by created_at", HOST_HOP, agg_key=AMOUNT_LAST_30D,
            dimensions=["line_items.created_at"], time_dimensions=month_td(),
            measures=[measure("amount:last(window='30d')")],
        )

    async def test_fanning_time_dimension(self, tmp_path) -> None:
        engine = await make_engine(str(tmp_path), orders_default="created_at")
        await _refused(
            engine, "ranks/reads by created_at", HOST_HOP, agg_key=AMOUNT_LAST,
            time_dimensions=month_td("line_items.created_at"),
            measures=[measure("amount:last")],
        )


# --------------------------------------------------------------------------- #
# 1.4 Explicit arguments and definition-bearing defaults
# --------------------------------------------------------------------------- #
class TestExplicitAndDefinitionKeys:
    async def test_explicit_derived_argument_names_the_argument(self, tmp_path) -> None:
        engine = await make_engine(str(tmp_path), orders_default="created_at")
        await _refused(
            engine, "ranks/reads by li_ts", HOST_HOP, agg_key=AMOUNT_LAST_LI_TS,
            measures=[measure("amount:last(li_ts)")],
        )

    async def test_explicit_structural_argument_keeps_its_message(self, tmp_path) -> None:
        engine = await make_engine(str(tmp_path), orders_default="created_at")
        await _refused(
            engine, "ranks/reads by created_at", HOST_HOP, agg_key=AMOUNT_LAST_LI_CREATED,
            measures=[measure("amount:last(line_items.created_at)")],
        )

    async def test_default_whose_filter_crosses(self, tmp_path) -> None:
        engine = await make_engine(str(tmp_path), orders_default="li_flag_ts")
        await _refused(
            engine, "ranks/reads by li_flag_ts", HOST_HOP, agg_key=AMOUNT_LAST,
            measures=[measure("amount:last")],
        )

    async def test_unanalyzable_default(self, tmp_path) -> None:
        engine = await make_engine(str(tmp_path), orders_default="bad_ts")
        await _refused(
            engine, "names derived column 'bad_ts'", "no supported dialect can analyse",
            agg_key=AMOUNT_LAST, measures=[measure("amount:last")],
        )


# --------------------------------------------------------------------------- #
# 1.5 Safe keys keep executing (the over-refusal guard)
# --------------------------------------------------------------------------- #
class TestSafeKeysExecute:
    async def test_local_default(self, tmp_path) -> None:
        engine = await make_engine(str(tmp_path), orders_default="created_at")
        resp = await engine.execute(orders_q(
            measures=[measure("amount:last"), measure("amount:first", "f")],
        ))
        assert resp.data == [{"orders.l": LAST_AMOUNT, "orders.f": FIRST_AMOUNT}]

    async def test_filter_only_temporal_column_without_default(self, tmp_path) -> None:
        engine = await make_engine(str(tmp_path))
        resp = await engine.execute(orders_q(
            filters=["created_at > '2023-01-01'"], measures=[measure("amount:last")],
        ))
        assert resp.data == [{"orders.l": LAST_AMOUNT}]

    async def test_derived_default_over_proven_hop(self, tmp_path) -> None:
        engine = await make_engine(str(tmp_path), orders_default="cust_signup")
        resp = await engine.execute(orders_q(measures=[measure("amount:last")]))
        assert resp.data == [{"orders.l": LAST_AMOUNT}]
        assert resp.sql is not None
        order_expr, ranked_select = _ranked_cte_order_expr(resp.sql)
        _assert_plain_ranking_expr(order_expr, leaf="signup_at")
        assert "customers" in _joined_tables(ranked_select)

    async def test_windowed_bucket_key_wins_over_a_crossing_default(self, tmp_path) -> None:
        engine = await make_engine(str(tmp_path), orders_default="li_ts")
        resp = await engine.execute(orders_q(
            time_dimensions=month_td(), measures=[measure("amount:last(window='30d')")],
        ))
        assert _by_month(resp) == {JAN: LAST_AMOUNT}
        assert resp.sql is not None and "line_items" not in resp.sql

    async def test_windowed_explicit_derived_key_over_proven_hop(self, tmp_path) -> None:
        engine = await make_engine(str(tmp_path), orders_default="created_at")
        resp = await engine.execute(orders_q(
            time_dimensions=month_td(),
            measures=[measure("amount:last(cust_signup, window='30d')")],
        ))
        assert _by_month(resp) == {JAN: LAST_AMOUNT}
        assert resp.sql is not None
        rank_expr, src_select = _w_rank_expr(resp.sql)
        _assert_plain_ranking_expr(rank_expr, leaf="signup_at")
        assert "customers" in _joined_tables(src_select)


# --------------------------------------------------------------------------- #
# 1.6 Resolver units — typed default, no target_path
# --------------------------------------------------------------------------- #
def _resolve(default: Optional[str]):
    models = dev1945_models(orders_default=default)
    return resolve_ranking_time_key(
        key=AMOUNT_LAST, root_model=models[0], bundle=dev1945_bundle(models),
    )


class TestResolver:
    @pytest.mark.parametrize("default", ["li_ts", "li_flag_ts", "cust_signup"])
    def test_expanding_default_is_typed(self, default: str) -> None:
        assert _resolve(default) == ColumnSqlKey(path=(), model="orders", column_name=default)

    def test_plain_default_stays_a_column_key(self) -> None:
        assert _resolve("created_at") == ColumnKey(path=(), leaf="created_at")

    def test_no_target_path_parameter(self) -> None:
        assert "target_path" not in inspect.signature(resolve_ranking_time_key).parameters
