"""A leading qualifier naming the query root resolves to the root at bind time (system §3.9)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.errors import RefinementConflictError
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev2137_fixtures import list_engine, measure
from tests._dev2187_fixtures import BACKENDS, assert_same, canonical_rows, inline_model, orders_engine
from tests._saved_query_refinement_fixtures import MONTHLY, REVENUE, by_month, refine_engine


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with orders_engine(request.param) as eng:
        yield eng


def _orders(**body: Any) -> dict[str, Any]:
    return {"source_model": "orders", **body}


# --------------------------------------------------------------------------- #
# Qualified ≡ bare, every position, model root
# --------------------------------------------------------------------------- #
async def test_every_position_at_once(engine: SlayerQueryEngine) -> None:
    resp = await assert_same(
        engine,
        _orders(dimensions=["orders.status"], time_dimensions=["month(orders.created_at)"],
                measures=["sum(orders.amount)"], filters=["orders.amount > 5"], order=[{"column": "orders.status"}]),
        _orders(dimensions=["status"], time_dimensions=["month(created_at)"],
                measures=["sum(amount)"], filters=["amount > 5"], order=[{"column": "status"}]),
    )
    assert resp.columns == ["orders.status", "orders.created_at", "orders.amount_sum"]


POSITIONS = {
    "dimension": ({"dimensions": ["orders.status"]}, {"dimensions": ["status"]}),
    "joined-dimension": ({"dimensions": ["orders.customers.name"]}, {"dimensions": ["customers.name"]}),
    "column-time-dimension": (
        {"time_dimensions": [{"dimension": "orders.created_at", "granularity": "month", "label": "M"}]},
        {"time_dimensions": [{"dimension": "created_at", "granularity": "month", "label": "M"}]},
    ),
    "expression-time-dimension": (
        {"time_dimensions": [{"dimension": "coalesce(orders.shipped_at, orders.created_at)",
                              "granularity": "month", "name": "d"}]},
        {"time_dimensions": [{"dimension": "coalesce(shipped_at, created_at)", "granularity": "month", "name": "d"}]},
    ),
    "measure": ({"measures": ["sum(orders.amount)"]}, {"measures": ["sum(amount)"]}),
    "colon-measure": ({"measures": ["orders.amount:sum"]}, {"measures": ["amount:sum"]}),
    "star-count": ({"measures": ["count(orders.*)"]}, {"measures": ["count(*)"]}),
    "arithmetic-measure": (
        {"measures": [{"formula": "sum(orders.amount) / count(orders.*)", "name": "avg_amount"}]},
        {"measures": [{"formula": "sum(amount) / count(*)", "name": "avg_amount"}]},
    ),
    "joined-measure": ({"measures": ["avg(orders.customers.score)"]}, {"measures": ["avg(customers.score)"]}),
    "saved-measure": ({"measures": ["orders.revenue"]}, {"measures": ["revenue"]}),
    "aggregate-filter": (
        {"measures": [{"formula": "sum(orders.amount) FILTER (WHERE orders.status = 'paid')", "name": "p"}]},
        {"measures": [{"formula": "sum(amount) FILTER (WHERE status = 'paid')", "name": "p"}]},
    ),
    "transform": (
        {"time_dimensions": ["month(created_at)"], "measures": [{"formula": "cumsum(sum(orders.amount))", "name": "c"}]},
        {"time_dimensions": ["month(created_at)"], "measures": [{"formula": "cumsum(sum(amount))", "name": "c"}]},
    ),
    "filter": ({"measures": ["count(*)"], "filters": ["orders.amount > 15"]},
               {"measures": ["count(*)"], "filters": ["amount > 15"]}),
    "joined-filter": ({"measures": ["count(*)"], "filters": ["orders.customers.name = 'ann'"]},
                      {"measures": ["count(*)"], "filters": ["customers.name = 'ann'"]}),
    "order-column": (
        {"dimensions": ["status"], "measures": ["count(*)"], "order": [{"column": "orders.status", "direction": "desc"}]},
        {"dimensions": ["status"], "measures": ["count(*)"], "order": [{"column": "status", "direction": "desc"}]},
    ),
    "order-expression": (
        {"dimensions": ["status"], "measures": ["sum(amount)"],
         "order": [{"column": "sum(orders.amount)", "direction": "desc"}]},
        {"dimensions": ["status"], "measures": ["sum(amount)"], "order": [{"column": "sum(amount)", "direction": "desc"}]},
    ),
    "order-colon": (
        {"dimensions": ["status"], "measures": ["amount:sum"], "order": [{"column": "orders.amount:sum"}]},
        {"dimensions": ["status"], "measures": ["amount:sum"], "order": [{"column": "amount:sum"}]},
    ),
    "main-time-dimension": (
        {"time_dimensions": ["month(created_at)"], "measures": [{"formula": "change(sum(amount))", "name": "c"}],
         "main_time_dimension": "orders.created_at"},
        {"time_dimensions": ["month(created_at)"], "measures": [{"formula": "change(sum(amount))", "name": "c"}],
         "main_time_dimension": "created_at"},
    ),
    "computed-dimension": (
        {"dimensions": [{"expression": "orders.amount * 2", "name": "dbl"}], "measures": ["count(*)"]},
        {"dimensions": [{"expression": "amount * 2", "name": "dbl"}], "measures": ["count(*)"]},
    ),
}


@pytest.mark.parametrize("position", sorted(POSITIONS))
async def test_qualified_equals_bare(engine: SlayerQueryEngine, position: str) -> None:
    qualified, bare = POSITIONS[position]
    await assert_same(engine, _orders(**qualified), _orders(**bare))


async def test_a_non_root_qualifier_keeps_its_hop(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(_orders(dimensions=["customers.name"], measures=["count(*)"]))
    assert canonical_rows(resp) == [("ann", "2"), ("bob", "2")]


async def test_an_inner_hop_named_like_the_root_is_still_a_revisit(engine: SlayerQueryEngine) -> None:
    with pytest.raises(Exception, match="customers"):
        await engine.execute(_orders(measures=["count(customers.orders.id)"]))


async def test_a_longer_name_is_no_root_qualifier(engine: SlayerQueryEngine) -> None:
    with pytest.raises(Exception, match="reorders"):
        await engine.execute(_orders(measures=["count(*)"], filters=["reorders.status = 'paid'"]))


# --------------------------------------------------------------------------- #
# Auto-names keep the written text
# --------------------------------------------------------------------------- #
async def test_computed_dimension_auto_name_keeps_its_text(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(_orders(dimensions=[{"expression": "orders.amount * 2"}], measures=["count(*)"]))
    assert resp.columns == ["orders.orders_amount_2", "orders._count"]


async def test_expression_time_dimension_auto_name_keeps_its_text(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(_orders(
        time_dimensions=[{"dimension": "coalesce(orders.shipped_at, orders.created_at)", "granularity": "month"}],
        measures=["count(*)"],
    ))
    assert resp.columns == ["orders.coalesce_orders_shipped_at_orders_created_at", "orders._count"]


# --------------------------------------------------------------------------- #
# Every root kind
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("root", "qualifier"), [
    ({"source_name": "orders"}, "orders"),
    ("inline", "ad_hoc"),
    ("qb", "qb"),
], ids=["extension", "inline-model", "query-backed"])
async def test_every_root_kind_takes_its_own_name(engine: SlayerQueryEngine, root: Any, qualifier: str) -> None:
    source = inline_model() if root == "inline" else root
    await assert_same(
        engine,
        {"source_model": source, "dimensions": [f"{qualifier}.status"], "measures": ["count(*)"]},
        {"source_model": source, "dimensions": ["status"], "measures": ["count(*)"]},
    )


async def test_query_backed_root_measure(engine: SlayerQueryEngine) -> None:
    await assert_same(
        engine,
        {"source_model": "qb", "dimensions": ["qb.status"], "measures": ["sum(qb.tot)"], "filters": ["qb.tot > 25"]},
        {"source_model": "qb", "dimensions": ["status"], "measures": ["sum(tot)"], "filters": ["tot > 25"]},
    )


S1 = {"name": "s1", "source_model": "orders", "dimensions": ["status"],
      "measures": [{"formula": "sum(amount)", "name": "tot"}]}

STAGE_POSITIONS = {
    "computed-dimension": (
        {"dimensions": [{"expression": "s1.tot * 2", "name": "dbl"}]},
        {"dimensions": [{"expression": "tot * 2", "name": "dbl"}]},
    ),
    "measure": ({"measures": [{"formula": "sum(s1.tot)", "name": "t"}]}, {"measures": [{"formula": "sum(tot)", "name": "t"}]}),
    "star-count": ({"measures": [{"formula": "count(s1.*)", "name": "c"}]}, {"measures": [{"formula": "count(*)", "name": "c"}]}),
    "dimension": ({"dimensions": ["s1.status"], "measures": ["count(*)"]}, {"dimensions": ["status"], "measures": ["count(*)"]}),
    "filter": ({"dimensions": ["status"], "filters": ["s1.tot > 25"]}, {"dimensions": ["status"], "filters": ["tot > 25"]}),
    "order": (
        {"dimensions": ["status", "tot"], "order": [{"column": "s1.tot", "direction": "desc"}]},
        {"dimensions": ["status", "tot"], "order": [{"column": "tot", "direction": "desc"}]},
    ),
}


@pytest.mark.parametrize("position", sorted(STAGE_POSITIONS))
async def test_a_stage_root_takes_its_own_name(engine: SlayerQueryEngine, position: str) -> None:
    qualified, bare = STAGE_POSITIONS[position]
    await assert_same(engine, [S1, {"source_model": "s1", **qualified}], [S1, {"source_model": "s1", **bare}])


async def test_a_stage_root_qualifies_a_list_columns_elements() -> None:
    stage = {"name": "s", "source_model": "contractors", "dimensions": ["team"],
             "measures": [measure("collect(skills.value)", "team_skills")]}
    async with list_engine() as eng:
        await assert_same(
            eng,
            [stage, {"source_model": "s", "dimensions": ["s.team_skills.value"],
                     "measures": [measure("count(s.team_skills.*)", "n")]}],
            [stage, {"source_model": "s", "dimensions": ["team_skills.value"],
                     "measures": [measure("count(team_skills.*)", "n")]}],
        )


async def test_downstream_keys_of_a_qualified_dimension(engine: SlayerQueryEngine) -> None:
    final = {"source_model": "s1", "dimensions": ["status"], "measures": ["sum(amount_sum)"]}
    await assert_same(
        engine,
        [{"name": "s1", "source_model": "orders", "dimensions": ["orders.status"], "measures": ["sum(orders.amount)"]},
         final],
        [{"name": "s1", "source_model": "orders", "dimensions": ["status"], "measures": ["sum(amount)"]}, final],
    )


# --------------------------------------------------------------------------- #
# Precedence
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("backend", BACKENDS)
async def test_the_root_wins_over_an_edge_named_like_it(backend: str) -> None:
    async with orders_engine(backend, edge_name="orders") as eng:
        own = await eng.execute(_orders(dimensions=["orders.status"], measures=["count(*)"]))
        assert canonical_rows(own) == [("open", "1"), ("paid", "3")]
        joined = await eng.execute(_orders(dimensions=["orders.orders.name"], measures=["count(*)"]))
        assert canonical_rows(joined) == [("ann", "2"), ("bob", "2")]


async def test_a_root_qualified_filter_matches_a_selected_dotted_measure(engine: SlayerQueryEngine) -> None:
    await assert_same(
        engine,
        _orders(measures=["customers.aov"], filters=["orders.customers.aov > 1"]),
        _orders(measures=["customers.aov"], filters=["customers.aov > 1"]),
    )


# --------------------------------------------------------------------------- #
# Construction-time time-dimension dedupe
# --------------------------------------------------------------------------- #
def _tds(*entries: Any, source_model: Any = "orders") -> SlayerQuery:
    return SlayerQuery.model_validate({"source_model": source_model, "time_dimensions": list(entries)})


@pytest.mark.parametrize("source_model", ["orders", {"source_name": "orders"}], ids=["model", "extension"])
def test_a_root_qualified_duplicate_dedupes(source_model: Any) -> None:
    query = _tds("month(created_at)", "month(orders.created_at)", source_model=source_model)
    assert [(td.dimension, str(td.granularity)) for td in query.time_dimensions or []] == [("created_at", "month")]


def test_a_root_qualified_duplicate_with_another_range_conflicts() -> None:
    with pytest.raises(ValueError, match="Conflicting time dimensions"):
        _tds({"dimension": "created_at", "granularity": "month", "date_range": ["2025-01-01", "2025-06-30"]},
             {"dimension": "orders.created_at", "granularity": "month", "date_range": ["2025-01-01", "2025-03-31"]})


def test_literals_are_never_conflated() -> None:
    query = _tds(
        {"dimension": "iif(note = 'orders.a', created_at, shipped_at)", "granularity": "month",
         "date_range": ["2025-01-01", "2025-12-31"]},
        {"dimension": "iif(note = 'a', created_at, shipped_at)", "granularity": "month",
         "date_range": ["2025-01-01", "2025-06-30"]},
    )
    assert len(query.time_dimensions or []) == 2


def test_spacing_variants_collapse_at_construction() -> None:
    query = _tds("month(coalesce(shipped_at, created_at))", "month(COALESCE(shipped_at,created_at))")
    assert len(query.time_dimensions or []) == 1


async def test_a_root_qualified_duplicate_runs_once(engine: SlayerQueryEngine) -> None:
    await assert_same(
        engine,
        _orders(time_dimensions=["month(created_at)", "month(orders.created_at)"], measures=["count(*)"]),
        _orders(time_dimensions=["month(created_at)"], measures=["count(*)"]),
    )


# --------------------------------------------------------------------------- #
# Refinement merge
# --------------------------------------------------------------------------- #
@pytest.fixture(params=BACKENDS)
async def refine(request):
    async with refine_engine(request.param) as (eng, _):
        yield eng


@pytest.mark.parametrize("refinement", [
    {"measures": [{"formula": "sum(orders.amount)", "name": "revenue"}]},
    {"time_dimensions": ["month(orders.ordered_at)"]},
    {"dimensions": ["orders.region"], "measures": [REVENUE]},
], ids=["measure-formula", "time-dimension", "dimension"])
async def test_a_root_qualified_entry_is_the_saved_entry(refine: SlayerQueryEngine, refinement: dict) -> None:
    plain = await refine.execute("monthly_revenue", refine={k: v for k, v in refinement.items() if k == "dimensions"})
    refined = await refine.execute("monthly_revenue", refine=refinement)
    assert refined.sql == plain.sql
    assert canonical_rows(refined) == canonical_rows(plain)


@pytest.mark.parametrize("measures", [
    [{"formula": "revenue", "name": "revenue"}, "count(*)"],
    [{"formula": "revenue"}, "count(*)"],
    ["revenue", "count(*)"],
    ["orders.revenue", "count(*)"],
    [{"formula": "orders.revenue", "name": "revenue"}, "count(*)"],
], ids=["named", "unnamed", "shorthand", "root-qualified", "root-qualified-named"])
async def test_restating_a_saved_measure_by_name(refine: SlayerQueryEngine, measures: list) -> None:
    refined = await refine.execute("monthly_revenue", refine={"measures": measures})
    expected = await refine.execute("monthly_revenue", refine={"measures": ["count(*)"]})
    assert refined.sql == expected.sql
    assert refined.columns == expected.columns
    assert by_month(refined.data) == MONTHLY


async def test_a_root_qualified_restatement_with_another_label_conflicts(refine: SlayerQueryEngine) -> None:
    with pytest.raises(RefinementConflictError) as info:
        await refine.execute("monthly_revenue", refine={"measures": [{"formula": "orders.revenue", "label": "Rev"}]})
    assert "measures" in str(info.value)
    assert "revenue" in str(info.value)


async def test_a_root_qualified_unnamed_formula_is_the_saved_formula(refine: SlayerQueryEngine) -> None:
    await refine.create_model_from_query(query={"source_model": "orders", "measures": ["sum(amount)"]}, name="total")
    plain = await refine.execute("total")
    refined = await refine.execute("total", refine={"measures": ["sum(orders.amount)"]})
    assert refined.sql == plain.sql
    assert refined.columns == plain.columns


async def test_refinement_literals_are_never_conflated(refine: SlayerQueryEngine) -> None:
    await refine.create_model_from_query(
        query={"source_model": "orders", "measures": ["sum(iif(status = 'paid', amount, 0))"]}, name="paid_total",
    )
    resp = await refine.execute("paid_total", refine={"measures": ["sum(iif(status = 'orders.paid', amount, 0))"]})
    assert len(resp.columns) == 2
    assert sorted(float(v) for v in resp.data[0].values()) == [0.0, 255.0]
