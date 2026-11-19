"""A leading qualifier naming the query root resolves to the root at bind time (semantics.arc42.md Principle 1)."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import RefinementConflictError
from slayer.core.models import DatasetField
from slayer.core.query import SlayerQuery
from slayer.engine.population.inference import spine_datasource
from slayer.engine.query_engine import SlayerQueryEngine

from tests._dev1866_fixtures import DS_FAN, make_inference_storage
from tests._dev2137_fixtures import list_engine, metric
from tests._dev2187_fixtures import BACKENDS, assert_same, canonical_rows, orders_engine, orders_dataset
from tests._saved_query_refinement_fixtures import MONTHLY, REVENUE, by_month, refine_engine
from tests._unrelated_datasets_fixtures import m, main_engine, query


@pytest.fixture(params=BACKENDS)
async def engine(request):
    async with orders_engine(request.param) as eng:
        yield eng


def _orders(**body: Any) -> dict[str, Any]:
    return {"population": "orders", **body}


# --------------------------------------------------------------------------- #
# Qualified ≡ bare, every position, model root
# --------------------------------------------------------------------------- #
async def test_every_position_at_once(engine: SlayerQueryEngine) -> None:
    resp = await assert_same(
        engine,
        _orders(dimensions=["orders.status"], time_dimensions=["date_trunc('month', orders.created_at)"],
                metrics=["sum(orders.amount)"], filter=["orders.amount > 5"], order=[{"field": "orders.status"}]),
        _orders(dimensions=["status"], time_dimensions=["date_trunc('month', created_at)"],
                metrics=["sum(amount)"], filter=["amount > 5"], order=[{"field": "status"}]),
    )
    assert resp.columns == ["orders.status", "orders.created_at", "orders.amount_sum"]


POSITIONS = {
    "dimension": ({"dimensions": ["orders.status"]}, {"dimensions": ["status"]}),
    "joined-dimension": ({"dimensions": ["orders.customers.name"]}, {"dimensions": ["customers.name"]}),
    "column-time-dimension": (
        {"time_dimensions": [{"field": "orders.created_at", "granularity": "month", "label": "M"}]},
        {"time_dimensions": [{"field": "created_at", "granularity": "month", "label": "M"}]},
    ),
    "expression-time-dimension": (
        {"time_dimensions": [{"field": "coalesce(orders.shipped_at, orders.created_at)",
                              "granularity": "month", "alias": "d"}]},
        {"time_dimensions": [{"field": "coalesce(shipped_at, created_at)", "granularity": "month", "alias": "d"}]},
    ),
    "measure": ({"metrics": ["sum(orders.amount)"]}, {"metrics": ["sum(amount)"]}),
    "star-count": ({"metrics": ["count(orders.*)"]}, {"metrics": ["count(*)"]}),
    "arithmetic-measure": (
        {"metrics": [{"metric": "sum(orders.amount) / count(orders.*)", "alias": "avg_amount"}]},
        {"metrics": [{"metric": "sum(amount) / count(*)", "alias": "avg_amount"}]},
    ),
    "joined-measure": ({"metrics": ["avg(orders.customers.score)"]}, {"metrics": ["avg(customers.score)"]}),
    "saved-measure": ({"metrics": ["orders.revenue"]}, {"metrics": ["revenue"]}),
    "aggregate-filter": (
        {"metrics": [{"metric": "sum(orders.amount) FILTER (WHERE orders.status = 'paid')", "alias": "p"}]},
        {"metrics": [{"metric": "sum(amount) FILTER (WHERE status = 'paid')", "alias": "p"}]},
    ),
    "transform": (
        {"time_dimensions": ["date_trunc('month', created_at)"], "metrics": [{"metric": "cumsum(sum(orders.amount))", "alias": "c"}]},
        {"time_dimensions": ["date_trunc('month', created_at)"], "metrics": [{"metric": "cumsum(sum(amount))", "alias": "c"}]},
    ),
    "filter": ({"metrics": ["count(*)"], "filter": ["orders.amount > 15"]},
               {"metrics": ["count(*)"], "filter": ["amount > 15"]}),
    "joined-filter": ({"metrics": ["count(*)"], "filter": ["orders.customers.name = 'ann'"]},
                      {"metrics": ["count(*)"], "filter": ["customers.name = 'ann'"]}),
    "order-column": (
        {"dimensions": ["status"], "metrics": ["count(*)"], "order": [{"field": "orders.status", "direction": "desc"}]},
        {"dimensions": ["status"], "metrics": ["count(*)"], "order": [{"field": "status", "direction": "desc"}]},
    ),
    "order-expression": (
        {"dimensions": ["status"], "metrics": ["sum(amount)"],
         "order": [{"field": "sum(orders.amount)", "direction": "desc"}]},
        {"dimensions": ["status"], "metrics": ["sum(amount)"], "order": [{"field": "sum(amount)", "direction": "desc"}]},
    ),
    "order-aggregate": (
        {"dimensions": ["status"], "metrics": ["sum(amount)"], "order": [{"field": "sum(orders.amount)"}]},
        {"dimensions": ["status"], "metrics": ["sum(amount)"], "order": [{"field": "sum(amount)"}]},
    ),
    "main-time-dimension": (
        {"time_dimensions": ["date_trunc('month', created_at)"], "metrics": [{"metric": "change(sum(amount))", "alias": "c"}],
         "main_time_dimension": "orders.created_at"},
        {"time_dimensions": ["date_trunc('month', created_at)"], "metrics": [{"metric": "change(sum(amount))", "alias": "c"}],
         "main_time_dimension": "created_at"},
    ),
    "computed-dimension": (
        {"dimensions": [{"field": "orders.amount * 2", "alias": "dbl"}], "metrics": ["count(*)"]},
        {"dimensions": [{"field": "amount * 2", "alias": "dbl"}], "metrics": ["count(*)"]},
    ),
}


@pytest.mark.parametrize("position", sorted(POSITIONS))
async def test_qualified_equals_bare(engine: SlayerQueryEngine, position: str) -> None:
    qualified, bare = POSITIONS[position]
    await assert_same(engine, _orders(**qualified), _orders(**bare))


async def test_a_non_root_qualifier_keeps_its_hop(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(_orders(dimensions=["customers.name"], metrics=["count(*)"]))
    assert canonical_rows(resp) == [("ann", "2"), ("bob", "2")]


async def test_an_inner_hop_named_like_the_root_is_still_a_revisit(engine: SlayerQueryEngine) -> None:
    body = _orders(metrics=["count(customers.orders.id)"])
    with pytest.raises(Exception, match="customers"):
        await engine.execute(body)


async def test_a_longer_name_is_no_root_qualifier(engine: SlayerQueryEngine) -> None:
    body = _orders(metrics=["count(*)"], filter=["reorders.status = 'paid'"])
    with pytest.raises(Exception, match="reorders"):
        await engine.execute(body)


# --------------------------------------------------------------------------- #
# Auto-names keep the written text
# --------------------------------------------------------------------------- #
async def test_computed_dimension_auto_name_keeps_its_text(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(_orders(dimensions=[{"field": "orders.amount * 2"}], metrics=["count(*)"]))
    assert resp.columns == ["orders.amount_2", "orders._count"]


async def test_expression_time_dimension_auto_name_keeps_its_text(engine: SlayerQueryEngine) -> None:
    resp = await engine.execute(_orders(
        time_dimensions=[{"field": "coalesce(orders.shipped_at, orders.created_at)", "granularity": "month"}],
        metrics=["count(*)"],
    ))
    assert resp.columns == ["orders.coalesce_shipped_at_created_at", "orders._count"]


# --------------------------------------------------------------------------- #
# Every root kind
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("root", "qualifier"), [
    ({"population": "orders", "extensions": [{"dataset": "orders"}]}, "orders"),
    ({"population": "qb"}, "qb"),
], ids=["extension", "query-backed"])
async def test_every_root_kind_takes_its_own_name(engine: SlayerQueryEngine, root: dict[str, Any], qualifier: str) -> None:
    await assert_same(
        engine,
        {**root, "dimensions": [f"{qualifier}.status"], "metrics": ["count(*)"]},
        {**root, "dimensions": ["status"], "metrics": ["count(*)"]},
    )


async def test_query_backed_root_measure(engine: SlayerQueryEngine) -> None:
    await assert_same(
        engine,
        {"population": "qb", "dimensions": ["qb.status"], "metrics": ["sum(qb.tot)"], "filter": ["qb.tot > 25"]},
        {"population": "qb", "dimensions": ["qb.status"], "metrics": ["sum(qb.tot)"], "filter": ["qb.tot > 25"]},
    )


S1 = {"name": "s1", "population": "orders", "dimensions": ["orders.status"],
      "metrics": [{"metric": "sum(orders.amount)", "alias": "tot"}]}

STAGE_POSITIONS = {
    "computed-dimension": (
        {"dimensions": [{"field": "s1.tot * 2", "alias": "dbl"}]},
        {"dimensions": [{"field": "tot * 2", "alias": "dbl"}]},
    ),
    "measure": ({"metrics": [{"metric": "sum(s1.tot)", "alias": "t"}]}, {"metrics": [{"metric": "sum(tot)", "alias": "t"}]}),
    "star-count": ({"metrics": [{"metric": "count(s1.*)", "alias": "c"}]}, {"metrics": [{"metric": "count(*)", "alias": "c"}]}),
    "dimension": ({"dimensions": ["s1.status"], "metrics": ["count(*)"]}, {"dimensions": ["status"], "metrics": ["count(*)"]}),
    "filter": ({"dimensions": ["status"], "filter": ["s1.tot > 25"]}, {"dimensions": ["status"], "filter": ["tot > 25"]}),
    "order": (
        {"dimensions": ["status", "tot"], "order": [{"field": "s1.tot", "direction": "desc"}]},
        {"dimensions": ["status", "tot"], "order": [{"field": "tot", "direction": "desc"}]},
    ),
}


@pytest.mark.parametrize("position", sorted(STAGE_POSITIONS))
async def test_a_stage_root_takes_its_own_name(engine: SlayerQueryEngine, position: str) -> None:
    qualified, bare = STAGE_POSITIONS[position]
    await assert_same(engine, [S1, {"population": "s1", **qualified}], [S1, {"population": "s1", **bare}])


async def test_a_stage_root_qualifies_a_list_columns_elements() -> None:
    stage = {"name": "s", "population": "contractors", "dimensions": ["contractors.team"],
             "metrics": [metric("collect(skills.value)", "team_skills")]}
    async with list_engine() as eng:
        await assert_same(
            eng,
            [stage, {"population": "s", "dimensions": ["s.team_skills.value"],
                     "metrics": [metric("count(s.team_skills.*)", "n")]}],
            [stage, {"population": "s", "dimensions": ["s.team_skills.value"],
                     "metrics": [metric("count(team_skills.*)", "n")]}],
        )


async def test_downstream_keys_of_a_qualified_dimension(engine: SlayerQueryEngine) -> None:
    final = {"population": "s1", "dimensions": ["s1.status"], "metrics": ["sum(s1.amount_sum)"]}
    await assert_same(
        engine,
        [{"name": "s1", "population": "orders", "dimensions": ["orders.status"], "metrics": ["sum(orders.amount)"]},
         final],
        [{"name": "s1", "population": "orders", "dimensions": ["orders.status"], "metrics": ["sum(orders.amount)"]}, final],
    )


# --------------------------------------------------------------------------- #
# Precedence
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("backend", BACKENDS)
async def test_the_root_wins_over_an_edge_named_like_it(backend: str) -> None:
    async with orders_engine(backend, edge_name="orders") as eng:
        own = await eng.execute(_orders(dimensions=["orders.status"], metrics=["count(*)"]))
        assert canonical_rows(own) == [("open", "1"), ("paid", "3")]
        joined = await eng.execute(_orders(dimensions=["orders.orders.name"], metrics=["count(*)"]))
        assert canonical_rows(joined) == [("ann", "2"), ("bob", "2")]


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_column_sql_root_qualifier_keeps_an_edge_named_like_the_root(backend: str) -> None:
    async with orders_engine(backend, edge_name="orders") as eng:
        model = orders_dataset(edge_name="orders")
        extra = DatasetField(name="cust_name", datatype=DataType.STRING, sql="orders.orders.name")
        await eng.storage.save_dataset(model.model_copy(update={"fields": [*model.fields, extra]}), _validate=False)
        resp = await eng.execute(_orders(dimensions=["cust_name"], metrics=["count(*)"]))
        assert canonical_rows(resp) == [("ann", "2"), ("bob", "2")]


# --------------------------------------------------------------------------- #
# Every reader of a query's references is root-relative
# --------------------------------------------------------------------------- #
async def test_a_root_qualified_spine_filter(engine: SlayerQueryEngine) -> None:
    body = {"dimensions": ["status"], "metrics": ["count(*)"]}
    await assert_same(
        engine,
        _orders(**body, filter=["orders.time_spine.timestamp >= date '2025-02-01'"]),
        _orders(**body, filter=["time_spine.timestamp >= date '2025-02-01'"]),
    )


@pytest.mark.parametrize("body", [
    {"metrics": ["count(basket.id)"]},
    {"metrics": ["count(time_spine.basket.id)"]},
    {"dimensions": ["time_spine.basket.id"]},
    {"filter": ["time_spine.basket.id > 1"]},
], ids=["bare", "measure", "dimension", "filter"])
async def test_a_spine_rooted_reference_anchors_its_datasource(body: dict[str, Any]) -> None:
    storage = await make_inference_storage()
    spine_rooted = SlayerQuery.model_validate({"population": "time_spine", **body})
    assert await spine_datasource(queries=[spine_rooted], storage=storage, data_source=None) == DS_FAN


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_root_qualified_hop_into_an_unrelated_dataset(backend: str) -> None:
    async with main_engine(backend) as eng:
        await assert_same(eng, query(metrics=[m("count(customers.events.id)", "n")]),
                          query(metrics=[m("count(events.id)", "n")]))


async def test_a_root_qualified_filter_matches_a_selected_dotted_measure(engine: SlayerQueryEngine) -> None:
    await assert_same(
        engine,
        _orders(metrics=["customers.aov"], filter=["orders.customers.aov > 1"]),
        _orders(metrics=["customers.aov"], filter=["customers.aov > 1"]),
    )


# --------------------------------------------------------------------------- #
# Construction-time time-dimension dedupe
# --------------------------------------------------------------------------- #
def _tds(*entries: Any, extensions: Any = None) -> SlayerQuery:
    root = {"population": "orders"} if extensions is None else {"population": "orders", "extensions": extensions}
    return SlayerQuery.model_validate({**root, "time_dimensions": list(entries)})


@pytest.mark.parametrize("extensions", [None, [{"dataset": "orders"}]], ids=["model", "extension"])
def test_a_root_qualified_duplicate_dedupes(extensions: Any) -> None:
    query = _tds("date_trunc('month', created_at)", "date_trunc('month', orders.created_at)", extensions=extensions)
    assert [(td.field, str(td.granularity)) for td in query.time_dimensions or []] == [("created_at", "month")]


def test_a_root_qualified_duplicate_with_another_range_conflicts() -> None:
    with pytest.raises(ValueError, match="Conflicting time dimensions"):
        _tds({"field": "created_at", "granularity": "month", "date_range": ["2025-01-01", "2025-06-30"]},
             {"field": "orders.created_at", "granularity": "month", "date_range": ["2025-01-01", "2025-03-31"]})


def test_literals_are_never_conflated() -> None:
    query = _tds(
        {"field": "iif(note = 'orders.a', created_at, shipped_at)", "granularity": "month",
         "date_range": ["2025-01-01", "2025-12-31"]},
        {"field": "iif(note = 'a', created_at, shipped_at)", "granularity": "month",
         "date_range": ["2025-01-01", "2025-06-30"]},
    )
    assert len(query.time_dimensions or []) == 2


def test_spacing_variants_collapse_at_construction() -> None:
    query = _tds("date_trunc('month', coalesce(shipped_at, created_at))", "date_trunc('month', COALESCE(shipped_at,created_at))")
    assert len(query.time_dimensions or []) == 1


async def test_a_root_qualified_duplicate_runs_once(engine: SlayerQueryEngine) -> None:
    await assert_same(
        engine,
        _orders(time_dimensions=["date_trunc('month', created_at)", "date_trunc('month', orders.created_at)"], metrics=["count(*)"]),
        _orders(time_dimensions=["date_trunc('month', created_at)"], metrics=["count(*)"]),
    )


# --------------------------------------------------------------------------- #
# Refinement merge
# --------------------------------------------------------------------------- #
@pytest.fixture(params=BACKENDS)
async def refine(request):
    async with refine_engine(request.param) as (eng, _):
        yield eng


@pytest.mark.parametrize("refinement", [
    {"metrics": [{"metric": "sum(orders.amount)", "alias": "revenue"}]},
    {"time_dimensions": ["date_trunc('month', orders.ordered_at)"]},
    {"dimensions": ["orders.region"], "metrics": [REVENUE]},
], ids=["measure-formula", "time-dimension", "dimension"])
async def test_a_root_qualified_entry_is_the_saved_entry(refine: SlayerQueryEngine, refinement: dict) -> None:
    plain = await refine.execute("monthly_revenue", refine={k: v for k, v in refinement.items() if k == "dimensions"})
    refined = await refine.execute("monthly_revenue", refine=refinement)
    assert refined.sql == plain.sql
    assert canonical_rows(refined) == canonical_rows(plain)


@pytest.mark.parametrize("measures", [
    [{"metric": "revenue", "alias": "revenue"}, "count(*)"],
    [{"metric": "revenue"}, "count(*)"],
    ["revenue", "count(*)"],
    ["orders.revenue", "count(*)"],
    [{"metric": "orders.revenue", "alias": "revenue"}, "count(*)"],
], ids=["named", "unnamed", "shorthand", "root-qualified", "root-qualified-named"])
async def test_restating_a_saved_measure_by_name(refine: SlayerQueryEngine, measures: list) -> None:
    refined = await refine.execute("monthly_revenue", refine={"metrics": measures})
    expected = await refine.execute("monthly_revenue", refine={"metrics": ["count(orders.*)"]})
    assert refined.sql == expected.sql
    assert refined.columns == expected.columns
    assert by_month(refined.data) == MONTHLY


async def test_a_root_qualified_restatement_with_another_label_conflicts(refine: SlayerQueryEngine) -> None:
    with pytest.raises(RefinementConflictError) as info:
        await refine.execute("monthly_revenue", refine={"metrics": [{"metric": "orders.revenue", "label": "Rev"}]})
    assert "metrics" in str(info.value)
    assert "revenue" in str(info.value)


async def test_a_root_qualified_unnamed_formula_is_the_saved_formula(refine: SlayerQueryEngine) -> None:
    await refine.create_dataset_from_query(query={"population": "orders", "metrics": ["sum(orders.amount)"]}, name="total")
    plain = await refine.execute("total")
    refined = await refine.execute("total", refine={"metrics": ["sum(orders.amount)"]})
    assert refined.sql == plain.sql
    assert refined.columns == plain.columns


async def test_refinement_literals_are_never_conflated(refine: SlayerQueryEngine) -> None:
    await refine.create_dataset_from_query(
        query={"population": "orders", "metrics": ["sum(iif(orders.status = 'paid', orders.amount, 0))"]}, name="paid_total",
    )
    resp = await refine.execute("paid_total", refine={"metrics": ["sum(iif(orders.status = 'orders.paid', orders.amount, 0))"]})
    assert len(resp.columns) == 2
    assert sorted(float(v) for v in resp.data[0].values()) == [0.0, 255.0]
