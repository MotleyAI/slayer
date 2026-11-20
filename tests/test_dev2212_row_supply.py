"""Rows of an inferred query are the row supply (spec: queries/population › Rows of an inferred query are the
row supply), plus the queries naming ``population`` that keep their rows."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import UnrelatedDatasetError
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.syntax import respell_bare_names

from tests._dev1737_fixtures import TableSpec
from tests._dev2212_fixtures import (
    BACKENDS,
    BASKETS_STATEMENTS,
    DS,
    HIDE_C3,
    NOTES,
    baskets_dataset,
    customers_dataset,
    kinds,
    notes_dataset,
    orders_dataset,
    rows,
    run,
    seed_datasets,
    seed_engine,
    table,
    warned_metrics,
    with_policy,
)

R, REV, RET, VIS, TIX = (
    "customers.region", "orders.total_revenue", "returns.total_returns", "visits.visit_count", "tickets.ticket_count")
MONTH = [{"field": "orders.ordered_at", "granularity": "month"}]


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


async def test_a_dimension_value_no_metric_reaches_is_not_a_row(backend: str) -> None:
    async with seed_engine(backend) as eng:
        revenue = await run(eng, dimensions=[R], metrics=[REV])
        returns = await run(eng, dimensions=[R], metrics=[RET])
    assert table(revenue, R, REV) == rows(("EAST", 350), ("WEST", 300))
    assert table(returns, R, RET) == rows(("EAST", 25), ("NORTH", 10))
    assert revenue.population == "customers"
    assert revenue.population_inferred is True


async def test_an_orphan_home_row_lands_in_the_null_tuple(backend: str) -> None:
    async with seed_engine(backend, orphan=True) as eng:
        plain = await run(eng, dimensions=[R], metrics=[REV])
        filtered = await run(eng, dimensions=[R], metrics=[REV], filter=["orders.amount > 60"])
    assert table(plain, R, REV) == rows(("EAST", 350), ("WEST", 300), (None, 70))
    assert table(filtered, R, REV) == rows(("EAST", 300), ("WEST", 300), (None, 70))
    assert plain.columns == filtered.columns == [R, REV]
    assert plain.population == filtered.population == "customers"


async def test_two_metrics_union_their_rows(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[REV, RET])
    assert table(resp, R, REV, RET) == rows(("EAST", 350, 25), ("WEST", 300, None), ("NORTH", None, 10))


async def test_a_count_reads_zero_on_a_row_another_metric_supplies(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[REV, VIS])
    assert table(resp, R, REV, VIS) == rows(("EAST", 350, 0), ("WEST", 300, 1), ("NORTH", None, 2))


@pytest.mark.parametrize("orphan, expected", [(False, 162.5), (True, 180.0)])
async def test_a_ratio_without_dimensions_returns_one_row(backend: str, orphan: bool, expected: float) -> None:
    async with seed_engine(backend, orphan=orphan) as eng:
        resp = await run(eng, metrics=[{"metric": "sum(orders.amount) / count_distinct(customers.customer_id)",
                                        "alias": "v"}])
    assert table(resp, "v") == rows((expected,))


async def test_unrelated_metrics_without_dimensions_return_one_row(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, metrics=[REV, TIX])
    assert table(resp, REV, TIX) == rows((650, 3))


@pytest.mark.parametrize("extra", [
    pytest.param({"filter": ["sum(returns.amount) >= 0 or orders.total_revenue > 0"]}, id="measure-filter"),
    pytest.param({"order": [{"field": "sum(returns.amount)", "direction": "desc"}]}, id="order-entry"),
    pytest.param({"dimensions": [R, {"field": "case when sum(returns.amount, partition_by=[customers.region]) > 0 "
                                              "then 'y' else 'n' end", "alias": "has_returns"}]},
                 id="computed-dimension"),
])
async def test_only_metrics_supply_rows(backend: str, extra: dict) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, **{"dimensions": [R], "metrics": [REV], **extra})
    assert {r[R] for r in resp.data} == {"EAST", "WEST"}


async def test_a_metrics_only_query_no_row_passes_returns_one_null_row(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, metrics=[REV], filter=["1 = 0"])
    assert table(resp, REV) == rows((None,))


async def test_dimension_only_rows_come_from_the_anchor(backend: str) -> None:
    async with seed_engine(backend) as eng:
        plain = await run(eng, dimensions=[R])
        filtered = await run(eng, dimensions=[R], filter=["orders.amount > 100"])
    assert table(plain, R) == rows(("EAST",), ("WEST",), ("NORTH",))
    assert table(filtered, R) == rows(("EAST",), ("WEST",))
    assert filtered.columns == [R]
    assert plain.population == filtered.population == "customers"


async def test_a_filter_never_brings_rows_back(backend: str) -> None:
    async with seed_engine(backend) as eng:
        grouped = await run(eng, dimensions=[R], metrics=[REV], filter=["orders.amount > 100"])
        by_order = await run(eng, dimensions=["orders.order_id", R], metrics=[REV], filter=["orders.amount > 100"])
        none = await run(eng, dimensions=[R], metrics=[REV], filter=["1 = 0"])
    assert table(grouped, R, REV) == rows(("EAST", 200), ("WEST", 300))
    assert table(by_order, "orders.order_id", R) == rows(("O2", "EAST"), ("O4", "WEST"))
    assert none.data == []


async def test_a_filter_restricts_every_home(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[REV, RET], filter=["orders.amount > 100"])
    assert table(resp, R, REV, RET) == rows(("EAST", 200, 25), ("WEST", 300, None))
    assert RET in warned_metrics(resp, "semi_join_pushed")
    assert resp.population == "customers"


async def test_a_cross_dataset_filter(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[REV],
                         filter=["orders.status = 'completed' and customers.region = 'EAST'"])
    assert table(resp, R, REV) == rows(("EAST", 150),)


async def test_an_unrelated_metric_supplies_the_anchors_cells(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[REV, TIX])
    assert table(resp, R, REV, TIX) == rows(("EAST", 350, 3), ("WEST", 300, 3), ("NORTH", None, 3))
    assert TIX in warned_metrics(resp, "associated")
    assert TIX in warned_metrics(resp, "unrelated_dataset")
    assert not kinds(resp, "broadcast")


async def test_an_associated_metric_supplies_the_tuples_its_home_rows_reach(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=["orders.status"], metrics=["customers.customer_count"])
    assert table(resp, "orders.status", "customers.customer_count") == rows(
        ("completed", 2), ("open", 2), (None, 1))
    assert kinds(resp, "associated")


async def test_a_partitioned_metric_supplies_its_own_cells_extended_by_the_anchor(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R, "customers.segment"],
                         metrics=[{"metric": "sum(orders.amount, partition_by=[customers.region])", "alias": "p"}])
    assert table(resp, R, "customers.segment", "p") == rows(
        ("EAST", "retail", 350), ("EAST", "corporate", 350), ("WEST", "retail", 300))


async def test_a_model_filtered_dimension_row_does_not_exist(backend: str) -> None:
    datasets = seed_datasets(customers=customers_dataset(filters=["region <> 'WEST'"]))
    async with seed_engine(backend, datasets=datasets) as eng:
        resp = await run(eng, dimensions=[R], metrics=[REV])
    assert table(resp, R, REV) == rows(("EAST", 350), (None, 300))


async def test_a_policy_hidden_dimension_row_does_not_exist(backend: str) -> None:
    async with seed_engine(backend) as eng, with_policy(eng, HIDE_C3) as hidden:
        resp = await run(hidden, dimensions=[R], metrics=[REV])
    assert table(resp, R, REV) == rows(("EAST", 350), (None, 300))


# --------------------------------------------------------------------------- #
# Producer forms
# --------------------------------------------------------------------------- #
#: form → (extra query fields, metric, empty value, DuckDB only).
FORMS: dict[str, tuple[dict[str, Any], str, Any, bool]] = {
    "ranked": ({}, "last(orders.amount, orders.order_id)", None, False),
    "ranked_first": ({}, "first(orders.amount, orders.order_id)", None, False),
    "windowed": ({"time_dimensions": MONTH}, "sum(orders.amount, window='90d')", None, False),
    "shifted": ({"time_dimensions": MONTH}, "time_shift(sum(orders.amount), -1)", None, False),
    "second_order": ({}, "max(sum(orders.amount, partition_by=[orders.customer_id]))", None, False),
    "list_child": ({}, "count(baskets.items.*)", 0, True),
    "keyless": ({}, "sum(notes.amount)", None, False),
}


def _keyed(resp, dims: list[str]) -> dict[tuple, dict]:
    out = {tuple(r[d] for d in dims): r for r in resp.data}
    assert len(out) == len(resp.data), f"duplicate keys in {resp.data}"
    return out


def _same(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return float(a) == pytest.approx(float(b))


@pytest.mark.parametrize("form", list(FORMS))
async def test_producer_forms_supply_rows_alike(backend: str, form: str) -> None:
    extra, metric, empty, duckdb_only = FORMS[form]
    if duckdb_only and backend != "duckdb":
        pytest.skip("list columns need native arrays")
    statements = BASKETS_STATEMENTS if backend == "duckdb" else ()
    extra_ds = (notes_dataset(), baskets_dataset()) if backend == "duckdb" else (notes_dataset(),)
    dims = [R, *(["orders.ordered_at"] if "time_dimensions" in extra else [])]
    m = {"metric": metric, "alias": "m"}
    async with seed_engine(backend, orphan=True, extra_tables=(NOTES,), extra_statements=statements,
                           datasets=seed_datasets(extra=extra_ds)) as eng:
        both = _keyed(await run(eng, dimensions=[R], metrics=[m, RET], **extra), dims)
        alone = _keyed(await run(eng, dimensions=[R], metrics=[m], **extra), dims)
        returns = _keyed(await run(eng, dimensions=[R], metrics=[RET], **extra), dims)
    assert set(both) == set(alone) | set(returns), form
    assert any(key[0] is None for key in alone), f"{form}: the orphan reaches no NULL tuple"
    for key, row in both.items():
        assert _same(row["m"], alone[key]["m"] if key in alone else empty), (form, key)
        assert _same(row[RET], returns[key][RET] if key in returns else None), (form, key)


async def test_an_absent_cell_and_an_all_null_cell_are_alike(backend: str) -> None:
    async with seed_engine(backend, null_returns=True) as eng:
        resp = await run(eng, dimensions=[R], metrics=[REV, {"metric": "sum(returns.amount)", "alias": "r"}],
                         filter=["sum(returns.amount) is null"])
    assert table(resp, R, REV, "r") == rows(("WEST", 300, None), ("SOUTH", 40, None))


# --------------------------------------------------------------------------- #
# Key types across row sources
# --------------------------------------------------------------------------- #
KDIM = TableSpec(name="kdim", columns=[("key_id", "TEXT"), ("k_dec", "DECIMAL"), ("k_ts", "TIMESTAMP"),
                                       ("k_txt", "TEXT")],
                 rows=[("K1", 1.5, "2025-01-01 10:00:00", "a"), ("K2", None, "2025-02-01 00:00:00", "b")])
FACTA = TableSpec(name="facta", columns=[("a_id", "TEXT"), ("key_id", "TEXT"), ("x", "DOUBLE")],
                  rows=[("A1", "K1", 1.0), ("A2", "K2", 2.0)])
FACTB = TableSpec(name="factb", columns=[("b_id", "TEXT"), ("key_id", "TEXT"), ("y", "DOUBLE")],
                  rows=[("B1", "K9", 5.0)])


def _key_datasets() -> list[SlayerDataset]:
    def fact(name: str, pk: str, value: str) -> SlayerDataset:
        return SlayerDataset(
            name=name, sql_table=name, data_source=DS,
            fields=[DatasetField(name=pk, datatype=DataType.STRING, primary_key=True),
                    DatasetField(name="key_id", datatype=DataType.STRING),
                    DatasetField(name=value, datatype=DataType.FLOAT)],
            relationships=[DatasetRelationship(to="kdim", from_columns=["key_id"], to_columns=["key_id"])],
        )
    kdim = SlayerDataset(
        name="kdim", sql_table="kdim", data_source=DS,
        fields=[DatasetField(name="key_id", datatype=DataType.STRING, primary_key=True),
                DatasetField(name="k_dec", datatype=DataType.DECIMAL),
                DatasetField(name="k_ts", datatype=DataType.DATETIME),
                DatasetField(name="k_txt", datatype=DataType.STRING)],
    )
    return [*seed_datasets(), kdim, fact("facta", "a_id", "x"), fact("factb", "b_id", "y")]


async def test_key_types_agree_across_row_sources(backend: str) -> None:
    keys = ["kdim.k_dec", "kdim.k_ts", "kdim.k_txt"]
    async with seed_engine(backend, extra_tables=(KDIM, FACTA, FACTB), datasets=_key_datasets()) as eng:
        resp = await run(eng, dimensions=keys, metrics=[{"metric": "sum(facta.x)", "alias": "x"},
                                                       {"metric": "sum(factb.y)", "alias": "y"}])
    assert len(resp.data) == 3
    assert [r["y"] for r in resp.data if all(r[k] is None for k in keys)] == [5.0]
    for k in keys:
        types = {type(r[k]) for r in resp.data if r[k] is not None}
        assert len(types) == 1, (k, types)


# --------------------------------------------------------------------------- #
# A query naming ``population`` keeps its rows
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("query, orphan, expected", [
    pytest.param({"population": "customers", "dimensions": [R], "metrics": [REV]}, False,
                 [("EAST", 350), ("WEST", 300), ("NORTH", None)], id="customers-revenue"),
    pytest.param({"population": "customers", "dimensions": [R], "metrics": [RET]}, False,
                 [("EAST", 25), ("WEST", None), ("NORTH", 10)], id="customers-returns"),
    pytest.param({"population": "orders", "dimensions": [R], "metrics": [REV]}, True,
                 [("EAST", 350), ("WEST", 300), (None, 70)], id="orders-orphan"),
    pytest.param({"population": "customers", "dimensions": [R], "metrics": [REV]}, True,
                 [("EAST", 350), ("WEST", 300), ("NORTH", None)], id="customers-orphan"),
    pytest.param({"population": "customers", "dimensions": [R]}, False,
                 [("EAST",), ("WEST",), ("NORTH",)], id="customers-dims-only"),
    pytest.param({"population": "orders", "dimensions": [R], "metrics": [REV], "filter": ["orders.amount > 100"]},
                 False, [("EAST", 200), ("WEST", 300)], id="orders-filtered"),
])
async def test_a_named_population_keeps_its_rows(backend: str, query: dict, orphan: bool,
                                                 expected: list[tuple]) -> None:
    async with seed_engine(backend, orphan=orphan) as eng:
        resp = await run(eng, **query)
    keys = [R, *([REV] if REV in query.get("metrics", []) else []), *([RET] if RET in query.get("metrics", []) else [])]
    assert table(resp, *keys) == rows(*expected)
    assert resp.population == query["population"]
    assert resp.population_inferred is False


async def test_a_dimension_under_two_aliases_projects_both(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[{"field": R, "alias": "r1"}, {"field": R, "alias": "r2"}],
                         metrics=[REV, RET])
    assert table(resp, "r1", "r2", REV, RET) == rows(
        ("EAST", "EAST", 350, 25), ("NORTH", "NORTH", None, 10), ("WEST", "WEST", 300, None))


@pytest.mark.parametrize(("condition", "expected"), [
    ("rev > 320", [("EAST", 350, 3)]),
    ("n = 0", [("NORTH", None, 0)]),
])
async def test_a_measure_filter_removes_supplied_rows(backend: str, condition: str, expected: list) -> None:
    # A key one branch supplies never resurfaces a row the measure filter removed; an absent cell reads its empty value.
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], filter=[condition], metrics=[
            {"metric": "sum(orders.amount)", "alias": "rev"}, {"metric": "count(orders.*)", "alias": "n"}, RET])
    assert table(resp, R, "rev", "n") == rows(*expected)


@pytest.mark.parametrize("region", [R, {"field": R, "alias": "region"}])
async def test_a_coarser_term_restricts_the_anchor_on_an_aliased_dimension(backend: str, region: Any) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[region, "customers.segment"],
                         metrics=[{"metric": "sum(orders.amount, partition_by=[customers.region])", "alias": "p"}])
    assert table(resp, resp.columns[0], "customers.segment", "p") == rows(
        ("EAST", "corporate", 350), ("EAST", "retail", 350), ("WEST", "retail", 300))


@pytest.mark.parametrize("metrics", [["ORDERS.total_revenue"], ["sum(ORDERS.amount)"], ["ORDERS.total_revenue", RET]])
async def test_an_unquoted_head_supplies_rows_in_any_case(backend: str, metrics: list) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=metrics)
        unit = await run(eng, metrics=metrics[:1])
    assert sorted(r[R] for r in resp.data) == (["EAST", "NORTH", "WEST"] if RET in metrics else ["EAST", "WEST"])
    assert unit.row_count == 1


@pytest.mark.parametrize("head", ["orders", "ORDERS"])
async def test_row_supply_cells_read_the_stage_variables(backend: str, head: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[REV, RET], filter=[f"{head}.status = '{{s}}'"],
                         variables={"s": "completed"})
    assert table(resp, R, REV, RET) == rows(("EAST", 150, 25))


@pytest.mark.parametrize("metric", ["customers.customer_count", "sum(orders.amount)"])
@pytest.mark.parametrize("spine", [False, True])
async def test_a_product_anchor_beside_a_metric_of_its_first_group_is_refused(
        backend: str, metric: str, spine: bool) -> None:
    # The metric's home reaches no path to tickets; DEV-2390 will compute it at its reachable grain instead.
    spine_td = [{"field": "time_spine.timestamp", "granularity": "month", "date_range": ["2025-01-01", "2025-02-28"]}]
    async with seed_engine(backend) as eng:
        with pytest.raises(UnrelatedDatasetError):
            await run(eng, dimensions=[R, "tickets.priority"], metrics=[metric],
                      **({"time_dimensions": spine_td} if spine else {}))


async def test_a_product_anchor_beside_a_metric_of_another_group_answers(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R, "tickets.priority"], metrics=[TIX])
    assert {(r[R], r["tickets.priority"], r[TIX]) for r in resp.data} == {
        (region, p, n) for region in ("EAST", "NORTH", "WEST") for p, n in (("high", 1), ("low", 2))}


@pytest.mark.xfail(strict=True, reason="DEV-2388: the source binds from the host orders, walking back into it")
@pytest.mark.parametrize("extra", [[], [RET]])
async def test_a_metric_written_through_another_anchor_supplies_rows(backend: str, extra: list) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R], metrics=[{"metric": "sum(customers.orders.amount)", "alias": "s"}, *extra])
    assert {r[R]: r["s"] for r in resp.data} == {"EAST": 350, "WEST": 300, **({"NORTH": None} if extra else {})}


async def test_a_filter_on_a_dimension_alias_restricts_the_supply(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[{"field": R, "alias": "region"}], metrics=[REV, RET],
                         filter=["region = 'EAST'"])
    assert table(resp, "region", REV, RET) == rows(("EAST", 350, 25))


@pytest.mark.parametrize(("text", "expected"), [
    ("region = 'EAST'", "customers.region = 'EAST'"),
    ("region <> 'region' AND customers.region IS NOT NULL", "customers.region <> 'region' AND customers.region IS NOT NULL"),
    ("upper(region) = 'X'", "upper(customers.region) = 'X'"),
    ('"region" = 1', '"region" = 1'),
    ("region(x) = 1", "region(x) = 1"),
    ("region = = 1", "region = = 1"),
    ("region = {region}", "customers.region = {region}"),
], ids=["bare", "string-and-path", "argument", "quoted", "call", "unparseable", "placeholder"])
def test_respell_bare_names_replaces_only_bare_references(text: str, expected: str) -> None:
    assert respell_bare_names(text, respell={"region": "customers.region"}.get) == expected


async def test_a_supplying_hosts_own_filter_never_restricts_another_home(backend: str) -> None:
    orders = orders_dataset(filters=["status = 'open'"])
    async with seed_engine(backend, datasets=seed_datasets(orders=orders)) as eng:
        resp = await run(eng, dimensions=[R], metrics=[REV, RET])
    assert table(resp, R, REV, RET) == rows(("EAST", 200, 25), ("NORTH", None, 10), ("WEST", 300, None))


async def test_a_filter_on_a_computed_dimension_alias_restricts_the_supply(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[{"field": "upper(customers.region)", "alias": "region"}], metrics=[REV, RET],
                         filter=["region = 'EAST'"])
    assert table(resp, "region", REV, RET) == rows(("EAST", 350, 25))


async def test_a_coarser_term_restricts_on_a_dimension_under_two_aliases(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[{"field": R, "alias": "a"}, {"field": R, "alias": "b"}, "customers.segment"],
                         metrics=[{"metric": "sum(orders.amount, partition_by=[customers.region])", "alias": "p"}])
    assert table(resp, "a", "b", "customers.segment", "p") == rows(
        ("EAST", "EAST", "corporate", 350), ("EAST", "EAST", "retail", 350), ("WEST", "WEST", "retail", 300))
