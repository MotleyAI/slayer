"""Declared cardinality never changes a value; trusted hops (spec: queries/semantics › Declared cardinality never
changes a value, › An aggregate input across an untrusted hop counts its home row once; models/join-cardinality ›
Provable to-one arity per orientation, › Trusted hops).

Seeds: ``tests/_dev2437_fixtures.py``.
"""

from __future__ import annotations

import importlib
from collections.abc import Callable
from datetime import date, datetime
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType, JoinType, RelationshipCardinality
from slayer.core.errors import UnsafeRelationshipInputError
from slayer.core.models import Aggregation, AggregationParam, DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.policy import ColumnFilterRuleset, SessionPolicy
from slayer.core.query import SlayerQuery
from slayer.core.relationship_walker import OrientedRelationship, list_child
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.sql.dialects.base import LIST_POSITION

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev1948_fixtures import AMOUNT_BY_TIER, chain_list, make_dev1948_engine, rows_by
from tests._dev2212_fixtures import with_policy
from tests._engine_helpers import seeded_exec_engine
from tests._dev2437_fixtures import (
    BACKENDS,
    C3_TWO_GROUPS,
    CLEAN,
    DATED_ORDERS,
    DS,
    DUP_C1,
    REGION,
    REVENUE,
    SPLIT_C1,
    customers_dataset,
    engine,
    joins_customers_inline,
    orders_dataset,
    region_revenue,
    retail_engine,
    returns_dataset,
    seed_datasets,
    sql_of,
    to_customers,
)

REGION_REVENUE: dict[str, Any] = {"dimensions": [REGION], "metrics": [REVENUE]}
EAST_FILTER = "customers.region = 'EAST'"


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


def q(**kw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate(kw)


async def run(eng: SlayerQueryEngine, **kw: Any) -> SlayerResponse:
    return await eng.execute(q(**kw))


def non_deprecation_warnings(resp: SlayerResponse) -> list[Any]:
    return [w for w in resp.warnings or [] if getattr(w, "kind", None) != "deprecated_input"]


def norm(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(sorted(map(repr, value)))
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return round(float(value), 6)
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    return value


def row_set(resp: SlayerResponse) -> list[tuple]:
    """Every row as a tuple over the response columns, values normalised, sorted."""
    return sorted((tuple(norm(r[c]) for c in resp.columns) for r in resp.data), key=repr)


def by_region(resp: SlayerResponse, key: str) -> dict[Any, Any]:
    return {r[REGION]: norm(r[key]) for r in resp.data}


# --------------------------------------------------------------------------- #
# queries/semantics › Declared cardinality never changes a value
# --------------------------------------------------------------------------- #
async def test_a_duplicated_dimension_row_counts_once_per_group(backend: str) -> None:
    async with engine(backend, DUP_C1) as eng:
        resp = await run(eng, **REGION_REVENUE)
    got = region_revenue(resp)
    assert (got["EAST"], got["WEST"]) == (350.0, 300.0)
    assert non_deprecation_warnings(resp) == []


async def test_a_filter_across_a_duplicated_row_counts_once(backend: str) -> None:
    async with engine(backend, DUP_C1) as eng:
        resp = await run(eng, metrics=[REVENUE], filter=[EAST_FILTER])
    assert [float(r[REVENUE]) for r in resp.data] == [350.0]


async def test_the_duplicated_value_is_reachable_only_by_trusting(backend: str) -> None:
    async with engine(backend, DUP_C1, trusted=True) as eng:
        resp = await run(eng, **REGION_REVENUE)
    assert region_revenue(resp)["EAST"] == 650.0


async def test_a_row_in_two_groups_counts_in_each(backend: str) -> None:
    async with engine(backend, C3_TWO_GROUPS) as eng:
        resp = await run(eng, **REGION_REVENUE)
    got = region_revenue(resp)
    assert (got["SOUTH"], got["WEST"]) == (300.0, 300.0)


async def test_clean_data_does_not_depend_on_trust(backend: str) -> None:
    async with engine(backend, CLEAN, trusted=True) as eng:
        trusted = await run(eng, **REGION_REVENUE)
    async with engine(backend, CLEAN) as eng:
        untrusted = await run(eng, **REGION_REVENUE)
    assert row_set(trusted) == row_set(untrusted)


async def test_a_computed_dimension_counts_each_distinct_value_once(backend: str) -> None:
    coast = "CASE WHEN customers.region IN ('EAST', 'WEST') THEN 'coast' END"
    async with engine(backend, SPLIT_C1) as eng:
        resp = await run(eng, dimensions=[{"field": coast, "alias": "g"}], metrics=[REVENUE])
    assert {r["g"]: float(r[REVENUE]) for r in resp.data}["coast"] == 650.0


async def test_a_filter_and_a_dimension_on_one_dataset_bind_the_same_row(backend: str) -> None:
    async with engine(backend, SPLIT_C1) as eng:
        resp = await run(eng, **REGION_REVENUE, filter=["customers.tier = 'gold'"])
    got = region_revenue(resp)
    assert (got["EAST"], got["WEST"]) == (300.0, 300.0)
    assert not joins_customers_inline(resp)
    assert "SELECT DISTINCT" in sql_of(resp)


async def test_error_mode_accepts_an_untrusted_declared_dimension(backend: str) -> None:
    async with engine(backend, DUP_C1) as eng:
        resp = await run(eng, **REGION_REVENUE, to_many_handling="error")
    assert region_revenue(resp)["EAST"] == 350.0


async def test_a_partition_key_across_an_untrusted_hop_is_admitted(backend: str) -> None:
    async with engine(backend, DUP_C1) as eng:
        resp = await run(eng, population="orders", dimensions=["orders.order_id", REGION], to_many_handling="error",
                         metrics=[{"metric": "sum(orders.amount, partition_by=[customers.region])", "alias": "t"}])
    assert {r["orders.order_id"]: float(r["t"]) for r in resp.data} == {
        "O1": 350.0, "O2": 350.0, "O3": 350.0, "O4": 300.0}


@pytest.mark.parametrize("query", [
    pytest.param(REGION_REVENUE, id="dimension"),
    pytest.param({"metrics": [REVENUE], "filter": [EAST_FILTER]}, id="filter"),
])
async def test_the_imported_oracle_retail_model_counts_each_order_once(query: dict[str, Any]) -> None:
    async with retail_engine() as eng:
        resp = await eng.execute(q(**query))
    east = next(r for r in resp.data if r.get(REGION, "EAST") == "EAST")
    assert float(east[REVENUE]) == 350.0


# --------------------------------------------------------------------------- #
# queries/semantics › Every aggregation family counts the home row once
# --------------------------------------------------------------------------- #
MONTH = {"field": "orders.ordered_at", "granularity": "month"}

FAMILIES = [
    pytest.param({"metrics": [{"metric": "count(*)", "alias": "v"}]}, id="count-star"),
    pytest.param({"metrics": [{"metric": "avg(orders.amount)", "alias": "v"}]}, id="avg"),
    pytest.param({"metrics": [{"metric": "count_distinct(orders.order_id)", "alias": "v"}]}, id="count-distinct"),
    pytest.param({"metrics": [{"metric": "first(orders.amount, orders.ordered_at)", "alias": "v"}]}, id="first"),
    pytest.param({"metrics": [{"metric": "last(orders.amount, orders.ordered_at)", "alias": "v"}]}, id="last"),
    pytest.param({"metrics": [{"metric": "collect(orders.order_id)", "alias": "v"}]}, id="collect"),
    pytest.param({"metrics": [{"metric": "sum(orders.amount, window='60d')", "alias": "v"}],
                  "time_dimensions": [MONTH]}, id="windowed"),
    pytest.param({"metrics": [{"metric": "rank(sum(orders.amount), direction='desc')", "alias": "v"},
                              {"metric": "sum(orders.amount)", "alias": "s"}]}, id="ranked"),
    pytest.param({"metrics": [{"metric": "sum(orders.amount) FILTER (WHERE orders.status = 'open')", "alias": "v"}]},
                 id="aggregate-filter"),
]


@pytest.mark.parametrize("query", FAMILIES)
async def test_every_aggregation_family_counts_the_home_row_once(backend: str, query: dict[str, Any]) -> None:
    if backend == "sqlite" and "collect(" in repr(query):
        pytest.skip("SQLite has no native lists")
    kw = {"population": "orders", "dimensions": [REGION], **query}
    async with engine(backend, DUP_C1) as eng:
        dirty = await run(eng, **kw)
    async with engine(backend, CLEAN) as eng:
        clean = await run(eng, **kw)
    assert row_set(dirty) == row_set(clean)


# --------------------------------------------------------------------------- #
# queries/semantics › An aggregate input across an untrusted hop counts its home row once
# --------------------------------------------------------------------------- #
WSUM = "wsum(orders.amount, weight=customers.weight)"
INPUT_ACROSS_HOP = [
    pytest.param(WSUM, id="parameter"),
    pytest.param("last(orders.amount, customers.weight)", id="ranking-key"),
    pytest.param("sum(orders.amount) FILTER (WHERE customers.tier = 'gold')", id="aggregate-filter-operand"),
]


def weighted_datasets(*, trusted: bool = False) -> list[SlayerDataset]:
    wsum = Aggregation(name="wsum", expression="SUM({value} * {weight})",
                       params=[AggregationParam(name="weight", sql="1")])
    return seed_datasets(orders=orders_dataset(trusted=trusted, aggregations=[wsum]))


async def test_a_weighted_sum_reads_one_weight_per_order(backend: str) -> None:
    async with engine(backend, DUP_C1, weighted_datasets()) as eng:
        resp = await run(eng, dimensions=[REGION], metrics=[{"metric": WSUM, "alias": "v"}])
    got = by_region(resp, "v")
    # C1's orders 100 and 200 each weighted 1 or 3; C2's 50 by 2.
    assert got["EAST"] in {400.0, 600.0, 800.0, 1000.0}
    assert got["WEST"] == 300.0


async def test_an_aggregate_filter_operand_across_the_hop_counts_once(backend: str) -> None:
    async with engine(backend, DUP_C1) as eng:
        resp = await run(eng, dimensions=[REGION], metrics=[
            {"metric": "sum(orders.amount) FILTER (WHERE customers.tier = 'gold')", "alias": "v"}])
    got = by_region(resp, "v")
    assert (got["EAST"], got["WEST"]) == (300.0, 300.0)


async def test_error_mode_refuses_a_population_input_read_back_across_the_hop(backend: str) -> None:
    # Home orders, population customers: each order reads its customer's weight back across orders → customers.
    async with engine(backend, DUP_C1, weighted_datasets()) as eng:
        with pytest.raises(UnsafeRelationshipInputError, match="orders → customers"):
            await run(eng, population="customers", metrics=[
                {"metric": "wsum(orders.amount, weight=customers.weight)", "alias": "v"}], to_many_handling="error")


async def test_error_mode_refuses_an_input_read_at_the_inputs_meet(backend: str) -> None:
    # Home orders (where the inputs meet), population customers: each order reads its customer's weight across the hop.
    async with engine(backend, DUP_C1, weighted_datasets()) as eng:
        with pytest.raises(UnsafeRelationshipInputError, match="orders → customers"):
            await run(eng, population="customers", metrics=[
                {"metric": "sum(customers.weight * orders.amount)", "alias": "v"}], to_many_handling="error")


@pytest.mark.parametrize("metric", INPUT_ACROSS_HOP)
async def test_error_mode_refuses_an_input_across_an_untrusted_hop(backend: str, metric: str) -> None:
    async with engine(backend, DUP_C1, weighted_datasets()) as eng:
        with pytest.raises(UnsafeRelationshipInputError) as raised:
            await run(eng, dimensions=[REGION], metrics=[{"metric": metric, "alias": "v"}], to_many_handling="error")
    text = str(raised.value)
    assert "orders" in text
    assert "customers" in text
    assert "trusted" in text
    assert "enforced" in text.lower()


@pytest.mark.parametrize("mode", [None, "associate", "error"])
async def test_a_trusted_hop_reads_the_input_as_before(backend: str, mode: Optional[str]) -> None:
    async with engine(backend, CLEAN, weighted_datasets(trusted=True)) as eng:
        resp = await run(eng, dimensions=[REGION], metrics=[{"metric": WSUM, "alias": "v"}],
                         **({"to_many_handling": mode} if mode else {}))
    got = by_region(resp, "v")
    assert (got["EAST"], got["WEST"]) == (400.0, 300.0)


# --------------------------------------------------------------------------- #
# The distinct-projection / association boundary: values and join shape
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(("predicate", "expected"), [
    pytest.param(f"{EAST_FILTER} OR orders.status = 'open'", 650.0, id="or"),
    pytest.param(f"NOT ({EAST_FILTER} AND orders.status = 'open')", 450.0, id="not"),
])
async def test_a_predicate_mixing_home_and_target_counts_once(backend: str, predicate: str, expected: float) -> None:
    async with engine(backend, DUP_C1) as eng:
        resp = await run(eng, metrics=[REVENUE], filter=[predicate])
    assert [float(r[REVENUE]) for r in resp.data] == [expected]
    assert not joins_customers_inline(resp)


@pytest.mark.parametrize(("dimensions", "expected"), [
    pytest.param([], {None: 600.0}, id="no-dimension"),
    pytest.param([REGION], {"EAST": 300.0, "WEST": 500.0}, id="same-branch-dimension"),
])
async def test_a_mixed_predicate_on_split_rows_counts_once(backend: str, dimensions: list[str],
                                                           expected: dict[Any, float]) -> None:
    # C1 is (EAST, gold) and (WEST, silver): O2 (open) meets both rows.
    predicate = "customers.tier = 'gold' OR orders.status = 'open'" if dimensions else (
        "customers.region = 'WEST' OR orders.status = 'open'")
    async with engine(backend, SPLIT_C1) as eng:
        resp = await run(eng, dimensions=dimensions, metrics=[REVENUE], filter=[predicate])
    assert {r.get(REGION): float(r[REVENUE]) for r in resp.data} == expected


@pytest.mark.parametrize("mode", [None, "error"])
async def test_a_dimension_mixing_home_and_target_counts_once(backend: str, mode: Optional[str]) -> None:
    # Both of C1's rows give O1 'completed' and O2 'open': each order counts once in its value.
    mixed = "CASE WHEN customers.region IN ('EAST', 'WEST') THEN orders.status END"
    async with engine(backend, SPLIT_C1) as eng:
        resp = await run(eng, dimensions=[{"field": mixed, "alias": "g"}], metrics=[REVENUE],
                         **({"to_many_handling": mode} if mode else {}))
    assert {r["g"]: float(r[REVENUE]) for r in resp.data} == {"completed": 150.0, "open": 500.0}
    assert non_deprecation_warnings(resp) == []


def two_branch_datasets() -> list[SlayerDataset]:
    managers = SlayerDataset(name="managers", sql_table="managers", data_source=DS,
                             fields=[DatasetField(name="manager_id", datatype=DataType.STRING, primary_key=True),
                                     DatasetField(name="name", datatype=DataType.STRING)])
    to_managers = DatasetRelationship(to="managers", from_columns=["manager_id"], to_columns=["manager_id"],
                                      cardinality=RelationshipCardinality.MANY_TO_ONE)
    orders = orders_dataset(relationships=[to_customers(), to_managers])
    return [customers_dataset(), orders, returns_dataset(), managers]


async def test_two_target_branches_count_each_order_once(backend: str) -> None:
    async with engine(backend, DUP_C1, two_branch_datasets()) as eng:
        resp = await run(eng, dimensions=[REGION, "managers.name"], metrics=[REVENUE])
    got = {(r[REGION], r["managers.name"]): float(r[REVENUE]) for r in resp.data}
    assert got == {("EAST", "mia"): 300.0, ("EAST", "max"): 50.0, ("WEST", "max"): 300.0}
    assert not joins_customers_inline(resp)


def managed_customers_datasets(*, trusted: bool) -> list[SlayerDataset]:
    """``customers → managers`` an INNER hop; ``orders → customers`` trusted or not."""
    managers = two_branch_datasets()[-1]
    to_managers = DatasetRelationship(to="managers", from_columns=["manager_id"], to_columns=["manager_id"],
                                      cardinality=RelationshipCardinality.MANY_TO_ONE, join_type=JoinType.INNER)
    return [customers_dataset(relationships=[to_managers]), orders_dataset(trusted=trusted), returns_dataset(),
            managers]


async def test_an_inner_hop_beyond_the_untrusted_hop_drops_unmatched_orders(backend: str) -> None:
    # C3's manager M9 does not exist: O4 drops whether or not the hop is trusted.
    seed = [(c, r, "M9" if c == "C3" else m, t, w) for c, r, m, t, w in CLEAN]
    rows = {}
    for trusted in (True, False):
        async with engine(backend, seed, managed_customers_datasets(trusted=trusted)) as eng:
            resp = await run(eng, dimensions=["customers.managers.name"], metrics=[REVENUE])
        rows[trusted] = {r["customers.managers.name"]: float(r[REVENUE]) for r in resp.data}
    assert rows[False] == rows[True] == {"mia": 350.0}


@pytest.mark.parametrize("predicate", [
    pytest.param("customers.region IS NOT DISTINCT FROM NULL", id="null-safe-equality"),
    pytest.param("customers.region IS NULL", id="is-null"),
    pytest.param("COALESCE(customers.region, 'none') = 'none'", id="coalesce"),
])
async def test_a_null_keeping_filter_keeps_an_unmatched_order(backend: str, predicate: str) -> None:
    # C3 is missing: O4 (300) meets no customer and its NULL region passes the filter.
    async with engine(backend, [c for c in CLEAN if c[0] != "C3"]) as eng:
        resp = await run(eng, metrics=[REVENUE], filter=[predicate])
    assert [float(r[REVENUE]) for r in resp.data] == [300.0]


@pytest.mark.parametrize("predicate", [
    pytest.param("customers.tier IN ('gold', 'silver') OR customers.tier IS NULL", id="or-is-null"),
    pytest.param("COALESCE(customers.tier, 'x') <> 'y'", id="coalesce"),
])
async def test_a_null_keeping_filter_on_differing_rows_counts_once(backend: str, predicate: str) -> None:
    # C1 is both gold and silver; both rows pass: each order counts once.
    async with engine(backend, [*CLEAN, ("C1", "EAST", "M1", "silver", 1.0)]) as eng:
        resp = await run(eng, metrics=[REVENUE], filter=[predicate])
    assert [float(r[REVENUE]) for r in resp.data] == [650.0]


async def test_an_arithmetic_filter_on_the_target_filters_inside_the_projection(backend: str) -> None:
    async with engine(backend, DUP_C1) as eng:
        resp = await run(eng, metrics=[REVENUE], filter=["customers.weight * 2 + 1 > 2"])
    assert [float(r[REVENUE]) for r in resp.data] == [650.0]
    assert "__slayer_p" not in sql_of(resp)


async def test_a_target_column_named_like_an_internal_alias_reads_its_own_value(backend: str) -> None:
    # A physical column ``_slayer_p``, grouped beside a null-keeping filter: C1 (twice) and C3 are 'x'.
    customers = TableSpec(name="customers", columns=[("customer_id", "TEXT"), ("region", "TEXT"), ("_slayer_p", "TEXT")],
                          rows=[("C1", "EAST", "x"), ("C1", "EAST", "x"), ("C2", "EAST", "y"), ("C3", "WEST", "x")])
    datasets = [customers_dataset(fields=[DatasetField(name="_slayer_p", datatype=DataType.STRING)]),
                orders_dataset(), returns_dataset()]
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, [customers, DATED_ORDERS]),
                                  datasets=datasets) as (eng, _):
        resp = await run(eng, dimensions=["customers._slayer_p"], metrics=[REVENUE],
                         filter=["customers.region IS NULL OR customers._slayer_p = 'x'"])
    assert {r["customers._slayer_p"]: float(r[REVENUE]) for r in resp.data} == {"x": 600.0}


@pytest.mark.parametrize(("threshold", "expected"), [
    pytest.param(320, {"big": 650.0}, id="same-band"),
    pytest.param(400, {"big": 600.0, "small": 350.0}, id="split-bands"),
])
async def test_a_dimension_attached_through_a_duplicated_row_counts_once_per_value(
        backend: str, threshold: int, expected: dict[str, float]) -> None:
    # C1 is gold and silver: gold totals 600, silver 350. C1's orders count once in each band its rows reach.
    seed = [*CLEAN, ("C1", "EAST", "M1", "silver", 1.0)]
    band = {"field": f"CASE WHEN sum(orders.amount, partition_by=customers.tier) > {threshold} THEN 'big' "
                     "ELSE 'small' END", "alias": "band"}
    async with engine(backend, seed) as eng:
        resp = await run(eng, dimensions=[band], metrics=[REVENUE])
    assert {r["band"]: float(r[REVENUE]) for r in resp.data} == expected


async def test_an_input_reading_two_target_columns_reads_one_row(backend: str) -> None:
    # C1 is (EAST, weight 3) and (WEST, weight 1): C1's orders (300) contribute 0 or 300, never region and weight mixed.
    seed = [("C1", "EAST", "M1", "gold", 3.0), ("C1", "WEST", "M1", "gold", 1.0), *CLEAN[1:]]
    metric = "sum(orders.amount * CASE WHEN customers.region = 'WEST' THEN customers.weight ELSE 0 END)"
    async with engine(backend, seed) as eng:
        resp = await run(eng, metrics=[{"metric": metric, "alias": "m"}], filter=["orders.customer_id = 'C1'"])
    assert [float(r["m"]) for r in resp.data] in ([0.0], [300.0])


async def test_a_null_keeping_filter_drops_a_matched_order_whose_rows_all_fail(backend: str) -> None:
    # C2 is silver: O3 meets a customer the filter rejects, so its NULL-row reading never applies.
    async with engine(backend, CLEAN) as eng:
        resp = await run(eng, metrics=[REVENUE], filter=["customers.tier IS NULL OR customers.tier = 'gold'"])
    assert [float(r[REVENUE]) for r in resp.data] == [600.0]


async def test_a_dimension_alone_lowers_through_the_distinct_projection(backend: str) -> None:
    async with engine(backend, DUP_C1) as eng:
        resp = await run(eng, **REGION_REVENUE)
    assert not joins_customers_inline(resp)
    assert "SELECT DISTINCT" in sql_of(resp)


async def test_a_target_model_filter_sits_inside_the_projection(backend: str) -> None:
    datasets = seed_datasets(customers=customers_dataset(filters=["tier = 'gold'"]))
    async with engine(backend, DUP_C1, datasets) as eng:
        resp = await run(eng, **REGION_REVENUE)
    got = region_revenue(resp)
    assert (got["EAST"], got["WEST"]) == (300.0, 300.0)
    assert not joins_customers_inline(resp)
    assert "SELECT DISTINCT" in sql_of(resp)


async def test_row_level_security_on_the_target_sits_inside_the_projection(backend: str) -> None:
    policy = SessionPolicy(ruleset=ColumnFilterRuleset(column="tier", value="gold", on_unapplicable="pass"))
    async with engine(backend, DUP_C1) as eng, with_policy(eng, policy) as scoped:
        resp = await run(scoped, **REGION_REVENUE)
    got = region_revenue(resp)
    assert (got["EAST"], got["WEST"]) == (300.0, 300.0)
    assert not joins_customers_inline(resp)
    assert "SELECT DISTINCT" in sql_of(resp)


# --------------------------------------------------------------------------- #
# models/join-cardinality › Provable to-one arity per orientation, › Trusted hops
# --------------------------------------------------------------------------- #
async def test_a_declaration_alone_never_skips_de_duplication(backend: str) -> None:
    async with engine(backend, DUP_C1) as eng:
        resp = await run(eng, **REGION_REVENUE)
    assert region_revenue(resp)["EAST"] == 350.0
    assert non_deprecation_warnings(resp) == []


async def test_a_trusted_relationship_keeps_the_inline_join(backend: str) -> None:
    async with engine(backend, CLEAN, trusted=True) as eng:
        trusted = await run(eng, **REGION_REVENUE)
    async with engine(backend, CLEAN) as eng:
        untrusted = await run(eng, **REGION_REVENUE)
    assert row_set(trusted) == row_set(untrusted)
    assert joins_customers_inline(trusted)


def key_proven_datasets() -> list[SlayerDataset]:
    return seed_datasets(orders=orders_dataset(relationship=to_customers(trusted=True, cardinality=None)))


def reverse_datasets(*, trusted: bool) -> list[SlayerDataset]:
    data: dict[str, Any] = {"to": "orders", "from_columns": ["customer_id"], "to_columns": ["customer_id"],
                            "cardinality": RelationshipCardinality.ONE_TO_MANY}
    if trusted:
        data["trusted"] = True
    customers = customers_dataset(relationships=[DatasetRelationship.model_validate(data)])
    return seed_datasets(customers=customers, orders=orders_dataset(relationships=[]))


def enforced_key_datasets() -> list[SlayerDataset]:
    return seed_datasets(customers=customers_dataset(enforced_keys=[["customer_id"]]))


@pytest.mark.parametrize("datasets", [
    pytest.param(key_proven_datasets, id="key-proven-orientation"),
    pytest.param(lambda: reverse_datasets(trusted=True), id="to-one-reverse-of-one-to-many"),
    pytest.param(enforced_key_datasets, id="enforced-target-key"),
])
async def test_a_trusted_hop_joins_inline(backend: str, datasets: Callable[[], list[SlayerDataset]]) -> None:
    async with engine(backend, CLEAN, datasets()) as eng:
        resp = await run(eng, **REGION_REVENUE)
    got = region_revenue(resp)
    assert (got["EAST"], got["WEST"]) == (350.0, 300.0)
    assert joins_customers_inline(resp)


async def test_the_untrusted_reverse_of_one_to_many_counts_once(backend: str) -> None:
    async with engine(backend, DUP_C1, reverse_datasets(trusted=False)) as eng:
        resp = await run(eng, **REGION_REVENUE)
    assert region_revenue(resp)["EAST"] == 350.0


async def test_a_vacuous_trusted_flag_is_accepted_and_inert(backend: str) -> None:
    def datasets(*, trusted: bool) -> list[SlayerDataset]:
        rel = to_customers(trusted=trusted, cardinality=RelationshipCardinality.MANY_TO_MANY)
        return seed_datasets(customers=customers_dataset(keyed=False), orders=orders_dataset(relationship=rel))

    async with engine(backend, DUP_C1, datasets(trusted=True)) as eng:
        flagged = await run(eng, **REGION_REVENUE)
    async with engine(backend, DUP_C1, datasets(trusted=False)) as eng:
        plain = await run(eng, **REGION_REVENUE)
    assert row_set(flagged) == row_set(plain)
    assert sql_of(flagged) == sql_of(plain)


@pytest.fixture(params=BACKENDS)
async def stage_engine(request: pytest.FixtureRequest):
    async for eng in make_dev1948_engine(request):
        yield eng


async def test_a_stage_grain_is_trusted(stage_engine: SlayerQueryEngine) -> None:
    stages: list[SlayerQuery | dict] = [*chain_list()]
    resp = await stage_engine.execute(stages)
    assert rows_by(resp.data, key="b.c__tr", value="total") == AMOUNT_BY_TIER
    assert "SELECT DISTINCT" not in sql_of(resp).upper()


PASS_THROUGH_STAGES: list[dict[str, Any]] = [
    {"name": "s", "population": "orders", "dimensions": ["orders.order_id", "orders.customer_id"],
     "metrics": [{"metric": "sum(orders.amount)", "alias": "amt"}]},
    {"population": "s", "dimensions": [REGION], "metrics": [{"metric": "sum(s.amt)", "alias": "total"}]},
]


async def test_a_stage_edge_inherited_on_pass_through_keys_carries_trust(backend: str) -> None:
    stages: list[SlayerQuery | dict] = [q(**s) for s in PASS_THROUGH_STAGES]
    async with engine(backend, CLEAN, trusted=True) as eng:
        trusted = await eng.execute(stages)
    async with engine(backend, DUP_C1) as eng:
        untrusted = await eng.execute(stages)
    assert joins_customers_inline(trusted)
    assert by_region(untrusted, "total")["EAST"] == 350.0


async def test_the_spine_instant_is_trusted(backend: str) -> None:
    spine = {"field": "time_spine.timestamp", "granularity": "month", "date_range": ["2025-01-01", "2025-03-31"]}
    async with engine(backend, CLEAN) as eng:
        resp = await run(eng, population="orders", time_dimensions=[spine], metrics=[REVENUE])
    assert [float(r[REVENUE]) for r in resp.data] == [150.0, 200.0, 300.0]
    assert "SELECT DISTINCT" not in sql_of(resp).upper()


async def test_a_keyless_home_counts_each_row_once(backend: str) -> None:
    keyless = orders_dataset()
    keyless = keyless.model_copy(update={"fields": [f.model_copy(update={"primary_key": False}) for f in keyless.fields]})
    async with engine(backend, DUP_C1, seed_datasets(orders=keyless)) as eng:
        resp = await run(eng, **REGION_REVENUE)
    got = region_revenue(resp)
    assert (got["EAST"], got["WEST"]) == (350.0, 300.0)


def trusted_to_one(*, edge: OrientedRelationship, target_dataset: SlayerDataset) -> bool:
    predicate = getattr(importlib.import_module("slayer.core.relationship_walker"), "trusted_to_one")
    return predicate(edge=edge, target_dataset=target_dataset)


def _tagged_parent() -> SlayerDataset:
    return SlayerDataset(name="posts", sql_table="posts", data_source=DS, fields=[
        DatasetField(name="post_id", datatype=DataType.STRING, primary_key=True),
        DatasetField(name="tags", datatype=DataType.ARRAY, element_type=DataType.STRING)])


def test_a_list_childs_hop_to_its_parent_is_trusted_by_construction() -> None:
    parent = _tagged_parent()
    child = list_child(parent=parent, field=parent.fields[1])
    walker = importlib.import_module("slayer.core.relationship_walker")
    back = walker._lateral_edge(parent=parent, field="tags", forward=False)
    assert back.source_dataset == child.name
    assert trusted_to_one(edge=back, target_dataset=parent)


def test_a_list_position_alone_is_not_a_constructed_key() -> None:
    parent = _tagged_parent()
    child = list_child(parent=parent, field=parent.fields[1])
    edge = OrientedRelationship(source_dataset="orders", to=child.name, column_pairs=[("line_no", LIST_POSITION)],
                                join_type=JoinType.LEFT, cardinality=RelationshipCardinality.MANY_TO_ONE, name=None,
                                declaring_dataset="orders")
    assert not trusted_to_one(edge=edge, target_dataset=child)
