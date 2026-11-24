"""Raw-row mode lists the rows of the named population, else of the dimension anchor read from dimensions only, and
every dimension must be determined by the population (spec: queries/population › Dimension-determined default ›
Raw-row mode infers from the dimensions without unproven hops; queries/semantics › Raw-row dimensions are determined
by the population; semantics Axiom 12.5)."""

from __future__ import annotations

from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import (
    DistinctDimensionValuesError,
    PopulationInferenceError,
    UnanalyzableDependencyError,
    UnrelatedDatasetError,
    UnsafeRelationshipInputError,
)
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

from tests._dev2212_fixtures import (
    BACKENDS,
    BASKETS_STATEMENTS,
    baskets_dataset,
    customers_dataset,
    orders_dataset,
    q,
    rows,
    run,
    seed_datasets,
    seed_engine,
    table,
    tickets_dataset,
    to_customers,
)

R, SEG = "customers.region", "customers.segment"
MONTH = {"field": "orders.ordered_at", "granularity": "month"}
MODES = [None, "associate", "error", "broadcast"]


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _orders_with(*fields: DatasetField, relationship: Optional[DatasetRelationship] = None) -> SlayerDataset:
    ds = orders_dataset(relationship=relationship)
    return ds.model_copy(update={"fields": [*ds.fields, *fields]})


def _unproven_datasets(*fields: DatasetField) -> list[SlayerDataset]:
    """``orders`` → ``customers`` with no declared cardinality and no covering key."""
    return seed_datasets(customers=customers_dataset(keyed=False),
                         orders=_orders_with(*fields, relationship=to_customers(cardinality=None)))


async def _raw(eng: SlayerQueryEngine, population: str, **kw: Any) -> SlayerResponse:
    return await run(eng, population=population, distinct_dimension_values=False, **kw)


def _names(exc: pytest.ExceptionInfo[DistinctDimensionValuesError], *, entry: str, hop: str) -> None:
    """The message names ``entry``, and ``hop`` outside it."""
    text = str(exc.value)
    assert entry in text, text
    assert hop in text.replace(entry, ""), text


async def test_raw_rows_of_a_named_population(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, population="customers", dimensions=[R], distinct_dimension_values=False)
    assert table(resp, R) == rows(("EAST",), ("EAST",), ("WEST",), ("NORTH",))


async def test_raw_rows_of_the_anchor(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R, SEG], distinct_dimension_values=False)
    assert table(resp, R, SEG) == rows(("EAST", "retail"), ("EAST", "corporate"), ("WEST", "retail"),
                                       ("NORTH", "retail"))
    assert resp.population == "customers"
    assert resp.population_inferred is True


async def test_a_finer_filter_restricts_the_anchor_rows_and_never_moves_it(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R, SEG], filter=["orders.amount > 40"], distinct_dimension_values=False)
    assert table(resp, R, SEG) == rows(("EAST", "retail"), ("EAST", "corporate"), ("WEST", "retail"))
    assert resp.population == "customers"


async def test_an_unknown_cardinality_hop_fails_closed_in_raw_rows(backend: str) -> None:
    datasets = seed_datasets(customers=customers_dataset(keyed=False),
                             orders=orders_dataset(relationship=to_customers(cardinality=None)))
    async with seed_engine(backend, datasets=datasets) as eng:
        grouped = await run(eng, dimensions=["orders.status", R])
        assert table(grouped, "orders.status", R) == rows(("completed", "EAST"), ("open", "EAST"), ("open", "WEST"))
        assert grouped.population == "orders"
        with pytest.raises(PopulationInferenceError):
            await run(eng, dimensions=["orders.status", R], distinct_dimension_values=False)


async def test_an_unconnected_group_fails_closed_in_raw_rows(backend: str) -> None:
    async with seed_engine(backend) as eng:
        with pytest.raises(PopulationInferenceError):
            await run(eng, dimensions=[R, "tickets.priority"], distinct_dimension_values=False)


# --------------------------------------------------------------------------- #
# Raw-row dimensions are determined by the population
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("mode", MODES)
async def test_a_fanning_dimension_under_a_named_population_is_refused(backend: str, mode: Optional[str]) -> None:
    extra = {} if mode is None else {"to_many_handling": mode}
    async with seed_engine(backend) as eng:
        with pytest.raises(DistinctDimensionValuesError) as exc:
            await _raw(eng, "customers", dimensions=["orders.amount"], **extra)
    _names(exc, entry="orders.amount", hop="orders")


async def test_a_pinned_population_cannot_bypass_determination(backend: str) -> None:
    async with seed_engine(backend) as eng:
        with pytest.raises(DistinctDimensionValuesError) as exc:
            await _raw(eng, "customers", population_inferred=True, dimensions=["orders.amount"])
    _names(exc, entry="orders.amount", hop="orders")


@pytest.mark.parametrize("expr", ["orders.amount * 2", "customers.region || orders.status"])
async def test_a_computed_dimension_over_a_fanning_field_is_refused(backend: str, expr: str) -> None:
    async with seed_engine(backend) as eng:
        with pytest.raises(DistinctDimensionValuesError) as exc:
            await _raw(eng, "customers", dimensions=[{"field": expr, "alias": "x"}])
    text = str(exc.value)
    assert expr in text or "'x'" in text, text
    assert "orders" in text.replace(expr, ""), text


async def test_a_time_dimension_across_a_fanning_hop_is_refused(backend: str) -> None:
    async with seed_engine(backend) as eng:
        with pytest.raises(DistinctDimensionValuesError) as exc:
            await _raw(eng, "customers", time_dimensions=[MONTH])
    _names(exc, entry="orders.ordered_at", hop="orders")


async def test_a_homeless_time_dimension_keeps_its_own_error(backend: str) -> None:
    td = {"field": "orders.ordered_at + returns.amount", "alias": "t", "granularity": "month"}
    async with seed_engine(backend) as eng:
        with pytest.raises(UnsafeRelationshipInputError):
            await _raw(eng, "customers", time_dimensions=[td])


async def test_a_list_child_is_refused_and_the_whole_list_is_not() -> None:
    async with seed_engine("duckdb", extra_statements=BASKETS_STATEMENTS,
                           datasets=seed_datasets(extra=(baskets_dataset(),))) as eng:
        with pytest.raises(DistinctDimensionValuesError) as exc:
            await _raw(eng, "baskets", dimensions=["baskets.items.value"])
        whole = await _raw(eng, "baskets", dimensions=["baskets.items"])
    _names(exc, entry="baskets.items.value", hop="items")
    assert sorted(r["baskets.items"] for r in whole.data) == [["a", "b"], ["c"]]


async def test_an_unknown_cardinality_hop_under_a_named_population_is_refused(backend: str) -> None:
    async with seed_engine(backend, datasets=_unproven_datasets()) as eng:
        with pytest.raises(DistinctDimensionValuesError) as exc:
            await _raw(eng, "orders", dimensions=[R])
    _names(exc, entry=R, hop="customers")


async def test_a_derived_field_crossing_an_unproven_hop_is_refused_through_its_closure(backend: str) -> None:
    cust_region = DatasetField(name="cust_region", datatype=DataType.STRING, expression="customers.region")
    async with seed_engine(backend, datasets=_unproven_datasets(cust_region)) as eng:
        with pytest.raises(DistinctDimensionValuesError) as exc:
            await _raw(eng, "orders", dimensions=["orders.cust_region"])
    _names(exc, entry="orders.cust_region", hop="customers")


async def test_a_derived_field_local_to_the_population_is_accepted(backend: str) -> None:
    double = DatasetField(name="double_amount", datatype=DataType.FLOAT, expression="amount * 2")
    async with seed_engine(backend, datasets=seed_datasets(orders=_orders_with(double))) as eng:
        resp = await _raw(eng, "orders", dimensions=["orders.double_amount"])
    assert table(resp, "orders.double_amount") == rows((200,), (400,), (100,), (600,))


async def test_an_unanalysable_dependency_fails_closed(backend: str) -> None:
    bad = DatasetField(name="bad", datatype=DataType.STRING, expression=ExpressionObject(
        dialects=[DialectEntry(dialect=d, expression=")((( bad") for d in ("DUCKDB", "SQLITE")]))
    async with seed_engine(backend, datasets=seed_datasets(orders=_orders_with(bad))) as eng:
        with pytest.raises(UnanalyzableDependencyError):
            await _raw(eng, "orders", dimensions=["orders.bad"])


@pytest.mark.parametrize(("datatype", "kw", "where"), [
    (DataType.STRING, {"dimensions": ["orders.bad"]}, "dimension 'orders.bad'"),
    (DataType.DATETIME, {"time_dimensions": [{"field": "orders.bad", "granularity": "month"}]},
     "time dimension 'orders.bad'"),
], ids=["dimension", "time_dimension"])
async def test_an_unanalysable_dependency_is_located_at_its_entry(
    backend: str, datatype: DataType, kw: dict[str, Any], where: str,
) -> None:
    bad = DatasetField(name="bad", datatype=datatype, expression=ExpressionObject(
        dialects=[DialectEntry(dialect=d, expression=")((( bad") for d in ("DUCKDB", "SQLITE")]))
    async with seed_engine(backend, datasets=seed_datasets(orders=_orders_with(bad))) as eng:
        with pytest.raises(UnanalyzableDependencyError) as exc:
            await _raw(eng, "customers", **kw)
    assert where in str(exc.value)
    assert "aggregate" not in str(exc.value)


@pytest.mark.parametrize("dim", ["tickets.priority", "tickets.bad"])
async def test_an_unrelated_dataset_keeps_its_own_error(backend: str, dim: str) -> None:
    bad = DatasetField(name="bad", datatype=DataType.STRING, expression=ExpressionObject(
        dialects=[DialectEntry(dialect=d, expression=")((( bad") for d in ("DUCKDB", "SQLITE")]))
    tickets = tickets_dataset()
    datasets = [tickets.model_copy(update={"fields": [*tickets.fields, bad]}) if d.name == "tickets" else d
                for d in seed_datasets()]
    async with seed_engine(backend, datasets=datasets) as eng:
        with pytest.raises(UnrelatedDatasetError):
            await _raw(eng, "customers", dimensions=[R, dim])


async def test_a_stage_population_dimension_across_a_fanning_inherited_edge_is_refused(backend: str) -> None:
    stage = q(name="c", population="customers", dimensions=[{"field": "customers.customer_id", "alias": "customer_id"},
                                                            {"field": R, "alias": "region"}])
    fanning = q(population="c", distinct_dimension_values=False, dimensions=["c.region", "orders.amount"])
    async with seed_engine(backend) as eng:
        with pytest.raises(DistinctDimensionValuesError) as exc:
            await eng.execute([stage, fanning])
        own = await eng.execute([stage, q(population="c", distinct_dimension_values=False, dimensions=["c.region"])])
    _names(exc, entry="orders.amount", hop="orders")
    assert table(own, "c.region") == rows(("EAST",), ("EAST",), ("WEST",), ("NORTH",))


async def test_determined_dimensions_keep_one_row_per_population_row(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await _raw(eng, "orders", dimensions=["orders.amount", R])
    assert table(resp, "orders.amount", R) == rows((100, "EAST"), (200, "EAST"), (50, "EAST"), (300, "WEST"))


async def test_grouped_mode_under_the_same_population_is_unchanged(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, population="customers", dimensions=["orders.amount"])
    assert table(resp, "orders.amount") == rows((100,), (200,), (50,), (300,), (None,))


async def test_an_inferred_computed_dimension_is_still_refused_at_inference(backend: str) -> None:
    async with seed_engine(backend, datasets=_unproven_datasets()) as eng:
        with pytest.raises(PopulationInferenceError):
            await run(eng, distinct_dimension_values=False,
                      dimensions=[R, {"field": "customers.region || orders.status", "alias": "x"}])


async def test_an_inferred_time_dimension_is_still_refused_at_inference(backend: str) -> None:
    async with seed_engine(backend) as eng:
        with pytest.raises(PopulationInferenceError):
            await run(eng, distinct_dimension_values=False, dimensions=["returns.amount"], time_dimensions=[MONTH])


@pytest.mark.parametrize("raw", [True, False], ids=["raw", "grouped"])
async def test_an_inferred_list_child_is_refused_at_inference(raw: bool) -> None:
    # DEV-2448 makes the grouped case infer the list child as its population.
    dims = ["orders.status", "baskets.items.value"] if raw else ["baskets.items.value"]
    async with seed_engine("duckdb", extra_statements=BASKETS_STATEMENTS,
                           datasets=seed_datasets(extra=(baskets_dataset(),))) as eng:
        with pytest.raises(PopulationInferenceError):
            await run(eng, distinct_dimension_values=not raw, dimensions=dims)
