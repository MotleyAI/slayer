"""Raw-row mode lists the rows of the named population, else of the dimension anchor read from dimensions only
(spec: queries/population › Dimension-determined default › Raw-row mode uses the same rule; semantics Axiom 12.5)."""

from __future__ import annotations

import pytest

from slayer.core.errors import PopulationInferenceError

from tests._dev2212_fixtures import (
    BACKENDS,
    customers_dataset,
    orders_dataset,
    rows,
    run,
    seed_datasets,
    seed_engine,
    table,
    to_customers,
)

R, SEG = "customers.region", "customers.segment"


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


async def test_raw_rows_of_a_named_population(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, population="customers", dimensions=[R], distinct_dimension_values=False)
    assert table(resp, R) == rows(("EAST",), ("EAST",), ("WEST",), ("NORTH",))


async def test_raw_rows_of_the_anchor(backend: str) -> None:
    async with seed_engine(backend) as eng:
        resp = await run(eng, dimensions=[R, SEG], distinct_dimension_values=False)
    assert table(resp, R, SEG) == rows(("EAST", "retail"), ("EAST", "corporate"), ("WEST", "retail"),
                                       ("NORTH", "retail"))
    assert resp.population == "customers" and resp.population_inferred is True


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
