"""Every dataset a statement reads binds its own Mode-A variables under the query's layers."""

from __future__ import annotations

from typing import Any

import pytest

from slayer.core.models import SlayerDataset

from tests._dev2212_fixtures import BACKENDS, customers_dataset, orders_dataset, returns_dataset, run, seed_engine

R = "customers.region"


def _returns(**update: Any) -> SlayerDataset:
    return returns_dataset().model_copy(update={"filters": ["amount > {m}"], "query_variables": {"m": 12}, **update})


def _datasets(*, customers: SlayerDataset | None = None, returns: SlayerDataset | None = None) -> list[SlayerDataset]:
    return [customers or customers_dataset(), orders_dataset(), returns or _returns()]


async def _returns_by_region(backend: str, *, datasets: list[SlayerDataset], **query: Any) -> dict:
    async with seed_engine(backend, datasets=datasets) as eng:
        resp = await run(eng, population="customers", dimensions=[R], metrics=["returns.total_returns"], **query)
    return {r[R]: r["returns.total_returns"] for r in resp.data}


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_joined_dataset_binds_its_own_defaults(backend: str) -> None:
    # R1 C1 (EAST) 25 passes `amount > 12`; R2 C4 (NORTH) 10 does not.
    got = await _returns_by_region(backend, datasets=_datasets())
    assert (got["EAST"], got["NORTH"]) == (25, None)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_the_query_variables_override_a_joined_datasets_defaults(backend: str) -> None:
    got = await _returns_by_region(backend, datasets=_datasets(), variables={"m": 5})
    assert (got["EAST"], got["NORTH"]) == (25, 10)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_the_populations_defaults_never_reach_a_joined_dataset(backend: str) -> None:
    customers = customers_dataset().model_copy(update={"query_variables": {"m": 100}})
    got = await _returns_by_region(backend, datasets=_datasets(customers=customers))
    assert (got["EAST"], got["NORTH"]) == (25, None)
