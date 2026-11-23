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


def _no_default_returns() -> SlayerDataset:
    return returns_dataset().model_copy(update={"filters": ["amount > {m}"]})


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_named_stage_binds_a_joined_dataset_under_its_own_variables(backend: str) -> None:
    # Only the stage supplies m.
    async with seed_engine(backend, datasets=_datasets(returns=_no_default_returns())) as eng:
        resp = await eng.execute([
            {"name": "s", "population": "customers", "dimensions": [R],
             "metrics": [{"metric": "returns.total_returns", "alias": "tr"}], "variables": {"m": 5}},
            {"population": "s", "dimensions": ["s.region", "s.tr"]},
        ])
    got = {r["s.region"]: r["s.tr"] for r in resp.data}
    assert (got["EAST"], got["NORTH"]) == (25, 10)


async def _root_less_stage_returns(backend: str, *, stage_seg: str, root_seg: str) -> list:
    customers = customers_dataset().model_copy(update={"filters": ["segment != '{seg}'"]})
    async with seed_engine(backend, datasets=_datasets(customers=customers)) as eng:
        resp = await eng.execute([
            {"name": "s", "dimensions": [R], "metrics": [{"metric": "returns.total_returns", "alias": "tr"}],
             "variables": {"seg": stage_seg}},
            {"population": "s", "dimensions": ["s.region", "s.tr"], "variables": {"seg": root_seg}},
        ])
    return sorted((r["s.region"], r["s.tr"]) for r in resp.data)


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_root_less_stages_cells_bind_under_the_stages_variables(backend: str) -> None:
    got = await _root_less_stage_returns(backend, stage_seg="corporate", root_seg="retail")
    assert got == await _root_less_stage_returns(backend, stage_seg="corporate", root_seg="corporate")


@pytest.mark.parametrize("backend", BACKENDS)
async def test_a_spliced_stage_binds_a_joined_dataset_under_its_own_variables(backend: str) -> None:
    qb = SlayerDataset.model_validate({"name": "qb", "data_source": "test", "source_queries": [
        {"population": "customers", "dimensions": [R],
         "metrics": [{"metric": "returns.total_returns", "alias": "tr"}], "variables": {"m": 5}}]})
    async with seed_engine(backend, datasets=[*_datasets(returns=_returns(query_variables={"m": 20})), qb]) as eng:
        resp = await run(eng, population="qb", dimensions=["qb.region", "qb.tr"], variables={"m": 20})
    got = {r["qb.region"]: r["qb.tr"] for r in resp.data}
    assert (got["EAST"], got["NORTH"]) == (25, 10)
