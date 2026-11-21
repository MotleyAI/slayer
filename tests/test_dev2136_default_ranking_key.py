"""The default ranking key reads the query's dimensions in every ranked producer (spec: aggregations/functional-form › Default ranking key)."""

from __future__ import annotations

from typing import Any, Optional

import pytest

from slayer.core.errors import UnsafeRelationshipInputError
from slayer.core.models import SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerResponse
from tests._dev2119_fixtures import BACKENDS, EFF, bucket, by_bucket, column, dev2119_datasets, engine_for, m, num, q, td

_KEEP = "keep"


def _datasets(*, orders_default: Optional[str] = _KEEP, customers_default: Optional[str] = _KEEP) -> list[SlayerDataset]:
    """The dev2119 datasets with ``orders`` / ``customers`` default time dimensions overridden."""
    out = []
    for ds in dev2119_datasets():
        if ds.name == "orders" and orders_default != _KEEP:
            ds = ds.model_copy(update={"default_time_dimension": orders_default})
        elif ds.name == "customers" and customers_default != _KEEP:
            ds = ds.model_copy(update={"default_time_dimension": customers_default})
        out.append(ds)
    return out


async def _run(backend: str, query: Any, **overrides: Optional[str]) -> SlayerResponse:
    async with engine_for(backend, datasets=_datasets(**overrides)) as engine:
        return await engine.execute(query)


def _values_by(resp: SlayerResponse, key: str, value: str = "v") -> dict[Any, set]:
    """``{key value: {every value cell in that group}}``."""
    out: dict[Any, set] = {}
    for row in resp.data:
        out.setdefault(row[key], set()).add(num(row[value]))
    return out


E_DIM = {"field": EFF, "alias": "e"}


@pytest.mark.parametrize("backend", BACKENDS)
class TestLocalProducers:
    async def test_coarser_partition_reads_temporal_computed_dimension(self, backend: str) -> None:
        resp = await _run(backend, q(
            dimensions=["orders.region", E_DIM], time_dimensions=[td("orders.created_at")],
            metrics=[m("last(orders.amount, partition_by=orders.region)")],
        ))
        assert _values_by(resp, "orders.region") == {"US": {60.0}, "EU": {50.0}}

    async def test_coarser_partition_reads_time_dimension_source(self, backend: str) -> None:
        resp = await _run(backend, q(
            dimensions=["orders.region"], time_dimensions=[td("orders.shipped_at")],
            metrics=[m("last(orders.amount, partition_by=orders.region)")],
        ))
        assert _values_by(resp, "orders.region") == {"US": {60.0}, "EU": {50.0}}

    async def test_no_model_default_ranks_by_time_dimension_source(self, backend: str) -> None:
        resp = await _run(backend, q(
            dimensions=["orders.region"], time_dimensions=[td("orders.created_at")],
            metrics=[m("last(orders.amount, partition_by=orders.region)")],
        ), orders_default=None)
        assert _values_by(resp, "orders.region") == {"US": {40.0}, "EU": {50.0}}

    async def test_nested_in_second_order_aggregation(self, backend: str) -> None:
        resp = await _run(backend, q(
            dimensions=["orders.region", E_DIM], time_dimensions=[td("orders.created_at")],
            metrics=[m("avg(last(orders.amount, partition_by=[orders.region, orders.product]), "
                       "partition_by=orders.region)")],
        ))
        assert _values_by(resp, "orders.region") == {"US": {45.0}, "EU": {35.0}}

    async def test_windowed_partition_ranks_by_query_dimension(self, backend: str) -> None:
        resp = await _run(backend, q(
            dimensions=["orders.region", E_DIM], time_dimensions=[td("orders.created_at")],
            metrics=[m("last(orders.amount, window='90d', partition_by=orders.region)")],
        ))
        month = column(resp, "created_at")
        us_feb = {num(r["v"]) for r in resp.data if r["orders.region"] == "US" and bucket(r[month]) == "2025-02"}
        assert us_feb == {60.0}

    async def test_shifted_partition_ranks_by_time_dimension_source(self, backend: str) -> None:
        resp = await _run(backend, q(
            dimensions=["orders.region"], time_dimensions=[td("orders.created_at", "year")],
            metrics=[m("time_shift(last(orders.amount, partition_by=orders.region), -1)")],
        ), orders_default="shipped_at")
        assert by_bucket(resp, column(resp, "created_at"), width=4, by=["orders.region"])[("US", "2025")] == 40.0


@pytest.mark.parametrize("backend", BACKENDS)
async def test_each_stage_reads_its_own_dimensions(backend: str) -> None:
    stage = SlayerQuery.model_validate({
        "name": "s1", "population": "orders",
        "dimensions": ["orders.id", "orders.region", E_DIM, "orders.created_at"],
        "metrics": [{"metric": "last(orders.amount, partition_by=orders.region)", "alias": "v"}],
    })
    root = SlayerQuery.model_validate({
        "population": "s1", "dimensions": ["s1.region", "s1.created_at"],
        "metrics": [{"metric": "last(s1.id, partition_by=s1.region)", "alias": "w"},
                    {"metric": "max(s1.v)", "alias": "first_stage"}],
    })
    resp = await _run(backend, [stage, root])
    assert _values_by(resp, "s1.region", "first_stage") == {"US": {60.0}, "EU": {50.0}}
    assert _values_by(resp, "s1.region", "w") == {"US": {4.0}, "EU": {5.0}}


@pytest.mark.parametrize("backend", BACKENDS)
class TestCrossModelProducers:
    async def test_reads_query_time_dimension_its_home_determines(self, backend: str) -> None:
        resp = await _run(backend, q(
            dimensions=["customers.region"], time_dimensions=[td("customers.updated_at")],
            metrics=[m("last(customers.spend, partition_by=customers.region)")],
        ))
        assert _values_by(resp, "customers.region") == {"North": {100.0}, "South": {200.0}}

    async def test_reads_computed_query_dimension(self, backend: str) -> None:
        resp = await _run(backend, q(
            dimensions=["customers.region", {"field": "coalesce(customers.updated_at, customers.signup_ts)", "alias": "cu"}],
            metrics=[m("last(customers.spend, partition_by=customers.region)")],
        ))
        assert _values_by(resp, "customers.region") == {"North": {300.0}, "South": {200.0}}

    async def test_skips_undetermined_candidate_in_order(self, backend: str) -> None:
        resp = await _run(backend, q(
            dimensions=["customers.region", "orders.created_at", "customers.updated_at"],
            metrics=[m("last(customers.spend, partition_by=customers.region)")],
        ), customers_default="signup_ts")
        assert _values_by(resp, "customers.region") == {"North": {100.0}, "South": {200.0}}

    async def test_falls_back_to_home_default(self, backend: str) -> None:
        resp = await _run(backend, q(
            dimensions=["customers.region"], time_dimensions=[td("orders.created_at")],
            metrics=[m("last(customers.spend, partition_by=customers.region)")],
        ), customers_default="signup_ts")
        assert _values_by(resp, "customers.region") == {"North": {300.0}, "South": {None}}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("formula", [
    "last(accounts.spend, partition_by=accounts.id)",
    "last(accounts.spend)",
])
async def test_fanning_query_dimension_as_default_fails_closed(backend: str, formula: str) -> None:
    query = SlayerQuery.model_validate({
        "population": "accounts", "dimensions": ["accounts.id", "orders.created_at"],
        "metrics": [{"metric": formula, "alias": "v"}],
    })
    with pytest.raises(UnsafeRelationshipInputError) as ei:
        await _run(backend, query)
    assert "created_at" in str(ei.value)
    assert "orders" in str(ei.value)
