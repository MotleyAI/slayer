"""Unanalysable, unreachable and ambiguous partition keys fail closed; a host-safe key stays mode-aware."""

from __future__ import annotations

import pytest

from slayer.core.enums import RelationshipCardinality
from slayer.core.errors import AmbiguousRelationshipPathError, PartitionKeyError, UnrelatedDatasetError
from slayer.core.models import (
    DatasetField,
    DataType,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.query import QueryMetric, SlayerQuery
from tests._dev1900_fixtures import (
    dev1900_datasets,
    make_exec_engine,
    orders_q,
    unparseable_derived_datasets,
)

HOP = "region_events"
MODES = ["broadcast", "error", "associate"]


def _island_datasets():
    """dev1900 graph + an isolated dataset with no relationship to anything — a partition key
    naming it has no route, so it fails closed upstream at dimension resolution,
    never reaching the fanning guard (nor falsely naming its hop)."""
    models = dev1900_datasets()
    models.append(SlayerDataset(
        name="island", data_source="test", sql_table="region_events",
        fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                 DatasetField(name="value", datatype=DataType.FLOAT)]))
    return models


def _ambiguous_datasets():
    """dev1900 graph + a second ``regions → region_events`` edge — the bare hop is
    now ambiguous, so a partition key crossing it fails closed deterministically
    instead of silently picking one edge."""
    models = dev1900_datasets()
    regions = next(m for m in models if m.name == "regions")
    regions.relationships.append(DatasetRelationship(
        to="region_events", from_columns=["id"], to_columns=["id"],
        cardinality=RelationshipCardinality.ONE_TO_MANY))
    return models


@pytest.fixture(params=["sqlite", "duckdb"])
async def engine(request):
    async for e in make_exec_engine(request):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def unparse_engine(request):
    async for e in make_exec_engine(request, datasets=unparseable_derived_datasets()):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def island_engine(request):
    async for e in make_exec_engine(request, datasets=_island_datasets()):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def ambig_engine(request):
    async for e in make_exec_engine(request, datasets=_ambiguous_datasets()):
        yield e


def _rows(resp, dim: str, val: str) -> dict:
    return {r[dim]: r[val] for r in resp.data}


def _regions_q(**kw) -> SlayerQuery:
    kw.setdefault("population", "regions")
    return SlayerQuery(**kw)


def _status_xmodel_q(mode: str) -> SlayerQuery:
    """Host-safe key: ``status`` is a plain orders field (safe from the orders host),
    unattributable only from the cross-dataset ``customers`` root — the mode-aware case."""
    return orders_q(
        dimensions=["orders.status"],
        metrics=[QueryMetric(metric="sum(customers.spend, partition_by=orders.status)", alias="w")],
        to_many_handling=mode)


class TestUnanalysableKey:
    """Scenario: Unanalysable derived partition key fails closed without naming a hop."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_unparseable_fails_closed_no_false_hop(self, unparse_engine, mode):
        query = _regions_q(
            dimensions=["regions.name"],
            metrics=[QueryMetric(
                metric="sum(regions.pop, partition_by=regions.unparseable)", alias="w")],
            to_many_handling=mode)
        with pytest.raises(PartitionKeyError) as ei:
            await unparse_engine.execute(query)
        msg = str(ei.value)
        assert "unparseable" in msg, msg
        assert HOP not in msg, msg  # never invent a hop it could not prove


class TestHostSafeKeyStaysModeAware:
    """Scenario: Host-safe partition key keeps its mode-aware resolution."""

    async def test_associate_succeeds(self, engine):
        resp = await engine.execute(_status_xmodel_q("associate"))
        assert _rows(resp, "orders.status", "w") == pytest.approx(
            {"new": 290.0, "ok": 420.0})

    @pytest.mark.parametrize("mode", ["broadcast", "error"])
    async def test_non_associate_modes_refuse(self, engine, mode):
        query = _status_xmodel_q(mode)
        with pytest.raises(ValueError) as ei:
            await engine.execute(query)
        # the mode-aware cross-dataset check (root=customers), NOT the fanning-from-host guard
        assert "attributable from customers" in str(ei.value), str(ei.value)

    async def test_safe_local_derived_partition_key_executes(self, engine):
        """A local derived key (``derived_pop = pop * 2``) crosses no hop — it still runs."""
        resp = await engine.execute(_regions_q(
            dimensions=["regions.derived_pop"],
            metrics=[QueryMetric(metric="sum(regions.pop, partition_by=regions.derived_pop)", alias="w")]))
        assert _rows(resp, "regions.derived_pop", "w") == pytest.approx(
            {200.0: 100.0, 400.0: 200.0})


class TestUnreachableAndAmbiguousFailClosed:
    """An unreachable partition_by path is a row-level read of an unrelated dataset and an
    ambiguous one fails at dimension resolution — neither reaches the fanning guard, so
    each fails closed deterministically without falsely naming the fanning hop."""

    async def test_unreachable_path_fails_closed_no_false_hop(self, island_engine):
        query = _regions_q(
            dimensions=["regions.name"],
            metrics=[QueryMetric(
                metric="sum(regions.pop, partition_by=island.value)", alias="w")])
        with pytest.raises(UnrelatedDatasetError) as ei:
            await island_engine.execute(query)
        assert HOP not in str(ei.value), str(ei.value)

    async def test_ambiguous_path_fails_closed_deterministically(self, ambig_engine):
        query = _regions_q(
            dimensions=["regions.name"],
            metrics=[QueryMetric(
                metric="sum(regions.pop, partition_by=regions.region_events.value)", alias="w")])
        with pytest.raises(AmbiguousRelationshipPathError) as ei:
            await ambig_engine.execute(query)
        assert len(ei.value.candidates) == 2  # named, not silently picked
