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
    cust_q,
    dev1900_datasets,
    make_exec_engine,
    orders_q,
    unparseable_derived_datasets,
)

HOP = "region_events"
MODES = ["broadcast", "error", "associate"]


def _orders_fan_datasets():
    """dev1900 graph + an orders-local derived field whose definition crosses the
    fanning ``regions → region_events`` hop — a path-less key anchored at orders that
    also carries a time axis (``ordered_at``) for the windowed position."""
    models = dev1900_datasets()
    orders = next(m for m in models if m.name == "orders")
    orders.fields.append(DatasetField(
        name="bad_amt", datatype=DataType.FLOAT,
        expression="customers.regions.region_events.value"))
    return models


@pytest.fixture(params=["sqlite", "duckdb"])
async def fan_engine(request):
    async for e in make_exec_engine(request, datasets=_orders_fan_datasets()):
        yield e


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
    """Anchor-safe key: ``status`` is a plain orders field (safe from its anchor orders),
    unattributable from the aggregate's home ``customers`` — the mode-aware case."""
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


class TestAnchorSafeKeyStaysModeAware:
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
        # the mode-aware attribution check from the home customers, NOT the fanning-from-anchor guard
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


def _mode(mode: str | None) -> dict:
    return {"to_many_handling": mode} if mode else {}


def _windowed_bad_amt_q(metric: str, mode: str | None) -> SlayerQuery:
    return orders_q(
        dimensions=["orders.bad_amt"], time_dimensions=["date_trunc('month', orders.ordered_at)"],
        metrics=[QueryMetric(metric=metric, alias="w")], **_mode(mode))


def _bad_pop_q(metric: str, mode: str | None) -> SlayerQuery:
    return _regions_q(dimensions=["regions.bad_pop"], metrics=[QueryMetric(metric=metric, alias="w")], **_mode(mode))


#: (query builder, partitioned measure, its dimension spelling) — each key's closure fans from its anchor.
_ANCHOR_FAN = {
    "windowed": (_windowed_bad_amt_q, "sum(orders.amount, window='90d', partition_by=orders.bad_amt)",
                 "sum(orders.amount, window='90d')"),
    "cross_model": (_bad_pop_q, "sum(region_events.value, partition_by=regions.bad_pop)", "sum(region_events.value)"),
    "local": (_bad_pop_q, "sum(regions.pop, partition_by=regions.bad_pop)", "sum(regions.pop)"),
}


def _row_set(resp) -> list:
    return sorted(tuple(sorted((k, str(v)) for k, v in r.items())) for r in resp.data)


class TestAnchorFanningKeyResolvesPerMode:
    """Axiom 8: a key whose closure fans from its anchor is a dimension like any other — it associates like its
    dimension spelling and is refused outside association."""

    @pytest.mark.parametrize("mode", [None, "associate"])
    @pytest.mark.parametrize("shape", list(_ANCHOR_FAN))
    async def test_associates_like_the_dimension_spelling(self, fan_engine, shape, mode):
        build, metric, plain = _ANCHOR_FAN[shape]
        got = await fan_engine.execute(build(metric, mode))
        assert got.data
        assert _row_set(got) == _row_set(await fan_engine.execute(build(plain, None)))

    @pytest.mark.parametrize("mode", ["broadcast", "error"])
    @pytest.mark.parametrize("shape", list(_ANCHOR_FAN))
    async def test_refused_outside_association(self, fan_engine, shape, mode):
        build, metric, _plain = _ANCHOR_FAN[shape]
        query = build(metric, mode)
        with pytest.raises(PartitionKeyError, match=HOP):
            await fan_engine.execute(query)


class TestAxiom8Clause:
    """Normative (Axiom 8): a key unattributable from the aggregation's home resolves by mode; one local to its
    anchor is safe across a population fan."""

    async def test_unattributable_from_further_root_still_associates(self, engine):
        resp = await engine.execute(_status_xmodel_q("associate"))
        assert _rows(resp, "orders.status", "w") == pytest.approx(
            {"new": 290.0, "ok": 420.0})

    @pytest.mark.parametrize("mode", [*MODES, None])
    async def test_a_key_local_to_its_anchor_is_safe_across_a_population_fan(self, engine, mode):
        resp = await engine.execute(cust_q(
            dimensions=["orders.status"],
            metrics=[QueryMetric(metric="sum(orders.amount, partition_by=[orders.status])", alias="w")],
            **({"to_many_handling": mode} if mode else {})))
        plain = await engine.execute(cust_q(dimensions=["orders.status"],
                                            metrics=[QueryMetric(metric="sum(orders.amount)", alias="w")]))
        assert _rows(resp, "orders.status", "w") == pytest.approx(_rows(plain, "orders.status", "w"))
