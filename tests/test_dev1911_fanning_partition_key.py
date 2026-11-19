"""A path-less derived partition key fanning from its host fails closed
in every mode.

A ``partition_by=`` naming a host-local derived column whose definition crosses the
1:N ``regions → region_events`` hop (e.g. ``bad_pop = pop + region_events.value``)
is not single-valued at the host grain, so no ``to_many_handling`` mode can count it
inline — it is a mode-invariant input-safety error, distinct from a dimension merely
unattributable from a further (cross-model) root, which still resolves by mode. The
guard runs at bind (mode-unaware, one chokepoint), so it fires uniformly across every
position the key can appear in. On the shared fixture graph, dual-engine.

Covers ``queries/attribution-modes``.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import RelationshipCardinality
from slayer.core.errors import AmbiguousRelationshipPathError, UnrelatedDatasetError
from slayer.core.models import (
    DatasetField,
    DataType,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.query import QueryMetric, SlayerQuery
from tests._dev1900_fixtures import (
    BAD_POP,
    dev1900_datasets,
    make_exec_engine,
    orders_q,
    unparseable_derived_datasets,
)

HOP = "region_events"
MODES = ["broadcast", "error", "associate"]


def _orders_fan_datasets():
    """dev1900 graph + an orders-local derived column whose definition crosses the
    fanning ``regions → region_events`` hop — a path-less key on an orders host that
    also carries a time axis (``ordered_at``) for the windowed position."""
    models = dev1900_datasets()
    orders = next(m for m in models if m.name == "orders")
    orders.fields.append(DatasetField(
        name="bad_amt", datatype=DataType.FLOAT,
        sql="amount + customers.regions.region_events.value"))
    return models


def _island_datasets():
    """dev1900 graph + an isolated model with no join to anything — a partition key
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
async def fan_engine(request):
    async for e in make_exec_engine(request, datasets=_orders_fan_datasets()):
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


async def _assert_fails_naming_hop(eng, query: SlayerQuery) -> str:
    """Fails closed at the bind-time partition-key safety guard, naming the fanning hop.
    The contract-phrase assertion pins the guard (not an incidental error-mode broadcast
    refusal, whose message names the hop too but is a different template)."""
    with pytest.raises(ValueError) as ei:
        await eng.execute(query)
    msg = str(ei.value)
    assert HOP in msg, msg
    assert "every partition key must be attributable" in msg, msg
    return msg


def _status_xmodel_q(mode: str) -> SlayerQuery:
    """Host-safe key: ``status`` is a plain orders column (safe from the orders host),
    unattributable only from the cross-model ``customers`` root — the mode-aware case."""
    return orders_q(
        dimensions=["orders.status"],
        metrics=[QueryMetric(metric="sum(customers.spend, partition_by=orders.status)", alias="w")],
        to_many_handling=mode)


class TestPathlessDerivedFanningEveryMode:
    """Scenario: Path-less derived fanning partition key fails closed in every mode."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_repro_raises_naming_the_hop(self, engine, mode):
        await _assert_fails_naming_hop(engine, _regions_q(
            dimensions=["regions.bad_pop"],
            metrics=[QueryMetric(metric="sum(regions.pop, partition_by=regions.bad_pop)", alias="w")],
            to_many_handling=mode))


class TestChainedDerived:
    """Scenario: Chained derived fanning partition key fails closed."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_chained_raises_naming_the_hop(self, engine, mode):
        await _assert_fails_naming_hop(engine, _regions_q(
            dimensions=["regions.bad_pop2"],
            metrics=[QueryMetric(metric="sum(regions.pop, partition_by=regions.bad_pop2)", alias="w")],
            to_many_handling=mode))


class TestEveryPosition:
    """Scenario: Fanning partition key fails closed in every position."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_transform_partition_set(self, engine, mode):
        await _assert_fails_naming_hop(engine, _regions_q(
            dimensions=["regions.bad_pop"],
            metrics=[QueryMetric(metric="rank(sum(regions.pop), partition_by=regions.bad_pop, direction='desc')", alias="w")],
            to_many_handling=mode))

    @pytest.mark.parametrize("mode", MODES)
    async def test_reaggregation_inner(self, engine, mode):
        await _assert_fails_naming_hop(engine, _regions_q(
            dimensions=["regions.name"],
            metrics=[QueryMetric(metric="avg(sum(regions.pop, partition_by=regions.bad_pop))", alias="w")],
            to_many_handling=mode))

    @pytest.mark.parametrize("mode", MODES)
    async def test_computed_dimension(self, engine, mode):
        await _assert_fails_naming_hop(engine, _regions_q(
            dimensions=["CASE WHEN sum(regions.pop, partition_by=regions.bad_pop) > 150 THEN 1 ELSE 0 END"],
            metrics=[QueryMetric(metric="sum(regions.pop)", alias="p")],
            to_many_handling=mode))

    @pytest.mark.parametrize("mode", MODES)
    async def test_filter_position(self, engine, mode):
        await _assert_fails_naming_hop(engine, _regions_q(
            dimensions=["regions.bad_pop"],
            metrics=[QueryMetric(metric="sum(regions.pop)", alias="p")],
            filter=["sum(regions.pop, partition_by=regions.bad_pop) > 150"],
            to_many_handling=mode))

    @pytest.mark.parametrize("mode", MODES)
    async def test_order_position(self, engine, mode):
        await _assert_fails_naming_hop(engine, _regions_q(
            dimensions=["regions.bad_pop"],
            metrics=[QueryMetric(metric="sum(regions.pop)", alias="p")],
            order=[{"field": "sum(regions.pop, partition_by=regions.bad_pop)", "direction": "asc"}],
            to_many_handling=mode))

    @pytest.mark.parametrize("mode", MODES)
    async def test_windowed_position(self, fan_engine, mode):
        await _assert_fails_naming_hop(fan_engine, orders_q(
            dimensions=["orders.bad_amt"],
            time_dimensions=["date_trunc('month', orders.ordered_at)"],
            metrics=[QueryMetric(
                metric="sum(orders.amount, window='90d', partition_by=orders.bad_amt)", alias="w")],
            to_many_handling=mode))

    @pytest.mark.parametrize("mode", MODES)
    async def test_cross_model_aggregate_with_fanning_host_key(self, engine, mode):
        await _assert_fails_naming_hop(engine, _regions_q(
            dimensions=["regions.bad_pop"],
            metrics=[QueryMetric(
                metric="sum(region_events.value, partition_by=regions.bad_pop)", alias="w")],
            to_many_handling=mode))


class TestUnanalysableKey:
    """Scenario: Unanalysable derived partition key fails closed without naming a hop."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_unparseable_fails_closed_no_false_hop(self, unparse_engine, mode):
        query = _regions_q(
            dimensions=["regions.name"],
            metrics=[QueryMetric(
                metric="sum(regions.pop, partition_by=regions.unparseable)", alias="w")],
            to_many_handling=mode)
        with pytest.raises(ValueError) as ei:
            await unparse_engine.execute(query)
        msg = str(ei.value)
        assert "every partition key must be attributable" in msg, msg  # the safety guard
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
        # the mode-aware cross-model check (root=customers), NOT the fanning-from-host guard
        assert "attributable from customers" in str(ei.value), str(ei.value)

    async def test_safe_local_derived_partition_key_executes(self, engine):
        """A local derived key (``derived_pop = pop * 2``) crosses no hop — it still runs."""
        resp = await engine.execute(_regions_q(
            dimensions=["regions.derived_pop"],
            metrics=[QueryMetric(metric="sum(regions.pop, partition_by=regions.derived_pop)", alias="w")]))
        assert _rows(resp, "regions.derived_pop", "w") == pytest.approx(
            {200.0: 100.0, 400.0: 200.0})


class TestDiagnosticNamesHop:
    """The diagnostic names the fanning hop for the path-BEARING spelling too, and
    states the remedy."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_path_bearing_spelling_names_the_hop(self, engine, mode):
        await _assert_fails_naming_hop(engine, orders_q(
            dimensions=[BAD_POP],
            metrics=[QueryMetric(
                metric="sum(orders.amount, partition_by=customers.regions.bad_pop)", alias="w")],
            to_many_handling=mode))

    async def test_error_names_the_remedy(self, engine):
        msg = await _assert_fails_naming_hop(engine, _regions_q(
            dimensions=["regions.bad_pop"],
            metrics=[QueryMetric(metric="sum(regions.pop, partition_by=regions.bad_pop)", alias="w")]))
        assert "cardinality" in msg, msg
        assert "unique key" in msg, msg


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


class TestAxiom8Clause:
    """Normative (Axiom 8): a partition key whose closure fans from its host is a
    mode-invariant input-safety error, distinct from a dimension unattributable only
    from a further root (which resolves by mode)."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_fanning_from_host_fails_in_every_mode(self, engine, mode):
        await _assert_fails_naming_hop(engine, _regions_q(
            dimensions=["regions.bad_pop"],
            metrics=[QueryMetric(metric="sum(regions.pop, partition_by=regions.bad_pop)", alias="w")],
            to_many_handling=mode))

    async def test_unattributable_from_further_root_still_associates(self, engine):
        resp = await engine.execute(_status_xmodel_q("associate"))
        assert _rows(resp, "orders.status", "w") == pytest.approx(
            {"new": 290.0, "ok": 420.0})
