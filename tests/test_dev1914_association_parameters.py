"""Every parameter shape associates or raises its own typed error; a list-child association reads a parameter its
root row determines (spec: queries/attribution-modes › Association eligibility, input handling and row identity)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from pytest_postgresql import factories

from slayer.core.errors import PartitionKeyError, SlayerError, UnsafeRelationshipInputError
from slayer.core.models import Aggregation, AggregationParam, SlayerDataset
from tests._dev1892_fixtures import (
    ASSOC_WAVG_POP_BY_STATUS,
    ASSOC_WAVG_SPEND_BY_STATUS,
    ASSOC_WSUM6_BY_STATUS,
    ASSOC_WSUM_BY_STATUS,
)
from tests._dev1900_fixtures import make_exec_engine
from tests._dev1919_fixtures import RANKED, ranked_global
from tests._dev1946_fixtures import ranked_assoc_by_status
from tests._dev1914_fixtures import (
    associated_warnings,
    broadcast_warnings,
    by_status,
    dev1900_datasets,
    keyless,
    metric,
    num,
    query,
)
from tests._dev2120_fixtures import contractors_dataset, contractors_statements
from tests._dev2137_fixtures import list_engine
from tests._pg_helpers import client_datasource, pg_query_engine

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

MODES = [pytest.param(None, id="default"), pytest.param("associate", id="associate")]

# --------------------------------------------------------------------------- #
# Parameter-shape inventory (orders-rooted, by status, over the distinct customers of each status)
# --------------------------------------------------------------------------- #
#: weight = the customer's tier maximum (gold 100, silver 150, bronze 40).
TIER_MAX_WAVG_BY_STATUS = {"ok": 53500.0 / 600.0, "new": 34100.0 / 290.0}
#: shape → (measure, associated cells, broadcast value over all seven customers).
EXECUTING_SHAPES: dict[str, tuple[str, dict, float]] = {
    "field-valued": ("weighted_avg(customers.spend, weight=customers.spend)", ASSOC_WAVG_SPEND_BY_STATUS,
                     48025.0 / 515.0),
    "field-default": ("wsum(customers.spend)", ASSOC_WSUM_BY_STATUS, 48025.0),
    "to-one-default": ("wgood(customers.spend)", ASSOC_WSUM6_BY_STATUS, 134000.0),
    "attached-aggregate": ("weighted_avg(customers.spend, weight=max(customers.spend, partition_by=customers.tier))",
                           TIER_MAX_WAVG_BY_STATUS, 60600.0 / 740.0),
    "to-one-path": ("weighted_avg(customers.spend, weight=customers.regions.pop)", ASSOC_WAVG_POP_BY_STATUS,
                    67000.0 / 900.0),
}
FANNING_SHAPE = "weighted_avg(customers.spend, weight=customers.regions.bad_pop)"


def inventory_datasets(*, keyed: bool) -> list[SlayerDataset]:
    """The ``_dev1900`` graph plus ``wsum`` (weight defaults to ``spend``); ``keyed=False`` strips customers' key."""
    datasets = dev1900_datasets()
    customers = next(d for d in datasets if d.name == "customers")
    customers.aggregations.append(Aggregation(name="wsum", expression="SUM({value} * {weight})",
                                              params=[AggregationParam(name="weight", sql="spend")]))
    return datasets if keyed else keyless(datasets, name="customers")


@pytest.fixture(params=[("sqlite", True), ("duckdb", True), ("sqlite", False), ("duckdb", False)],
                ids=["sqlite-keyed", "duckdb-keyed", "sqlite-keyless", "duckdb-keyless"])
async def inventory_engine(request):
    dialect, keyed = request.param
    async for e in make_exec_engine(SimpleNamespace(param=dialect), datasets=inventory_datasets(keyed=keyed)):
        yield e


def _by_status(mode, expression: str):
    return query(mode, population="orders", dimensions=["orders.status"], metrics=[metric(expression, "v")])


class TestParameterInventory:
    @pytest.mark.parametrize("mode", MODES)
    @pytest.mark.parametrize("shape", EXECUTING_SHAPES)
    async def test_associates(self, inventory_engine, shape, mode) -> None:
        expression, expected, _ = EXECUTING_SHAPES[shape]
        resp = await inventory_engine.execute(_by_status(mode, expression))
        assert by_status(resp, "v") == pytest.approx(expected)
        assert associated_warnings(resp)
        assert not broadcast_warnings(resp)

    @pytest.mark.parametrize("shape", EXECUTING_SHAPES)
    async def test_broadcasts(self, inventory_engine, shape) -> None:
        expression, expected, total = EXECUTING_SHAPES[shape]
        resp = await inventory_engine.execute(_by_status("broadcast", expression))
        assert by_status(resp, "v") == pytest.approx(dict.fromkeys(expected, total))
        assert broadcast_warnings(resp)

    @pytest.mark.parametrize("shape", EXECUTING_SHAPES)
    async def test_refused_in_error_mode(self, inventory_engine, shape) -> None:
        q = _by_status("error", EXECUTING_SHAPES[shape][0])
        with pytest.raises(SlayerError):
            await inventory_engine.execute(q)

    @pytest.mark.parametrize("mode", [None, "broadcast", "associate", "error"])
    async def test_fanning_parameter_fails_input_safety(self, inventory_engine, mode) -> None:
        q = _by_status(mode, FANNING_SHAPE)
        with pytest.raises(UnsafeRelationshipInputError, match="bad_pop"):
            await inventory_engine.execute(q)


@pytest.fixture(params=["sqlite", "duckdb"])
async def keyed_engine(request):
    async for e in make_exec_engine(request, datasets=inventory_datasets(keyed=True)):
        yield e


@pytest.fixture(params=["sqlite", "duckdb"])
async def keyless_engine(request):
    async for e in make_exec_engine(request, datasets=inventory_datasets(keyed=False)):
        yield e


class TestTransformValuedParameter:
    """weight = the customer's region rank by order total (North 1, NULL 2, South 3)."""

    @pytest.mark.parametrize("mode", MODES)
    async def test_associates(self, keyed_engine, mode) -> None:
        resp = await keyed_engine.execute(_by_status(mode, RANKED))
        assert by_status(resp, "v") == pytest.approx(ranked_assoc_by_status())
        assert associated_warnings(resp)
        assert not broadcast_warnings(resp)

    async def test_broadcasts(self, keyed_engine) -> None:
        resp = await keyed_engine.execute(_by_status("broadcast", RANKED))
        assert by_status(resp, "v") == pytest.approx(dict.fromkeys(ranked_assoc_by_status(), ranked_global()))
        assert broadcast_warnings(resp)

    async def test_refused_in_error_mode(self, keyed_engine) -> None:
        q = _by_status("error", RANKED)
        with pytest.raises(SlayerError):
            await keyed_engine.execute(q)

    @pytest.mark.parametrize("mode", ["broadcast"])
    async def test_keyless_home_partition_key_refused_under_broadcast(self, keyless_engine, mode) -> None:
        """The inner partition key crosses the keyless (unproven) orders → customers hop."""
        q = _by_status(mode, RANKED)
        with pytest.raises(PartitionKeyError, match="regions.name"):
            await keyless_engine.execute(q)


# --------------------------------------------------------------------------- #
# List-child association: the entity grain is the root row
# --------------------------------------------------------------------------- #
#: weighted_avg(rate, weight=rate) over the distinct contractors listing each skill: go = Ann 10, Eve 50;
#: sql = Ann 10, Bob 20.
LIST_WAVG_BY_SKILL = {"go": 2600.0 / 60.0, "sql": 500.0 / 30.0}
LIST_MEASURE = "weighted_avg(contractors.rate, weight=contractors.rate)"


def _skills(mode):
    return query(mode, population="contractors", dimensions=["contractors.skills.value"],
                 metrics=[metric(LIST_MEASURE, "wa")])


def _skill_cells(resp) -> dict[Any, Any]:
    cells = {r["contractors.skills.value"]: num(r["wa"]) for r in resp.data}
    return {k: cells.get(k) for k in LIST_WAVG_BY_SKILL}


@pytest.fixture(params=[True, False], ids=["keyed", "keyless"])
async def duckdb_lists(request):
    async with list_engine(keyed=request.param) as e:
        yield e


@pytest.fixture(params=[True, False], ids=["keyed", "keyless"])
async def postgres_lists(request, postgresql, tmp_path):
    cur = postgresql.cursor()
    for stmt in contractors_statements(dialect="postgres"):
        cur.execute(stmt)
    postgresql.commit()
    ds = client_datasource(postgresql.info)
    async with pg_query_engine(ds, base_dir=str(tmp_path),
                               datasets=[contractors_dataset(data_source="pg", keyed=request.param)]) as e:
        yield e


class TestListChildParameter:
    @pytest.mark.parametrize("mode", MODES)
    async def test_duckdb(self, duckdb_lists, mode) -> None:
        resp = await duckdb_lists.execute(_skills(mode))
        assert _skill_cells(resp) == pytest.approx(LIST_WAVG_BY_SKILL)
        assert associated_warnings(resp)
        assert not broadcast_warnings(resp)

    @pytest.mark.parametrize("mode", MODES)
    async def test_postgres(self, postgres_lists, mode) -> None:
        resp = await postgres_lists.execute(_skills(mode))
        assert _skill_cells(resp) == pytest.approx(LIST_WAVG_BY_SKILL)
        assert associated_warnings(resp)
        assert not broadcast_warnings(resp)
