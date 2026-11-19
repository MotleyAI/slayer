"""Shared fixtures for canonical relationship-path spelling.

The ``_dev1900_fixtures`` ``orders → customers → regions → region_events`` graph with the
``customers → regions`` edge named ``hr`` (so it may be spelled ``hr`` or
``regions``), plus:

* ``regions.pop`` labelled ``Population`` (so metric metadata surfaces);
* ``regions.rname`` (physical copy of ``name``) and ``regions.founded_at``
  (North 2020-01-15, South 2021-06-15);
* ``regions.maxpop`` (saved ``max(pop)``), ``customers.tot_spend`` (saved
  ``sum(spend)``), ``orders.rp`` (saved ``max(customers.regions.pop)``);
* ``orders.region_label`` (Mode-A ``customers.regions.rname``),
  ``orders.north_amount`` (``amount`` masked by ``customers.regions.rname = 'North'``) and the
  aggregation ``orders.wpop`` whose ``weight`` defaults to ``customers.regions.pop``.

Oracles derive from the ``_dev1840``/``_dev1900`` fixtures' dataset: amount by region North 100,
South 20, region-less 47; pop North 100, South 200; region_events.value max
North 50, South 30.
"""

from __future__ import annotations

from typing import AsyncIterator, List, Optional

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database.client import SlayerSQLClient
from slayer.storage.sqlite_conn import transaction
from tests import _dev1900_fixtures
from tests._dev1900_fixtures import dev1900_datasets
from tests._engine_helpers import seeded_exec_engine

_REGION_EXTRA = [("North", "2020-01-15", 1), ("South", "2021-06-15", 2)]


def _by_name(datasets: List[SlayerDataset], name: str) -> SlayerDataset:
    return next(m for m in datasets if m.name == name)


def dev1954_datasets(*, named: bool = True) -> List[SlayerDataset]:
    """``named=False`` leaves the ``customers → regions`` edge unnamed."""
    models = dev1900_datasets()
    customers = _by_name(models, "customers")
    customers.relationships = [
        DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"],
                  cardinality=RelationshipCardinality.MANY_TO_ONE,
                  name="hr" if named else None)
        if j.to == "regions" else j
        for j in customers.relationships
    ]
    customers.metrics.append(DatasetMetric(name="tot_spend", expression="sum(spend)"))

    regions = _by_name(models, "regions")
    regions.fields = [c.model_copy(update={"label": "Population"}) if c.name == "pop" else c
                       for c in regions.fields]
    regions.fields.append(DatasetField(name="rname", datatype=DataType.STRING, label="Region name"))
    regions.fields.append(DatasetField(name="founded_at", datatype=DataType.DATETIME,
                                  label="Founded"))
    regions.metrics.append(DatasetMetric(name="maxpop", expression="max(pop)"))

    orders = _by_name(models, "orders")
    orders.fields.append(DatasetField(
        name="north_amount", datatype=DataType.FLOAT, sql="amount",
        filter="customers.regions.rname = 'North'"))
    orders.fields.append(DatasetField(
        name="region_label", datatype=DataType.STRING, sql="customers.regions.rname"))
    orders.metrics.append(DatasetMetric(name="rp", expression="max(customers.regions.pop)"))
    orders.aggregations.append(Aggregation(
        name="wpop", expression="SUM({value} * {weight})",
        params=[AggregationParam(name="weight", sql="customers.regions.pop")]))
    return models


def _seed_sqlite(db_path: str) -> None:
    _dev1900_fixtures._seed_sqlite(db_path)  # noqa: SLF001 — shared base seed
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute("ALTER TABLE regions ADD COLUMN rname TEXT")
        cur.execute("ALTER TABLE regions ADD COLUMN founded_at TEXT")
        cur.executemany("UPDATE regions SET rname=?, founded_at=? WHERE id=?",
                        _REGION_EXTRA)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    _dev1900_fixtures._seed_duckdb(db_path)  # noqa: SLF001 — shared base seed
    con = duckdb.connect(db_path)
    con.execute("ALTER TABLE regions ADD COLUMN rname VARCHAR")
    con.execute("ALTER TABLE regions ADD COLUMN founded_at TIMESTAMP")
    for rname, founded, rid in _REGION_EXTRA:
        con.execute("UPDATE regions SET rname=?, founded_at=? WHERE id=?",
                    [rname, founded, rid])
    con.close()


async def make_exec_engine(
    request, *, datasets: Optional[List[SlayerDataset]] = None,
) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed,
        datasets=datasets if datasets is not None else dev1954_datasets(),
    ) as (engine, _db):
        yield engine


def slayer_query(**kw) -> SlayerQuery:
    return SlayerQuery.model_validate(kw)


def orders_q(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    return slayer_query(**kw)


def table_count(sql: str, table: str, *, dialect: str = "duckdb") -> int:
    """How many times ``table`` is read as a physical table in ``sql``."""
    return sum(1 for t in sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Table)
               if t.name == table)


def by_key(resp, dim: str, metric: str) -> dict:
    """``{dim value: metric value}`` over ``resp.data``."""
    out = {r[dim]: r[metric] for r in resp.data}
    assert len(out) == len(resp.data), "duplicate result rows for one group key"
    return out


def execution_spy(monkeypatch: pytest.MonkeyPatch) -> list:
    """Record every SQL string sent to the database."""
    calls: list = []
    orig = SlayerSQLClient.execute

    async def spy(self, sql, timeout_seconds=120):
        calls.append(sql)
        return await orig(self, sql=sql, timeout_seconds=timeout_seconds)

    monkeypatch.setattr(SlayerSQLClient, "execute", spy)
    return calls


def stale_warnings(resp) -> list:
    return [w for w in (resp.warnings or [])
            if getattr(w, "rule_id", None) == "STALE_PATH_SPELLING"]


#: Canonical result keys (query rooted at orders).
RNAME = "orders.customers.hr.rname"
#: SUM(amount) per region name.
AMOUNT_BY_RNAME = {"North": 100.0, "South": 20.0, None: 47.0}
#: MAX(pop) per region name.
POP_MAX_BY_RNAME = {"North": 100.0, "South": 200.0, None: None}
#: MAX(region_events.value) per region name.
EVENT_MAX_BY_RNAME = {"North": 50.0, "South": 30.0, None: None}
#: SUM(amount * pop) per region name.
WPOP_BY_RNAME = {"North": 10000.0, "South": 4000.0, None: None}
#: SUM(spend) per region name over customers (c4 region-less).
SPEND_BY_RNAME = {"North": 280.0, "South": 195.0}
