"""Shared fixtures for DEV-1954 — canonical join-path spelling.

The DEV-1900 ``orders → customers → regions → region_events`` graph with the
``customers → regions`` edge named ``hr`` (so it may be spelled ``hr`` or
``regions``), plus:

* ``regions.pop`` labelled ``Population`` (so measure metadata surfaces);
* ``regions.rname`` (physical copy of ``name``) and ``regions.founded_at``
  (North 2020-01-15, South 2021-06-15);
* ``regions.maxpop`` (saved ``pop:max``), ``customers.tot_spend`` (saved
  ``spend:sum``), ``orders.rp`` (saved ``customers.regions.pop:max``);
* ``orders.region_label`` (Mode-A ``customers.regions.rname``),
  ``orders.north_amount`` (``amount`` masked by ``customers.regions.rname = 'North'``) and the
  aggregation ``orders.wpop`` whose ``weight`` defaults to ``customers.regions.pop``.

Oracles derive from the DEV-1840/1900 dataset: amount by region North 100,
South 20, region-less 47; pop North 100, South 200; region_events.value max
North 50, South 30.
"""

from __future__ import annotations

from typing import AsyncIterator, List, Optional

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.client import SlayerSQLClient
from slayer.storage.sqlite_conn import transaction
from tests import _dev1900_fixtures
from tests._dev1900_fixtures import dev1900_models
from tests._engine_helpers import seeded_exec_engine

_REGION_EXTRA = [("North", "2020-01-15", 1), ("South", "2021-06-15", 2)]


def _by_name(models: List[SlayerModel], name: str) -> SlayerModel:
    return next(m for m in models if m.name == name)


def dev1954_models(*, named: bool = True) -> List[SlayerModel]:
    """``named=False`` leaves the ``customers → regions`` edge unnamed."""
    models = dev1900_models()
    customers = _by_name(models, "customers")
    customers.joins = [
        ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE,
                  name="hr" if named else None)
        if j.target_model == "regions" else j
        for j in customers.joins
    ]
    customers.measures.append(ModelMeasure(name="tot_spend", formula="spend:sum"))

    regions = _by_name(models, "regions")
    regions.columns = [c.model_copy(update={"label": "Population"}) if c.name == "pop" else c
                       for c in regions.columns]
    regions.columns.append(Column(name="rname", type=DataType.TEXT, label="Region name"))
    regions.columns.append(Column(name="founded_at", type=DataType.TIMESTAMP,
                                  label="Founded"))
    regions.measures.append(ModelMeasure(name="maxpop", formula="pop:max"))

    orders = _by_name(models, "orders")
    orders.columns.append(Column(
        name="north_amount", type=DataType.DOUBLE, sql="amount",
        filter="customers.regions.rname = 'North'"))
    orders.columns.append(Column(
        name="region_label", type=DataType.TEXT, sql="customers.regions.rname"))
    orders.measures.append(ModelMeasure(name="rp", formula="customers.regions.pop:max"))
    orders.aggregations.append(Aggregation(
        name="wpop", formula="SUM({value} * {weight})",
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
    request, *, models: Optional[List[SlayerModel]] = None,
) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed,
        models=models if models is not None else dev1954_models(),
    ) as (engine, _db):
        yield engine


def slayer_query(**kw) -> SlayerQuery:
    return SlayerQuery.model_validate(kw)


def orders_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return slayer_query(**kw)


def table_count(sql: str, table: str, *, dialect: str = "duckdb") -> int:
    """How many times ``table`` is read as a physical table in ``sql``."""
    return sum(1 for t in sqlglot.parse_one(sql, dialect=dialect).find_all(exp.Table)
               if t.name == table)


def by_key(resp, dim: str, measure: str) -> dict:
    """``{dim value: measure value}`` over ``resp.data``."""
    out = {r[dim]: r[measure] for r in resp.data}
    assert len(out) == len(resp.data), "duplicate result rows for one group key"
    return out


def execution_spy(monkeypatch) -> list:
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
