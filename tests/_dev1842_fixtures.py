"""Shared fixtures for dotted references to another dataset's saved
metrics (``customers.aov`` from an ``orders``-rooted query).

Reuses the ``_dev1836_fixtures`` seed data/schema and layers saved metrics onto ``customers``
and ``orders``; every equality test pairs a dotted form with its hand-expanded
twin (bound-tree-identical). Underscore-prefixed so pytest skips collection.
"""

from __future__ import annotations

from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.core.query import SlayerQuery, TimeDimension
from slayer.core.warnings import BroadcastGrainWarningPayload
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from tests._engine_helpers import seeded_exec_engine

from tests._dev1836_fixtures import (
    _seed_duckdb,
    _seed_sqlite,
    customers_dataset as _customers_1836,
    orders_dataset as _orders_1836,
    regions_dataset,
    segments_dataset,
)
from tests._engine_helpers import _engine_generate

# Re-export the ``_dev1836_fixtures`` oracles so executed-value tests read from one
# place. (The seed data is identical; only the dataset definitions gain metrics.)
from tests._dev1836_fixtures import (  # noqa: E402,F401  (re-export)
    GOLD_SPEND_BY_REGION,
    POP_TOTAL,
    SPEND_BY_REGION,
    SPEND_BY_TIER,
    SPEND_TOTAL,
)


# Saved metrics on ``customers`` — each exercises one re-anchoring kind
# (composite, nested/recursive, self-qualified, own-relationship, field-filter local /
# proven-hop / unproven-hop, partition_by local / grand, transform, typed,
# round-trip, cross-dataset cycle). See EQUIV_PAIRS for the hand-expanded twins.
CUSTOMER_MEASURES: List[DatasetMetric] = [
    DatasetMetric(name="aov", expression="sum(spend) / count(*)"),
    DatasetMetric(name="aov_big", expression="aov * 2"),
    DatasetMetric(name="self_aov", expression="sum(customers.spend) / count(customers.*)"),
    DatasetMetric(name="pop_total", expression="sum(regions.pop)"),
    DatasetMetric(name="gold_spend_total", expression="sum(gold_spend)"),
    DatasetMetric(name="north_spend_total", expression="sum(north_spend)"),
    DatasetMetric(name="vip_spend_total", expression="sum(vip_spend)"),
    DatasetMetric(name="spend_by_tier", expression="sum(spend, partition_by=tier)"),
    DatasetMetric(name="spend_grand", expression="sum(spend, partition_by=[])"),
    DatasetMetric(name="spend_run", expression="cumsum(sum(spend))"),
    DatasetMetric(name="typed_aov", expression="sum(spend) / count(*)", datatype=DataType.FLOAT),
    DatasetMetric(name="order_total", expression="sum(orders.amount)"),
    DatasetMetric(name="cyc_c", expression="orders.cyc_o"),
]

# On ``orders`` — host metrics whose bare-name resolution recurses into a
# dotted cross-dataset reference, plus the cross-dataset cycle partner.
ORDERS_MEASURES: List[DatasetMetric] = [
    DatasetMetric(name="host_runs_dotted", expression="cumsum(customers.aov)"),
    DatasetMetric(name="host_mix", expression="sum(amount) / customers.aov"),
    DatasetMetric(name="cyc_o", expression="customers.cyc_c"),
]


# Datasets — ``_dev1836_fixtures`` shapes with the saved metrics overlaid.
#: A filtered field whose predicate crosses the PROVEN customers → regions hop
#: (safe cross-dataset input, so ``sum(north_spend)`` executes; cf. ``vip_spend``).
_NORTH_SPEND = DatasetField(
    name="north_spend", datatype=DataType.FLOAT, sql="spend",
    filter="regions.name = 'North'",
)


def customers_dataset() -> SlayerDataset:
    base = _customers_1836()
    return base.model_copy(update={
        "fields": list(base.fields) + [_NORTH_SPEND],
        "metrics": CUSTOMER_MEASURES,
    })


def orders_dataset() -> SlayerDataset:
    return _orders_1836().model_copy(update={"metrics": ORDERS_MEASURES})


def dev1842_datasets() -> List[SlayerDataset]:
    """``[host, *referenced]`` in the order ``_engine_generate`` wants."""
    return [orders_dataset(), customers_dataset(), regions_dataset(), segments_dataset()]


# Query + generation shorthands.
def q(**kw):
    kw.setdefault("population", "orders")
    return SlayerQuery(**kw)


def month_td(column: str = "orders.ordered_at"):
    return [TimeDimension(
        field=column, granularity=TimeGranularity.MONTH,
    )]


async def gen(query, *, dialect: str = "duckdb", validate: bool = False) -> str:
    models = dev1842_datasets()
    return await _engine_generate(
        query=query, dataset=models[0], extra_datasets=models[1:],
        dialect=dialect, validate=validate,
    )


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture; each test module wraps
    this in ``@pytest.fixture`` so the fixture name lives where used."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, datasets=dev1842_datasets()) as (engine, _db):
        yield engine


def rows_by(resp, *keys) -> dict:
    """Index ``resp.data`` rows by the given result-column key tuple."""
    out = {}
    for r in resp.data:
        out[tuple(r[k] for k in keys)] = r
    assert len(out) == len(resp.data), "duplicate result rows for one group key"
    return out


def broadcast_warnings(resp: SlayerResponse) -> list[BroadcastGrainWarningPayload]:
    return [w for w in (resp.warnings or [])
            if isinstance(w, BroadcastGrainWarningPayload)]


#: (dotted saved-metric spelling, hand-expanded host-prefixed twin). Every pair
#: MUST be bound-tree-identical — identical SQL and executed values. The hand
#: forms are all supported cross-dataset spellings that work today.
EQUIV_PAIRS = {
    "simple_composite": ("customers.aov", "sum(customers.spend) / count(customers.*)"),
    "nested_saved": ("customers.aov_big",
                     "(sum(customers.spend) / count(customers.*)) * 2"),
    "self_qualified": ("customers.self_aov",
                       "sum(customers.spend) / count(customers.*)"),
    "nested_join": ("customers.pop_total", "sum(customers.regions.pop)"),
    "filter_owner_local": ("customers.gold_spend_total", "sum(customers.gold_spend)"),
    "filter_join_crossing": ("customers.north_spend_total", "sum(customers.north_spend)"),
    "partition_local": ("customers.spend_by_tier",
                        "sum(customers.spend, partition_by=customers.tier)"),
    "partition_grand": ("customers.spend_grand",
                        "sum(customers.spend, partition_by=[])"),
    "host_wraps_dotted": ("cumsum(customers.aov)",
                          "cumsum(sum(customers.spend) / count(customers.*))"),
    "host_mixes_dotted": ("sum(orders.amount) / customers.aov",
                          "sum(orders.amount) / (sum(customers.spend) / count(customers.*))"),
}

#: The transform pair needs a host time dimension; kept separate so callers can
#: attach ``time_dimensions`` for both spellings.
TRANSFORM_PAIR = ("customers.spend_run", "cumsum(sum(customers.spend))")

#: Pairs where the hand form itself is REJECTED today by a cross-dataset guard (an
#: input crossing an unproven relationship hop). The dotted form must inherit the same
#: rejection — parity is "both raise the same error", not "same values".
PARITY_ERROR_PAIRS = {
    "filter_unproven_hop": ("customers.vip_spend_total", "sum(customers.vip_spend)"),
}


__all__ = [
    "CUSTOMER_MEASURES", "ORDERS_MEASURES",
    "customers_dataset", "orders_dataset", "regions_dataset", "segments_dataset",
    "dev1842_datasets", "q", "month_td", "gen", "make_exec_engine",
    "rows_by", "broadcast_warnings",
    "EQUIV_PAIRS", "TRANSFORM_PAIR", "PARITY_ERROR_PAIRS",
    "DatasetMetric", "SlayerDataset", "DataType",
    "GOLD_SPEND_BY_REGION", "POP_TOTAL", "SPEND_BY_REGION", "SPEND_BY_TIER",
    "SPEND_TOTAL",
]
