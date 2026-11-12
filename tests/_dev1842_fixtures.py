"""Shared fixtures for DEV-1842 — dotted references to another model's saved
measures (``customers.aov`` from an ``orders``-rooted query).

Reuses the DEV-1836 dataset/schema and layers saved measures onto ``customers``
and ``orders``; every equality test pairs a dotted form with its hand-expanded
twin (bound-tree-identical). Underscore-prefixed so pytest skips collection.
"""

from __future__ import annotations

from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from tests._engine_helpers import seeded_exec_engine

from tests._dev1836_fixtures import (
    _seed_duckdb,
    _seed_sqlite,
    customers_model as _customers_1836,
    orders_model as _orders_1836,
    regions_model,
    segments_model,
)
from tests._engine_helpers import _engine_generate

# Re-export the DEV-1836 oracles so DEV-1842 executed-value tests read from one
# place. (The dataset is identical; only the model definitions gain measures.)
from tests._dev1836_fixtures import (  # noqa: E402,F401  (re-export)
    GOLD_SPEND_BY_REGION,
    POP_TOTAL,
    SPEND_BY_REGION,
    SPEND_BY_TIER,
    SPEND_TOTAL,
)


# Saved measures on ``customers`` — each exercises one re-anchoring kind
# (composite, nested/recursive, self-qualified, own-join, column-filter local /
# proven-hop / unproven-hop, partition_by local / grand, transform, typed,
# round-trip, cross-model cycle). See EQUIV_PAIRS for the hand-expanded twins.
CUSTOMER_MEASURES: List[ModelMeasure] = [
    ModelMeasure(name="aov", formula="sum(spend) / count(*)"),
    ModelMeasure(name="aov_big", formula="aov * 2"),
    ModelMeasure(name="self_aov", formula="sum(customers.spend) / count(customers.*)"),
    ModelMeasure(name="pop_total", formula="sum(regions.pop)"),
    ModelMeasure(name="gold_spend_total", formula="sum(gold_spend)"),
    ModelMeasure(name="north_spend_total", formula="sum(north_spend)"),
    ModelMeasure(name="vip_spend_total", formula="sum(vip_spend)"),
    ModelMeasure(name="spend_by_tier", formula="sum(spend, partition_by=tier)"),
    ModelMeasure(name="spend_grand", formula="sum(spend, partition_by=[])"),
    ModelMeasure(name="spend_run", formula="cumsum(sum(spend))"),
    ModelMeasure(name="typed_aov", formula="sum(spend) / count(*)", type=DataType.DOUBLE),
    ModelMeasure(name="order_total", formula="sum(orders.amount)"),
    ModelMeasure(name="cyc_c", formula="orders.cyc_o"),
]

# On ``orders`` — host measures whose bare-name resolution recurses into a
# dotted cross-model reference, plus the cross-model cycle partner.
ORDERS_MEASURES: List[ModelMeasure] = [
    ModelMeasure(name="host_runs_dotted", formula="cumsum(customers.aov)"),
    ModelMeasure(name="host_mix", formula="sum(amount) / customers.aov"),
    ModelMeasure(name="cyc_o", formula="customers.cyc_c"),
]


# Models — DEV-1836 shapes with the saved measures overlaid.
#: A filtered column whose predicate crosses the PROVEN customers → regions hop
#: (safe cross-model input, so ``sum(north_spend)`` executes; cf. ``vip_spend``).
_NORTH_SPEND = Column(
    name="north_spend", type=DataType.DOUBLE, sql="spend",
    filter="regions.name = 'North'",
)


def customers_model() -> SlayerModel:
    base = _customers_1836()
    return base.model_copy(update={
        "columns": list(base.columns) + [_NORTH_SPEND],
        "measures": CUSTOMER_MEASURES,
    })


def orders_model() -> SlayerModel:
    return _orders_1836().model_copy(update={"measures": ORDERS_MEASURES})


def dev1842_models() -> List[SlayerModel]:
    """``[host, *referenced]`` in the order ``_engine_generate`` wants."""
    return [orders_model(), customers_model(), regions_model(), segments_model()]


# Query + generation shorthands.
def q(**kw):
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


def month_td(column: str = "ordered_at"):
    return [TimeDimension(
        dimension=column, granularity=TimeGranularity.MONTH,
    )]


async def gen(query, *, dialect: str = "duckdb", validate: bool = False) -> str:
    models = dev1842_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:],
        dialect=dialect, validate=validate,
    )


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture; each test module wraps
    this in ``@pytest.fixture`` so the fixture name lives where used."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(dialect=dialect, seed=seed, models=dev1842_models()) as (engine, _db):
        yield engine


def rows_by(resp, *keys) -> dict:
    """Index ``resp.data`` rows by the given result-column key tuple."""
    out = {}
    for r in resp.data:
        out[tuple(r[k] for k in keys)] = r
    assert len(out) == len(resp.data), "duplicate result rows for one group key"
    return out


def broadcast_warnings(resp) -> list:
    return [w for w in (resp.warnings or [])
            if getattr(w, "kind", None) == "broadcast"]


#: (dotted saved-measure spelling, hand-expanded host-prefixed twin). Every pair
#: MUST be bound-tree-identical — identical SQL and executed values. The hand
#: forms are all DEV-1836-supported cross-model spellings that work today.
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
    "host_mixes_dotted": ("sum(amount) / customers.aov",
                          "sum(amount) / (sum(customers.spend) / count(customers.*))"),
}

#: The transform pair needs a host time dimension; kept separate so callers can
#: attach ``time_dimensions`` for both spellings.
TRANSFORM_PAIR = ("customers.spend_run", "cumsum(sum(customers.spend))")

#: Pairs where the hand form itself is REJECTED today by a DEV-1836 guard (an
#: input crossing an unproven join hop). The dotted form must inherit the same
#: rejection — parity is "both raise the same error", not "same values".
PARITY_ERROR_PAIRS = {
    "filter_unproven_hop": ("customers.vip_spend_total", "sum(customers.vip_spend)"),
}


__all__ = [
    "CUSTOMER_MEASURES", "ORDERS_MEASURES",
    "customers_model", "orders_model", "regions_model", "segments_model",
    "dev1842_models", "q", "month_td", "gen", "make_exec_engine",
    "rows_by", "broadcast_warnings",
    "EQUIV_PAIRS", "TRANSFORM_PAIR", "PARITY_ERROR_PAIRS",
    "ModelMeasure", "SlayerModel", "DataType",
    "GOLD_SPEND_BY_REGION", "POP_TOTAL", "SPEND_BY_REGION", "SPEND_BY_TIER",
    "SPEND_TOTAL",
]
