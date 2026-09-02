"""Shared fixtures for DEV-1842 — dotted references to another model's saved
measures (``customers.aov`` from an ``orders``-rooted query).

Reuses the DEV-1836 dataset/schema and layers saved measures onto ``customers``
and ``orders``; every equality test pairs a dotted form with its hand-expanded
twin (bound-tree-identical). Underscore-prefixed so pytest skips collection.
"""

from __future__ import annotations

import os
import tempfile
from typing import AsyncIterator, List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

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
    ModelMeasure(name="aov", formula="spend:sum / *:count"),
    ModelMeasure(name="aov_big", formula="aov * 2"),
    ModelMeasure(name="self_aov", formula="customers.spend:sum / customers.*:count"),
    ModelMeasure(name="pop_total", formula="regions.pop:sum"),
    ModelMeasure(name="gold_spend_total", formula="gold_spend:sum"),
    ModelMeasure(name="north_spend_total", formula="north_spend:sum"),
    ModelMeasure(name="vip_spend_total", formula="vip_spend:sum"),
    ModelMeasure(name="spend_by_tier", formula="spend:sum(partition_by=tier)"),
    ModelMeasure(name="spend_grand", formula="spend:sum(partition_by=[])"),
    ModelMeasure(name="spend_run", formula="cumsum(spend:sum)"),
    ModelMeasure(name="typed_aov", formula="spend:sum / *:count", type=DataType.DOUBLE),
    ModelMeasure(name="order_total", formula="orders.amount:sum"),
    ModelMeasure(name="cyc_c", formula="orders.cyc_o"),
]

# On ``orders`` — host measures whose bare-name resolution recurses into a
# dotted cross-model reference, plus the cross-model cycle partner.
ORDERS_MEASURES: List[ModelMeasure] = [
    ModelMeasure(name="host_runs_dotted", formula="cumsum(customers.aov)"),
    ModelMeasure(name="host_mix", formula="amount:sum / customers.aov"),
    ModelMeasure(name="cyc_o", formula="customers.cyc_c"),
]


# Models — DEV-1836 shapes with the saved measures overlaid.
#: A filtered column whose predicate crosses the PROVEN customers → regions hop
#: (safe cross-model input, so ``north_spend:sum`` executes; cf. ``vip_spend``).
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
        dimension=ColumnRef(name=column), granularity=TimeGranularity.MONTH,
    )]


async def gen(query, *, dialect: str = "duckdb", validate: bool = False) -> str:
    models = dev1842_models()
    return await _engine_generate(
        query=query, model=models[0], extra_models=models[1:],
        dialect=dialect, validate=validate,
    )


async def _engine_for(*, dialect: str, db_path: str) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=os.path.join(os.path.dirname(db_path), "store"))
    await storage.save_datasource(
        DatasourceConfig(name="test", type=dialect, database=db_path)
    )
    for model in dev1842_models():
        await storage.save_model(model, _validate=False)
    return SlayerQueryEngine(storage=storage)


async def make_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture; each test module wraps
    this in ``@pytest.fixture`` so the fixture name lives where used."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, f"data.{dialect}")
        if dialect == "sqlite":
            _seed_sqlite(db_path)
        else:
            _seed_duckdb(db_path)
        yield await _engine_for(dialect=dialect, db_path=db_path)


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
    "simple_composite": ("customers.aov", "customers.spend:sum / customers.*:count"),
    "nested_saved": ("customers.aov_big",
                     "(customers.spend:sum / customers.*:count) * 2"),
    "self_qualified": ("customers.self_aov",
                       "customers.spend:sum / customers.*:count"),
    "nested_join": ("customers.pop_total", "customers.regions.pop:sum"),
    "filter_owner_local": ("customers.gold_spend_total", "customers.gold_spend:sum"),
    "filter_join_crossing": ("customers.north_spend_total", "customers.north_spend:sum"),
    "partition_local": ("customers.spend_by_tier",
                        "customers.spend:sum(partition_by=customers.tier)"),
    "partition_grand": ("customers.spend_grand",
                        "customers.spend:sum(partition_by=[])"),
    "host_wraps_dotted": ("cumsum(customers.aov)",
                          "cumsum(customers.spend:sum / customers.*:count)"),
    "host_mixes_dotted": ("amount:sum / customers.aov",
                          "amount:sum / (customers.spend:sum / customers.*:count)"),
}

#: The transform pair needs a host time dimension; kept separate so callers can
#: attach ``time_dimensions`` for both spellings.
TRANSFORM_PAIR = ("customers.spend_run", "cumsum(customers.spend:sum)")

#: Pairs where the hand form itself is REJECTED today by a DEV-1836 guard (an
#: input crossing an unproven join hop). The dotted form must inherit the same
#: rejection — parity is "both raise the same error", not "same values".
PARITY_ERROR_PAIRS = {
    "filter_unproven_hop": ("customers.vip_spend_total", "customers.vip_spend:sum"),
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
