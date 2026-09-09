"""Shared fixtures for DEV-1865 — filters and order targets compile as hidden
fields/measures (positions by construction).

Reuses the DEV-1739/1824 models + dataset verbatim (``orders → customers →
regions``) and their hand-computed oracles. Adds a ``plan_query`` bundle helper
for plan-shape tests and the cross-model partition oracle the newly-legal
cross-model-partitioned filter asserts against.
"""

from __future__ import annotations

from typing import List, Optional

from slayer.core.query import SlayerQuery
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query

from tests._dev1824_fixtures import (  # noqa: F401 — re-exported fixture surface
    BAND35,
    BAND35_OF,
    CITY_TOTAL,
    GRAND_TOTAL,
    RC_GROUPS,
    REGION_TOTAL,
    TRAILING_90D_REGION,
    ColumnRef,
    ModelMeasure,
    TimeDimension,
    TimeGranularity,
    approx_sum,
    dev1739_models,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    q,
    rows_by,
)

dev1865_models = dev1739_models

#: customers.spend:sum(partition_by=customers.regions.name) — RegN=c1+c2, RegS=c3.
CM_REGION_SPEND = {"RegN": 300.0, "RegS": 50.0}
#: customers.spend:sum(partition_by=customers.tier) over reachable customers.
CM_TIER_SPEND = {"gold": 150.0, "silver": 200.0}


def bundle(models: Optional[List] = None) -> ResolvedSourceBundle:
    models = models if models is not None else dev1865_models()
    return ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:])


def plan(query: SlayerQuery, **kw):
    """A ``PlannedQuery`` for one stage over the dev1865 models."""
    return plan_query(query=query, bundle=bundle(), **kw)


__all__ = [
    "BAND35", "BAND35_OF", "CITY_TOTAL", "CM_REGION_SPEND", "CM_TIER_SPEND",
    "ColumnRef", "GRAND_TOTAL", "ModelMeasure", "RC_GROUPS", "REGION_TOTAL",
    "SlayerQuery", "TRAILING_90D_REGION", "TimeDimension", "TimeGranularity",
    "approx_sum", "bundle", "dev1739_models", "dev1865_models", "gen",
    "make_exec_engine", "month_key", "month_td", "plan", "q", "rows_by",
]
