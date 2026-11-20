"""Shared fixtures for filters and order targets compiling as hidden
fields/metrics (positions by construction).

Reuses the ``_dev1739``/``_dev1824`` fixtures' datasets + seed data verbatim (``orders → customers →
regions``) and their hand-computed oracles. Adds a ``plan_query`` bundle helper
for plan-shape tests and the cross-dataset partition oracle the newly-legal
cross-dataset-partitioned filter asserts against.
"""

from __future__ import annotations

from typing import List, Optional

from slayer.core.query import SlayerQuery
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.plan import plan_query

from tests._dev1824_fixtures import (  # noqa: F401 — re-exported fixture surface
    BAND35,
    BAND35_OF,
    CITY_TOTAL,
    GRAND_TOTAL,
    RC_GROUPS,
    REGION_TOTAL,
    TRAILING_90D_REGION,
    FieldRef,
    DatasetMetric,
    TimeDimension,
    TimeGranularity,
    approx_sum,
    dev1739_datasets,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    q,
    rows_by,
)

dev1865_datasets = dev1739_datasets

#: customers.spend:sum(partition_by=customers.regions.name) — RegN=c1+c2, RegS=c3.
CM_REGION_SPEND = {"RegN": 300.0, "RegS": 50.0}
#: customers.spend:sum(partition_by=customers.tier) over reachable customers.
CM_TIER_SPEND = {"gold": 150.0, "silver": 200.0}


def bundle(datasets: Optional[List] = None) -> ResolvedSourceBundle:
    datasets = datasets if datasets is not None else dev1865_datasets()
    return ResolvedSourceBundle(dialect="postgres", population=datasets[0], referenced_datasets=datasets[1:])


def plan(query: SlayerQuery, **kw):
    """A ``PlannedQuery`` for one stage over the dev1865 datasets."""
    return plan_query(query=query, bundle=bundle(), **kw)


__all__ = [
    "BAND35", "BAND35_OF", "CITY_TOTAL", "CM_REGION_SPEND", "CM_TIER_SPEND",
    "FieldRef", "GRAND_TOTAL", "DatasetMetric", "RC_GROUPS", "REGION_TOTAL",
    "SlayerQuery", "TRAILING_90D_REGION", "TimeDimension", "TimeGranularity",
    "approx_sum", "bundle", "dev1739_datasets", "dev1865_datasets", "gen",
    "make_exec_engine", "month_key", "month_td", "plan", "q", "rows_by",
]
