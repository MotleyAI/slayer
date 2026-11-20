"""Shared fixtures for the ``to_many_handling`` mode axis and the
distinct-entity association producer.

Built on ``tests/_dev1840_fixtures.py`` (same graph, dual-engine dataset,
``make_exec_engine``), which already carries the shape this issue turns on:
customer 1 has TWO ``ok`` orders (o1, o10), so a naive join double-counts c1's
spend in the ``ok`` cell. Distinct-customer populations by orders-level status:
ok {c1,c2,c3,c5,c6}, new {c1,c2,c4}; o8 is an orphan order. Oracles below are
hand-computed from that dataset; ``to_many_handling`` queries build lazily so a
missing field fails per-test, not at collection.
"""

from __future__ import annotations

from typing import List

from slayer.core.models import Aggregation, AggregationParam, DatasetField, SlayerDataset
from slayer.core.warnings import AssociatedWarningPayload, SemiJoinPushedWarningPayload
from slayer.engine.query_engine import SlayerResponse

from tests._dev1836_fixtures import (
    broadcast_warnings,
    rows_by,
)
from tests._dev1840_fixtures import (
    FieldRef,
    DataType,
    DatasetRelationship,
    DatasetMetric,
    SlayerQuery,
    TimeDimension,
    TimeGranularity,
    dev1840_datasets,
    make_exec_engine,
    q,
)


def _customers(datasets: List[SlayerDataset]) -> SlayerDataset:
    return next(m for m in datasets if m.name == "customers")


def dev1841_datasets(**kw) -> List[SlayerDataset]:
    """The ``_dev1840_fixtures`` graph plus a metric-local filtered field on customers:
    ``gold_spend`` = ``spend`` restricted (inside the aggregate) to gold tier."""
    models = dev1840_datasets(**kw)
    _customers(models).fields.append(
        DatasetField(name="gold_spend", datatype=DataType.FLOAT, sql="spend",
               filter="tier = 'gold'"),
    )
    return models


def keyless_root_datasets() -> List[SlayerDataset]:
    """The graph with customers stripped of its unique key (associated by row identity)."""
    models = dev1840_datasets()
    cust = _customers(models)
    cust.fields = [
        (c.model_copy(update={"primary_key": False}) if c.name == "id" else c)
        for c in cust.fields
    ]
    return models


def crossing_input_datasets() -> List[SlayerDataset]:
    """Graph + a customers field filtering across the fanning customers→orders hop (an input not constant per customer)."""
    models = dev1840_datasets()
    _customers(models).fields.append(
        DatasetField(name="vip_spend", datatype=DataType.FLOAT, sql="spend",
               filter="orders.status = 'ok'"),
    )
    return models


def field_default_agg_datasets() -> List[SlayerDataset]:
    """Graph + a custom aggregation whose parameter *defaults* to a field (``weight`` → ``spend``)."""
    models = dev1840_datasets()
    _customers(models).aggregations.append(
        Aggregation(name="wsum", expression="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="spend")]),
    )
    return models


def assoc_q(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    return SlayerQuery(to_many_handling="associate", **kw)


def error_q(**kw) -> SlayerQuery:
    kw.setdefault("population", "orders")
    return SlayerQuery(to_many_handling="error", **kw)


def bcast_q(**kw) -> SlayerQuery:
    """Explicit broadcast."""
    kw.setdefault("population", "orders")
    return SlayerQuery(to_many_handling="broadcast", **kw)


def cust_q(**kw) -> SlayerQuery:
    """Rooted at customers (local-aggregate-over-a-fanning-dimension shapes)."""
    kw.setdefault("population", "customers")
    return SlayerQuery(**kw)


def associated_warnings(resp: SlayerResponse) -> list[AssociatedWarningPayload]:
    """The response's ``kind == "associated"`` warnings (non-additive cells)."""
    return [w for w in (resp.warnings or [])
            if isinstance(w, AssociatedWarningPayload)]


def pushed_filter_infos(resp: SlayerResponse) -> list[SemiJoinPushedWarningPayload]:
    """The response's semi-join-pushed informational entries."""
    return [w for w in (resp.warnings or [])
            if isinstance(w, SemiJoinPushedWarningPayload)]


# Oracles — hand-computed from the ``_dev1840_fixtures`` dataset (per-cell = distinct entities).
ASSOC_SPEND_BY_STATUS = {"ok": 420.0, "new": 290.0}
#: broadcast default: metric total over every customer incl. orderless c7.
BCAST_SPEND_CROSS = 515.0

ASSOC_LOCAL_SPEND_BY_STATUS = {"ok": 420.0, "new": 290.0}
#: naive join-multiplied ``ok`` cell (c1 counted twice) — must be unreachable.
LOCAL_OK_FAN_DEFECT = 520.0
LOCAL_SPEND_TOTAL = 515.0  # LOCAL spend broadcast total, incl. c7

ASSOC_COUNT_BY_STATUS = {"ok": 5, "new": 3}  # customers.*:count, distinct/cell
ASSOC_MEDIAN_BY_STATUS = {"ok": 80.0, "new": 100.0}  # odd populations
ASSOC_POP_BY_STATUS = {"ok": 300.0, "new": 100.0}  # distinct regions/cell
ASSOC_GOLD_SPEND_BY_STATUS = {"ok": 190.0, "new": 100.0}  # metric-local gold filter
ASSOC_APP_SPEND_BY_STATUS = {"ok": 140.0, "new": 250.0}  # host filter channel='app'
ASSOC_BASIC_SPEND_BY_STATUS = {"ok": 240.0, "new": 100.0}  # sibling plans.level='basic' pushed
SPEND_BY_TIER = {"gold": 245.0, "silver": 230.0, "bronze": 40.0}  # attributable, mode-invariant


def status_key(resp) -> dict:
    """Rows keyed by the ``orders.status`` cell."""
    return rows_by(resp, "orders.status")


__all__ = [
    "DatasetField", "DataType", "DatasetMetric", "SlayerQuery", "SlayerDataset",
    "FieldRef", "TimeDimension", "TimeGranularity", "DatasetRelationship",
    "dev1840_datasets", "dev1841_datasets", "keyless_root_datasets",
    "crossing_input_datasets", "field_default_agg_datasets",
    "make_exec_engine", "q", "assoc_q", "error_q", "bcast_q", "cust_q",
    "rows_by", "status_key", "broadcast_warnings",
    "associated_warnings", "pushed_filter_infos",
    "ASSOC_SPEND_BY_STATUS", "BCAST_SPEND_CROSS",
    "ASSOC_LOCAL_SPEND_BY_STATUS", "LOCAL_OK_FAN_DEFECT", "LOCAL_SPEND_TOTAL",
    "ASSOC_COUNT_BY_STATUS", "ASSOC_MEDIAN_BY_STATUS", "ASSOC_POP_BY_STATUS",
    "ASSOC_GOLD_SPEND_BY_STATUS", "ASSOC_APP_SPEND_BY_STATUS",
    "ASSOC_BASIC_SPEND_BY_STATUS", "SPEND_BY_TIER",
]
