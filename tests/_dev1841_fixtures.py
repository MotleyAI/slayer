"""Shared fixtures for DEV-1841 — the ``to_many_handling`` mode axis and the
distinct-entity association producer.

Built on top of ``tests/_dev1840_fixtures.py``: same orders → customers →
regions → plans/stores graph, same dual-engine (SQLite + DuckDB) seeded
dataset, same ``make_exec_engine``. That dataset already carries the shape this
issue turns on — customer 1 has TWO ``ok`` orders (o1, o10), so a naive join
double-counts c1's spend in the ``ok`` cell.

All oracles below are hand-computed from the DEV-1840 dataset (see that module's
docstring for the rows). Distinct-entity populations by orders-level status:

    ok  orders → customers {c1,c2,c3,c5,c6}     new orders → customers {c1,c2,c4}
    (o8 is an orphan order — no customer — so it joins to nothing.)

Everything a test constructs with ``to_many_handling`` is built lazily inside a
function (never at module import) so a missing field fails per-test, not at
collection.
"""

from __future__ import annotations

from typing import List

from slayer.core.models import Column, SlayerModel

from tests._dev1836_fixtures import (
    broadcast_warnings,
    dropped_filter_warnings,
    rows_by,
)
from tests._dev1840_fixtures import (
    ColumnRef,
    DataType,
    ModelJoin,
    ModelMeasure,
    SlayerQuery,
    TimeDimension,
    TimeGranularity,
    dev1840_models,
    make_exec_engine,
    q,
)


# --------------------------------------------------------------------------- #
# Model variants.
# --------------------------------------------------------------------------- #
def _customers(models: List[SlayerModel]) -> SlayerModel:
    return next(m for m in models if m.name == "customers")


def dev1841_models(**kw) -> List[SlayerModel]:
    """DEV-1840 graph plus a measure-local filtered column on customers:
    ``gold_spend`` = ``spend`` restricted (inside the aggregate) to gold tier."""
    models = dev1840_models(**kw)
    _customers(models).columns.append(
        Column(name="gold_spend", type=DataType.DOUBLE, sql="spend",
               filter="tier = 'gold'"),
    )
    return models


def keyless_root_models() -> List[SlayerModel]:
    """The graph with customers stripped of its unique key — association cannot
    dedup entities, so associate-mode resolution must fail closed."""
    models = dev1840_models()
    cust = _customers(models)
    cust.columns = [
        (c.model_copy(update={"primary_key": False}) if c.name == "id" else c)
        for c in cust.columns
    ]
    return models


# --------------------------------------------------------------------------- #
# Query shorthands (lazy — the mode field may not exist yet).
# --------------------------------------------------------------------------- #
def assoc_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(to_many_handling="associate", **kw)


def error_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(to_many_handling="error", **kw)


def bcast_q(**kw) -> SlayerQuery:
    """Explicit broadcast — identical to the default, spelled out."""
    kw.setdefault("source_model", "orders")
    return SlayerQuery(to_many_handling="broadcast", **kw)


def cust_q(**kw) -> SlayerQuery:
    """Rooted at customers (local-aggregate-over-a-fanning-dimension shapes)."""
    kw.setdefault("source_model", "customers")
    return SlayerQuery(**kw)


def associated_warnings(resp) -> list:
    """The response's ``kind == "associated"`` warnings (non-additive cells)."""
    return [w for w in (resp.warnings or [])
            if getattr(w, "kind", None) == "associated"]


def pushed_filter_infos(resp) -> list:
    """The response's semi-join-pushed informational entries (DEV-1841 amends
    DEV-1840's silence)."""
    return [w for w in (resp.warnings or [])
            if getattr(w, "kind", None) == "semi_join_pushed"]


# --------------------------------------------------------------------------- #
# Oracles (hand-computed; see module docstring + DEV-1840 dataset).
# --------------------------------------------------------------------------- #
#: rooted orders, ``customers.spend:sum`` by status — distinct customers/cell.
ASSOC_SPEND_BY_STATUS = {"ok": 420.0, "new": 290.0}
#: the broadcast (default) value — the metric total over EVERY customer (incl.
#: orderless c7), repeated across cells; unchanged from DEV-1840 (its "515").
BCAST_SPEND_CROSS = 515.0

#: rooted customers, LOCAL ``spend:sum`` by orders.status — same populations.
ASSOC_LOCAL_SPEND_BY_STATUS = {"ok": 420.0, "new": 290.0}
#: the naive join-multiplied ``ok`` cell (c1 counted twice) — must be unreachable.
LOCAL_OK_FAN_DEFECT = 520.0
#: LOCAL ``spend:sum`` over every customer (the broadcast total, incl. c7).
LOCAL_SPEND_TOTAL = 515.0

#: rooted orders, ``customers.*:count`` by status — distinct customers/cell.
ASSOC_COUNT_BY_STATUS = {"ok": 5, "new": 3}

#: rooted orders, ``customers.spend:median`` by status — odd populations, so the
#: median is an exact middle element (interpolation-agnostic across engines).
ASSOC_MEDIAN_BY_STATUS = {"ok": 80.0, "new": 100.0}

#: rooted orders, ``customers.regions.pop:sum`` by status — distinct REGIONS/cell
#: (metric root is regions, two hops from the host).
ASSOC_POP_BY_STATUS = {"ok": 300.0, "new": 100.0}

#: rooted orders, ``customers.gold_spend:sum`` by status — the measure-local
#: filter restricts the association to gold customers before per-cell summing.
ASSOC_GOLD_SPEND_BY_STATUS = {"ok": 190.0, "new": 100.0}

#: rooted orders, ``customers.spend:sum`` by status, host-local filter
#: ``channel = 'app'`` applied inline (app orders: o2 c1, o4 c2, o5 c3, o7 c5).
ASSOC_APP_SPEND_BY_STATUS = {"ok": 140.0, "new": 250.0}

#: rooted orders (weak plans), ``customers.spend:sum`` by status, sibling-branch
#: filter ``customers.plans.level = 'basic'`` pushed by semi-join into the
#: association (basic customers c1/c3/c5).
ASSOC_BASIC_SPEND_BY_STATUS = {"ok": 240.0, "new": 100.0}

#: fully-attributable slice — mode-invariant (from DEV-1840's SPEND_ALL_BY_TIER).
SPEND_BY_TIER = {"gold": 245.0, "silver": 230.0, "bronze": 40.0}


def status_key(resp, root: str = "orders") -> dict:
    """Rows keyed by the status cell for a ``root``-rooted query."""
    col = f"{root}.status" if root == "orders" else f"{root}.orders.status"
    return rows_by(resp, col)


__all__ = [
    "Column", "DataType", "ModelMeasure", "SlayerQuery", "SlayerModel",
    "ColumnRef", "TimeDimension", "TimeGranularity", "ModelJoin",
    "dev1840_models", "dev1841_models", "keyless_root_models",
    "make_exec_engine", "q", "assoc_q", "error_q", "bcast_q", "cust_q",
    "rows_by", "status_key", "broadcast_warnings", "dropped_filter_warnings",
    "associated_warnings", "pushed_filter_infos",
    "ASSOC_SPEND_BY_STATUS", "BCAST_SPEND_CROSS",
    "ASSOC_LOCAL_SPEND_BY_STATUS", "LOCAL_OK_FAN_DEFECT", "LOCAL_SPEND_TOTAL",
    "ASSOC_COUNT_BY_STATUS", "ASSOC_MEDIAN_BY_STATUS", "ASSOC_POP_BY_STATUS",
    "ASSOC_GOLD_SPEND_BY_STATUS", "ASSOC_APP_SPEND_BY_STATUS",
    "ASSOC_BASIC_SPEND_BY_STATUS", "SPEND_BY_TIER",
]
