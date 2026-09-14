"""Shared fixtures for DEV-1892 — column-reference / aggregate-valued parameters
lifted onto the two-level kernel (association + re-aggregation).

Builds on ``_dev1841_fixtures`` (orders→customers graph, associate mode) and
``_dev1847_fixtures`` (sales / corders re-aggregation graph). Every oracle below
is hand-computed from those seed rows and cross-checked by the executed-value
suites; the manual two-stage ``source_queries`` encoding is the independent
reference for the weighted re-aggregation.
"""

from __future__ import annotations

import re
from typing import List

from slayer.core.errors import SlayerError

from tests._dev1841_fixtures import (
    Aggregation,
    AggregationParam,
    Column,
    DataType,
    SlayerModel,
    dev1840_models,
)
from tests._dev1847_fixtures import (
    ColumnRef,
    ModelMeasure,
    SlayerQuery,
    dev1847_models,
)

_ISSUE_RE = re.compile(r"DEV-\d+")


def assert_ref_free(message: str) -> None:
    """A user-facing typed error must not leak an issue reference."""
    assert not _ISSUE_RE.search(message), f"error leaks an issue reference: {message!r}"


def assert_grain_residue(exc: SlayerError, *, param: str) -> None:
    """The one-rule parameter error names the parameter, the grain, and the
    ``partition_by=`` remedy — ref-free (the discriminator from the old gate,
    whose message named neither the grain nor partition_by)."""
    msg = str(exc)
    assert param in msg, f"error does not name parameter {param!r}: {msg!r}"
    assert "grain" in msg.lower(), f"error does not name the grain: {msg!r}"
    assert "partition_by" in msg, f"error omits the partition_by remedy: {msg!r}"
    assert_ref_free(msg)


def _customers(models: List[SlayerModel]) -> SlayerModel:
    return next(m for m in models if m.name == "customers")


# --------------------------------------------------------------------------- #
# Helper model variants.
# --------------------------------------------------------------------------- #
def shared_default_name_models() -> List[SlayerModel]:
    """Both host (orders) and target (customers) carry a ``spend`` column, and a
    custom ``wsum`` on customers defaults ``weight`` to the bare name ``spend``.
    The owner-anchored default must bind customers.spend (43400/34100), never the
    host's per-order ``spend`` — the anchoring regression."""
    models = dev1840_models()
    _customers(models).aggregations.append(
        Aggregation(name="wsum", formula="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="spend")]),
    )
    orders = next(m for m in models if m.name == "orders")
    # A host column also named ``spend`` — host-anchoring would weight by this.
    orders.columns.append(Column(name="spend", type=DataType.DOUBLE, sql="amount"))
    return models


def expr_default_name_models() -> List[SlayerModel]:
    """A custom ``wsum2`` whose ``weight`` default is a target-relative EXPRESSION
    (``spend * 1``): the owner path must anchor the expansion on customers."""
    models = dev1840_models()
    _customers(models).aggregations.append(
        Aggregation(name="wsum2", formula="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="spend * 1")]),
    )
    return models


def toone_filter_models() -> List[SlayerModel]:
    """customers gains ``north_spend`` (spend filtered on ``regions.name='North'``),
    plus ``wsum`` (weight default ``spend``) and ``wsum4`` (``scale`` default over
    the to-one ``regions.pop``) for home-shorter-than-source rendering."""
    models = dev1840_models()
    cust = _customers(models)
    cust.columns.append(
        Column(name="north_spend", type=DataType.DOUBLE, sql="spend",
               filter="regions.name = 'North'"),
    )
    cust.aggregations.append(
        Aggregation(name="wsum", formula="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="spend")]),
    )
    cust.aggregations.append(
        Aggregation(name="wsum4", formula="SUM({value} * {weight} * {scale})",
                    params=[AggregationParam(name="weight", sql="spend"),
                            AggregationParam(name="scale", sql="regions.pop")]),
    )
    return models


def toone_default_models() -> List[SlayerModel]:
    """A custom ``wsum3`` on customers whose ``weight`` default is a to-one path
    (``regions.pop``): the owner-anchored default joins customers→regions per entity."""
    models = dev1840_models()
    _customers(models).aggregations.append(
        Aggregation(name="wsum3", formula="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="regions.pop")]),
    )
    return models


def derived_default_models() -> List[SlayerModel]:
    """``wsum5`` on customers with a default over a to-one path to a DERIVED column
    (``regions.derived_pop`` = ``pop * 2``): the default must expand its SQL."""
    models = dev1840_models()
    next(m for m in models if m.name == "regions").columns.append(
        Column(name="derived_pop", type=DataType.DOUBLE, sql="pop * 2"),
    )
    _customers(models).aggregations.append(
        Aggregation(name="wsum5", formula="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="regions.derived_pop")]),
    )
    return models


def fanning_derived_source_models() -> List[SlayerModel]:
    """customers gains ``bad_spend`` = ``spend + orders.amount``: a derived source
    crossing the fanning customers→orders hop, which must fail closed."""
    models = dev1840_models()
    _customers(models).columns.append(
        Column(name="bad_spend", type=DataType.DOUBLE, sql="spend + orders.amount"),
    )
    return models


def weighted_source_queries_model() -> SlayerModel:
    """The manual two-stage encoding of
    ``weighted_avg(sum(amount, partition_by=[city, region]),
    weight=count(id, partition_by=[city, region]))`` by region — the independent
    oracle the re-aggregation must match."""
    return SlayerModel(
        name="wavg_city_by_region", data_source="test",
        source_queries=[
            SlayerQuery(
                name="cr_wtotals", source_model="sales",
                dimensions=[ColumnRef(name="region"), ColumnRef(name="city")],
                measures=[ModelMeasure(formula="amount:sum", name="ct"),
                          ModelMeasure(formula="id:count", name="cc")]),
            SlayerQuery(
                source_model="cr_wtotals",
                dimensions=[ColumnRef(name="region")],
                measures=[ModelMeasure(formula="ct:weighted_avg(weight=cc)",
                                       name="w")]),
        ])


def weighted_sales_models() -> List[SlayerModel]:
    return dev1847_models() + [weighted_source_queries_model()]


# Association oracles (per-cell = DISTINCT customers by status:
# ok {c1,c2,c3,c5,c6}, new {c1,c2,c4}; spends c1 100 c2 150 c3 60 c4 40 c5 80 c6 30).
#: weighted_avg(spend, weight=spend) = SUM(spend^2) / SUM(spend) per cell.
ASSOC_WAVG_SPEND_BY_STATUS = {"ok": 43400.0 / 420.0, "new": 34100.0 / 290.0}
#: naive join-multiplied ``ok`` (c1 counted twice: +100^2 numerator, +100 denom).
ASSOC_WAVG_OK_FAN_DEFECT = 53400.0 / 520.0
#: wsum default (weight defaults to the spend column) = SUM(spend^2) per cell.
ASSOC_WSUM_BY_STATUS = {"ok": 43400.0, "new": 34100.0}
#: weighted_avg(spend, weight=customers.regions.pop): ok=56000/700; new drops
#: c4's NULL-weight term, denom NULLIF -> 25000/200.
ASSOC_WAVG_POP_BY_STATUS = {"ok": 56000.0 / 700.0, "new": 25000.0 / 200.0}

# Re-aggregation oracles (sales graph; [city, region] cell totals / id counts).
#   North: Alpha 30/3, Beta 60/1 | South: Alpha 40/2, Gamma 100/1
#   East: Delta 50/1, Epsilon 50/1, Zeta 80/1 | Gap: NULL 12/2, Kappa 8/1
#   Void: Xi NULL/2
#: weighted_avg(city_total, weight=id_count) by region = SUM(t*c)/SUM(c).
#: Void's only cell has a NULL total -> NULL.
WAVG_CITY_BY_REGION = {
    "North": 150.0 / 4.0,   # (30*3 + 60*1) / 4  = 37.5
    "South": 180.0 / 3.0,   # (40*2 + 100*1) / 3 = 60.0
    "East": 180.0 / 3.0,    # (50+50+80) / 3     = 60.0
    "Gap": 32.0 / 3.0,      # (12*2 + 8*1) / 3   = 10.6667
    "Void": None,
}
#: the UNWEIGHTED avg (each city once) — the distinguishable wrong value where
#: the weights differ (North 45 vs 37.5; Gap 10 vs 10.667).
UNWEIGHTED_CITY_BY_REGION = {"North": 45.0, "South": 70.0, "East": 60.0, "Gap": 10.0}
#: degenerate weighted identity: weighted_avg over a single (region) cell = the
#: region total (weight cancels). Void's only cell has a NULL total -> NULL.
DEGENERATE_WAVG_BY_REGION = {
    "North": 90.0, "South": 140.0, "East": 180.0, "Gap": 20.0, "Void": None}
#: corders global: weighted_avg(sum(amount, partition_by=customer_id),
#: weight=customers.region_id). cells c1 v30/w1, c2 v40/w1, c3 v100/w2.
CORDERS_GLOBAL_WAVG = 270.0 / 4.0  # 67.5
#: the unweighted corders global avg — distinguishable from the weighted value.
CORDERS_GLOBAL_UNWEIGHTED = 170.0 / 3.0
#: nested-path entity-key seed: avg(sum(amount, partition_by=customers.id)) by
#: region_id — customer cells c1 30 / c2 40 (region 1), c3 100 (region 2).
SEEDED_AVG_BY_REGION_ID = {1: 35.0, 2: 100.0}
#: foreign-key seed (Axiom 1): avg(sum(amount, partition_by=customers.region_id))
#: by regions.name — region_id cells 1=70 (c1 10+20, c2 40), 2=100 (c3); each
#: region_id pins one region row, so the to-one target name is attributable.
FK_SEED_AVG_BY_REGION_NAME = {"North": 70.0, "South": 100.0}


__all__ = [
    "Aggregation", "AggregationParam", "Column", "DataType", "SlayerModel",
    "ColumnRef", "ModelMeasure", "SlayerQuery", "SlayerError",
    "assert_ref_free", "assert_grain_residue",
    "shared_default_name_models", "expr_default_name_models",
    "toone_filter_models", "toone_default_models", "derived_default_models",
    "fanning_derived_source_models", "weighted_source_queries_model",
    "weighted_sales_models",
    "ASSOC_WAVG_SPEND_BY_STATUS", "ASSOC_WAVG_OK_FAN_DEFECT",
    "ASSOC_WSUM_BY_STATUS", "ASSOC_WAVG_POP_BY_STATUS",
    "WAVG_CITY_BY_REGION", "UNWEIGHTED_CITY_BY_REGION",
    "DEGENERATE_WAVG_BY_REGION", "CORDERS_GLOBAL_WAVG",
    "CORDERS_GLOBAL_UNWEIGHTED", "SEEDED_AVG_BY_REGION_ID",
    "FK_SEED_AVG_BY_REGION_NAME",
]
