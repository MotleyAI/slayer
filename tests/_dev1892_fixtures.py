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

from slayer.core.query import QueryDimension, QueryMetric

from tests._dev1841_fixtures import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DataType,
    SlayerDataset,
    dev1840_datasets,
)
from tests._dev1847_fixtures import (
    FieldRef,
    DatasetMetric,
    SlayerQuery,
    dev1847_datasets,
)

_ISSUE_RE = re.compile(r"DEV-\d+")


def assert_ref_free(message: str) -> None:
    """A user-facing typed error must not leak an issue reference."""
    assert not _ISSUE_RE.search(message), f"error leaks an issue reference: {message!r}"


def assert_grain_residue(exc: SlayerError, *, param: str) -> None:
    """The one-rule parameter error names the parameter, grain, and ``partition_by=`` remedy, ref-free."""
    msg = str(exc)
    assert param in msg, f"error does not name parameter {param!r}: {msg!r}"
    assert "grain" in msg.lower(), f"error does not name the grain: {msg!r}"
    assert "partition_by" in msg, f"error omits the partition_by remedy: {msg!r}"
    assert_ref_free(msg)


def _customers(datasets: List[SlayerDataset]) -> SlayerDataset:
    return next(m for m in datasets if m.name == "customers")


# --------------------------------------------------------------------------- #
# Helper model variants.
# --------------------------------------------------------------------------- #
def shared_default_name_datasets() -> List[SlayerDataset]:
    """Host and target both carry ``spend``; ``wsum`` on customers defaults ``weight`` to bare ``spend`` — must bind customers.spend (43400/34100), not the host's."""
    models = dev1840_datasets()
    _customers(models).aggregations.append(
        Aggregation(name="wsum", expression="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="spend")]),
    )
    orders = next(m for m in models if m.name == "orders")
    # A host column also named ``spend`` — host-anchoring would weight by this.
    orders.fields.append(DatasetField(name="spend", datatype=DataType.FLOAT, sql="amount"))
    return models


def expr_default_name_datasets() -> List[SlayerDataset]:
    """``wsum2`` weight default is a target-relative expression (``spend * 1``) — anchored on customers."""
    models = dev1840_datasets()
    _customers(models).aggregations.append(
        Aggregation(name="wsum2", expression="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="spend * 1")]),
    )
    return models


def toone_filter_datasets() -> List[SlayerDataset]:
    """customers gains ``north_spend`` (spend filtered on region) plus ``wsum``/``wsum4`` (``scale`` over to-one ``regions.pop``) for home-shorter-than-source rendering."""
    models = dev1840_datasets()
    cust = _customers(models)
    cust.fields.append(
        DatasetField(name="north_spend", datatype=DataType.FLOAT, sql="spend",
               filter="regions.name = 'North'"),
    )
    cust.aggregations.append(
        Aggregation(name="wsum", expression="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="spend")]),
    )
    cust.aggregations.append(
        Aggregation(name="wsum4", expression="SUM({value} * {weight} * {scale})",
                    params=[AggregationParam(name="weight", sql="spend"),
                            AggregationParam(name="scale", sql="regions.pop")]),
    )
    return models


def toone_default_datasets() -> List[SlayerDataset]:
    """``wsum3`` weight default is a to-one path (``regions.pop``) — joins customers→regions per entity."""
    models = dev1840_datasets()
    _customers(models).aggregations.append(
        Aggregation(name="wsum3", expression="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="regions.pop")]),
    )
    return models


def derived_default_datasets() -> List[SlayerDataset]:
    """``wsum5`` default is a to-one path to a DERIVED column (``regions.derived_pop`` = ``pop * 2``) — must expand its SQL."""
    models = dev1840_datasets()
    next(m for m in models if m.name == "regions").fields.append(
        DatasetField(name="derived_pop", datatype=DataType.FLOAT, sql="pop * 2"),
    )
    _customers(models).aggregations.append(
        Aggregation(name="wsum5", expression="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="regions.derived_pop")]),
    )
    return models


def fanning_derived_source_datasets() -> List[SlayerDataset]:
    """customers gains ``bad_spend`` = ``spend + orders.amount`` — a derived source crossing the fanning customers→orders hop, must fail closed."""
    models = dev1840_datasets()
    _customers(models).fields.append(
        DatasetField(name="bad_spend", datatype=DataType.FLOAT, sql="spend + orders.amount"),
    )
    return models


def qualified_expr_default_datasets() -> List[SlayerDataset]:
    """``wsum6`` weight default is an expression over a to-one qualified ref (``regions.pop * 2``)."""
    models = dev1840_datasets()
    _customers(models).aggregations.append(
        Aggregation(name="wsum6", expression="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="regions.pop * 2")]),
    )
    return models


def mixed_expr_default_datasets() -> List[SlayerDataset]:
    """``wsum7`` weight default mixes an owner column with a qualified ref (``spend * regions.pop``)."""
    models = dev1840_datasets()
    _customers(models).aggregations.append(
        Aggregation(name="wsum7", expression="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="spend * regions.pop")]),
    )
    return models


def root_named_expr_default_datasets() -> List[SlayerDataset]:
    """``wroot`` on customers: weight default ``orders.amount + 0`` names the query
    root, so it resolves root-local and widens the home to orders (DEV-1931)."""
    models = dev1840_datasets()
    _customers(models).aggregations.append(
        Aggregation(name="wroot", expression="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="orders.amount + 0")]),
    )
    return models


def self_qualified_expr_default_datasets() -> List[SlayerDataset]:
    """``wsum8`` weight default self-qualifies the owner (``customers.spend * 1``) — a local ref, not a hop."""
    models = dev1840_datasets()
    _customers(models).aggregations.append(
        Aggregation(name="wsum8", expression="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="customers.spend * 1")]),
    )
    return models


def derived_local_expr_default_datasets() -> List[SlayerDataset]:
    """``wsum9`` weight default mixes a DERIVED local column with a qualified ref (``double_spend * regions.pop``)."""
    models = dev1840_datasets()
    cust = _customers(models)
    cust.fields.append(
        DatasetField(name="double_spend", datatype=DataType.FLOAT, sql="spend * 2"),
    )
    cust.aggregations.append(
        Aggregation(name="wsum9", expression="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight",
                                             sql="double_spend * regions.pop")]),
    )
    return models


def literal_only_reagg_datasets() -> List[SlayerDataset]:
    """``wconst`` on sales: expr default referencing NO columns, only literals —
    must ride the plain default machinery, never a false ``amount`` dependency."""
    models = dev1847_datasets()
    sales = next(m for m in models if m.name == "sales")
    sales.aggregations.append(
        Aggregation(name="wconst", expression="SUM({value} * {weight}) / SUM({weight})",
                    params=[AggregationParam(
                        name="weight",
                        sql="CASE WHEN 'amount' = 'amount' THEN 2.0 ELSE 1.0 END")]),
    )
    return models


def unmodeled_physical_expr_default_datasets() -> List[SlayerDataset]:
    """``wphys`` weight default references ``tier`` after it is REMOVED from the model — a physical-only column, so the default fails closed."""
    models = dev1840_datasets()
    cust = _customers(models)
    cust.fields = [c for c in cust.fields if c.name != "tier"]
    cust.aggregations.append(
        Aggregation(name="wphys", expression="SUM({value} * {weight})",
                    params=[AggregationParam(
                        name="weight",
                        sql="CASE WHEN tier = 'gold' THEN 2.0 ELSE 1.0 END")]),
    )
    return models


def unparseable_expr_default_datasets() -> List[SlayerDataset]:
    """``wugly`` weight default that no dialect parses — must fail closed, never render raw."""
    models = dev1840_datasets()
    _customers(models).aggregations.append(
        Aggregation(name="wugly", expression="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql=")((( bad")]),
    )
    return models


def opaque_expr_default_datasets() -> List[SlayerDataset]:
    """``wopq`` weight default references an unresolvable qualifier (``nosuch.col``) — must fail closed."""
    models = dev1840_datasets()
    _customers(models).aggregations.append(
        Aggregation(name="wopq", expression="SUM({value} * {weight})",
                    params=[AggregationParam(name="weight", sql="nosuch.col + 1")]),
    )
    return models


def literal_collision_reagg_datasets() -> List[SlayerDataset]:
    """``wlit`` expr default whose string literal equals a non-grain column name (``'amount'``) — must not read as a dependency."""
    models = dev1847_datasets()
    sales = next(m for m in models if m.name == "sales")
    sales.aggregations.append(
        Aggregation(name="wlit", expression="SUM({value} * {weight}) / SUM({weight})",
                    params=[AggregationParam(
                        name="weight",
                        sql="CASE WHEN region = 'amount' THEN 1.0 ELSE 2.0 END")]),
    )
    return models


def qualified_reagg_default_datasets() -> List[SlayerDataset]:
    """``cwavg`` weight default is a qualified expr over the to-one chain, FK-seeded by the operand grain (``customers.region_id * 1``)."""
    models = dev1847_datasets()
    corders = next(m for m in models if m.name == "corders")
    corders.aggregations.append(
        Aggregation(name="cwavg", expression="SUM({value} * {weight}) / SUM({weight})",
                    params=[AggregationParam(name="weight",
                                             sql="customers.region_id * 1")]),
    )
    return models


def weighted_source_queries_dataset() -> SlayerDataset:
    """Manual two-stage ``weighted_avg(sum(amount), weight=count(id))`` by region — the independent oracle the re-aggregation must match."""
    return SlayerDataset(
        name="wavg_city_by_region", data_source="test",
        source_queries=[
            SlayerQuery(
                name="cr_wtotals", population="sales",
                dimensions=[QueryDimension(field="sales.region"), QueryDimension(field="sales.city")],
                metrics=[QueryMetric(metric="sum(sales.amount)", alias="ct"),
                          QueryMetric(metric="count(sales.id)", alias="cc")]),
            SlayerQuery(
                population="cr_wtotals",
                dimensions=[QueryDimension(field="cr_wtotals.region")],
                metrics=[QueryMetric(metric="weighted_avg(cr_wtotals.ct, weight=cr_wtotals.cc)",
                                       alias="w")]),
        ])


def weighted_sales_datasets() -> List[SlayerDataset]:
    return dev1847_datasets() + [weighted_source_queries_dataset()]


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
#: wsum6 (weight default regions.pop * 2) = 2 * SUM(spend * pop) per cell.
ASSOC_WSUM6_BY_STATUS = {"ok": 112000.0, "new": 50000.0}
#: wsum7 (weight default spend * regions.pop) = SUM(spend^2 * pop) per cell.
ASSOC_WSUM7_BY_STATUS = {"ok": 5340000.0, "new": 3250000.0}
#: wsum9 (weight default double_spend * regions.pop) = 2 * SUM(spend^2 * pop).
ASSOC_WSUM9_BY_STATUS = {"ok": 10680000.0, "new": 6500000.0}

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
    "Aggregation", "AggregationParam", "DatasetField", "DataType", "SlayerDataset",
    "FieldRef", "DatasetMetric", "SlayerQuery", "SlayerError",
    "assert_ref_free", "assert_grain_residue",
    "shared_default_name_datasets", "expr_default_name_datasets",
    "toone_filter_datasets", "toone_default_datasets", "derived_default_datasets",
    "fanning_derived_source_datasets", "weighted_source_queries_dataset",
    "weighted_sales_datasets",
    "qualified_expr_default_datasets", "mixed_expr_default_datasets",
    "root_named_expr_default_datasets", "opaque_expr_default_datasets",
    "self_qualified_expr_default_datasets", "derived_local_expr_default_datasets",
    "unmodeled_physical_expr_default_datasets", "unparseable_expr_default_datasets",
    "literal_collision_reagg_datasets", "literal_only_reagg_datasets",
    "qualified_reagg_default_datasets",
    "ASSOC_WAVG_SPEND_BY_STATUS", "ASSOC_WAVG_OK_FAN_DEFECT",
    "ASSOC_WSUM_BY_STATUS", "ASSOC_WAVG_POP_BY_STATUS",
    "ASSOC_WSUM6_BY_STATUS", "ASSOC_WSUM7_BY_STATUS", "ASSOC_WSUM9_BY_STATUS",
    "WAVG_CITY_BY_REGION", "UNWEIGHTED_CITY_BY_REGION",
    "DEGENERATE_WAVG_BY_REGION", "CORDERS_GLOBAL_WAVG",
    "CORDERS_GLOBAL_UNWEIGHTED", "SEEDED_AVG_BY_REGION_ID",
    "FK_SEED_AVG_BY_REGION_NAME",
]
