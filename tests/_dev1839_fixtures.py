"""Shared fixtures for DEV-1839 — grain-union broadcasting for different-grain
aggregates.

Reuses the DEV-1739/1824 models, dataset, and execution harness verbatim
(``tests/_dev1824_fixtures.py``) and adds hand-computed oracles for the
union-grain scenarios, chosen so union-grain evaluation and any misgrained
evaluation differ visibly.

Oracle derivations (from the DEV-1739 rows; totals in ``_dev1824_fixtures``)
----------------------------------------------------------------------------
REGION_TOTAL   North=100 South=50 NULL=60; GRAND_TOTAL=210.
CITY_TOTAL     (N,CityA)=30 (N,CityB)=40 (N,NULL)=30 (S,CityC)=50 (NULL,CityD)=60
               (city values are region-unique, so partition_by=city matches).

MIXED_RANK     rank(region_total - city_total) over the (region, city) union:
               diffs 70 / 60 / 70 / 0 / 0 → RANK() desc 1 / 3 / 1 / 4 / 4.
KEYLESS_RANK   rank(region_total / grand_total) over the region union:
               shares 100/210, 50/210, 60/210 → North=1 NULL=2 South=3.
               (A keyless divisor misgrained to region makes every share 1.0
               and every rank tie at 1.)
SUBSET_RANK    rank(citypair_total - region_total): the (region, city)-grain
               aggregate is exactly the union, region is a strict subset;
               diffs -70 / -60 / -70 / 0 / 0 → ranks 4 / 3 / 4 / 1 / 1.
NESTED_RANK    rank(cumsum(region_month_total) - city_total) over the
               (region, city, month) union. cumsum at its own (region, month)
               grain (months within region): (N,Jan)=30 (N,Feb)=100 (S,Jan)=25
               (S,Mar)=50 (NULL,Mar)=60. Union-row values: (N,CityA,Jan)=0
               (N,CityB,Feb)=60 (N,NULL,Feb)=70 (S,CityC,Jan)=-25
               (S,CityC,Mar)=0 (NULL,CityD,Mar)=0 → ranks 3 / 2 / 1 / 6 / 3 / 3.
EXPLICIT_PART  MIXED_RANK re-partitioned by region: North diffs 70/70/60 →
               1/1/3; single South and NULL-region rows → 1.
DUAL_MEASURE   MIXED_RANK's expression as a MEASURE at the (region, city,
               channel) query grain: broadcast diffs 70×3, 60, 0×3 →
               ranks 1/1/1/4/5/5/5.
SAMEGRAIN_RANK rank(amount_region + ok_region): 200 / 75 / 120 →
               North=1 NULL=2 South=3 (ok_amount region totals 100 / 25 / 60).
MEASURE_DIFF   region_total - city_total per (region, city) row: 70/60/70/0/0.
SAMEGRAIN_DIFF amount_region - ok_region per region: N=0 S=25 NULL=0.
"""

from __future__ import annotations

from tests._dev1824_fixtures import (  # noqa: F401 — re-exported fixture surface
    CITY_TOTAL,
    ColumnRef,
    GRAND_TOTAL,
    ModelMeasure,
    RCM_GROUPS,
    RC_GROUPS,
    REGION_MONTH_TOTAL,
    REGION_TOTAL,
    SlayerQuery,
    TimeDimension,
    TimeGranularity,
    dev1824_models,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    q,
    rows_by,
)

dev1839_models = dev1824_models

# --------------------------------------------------------------------------- #
# Dimension expressions under test.
# --------------------------------------------------------------------------- #
MIXED_RANK = "rank(amount:sum(partition_by=region) - amount:sum(partition_by=city))"
KEYLESS_RANK = "rank(amount:sum(partition_by=region) / amount:sum(partition_by=[]))"
SUBSET_RANK = (
    "rank(amount:sum(partition_by=[region, city]) - amount:sum(partition_by=region))"
)
NESTED_RANK = (
    "rank(cumsum(amount:sum(partition_by=[region, ordered_at])) - "
    "amount:sum(partition_by=city))"
)
EXPLICIT_PART_RANK = (
    "rank(amount:sum(partition_by=region) - amount:sum(partition_by=city), "
    "partition_by=region)"
)
SAMEGRAIN_RANK = (
    "rank(amount:sum(partition_by=region) + ok_amount:sum(partition_by=region))"
)
MEASURE_DIFF = "amount:sum(partition_by=region) - amount:sum(partition_by=city)"
SAMEGRAIN_DIFF = (
    "amount:sum(partition_by=region) - ok_amount:sum(partition_by=region)"
)

# --------------------------------------------------------------------------- #
# Oracles, keyed by result-grain tuples.
# --------------------------------------------------------------------------- #
MIXED_DIFF_OF = {
    ("North", "CityA"): 70.0, ("North", "CityB"): 60.0, ("North", None): 70.0,
    ("South", "CityC"): 0.0, (None, "CityD"): 0.0,
}
MIXED_RANK_OF = {
    ("North", "CityA"): 1, ("North", "CityB"): 3, ("North", None): 1,
    ("South", "CityC"): 4, (None, "CityD"): 4,
}
KEYLESS_RANK_OF = {"North": 1, None: 2, "South": 3}
SUBSET_RANK_OF = {
    ("North", "CityA"): 4, ("North", "CityB"): 3, ("North", None): 4,
    ("South", "CityC"): 1, (None, "CityD"): 1,
}
REGION_CUMSUM = {
    ("North", "2024-01"): 30.0, ("North", "2024-02"): 100.0,
    ("South", "2024-01"): 25.0, ("South", "2024-03"): 50.0,
    (None, "2024-03"): 60.0,
}
NESTED_RANK_OF = {
    ("North", "CityA", "2024-01"): 3, ("North", "CityB", "2024-02"): 2,
    ("North", None, "2024-02"): 1, ("South", "CityC", "2024-01"): 6,
    ("South", "CityC", "2024-03"): 3, (None, "CityD", "2024-03"): 3,
}
EXPLICIT_PART_RANK_OF = {
    ("North", "CityA"): 1, ("North", "CityB"): 3, ("North", None): 1,
    ("South", "CityC"): 1, (None, "CityD"): 1,
}
#: The (region, city, month) query grain — 6 groups with their amount sums.
RCM_TOTAL = {
    ("North", "CityA", "2024-01"): 30.0, ("North", "CityB", "2024-02"): 40.0,
    ("North", None, "2024-02"): 30.0, ("South", "CityC", "2024-01"): 25.0,
    ("South", "CityC", "2024-03"): 25.0, (None, "CityD", "2024-03"): 60.0,
}
#: The (region, city, channel) query grain — 7 groups with their amount sums.
RCC_TOTAL = {
    ("North", "CityA", "web"): 10.0, ("North", "CityA", "app"): 20.0,
    ("North", "CityB", "web"): 40.0, ("North", None, "web"): 30.0,
    ("South", "CityC", "web"): 25.0, ("South", "CityC", "app"): 25.0,
    (None, "CityD", "web"): 60.0,
}
#: MIXED_RANK's expression as a measure at the (region, city, channel) grain.
DUAL_MEASURE_RANK_OF = {
    ("North", "CityA", "web"): 1, ("North", "CityA", "app"): 1,
    ("North", "CityB", "web"): 4, ("North", None, "web"): 1,
    ("South", "CityC", "web"): 5, ("South", "CityC", "app"): 5,
    (None, "CityD", "web"): 5,
}
SAMEGRAIN_RANK_OF = {"North": 1, None: 2, "South": 3}
SAMEGRAIN_DIFF_OF = {"North": 0.0, "South": 25.0, None: 0.0}
OK_REGION_TOTAL = {"North": 100.0, "South": 25.0, None: 60.0}


__all__ = [
    "CITY_TOTAL", "ColumnRef", "DUAL_MEASURE_RANK_OF", "EXPLICIT_PART_RANK",
    "EXPLICIT_PART_RANK_OF", "GRAND_TOTAL", "KEYLESS_RANK", "KEYLESS_RANK_OF",
    "MEASURE_DIFF", "MIXED_DIFF_OF", "MIXED_RANK", "MIXED_RANK_OF",
    "ModelMeasure", "NESTED_RANK", "NESTED_RANK_OF", "OK_REGION_TOTAL",
    "RCC_TOTAL", "RCM_GROUPS", "RCM_TOTAL", "RC_GROUPS", "REGION_CUMSUM",
    "REGION_MONTH_TOTAL", "REGION_TOTAL", "SAMEGRAIN_DIFF", "SAMEGRAIN_DIFF_OF",
    "SAMEGRAIN_RANK", "SAMEGRAIN_RANK_OF", "SUBSET_RANK", "SUBSET_RANK_OF",
    "SlayerQuery", "TimeDimension", "TimeGranularity", "dev1839_models", "gen",
    "make_exec_engine", "month_key", "month_td", "q", "rows_by",
]
