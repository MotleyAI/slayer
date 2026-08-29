"""Shared fixtures for DEV-1824 — the deferred ``partition_by=`` shapes
(``window=``, ``first``/``last``, transform nesting, filter references) and the
computed-dimension symmetry lifts.

Reuses the DEV-1739 models and dataset verbatim (``tests/_dev1739_fixtures.py``:
``orders`` with region/city/channel/ordered_at, ``orders → customers → regions``)
and adds the hand-computed oracles for the newly lifted shapes.

Oracle derivations (from the DEV-1739 rows)
-------------------------------------------
orders (id, cust, region, city, channel, amount, status, ordered_at):
   1  c1  North  CityA  web   10  ok    2024-01-10
   2  c1  North  CityA  app   20  ok    2024-01-20
   3  c2  North  CityB  web   40  ok    2024-02-10
   4  c2  North  NULL   web   30  ok    2024-02-15
   5  c3  South  CityC  web   25  ok    2024-01-25
   6  c3  South  CityC  app   25  hold  2024-03-05
   7  c1  NULL   CityD  web   60  ok    2024-03-10

REGION_TOTAL          amount:sum by region: North=100 South=50 NULL=60.
CITY_TOTAL            amount:sum by (region, city): see rows above.
REGION_MONTH_TOTAL    amount:sum by (region, month).
TRAILING_90D_REGION   amount:sum(window='90d', partition_by=region) as of each
                      (region, month) bucket = region running total: the widest
                      row-to-bucket-end gap is 66 days (South Jan 25 → Mar 31),
                      so a 90d trailing window covers every prior region row:
                      (N,Jan)=30 (N,Feb)=30+70=100 (S,Jan)=25 (S,Mar)=50
                      (NULL,Mar)=60.
TRAILING_45D_REGION   the duration-sensitive variant: 45d back from Feb's end
                      (~Jan 15) drops North's Jan 10 row → (N,Feb)=20+40+30=90;
                      45d back from Mar's end (~Feb 15) drops South's Jan 25
                      row → (S,Mar)=25. Distinguishes real duration handling
                      from a running total. No row sits within 4 days of a
                      window boundary, so bucket-end conventions cannot flip
                      any value.
REGION_LAST/_FIRST    amount:last/first(partition_by=region), ranked by
                      ordered_at (model default_time_dimension): North last is
                      row 4 (Feb 15 → 30), first row 1 (Jan 10 → 10); South last
                      row 6 (Mar 5 → 25), first row 5 (25); NULL row 7 (60).
REGION_MONTH_LAST     amount:last(partition_by=[region, ordered_at]) at month
                      grain: (N,Jan): Jan 20 → 20; (N,Feb): Feb 15 → 30;
                      (S,Jan)=25; (S,Mar)=25; (NULL,Mar)=60.
REGION_LAST_AT        ordered_at:last(partition_by=region): the region's latest
                      order date (as YYYY-MM-DD).
BAND35 / BAND35_OF    computed dimension banding city totals at >35:
                      CityA 30→0, CityB 40→1, North-NULL 30→0, CityC 50→1,
                      CityD 60→1.
GRAND_TOTAL           210.
"""

from __future__ import annotations

from tests._dev1739_fixtures import (  # noqa: F401 — re-exported fixture surface
    ColumnRef,
    ModelMeasure,
    SlayerQuery,
    TimeDimension,
    TimeGranularity,
    approx_sum,
    cm_cte_bodies,
    customers_model,
    dev1739_models,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    orders_model,
    regions_model,
    rows_by,
)

dev1824_models = dev1739_models

REGION_TOTAL = {"North": 100.0, "South": 50.0, None: 60.0}
CITY_TOTAL = {
    ("North", "CityA"): 30.0, ("North", "CityB"): 40.0, ("North", None): 30.0,
    ("South", "CityC"): 50.0, (None, "CityD"): 60.0,
}
REGION_MONTH_TOTAL = {
    ("North", "2024-01"): 30.0, ("North", "2024-02"): 70.0,
    ("South", "2024-01"): 25.0, ("South", "2024-03"): 25.0,
    (None, "2024-03"): 60.0,
}
TRAILING_90D_REGION = {
    ("North", "2024-01"): 30.0, ("North", "2024-02"): 100.0,
    ("South", "2024-01"): 25.0, ("South", "2024-03"): 50.0,
    (None, "2024-03"): 60.0,
}
TRAILING_45D_REGION = {
    ("North", "2024-01"): 30.0, ("North", "2024-02"): 90.0,
    ("South", "2024-01"): 25.0, ("South", "2024-03"): 25.0,
    (None, "2024-03"): 60.0,
}
REGION_LAST = {"North": 30.0, "South": 25.0, None: 60.0}
REGION_FIRST = {"North": 10.0, "South": 25.0, None: 60.0}
REGION_MONTH_LAST = {
    ("North", "2024-01"): 20.0, ("North", "2024-02"): 30.0,
    ("South", "2024-01"): 25.0, ("South", "2024-03"): 25.0,
    (None, "2024-03"): 60.0,
}
REGION_LAST_AT = {"North": "2024-02-15", "South": "2024-03-05", None: "2024-03-10"}
GRAND_TOTAL = 210.0

#: The (region, city) result grain — 5 groups; (region, city, month) — 6.
RC_GROUPS = frozenset(CITY_TOTAL)
RCM_GROUPS = frozenset({
    ("North", "CityA", "2024-01"), ("North", "CityB", "2024-02"),
    ("North", None, "2024-02"), ("South", "CityC", "2024-01"),
    ("South", "CityC", "2024-03"), (None, "CityD", "2024-03"),
})

BAND35 = "CASE WHEN amount:sum(partition_by=city) > 35 THEN 1 ELSE 0 END"
#: Band value per (region, city) under BAND35.
BAND35_OF = {
    ("North", "CityA"): 0, ("North", "CityB"): 1, ("North", None): 0,
    ("South", "CityC"): 1, (None, "CityD"): 1,
}


def q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery(**kw)


__all__ = [
    "BAND35", "BAND35_OF", "CITY_TOTAL", "ColumnRef", "GRAND_TOTAL",
    "ModelMeasure", "RCM_GROUPS", "RC_GROUPS", "REGION_FIRST", "REGION_LAST",
    "REGION_LAST_AT", "REGION_MONTH_LAST", "REGION_MONTH_TOTAL", "REGION_TOTAL",
    "SlayerQuery", "TRAILING_45D_REGION", "TRAILING_90D_REGION",
    "TimeDimension", "TimeGranularity",
    "approx_sum", "cm_cte_bodies", "customers_model", "dev1824_models",
    "dev1739_models", "gen", "make_exec_engine", "month_key", "month_td",
    "orders_model", "q", "regions_model", "rows_by",
]
