"""Shared fixtures for DEV-1835 — local family unification (bare windowed /
first-last onto the regroup primitive) + attach-dedup.

Reuses the DEV-1739/1824/1837 models, dataset, and oracles verbatim and adds
hand-computed oracles for the migrated families, the guard-dissolution shapes,
and the union-grain broadcasts, plus a shipped-at model variant for the
explicit-ranking-column cases.

Oracle derivations (from the DEV-1739 rows; row list in ``_dev1824_fixtures``)
------------------------------------------------------------------------------
CITY_WM        bare ``amount:sum(window='1y')`` at (region, city, month): the
               window spans the whole dataset, so each bucket carries the
               running (region, city) total: (N,CityA,Jan)=30 (N,CityB,Feb)=40
               (N,NULL,Feb)=30 (S,CityC,Jan)=25 (S,CityC,Mar)=25+25=50
               (NULL,CityD,Mar)=60.
CITY_LAST_RC   bare ``amount:last`` at (region, city) — value of the latest
               ``ordered_at`` row per group: (N,CityA)=20 (row 2, Jan 20)
               (N,CityB)=40 (N,NULL)=30 (S,CityC)=25 (row 6) (NULL,CityD)=60.
CITY_MONTH_LAST  the same at (region, city, month): (N,CityA,Jan)=20 others as
               above with (S,CityC) split into Jan=25 / Mar=25.
WM_X / RK_X    the bare-windowed / bare-last oracle per matrix dimension
               family (grain = every projected slot; wm '1y' ≡ running total
               within the non-time dims, so WM_X mirrors the cumsum series).
OK_W90         ``ok_amount:sum(window='90d')`` at (region, month) — row 6
               (hold) drops, so (S,Mar)=25 (vs 50 unfiltered): (N,Jan)=30
               (N,Feb)=100 (S,Jan)=25 (S,Mar)=25 (NULL,Mar)=60.
OK_LAST        ``ok_amount:last`` by region — the latest OK row per region:
               N=30 S=25 (row 5) NULL=60 (numerically equal to REGION_LAST;
               the all-NULL ``nomatch:last`` is the discriminating twin).
CUMSUM_OVER_W90  cumsum of TRAILING_90D_REGION months within region:
               N: 30, 130 · S: 25, 75 · NULL: 60.
W90_RATIO      TRAILING_90D_REGION / REGION_MONTH_TOTAL per bucket:
               (N,Jan)=1 (N,Feb)=100/70 (S,Jan)=1 (S,Mar)=2 (NULL,Mar)=1.
TS_OVER_W90    time_shift(w90, -1), calendar months: (N,Feb)=30, rest NULL.
TS_OVER_LAST   time_shift(amount:last, -1) over REGION_MONTH_LAST (N,Jan)=20
               (N,Feb)=30 (S,Jan)=25 (S,Mar)=25 (NULL,Mar)=60: only (N,Feb)
               has a previous calendar bucket → 20; rest NULL.
CHANGE_OVER_LAST / CHANGE_PCT_OVER_LAST  change(amount:last): (N,Feb)=30-20=10;
               change_pct: 10/20=0.5; rest NULL.
UNION_WM_RANK  dimension ``rank(amount:sum(window='90d', partition_by=region)
               - amount:sum(partition_by=region))``, month TD. Union grain =
               (region, bucket); diffs w90-total: (N,Jan)=-70 (N,Feb)=0
               (S,Jan)=-25 (S,Mar)=0 (NULL,Mar)=0 → RANK() desc 5/1/4/1/1.
UNION_RK_RANK  dimension ``rank(amount:last(partition_by=region) -
               amount:sum(partition_by=city))``. Union grain = (region, city);
               region-last broadcast (30/25/60) minus city totals
               (30/40/30/50/60): 0/-10/0/-25/0 → RANK() desc 1/4/1/5/1.
ORDER_BY_W90_DESC  (region, month) keys ordered by the hidden windowed value
               100/60/50/30/25 desc: (N,Feb) (NULL,Mar) (S,Mar) (N,Jan) (S,Jan).
LAST/FIRST_BY_SHIPPED  on the shipped-at variant (dates below): North's latest
               shipment is row 1 (amount 10), earliest row 2 (20); South's
               rows both 25; NULL region 60.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator, List

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1824_fixtures import (  # noqa: F401 — re-exported fixture surface
    REGION_FIRST,
    REGION_MONTH_LAST,
    REGION_MONTH_TOTAL,
    TRAILING_45D_REGION,
)
from tests._dev1837_fixtures import (  # noqa: F401 — re-exported fixture surface
    BAND35,
    BAND35_OF,
    CITY_TOTAL,
    COL_WM,
    ColumnRef,
    GRAND_TOTAL,
    ModelMeasure,
    REGION_LAST,
    REGION_TOTAL,
    SlayerQuery,
    TRAILING_90D_REGION,
    TimeDimension,
    TimeGranularity,
    dev1837_models,
    dim_key,
    gen,
    make_exec_engine,
    month_key,
    month_td,
    q,
    rows_by,
    with_nulls,
)

dev1835_models = dev1837_models

# --------------------------------------------------------------------------- #
# Migrated-family executed-value oracles.
# --------------------------------------------------------------------------- #
CITY_WM = {
    ("North", "CityA", "2024-01"): 30.0, ("North", "CityB", "2024-02"): 40.0,
    ("North", None, "2024-02"): 30.0, ("South", "CityC", "2024-01"): 25.0,
    ("South", "CityC", "2024-03"): 50.0, (None, "CityD", "2024-03"): 60.0,
}
CITY_LAST_RC = {
    ("North", "CityA"): 20.0, ("North", "CityB"): 40.0, ("North", None): 30.0,
    ("South", "CityC"): 25.0, (None, "CityD"): 60.0,
}
CITY_MONTH_LAST = {
    ("North", "CityA", "2024-01"): 20.0, ("North", "CityB", "2024-02"): 40.0,
    ("North", None, "2024-02"): 30.0, ("South", "CityC", "2024-01"): 25.0,
    ("South", "CityC", "2024-03"): 25.0, (None, "CityD", "2024-03"): 60.0,
}

#: Bare ``amount:sum(window='1y')`` per matrix dimension family, keyed like
#: ``GROUP_M_MONTH[family]``. '1y' spans the dataset, so each entry is the
#: running total within the family's non-time dims.
WM_X = {
    "col": dict(COL_WM),
    "expr": {
        ("North", "citya", "2024-01"): 30.0, ("North", "cityb", "2024-02"): 40.0,
        ("North", None, "2024-02"): 30.0, ("South", "cityc", "2024-01"): 25.0,
        ("South", "cityc", "2024-03"): 50.0, (None, "cityd", "2024-03"): 60.0,
    },
    "band": {
        ("North", 0, "2024-01"): 30.0, ("North", 0, "2024-02"): 60.0,
        ("North", 1, "2024-02"): 40.0, ("South", 1, "2024-01"): 25.0,
        ("South", 1, "2024-03"): 50.0, (None, 1, "2024-03"): 60.0,
    },
    "bare": {
        ("North", 30.0, "2024-01"): 30.0, ("North", 30.0, "2024-02"): 60.0,
        ("North", 40.0, "2024-02"): 40.0, ("South", 50.0, "2024-01"): 25.0,
        ("South", 50.0, "2024-03"): 50.0, (None, 60.0, "2024-03"): 60.0,
    },
    "rank": {
        ("North", 1, "2024-01"): 30.0, ("North", 1, "2024-02"): 100.0,
        ("South", 3, "2024-01"): 25.0, ("South", 3, "2024-03"): 50.0,
        (None, 2, "2024-03"): 60.0,
    },
    "mixed": {
        ("North", "CityA", 1, "2024-01"): 30.0,
        ("North", "CityB", 3, "2024-02"): 40.0,
        ("North", None, 1, "2024-02"): 30.0,
        ("South", "CityC", 4, "2024-01"): 25.0,
        ("South", "CityC", 4, "2024-03"): 50.0,
        (None, "CityD", 4, "2024-03"): 60.0,
    },
}

#: Bare ``amount:last`` per matrix dimension family (timeless), keyed like
#: ``GROUP_M[family]`` — the value of the group's latest ``ordered_at`` row.
RK_X = {
    "col": {("North",): 30.0, ("South",): 25.0, (None,): 60.0},
    "expr": {
        ("North", "citya"): 20.0, ("North", "cityb"): 40.0, ("North", None): 30.0,
        ("South", "cityc"): 25.0, (None, "cityd"): 60.0,
    },
    "band": {("North", 0): 30.0, ("North", 1): 40.0, ("South", 1): 25.0, (None, 1): 60.0},
    "bare": {
        ("North", 30.0): 30.0, ("North", 40.0): 40.0,
        ("South", 50.0): 25.0, (None, 60.0): 60.0,
    },
    "rank": {("North", 1): 30.0, ("South", 3): 25.0, (None, 2): 60.0},
    "mixed": {
        ("North", "CityA", 1): 20.0, ("North", "CityB", 3): 40.0,
        ("North", None, 1): 30.0, ("South", "CityC", 4): 25.0,
        (None, "CityD", 4): 60.0,
    },
}

# --------------------------------------------------------------------------- #
# Guard-dissolution oracles (region grain + month unless noted).
# --------------------------------------------------------------------------- #
OK_W90 = {
    ("North", "2024-01"): 30.0, ("North", "2024-02"): 100.0,
    ("South", "2024-01"): 25.0, ("South", "2024-03"): 25.0,
    (None, "2024-03"): 60.0,
}
OK_LAST = {"North": 30.0, "South": 25.0, None: 60.0}
CUMSUM_OVER_W90 = {
    ("North", "2024-01"): 30.0, ("North", "2024-02"): 130.0,
    ("South", "2024-01"): 25.0, ("South", "2024-03"): 75.0,
    (None, "2024-03"): 60.0,
}
W90_RATIO = {
    ("North", "2024-01"): 1.0, ("North", "2024-02"): 100.0 / 70.0,
    ("South", "2024-01"): 1.0, ("South", "2024-03"): 2.0,
    (None, "2024-03"): 1.0,
}
TS_OVER_W90 = {("North", "2024-02"): 30.0}
TS_OVER_LAST = {("North", "2024-02"): 20.0}
CHANGE_OVER_LAST = {("North", "2024-02"): 10.0}
CHANGE_PCT_OVER_LAST = {("North", "2024-02"): 0.5}

# --------------------------------------------------------------------------- #
# Union-grain (DEV-1839 lift, task 3.7) oracles.
# --------------------------------------------------------------------------- #
UNION_WM_DIM = (
    "rank(amount:sum(window='90d', partition_by=region) - "
    "amount:sum(partition_by=region))"
)
UNION_RK_DIM = (
    "rank(amount:last(partition_by=region) - amount:sum(partition_by=city))"
)
UNION_WM_RANK = {
    ("North", "2024-01"): 5, ("North", "2024-02"): 1,
    ("South", "2024-01"): 4, ("South", "2024-03"): 1, (None, "2024-03"): 1,
}
UNION_RK_RANK = {
    ("North", "CityA"): 1, ("North", "CityB"): 4, ("North", None): 1,
    ("South", "CityC"): 5, (None, "CityD"): 1,
}

#: (region, month) keys ordered by the hidden ``amount:sum(window='90d')`` desc.
ORDER_BY_W90_DESC = [
    ("North", "2024-02"), (None, "2024-03"), ("South", "2024-03"),
    ("North", "2024-01"), ("South", "2024-01"),
]


def cte_aliases(sql: str, prefix: str, *, dialect: str = "sqlite") -> List[str]:
    """Aliases of TOP-LEVEL WITH CTEs whose name starts with ``prefix`` —
    nested WITHs are excluded deliberately (Codex F2), so a producer-internal
    CTE can never inflate a dedup count."""
    tree = sqlglot.parse_one(sql, read=dialect)
    # sqlglot 30.x stores a Select's top-level CTE block under ``with_``; older
    # builds used ``with``. Read both so the top-level-only intent survives the
    # version (a bare ``args.get("with")`` silently returns None on 30.x).
    with_node = tree.args.get("with_") or tree.args.get("with")
    ctes = with_node.expressions if isinstance(with_node, exp.With) else []
    return sorted(cte.alias for cte in ctes if cte.alias.startswith(prefix))


# --------------------------------------------------------------------------- #
# Shipped-at model variant — a second ranking timestamp for the explicit
# ranking-column cases. Same rows as DEV-1739 plus ``shipped_at``.
# --------------------------------------------------------------------------- #
_SHIPPED_ROWS = [
    # (id, customer_id, region, city, channel, amount, status, ordered_at, shipped_at)
    (1, 1, "North", "CityA", "web", 10.0, "ok", "2024-01-10", "2024-03-01"),
    (2, 1, "North", "CityA", "app", 20.0, "ok", "2024-01-20", "2024-01-25"),
    (3, 2, "North", "CityB", "web", 40.0, "ok", "2024-02-10", "2024-02-12"),
    (4, 2, "North", None, "web", 30.0, "ok", "2024-02-15", "2024-02-16"),
    (5, 3, "South", "CityC", "web", 25.0, "ok", "2024-01-25", "2024-03-20"),
    (6, 3, "South", "CityC", "app", 25.0, "hold", "2024-03-05", "2024-03-06"),
    (7, 1, None, "CityD", "web", 60.0, "ok", "2024-03-10", "2024-03-11"),
]

LAST_BY_ORDERED = dict(REGION_LAST)
LAST_BY_SHIPPED = {"North": 10.0, "South": 25.0, None: 60.0}
FIRST_BY_SHIPPED = {"North": 20.0, "South": 25.0, None: 60.0}


def shipped_orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        default_time_dimension="ordered_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="region", type=DataType.TEXT),
            Column(name="city", type=DataType.TEXT),
            Column(name="channel", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            Column(name="shipped_at", type=DataType.TIMESTAMP),
            # Derived column — the "every dimension kind" scenario (Codex F3).
            Column(name="city_key", type=DataType.TEXT, sql="LOWER(city)"),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


def _seed_shipped_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    con.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "region TEXT, city TEXT, channel TEXT, amount REAL, status TEXT, "
        "ordered_at TEXT, shipped_at TEXT)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?)", _SHIPPED_ROWS)
    con.commit()
    con.close()


def _seed_shipped_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, region VARCHAR, "
        "city VARCHAR, channel VARCHAR, amount DOUBLE, status VARCHAR, "
        "ordered_at TIMESTAMP, shipped_at TIMESTAMP)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?)", _SHIPPED_ROWS)
    con.close()


async def make_shipped_exec_engine(request) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture over the shipped-at
    model (mirrors ``_dev1739_fixtures.make_exec_engine``)."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, f"data.{dialect}")
        if dialect == "sqlite":
            _seed_shipped_sqlite(db_path)
        else:
            _seed_shipped_duckdb(db_path)
        storage = YAMLStorage(base_dir=os.path.join(d, "store"))
        await storage.save_datasource(
            DatasourceConfig(name="test", type=dialect, database=db_path)
        )
        await storage.save_model(shipped_orders_model(), _validate=False)
        yield SlayerQueryEngine(storage=storage)


# Shared executed-value response reducers (the per-dialect exec_backend /
# shipped_backend fixtures stay inline in each module — pytest fixture imports
# trip F811 where the fixture name shadows the test-method parameter).
def _by_region(resp, col: str) -> dict:
    mapping = {r["orders.region"]: r[f"orders.{col}"] for r in resp.data}
    assert len(mapping) == len(resp.data), f"duplicate region rows: {resp.data}"
    return mapping


def _by_region_month(resp, col: str) -> dict:
    mapping = {
        (r["orders.region"], month_key(r["orders.ordered_at"])): r[f"orders.{col}"]
        for r in resp.data
    }
    assert len(mapping) == len(resp.data), f"duplicate (region, month) rows: {resp.data}"
    return mapping


def _assert_map(got: dict, expected: dict) -> None:
    assert set(got) == set(expected), sorted(map(str, got))
    for key, value in expected.items():
        if value is None:
            assert got[key] is None, f"{key}"
        else:
            assert got[key] is not None, f"{key}: expected {value}, got NULL"
            assert float(got[key]) == pytest.approx(value), f"{key}"


__all__ = [
    "_by_region", "_by_region_month", "_assert_map",
    "BAND35", "BAND35_OF", "CHANGE_OVER_LAST", "CHANGE_PCT_OVER_LAST",
    "CITY_LAST_RC", "CITY_MONTH_LAST", "CITY_TOTAL", "CITY_WM", "COL_WM",
    "CUMSUM_OVER_W90", "ColumnRef", "FIRST_BY_SHIPPED", "GRAND_TOTAL",
    "LAST_BY_ORDERED", "LAST_BY_SHIPPED", "ModelMeasure", "OK_LAST", "OK_W90",
    "ORDER_BY_W90_DESC", "REGION_FIRST", "REGION_LAST", "REGION_MONTH_LAST",
    "REGION_MONTH_TOTAL", "REGION_TOTAL", "RK_X", "SlayerQuery",
    "TRAILING_45D_REGION", "TRAILING_90D_REGION", "TS_OVER_LAST", "TS_OVER_W90",
    "TimeDimension", "TimeGranularity", "UNION_RK_DIM", "UNION_RK_RANK",
    "UNION_WM_DIM", "UNION_WM_RANK", "W90_RATIO", "WM_X", "cte_aliases",
    "dev1835_models", "dim_key", "gen", "make_exec_engine",
    "make_shipped_exec_engine", "month_key", "month_td", "q", "rows_by",
    "shipped_orders_model", "with_nulls",
]
