"""Shared fixtures for DEV-1901 — aggregation parameters bind once.

The DEV-1908 graph (``orders → customers → regions → countries``, fanning
``regions → region_events``) plus:

* ``orders.factor`` / ``customers.factor`` — one column name on the root AND the
  owner (a bare ``factor`` default must read the owner's);
* ``customers."group"`` — a reserved-word column that must stay quoted;
* ``orders.cust_spend`` — derived ``customers.spend`` (crosses a to-one hop);
* aggregations whose defaults are a bare / dotted / derived column, a literal, a
  free SQL expression, or unanalysable text.

Dataset extensions (DEV-1908 dataset otherwise verbatim): ``orders.factor = 100 +
id``; ``customers.factor = id``; ``customers."group" = id``.
"""

from __future__ import annotations

from collections import defaultdict
from typing import AsyncIterator, Dict, List, Optional

import pytest

from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.storage.sqlite_conn import transaction
from tests import _dev1908_fixtures
from tests._dev1840_fixtures import _CUSTOMERS_ROWS, _ORDERS_ROWS, _REGIONS_ROWS
from tests._dev1908_fixtures import _COUNTRIES_ROWS, _REGION_COUNTRY, dev1908_models
from tests._engine_helpers import seeded_exec_engine

W = "SUM({value} * {weight})"
CASE_O = "CASE WHEN amount > 10 THEN amount ELSE 0 END"
CASE_C = "CASE WHEN spend > 50 THEN spend ELSE 0 END"
CASE_POP = ("CASE WHEN customers.regions.pop > 150 THEN customers.regions.pop "
            "ELSE 0 END")

#: Unanalysable default texts, by aggregation name.
BAD_DEFAULTS = {
    "wbad_parse": ")((( bad",
    "wbad_agg": "SUM(amount)",
    "wbad_win": "ROW_NUMBER() OVER (ORDER BY id)",
    "wbad_sub": "(SELECT MAX(amount) FROM orders)",
}


def _w(name: str, default: str, *, formula: str = W, param: str = "weight") -> Aggregation:
    return Aggregation(name=name, formula=formula,
                       params=[AggregationParam(name=param, sql=default)])


def _by_name(models: List[SlayerModel], name: str) -> SlayerModel:
    return next(m for m in models if m.name == name)


def dev1901_models() -> List[SlayerModel]:
    """DEV-1908 graph + the DEV-1901 columns and aggregations. Host first (orders)."""
    models = dev1908_models()
    orders = _by_name(models, "orders")
    orders.columns.extend([
        Column(name="factor", type=DataType.DOUBLE),
        Column(name="cust_spend", type=DataType.DOUBLE, sql="customers.spend"),
    ])
    orders.aggregations.extend([
        _w("wsum", "store_no"),
        _w("wcase", CASE_O),
        _w("wcs", "customers.spend"),
        _w("wcd", "cust_spend"),
        _w("wcase_pop", CASE_POP),
        _w("wlit", "2", formula="SUM({value}) * {k}", param="k"),
        _w("wzero", "1 + 1"),
        _w("wfmt_a", "customers.spend * 1"),
        _w("wfmt_b", "customers.spend*1"),
        _w("wmissing", "nosuch"),
        Aggregation(name="wunbound", formula="SUM({value}) * {scale}"),
        *(_w(name, text) for name, text in BAD_DEFAULTS.items()),
    ])
    customers = _by_name(models, "customers")
    customers.columns.extend([
        Column(name="factor", type=DataType.DOUBLE),
        Column(name="group", type=DataType.DOUBLE),
    ])
    customers.aggregations.extend([
        _w("wcase_c", CASE_C),
        _w("wshadow", "factor"),
        _w("wmix_q", '"group" + orders.amount'),
        _w("wstore", "stores.rent"),
    ])
    customers.measures.append(ModelMeasure(name="wsaved", formula="spend:wshadow"))
    return models


def revisit_models() -> List[SlayerModel]:
    """``orders → customers →(hr) regions →(back) customers``; ``wrev`` on customers
    defaults to ``hr.back.spend`` (a revisit through edge names, which never cancel)."""
    orders = SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="customer_id", type=DataType.INT),
                 Column(name="amount", type=DataType.DOUBLE)],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                         cardinality=JoinCardinality.MANY_TO_ONE)])
    customers = SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="region_id", type=DataType.INT),
                 Column(name="spend", type=DataType.DOUBLE)],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]],
                         name="hr", cardinality=JoinCardinality.MANY_TO_ONE)],
        aggregations=[_w("wrev", "hr.back.spend"), _w("wrev_expr", "hr.back.spend * 1")])
    regions = SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="manager_id", type=DataType.INT)],
        joins=[ModelJoin(target_model="customers", join_pairs=[["manager_id", "id"]],
                         name="back", cardinality=JoinCardinality.MANY_TO_ONE)])
    return [orders, customers, regions]


def window_param_models(*, placeholder: bool) -> List[SlayerModel]:
    """``orders`` with ``wwin``, whose formula reads ``{window}`` (``placeholder``)
    or which declares a ``window`` parameter."""
    agg = (Aggregation(name="wwin", formula="SUM({value}) * {window}",
                       params=[AggregationParam(name="window", sql="2")])
           if placeholder else
           Aggregation(name="wwin", formula="SUM({value}) * {window}"))
    orders = SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="amount", type=DataType.DOUBLE)],
        aggregations=[agg])
    return [orders]


def bundle_of(models: List[SlayerModel], *, dialect: str = "postgres") -> ResolvedSourceBundle:
    return ResolvedSourceBundle(dialect=dialect, source_model=models[0],
                                referenced_models=models[1:])


def bundle1901(*, dialect: str = "postgres") -> ResolvedSourceBundle:
    return bundle_of(dev1901_models(), dialect=dialect)


# --------------------------------------------------------------------------- #
# Dual-engine seed.
# --------------------------------------------------------------------------- #
_ORDER_FACTOR = [(100.0 + o[0], o[0]) for o in _ORDERS_ROWS]      # (factor, id)
_CUST_EXTRA = [(float(c[0]), float(c[0]), c[0]) for c in _CUSTOMERS_ROWS]  # (factor, group, id)


def _seed_sqlite(db_path: str) -> None:
    _dev1908_fixtures._seed_sqlite(db_path)  # noqa: SLF001 — shared base seed
    with transaction(db_path) as con:
        cur = con.cursor()
        cur.execute("ALTER TABLE orders ADD COLUMN factor REAL")
        cur.executemany("UPDATE orders SET factor=? WHERE id=?", _ORDER_FACTOR)
        cur.execute("ALTER TABLE customers ADD COLUMN factor REAL")
        cur.execute('ALTER TABLE customers ADD COLUMN "group" REAL')
        cur.executemany('UPDATE customers SET factor=?, "group"=? WHERE id=?', _CUST_EXTRA)


def _seed_duckdb(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    _dev1908_fixtures._seed_duckdb(db_path)  # noqa: SLF001 — shared base seed
    con = duckdb.connect(db_path)
    con.execute("ALTER TABLE orders ADD COLUMN factor DOUBLE")
    for factor, oid in _ORDER_FACTOR:
        con.execute("UPDATE orders SET factor=? WHERE id=?", [factor, oid])
    con.execute("ALTER TABLE customers ADD COLUMN factor DOUBLE")
    con.execute('ALTER TABLE customers ADD COLUMN "group" DOUBLE')
    for factor, group, cid in _CUST_EXTRA:
        con.execute('UPDATE customers SET factor=?, "group"=? WHERE id=?', [factor, group, cid])
    con.close()


async def make_exec_engine(
    request, *, models: Optional[List[SlayerModel]] = None,
) -> AsyncIterator[SlayerQueryEngine]:
    """Body for a ``params=["sqlite", "duckdb"]`` fixture."""
    dialect = request.param
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    seed = _seed_duckdb if dialect == "duckdb" else _seed_sqlite
    async with seeded_exec_engine(
        dialect=dialect, seed=seed,
        models=models if models is not None else dev1901_models(),
    ) as (engine, _db):
        yield engine


def orders_q(**kw) -> SlayerQuery:
    kw.setdefault("source_model", "orders")
    return SlayerQuery.model_validate(kw)


# --------------------------------------------------------------------------- #
# Oracles — hand-derived; ``derive_oracles`` recomputes each from the raw rows.
# --------------------------------------------------------------------------- #
#: SUM(amount * store_no).
WSUM = 242.0
#: SUM(amount * CASE WHEN amount > 10 THEN amount ELSE 0 END).
WCASE_LOCAL = 3894.0
#: SUM over customers of spend * CASE WHEN spend > 50 THEN spend ELSE 0 END.
WCASE_CM = 45525.0
#: The same, associated by order status.
WCASE_ASSOC = {"ok": 42500.0, "new": 32500.0}
#: The same, trailing-1y by signup month.
WCASE_WINDOW = {"2024-01": 10000.0, "2024-02": 32500.0,
                "2024-03": 36100.0, "2024-04": 45525.0}
#: SUM(region amount sum * CASE on pop) over sum(amount, partition_by=region).
WCASE_SECOND_ORDER = 4000.0
#: SUM(amount * customers.spend) per status.
WCS_BY_STATUS = {"ok": 7660.0, "new": 7350.0}
#: SUM(amount) per status.
AMOUNT_BY_STATUS = {"ok": 82.0, "new": 85.0}
#: SUM(amount * customers.region_id).
W_REGION_ID = 140.0
#: SUM over customers of spend * customers.factor; associated / windowed.
SHADOW_CM = 1705.0
SHADOW_ASSOC = {"ok": 1160.0, "new": 560.0}
SHADOW_WINDOW = {"2024-01": 100.0, "2024-02": 400.0, "2024-03": 740.0, "2024-04": 1705.0}
#: SUM over orders of customers.spend * (customers."group" + amount).
MIX_QUOTED = 16830.0
#: SUM over region_events of countries.gdp * value (each event once).
WSUM_FAN = 160000.0


def derive_oracles() -> dict:
    """Recompute every oracle straight from the raw row tuples."""
    cust = {c[0]: c for c in _CUSTOMERS_ROWS}       # id → (id, region, plan, tier, spend, signup)
    pop = {r[0]: r[2] for r in _REGIONS_ROWS}
    gdp_of_region = {rid: next(c[2] for c in _COUNTRIES_ROWS if c[0] == cid)
                     for rid, cid in _REGION_COUNTRY}
    events = _dev1908_fixtures._dev1900_fixtures._REGION_EVENTS_ROWS  # noqa: SLF001

    def case_c(spend: float) -> float:
        return spend if spend > 50 else 0.0

    by_status: Dict[str, set] = defaultdict(set)
    for o in _ORDERS_ROWS:
        if o[1] is not None:
            by_status[o[2]].add(o[1])

    def windowed(term) -> dict:
        month: Dict[str, float] = defaultdict(float)
        for c in _CUSTOMERS_ROWS:
            month[c[5][:7]] += term(c)
        out, running = {}, 0.0
        for m in sorted(month):
            running += month[m]
            out[m] = running
        return out

    region_amt: Dict[int, float] = defaultdict(float)
    for o in _ORDERS_ROWS:
        rid = cust[o[1]][1] if o[1] is not None else None
        if rid is not None:
            region_amt[rid] += o[4]

    def wcs(o) -> float:
        return o[4] * cust[o[1]][4] if o[1] is not None else 0.0

    def region_id(o) -> float:
        rid = cust[o[1]][1] if o[1] is not None else None
        return o[4] * rid if rid is not None else 0.0

    def mix(o) -> float:
        return cust[o[1]][4] * (o[1] + o[4]) if o[1] is not None else 0.0

    return {
        "WSUM": sum(o[4] * o[7] for o in _ORDERS_ROWS),
        "WCASE_LOCAL": sum(o[4] * (o[4] if o[4] > 10 else 0.0) for o in _ORDERS_ROWS),
        "WCASE_CM": sum(c[4] * case_c(c[4]) for c in _CUSTOMERS_ROWS),
        "WCASE_ASSOC": {s: sum(cust[i][4] * case_c(cust[i][4]) for i in ids)
                        for s, ids in by_status.items()},
        "WCASE_WINDOW": windowed(lambda c: c[4] * case_c(c[4])),
        "WCASE_SECOND_ORDER": sum(amt * (pop[rid] if pop[rid] > 150 else 0.0)
                                  for rid, amt in region_amt.items()),
        "WCS_BY_STATUS": {s: sum(wcs(o) for o in _ORDERS_ROWS if o[2] == s)
                          for s in ("ok", "new")},
        "AMOUNT_BY_STATUS": {s: sum(o[4] for o in _ORDERS_ROWS if o[2] == s)
                             for s in ("ok", "new")},
        "W_REGION_ID": sum(region_id(o) for o in _ORDERS_ROWS),
        "SHADOW_CM": sum(c[4] * c[0] for c in _CUSTOMERS_ROWS),
        "SHADOW_ASSOC": {s: sum(cust[i][4] * i for i in ids) for s, ids in by_status.items()},
        "SHADOW_WINDOW": windowed(lambda c: c[4] * c[0]),
        "MIX_QUOTED": sum(mix(o) for o in _ORDERS_ROWS),
        "WSUM_FAN": sum(gdp_of_region[rid] * value for _eid, rid, value in events),
    }


ORACLE_LITERALS = {
    "WSUM": WSUM, "WCASE_LOCAL": WCASE_LOCAL, "WCASE_CM": WCASE_CM,
    "WCASE_ASSOC": WCASE_ASSOC, "WCASE_WINDOW": WCASE_WINDOW,
    "WCASE_SECOND_ORDER": WCASE_SECOND_ORDER, "WCS_BY_STATUS": WCS_BY_STATUS,
    "AMOUNT_BY_STATUS": AMOUNT_BY_STATUS, "W_REGION_ID": W_REGION_ID,
    "SHADOW_CM": SHADOW_CM, "SHADOW_ASSOC": SHADOW_ASSOC, "SHADOW_WINDOW": SHADOW_WINDOW,
    "MIX_QUOTED": MIX_QUOTED, "WSUM_FAN": WSUM_FAN,
}
