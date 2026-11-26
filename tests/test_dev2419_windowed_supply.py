"""Windowed aggregates in inferred queries: they supply their unwindowed twin's cells, and their value in a row
depends only on that row's cell (spec: queries/population › Rows of an inferred query are the row supply).

Association seed (60-day frame of a month bucket = ``[bucket_end − 60 days, bucket_end)``)::

    customers: Ann 50 03-01 | Bob 70 02-01 | Cy 60 01-15 (tenant t2) | Eve NULL 04-01 | Fay 25 05-01   (signup, 2024)
    orders:    1 Ann open 30 03-01 | 2 Bob paid 5 02-01 | 3 Cy open 60 04-01 | 4 Cy paid 8 01-10
    order_items: order 1 → P1 food | order 2 → P2 toys | order 4 → P2 toys;  products: P1 food, P2 toys, P5 books
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Sequence
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.sql import generator as generator_module
from slayer.sql.generator import SQLGenerator, _WindowRows

from tests._dev1737_fixtures import TableSpec, seed_backend
from tests._dev2212_fixtures import BACKENDS, HIDE_C3, kinds, with_policy
from tests._engine_helpers import seeded_exec_engine

TABLES = [
    TableSpec(name="customers", columns=[("id", "INT"), ("name", "TEXT"), ("credit", "DOUBLE"), ("signup_at", "DATE"),
                                         ("tenant", "TEXT")],
              rows=[(1, "Ann", 50.0, "2024-03-01", "t1"), (2, "Bob", 70.0, "2024-02-01", "t1"),
                    (3, "Cy", 60.0, "2024-01-15", "t2"), (4, "Eve", None, "2024-04-01", "t1"),
                    (5, "Fay", 25.0, "2024-05-01", "t1")]),
    TableSpec(name="orders", columns=[("id", "INT"), ("customer_id", "INT"), ("status", "TEXT"), ("amount", "DOUBLE"),
                                      ("ordered_at", "DATE")],
              rows=[(1, 1, "open", 30.0, "2024-03-01"), (2, 2, "paid", 5.0, "2024-02-01"),
                    (3, 3, "open", 60.0, "2024-04-01"), (4, 3, "paid", 8.0, "2024-01-10")]),
    TableSpec(name="order_items", columns=[("id", "INT"), ("order_id", "INT"), ("product_id", "INT"), ("qty", "DOUBLE")],
              rows=[(1, 1, 1, 1.0), (2, 2, 2, 1.0), (3, 4, 2, 1.0)]),
    TableSpec(name="products", columns=[("id", "INT"), ("category", "TEXT")],
              rows=[(1, "food"), (2, "toys"), (5, "books")]),
]


def _rel(to: str, frm: str) -> DatasetRelationship:
    return DatasetRelationship(to=to, from_columns=[frm], to_columns=["id"],
                               cardinality=RelationshipCardinality.MANY_TO_ONE)


def _ds(name: str, fields: list[tuple[str, DataType]], rels: Sequence[DatasetRelationship] = (),
        **kw: Any) -> SlayerDataset:
    return SlayerDataset(name=name, sql_table=name, data_source="test", relationships=list(rels), **kw, fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        *(DatasetField(name=n, datatype=t) for n, t in fields)])


def _datasets(customer_filters: Optional[list[str]] = None) -> list[SlayerDataset]:
    return [
        _ds("customers", [("name", DataType.STRING), ("credit", DataType.FLOAT), ("signup_at", DataType.DATE)],
            default_time_dimension="signup_at", filters=customer_filters or []),
        _ds("orders", [("customer_id", DataType.INTEGER), ("status", DataType.STRING), ("amount", DataType.FLOAT),
                       ("ordered_at", DataType.DATE)], [_rel("customers", "customer_id")],
            default_time_dimension="ordered_at"),
        _ds("order_items", [("order_id", DataType.INTEGER), ("product_id", DataType.INTEGER), ("qty", DataType.FLOAT)],
            [_rel("orders", "order_id"), _rel("products", "product_id")]),
        _ds("products", [("category", DataType.STRING)]),
    ]


@contextlib.asynccontextmanager
async def _engine(backend: str, *, customer_filters: Optional[list[str]] = None,
                  ) -> AsyncGenerator[SlayerQueryEngine]:
    async with seeded_exec_engine(dialect=backend, seed=lambda p: seed_backend(backend, p, TABLES),
                                  datasets=_datasets(customer_filters)) as (eng, _):
        yield eng


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _month(field: str) -> list[dict[str, str]]:
    return [{"field": field, "granularity": "month"}]


CAT_ORDER_MONTH = {"dimensions": ["products.category"], "time_dimensions": _month("orders.ordered_at")}
STATUS_SIGNUP_MONTH = {"dimensions": ["orders.status"], "time_dimensions": _month("customers.signup_at")}
STATUS_ORDER_MONTH = {"dimensions": ["orders.status"], "time_dimensions": _month("orders.ordered_at")}
SPINE = {"dimensions": ["orders.status"], "time_dimensions": [
    {"field": "time_spine.timestamp", "granularity": "month", "date_range": ["2024-01-01", "2024-06-30"]}]}


def _m(expr: str, alias: str = "v") -> dict[str, str]:
    return {"metric": expr, "alias": alias}


async def _run(eng: SlayerQueryEngine, body: dict[str, Any], **extra: Any) -> SlayerResponse:
    return await eng.execute(SlayerQuery.model_validate({**body, **extra}))


def _key(v: Any) -> Any:
    """Months compare by ``YYYY-MM`` across dialects."""
    s = str(v)
    return s[:7] if v is not None and s[:4].isdigit() and s[4:5] == "-" else v


def _val(v: Any) -> Any:
    if isinstance(v, bool) or v is None:
        return v
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, (list, tuple)):
        return tuple(sorted(v, key=repr))
    return v


def _cells(resp: SlayerResponse, keys: Sequence[str], value: str = "v") -> dict[tuple, Any]:
    out = {tuple(_key(r[k]) for k in keys): _val(r[value]) for r in resp.data}
    assert len(out) == len(resp.data), resp.data
    return out


# --------------------------------------------------------------------------- #
# Spec scenarios
# --------------------------------------------------------------------------- #
CAT_KEYS = ("products.category", "orders.ordered_at")
STATUS_SIGNUP_KEYS = ("orders.status", "customers.signup_at")
SPINE_KEYS = ("orders.status", "time_spine.timestamp")


async def test_a_windowed_metric_supplies_the_cells_of_its_unwindowed_twin(backend: str) -> None:
    async with _engine(backend) as eng:
        win_sum = await _run(eng, CAT_ORDER_MONTH, metrics=[_m("sum(customers.credit, window='60d')")])
        win_count = await _run(eng, CAT_ORDER_MONTH, metrics=[_m("count(customers.*, window='60d')")])
        plain = await _run(eng, CAT_ORDER_MONTH, metrics=[_m("sum(customers.credit)")])
    cells = [("food", "2024-03"), ("toys", "2024-01"), ("toys", "2024-02"), (None, "2024-04"), (None, None)]
    assert _cells(win_sum, CAT_KEYS) == dict(zip(cells, [50.0, 60.0, 130.0, 60.0, None]))
    assert _cells(win_count, CAT_KEYS) == dict(zip(cells, [1.0, 1.0, 2.0, 1.0, 0.0]))
    assert _cells(plain, CAT_KEYS) == dict(zip(cells, [50.0, 60.0, 70.0, 60.0, 25.0]))


#: count(customers.*, window='60d') by status × signup month; Eve and Fay (no orders) in the NULL status.
STATUS_SIGNUP_WINDOWED = {("open", "2024-01"): 1.0, ("open", "2024-03"): 1.0, ("paid", "2024-01"): 1.0,
                          ("paid", "2024-02"): 2.0, (None, "2024-04"): 1.0, (None, "2024-05"): 1.0}
COUNT_AND_WINDOWED = [_m("count(customers.*)", "c"), _m("count(customers.*, window='60d')", "w")]


@pytest.mark.parametrize("sibling", [[], [_m("sum(orders.amount)", "s")]], ids=["alone", "beside-orders"])
async def test_a_windowed_value_is_the_same_whatever_hosts_the_query(backend: str, sibling: list) -> None:
    async with _engine(backend) as eng:
        inferred = await _run(eng, STATUS_SIGNUP_MONTH, metrics=[*COUNT_AND_WINDOWED, *sibling])
        named = await _run(eng, STATUS_SIGNUP_MONTH, metrics=COUNT_AND_WINDOWED, population="customers")
    assert _cells(inferred, STATUS_SIGNUP_KEYS, "w") == STATUS_SIGNUP_WINDOWED
    assert _cells(named, STATUS_SIGNUP_KEYS, "w") == STATUS_SIGNUP_WINDOWED
    assert _cells(inferred, STATUS_SIGNUP_KEYS, "c") == {k: 1.0 for k in STATUS_SIGNUP_WINDOWED}


def _spine_expected() -> dict[tuple, float]:
    months = [f"2024-0{i}" for i in range(1, 7)]
    out = {(s, mo): 0.0 for s in ("open", "paid", None) for mo in months}
    out.update({("open", "2024-01"): 1.0, ("open", "2024-02"): 1.0, ("open", "2024-03"): 1.0,
                ("paid", "2024-01"): 1.0, ("paid", "2024-02"): 2.0, ("paid", "2024-03"): 1.0,
                (None, "2024-04"): 1.0, (None, "2024-05"): 1.0})
    return out


@pytest.mark.parametrize("population", [None, "customers"], ids=["inferred", "named"])
async def test_a_windowed_spine_metric_supplies_its_home_tuples(backend: str, population: Optional[str]) -> None:
    extra = {"population": population} if population else {}
    async with _engine(backend) as eng:
        resp = await _run(eng, SPINE, metrics=[_m("count(customers.*, window='60d')", "w")], **extra)
    assert _cells(resp, SPINE_KEYS, "w") == _spine_expected()


# --------------------------------------------------------------------------- #
# Parity matrix: every window-capable built-in, associated and determined
# --------------------------------------------------------------------------- #
#: name → (associated metric over customers, determined metric over orders, DuckDB only).
BUILTINS: dict[str, tuple[str, str, bool]] = {
    "sum": ("sum(customers.credit{w})", "sum(orders.amount{w})", False),
    "count_field": ("count(customers.credit{w})", "count(orders.amount{w})", False),
    "count_star": ("count(customers.*{w})", "count(orders.*{w})", False),
    "avg": ("avg(customers.credit{w})", "avg(orders.amount{w})", False),
    "min": ("min(customers.credit{w})", "min(orders.amount{w})", False),
    "max": ("max(customers.credit{w})", "max(orders.amount{w})", False),
    "count_distinct": ("count_distinct(customers.name{w})", "count_distinct(orders.status{w})", False),
    "collect": ("collect(customers.name{w})", "collect(orders.status{w})", True),
}
WIN = ", window='60d'"
STATUS_ORDER_KEYS = ("orders.status", "orders.ordered_at")


def _matrix() -> list:
    return [pytest.param(b, name, side, id=f"{b}-{name}-{side}")
            for b in BACKENDS for name, (_, _, duck) in BUILTINS.items() if b == "duckdb" or not duck
            for side in ("associated", "determined")]


def _builtin(name: str, side: str, *, windowed: bool) -> str:
    assoc, det, _ = BUILTINS[name]
    return (assoc if side == "associated" else det).format(w=WIN if windowed else "")


@pytest.mark.parametrize(("dialect", "name", "side"), _matrix())
async def test_a_windowed_builtin_supplies_its_twins_rows_with_its_population_values(
    dialect: str, name: str, side: str,
) -> None:
    home = "customers" if side == "associated" else "orders"
    async with _engine(dialect) as eng:
        windowed = await _run(eng, STATUS_ORDER_MONTH, metrics=[_m(_builtin(name, side, windowed=True))])
        twin = await _run(eng, STATUS_ORDER_MONTH, metrics=[_m(_builtin(name, side, windowed=False))])
        named = await _run(eng, STATUS_ORDER_MONTH, metrics=[_m(_builtin(name, side, windowed=True))],
                           population=home)
    assert set(_cells(windowed, STATUS_ORDER_KEYS)) == set(_cells(twin, STATUS_ORDER_KEYS))
    assert _cells(windowed, STATUS_ORDER_KEYS) == _cells(named, STATUS_ORDER_KEYS)


def _associated() -> list:
    return [pytest.param(b, name, id=f"{b}-{name}")
            for b in BACKENDS for name, (_, _, duck) in BUILTINS.items() if b == "duckdb" or not duck]


@pytest.mark.parametrize(("dialect", "name"), _associated())
async def test_a_windowed_value_does_not_change_beside_another_homes_supplier(dialect: str, name: str) -> None:
    """``sum(orders.amount)`` supplies (NULL, April): order 3 has no items."""
    metric = _m(_builtin(name, "associated", windowed=True))
    async with _engine(dialect) as eng:
        alone = await _run(eng, CAT_ORDER_MONTH, metrics=[metric])
        beside = await _run(eng, CAT_ORDER_MONTH, metrics=[metric, _m("sum(orders.amount)", "s")])
        twin = await _run(eng, CAT_ORDER_MONTH, metrics=[_m(_builtin(name, "associated", windowed=False))])
    own = _cells(alone, CAT_KEYS)
    assert set(own) == set(_cells(twin, CAT_KEYS))
    shared = _cells(beside, CAT_KEYS)
    assert {k: shared[k] for k in own} == own


# --------------------------------------------------------------------------- #
# Position: a windowed aggregate in a filter or an order key reads the metric's value
# --------------------------------------------------------------------------- #
SUPPLIERS = [_m("count(customers.*)", "c"), _m("sum(orders.amount)", "s")]


async def test_a_windowed_filter_reads_the_metric_value(backend: str) -> None:
    async with _engine(backend) as eng:
        resp = await _run(eng, STATUS_SIGNUP_MONTH, metrics=SUPPLIERS, filter=["count(customers.*, window='60d') = 1"])
    assert set(_cells(resp, STATUS_SIGNUP_KEYS, "c")) == {k for k, v in STATUS_SIGNUP_WINDOWED.items() if v == 1.0}


async def test_a_windowed_order_key_reads_the_metric_value(backend: str) -> None:
    async with _engine(backend) as eng:
        resp = await _run(eng, STATUS_SIGNUP_MONTH, metrics=SUPPLIERS, order=[
            {"field": "count(customers.*, window='60d')", "direction": "desc"},
            {"field": "customers.signup_at", "direction": "desc"}])
    # (paid, Feb) 2, then every 1 by signup month desc
    assert [_key(r["customers.signup_at"]) for r in resp.data] == [
        "2024-02", "2024-05", "2024-04", "2024-03", "2024-01", "2024-01"]


# --------------------------------------------------------------------------- #
# Filters restrict the windowed domain
# --------------------------------------------------------------------------- #
WINDOWED_PAIR = [_m("count(customers.*, window='60d')", "w"), _m("sum(customers.credit, window='60d')", "ws")]


async def _windowed_pair(eng: SlayerQueryEngine, **extra: Any) -> tuple[SlayerResponse, SlayerResponse]:
    inferred = await _run(eng, STATUS_SIGNUP_MONTH, metrics=WINDOWED_PAIR, **extra)
    named = await _run(eng, STATUS_SIGNUP_MONTH, metrics=WINDOWED_PAIR, population="customers", **extra)
    return inferred, named


def _pair_cells(resp: SlayerResponse) -> dict[tuple, tuple]:
    return {k: (v, _cells(resp, STATUS_SIGNUP_KEYS, "ws")[k]) for k, v in _cells(resp, STATUS_SIGNUP_KEYS, "w").items()}


async def test_a_home_field_filter_restricts_the_windowed_domain(backend: str) -> None:
    # guard; Eve (NULL) and Fay (25) fail credit > 30
    async with _engine(backend) as eng:
        inferred, named = await _windowed_pair(eng, filter=["customers.credit > 30"])
    expected = {("open", "2024-01"): (1.0, 60.0), ("paid", "2024-01"): (1.0, 60.0),
                ("paid", "2024-02"): (2.0, 130.0), ("open", "2024-03"): (1.0, 50.0)}
    assert _pair_cells(inferred) == _pair_cells(named) == expected


async def test_a_cross_dataset_filter_places_the_entity_by_the_same_related_row(backend: str) -> None:
    # guard; Cy's paid order (01-10) places Cy in January and February; his open April order is filtered out
    async with _engine(backend) as eng:
        metrics = [_m("count(customers.*, window='60d')", "w")]
        inferred = await _run(eng, STATUS_ORDER_MONTH, metrics=metrics, filter=["orders.status = 'paid'"])
        named = await _run(eng, STATUS_ORDER_MONTH, metrics=metrics, filter=["orders.status = 'paid'"],
                           population="customers")
    expected = {("paid", "2024-01"): 1.0, ("paid", "2024-02"): 2.0}
    assert _cells(inferred, STATUS_ORDER_KEYS, "w") == _cells(named, STATUS_ORDER_KEYS, "w") == expected


async def test_a_model_filter_restricts_the_windowed_domain(backend: str) -> None:
    async with _engine(backend, customer_filters=["name <> 'Bob'"]) as eng:
        inferred, named = await _windowed_pair(eng)
    expected = {("open", "2024-01"): (1.0, 60.0), ("paid", "2024-01"): (1.0, 60.0), ("open", "2024-03"): (1.0, 50.0),
                (None, "2024-04"): (1.0, None), (None, "2024-05"): (1.0, 25.0)}
    assert _pair_cells(inferred) == _pair_cells(named) == expected


async def test_a_policy_restricts_the_windowed_domain(backend: str) -> None:
    # the policy hides Cy (tenant t2)
    async with _engine(backend) as eng, with_policy(eng, HIDE_C3) as hidden:
        inferred, named = await _windowed_pair(hidden)
    expected = {("paid", "2024-02"): (1.0, 70.0), ("open", "2024-03"): (1.0, 50.0),
                (None, "2024-04"): (1.0, None), (None, "2024-05"): (1.0, 25.0)}
    assert _pair_cells(inferred) == _pair_cells(named) == expected


# --------------------------------------------------------------------------- #
# Broadcast keeps its coarse supply (guard)
# --------------------------------------------------------------------------- #
async def test_broadcast_keeps_the_coarse_windowed_supply(backend: str) -> None:
    # windowed over signup months, then broadcast onto the statuses; no NULL-status row
    async with _engine(backend) as eng:
        resp = await _run(eng, STATUS_SIGNUP_MONTH, to_many_handling="broadcast", metrics=[
            _m("count(customers.*, window='60d')", "w"), _m("sum(customers.credit, window='60d')", "ws")])
    assert _pair_cells(resp) == {("open", "2024-01"): (1.0, 60.0), ("open", "2024-03"): (2.0, 120.0),
                                 ("paid", "2024-01"): (1.0, 60.0), ("paid", "2024-02"): (2.0, 130.0)}
    warnings = kinds(resp, "broadcast")
    assert sorted(w.metric for w in warnings) == ["w", "ws"]
    assert all(w.partition_by == ["customers.signup_at"] for w in warnings)


# --------------------------------------------------------------------------- #
# Producer reuse keys on the window column mapping
# --------------------------------------------------------------------------- #
def test_window_producer_reuse_keys_on_the_column_mapping(monkeypatch: pytest.MonkeyPatch) -> None:
    # one producer, one stage relation, two grain-slot → stage-column mappings: no shared CTE
    monkeypatch.setattr(generator_module, "regroup_producer_identity", lambda attach: "producer")
    by_status = _WindowRows(relation="_stage", columns={"g0": "status"})
    by_month = _WindowRows(relation="_stage", columns={"g0": "month"})
    assert (SQLGenerator._producer_identity(None, window_rows=by_status)
            != SQLGenerator._producer_identity(None, window_rows=by_month))
