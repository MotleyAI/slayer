"""Every importer reads every known ambiguous form with the source's meaning (importers principle 1;
spec: importers/meaning-preservation › Every importer is checked against the source's numbers)."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import SlayerDataset
from tests._dev2171_fixtures import (
    by_id, column, counts_by, cube_issues, entries, has_metric, import_cube, import_dbt, datasets_of, rows, seed,
)

PAID = [{"sql": "{CUBE}.status = 'paid'"}]
CUBE_ID = {"name": "id", "sql": "{CUBE}.id", "type": "number", "primary_key": True}

CUBES = [
    {"name": "orders", "sql_table": "orders",
     "joins": [{"name": "customers", "relationship": "many_to_one", "sql": "{CUBE}.customer_id = {customers}.id"}],
     "dimensions": [
         CUBE_ID,
         {"name": "status_upper", "sql": "UPPER({CUBE}.status)"},
         {"name": "tags", "sql": "{CUBE}.tags"},
         {"name": "amount_x2", "sql": "{CUBE}.amount * 2", "type": "number"},
     ],
     "measures": [
         {"name": "paid_cnt", "type": "number", "sql": "SUM(CASE WHEN {CUBE}.status = 'paid' THEN 1 END)"},
         {"name": "paid_amount", "type": "number",
          "sql": "SUM(CASE WHEN {CUBE}.status = 'paid' THEN {CUBE}.amount END)"},
         {"name": "paid_sum", "type": "sum", "sql": "{CUBE}.amount", "filters": PAID},
         {"name": "via_dotted", "type": "number", "sql": "{orders.paid_sum}"},
         {"name": "via_cube", "type": "number", "sql": "{CUBE.paid_sum}"},
         {"name": "share", "type": "number",
          "sql": "SUM({CUBE}.amount) FILTER (WHERE {CUBE}.status = 'paid') / COUNT(*)"},
         {"name": "tag_cnt", "type": "count", "sql": "{CUBE}.tags"},
         {"name": "tag_kinds", "type": "count_distinct", "sql": "{CUBE}.tags"},
         {"name": "windowed", "type": "number", "sql": "SUM({CUBE}.amount) OVER ()"},
     ],
     "segments": [{"name": "is_paid", "sql": "{CUBE}.status = 'paid'"}]},
    {"name": "orders_clash", "sql_table": "orders",
     "joins": [{"name": "customers", "relationship": "many_to_one", "sql": "{CUBE}.customer_id = {customers}.id"}],
     "dimensions": [
         CUBE_ID,
         {"name": "status_upper", "sql": "UPPER({CUBE}.status)"},
         {"name": "status", "sql": "{CUBE}.state"},
         {"name": "customer_id", "sql": "{CUBE}.legacy_ref", "type": "number"},
     ],
     "measures": [
         {"name": "paid_cnt", "type": "number", "sql": "SUM(CASE WHEN {CUBE}.status = 'paid' THEN 1 END)"},
         {"name": "paid_sum", "type": "sum", "sql": "{CUBE}.amount", "filters": PAID},
     ],
     "segments": [{"name": "is_paid", "sql": "{CUBE}.status = 'paid'"}]},
    {"name": "orders_self", "sql_table": "orders",
     "dimensions": [CUBE_ID, {"name": "status", "sql": "UPPER({CUBE}.status)"}],
     "measures": [{"name": "paid_sum", "type": "sum", "sql": "{CUBE}.amount", "filters": PAID}]},
    {"name": "customers", "sql_table": "customers",
     "dimensions": [CUBE_ID, {"name": "name", "sql": "{CUBE}.name"}]},
]
VIEWS = [{"name": "orders_view", "cubes": [{"join_path": "orders_self", "includes": ["id", "status", "paid_sum"]}]}]

DBT_MODELS = [
    {"name": "orders", "model": "orders",
     "entities": [{"name": "order", "type": "primary", "expr": "id"},
                  {"name": "customer", "type": "foreign", "expr": "customer_id"}],
     "dimensions": [
         {"name": "status_upper", "type": "categorical", "expr": "UPPER(status)"},
         {"name": "tags", "type": "categorical"},
     ],
     "measures": [
         {"name": "paid_cnt", "agg": "sum", "expr": "CASE WHEN status = 'paid' THEN 1 ELSE 0 END"},
         {"name": "paid_bool", "agg": "sum_boolean", "expr": "status = 'paid'"},
         {"name": "total", "agg": "sum", "expr": "amount"},
         {"name": "tag_cnt", "agg": "count", "expr": "tags"},
         {"name": "tag_kinds", "agg": "count_distinct", "expr": "tags"},
     ]},
    {"name": "orders_clash", "model": "orders",
     "entities": [{"name": "order_clash", "type": "primary", "expr": "id"},
                  {"name": "customer", "type": "foreign", "expr": "customer_id"}],
     "dimensions": [
         {"name": "status", "type": "categorical", "expr": "state"},
         {"name": "status_upper", "type": "categorical", "expr": "UPPER(status)"},
         {"name": "customer_id", "type": "categorical", "expr": "legacy_ref"},
     ],
     "measures": [
         {"name": "paid_cnt", "agg": "sum", "expr": "CASE WHEN status = 'paid' THEN 1 ELSE 0 END"},
         {"name": "paid_bool", "agg": "sum_boolean", "expr": "status = 'paid'"},
     ]},
    {"name": "orders_self", "model": "orders",
     "entities": [{"name": "order_self", "type": "primary", "expr": "id"}],
     "dimensions": [{"name": "status", "type": "categorical", "expr": "UPPER(status)"}]},
    {"name": "customers", "model": "customers",
     "entities": [{"name": "customer", "type": "primary", "expr": "id"}],
     "dimensions": [{"name": "name", "type": "categorical"}]},
]


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed(tmp_path)


async def _values(*, datasets: Iterable[SlayerDataset], db: Path, dataset: str, names: list[str]) -> dict[str, Any]:
    (row,) = await rows(datasets=datasets, db=db, payload={"population": dataset, "metrics": names})
    return {name: row[f"{dataset}.{name}"] for name in names}


UPPER_COUNTS = {"PAID": 2, "OPEN": 1}
STATE_COUNTS = {"open": 1, "paid": 1, "x": 1}


async def test_cube(db: Path) -> None:
    result = import_cube(CUBES, views=VIEWS, db=db)
    models = result.datasets
    assert await counts_by(datasets=models, db=db, dataset="orders", dimension="status_upper") == UPPER_COUNTS
    assert await counts_by(datasets=models, db=db, dataset="orders", dimension="is_paid") == {True: 2, False: 1}
    assert await counts_by(datasets=models, db=db, dataset="orders", dimension="customers.name") == {"Ann": 2, "Bo": 1}
    assert await by_id(datasets=models, db=db, dataset="orders", dimension="amount_x2") == [20, 40, 10]
    assert await _values(datasets=models, db=db, dataset="orders", names=[
        "paid_cnt", "paid_amount", "paid_sum", "via_dotted", "via_cube", "share", "tag_cnt",
    ]) == pytest.approx({"paid_cnt": 2, "paid_amount": 30, "paid_sum": 30, "via_dotted": 30, "via_cube": 30,
                         "share": 10, "tag_cnt": 2})
    tags = column(datasets_of(result)["orders"], "tags")
    assert (tags.datatype, tags.element_type) == (DataType.ARRAY, DataType.STRING)
    assert await _values(datasets=models, db=db, dataset="orders_self", names=["paid_sum"]) == {"paid_sum": 30}
    assert await _values(datasets=models, db=db, dataset="orders_view", names=["paid_sum"]) == {"paid_sum": 30}
    assert await counts_by(datasets=models, db=db, dataset="orders_clash", dimension="status") == STATE_COUNTS
    for refused in ("tag_kinds", "windowed"):
        assert not has_metric(result, "orders", refused)
        assert cube_issues(result, refused), refused
    assert await counts_by(datasets=models, db=db, dataset="orders_clash", dimension="status_upper") == UPPER_COUNTS
    assert await counts_by(datasets=models, db=db, dataset="orders_clash", dimension="is_paid") == {True: 2, False: 1}
    assert await counts_by(datasets=models, db=db, dataset="orders_clash", dimension="customers.name") == {
        "Ann": 2, "Bo": 1}
    assert await _values(datasets=models, db=db, dataset="orders_clash", names=["paid_cnt", "paid_sum"]) == {
        "paid_cnt": 2, "paid_sum": 30}
    assert await counts_by(datasets=models, db=db, dataset="orders_self", dimension="status") == UPPER_COUNTS
    assert await counts_by(datasets=models, db=db, dataset="orders_view", dimension="status") == UPPER_COUNTS


async def test_dbt(db: Path) -> None:
    result = import_dbt(DBT_MODELS, db=db)
    models = result.datasets
    assert await counts_by(datasets=models, db=db, dataset="orders", dimension="status_upper") == UPPER_COUNTS
    assert await counts_by(datasets=models, db=db, dataset="orders", dimension="customers.name") == {"Ann": 2, "Bo": 1}
    assert await _values(datasets=models, db=db, dataset="orders", names=["paid_cnt", "paid_bool", "total", "tag_cnt"]) \
        == {"paid_cnt": 2, "paid_bool": 2, "total": 35, "tag_cnt": 2}
    tags = column(datasets_of(result)["orders"], "tags")
    assert (tags.datatype, tags.element_type) == (DataType.ARRAY, DataType.STRING)
    assert await counts_by(datasets=models, db=db, dataset="orders_clash", dimension="status") == STATE_COUNTS
    assert not has_metric(result, "orders", "tag_kinds")
    assert entries(result, "tag_kinds")
    assert await counts_by(datasets=models, db=db, dataset="orders_clash", dimension="status_upper") == UPPER_COUNTS
    assert await counts_by(datasets=models, db=db, dataset="orders_clash", dimension="customers.name") == {
        "Ann": 2, "Bo": 1}
    assert await _values(datasets=models, db=db, dataset="orders_clash", names=["paid_cnt", "paid_bool"]) == {
        "paid_cnt": 2, "paid_bool": 2}
    assert await counts_by(datasets=models, db=db, dataset="orders_self", dimension="status") == UPPER_COUNTS

