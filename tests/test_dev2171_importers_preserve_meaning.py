"""Every importer reads every known ambiguous form with the source's meaning (principle 20;
spec: importers/meaning-preservation › Every importer is checked against the source's numbers)."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import SlayerModel
from tests._dev2171_fixtures import (
    by_id, column, counts_by, cube_issues, entries, has_column, has_measure, import_cube, import_dbt, import_osi,
    models_of, rows, seed,
)
from tests._osi_helpers import ds, f, flat, m, rel, unconverted

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

OSI_DOCUMENT = flat([
    ds("orders", fields=[
        f("id"), f("status"), f("status_upper", "UPPER(status)"), f("tags"), f("note"), f("amount"),
        f("customer_id"),
        f("has_a", "ARRAY_CONTAINS(tags, 'a')"), f("n_tags", "CARDINALITY(tags)"),
    ], metrics=[
        m("with_b", "COUNT(*) FILTER (WHERE 'b' = ANY(tags))"), m("tag_cnt", "COUNT(tags)"),
        m("obrien", "COUNT(*) FILTER (WHERE note = 'O''Brien\\x')"),
        m("doubled", "SUM(amount * 2)"), m("padded", "SUM(LENGTH(LPAD(status, 12, 'x')))"),
        m("tag_max", "MAX(tags)"), m("tag_kinds", "COUNT(DISTINCT tags)"),
    ]),
    ds("orders_clash", source="orders", fields=[f("id"), f("status", "state"), f("status_upper", "UPPER(status)")]),
    ds("orders_self", source="orders", fields=[f("id"), f("status", "UPPER(status)")]),
    ds("customers", fields=[f("id"), f("name")]),
], relationships=[rel("o2c", src="orders", to="customers", from_columns=["customer_id"], to_columns=["id"])],
   metrics=[m("statuses", "ARRAY_AGG(DISTINCT orders.status_upper)"),
            m("plus_customer", "SUM(orders.amount + customers.id)")])


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed(tmp_path)


async def _values(*, models: Iterable[SlayerModel], db: Path, model: str, names: list[str]) -> dict[str, Any]:
    (row,) = await rows(models=models, db=db, payload={"source_model": model, "measures": names})
    return {name: row[f"{model}.{name}"] for name in names}


UPPER_COUNTS = {"PAID": 2, "OPEN": 1}
STATE_COUNTS = {"open": 1, "paid": 1, "x": 1}


async def test_cube(db: Path) -> None:
    result = import_cube(CUBES, views=VIEWS, db=db)
    models = result.models
    assert await counts_by(models=models, db=db, model="orders", dimension="status_upper") == UPPER_COUNTS
    assert await counts_by(models=models, db=db, model="orders", dimension="is_paid") == {True: 2, False: 1}
    assert await counts_by(models=models, db=db, model="orders", dimension="customers.name") == {"Ann": 2, "Bo": 1}
    assert await by_id(models=models, db=db, model="orders", dimension="amount_x2") == [20, 40, 10]
    assert await _values(models=models, db=db, model="orders", names=[
        "paid_cnt", "paid_amount", "paid_sum", "via_dotted", "via_cube", "share", "tag_cnt",
    ]) == pytest.approx({"paid_cnt": 2, "paid_amount": 30, "paid_sum": 30, "via_dotted": 30, "via_cube": 30,
                         "share": 10, "tag_cnt": 2})
    tags = column(models_of(result)["orders"], "tags")
    assert (tags.type, tags.element_type) == (DataType.ARRAY, DataType.TEXT)
    assert await _values(models=models, db=db, model="orders_self", names=["paid_sum"]) == {"paid_sum": 30}
    assert await _values(models=models, db=db, model="orders_view", names=["paid_sum"]) == {"paid_sum": 30}
    assert await counts_by(models=models, db=db, model="orders_clash", dimension="status") == STATE_COUNTS
    for refused in ("tag_kinds", "windowed"):
        assert not has_measure(result, "orders", refused)
        assert cube_issues(result, refused), refused
    clash = models_of(result)["orders_clash"]
    for member in ("status_upper", "paid_cnt", "paid_sum", "is_paid"):
        assert clash.get_column(member) is None, member
        assert clash.get_measure(member) is None, member
        assert [i for i in cube_issues(result, member) if "status" in i.message], member
    assert clash.joins == []
    assert [i for i in cube_issues(result, "customer_id") if i.cube == "orders_clash"]
    assert not has_column(result, "orders_self", "status")
    assert cube_issues(result, "status")


async def test_dbt(db: Path) -> None:
    result = import_dbt(DBT_MODELS, db=db)
    models = result.models
    assert await counts_by(models=models, db=db, model="orders", dimension="status_upper") == UPPER_COUNTS
    assert await counts_by(models=models, db=db, model="orders", dimension="customers.name") == {"Ann": 2, "Bo": 1}
    assert await _values(models=models, db=db, model="orders", names=["paid_cnt", "paid_bool", "total", "tag_cnt"]) \
        == {"paid_cnt": 2, "paid_bool": 2, "total": 35, "tag_cnt": 2}
    tags = column(models_of(result)["orders"], "tags")
    assert (tags.type, tags.element_type) == (DataType.ARRAY, DataType.TEXT)
    assert await counts_by(models=models, db=db, model="orders_clash", dimension="status") == STATE_COUNTS
    assert not has_measure(result, "orders", "tag_kinds")
    assert entries(result, "tag_kinds")
    for member in ("status_upper", "paid_cnt", "paid_bool"):
        assert not has_column(result, "orders_clash", member)
        assert not has_measure(result, "orders_clash", member)
        assert [e for e in entries(result, member) if "status" in e.message], member
    assert models_of(result)["orders_clash"].joins == []
    assert entries(result, "customer_id")
    assert not has_column(result, "orders_self", "status")
    assert entries(result, "status")


async def test_osi(db: Path) -> None:
    result = import_osi(OSI_DOCUMENT, db=db)
    models = result.models
    assert await counts_by(models=models, db=db, model="orders", dimension="status_upper") == UPPER_COUNTS
    assert await by_id(models=models, db=db, model="orders", dimension="has_a") == [True, False, None]
    assert await by_id(models=models, db=db, model="orders", dimension="n_tags") == [2, 1, None]
    assert await _values(models=models, db=db, model="orders", names=[
        "with_b", "tag_cnt", "obrien", "doubled", "padded", "plus_customer",
    ]) == {"with_b": 2, "tag_cnt": 2, "obrien": 1, "doubled": 70, "padded": 36, "plus_customer": 339}
    (statuses,) = (await _values(models=models, db=db, model="orders", names=["statuses"])).values()
    assert sorted(statuses) == ["OPEN", "PAID"]
    assert await counts_by(models=models, db=db, model="orders_clash", dimension="status") == STATE_COUNTS
    for refused in ("tag_max", "tag_kinds"):
        assert unconverted(result, refused), refused
    assert not has_column(result, "orders_clash", "status_upper")
    assert [e for e in entries(result, "status_upper") if "status" in e.message]
    assert not has_column(result, "orders_self", "status")
    assert entries(result, "status")
