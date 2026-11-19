"""Cube and dbt imports read a physical column whose name a field has taken, through one shared hidden reader.

Specs: importers/meaning-preservation, importers/cube-import, importers/dbt-import.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.core.models import SlayerDataset
from tests._dev2171_fixtures import (
    by_id, counts_by, cube_issues, datasets_of, has_column, import_cube, import_dbt, rows, seed, value,
)

ID = {"name": "id", "sql": "{CUBE}.id", "type": "number", "primary_key": True}
SELF_NAMED = {"name": "status", "sql": "UPPER({CUBE}.status)"}
CUSTOMERS = {"name": "customers", "sql_table": "customers", "dimensions": [
    ID, {"name": "name", "sql": "{CUBE}.name"}, {"name": "region", "sql": "{CUBE}.region"}]}
JOIN = {"name": "customers", "relationship": "many_to_one", "sql": "{CUBE}.customer_id = {customers}.id"}


def dim(name: str, sql: str, **extra: Any) -> dict[str, Any]:
    return {"name": name, "sql": sql, **extra}


def cube(name: str = "orders", *, dimensions: list[dict[str, Any]] | None = None, **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"name": name, "dimensions": [ID, *(dimensions or [])], "measures": [], **extra}
    if "sql" not in extra:
        out.setdefault("sql_table", "orders")
    return out


def fields_of(dataset: SlayerDataset) -> list[dict[str, Any]]:
    return dataset.model_dump(mode="json", exclude_none=True)["fields"]


def readers_of(dataset: SlayerDataset, column: str) -> list[dict[str, Any]]:
    """Hidden fields of ``dataset`` reading the physical ``column`` bare and unfiltered."""
    return [f for f in fields_of(dataset)
            if f.get("hidden") and f.get("expression") == column and "filter" not in f]


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed(tmp_path)


# ── Cube ──────────────────────────────────────────────────────────────────

async def test_another_cubes_physical_column_masked(db: Path) -> None:
    masked = {**CUSTOMERS, "dimensions": [ID, dim("name", "{CUBE}.region")]}
    result = import_cube([cube(dimensions=[dim("cust_name", "{customers}.name")], joins=[JOIN]), masked], db=db)
    assert readers_of(datasets_of(result)["customers"], "name")
    assert not cube_issues(result, "cust_name")
    assert await counts_by(datasets=result.datasets, db=db, dataset="orders", dimension="cust_name") == {
        "Ann": 2, "Bo": 1}


async def test_pushdown_body_over_a_masked_column(db: Path) -> None:
    source = "SELECT * FROM orders WHERE {FILTER_PARAMS.orders.status.filter('status')}"
    result = import_cube([cube(sql=source, dimensions=[SELF_NAMED])], db=db)
    models = result.datasets
    assert await counts_by(datasets=models, db=db, dataset="orders", dimension="status") == {"PAID": 2, "OPEN": 1}
    out = await rows(datasets=models, db=db, payload={
        "population": "orders", "dimensions": ["id"], "order": [{"field": "id"}], "variables": {"status": ["paid"]}})
    assert [r["orders.id"] for r in out] == [1, 2]
    assert await value(datasets=models, db=db, dataset="orders", expression="count(*)") == 3


class TestHiddenReader:
    @pytest.mark.parametrize("reverse", [False, True], ids=["declared", "reversed"])
    async def test_a_taken_name_gets_the_next_suffix(self, db: Path, reverse: bool) -> None:
        join = {**JOIN, "sql": "{CUBE}.status = {customers}.region"}
        dimensions = [dim("status_raw", "{CUBE}.state"), dim("status", "{CUBE}.state")]
        result = import_cube([cube(dimensions=dimensions[::-1] if reverse else dimensions,
                                   joins=[join]), CUSTOMERS], db=db)
        orders = datasets_of(result)["orders"]
        (helper,) = readers_of(orders, "status")
        assert helper["name"] == "status_raw_2"
        (relationship,) = orders.relationships
        assert relationship.from_columns == ["status_raw_2"]
        assert await counts_by(datasets=result.datasets, db=db, dataset="orders", dimension="status_raw") == {
            "open": 1, "paid": 1, "x": 1}

    def test_one_helper_serves_every_surface(self, db: Path) -> None:
        orders = cube(dimensions=[SELF_NAMED])
        back = {"name": "orders", "relationship": "many_to_one", "sql": "{CUBE}.region = {orders}.status"}
        customers = {**CUSTOMERS, "joins": [back],
                     "dimensions": [*CUSTOMERS["dimensions"], dim("order_status", "{orders}.status")]}
        result = import_cube([orders, customers], db=db)
        imported = datasets_of(result)
        (helper,) = readers_of(imported["orders"], "status")
        (to_orders,) = imported["customers"].relationships
        assert to_orders.to_columns == [helper["name"]]
        assert has_column(result, "customers", "order_status")
        assert not cube_issues(result, "order_status")

    def test_re_import_is_stable(self, db: Path) -> None:
        project = [cube(dimensions=[SELF_NAMED, dim("customer_id", "{CUBE}.legacy_ref", type="number"),
                                    dim("status_upper", "UPPER({CUBE}.status)")], joins=[JOIN]), CUSTOMERS]
        first, second = import_cube(project, db=db), import_cube(project, db=db)
        assert has_column(first, "orders", "status")
        assert [d.model_dump(mode="json") for d in first.datasets] == [d.model_dump(mode="json") for d in second.datasets]


# ── dbt ───────────────────────────────────────────────────────────────────

async def test_dbt_source_key_named_by_a_derived_dimension(db: Path) -> None:
    orders = {"name": "orders", "model": "orders",
              "entities": [{"name": "order", "type": "primary", "expr": "id"},
                           {"name": "customer", "type": "foreign", "expr": "customer_id"}],
              "dimensions": [{"name": "customer_id", "type": "categorical", "expr": "UPPER(customer_id)"}]}
    customers = {"name": "customers", "model": "customers",
                 "entities": [{"name": "customer", "type": "primary", "expr": "id"}],
                 "dimensions": [{"name": "name", "type": "categorical"}]}
    result = import_dbt([orders, customers], db=db)
    imported = datasets_of(result)["orders"]
    (helper,) = readers_of(imported, "customer_id")
    (relationship,) = imported.relationships
    assert relationship.from_columns == [helper["name"]]
    assert await counts_by(datasets=result.datasets, db=db, dataset="orders", dimension="customers.name") == {
        "Ann": 2, "Bo": 1}


async def test_nested_scope_dimension_keeps_its_subquery(db: Path) -> None:
    result = import_cube([cube(dimensions=[dim("amount", "{CUBE}.amount * 2", type="number"),
                                           dim("min_amount", "(SELECT MIN(amount) FROM orders)", type="number")])],
                         db=db)
    (field,) = [f for f in fields_of(datasets_of(result)["orders"]) if f["name"] == "min_amount"]
    assert any("SELECT MIN(amount) FROM orders" in e["expression"] for e in field["expression"]["dialects"])
    assert await by_id(datasets=result.datasets, db=db, dataset="orders", dimension="min_amount") == [5, 5, 5]
