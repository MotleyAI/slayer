"""Until DEV-2175, a construct reading a physical name a different column holds, and a self-named member, is
dropped with a report; the rest of the model imports and saves."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slayer.osi.models import OSIDocument
from slayer.osi.parser import parse_osi_path
from tests._dev2171_fixtures import (
    by_id, column, convert_osi, counts_by, cube_issues, entries, has_column, has_measure, import_cube, import_dbt,
    models_of, seed, value,
)
from tests._osi_helpers import ds, f, flat, legacy, write_yaml

STATE_COUNTS = {"open": 1, "paid": 1, "x": 1}
ID = {"name": "id", "sql": "{CUBE}.id", "type": "number", "primary_key": True}
RENAMED_STATUS = {"name": "status", "sql": "{CUBE}.state"}
PAID = [{"sql": "{CUBE}.status = 'paid'"}]
CUSTOMERS = {"name": "customers", "sql_table": "customers", "dimensions": [ID, {"name": "name", "sql": "{CUBE}.name"}]}
JOIN = {"name": "customers", "relationship": "many_to_one", "sql": "{CUBE}.customer_id = {customers}.id"}


@pytest.fixture
def db(tmp_path: Path) -> Path:
    return seed(tmp_path)


def _reported(issues: list[Any], name: str) -> bool:
    return any(name in i.message for i in issues)


class TestCube:
    CLASHING = [
        {"name": "status_upper", "sql": "UPPER({CUBE}.status)"},
        {"name": "bucket", "case": {"when": [{"sql": "{CUBE}.status = 'paid'", "label": "P"}], "else": {"label": "O"}}},
    ]
    MEASURES = [
        {"name": "paid_amount", "type": "sum", "sql": "{CUBE}.amount", "filters": PAID},
        {"name": "paid_cnt", "type": "number", "sql": "SUM(CASE WHEN {CUBE}.status = 'paid' THEN 1 END)"},
        {"name": "total", "type": "sum", "sql": "{CUBE}.amount"},
    ]

    @pytest.mark.parametrize("status_first", [True, False])
    async def test_collisions_are_dropped(self, db: Path, status_first: bool) -> None:
        dims = [RENAMED_STATUS, *self.CLASHING] if status_first else [*self.CLASHING, RENAMED_STATUS]
        cube = {"name": "orders", "sql_table": "orders", "dimensions": [ID, *dims], "measures": self.MEASURES,
                "segments": [{"name": "is_paid", "sql": "{CUBE}.status = 'paid'"}]}
        result = import_cube([cube], db=db)
        model = models_of(result)["orders"]
        for name in ("status_upper", "bucket", "is_paid", "paid_amount", "paid_cnt"):
            assert model.get_column(name) is None, name
            assert model.get_measure(name) is None, name
            assert _reported(cube_issues(result, name), "status"), name
        assert await counts_by(models=result.models, db=db, model="orders", dimension="status") == STATE_COUNTS
        assert await value(models=result.models, db=db, model="orders", formula="total") == 35

    async def test_self_named_member_is_dropped(self, db: Path) -> None:
        cube = {"name": "orders", "sql_table": "orders",
                "dimensions": [ID, {"name": "status", "sql": "UPPER({CUBE}.status)"}],
                "measures": [{"name": "paid_amount", "type": "sum", "sql": "{CUBE}.amount", "filters": PAID}]}
        result = import_cube([cube], db=db)
        assert not has_column(result, "orders", "status")
        assert cube_issues(result, "status")
        assert await value(models=result.models, db=db, model="orders", formula="paid_amount") == 30

    def test_masked_join_key_drops_the_join(self) -> None:
        cube = {"name": "orders", "sql_table": "orders", "joins": [JOIN],
                "dimensions": [ID, {"name": "customer_id", "sql": "{CUBE}.legacy_ref", "type": "number"}]}
        result = import_cube([cube, CUSTOMERS])
        assert models_of(result)["orders"].joins == []
        assert _reported(cube_issues(result, "customers"), "customer_id")

    async def test_nested_scope_reference_does_not_collide(self, db: Path) -> None:
        cube = {"name": "orders", "sql_table": "orders", "dimensions": [
            ID, {"name": "amount", "sql": "{CUBE}.amount * 2", "type": "number"},
            {"name": "min_amount", "sql": "(SELECT MIN(amount) FROM orders)", "type": "number"}]}
        result = import_cube([cube], db=db)
        assert "SELECT MIN(amount) FROM orders" in (column(models_of(result)["orders"], "min_amount").sql or "")
        assert await by_id(models=result.models, db=db, model="orders", dimension="min_amount") == [5, 5, 5]

    async def test_view_over_a_filtered_measure(self, db: Path) -> None:
        cube = {"name": "orders", "sql_table": "orders", "dimensions": [ID], "measures": [
            {"name": "paid_sum", "type": "sum", "sql": "{CUBE}.amount", "filters": PAID}]}
        view = {"name": "orders_view", "cubes": [{"join_path": "orders", "includes": ["id", "paid_sum"]}]}
        result = import_cube([cube], views=[view], db=db)
        assert await value(models=result.models, db=db, model="orders_view", formula="paid_sum") == 30


class TestDbt:
    async def test_collisions_are_dropped(self, db: Path) -> None:
        sm = {"name": "orders", "model": "orders",
              "entities": [{"name": "order", "type": "primary", "expr": "id"}],
              "dimensions": [{"name": "status", "type": "categorical", "expr": "state"},
                             {"name": "status_upper", "type": "categorical", "expr": "UPPER(status)"}],
              "measures": [{"name": "paid_cnt", "agg": "sum", "expr": "CASE WHEN status = 'paid' THEN 1 ELSE 0 END"},
                           {"name": "paid_bool", "agg": "sum_boolean", "expr": "status = 'paid'"},
                           {"name": "total", "agg": "sum", "expr": "amount"}]}
        result = import_dbt([sm], db=db)
        for name in ("status_upper", "paid_cnt", "paid_bool"):
            assert not has_column(result, "orders", name), name
            assert not has_measure(result, "orders", name), name
            assert _reported(entries(result, name), "status"), name
        assert await counts_by(models=result.models, db=db, model="orders", dimension="status") == STATE_COUNTS
        assert await value(models=result.models, db=db, model="orders", formula="total") == 35

    def test_self_named_member_and_masked_entity(self) -> None:
        orders = {"name": "orders", "model": "orders",
                  "entities": [{"name": "order", "type": "primary", "expr": "id"},
                               {"name": "customer", "type": "foreign", "expr": "customer_id"}],
                  "dimensions": [{"name": "status", "type": "categorical", "expr": "UPPER(status)"},
                                 {"name": "customer_id", "type": "categorical", "expr": "legacy_ref"}]}
        customers = {"name": "customers", "model": "customers",
                     "entities": [{"name": "customer", "type": "primary", "expr": "id"}]}
        result = import_dbt([orders, customers])
        assert not has_column(result, "orders", "status")
        assert entries(result, "status")
        assert models_of(result)["orders"].joins == []
        assert entries(result, "customer_id")


@pytest.mark.parametrize("kind", ["current", "legacy"])
async def test_osi_collisions_are_dropped(db: Path, tmp_path: Path, kind: str) -> None:
    datasets = [ds("orders", fields=[f("id"), f("status", "state"), f("status_upper", "UPPER(status)")]),
                ds("orders_self", source="orders", fields=[f("id"), f("status", "UPPER(status)")])]
    docs = ([OSIDocument.model_validate(flat(datasets))] if kind == "current"
            else parse_osi_path(write_yaml(tmp_path / "legacy.yaml", legacy({"name": "shop", "datasets": datasets}))))
    result = convert_osi(docs, db=db)
    assert not has_column(result, "orders", "status_upper")
    assert _reported(entries(result, "status_upper"), "status")
    assert await counts_by(models=result.models, db=db, model="orders", dimension="status") == STATE_COUNTS
    self_status = models_of(result)["orders_self"].get_column("status")
    assert self_status is None if kind == "current" else (
        self_status is not None and self_status.is_base and self_status.physical_name == "status")
    assert entries(result, "status")
