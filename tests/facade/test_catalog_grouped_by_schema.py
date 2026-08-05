"""DEV-1594: build_catalog_grouped_by_schema — datasources as Postgres schemas.

Datasources map to schemas via ``postgres_schema`` (default ``public``); join
BFS stays per-datasource; same-name collisions in one schema are resolved by
``datasource_priority`` (loser shadowed).
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.facade.catalog import build_catalog_grouped_by_schema


def _model(name: str, ds: str, *, joins=None) -> SlayerModel:
    return SlayerModel(
        name=name, data_source=ds, sql_table=name,
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="val", type=DataType.DOUBLE),
        ],
        joins=joins or [],
    )


def _schema(catalog, name):
    return next(s for s in catalog.schemas if s.name == name)


def _table_names(schema) -> set[str]:
    return {t.name for t in schema.tables}


def test_default_groups_all_into_public():
    catalog = build_catalog_grouped_by_schema(
        models_by_datasource={
            "sales": [_model("orders", "sales")],
            "hr": [_model("people", "hr")],
        },
    )
    assert {s.name for s in catalog.schemas} == {"public"}
    assert _table_names(_schema(catalog, "public")) == {"orders", "people"}


def test_explicit_schema_separates_datasources():
    catalog = build_catalog_grouped_by_schema(
        models_by_datasource={
            "sales": [_model("orders", "sales")],
            "hr": [_model("people", "hr")],
        },
        schema_by_datasource={"hr": "hr_schema"},
    )
    assert {s.name for s in catalog.schemas} == {"public", "hr_schema"}
    assert _table_names(_schema(catalog, "public")) == {"orders"}
    assert _table_names(_schema(catalog, "hr_schema")) == {"people"}


def test_collision_resolved_by_priority():
    catalog = build_catalog_grouped_by_schema(
        models_by_datasource={
            "primary": [_model("orders", "primary")],
            "secondary": [_model("orders", "secondary")],
        },
        datasource_priority=["secondary", "primary"],
    )
    public = _schema(catalog, "public")
    tables = [t for t in public.tables if t.name == "orders"]
    assert len(tables) == 1
    # secondary has higher priority (earlier) -> it wins.
    assert tables[0].model_ref.data_source == "secondary"


def test_join_scoping_does_not_cross_datasources():
    # Both datasources have a 'customers' model; 'orders' in sales joins to it.
    # The join must resolve to sales.customers, never hr.customers.
    orders = _model(
        "orders", "sales",
        joins=[ModelJoin(target_model="customers", join_pairs=[["id", "id"]])],
    )
    catalog = build_catalog_grouped_by_schema(
        models_by_datasource={
            "sales": [orders, _model("customers", "sales")],
            "hr": [_model("customers", "hr")],
        },
        schema_by_datasource={"hr": "hr_schema"},
    )
    sales_orders = next(
        t for t in _schema(catalog, "public").tables if t.name == "orders"
    )
    assert [j.target_model for j in sales_orders.joins] == ["customers"]
