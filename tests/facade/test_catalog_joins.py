"""Tests for FacadeTable.joins[] + model_ref (DEV-1565).

Lives in a sibling file so the FacadeJoin / model_ref imports do not block
collection of the broader test_catalog.py suite during the TDD-Phase-1
window (before slayer.facade.catalog gains these symbols)."""

from __future__ import annotations

from slayer.core.enums import DataType, JoinType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.facade.catalog import (
    FacadeCatalog,
    FacadeJoin,
    FacadeTable,
    build_catalog,
)


def _model(
    *,
    name: str,
    data_source: str = "ds1",
    columns: list[Column] | None = None,
    joins: list[ModelJoin] | None = None,
    hidden: bool = False,
) -> SlayerModel:
    return SlayerModel(
        name=name,
        data_source=data_source,
        sql_table=name,
        columns=columns or [],
        joins=joins or [],
        hidden=hidden,
    )


def _find_table(catalog: FacadeCatalog, *, schema: str, table: str) -> FacadeTable:
    schema_obj = next(s for s in catalog.schemas if s.name == schema)
    return next(t for t in schema_obj.tables if t.name == table)


def test_facade_table_joins_populated_from_model_joins() -> None:
    orders = _model(
        name="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="store_id", type=DataType.INT),
        ],
        joins=[ModelJoin(target_model="stores", join_pairs=[["store_id", "id"]])],
    )
    stores = _model(
        name="stores",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )
    cat = build_catalog(models_by_datasource={"ds1": [orders, stores]})
    table = _find_table(cat, schema="ds1", table="orders")
    assert table.joins == [
        FacadeJoin(
            target_model="stores",
            join_pairs=[["store_id", "id"]],
            join_type=JoinType.LEFT,
        ),
    ]


def test_facade_table_joins_preserve_join_type() -> None:
    orders = _model(
        name="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="store_id", type=DataType.INT),
        ],
        joins=[ModelJoin(
            target_model="stores",
            join_pairs=[["store_id", "id"]],
            join_type=JoinType.INNER,
        )],
    )
    stores = _model(
        name="stores",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    )
    cat = build_catalog(models_by_datasource={"ds1": [orders, stores]})
    table = _find_table(cat, schema="ds1", table="orders")
    assert table.joins[0].join_type == JoinType.INNER


def test_facade_table_joins_filter_to_hidden_target() -> None:
    """Joins whose target is hidden (or absent) are excluded — symmetric
    with the BFS dim/metric expansion's hidden-target filter. Otherwise the
    translator's existence check could match a join that isn't addressable
    via the rest of the catalog."""
    orders = _model(
        name="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="store_id", type=DataType.INT),
        ],
        joins=[ModelJoin(target_model="stores", join_pairs=[["store_id", "id"]])],
    )
    hidden_stores = _model(
        name="stores",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        hidden=True,
    )
    cat = build_catalog(models_by_datasource={"ds1": [orders, hidden_stores]})
    table = _find_table(cat, schema="ds1", table="orders")
    assert table.joins == []


def test_facade_table_joins_filter_to_absent_target() -> None:
    """If a ModelJoin points at a target model that isn't in the catalog at
    all (typo / cross-datasource / deleted), the join is silently dropped
    from FacadeTable.joins[] — symmetric with the hidden-target filter and
    with the BFS dim/metric expansion's own filter."""
    orders = _model(
        name="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="store_id", type=DataType.INT),
        ],
        joins=[ModelJoin(target_model="ghost", join_pairs=[["store_id", "id"]])],
    )
    cat = build_catalog(models_by_datasource={"ds1": [orders]})
    table = _find_table(cat, schema="ds1", table="orders")
    assert table.joins == []


def test_facade_table_model_ref_carries_underlying_slayer_model() -> None:
    """Hidden FK columns must remain visible on model_ref.columns so the
    translator's ON-column existence check (against SlayerModel.columns,
    not the filtered facade dimensions) keeps working."""
    orders = _model(
        name="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="store_id", type=DataType.INT, hidden=True),
        ],
        joins=[ModelJoin(target_model="stores", join_pairs=[["store_id", "id"]])],
    )
    stores = _model(
        name="stores",
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
    )
    cat = build_catalog(models_by_datasource={"ds1": [orders, stores]})
    table = _find_table(cat, schema="ds1", table="orders")
    dim_names = {d.name for d in table.dimensions}
    assert "store_id" not in dim_names
    assert any(c.name == "store_id" for c in table.model_ref.columns)
    assert table.model_ref.joins[0].target_model == "stores"
