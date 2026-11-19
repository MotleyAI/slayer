"""Tests for FacadeTable.relationships[] + dataset_ref.

Lives in a sibling file so the FacadeRelationship / dataset_ref imports do not block
collection of the broader test_catalog.py suite during the TDD-Phase-1
window (before slayer.facade.catalog gains these symbols)."""

from __future__ import annotations

from slayer.core.enums import DataType, JoinType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.facade.catalog import (
    FacadeCatalog,
    FacadeRelationship,
    FacadeTable,
    build_catalog,
)


def _dataset(
    *,
    name: str,
    data_source: str = "ds1",
    columns: list[DatasetField] | None = None,
    joins: list[DatasetRelationship] | None = None,
    hidden: bool = False,
) -> SlayerDataset:
    return SlayerDataset(
        name=name,
        data_source=data_source,
        sql_table=name,
        fields=columns or [],
        relationships=joins or [],
        hidden=hidden,
    )


def _find_table(catalog: FacadeCatalog, *, schema: str, table: str) -> FacadeTable:
    schema_obj = next(s for s in catalog.schemas if s.name == schema)
    return next(t for t in schema_obj.tables if t.name == table)


def test_facade_table_joins_populated_from_model_joins() -> None:
    orders = _dataset(
        name="orders",
        columns=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="store_id", datatype=DataType.INTEGER),
        ],
        joins=[DatasetRelationship(to="stores", from_columns=["store_id"], to_columns=["id"])],
    )
    stores = _dataset(
        name="stores",
        columns=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", datatype=DataType.STRING),
        ],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [orders, stores]})
    table = _find_table(cat, schema="ds1", table="orders")
    assert table.relationships == [
        FacadeRelationship(
            to="stores",
            column_pairs=[("store_id", "id")],
            join_type=JoinType.LEFT,
        ),
    ]


def test_facade_table_joins_preserve_join_type() -> None:
    orders = _dataset(
        name="orders",
        columns=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="store_id", datatype=DataType.INTEGER),
        ],
        joins=[DatasetRelationship(
            to="stores",
            from_columns=["store_id"], to_columns=["id"],
            join_type=JoinType.INNER,
        )],
    )
    stores = _dataset(
        name="stores",
        columns=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [orders, stores]})
    table = _find_table(cat, schema="ds1", table="orders")
    assert table.relationships[0].join_type == JoinType.INNER


def test_facade_table_joins_filter_to_hidden_target() -> None:
    """Relationships whose target is hidden (or absent) are excluded — symmetric
    with the BFS dim/metric expansion's hidden-target filter. Otherwise the
    translator's existence check could match a relationship that isn't addressable
    via the rest of the catalog."""
    orders = _dataset(
        name="orders",
        columns=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="store_id", datatype=DataType.INTEGER),
        ],
        joins=[DatasetRelationship(to="stores", from_columns=["store_id"], to_columns=["id"])],
    )
    hidden_stores = _dataset(
        name="stores",
        columns=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
        hidden=True,
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [orders, hidden_stores]})
    table = _find_table(cat, schema="ds1", table="orders")
    assert table.relationships == []


def test_facade_table_joins_filter_to_absent_target() -> None:
    """If a DatasetRelationship points at a target dataset that isn't in the catalog at
    all (typo / cross-datasource / deleted), the relationship is silently dropped
    from FacadeTable.relationships[] — symmetric with the hidden-target filter and
    with the BFS dim/metric expansion's own filter."""
    orders = _dataset(
        name="orders",
        columns=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="store_id", datatype=DataType.INTEGER),
        ],
        joins=[DatasetRelationship(to="ghost", from_columns=["store_id"], to_columns=["id"])],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [orders]})
    table = _find_table(cat, schema="ds1", table="orders")
    assert table.relationships == []


def test_facade_table_model_ref_carries_underlying_slayer_model() -> None:
    """Hidden FK fields must remain visible on dataset_ref.fields so the
    translator's ON-column existence check (against SlayerDataset.fields,
    not the filtered facade dimensions) keeps working."""
    orders = _dataset(
        name="orders",
        columns=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="store_id", datatype=DataType.INTEGER, hidden=True),
        ],
        joins=[DatasetRelationship(to="stores", from_columns=["store_id"], to_columns=["id"])],
    )
    stores = _dataset(
        name="stores",
        columns=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
    )
    cat = build_catalog(datasets_by_datasource={"ds1": [orders, stores]})
    table = _find_table(cat, schema="ds1", table="orders")
    dim_names = {d.name for d in table.dimensions}
    assert "store_id" not in dim_names
    assert any(c.name == "store_id" for c in table.dataset_ref.fields)
    assert table.dataset_ref.relationships[0].to == "stores"
