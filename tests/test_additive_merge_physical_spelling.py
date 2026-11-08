"""Re-ingest matches a live column to the stored base column it is the physical spelling of."""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.engine.ingestion import _additive_merge_existing


def _stored(*, amount_type: DataType = DataType.DOUBLE) -> SlayerModel:
    """The 0.10.2 Cube-import shape: a renamed base column and a measure named like the physical column."""
    return SlayerModel(
        name="orders", sql_table="orders", data_source="ds",
        columns=[
            Column(name="order_id", type=DataType.INT, primary_key=True),
            Column(name="amount_col", sql="amount", type=amount_type),
        ],
        measures=[ModelMeasure(formula="sum(amount_col)", name="amount")],
    )


def _live(*, amount_type: DataType = DataType.DOUBLE, description: str | None = None) -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="ds",
        columns=[
            Column(name="order_id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=amount_type, description=description),
        ],
    )


def test_renamed_column_is_not_duplicated():
    result = _additive_merge_existing(persisted=_stored(), fresh=_live())
    assert result.new_columns == []
    assert [c.name for c in result.merged.columns] == ["order_id", "amount_col"]


def test_renamed_column_merges_live_metadata():
    result = _additive_merge_existing(persisted=_stored(), fresh=_live(description="Order amount"))
    assert result.described_columns == ["amount_col"]
    column = result.merged.get_column("amount_col")
    assert column is not None
    assert column.description == "Order amount"


def test_renamed_column_is_widened_on_sqlite():
    result = _additive_merge_existing(
        persisted=_stored(amount_type=DataType.INT), fresh=_live(amount_type=DataType.DOUBLE),
        sqlite_widen_enabled=True,
    )
    assert result.widened_columns == ["amount_col"]
    column = result.merged.get_column("amount_col")
    assert column is not None
    assert column.type is DataType.DOUBLE


def test_column_named_like_the_physical_spelling_keeps_its_own_match():
    stored = SlayerModel(
        name="t", sql_table="t", data_source="ds",
        columns=[Column(name="x", sql="y", type=DataType.INT), Column(name="y", type=DataType.INT)],
    )
    live = SlayerModel(
        name="t", sql_table="t", data_source="ds",
        columns=[Column(name="x", type=DataType.TEXT), Column(name="y", type=DataType.DOUBLE)],
    )
    result = _additive_merge_existing(persisted=stored, fresh=live, sqlite_widen_enabled=True)
    assert result.new_columns == []
    column = result.merged.get_column("y")
    assert column is not None
    assert column.type is DataType.DOUBLE
