"""Re-ingest matches a live column to the stored base column it is the physical spelling of."""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.live_schema.ingestion import _additive_merge_existing


def _stored(*, amount_type: DataType = DataType.DOUBLE) -> SlayerDataset:
    """The 0.10.2 Cube-import shape: a renamed base column and a measure named like the physical column."""
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="ds",
        fields=[
            DatasetField(name="order_id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="amount_col", sql="amount", datatype=amount_type),
        ],
        metrics=[DatasetMetric(expression="sum(amount_col)", name="amount")],
    )


def _live(*, amount_type: DataType = DataType.DOUBLE, description: str | None = None) -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="ds",
        fields=[
            DatasetField(name="order_id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="amount", datatype=amount_type, description=description),
        ],
    )


def test_renamed_column_is_not_duplicated():
    result = _additive_merge_existing(persisted=_stored(), fresh=_live())
    assert result.new_fields == []
    assert [c.name for c in result.merged.fields] == ["order_id", "amount_col"]


def test_renamed_column_merges_live_metadata():
    result = _additive_merge_existing(persisted=_stored(), fresh=_live(description="Order amount"))
    assert result.described_fields == ["amount_col"]
    column = result.merged.get_field("amount_col")
    assert column is not None
    assert column.description == "Order amount"


def test_renamed_column_is_widened_on_sqlite():
    result = _additive_merge_existing(
        persisted=_stored(amount_type=DataType.INT), fresh=_live(amount_type=DataType.DOUBLE),
        sqlite_widen_enabled=True,
    )
    assert result.widened_fields == ["amount_col"]
    column = result.merged.get_field("amount_col")
    assert column is not None
    assert column.datatype is DataType.DOUBLE


def test_column_named_like_the_physical_spelling_keeps_its_own_match():
    stored = SlayerDataset(
        name="t", sql_table="t", data_source="ds",
        fields=[DatasetField(name="x", sql="y", datatype=DataType.INT), DatasetField(name="y", datatype=DataType.INT)],
    )
    live = SlayerDataset(
        name="t", sql_table="t", data_source="ds",
        fields=[DatasetField(name="x", datatype=DataType.TEXT), DatasetField(name="y", datatype=DataType.DOUBLE)],
    )
    result = _additive_merge_existing(persisted=stored, fresh=live, sqlite_widen_enabled=True)
    assert result.new_fields == []
    column = result.merged.get_field("y")
    assert column is not None
    assert column.datatype is DataType.DOUBLE


def test_live_column_named_like_a_measure_in_another_case_is_renamed():
    stored = SlayerDataset(name="t", sql_table="t", data_source="ds",
                         fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True)],
                         metrics=[DatasetMetric(expression="count(*)", name="premium")])
    live = SlayerDataset(name="t", sql_table="t", data_source="ds",
                       fields=[DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                                DatasetField(name="Premium", datatype=DataType.DOUBLE)])
    result = _additive_merge_existing(persisted=stored, fresh=live)
    column = result.merged.fields[-1]
    assert (column.name, column.physical_name) == ("Premium_2", "Premium")
