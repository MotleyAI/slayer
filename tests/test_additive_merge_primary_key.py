"""Re-ingest gap-fills a stored dataset's missing primary key, and never changes one it has."""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.live_schema.ingestion import _additive_merge_existing


def _dataset(*, keys: set[str], fields: tuple[tuple[str, str | None], ...]) -> SlayerDataset:
    return SlayerDataset(
        name="supplies", sql_table="supplies", data_source="ds",
        fields=[DatasetField(name=name, expression=sql, datatype=DataType.STRING, primary_key=name in keys) for name, sql in fields],
    )


_LIVE = (("id", None), ("sku", None), ("cost", None))


def _keys(dataset: SlayerDataset) -> set[str]:
    return {c.name for c in dataset.fields if c.primary_key}


def test_keyless_model_gains_the_live_composite_key():
    result = _additive_merge_existing(
        persisted=_dataset(keys=set(), fields=_LIVE), fresh=_dataset(keys={"id", "sku"}, fields=_LIVE),
    )
    assert _keys(result.merged) == {"id", "sku"}
    assert result.has_changes


def test_stored_key_is_never_changed():
    result = _additive_merge_existing(
        persisted=_dataset(keys={"cost"}, fields=_LIVE), fresh=_dataset(keys={"id", "sku"}, fields=_LIVE),
    )
    assert _keys(result.merged) == {"cost"}
    assert not result.has_changes


def test_key_fills_onto_the_column_spelling_it():
    stored = _dataset(keys=set(), fields=(("supply_id", "id"), ("sku", None), ("cost", None)))
    result = _additive_merge_existing(persisted=stored, fresh=_dataset(keys={"id", "sku"}, fields=_LIVE))
    assert _keys(result.merged) == {"supply_id", "sku"}


def test_new_live_key_column_does_not_widen_a_stored_key():
    stored = _dataset(keys={"id"}, fields=(("id", None), ("cost", None)))
    result = _additive_merge_existing(persisted=stored, fresh=_dataset(keys={"id", "sku"}, fields=_LIVE))
    assert result.new_fields == ["sku"]
    assert _keys(result.merged) == {"id"}


def test_renamed_live_column_takes_its_key_and_comment():
    """A live ``_count`` is the dataset field ``count_col``."""
    def counts(*, key: bool, description: str | None) -> SlayerDataset:
        return SlayerDataset(name="t", sql_table="t", data_source="ds", fields=[
            DatasetField(name="count_col", expression="_count", datatype=DataType.INTEGER, primary_key=key, description=description),
        ])

    result = _additive_merge_existing(
        persisted=counts(key=False, description=None), fresh=counts(key=True, description="Live comment"),
    )
    column = result.merged.get_field("count_col")
    assert column is not None
    assert column.primary_key
    assert column.description == "Live comment"
    assert result.new_fields == []


def test_computed_column_named_like_the_key_takes_no_key_metadata():
    stored = SlayerDataset(name="t", sql_table="t", data_source="ds", fields=[
        DatasetField(name="id", expression="id + 1", datatype=DataType.INTEGER),
    ])
    fresh = SlayerDataset(name="t", sql_table="t", data_source="ds", fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True, unique=True),
    ])
    column = _additive_merge_existing(persisted=stored, fresh=fresh).merged.get_field("id")
    assert column is not None
    assert not column.primary_key
    assert not column.unique
    assert column.expression == "id + 1"


def test_composite_key_lands_whole_or_not_at_all():
    stored = _dataset(keys=set(), fields=(("id", "id || ''"), ("sku", None)))
    result = _additive_merge_existing(persisted=stored, fresh=_dataset(keys={"id", "sku"}, fields=_LIVE))
    assert _keys(result.merged) == set()


def test_key_metadata_follows_the_physical_column():
    """A stored ``id`` reading physical ``sku`` describes ``sku``, not the live ``id``."""
    stored = _dataset(keys=set(), fields=(("id", "sku"), ("sku", None)))
    fresh = SlayerDataset(name="supplies", sql_table="supplies", data_source="ds", fields=[
        DatasetField(name="id", datatype=DataType.STRING, primary_key=True, unique=True),
        DatasetField(name="sku", datatype=DataType.STRING, primary_key=True),
    ])
    merged = _additive_merge_existing(persisted=stored, fresh=fresh).merged
    assert _keys(merged) == set()
    column = merged.get_field("id")
    assert column is not None
    assert not column.unique
