"""Re-ingest gap-fills a stored model's missing primary key, and never changes one it has."""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.engine.ingestion import _additive_merge_existing


def _model(*, keys: set[str], columns: tuple[tuple[str, str | None], ...]) -> SlayerModel:
    return SlayerModel(
        name="supplies", sql_table="supplies", data_source="ds",
        columns=[Column(name=name, sql=sql, type=DataType.TEXT, primary_key=name in keys) for name, sql in columns],
    )


_LIVE = (("id", None), ("sku", None), ("cost", None))


def _keys(model: SlayerModel) -> set[str]:
    return {c.name for c in model.columns if c.primary_key}


def test_keyless_model_gains_the_live_composite_key():
    result = _additive_merge_existing(
        persisted=_model(keys=set(), columns=_LIVE), fresh=_model(keys={"id", "sku"}, columns=_LIVE),
    )
    assert _keys(result.merged) == {"id", "sku"}
    assert result.has_changes


def test_stored_key_is_never_changed():
    result = _additive_merge_existing(
        persisted=_model(keys={"cost"}, columns=_LIVE), fresh=_model(keys={"id", "sku"}, columns=_LIVE),
    )
    assert _keys(result.merged) == {"cost"}
    assert not result.has_changes


def test_key_fills_onto_the_column_spelling_it():
    stored = _model(keys=set(), columns=(("supply_id", "id"), ("sku", None), ("cost", None)))
    result = _additive_merge_existing(persisted=stored, fresh=_model(keys={"id", "sku"}, columns=_LIVE))
    assert _keys(result.merged) == {"supply_id", "sku"}


def test_new_live_key_column_does_not_widen_a_stored_key():
    stored = _model(keys={"id"}, columns=(("id", None), ("cost", None)))
    result = _additive_merge_existing(persisted=stored, fresh=_model(keys={"id", "sku"}, columns=_LIVE))
    assert result.new_columns == ["sku"]
    assert _keys(result.merged) == {"id"}


def test_renamed_live_column_takes_its_key_and_comment():
    """A live ``_count`` is the model column ``count_col``."""
    def counts(*, key: bool, description: str | None) -> SlayerModel:
        return SlayerModel(name="t", sql_table="t", data_source="ds", columns=[
            Column(name="count_col", sql="_count", type=DataType.INT, primary_key=key, description=description),
        ])

    result = _additive_merge_existing(
        persisted=counts(key=False, description=None), fresh=counts(key=True, description="Live comment"),
    )
    column = result.merged.get_column("count_col")
    assert column is not None
    assert column.primary_key
    assert column.description == "Live comment"
    assert result.new_columns == []


def test_computed_column_named_like_the_key_takes_no_key_metadata():
    stored = SlayerModel(name="t", sql_table="t", data_source="ds", columns=[
        Column(name="id", sql="id + 1", type=DataType.INT),
    ])
    fresh = SlayerModel(name="t", sql_table="t", data_source="ds", columns=[
        Column(name="id", type=DataType.INT, primary_key=True, unique=True),
    ])
    column = _additive_merge_existing(persisted=stored, fresh=fresh).merged.get_column("id")
    assert column is not None
    assert not column.primary_key
    assert not column.unique
    assert column.sql == "id + 1"


def test_composite_key_lands_whole_or_not_at_all():
    stored = _model(keys=set(), columns=(("id", "id || ''"), ("sku", None)))
    result = _additive_merge_existing(persisted=stored, fresh=_model(keys={"id", "sku"}, columns=_LIVE))
    assert _keys(result.merged) == set()
