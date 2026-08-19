"""Data-model coverage for ``ModelJoin.cardinality`` and ``Column.unique``."""

import tempfile

from slayer.core.enums import DataType, JoinCardinality, JoinType, invert_cardinality
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# JoinCardinality enum
# ---------------------------------------------------------------------------


def test_join_cardinality_enum_values() -> None:
    # LookML-style string values, source->target reading.
    assert JoinCardinality.ONE_TO_ONE == "one_to_one"
    assert JoinCardinality.ONE_TO_MANY == "one_to_many"
    assert JoinCardinality.MANY_TO_ONE == "many_to_one"
    assert JoinCardinality.MANY_TO_MANY == "many_to_many"
    assert {c.value for c in JoinCardinality} == {
        "one_to_one",
        "one_to_many",
        "many_to_one",
        "many_to_many",
    }


def test_join_cardinality_from_string() -> None:
    assert JoinCardinality("many_to_one") is JoinCardinality.MANY_TO_ONE


# ---------------------------------------------------------------------------
# invert_cardinality
# ---------------------------------------------------------------------------


def test_invert_cardinality_swaps_directional() -> None:
    assert invert_cardinality(JoinCardinality.MANY_TO_ONE) is JoinCardinality.ONE_TO_MANY
    assert invert_cardinality(JoinCardinality.ONE_TO_MANY) is JoinCardinality.MANY_TO_ONE


def test_invert_cardinality_self_inverse_symmetric() -> None:
    assert invert_cardinality(JoinCardinality.ONE_TO_ONE) is JoinCardinality.ONE_TO_ONE
    assert invert_cardinality(JoinCardinality.MANY_TO_MANY) is JoinCardinality.MANY_TO_MANY


def test_invert_cardinality_none_passthrough() -> None:
    assert invert_cardinality(None) is None


def test_invert_cardinality_is_involutive() -> None:
    for c in JoinCardinality:
        assert invert_cardinality(invert_cardinality(c)) is c


# ---------------------------------------------------------------------------
# ModelJoin.cardinality
# ---------------------------------------------------------------------------


def test_modeljoin_cardinality_defaults_none() -> None:
    j = ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])
    assert j.cardinality is None


def test_modeljoin_accepts_cardinality_enum_and_string() -> None:
    j1 = ModelJoin(
        target_model="customers",
        join_pairs=[["customer_id", "id"]],
        cardinality=JoinCardinality.MANY_TO_ONE,
    )
    assert j1.cardinality is JoinCardinality.MANY_TO_ONE

    j2 = ModelJoin(
        target_model="customers",
        join_pairs=[["customer_id", "id"]],
        cardinality="one_to_one",
    )
    assert j2.cardinality is JoinCardinality.ONE_TO_ONE


def test_modeljoin_cardinality_serializes() -> None:
    j = ModelJoin(
        target_model="customers",
        join_pairs=[["customer_id", "id"]],
        cardinality=JoinCardinality.MANY_TO_ONE,
    )
    dumped = j.model_dump()
    assert dumped["cardinality"] == "many_to_one"
    # Round-trips back into an equivalent object.
    assert ModelJoin.model_validate(dumped).cardinality is JoinCardinality.MANY_TO_ONE


def test_modeljoin_cardinality_orthogonal_to_join_type() -> None:
    # LEFT join can still carry many_to_one; the two axes are independent.
    j = ModelJoin(
        target_model="customers",
        join_pairs=[["customer_id", "id"]],
        join_type=JoinType.LEFT,
        cardinality=JoinCardinality.MANY_TO_ONE,
    )
    assert j.join_type is JoinType.LEFT
    assert j.cardinality is JoinCardinality.MANY_TO_ONE


# ---------------------------------------------------------------------------
# Column.unique
# ---------------------------------------------------------------------------


def test_column_unique_defaults_false() -> None:
    c = Column(name="id", type=DataType.INT)
    assert c.unique is False


def test_column_accepts_unique() -> None:
    c = Column(name="email", type=DataType.TEXT, unique=True)
    assert c.unique is True


def test_column_unique_serializes() -> None:
    c = Column(name="email", type=DataType.TEXT, unique=True)
    assert c.model_dump()["unique"] is True
    assert Column.model_validate(c.model_dump()).unique is True


# ---------------------------------------------------------------------------
# Back-compat: old data without the new fields validates unchanged (no bump)
# ---------------------------------------------------------------------------


def test_old_join_without_cardinality_validates() -> None:
    j = ModelJoin.model_validate({"target_model": "c", "join_pairs": [["a", "b"]]})
    assert j.cardinality is None


def test_old_column_without_unique_validates() -> None:
    c = Column.model_validate({"name": "id", "type": "INT"})
    assert c.unique is False


def test_current_version_data_without_the_new_fields_needs_no_migration() -> None:
    """cardinality / unique are additive — they default in with no version change.

    Payload is stamped at the CURRENT version, so nothing migrates: if either
    field ever needed a migration step, the version would have to move and this
    assertion would fail. (Asserting a literal, or the default, against a
    migrated v7 payload proves nothing — both pass after any unrelated bump.)
    """
    current_version = SlayerModel.model_fields["version"].default
    m = SlayerModel.model_validate(
        {
            "version": current_version,
            "name": "orders",
            "sql_table": "orders",
            "data_source": "testds",
            "columns": [{"name": "customer_id", "type": "INT"}],
            "joins": [
                {"target_model": "customers", "join_pairs": [["customer_id", "id"]]}
            ],
        }
    )
    assert m.version == current_version
    assert m.joins[0].cardinality is None
    assert m.columns[0].unique is False


def test_pre_existing_v7_data_still_validates() -> None:
    """Older payloads migrate up and the new fields default in."""
    m = SlayerModel.model_validate(
        {
            "version": 7,
            "name": "orders",
            "sql_table": "orders",
            "data_source": "testds",
            "columns": [{"name": "customer_id", "type": "INT"}],
            "joins": [
                {"target_model": "customers", "join_pairs": [["customer_id", "id"]]}
            ],
        }
    )
    assert m.joins[0].cardinality is None
    assert m.columns[0].unique is False


# ---------------------------------------------------------------------------
# Storage round-trips (both backends persist the new fields)
# ---------------------------------------------------------------------------


def _model_with_cardinality() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="testds",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="email", type=DataType.TEXT, unique=True),
            Column(name="customer_id", type=DataType.INT),
        ],
        joins=[
            ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "id"]],
                cardinality=JoinCardinality.MANY_TO_ONE,
            )
        ],
    )


async def test_yaml_roundtrip_preserves_cardinality_and_unique() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        await storage.save_model(_model_with_cardinality())
        loaded = await storage.get_model("orders", data_source="testds")
        assert loaded is not None
        assert loaded.joins[0].cardinality is JoinCardinality.MANY_TO_ONE
        unique_col = next(c for c in loaded.columns if c.name == "email")
        assert unique_col.unique is True


async def test_sqlite_roundtrip_preserves_cardinality_and_unique() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        storage = SQLiteStorage(db_path=f"{tmp}/s.db")
        await storage.save_model(_model_with_cardinality())
        loaded = await storage.get_model("orders", data_source="testds")
        assert loaded is not None
        assert loaded.joins[0].cardinality is JoinCardinality.MANY_TO_ONE
        unique_col = next(c for c in loaded.columns if c.name == "email")
        assert unique_col.unique is True
