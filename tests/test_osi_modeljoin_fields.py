"""DatasetRelationship gains optional ``description`` + ``meta``.

These carry OSI relationship ai_context. The fields are purely additive/optional
(no SlayerModel version bump), so old v7 join data validates unchanged, and both
storage backends persist the new fields.
"""

import tempfile


from slayer.core.enums import DataType, JoinType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


def _dataset_with_relationship() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="orders",
        data_source="testds",
        fields=[DatasetField(name="customer_id", datatype=DataType.INTEGER)],
        relationships=[
            DatasetRelationship(
                to="customers",
                from_columns=["customer_id"], to_columns=["customer_id"],
                join_type=JoinType.LEFT,
                description="Each order has one customer.",
                meta={"osi_ai_context": {"instructions": "Each order has one customer."}},
            )
        ],
    )


def test_modeljoin_has_optional_description_and_meta_defaulting_none() -> None:
    j = DatasetRelationship(to="c", from_columns=["a"], to_columns=["b"])
    assert j.description is None
    assert j.meta is None


def test_modeljoin_accepts_description_and_meta() -> None:
    j = DatasetRelationship(
        to="c",
        from_columns=["a"], to_columns=["b"],
        description="desc",
        meta={"k": "v"},
    )
    assert j.description == "desc"
    assert j.meta == {"k": "v"}


def test_old_v7_join_without_new_fields_validates() -> None:
    # Data written before the fields existed omits them entirely.
    j = DatasetRelationship.model_validate({"to": "c", "from_columns": ["a"], "to_columns": ["b"]})
    assert j.description is None
    assert j.meta is None
    # And a whole v7 model whose join lacks the fields still loads.
    m = SlayerDataset.model_validate(
        {
            "version": 7,
            "name": "orders",
            "sql_table": "orders",
            "data_source": "testds",
            "columns": [{"name": "customer_id", "type": "INT"}],
            "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "customer_id"]]}],
        }
    )
    assert m.relationships[0].description is None
    assert m.relationships[0].meta is None


async def test_yaml_roundtrip_preserves_join_fields() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        await storage.save_dataset(_dataset_with_relationship())
        loaded = await storage.get_dataset("orders", data_source="testds")
        assert loaded is not None
        assert loaded.relationships[0].description == "Each order has one customer."
        assert loaded.relationships[0].meta == {
            "osi_ai_context": {"instructions": "Each order has one customer."}
        }


async def test_sqlite_roundtrip_preserves_join_fields() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        storage = SQLiteStorage(db_path=f"{tmp}/s.db")
        await storage.save_dataset(_dataset_with_relationship())
        loaded = await storage.get_dataset("orders", data_source="testds")
        assert loaded is not None
        assert loaded.relationships[0].description == "Each order has one customer."
        assert loaded.relationships[0].meta["osi_ai_context"]["instructions"] == (
            "Each order has one customer."
        )
