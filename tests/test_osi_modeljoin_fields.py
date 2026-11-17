"""ModelJoin gains optional ``description`` + ``meta`` (DEV-1643).

These carry OSI relationship ai_context. The fields are purely additive/optional
(no SlayerModel version bump), so old v7 join data validates unchanged, and both
storage backends persist the new fields.
"""

import tempfile


from slayer.core.enums import DataType, JoinType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


def _model_with_join() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="orders",
        data_source="testds",
        fields=[DatasetField(name="customer_id", datatype=DataType.INT)],
        relationships=[
            DatasetRelationship(
                to="customers",
                join_pairs=[["customer_id", "customer_id"]],
                join_type=JoinType.LEFT,
                description="Each order has one customer.",
                meta={"osi_ai_context": {"instructions": "Each order has one customer."}},
            )
        ],
    )


def test_modeljoin_has_optional_description_and_meta_defaulting_none() -> None:
    j = DatasetRelationship(to="c", join_pairs=[["a", "b"]])
    assert j.description is None
    assert j.meta is None


def test_modeljoin_accepts_description_and_meta() -> None:
    j = DatasetRelationship(
        to="c",
        join_pairs=[["a", "b"]],
        description="desc",
        meta={"k": "v"},
    )
    assert j.description == "desc"
    assert j.meta == {"k": "v"}


def test_old_v7_join_without_new_fields_validates() -> None:
    # Data written before the fields existed omits them entirely.
    j = DatasetRelationship.model_validate({"to": "c", "join_pairs": [["a", "b"]]})
    assert j.description is None and j.meta is None
    # And a whole v7 model whose join lacks the fields still loads.
    m = SlayerDataset.model_validate(
        {
            "version": 7,
            "name": "orders",
            "sql_table": "orders",
            "data_source": "testds",
            "fields": [{"name": "customer_id", "datatype": "INT"}],
            "relationships": [{"to": "customers", "join_pairs": [["customer_id", "customer_id"]]}],
        }
    )
    assert m.relationships[0].description is None and m.relationships[0].meta is None


async def test_yaml_roundtrip_preserves_join_fields() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        await storage.save_dataset(_model_with_join())
        loaded = await storage.get_dataset("orders", data_source="testds")
        assert loaded is not None
        assert loaded.relationships[0].description == "Each order has one customer."
        assert loaded.relationships[0].meta == {
            "osi_ai_context": {"instructions": "Each order has one customer."}
        }


async def test_sqlite_roundtrip_preserves_join_fields() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        storage = SQLiteStorage(db_path=f"{tmp}/s.db")
        await storage.save_dataset(_model_with_join())
        loaded = await storage.get_dataset("orders", data_source="testds")
        assert loaded is not None
        assert loaded.relationships[0].description == "Each order has one customer."
        assert loaded.relationships[0].meta["osi_ai_context"]["instructions"] == (
            "Each order has one customer."
        )
