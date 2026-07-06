"""ModelJoin gains optional ``description`` + ``meta`` (DEV-1643).

These carry OSI relationship ai_context. The fields are purely additive/optional
(no SlayerModel version bump), so old v7 join data validates unchanged, and both
storage backends persist the new fields.
"""

import tempfile


from slayer.core.enums import DataType, JoinType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


def _model_with_join() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="testds",
        columns=[Column(name="customer_id", type=DataType.INT)],
        joins=[
            ModelJoin(
                target_model="customers",
                join_pairs=[["customer_id", "customer_id"]],
                join_type=JoinType.LEFT,
                description="Each order has one customer.",
                meta={"osi_ai_context": {"instructions": "Each order has one customer."}},
            )
        ],
    )


def test_modeljoin_has_optional_description_and_meta_defaulting_none() -> None:
    j = ModelJoin(target_model="c", join_pairs=[["a", "b"]])
    assert j.description is None
    assert j.meta is None


def test_modeljoin_accepts_description_and_meta() -> None:
    j = ModelJoin(
        target_model="c",
        join_pairs=[["a", "b"]],
        description="desc",
        meta={"k": "v"},
    )
    assert j.description == "desc"
    assert j.meta == {"k": "v"}


def test_old_v7_join_without_new_fields_validates() -> None:
    # Data written before the fields existed omits them entirely.
    j = ModelJoin.model_validate({"target_model": "c", "join_pairs": [["a", "b"]]})
    assert j.description is None and j.meta is None
    # And a whole v7 model whose join lacks the fields still loads.
    m = SlayerModel.model_validate(
        {
            "version": 7,
            "name": "orders",
            "sql_table": "orders",
            "data_source": "testds",
            "columns": [{"name": "customer_id", "type": "INT"}],
            "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "customer_id"]]}],
        }
    )
    assert m.joins[0].description is None and m.joins[0].meta is None


async def test_yaml_roundtrip_preserves_join_fields() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        storage = YAMLStorage(base_dir=tmp)
        await storage.save_model(_model_with_join())
        loaded = await storage.get_model("orders", data_source="testds")
        assert loaded is not None
        assert loaded.joins[0].description == "Each order has one customer."
        assert loaded.joins[0].meta == {
            "osi_ai_context": {"instructions": "Each order has one customer."}
        }


async def test_sqlite_roundtrip_preserves_join_fields() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        storage = SQLiteStorage(db_path=f"{tmp}/s.db")
        await storage.save_model(_model_with_join())
        loaded = await storage.get_model("orders", data_source="testds")
        assert loaded is not None
        assert loaded.joins[0].description == "Each order has one customer."
        assert loaded.joins[0].meta["osi_ai_context"]["instructions"] == (
            "Each order has one customer."
        )
