"""The mirrored reverse INNER join must carry the inverted cardinality."""

import tempfile

import pytest_asyncio

from slayer.core.enums import DataType, JoinCardinality, JoinType
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.storage.join_sync import _mirror_inner_joins
from slayer.storage.yaml_storage import YAMLStorage


def _model(name: str, *, joins: list[ModelJoin] | None = None) -> SlayerModel:
    return SlayerModel(
        name=name,
        sql_table=name,
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="fk_id", sql="fk_id", type=DataType.DOUBLE),
        ],
        joins=joins or [],
    )


def _inner_join(target: str, cardinality: JoinCardinality | None) -> ModelJoin:
    return ModelJoin(
        target_model=target,
        join_pairs=[["fk_id", "id"]],
        join_type=JoinType.INNER,
        cardinality=cardinality,
    )


@pytest_asyncio.fixture
async def raw_storage():
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(
            DatasourceConfig(name="test", type="sqlite", database=":memory:")
        )
        yield storage


async def _reverse_join(storage, source_name: str, from_model: str):
    m = await storage.get_model(from_model)
    return next((j for j in m.joins if j.target_model == source_name), None)


class TestMirrorInvertsCardinality:
    async def test_many_to_one_reverses_to_one_to_many(self, raw_storage) -> None:
        a = _model("a", joins=[_inner_join("b", JoinCardinality.MANY_TO_ONE)])
        b = _model("b")
        await raw_storage.save_model(a)
        await raw_storage.save_model(b)

        await _mirror_inner_joins(a, raw_storage)

        rev = await _reverse_join(raw_storage, "a", "b")
        assert rev is not None
        assert rev.cardinality is JoinCardinality.ONE_TO_MANY

    async def test_one_to_one_self_inverse(self, raw_storage) -> None:
        a = _model("a", joins=[_inner_join("b", JoinCardinality.ONE_TO_ONE)])
        b = _model("b")
        await raw_storage.save_model(a)
        await raw_storage.save_model(b)

        await _mirror_inner_joins(a, raw_storage)

        rev = await _reverse_join(raw_storage, "a", "b")
        assert rev.cardinality is JoinCardinality.ONE_TO_ONE

    async def test_many_to_many_self_inverse(self, raw_storage) -> None:
        a = _model("a", joins=[_inner_join("b", JoinCardinality.MANY_TO_MANY)])
        b = _model("b")
        await raw_storage.save_model(a)
        await raw_storage.save_model(b)

        await _mirror_inner_joins(a, raw_storage)

        rev = await _reverse_join(raw_storage, "a", "b")
        assert rev.cardinality is JoinCardinality.MANY_TO_MANY

    async def test_none_cardinality_reverse_stays_none(self, raw_storage) -> None:
        a = _model("a", joins=[_inner_join("b", None)])
        b = _model("b")
        await raw_storage.save_model(a)
        await raw_storage.save_model(b)

        await _mirror_inner_joins(a, raw_storage)

        rev = await _reverse_join(raw_storage, "a", "b")
        assert rev is not None
        assert rev.cardinality is None

    async def test_existing_reverse_reconciled_when_only_cardinality_changes(
        self, raw_storage
    ) -> None:
        # b already has a reverse join to a, but with a stale/absent cardinality.
        a = _model("a", joins=[_inner_join("b", JoinCardinality.MANY_TO_ONE)])
        b = _model(
            "b",
            joins=[
                ModelJoin(
                    target_model="a",
                    join_pairs=[["id", "fk_id"]],
                    join_type=JoinType.INNER,
                    cardinality=None,
                )
            ],
        )
        await raw_storage.save_model(a)
        await raw_storage.save_model(b)

        await _mirror_inner_joins(a, raw_storage)

        rev = await _reverse_join(raw_storage, "a", "b")
        # Reverse of many_to_one is one_to_many — reconciled even though the
        # join_pairs were already correct.
        assert rev.cardinality is JoinCardinality.ONE_TO_MANY
        # Not duplicated.
        b_reloaded = await raw_storage.get_model("b")
        assert sum(1 for j in b_reloaded.joins if j.target_model == "a") == 1
