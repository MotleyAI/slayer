"""Ossie export: joins become FK-direction relationships keyed by field names."""

from pathlib import Path

import pytest

from slayer.core.enums import RelationshipCardinality, JoinType
from slayer.core.models import DatasetRelationship
from tests._osi_export_fixtures import BASIC_MEASURES, customers, orders, to_customers, visits
from tests._osi_export_helpers import export_datasets, make_env, mentions, relationships, validate


@pytest.fixture
async def env(tmp_path: Path):
    return await make_env(tmp_path)


def _shape(rel: dict) -> tuple:
    return rel["from"], rel["to"], rel["from_columns"], rel["to_columns"]


def _to_orders(cardinality: RelationshipCardinality | None) -> DatasetRelationship:
    return DatasetRelationship(to="orders", from_columns=["id"], to_columns=["customer_id"], cardinality=cardinality)


_FK = ("orders", "customers", ["customer_id"], ["id"])


async def test_many_to_one_join(env) -> None:
    result = await export_datasets(env, customers(), orders(metrics=BASIC_MEASURES))
    assert [_shape(r) for r in relationships(result.document)] == [_FK]


async def test_one_to_one_join(env) -> None:
    model = orders(metrics=BASIC_MEASURES, joins=[to_customers(cardinality=RelationshipCardinality.ONE_TO_ONE)])
    result = await export_datasets(env, customers(), model)
    assert [_shape(r) for r in relationships(result.document)] == [_FK]


async def test_one_to_many_join_is_reversed(env) -> None:
    result = await export_datasets(env, customers(joins=[_to_orders(RelationshipCardinality.ONE_TO_MANY)]),
                                 orders(metrics=BASIC_MEASURES, joins=[]))
    assert [_shape(r) for r in relationships(result.document)] == [_FK]


async def test_mirrored_joins_merge_into_one_relationship(env) -> None:
    result = await export_datasets(
        env,
        customers(joins=[_to_orders(RelationshipCardinality.ONE_TO_MANY).model_copy(update={"name": "placed_orders"})]),
        orders(metrics=BASIC_MEASURES))
    assert [_shape(r) for r in relationships(result.document)] == [_FK]


async def test_undetermined_cardinality_proven_by_target_key(env) -> None:
    model = orders(metrics=BASIC_MEASURES, joins=[to_customers(cardinality=None)])
    result = await export_datasets(env, customers(), model)
    assert [_shape(r) for r in relationships(result.document)] == [_FK]
    assert not result.entries


async def test_undetermined_cardinality_proven_by_source_key(env) -> None:
    result = await export_datasets(env, customers(joins=[_to_orders(None)]), orders(metrics=BASIC_MEASURES, joins=[]))
    assert [_shape(r) for r in relationships(result.document)] == [_FK]


async def test_undetermined_cardinality_not_provable_is_reported(env) -> None:
    unkeyed = DatasetRelationship(to="orders", from_columns=["status"], to_columns=["status"])
    result = await export_datasets(env, customers(), orders(metrics=BASIC_MEASURES), visits(relationships=[unkeyed]))
    assert [_shape(r) for r in relationships(result.document)] == [_FK]
    [entry] = mentions(result, "visits", severity="dropped")
    assert entry.suggestion is not None
    assert "cardinality" in entry.suggestion
    validate(result.document)


async def test_many_to_many_join_is_reported(env) -> None:
    m2m = DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"],
                    cardinality=RelationshipCardinality.MANY_TO_MANY)
    result = await export_datasets(env, customers(), orders(metrics=BASIC_MEASURES, joins=[]), visits(relationships=[m2m]))
    assert relationships(result.document) == []
    [entry] = mentions(result, "visits", severity="dropped")
    assert entry.suggestion


async def test_relationship_named_after_its_join(env) -> None:
    model = orders(metrics=BASIC_MEASURES, joins=[to_customers(name="buyer")])
    [rel] = relationships((await export_datasets(env, customers(), model)).document)
    assert rel["name"] == "buyer"


async def test_generated_relationship_names_are_unique(env) -> None:
    to_cust = DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"],
                        cardinality=RelationshipCardinality.MANY_TO_ONE)
    result = await export_datasets(env, customers(), orders(metrics=BASIC_MEASURES), visits(relationships=[to_cust]))
    names = [r["name"] for r in relationships(result.document)]
    assert len(names) == 2
    assert len(set(names)) == 2
    validate(result.document)


async def test_inner_join_is_exported(env) -> None:
    model = orders(metrics=BASIC_MEASURES, joins=[to_customers(join_type=JoinType.INNER)])
    assert [_shape(r) for r in relationships((await export_datasets(env, customers(), model)).document)] == [_FK]
