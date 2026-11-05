"""Ossie export: joins become FK-direction relationships keyed by field names."""

from pathlib import Path

import pytest

from slayer.core.enums import JoinCardinality, JoinType
from slayer.core.models import ModelJoin
from tests._osi_export_fixtures import BASIC_MEASURES, customers, orders, to_customers, visits
from tests._osi_export_helpers import export_models, make_env, mentions, relationships, validate


@pytest.fixture
async def env(tmp_path: Path):
    return await make_env(tmp_path)


def _shape(rel: dict) -> tuple:
    return rel["from"], rel["to"], rel["from_columns"], rel["to_columns"]


def _to_orders(cardinality: JoinCardinality | None) -> ModelJoin:
    return ModelJoin(target_model="orders", join_pairs=[["id", "customer_id"]], cardinality=cardinality)


_FK = ("orders", "customers", ["customer_id"], ["id"])


async def test_many_to_one_join(env) -> None:
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES))
    assert [_shape(r) for r in relationships(result.document)] == [_FK]


async def test_one_to_one_join(env) -> None:
    model = orders(measures=BASIC_MEASURES, joins=[to_customers(cardinality=JoinCardinality.ONE_TO_ONE)])
    result = await export_models(env, customers(), model)
    assert [_shape(r) for r in relationships(result.document)] == [_FK]


async def test_one_to_many_join_is_reversed(env) -> None:
    result = await export_models(env, customers(joins=[_to_orders(JoinCardinality.ONE_TO_MANY)]),
                                 orders(measures=BASIC_MEASURES, joins=[]))
    assert [_shape(r) for r in relationships(result.document)] == [_FK]


async def test_mirrored_joins_merge_into_one_relationship(env) -> None:
    result = await export_models(env, customers(joins=[_to_orders(JoinCardinality.ONE_TO_MANY)]),
                                 orders(measures=BASIC_MEASURES))
    assert [_shape(r) for r in relationships(result.document)] == [_FK]


async def test_undetermined_cardinality_proven_by_target_key(env) -> None:
    model = orders(measures=BASIC_MEASURES, joins=[to_customers(cardinality=None)])
    result = await export_models(env, customers(), model)
    assert [_shape(r) for r in relationships(result.document)] == [_FK]
    assert not result.entries


async def test_undetermined_cardinality_proven_by_source_key(env) -> None:
    result = await export_models(env, customers(joins=[_to_orders(None)]), orders(measures=BASIC_MEASURES, joins=[]))
    assert [_shape(r) for r in relationships(result.document)] == [_FK]


async def test_undetermined_cardinality_not_provable_is_reported(env) -> None:
    unkeyed = ModelJoin(target_model="orders", join_pairs=[["status", "status"]])
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES), visits(joins=[unkeyed]))
    assert [_shape(r) for r in relationships(result.document)] == [_FK]
    [entry] = mentions(result, "visits", severity="dropped")
    assert entry.suggestion is not None
    assert "cardinality" in entry.suggestion
    validate(result.document)


async def test_many_to_many_join_is_reported(env) -> None:
    m2m = ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                    cardinality=JoinCardinality.MANY_TO_MANY)
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES, joins=[]), visits(joins=[m2m]))
    assert relationships(result.document) == []
    [entry] = mentions(result, "visits", severity="dropped")
    assert entry.suggestion


async def test_relationship_named_after_its_join(env) -> None:
    model = orders(measures=BASIC_MEASURES, joins=[to_customers(name="buyer")])
    [rel] = relationships((await export_models(env, customers(), model)).document)
    assert rel["name"] == "buyer"


async def test_generated_relationship_names_are_unique(env) -> None:
    to_cust = ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                        cardinality=JoinCardinality.MANY_TO_ONE)
    result = await export_models(env, customers(), orders(measures=BASIC_MEASURES), visits(joins=[to_cust]))
    names = [r["name"] for r in relationships(result.document)]
    assert len(names) == 2
    assert len(set(names)) == 2
    validate(result.document)


async def test_inner_join_is_exported(env) -> None:
    model = orders(measures=BASIC_MEASURES, joins=[to_customers(join_type=JoinType.INNER)])
    assert [_shape(r) for r in relationships((await export_models(env, customers(), model)).document)] == [_FK]
