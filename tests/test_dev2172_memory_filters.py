"""Memory entity tagging reads query filters through the parse tree (principle 18)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.memories.resolver import _field_entity_tokens, extract_entities_from_query
from slayer.storage.yaml_storage import YAMLStorage


@pytest.mark.parametrize(("text", "tokens"), [
    ("status = 'paid'", {"status"}),
    ("status = {s}", {"status"}),
    ("status = '{s}'", {"status"}),
    ("status = 'it''s'", {"status"}),
    ('"status" = \'x\'', {"status"}),
    ("payload ->> 'k' = 'v'", {"payload"}),
    ("lower(status) = 'x' AND amount BETWEEN 1 AND 2", {"status", "amount"}),
    ("CAST(amount AS INT) > 1", {"amount"}),
    ("TRY_CAST(code AS DOUBLE) > 1", {"code"}),
    ("status IS NOT NULL", {"status"}),
    ("status IN ('a', 'b')", {"status"}),
    ("customers.name = 'x'", {"customers.name"}),
    ("skills.value = 'go'", {"skills.value"}),
    ("contains_any(skills, ['go'])", {"skills"}),
    ("amount:sum > 100", {"amount:sum"}),
])
def test_filter_tokens(text: str, tokens: set[str]) -> None:
    assert set(_field_entity_tokens("filter", text)) == tokens


def test_aggregate_filter_tokens_carry_no_keywords() -> None:
    tokens = _field_entity_tokens("filter", "count(*) FILTER (WHERE amount > 0) > 1")
    assert "*:count" in tokens
    assert not {t.upper() for t in tokens} & {"FILTER", "WHERE", "COUNT"}


@pytest.mark.parametrize("text", ["status = = 'x'", "lower(", "status = 'unterminated"])
def test_unparseable_filter_is_one_token(text: str) -> None:
    assert _field_entity_tokens("filter", text) == [text]


@pytest.fixture
async def storage(tmp_path: Path) -> AsyncIterator[YAMLStorage]:
    store = YAMLStorage(base_dir=str(tmp_path))
    await store.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="x"))
    await store.save_model(SlayerModel(
        name="orders", data_source="mydb", sql_table="orders",
        columns=[Column(name="id", type=DataType.INT, primary_key=True), Column(name="status", type=DataType.TEXT),
                 Column(name="amount", type=DataType.DOUBLE)],
    ))
    yield store


async def test_cast_filter_tags_only_the_column(storage: YAMLStorage) -> None:
    query = SlayerQuery(source_model="orders", filters=["CAST(amount AS INT) > 1"],
                        measures=[ModelMeasure(formula="*:count")])
    result = await extract_entities_from_query(query, storage=storage)
    assert result.canonical_forms == ["mydb.orders", "mydb.orders.amount"]
