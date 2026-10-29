"""Mode B text is parsed only by ``core.syntax``; its consumers read the parse tree (principle 18)."""

from __future__ import annotations

import ast
from collections import Counter
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.memories.resolver import extract_entities_from_query
from slayer.storage.yaml_storage import YAMLStorage
from slayer.usage_reporting.features import query_features

SLAYER = Path(__file__).resolve().parents[1] / "slayer"
#: Rewrite superseded stored spellings the current grammar rejects (principle 11), so they cannot use it.
TOKENIZE_ALLOWED = {
    "storage/sql_string_migration.py",
    "storage/rank_direction_migration.py",
}


def _imports_tokenize(path: Path) -> bool:
    tree = ast.parse(path.read_text(), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import) and any(a.name.split(".")[0] == "tokenize" for a in node.names):
            return True
        if isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] == "tokenize":
            return True
    return False


def test_no_tokenize_outside_the_stored_version_migrations() -> None:
    offenders = sorted(
        rel for path in SLAYER.rglob("*.py")
        if (rel := path.relative_to(SLAYER).as_posix()) not in TOKENIZE_ALLOWED and _imports_tokenize(path)
    )
    assert offenders == []


def test_the_allowlist_names_real_tokenize_readers() -> None:
    assert all(_imports_tokenize(SLAYER / rel) for rel in TOKENIZE_ALLOWED)


# --------------------------------------------------------------------------- #
# Usage reporting reads the parse tree
# --------------------------------------------------------------------------- #
def _features(*measures: str) -> tuple[Counter[str], Counter[str]]:
    found = query_features({"source_model": "m", "measures": list(measures)})
    return Counter(found.aggregations), Counter(found.flags)


@pytest.mark.parametrize(("formula", "aggregation", "flag"), [
    ("sum(x)::int", "sum", "cast"),
    ("CAST(avg(y) AS DOUBLE)", "avg", "cast"),
    ("TRY_CAST(sum(y) AS INT)", "sum", "try_cast"),
    ("count(*) FILTER (WHERE z > 0)", "count", "aggregate_filter"),
])
def test_grammar_forms_count_no_custom(formula: str, aggregation: str, flag: str) -> None:
    aggregations, flags = _features(formula)
    assert aggregations == Counter({aggregation: 1})
    assert flags[flag] == 1


def test_count_distinct_counts_as_count_distinct() -> None:
    assert _features("count(DISTINCT x)")[0] == Counter({"count_distinct": 1})


def test_array_length_stays_custom() -> None:
    found = query_features({"source_model": "m", "measures": ["array_length(tags)"]})
    assert Counter(found.aggregations) == Counter({"custom": 1})
    assert found.functions == []


# --------------------------------------------------------------------------- #
# Memory tagging reads the parse tree
# --------------------------------------------------------------------------- #
@pytest.fixture
async def storage(tmp_path: Path) -> AsyncIterator[YAMLStorage]:
    store = YAMLStorage(base_dir=str(tmp_path))
    await store.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="x"))
    await store.save_model(SlayerModel(
        name="orders", data_source="mydb", sql_table="orders",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="region", type=DataType.TEXT), Column(name="amount", type=DataType.DOUBLE)],
    ))
    yield store


async def test_quoted_identifier_in_a_filter_is_tagged(storage: YAMLStorage) -> None:
    query = SlayerQuery(source_model="orders", filters=['"region" = \'x\''], measures=[ModelMeasure(formula="*:count")])
    result = await extract_entities_from_query(query, storage=storage)
    assert "mydb.orders.region" in result.canonical_forms
