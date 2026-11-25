"""Shared engines and accessors for the strict Ossie Layer 3 binding tests.

Fixtures are upstream's wide-table models (``retail``, ``stores``, ``stores_ambiguous``) over their SQL loaded into
in-process DuckDB, or ad-hoc documents / datasets over a SQL script. New names are reached through accessors, so a
name the implementation has not added yet fails only the tests that need it.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Sequence
from pathlib import Path
from typing import Any

import duckdb
import pytest

from slayer.core import errors
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2243_fixtures import make_storage, store_document
from tests._engine_helpers import owned_query_engine
from tests._ossie_gate import corpus_root

WIDE_TABLE = corpus_root() / "compliance" / "wide_table"
DS = "wide_table"
#: Fixture name → (SQL, model), per upstream's wide_table README.
FIXTURES = {
    "retail": ("retail/retail.sql", "retail/retail_model.ossie.yaml"),
    "stores": ("stores/stores.sql", "stores/stores_model.ossie.yaml"),
    "stores_ambiguous": ("stores/stores.sql", "stores/stores_ambiguous_model.ossie.yaml"),
}
STRICT = "ossie-l3"
NATIVE = "slayer"
#: Ossie §8, in precedence order.
CODES = (
    "E_INVALID_QUERY", "E_EMPTY_AGGREGATION_QUERY", "E_NAME_NOT_FOUND", "E_METRIC_IN_DIMENSIONS",
    "E_FIELD_IN_METRICS", "E_AGGREGATE_IN_WHERE", "E_WINDOW_IN_WHERE", "E_CROSS_DATASET_FILTER", "E_NAME_COLLISION",
    "E_UNAGGREGATED_FINER_GRAIN_REFERENCE", "E_DEFERRED_WINDOWED_METRIC", "E_AMBIGUOUS_MEASURE_GRAIN", "E_NO_PATH",
    "E_PARTIAL_DIMENSION_REACHABILITY", "E_NO_ANCHOR", "E_UNSUPPORTED_FILTER_PATH", "E_AMBIGUOUS_PATH",
)


def ossie_query_error() -> type[errors.OssieQueryError]:
    return errors.OssieQueryError


def load_sql(sql: str, database: Path) -> None:
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database))
    try:
        connection.execute(sql)
    finally:
        connection.close()


@contextlib.asynccontextmanager
async def sql_engine(base: Path, *, sql: str, documents: Sequence[str] = (), datasets: Sequence[SlayerDataset] = (),
                     data_source: str = DS) -> AsyncGenerator[SlayerQueryEngine]:
    """An executing DuckDB engine over ``sql`` with ``documents`` and standalone ``datasets`` stored."""
    database = base / f"{data_source}.duckdb"
    load_sql(sql, database)
    storage = make_storage("yaml", base)
    async with owned_query_engine(storage=storage,
                                  datasources=[DatasourceConfig(name=data_source, type="duckdb",
                                                                database=str(database))]) as engine:
        for text in documents:
            await store_document(storage, text, data_source=data_source)
        for dataset in datasets:
            await storage.save_dataset(dataset)
        yield engine


def fixture_texts(name: str) -> tuple[str, str]:
    """The fixture's SQL and model text."""
    sql, model = FIXTURES[name]
    return (WIDE_TABLE / sql).read_text(encoding="utf-8"), (WIDE_TABLE / model).read_text(encoding="utf-8")


@contextlib.asynccontextmanager
async def fixture_engine(base: Path, name: str = "retail", *, extra_documents: Sequence[str] = (),
                         datasets: Sequence[SlayerDataset] = ()) -> AsyncGenerator[SlayerQueryEngine]:
    """An executing engine over upstream wide-table fixture ``name``."""
    sql, model = fixture_texts(name)
    async with sql_engine(base, sql=sql, documents=(model, *extra_documents), datasets=datasets) as engine:
        yield engine


async def strict_error(engine: SlayerQueryEngine, query: Any, code: str, **kwargs: Any) -> Any:
    """Run ``query`` under the strict binding; assert it raises ``OssieQueryError`` with ``code``."""
    with pytest.raises(ossie_query_error()) as raised:
        await engine.execute(query, conformance=STRICT, **kwargs)
    assert raised.value.code == code, raised.value
    return raised.value


async def strict(engine: SlayerQueryEngine, query: Any, **kwargs: Any) -> Any:
    return await engine.execute(query, conformance=STRICT, **kwargs)


async def run_as(engine: SlayerQueryEngine, query: Any, conformance: str, **kwargs: Any) -> Any:
    """Run ``query`` under ``conformance``; the native one is the default, so it passes no argument."""
    if conformance == NATIVE:
        return await engine.execute(query, **kwargs)
    return await engine.execute(query, conformance=conformance, **kwargs)


def rows(response: Any, *keys: str) -> list[tuple[Any, ...]]:
    """The response's rows as tuples over ``keys`` (all columns when none), numbers as floats, sorted."""
    names = keys or tuple(response.columns)
    return sorted((tuple(_plain(row[key]) for key in names) for row in response.data), key=repr)


def _plain(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    try:
        return float(value)
    except (TypeError, ValueError):
        return value
