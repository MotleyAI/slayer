"""Spec: compatibility/ossie-compliance › every wide-table case and the mixed-case query compile on every Tier-1 dialect.

A dry run per dialect, scope-validated (``conftest``): a qualifier the database would fold differently from the relation
it names fails here, without a server.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
import pytest_asyncio
import sqlglot
from sqlglot import exp

from slayer.core.errors import OssieQueryError
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import get_dialect
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2217_fixtures import FIXTURES, STRICT, fixture_texts
from tests._dev2243_fixtures import store_document
from tests._ossie_gate import TIER1
from tests._ossie_gate_fixtures import MIXED_CASE_DS, WIDE_TABLE_DS, Placement, mixed_case_datasets, placed_model
from tests._ossie_gate_suite import MIXED_CASE_QUERY, ROOTED_QUERY, case_query, cases, read_expected

#: The datasource type each Tier-1 engine is configured as.
DS_TYPE = {"tsql": "mssql"}


async def _engine(engine: str, base: Path) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=str(base))
    for name in (WIDE_TABLE_DS, MIXED_CASE_DS):
        await storage.save_datasource(DatasourceConfig(name=name, type=DS_TYPE.get(engine, engine), host="h",
                                                       database="d"))
    for dataset in mixed_case_datasets():
        await storage.save_dataset(dataset, _validate=False)
    return SlayerQueryEngine(storage=storage)


@pytest_asyncio.fixture
async def wide_table(tmp_path: Path) -> AsyncGenerator[dict[str, dict[str, SlayerQueryEngine]]]:
    """Engine → fixture → a dry-run engine over that fixture's model."""
    out: dict[str, dict[str, SlayerQueryEngine]] = {}
    for engine in TIER1:
        out[engine] = {}
        for fixture in FIXTURES:
            base = tmp_path / engine / fixture
            storage = YAMLStorage(base_dir=str(base))
            await storage.save_datasource(DatasourceConfig(name=WIDE_TABLE_DS, type=DS_TYPE.get(engine, engine),
                                                           host="h", database="d"))
            await store_document(storage, placed_model(fixture_texts(fixture)[1], Placement(engine=engine, token="t")),
                                 data_source=WIDE_TABLE_DS)
            out[engine][fixture] = SlayerQueryEngine(storage=storage)
    yield out


async def _outcome(engine: SlayerQueryEngine, case: Path) -> str:
    try:
        await engine.execute(case_query(case), conformance=STRICT, dry_run=True)
    except OssieQueryError as error:
        return error.code
    return "sql"


@pytest.mark.parametrize("case", cases(), ids=lambda c: c.name)
async def test_every_wide_table_case_compiles_alike_on_every_dialect(
        wide_table: dict[str, dict[str, SlayerQueryEngine]], case: Path) -> None:
    fixture = read_expected(case).fixture
    outcomes = {engine: await _outcome(wide_table[engine][fixture], case) for engine in TIER1}
    assert set(outcomes.values()) == {read_expected(case).error or "sql"}, outcomes


def _column_references(sql: str, *, engine: str, name: str) -> list[exp.Column]:
    return [c for c in sqlglot.parse_one(sql, dialect=get_dialect(engine).sqlglot_name).find_all(exp.Column)
            if c.name == name]


@pytest.mark.parametrize("query", [MIXED_CASE_QUERY, ROOTED_QUERY], ids=["through a relationship", "on its root"])
@pytest.mark.parametrize("engine", TIER1)
async def test_a_mixed_case_column_is_quoted_wherever_it_is_read(tmp_path: Path, engine: str, query: dict) -> None:
    """As a dimension, in a filter and as a metric's input, the live column ``"externalId"`` is read quoted."""
    sql = (await (await _engine(engine, tmp_path)).execute(query, dry_run=True)).sql
    assert sql is not None
    references = _column_references(sql, engine=engine, name="externalId")
    assert references, sql
    assert all(c.this.quoted for c in references), sql
