"""Spec: compatibility/ossie-compliance › the gate on the in-process Tier-1 engines (SQLite, DuckDB).

The server engines run the same checks in ``tests/integration/test_integration_ossie_gate.py``.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
import pytest_asyncio

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2446_required_manifest import ROWS, Row
from tests._ossie_gate import IN_PROCESS, declaration_problems, mark_cell
from tests._ossie_gate_fixtures import FileProvider, Table, file_provider, layer1_engine, mixed_case_engine
from tests._ossie_gate_fixtures import wide_table_engines as open_wide_table_engines
from tests._ossie_gate_suite import (
    CASE_STATES, MIXED_CASE_QUERY, MIXED_CASE_ROWS, ROOTED_QUERY, ROOTED_ROWS, case_state, cases,
    check_case_under_strict, check_layer1, check_row_case_under_the_default, check_rows, check_seeded, row_cases,
    seeded_tables,
)

#: Engines live as long as the module's seeded database, on one loop.
MODULE_LOOP = pytest.mark.asyncio(loop_scope="module")


@pytest.fixture(scope="module", params=IN_PROCESS)
def provider(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> FileProvider:
    return file_provider(request.param, tmp_path_factory.mktemp(f"gate_{request.param}"))


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def layer1(provider: FileProvider, tmp_path_factory: pytest.TempPathFactory) -> AsyncGenerator[SlayerQueryEngine]:
    async with layer1_engine(provider, tmp_path_factory.mktemp("layer1")) as engine:
        yield engine


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def wide_table(provider: FileProvider,
                     tmp_path_factory: pytest.TempPathFactory) -> AsyncGenerator[dict[str, SlayerQueryEngine]]:
    async with open_wide_table_engines(provider, tmp_path_factory.mktemp("wide_table")) as engines:
        yield engines


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def mixed_case(provider: FileProvider,
                     tmp_path_factory: pytest.TempPathFactory) -> AsyncGenerator[SlayerQueryEngine]:
    async with mixed_case_engine(provider, tmp_path_factory.mktemp("mixed_case")) as engine:
        yield engine


@pytest.mark.parametrize("table", seeded_tables(), ids=lambda t: f"{t.schema_name or ''}.{t.name}")
def test_every_fixture_table_reads_back(provider: FileProvider, table: Table) -> None:
    check_seeded(provider, table, engine=provider.engine)


@MODULE_LOOP
@pytest.mark.parametrize("row", ROWS, ids=lambda r: r.id)
async def test_layer1_construct(request: pytest.FixtureRequest, provider: FileProvider, layer1: SlayerQueryEngine,
                                row: Row) -> None:
    mark_cell(request, row.state(provider.engine))
    await check_layer1(layer1, row, engine_name=provider.engine)


@MODULE_LOOP
@pytest.mark.parametrize("case", cases(), ids=lambda c: c.name)
async def test_wide_table_case_under_strict(request: pytest.FixtureRequest, provider: FileProvider,
                                            wide_table: dict[str, SlayerQueryEngine], case: Path) -> None:
    mark_cell(request, case_state(case.name, provider.engine))
    await check_case_under_strict(wide_table, case, engine_name=provider.engine)


@MODULE_LOOP
@pytest.mark.parametrize("case", row_cases(), ids=lambda c: c.name)
async def test_wide_table_row_case_under_the_default(request: pytest.FixtureRequest, provider: FileProvider,
                                                     wide_table: dict[str, SlayerQueryEngine], case: Path) -> None:
    mark_cell(request, case_state(case.name, provider.engine))
    await check_row_case_under_the_default(wide_table, case, engine_name=provider.engine)


@MODULE_LOOP
async def test_a_mixed_case_column_read_through_a_relationship(provider: FileProvider,
                                                               mixed_case: SlayerQueryEngine) -> None:
    """As a dimension, in a filter and as a metric's input."""
    await check_rows(mixed_case, MIXED_CASE_QUERY, MIXED_CASE_ROWS, engine_name=provider.engine)


@MODULE_LOOP
async def test_a_mixed_case_column_on_its_own_root(provider: FileProvider, mixed_case: SlayerQueryEngine) -> None:
    await check_rows(mixed_case, ROOTED_QUERY, ROOTED_ROWS, engine_name=provider.engine)


def test_every_wide_table_case_is_declared() -> None:
    assert declaration_problems(CASE_STATES, [c.name for c in cases()]) == []
