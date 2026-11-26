"""Spec: compatibility/ossie-compliance › the gate on the server Tier-1 engines.

Postgres, MySQL, ClickHouse, SQL Server, Trino, Snowflake and BigQuery; ``$SLAYER_GATE_ENGINES`` selects some and
requires them. The in-process engines run the same checks in ``tests/test_ossie_gate.py``.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator

import pytest
import pytest_asyncio
from pytest_postgresql import factories

from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2446_required_manifest import NO_REGEX, ROWS, Row
from tests._ossie_gate import mark_cell, selected_servers
from tests._ossie_gate_fixtures import Table, layer1_engine, mixed_case_engine
from tests._ossie_gate_fixtures import wide_table_engines as open_wide_table_engines
from tests._ossie_gate_suite import (
    MIXED_CASE_QUERY, MIXED_CASE_ROWS, ROOTED_QUERY, ROOTED_ROWS, case_state, cases, check_case_under_strict,
    check_layer1, check_layer1_value, check_row_case_under_the_default, check_rows, check_seeded, row_cases,
    seeded_tables, WideCase,
)
from tests.integration._ossie_gate_servers import SQLSERVER_2025, ServerProvider, server_provider, sqlserver_provider

#: The rows SQL Server refuses before 2025 for want of regular expressions.
REGEX_ROWS = [r for r in ROWS if r.state(NO_REGEX).state == "refused" and r.admitted]

#: Engines live as long as the module's seeded database, on one loop.
pytestmark = pytest.mark.integration
MODULE_LOOP = pytest.mark.asyncio(loop_scope="module")

postgresql_proc = factories.postgresql_proc(port=None)


@pytest.fixture(scope="module", params=selected_servers())
def provider(request: pytest.FixtureRequest) -> Generator[ServerProvider]:
    with server_provider(request.param, request) as seeded:
        yield seeded


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def layer1(provider: ServerProvider,
                 tmp_path_factory: pytest.TempPathFactory) -> AsyncGenerator[SlayerQueryEngine]:
    async with layer1_engine(provider, tmp_path_factory.mktemp("layer1")) as engine:
        yield engine


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def wide_table(provider: ServerProvider,
                     tmp_path_factory: pytest.TempPathFactory) -> AsyncGenerator[dict[str, SlayerQueryEngine]]:
    async with open_wide_table_engines(provider, tmp_path_factory.mktemp("wide_table")) as engines:
        yield engines


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def mixed_case(provider: ServerProvider,
                     tmp_path_factory: pytest.TempPathFactory) -> AsyncGenerator[SlayerQueryEngine]:
    async with mixed_case_engine(provider, tmp_path_factory.mktemp("mixed_case")) as engine:
        yield engine


@pytest.fixture(scope="module")
def sqlserver_2025() -> Generator[ServerProvider]:
    if NO_REGEX not in selected_servers():
        pytest.skip(f"{NO_REGEX} is not selected")
    with sqlserver_provider(SQLSERVER_2025) as seeded:
        yield seeded


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def layer1_on_2025(sqlserver_2025: ServerProvider,
                         tmp_path_factory: pytest.TempPathFactory) -> AsyncGenerator[SlayerQueryEngine]:
    async with layer1_engine(sqlserver_2025, tmp_path_factory.mktemp("layer1_2025")) as engine:
        yield engine


@pytest.mark.parametrize("table", seeded_tables(), ids=lambda t: f"{t.schema_name or ''}.{t.name}")
def test_every_fixture_table_reads_back(provider: ServerProvider, table: Table) -> None:
    check_seeded(provider, table, engine=provider.engine)


@MODULE_LOOP
@pytest.mark.parametrize("row", ROWS, ids=lambda r: r.id)
async def test_layer1_construct(request: pytest.FixtureRequest, provider: ServerProvider, layer1: SlayerQueryEngine,
                                row: Row) -> None:
    mark_cell(request, row.state(provider.engine))
    await check_layer1(layer1, row, engine_name=provider.engine)


@MODULE_LOOP
@pytest.mark.parametrize("case", cases(), ids=lambda c: c.name)
async def test_wide_table_case_under_strict(request: pytest.FixtureRequest, provider: ServerProvider,
                                            wide_table: dict[str, SlayerQueryEngine], case: WideCase) -> None:
    mark_cell(request, case_state(case.name, provider.engine))
    await check_case_under_strict(wide_table, case, engine_name=provider.engine)


@MODULE_LOOP
@pytest.mark.parametrize("case", row_cases(), ids=lambda c: c.name)
async def test_wide_table_row_case_under_the_default(request: pytest.FixtureRequest, provider: ServerProvider,
                                                     wide_table: dict[str, SlayerQueryEngine], case: WideCase) -> None:
    mark_cell(request, case_state(case.name, provider.engine))
    await check_row_case_under_the_default(wide_table, case, engine_name=provider.engine)


@MODULE_LOOP
async def test_a_mixed_case_column_read_through_a_relationship(provider: ServerProvider,
                                                               mixed_case: SlayerQueryEngine) -> None:
    """As a dimension, in a filter and as a metric's input."""
    await check_rows(mixed_case, MIXED_CASE_QUERY, MIXED_CASE_ROWS, engine_name=provider.engine)


@MODULE_LOOP
async def test_a_mixed_case_column_on_its_own_root(provider: ServerProvider, mixed_case: SlayerQueryEngine) -> None:
    await check_rows(mixed_case, ROOTED_QUERY, ROOTED_ROWS, engine_name=provider.engine)


@MODULE_LOOP
@pytest.mark.parametrize("row", REGEX_ROWS, ids=lambda r: r.id)
async def test_regular_expressions_on_sql_server_2025(layer1_on_2025: SlayerQueryEngine, row: Row) -> None:
    """The regular-expression rows SQL Server 2022 refuses have Ossie's value on 2025."""
    await check_layer1_value(layer1_on_2025, row, engine_name=NO_REGEX)
