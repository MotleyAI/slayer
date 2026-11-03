"""Every list construct has one meaning on every database with native lists (spec: queries/list-functions › One
meaning on every database with native lists): executed on DuckDB and Postgres, refused on SQLite at plan time,
emitted on every Tier-1 dialect."""

from __future__ import annotations

import pytest
from pytest_postgresql import factories

from slayer.core.errors import ListTypeError, SlayerError
from slayer.core.keys import SCALAR_SPECS
from slayer.core.models import DatasourceConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2120_fixtures import (
    LIST_COLUMNS,
    LIST_FUNCTIONS,
    NATIVE_LIST_DIALECTS,
    NO_LIST_DATABASES,
    TIER1,
    Case,
    contractors_engine,
    contractors_statements,
    emit,
    emit_contractors,
    list_cases,
    run_case,
)
from tests._dev2137_fixtures import Probe, all_models, assignments_statements, check_lists, list_engine, list_probes

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

LIST_CASES = list_cases()
LIST_CASE_IDS = [c.case_id for c in LIST_CASES]
PROBES = list_probes()
PROBE_IDS = [p.name for p in PROBES]
#: A semi-join beside a list child needs the ClickHouse server version, which a dry run cannot read.
_CLICKHOUSE_VERSION_GATED = frozenset({"elements-filtered-across-another-to-many"})


class TestListRegistry:
    def test_list_case_ids_are_unique(self) -> None:
        assert len(set(LIST_CASE_IDS)) == len(LIST_CASE_IDS)

    def test_probe_names_are_unique(self) -> None:
        assert len(set(PROBE_IDS)) == len(PROBE_IDS)

    def test_list_functions_are_registered(self) -> None:
        assert sorted(LIST_FUNCTIONS - set(SCALAR_SPECS)) == []

    @pytest.mark.parametrize("name", sorted(LIST_FUNCTIONS))
    def test_every_list_function_has_list_cases(self, name: str) -> None:
        assert any(name in c.covers for c in LIST_CASES), f"no list case covers {name!r}"


@pytest.fixture(scope="module")
async def duckdb_lists():
    async with contractors_engine("duckdb") as e:
        yield e


@pytest.fixture(scope="module")
async def sqlite_lists():
    async with contractors_engine("sqlite", models=all_models()) as e:
        yield e


@pytest.fixture(scope="module")
async def duckdb_probes():
    async with list_engine() as e:
        yield e


@pytest.mark.parametrize("case", LIST_CASES, ids=LIST_CASE_IDS)
async def test_list_case_executes_on_duckdb(duckdb_lists, case: Case) -> None:
    await run_case(duckdb_lists, case)


@pytest.mark.parametrize("probe", PROBES, ids=PROBE_IDS)
async def test_list_probe_executes_on_duckdb(duckdb_probes, probe: Probe) -> None:
    resp = await duckdb_probes.execute(probe.query)
    assert probe.read(resp) == probe.expected, resp.sql


def _is_list_construct(case: Case) -> bool:
    """A list function applied to a list (``IS NULL`` and the TEXT meanings are not list constructs)."""
    return bool(LIST_FUNCTIONS & set(case.covers)) and any(c in case.expr for c in LIST_COLUMNS)


LIST_CONSTRUCTS = [c for c in LIST_CASES if _is_list_construct(c)]


@pytest.mark.parametrize("case", LIST_CONSTRUCTS, ids=[c.case_id for c in LIST_CONSTRUCTS])
async def test_list_case_refused_on_sqlite(sqlite_lists, case: Case) -> None:
    with pytest.raises(ListTypeError) as info:
        await sqlite_lists.execute(case.query())
    msg = str(info.value)
    assert "SQLite" in msg, msg
    assert any(name in msg for name in LIST_FUNCTIONS & set(case.covers)), msg


@pytest.mark.parametrize("case", [c for c in LIST_CASES if not _is_list_construct(c)],
                         ids=lambda c: c.case_id)
async def test_non_list_constructs_execute_on_sqlite(sqlite_lists, case: Case) -> None:
    await run_case(sqlite_lists, case)


@pytest.mark.parametrize("probe", PROBES, ids=PROBE_IDS)
async def test_list_probe_refused_on_sqlite(sqlite_lists, probe: Probe) -> None:
    with pytest.raises(ListTypeError, match="SQLite"):
        await sqlite_lists.execute(probe.query)


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("case", LIST_CASES, ids=LIST_CASE_IDS)
async def test_list_case_emits(case: Case, dialect: str) -> None:
    if dialect in NATIVE_LIST_DIALECTS or not _is_list_construct(case):
        await emit_contractors(case.query(), dialect=dialect)
        return
    with pytest.raises(SlayerError) as info:
        await emit_contractors(case.query(), dialect=dialect)
    assert NO_LIST_DATABASES[dialect] in str(info.value), str(info.value)


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("probe", PROBES, ids=PROBE_IDS)
async def test_list_probe_emits(probe: Probe, dialect: str) -> None:
    if dialect == "clickhouse" and probe.name in _CLICKHOUSE_VERSION_GATED:
        pytest.skip("needs a ClickHouse server version")
    if dialect in NATIVE_LIST_DIALECTS:
        await emit(probe.query, dialect=dialect, models=all_models())
        return
    with pytest.raises(SlayerError) as info:
        await emit(probe.query, dialect=dialect, models=all_models())
    assert NO_LIST_DATABASES[dialect] in str(info.value), str(info.value)


@pytest.fixture
async def postgres_lists(postgresql, tmp_path):
    cur = postgresql.cursor()
    for stmt in [*contractors_statements(dialect="postgres"), *assignments_statements(dialect="postgres")]:
        cur.execute(stmt)
    postgresql.commit()
    info = postgresql.info
    ds = DatasourceConfig(name="test", type="postgres", host=info.host, port=info.port, database=info.dbname,
                          username=info.user, password="")
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(ds)
    for model in all_models():
        await storage.save_model(model)
    engine = SlayerQueryEngine(storage=storage)
    try:
        yield engine
    finally:
        engine.close()
        engine_factory.invalidate_engine(ds)


async def test_every_list_case_and_probe_executes_on_postgres(postgres_lists) -> None:
    await check_lists(postgres_lists, dialect="postgres")
