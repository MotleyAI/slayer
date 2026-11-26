"""Every Mode-B library function and operator has executed cases with one meaning (spec: queries/scalar-functions › One meaning on every Tier-1 dialect; queries/list-functions › One meaning on every database with native lists)."""

from __future__ import annotations

import pytest

from slayer.core.errors import SlayerError
from slayer.core.keys import SCALAR_SPECS
from tests._dev2120_fixtures import (
    EXEC_BACKENDS,
    LIST_COLUMNS,
    LIST_FUNCTIONS,
    NATIVE_LIST_DIALECTS,
    NEW_FUNCTIONS,
    NO_LIST_DATABASES,
    TIER1,
    Case,
    contractors_engine,
    emit_contractors,
    emit_lib,
    lib_engine,
    library_cases,
    list_cases,
    required_names,
    run_case,
)
from tests._dev2457_fixtures import BACKENDS as WINDOW_BACKENDS
from tests._dev2457_fixtures import window_engine
from tests._dev2457_matrix import CASE_IDS as WINDOW_CASE_IDS
from tests._dev2457_matrix import CASES as WINDOW_CASES
from tests._dev2457_matrix import (
    DUCKDB_HOLISTIC,
    HOLISTIC,
    HOLISTIC_IDS,
    WindowCase,
    assert_typed_refusal,
    check_case,
    emit_window,
    holds_window,
    window_values,
)

CASES = library_cases()
CASE_IDS = [c.case_id for c in CASES]
LIST_CASES = list_cases()
LIST_CASE_IDS = [c.case_id for c in LIST_CASES]


class TestRegistry:
    def test_case_ids_are_unique(self) -> None:
        assert len(set(CASE_IDS)) == len(CASE_IDS)

    def test_new_functions_are_registered(self) -> None:
        assert sorted(NEW_FUNCTIONS - set(SCALAR_SPECS)) == []

    @pytest.mark.parametrize("name", sorted(required_names()))
    def test_every_function_and_operator_has_cases(self, name: str) -> None:
        assert any(name in c.covers for c in [*CASES, *LIST_CASES]), f"no matrix case covers {name!r}"

    def test_every_registered_scalar_has_cases(self) -> None:
        covered = {n for c in [*CASES, *LIST_CASES] for n in c.covers}
        assert sorted(set(SCALAR_SPECS) - covered) == []


@pytest.fixture(scope="module", params=EXEC_BACKENDS)
async def engine(request):
    async with lib_engine(request.param) as e:
        yield e


@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
async def test_executes(engine, case: Case) -> None:
    await run_case(engine, case)


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("case", CASES, ids=CASE_IDS)
async def test_emits(case: Case, dialect: str) -> None:
    await emit_lib(case.query(), dialect=dialect)


class TestListRegistry:
    def test_list_case_ids_are_unique(self) -> None:
        assert len(set(LIST_CASE_IDS)) == len(LIST_CASE_IDS)

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
    async with contractors_engine("sqlite") as e:
        yield e


@pytest.mark.parametrize("case", LIST_CASES, ids=LIST_CASE_IDS)
async def test_list_case_executes_on_duckdb(duckdb_lists, case: Case) -> None:
    await run_case(duckdb_lists, case)


def _is_list_construct(case: Case) -> bool:
    """A list function applied to a list (``IS NULL`` and the TEXT meanings are not list constructs)."""
    return bool(LIST_FUNCTIONS & set(case.covers)) and any(c in case.expr for c in LIST_COLUMNS)


LIST_CONSTRUCTS = [c for c in LIST_CASES if _is_list_construct(c)]


@pytest.mark.parametrize("case", LIST_CONSTRUCTS, ids=[c.case_id for c in LIST_CONSTRUCTS])
async def test_list_case_refused_on_sqlite(sqlite_lists, case: Case) -> None:
    q = case.query()
    with pytest.raises(SlayerError) as info:
        await sqlite_lists.execute(q)
    msg = str(info.value)
    assert "SQLite" in msg, msg
    assert any(name in msg for name in LIST_FUNCTIONS & set(case.covers)), msg


@pytest.mark.parametrize("case", [c for c in LIST_CASES if not _is_list_construct(c)],
                         ids=lambda c: c.case_id)
async def test_non_list_constructs_execute_on_sqlite(sqlite_lists, case: Case) -> None:
    await sqlite_lists.execute(case.query())


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("case", LIST_CASES, ids=LIST_CASE_IDS)
async def test_list_case_emits(case: Case, dialect: str) -> None:
    if dialect in NATIVE_LIST_DIALECTS or not _is_list_construct(case):
        await emit_contractors(case.query(), dialect=dialect)
        return
    q = case.query()
    with pytest.raises(SlayerError) as info:
        await emit_contractors(q, dialect=dialect)
    assert NO_LIST_DATABASES[dialect] in str(info.value), str(info.value)


# --------------------------------------------------------------------------- #
# Window functions (spec: queries/window-functions › Every database runs the required windows or refuses
# with a typed error)
# --------------------------------------------------------------------------- #
def test_window_case_ids_are_unique() -> None:
    assert len(set(WINDOW_CASE_IDS)) == len(WINDOW_CASE_IDS)


def test_tier1_holds_the_emission_only_databases() -> None:
    assert {"snowflake", "bigquery"} <= set(TIER1)


@pytest.fixture(scope="module", params=WINDOW_BACKENDS)
async def windows(request):
    async with window_engine(request.param) as e:
        yield e


@pytest.fixture(scope="module")
async def duckdb_windows():
    async with window_engine("duckdb") as e:
        yield e


@pytest.fixture(scope="module")
async def sqlite_windows():
    async with window_engine("sqlite") as e:
        yield e


@pytest.mark.parametrize("case", WINDOW_CASES, ids=WINDOW_CASE_IDS)
async def test_the_matrix_executes(windows, case: WindowCase) -> None:
    await check_case(windows, case)


@pytest.mark.parametrize("dialect", TIER1)
@pytest.mark.parametrize("case", WINDOW_CASES, ids=WINDOW_CASE_IDS)
async def test_the_matrix_emits_a_window(case: WindowCase, dialect: str) -> None:
    sql = await emit_window(case, dialect=dialect)
    assert holds_window(sql, dialect=dialect), sql


@pytest.mark.parametrize("expression", DUCKDB_HOLISTIC, ids=["median", "percentile_cont"])
async def test_a_holistic_aggregate_where_the_database_windows_it(duckdb_windows, expression: str) -> None:
    got = await window_values(duckdb_windows, "orders", expression)
    assert {k: float(v) for k, v in got.items()} == dict.fromkeys(range(1, 8), 40.0)


@pytest.mark.parametrize(("expression", "function"), HOLISTIC, ids=HOLISTIC_IDS)
async def test_a_holistic_aggregate_where_the_database_does_not(sqlite_windows, expression: str, function: str) -> None:
    await assert_typed_refusal(sqlite_windows, "orders", expression, function, "SQLite")
