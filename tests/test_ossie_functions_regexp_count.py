"""``regexp_count`` (specs: queries/regex › Regex functions; queries/scalar-functions › Version-gated functions fail
closed, Every dialect translates a library function or refuses it)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any

import pytest

from slayer.core.errors import QueryTypeError, ServerVersionError, SlayerError, UnsupportedServerFunctionError
from slayer.engine.query_engine import SlayerQueryEngine
from tests._dev2120_fixtures import patch_profile
from tests._ossie_functions_fixtures import (
    BACKENDS, assert_twins, by_id, emit_fn, execute_offline, fn_body, fn_engine, fn_query, named,
)
from tests._ossie_gate import TIER1


@pytest.fixture(scope="module", params=BACKENDS)
async def engine(request: pytest.FixtureRequest) -> AsyncGenerator[SlayerQueryEngine]:
    async with fn_engine(request.param) as e:
        yield e


#: Dimension → value for rows holding ``status`` 'pending', 'aaaa' and NULL.
COUNTS = {
    "regexp_count(fn.status, 'n')": {1: 2, 2: 0, 3: None},
    "REGEXP_COUNT(fn.status, 'N')": {1: 0, 2: 0, 3: None},
    "regexp_count(fn.status, 'N', 'i')": {1: 2, 2: 0, 3: None},
    "regexp_count(fn.status, 'aa')": {1: 0, 2: 2, 3: None},
    "regexp_count(fn.status, 'z')": {1: 0, 2: 0, 3: None},
}
COUNT_QUERY = fn_query("regexp_count(fn.code, 'a')")
EMPTY_MATCH = fn_query("regexp_count(fn.code, 'x*')", filter=["fn.id = 1"])


def _ints(values: dict[int, Any]) -> dict[int, Any]:
    return {k: None if v is None else int(v) for k, v in values.items()}


class TestCounts:
    @pytest.mark.parametrize("expr", sorted(COUNTS))
    async def test_counts(self, engine: SlayerQueryEngine, expr: str) -> None:
        resp = await engine.execute(fn_query(expr, filter=["fn.id IN (1, 2, 3)"]))
        assert _ints(by_id(resp)) == COUNTS[expr]

    async def test_spellings_share_the_key(self, engine: SlayerQueryEngine) -> None:
        await assert_twins(engine, "REGEXP_COUNT(fn.status, 'n')", "regexp_count(fn.status, 'n')")

    async def test_empty_match_counts_as_postgres_or_is_refused(self, engine: SlayerQueryEngine) -> None:
        try:
            await engine.execute(EMPTY_MATCH, dry_run=True)
        except QueryTypeError as exc:
            assert "regexp_count" in str(exc), str(exc)
            with pytest.raises(QueryTypeError):
                await engine.execute(EMPTY_MATCH)
            return
        assert _ints(by_id(await engine.execute(EMPTY_MATCH))) == {1: 4}

    @pytest.mark.parametrize("dialect", [d for d in TIER1 if d != "mysql"])
    async def test_empty_match_emits_or_is_refused_at_plan_time(self, dialect: str) -> None:
        try:
            await emit_fn(EMPTY_MATCH, dialect=dialect)
        except QueryTypeError as exc:
            assert "regexp_count" in str(exc), str(exc)

    @pytest.mark.parametrize("dialect", [d for d in TIER1 if d != "mysql"])
    async def test_emits(self, dialect: str) -> None:
        await emit_fn(fn_query(named("regexp_count(fn.status, 'n')", "a"), named("regexp_count(fn.status, 'n', 'i')",
                                                                                   "b")), dialect=dialect)

    @pytest.mark.parametrize("expr", ["regexp_count(fn.status, 'n', 'g')", "regexp_count(fn.status, fn.code)",
                                      "regexp_count(fn.status)", "regexp_count(fn.status, 'n', 'i', 1)"])
    async def test_bad_calls_refused(self, engine: SlayerQueryEngine, expr: str) -> None:
        body = fn_body(expr)
        with pytest.raises((SlayerError, ValueError)) as info:
            await engine.execute(body)
        assert "regexp_count" in str(info.value).lower(), str(info.value)


async def _dry_run(query: Any, *, dialect: str) -> Any:
    return await emit_fn(query, dialect=dialect)


RUNS = {"dry-run": _dry_run, "execution": execute_offline}


class TestMySQL:
    @pytest.mark.parametrize("run", sorted(RUNS))
    async def test_refused(self, run: str) -> None:
        with pytest.raises(UnsupportedServerFunctionError) as info:
            await RUNS[run](COUNT_QUERY, dialect="mysql")
        assert "regexp_count" in str(info.value), str(info.value)
        assert "MySQL" in str(info.value), str(info.value)


class TestVersionGates:
    @pytest.mark.parametrize("run", sorted(RUNS))
    async def test_postgres_14_refused(self, monkeypatch: pytest.MonkeyPatch, run: str) -> None:
        patch_profile(monkeypatch, version=(14, 0))
        with pytest.raises(ServerVersionError) as info:
            await RUNS[run](COUNT_QUERY, dialect="postgres")
        assert "regexp_count" in str(info.value), str(info.value)
        assert "15" in str(info.value), str(info.value)

    async def test_postgres_15_emits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        patch_profile(monkeypatch, version=(15, 0))
        await emit_fn(COUNT_QUERY, dialect="postgres")

    @pytest.mark.parametrize("run", sorted(RUNS))
    async def test_sql_server_2022_refused(self, monkeypatch: pytest.MonkeyPatch, run: str) -> None:
        patch_profile(monkeypatch, version=(16, 0))
        with pytest.raises(ServerVersionError) as info:
            await RUNS[run](COUNT_QUERY, dialect="tsql")
        assert "regexp_count" in str(info.value), str(info.value)
        assert "2025" in str(info.value), str(info.value)

    async def test_sql_server_2025_emits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        patch_profile(monkeypatch, version=(17, 0))
        await emit_fn(COUNT_QUERY, dialect="tsql")

    @pytest.mark.parametrize("dialect", ["postgres", "tsql"])
    async def test_unknown_version_emits(self, monkeypatch: pytest.MonkeyPatch, dialect: str) -> None:
        patch_profile(monkeypatch, version=None)
        await emit_fn(COUNT_QUERY, dialect=dialect)
