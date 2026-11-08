"""Law (python.sql.arc42.md §3.1): no execution parses a statement it rendered.

The conftest autouse fixture (``tests/_emitted_reparse_law.py``) fails any test in which a
public engine method parses SQL text it rendered; these are its self-tests.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect

from slayer.async_utils import run_sync
from slayer.engine.cache import CacheConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import naming, scope_check
from slayer.database.client import SlayerSQLClient
from tests import _emitted_reparse_law as law
from tests import _statement_render_law as render_law
from tests._dev2189_fixtures import POLICIES, SHAPES, ClientSpy, file_engine


def _statement() -> exp.Select:
    return exp.select("a").from_("t").where(exp.column("a").eq(1))


def _render_and_parse() -> str:
    text = _statement().sql()
    sqlglot.parse_one(text)
    return text


@pytest.fixture
def entry():
    """A fake public engine method: its dynamic extent is an execution."""
    return law.guard_entry


class TestSelfCheck:
    def test_parse_of_a_rendered_statement_inside_an_execution_fails(self, entry) -> None:
        run = entry(_render_and_parse)
        with pytest.raises(law.StatementReparsed):
            run()
        assert law.take_violations()

    def test_parse_three_calls_below_the_render_fails(self, entry) -> None:
        def _parse(text: str) -> None:
            sqlglot.parse_one(text)

        def _middle(text: str) -> None:
            _parse(text)

        def _top() -> None:
            _middle(_statement().sql())

        run = entry(_top)
        with pytest.raises(law.StatementReparsed):
            run()
        assert law.take_violations()

    def test_parse_into_is_guarded(self, entry) -> None:
        def _run() -> None:
            sqlglot.parse_one(_statement().sql(), into=exp.Select)

        run = entry(_run)
        with pytest.raises(law.StatementReparsed):
            run()
        assert law.take_violations()

    def test_a_finished_statement_is_recorded(self, entry) -> None:
        finish = law.guard_finisher(lambda: "SELECT a FROM t WHERE a = 1 /* fitted */")

        def _run() -> None:
            sqlglot.parse_one(finish())

        run = entry(_run)
        with pytest.raises(law.StatementReparsed):
            run()
        assert law.take_violations()

    def test_a_swallowed_violation_is_still_kept(self, entry) -> None:
        def _run() -> None:
            try:
                _render_and_parse()
            except Exception:  # noqa: BLE001 — production code that swallows errors
                pass

        entry(_run)()
        assert law.take_violations()

    def test_async_entry_is_an_execution(self, entry) -> None:
        async def _run() -> None:
            _render_and_parse()

        pending = entry(_run)()
        with pytest.raises(law.StatementReparsed):
            asyncio.run(pending)
        assert law.take_violations()

    def test_a_thread_offload_shares_the_record(self, entry) -> None:
        async def _run() -> None:
            text = _statement().sql()
            await asyncio.to_thread(sqlglot.parse_one, text)

        pending = entry(_run)()
        with pytest.raises(law.StatementReparsed):
            asyncio.run(pending)
        assert law.take_violations()

    def test_a_sync_bridge_inside_a_running_loop_is_an_execution(self, entry) -> None:
        async def _inner() -> None:
            _render_and_parse()

        async def _caller() -> None:
            run_sync(entry(_inner)())

        pending = _caller()
        with pytest.raises(law.StatementReparsed):
            asyncio.run(pending)
        assert law.take_violations()

    def test_nested_entries_share_the_record(self, entry) -> None:
        def _outer() -> None:
            text = _statement().sql()
            entry(lambda: sqlglot.parse_one(text))()

        run = entry(_outer)
        with pytest.raises(law.StatementReparsed):
            run()
        assert law.take_violations()

    def test_a_containing_string_is_not_a_reparse(self, entry) -> None:
        def _run() -> None:
            text = _statement().sql()
            sqlglot.parse_one(f"SELECT * FROM ({text}) AS x")

        entry(_run)()
        assert law.take_violations() == []

    def test_a_value_render_is_not_recorded(self, entry) -> None:
        def _run() -> None:
            sqlglot.parse_one(exp.column("a").eq(1).sql())

        entry(_run)()
        assert law.take_violations() == []

    def test_a_value_layer_render_is_not_recorded(self, entry) -> None:
        render = law.guard_value_layer(lambda: _statement().sql())

        def _run() -> None:
            sqlglot.parse_one(render())

        entry(_run)()
        assert law.take_violations() == []

    def test_the_validators_may_parse_the_statement(self, entry) -> None:
        def _run() -> None:
            text = _statement().sql()
            scope_check.check_scope_closed(text)
            scope_check.assert_dependency_ordered_ctes(text)
            naming.assert_unique_cte_names(sql=text)

        entry(_run)()
        assert law.take_violations() == []

    def test_a_parse_after_the_execution_returns_passes(self, entry) -> None:
        text = entry(lambda: _statement().sql())()
        sqlglot.parse_one(text)
        assert law.take_violations() == []

    def test_a_parse_outside_any_execution_passes(self) -> None:
        _render_and_parse()
        assert law.take_violations() == []


class TestTargets:
    @pytest.mark.parametrize(("module", "qualname"), [*law.VALIDATORS, *law.FINISHERS, *law.VALUE_LAYER_EXEMPT])
    def test_every_target_exists(self, module: str, qualname: str) -> None:
        assert render_law.resolve(module, qualname) is not None, (
            f"law target {module}.{qualname} is gone; re-point the law at its successor"
        )

    def test_every_public_engine_method_is_an_execution(self) -> None:
        names = law.entry_points()
        assert {"execute", "evict", "refresh", "get_column_types", "detect_join_cardinality", "save_model"} <= set(names)
        for name in names:
            assert getattr(inspect.getattr_static(SlayerQueryEngine, name), law.GUARD_MARK, None) == "entry", name

    def test_validators_are_exempt_by_name_too(self) -> None:
        assert getattr(scope_check.assert_unique_cte_names, law.GUARD_MARK, None) == "validator"
        assert getattr(scope_check.check_scope_closed, law.GUARD_MARK, None) == "validator"

    def test_sqlglot_entries_are_guarded(self) -> None:
        assert getattr(Dialect.parse, law.GUARD_MARK, None) == "Dialect.parse"
        assert getattr(Dialect.parse_into, law.GUARD_MARK, None) == "Dialect.parse_into"
        assert getattr(Dialect.generate, law.GUARD_MARK, None) == "Dialect.generate"


# --------------------------------------------------------------------------- #
# Live engine
# --------------------------------------------------------------------------- #
class TestLiveEngine:
    async def test_a_reparse_in_the_pipeline_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        prepare = SlayerQueryEngine._prepare_pipeline

        async def _reparsing(self, *args, **kwargs):
            prepared = await prepare(self, *args, **kwargs)
            sqlglot.parse_one(prepared.sql, read=prepared.dialect)
            return prepared

        monkeypatch.setattr(SlayerQueryEngine, "_prepare_pipeline", _reparsing)
        async with file_engine("sqlite") as (engine, _):
            with pytest.raises(law.StatementReparsed):
                await engine.execute(SHAPES["plain"], dry_run=True)
        assert law.take_violations()

    async def test_a_reparse_three_calls_below_the_pipeline_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        execute = SlayerSQLClient.execute

        def _parse(sql: str) -> None:
            sqlglot.parse_one(sql, read="sqlite")

        def _middle(sql: str) -> None:
            _parse(sql)

        async def _reparsing(client: SlayerSQLClient, *args, **kwargs):
            _middle(kwargs["sql"])
            return await execute(client, *args, **kwargs)

        monkeypatch.setattr(SlayerSQLClient, "execute", _reparsing)
        async with file_engine("sqlite") as (engine, _):
            with pytest.raises(law.StatementReparsed):
                await engine.execute(SHAPES["plain"])
        assert law.take_violations()

    async def test_a_parse_of_the_response_sql_after_execute_passes(self) -> None:
        async with file_engine("sqlite") as (engine, _):
            response = await engine.execute(SHAPES["plain"])
        assert response.sql is not None
        sqlglot.parse_one(response.sql, read="sqlite")
        assert law.take_violations() == []

    async def test_a_query_backed_type_probe_renders_only_its_probe(self, monkeypatch: pytest.MonkeyPatch) -> None:
        with ClientSpy().installed(monkeypatch) as spy:
            async with file_engine("duckdb") as (engine, _):
                with law.recorded_renders() as renders:
                    types = await engine.get_column_types("spend")
        assert types["amount_sum"] == "number"
        assert [text for kind, text in renders if kind == "finish"] == spy.probed
        assert [kind for kind, _ in renders] == ["render", "finish"]


#: Every engine operation that renders and runs SQL, under a policy.
async def _every_operation(engine: SlayerQueryEngine) -> None:
    await engine.execute(SHAPES["joined_dims"], dry_run=True)
    await engine.execute(SHAPES["plain"], explain=True)
    await engine.execute(SHAPES["plain"], cache=True)
    await engine.evict(SHAPES["plain"])
    await engine.execute(SHAPES["multi_stage"], cache=True)
    await engine.refresh()
    await engine.execute(SHAPES["semi_join"])
    await engine.execute(SHAPES["query_backed"])
    assert await engine.get_column_types("orders")
    assert await engine.get_column_types("spend")
    await engine.detect_join_cardinality(data_source="test", model="customers")
    await engine.create_model_from_query(
        {"source_model": "orders", "dimensions": ["status"], "measures": ["amount:sum"]}, name="by_status",
    )


@pytest.mark.parametrize("policy", [None, "column_pass", "join_multi_hop"])
async def test_every_operation_keeps_the_law(policy: str | None) -> None:
    async with file_engine("sqlite") as (base, _):
        engine = SlayerQueryEngine(
            storage=base.storage, policy=POLICIES[policy] if policy else None,
            cache_config=CacheConfig(refresh_keys=(("orders", "COUNT(*)"), ("customers", "COUNT(*)"))),
        )
        try:
            await _every_operation(engine)
        finally:
            engine.close()
    assert law.take_violations() == []
