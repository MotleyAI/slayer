"""ClickHouse gating for semi-join pushdown.

The recursive plan predicate triggers the server-profile gate and the settings
attachment on planner-emitted SQL; below 25.4 (or unknown), or for a
``readonly = 1`` user who cannot enable correlated subqueries, the query fails
closed with a general error naming the filter — not the RLS ``ForcedFilterError``.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import Any

import sqlglot
from sqlglot import exp

import pytest

from slayer.core.errors import ForcedFilterError, SlayerError
from slayer.core.models import DatasourceConfig
from slayer.core.policy import (
    ColumnFilterRuleset,
    JoinFilterRule,
    JoinFilterRuleset,
    SessionPolicy,
)
from slayer.engine.plan import plan_query, plan_stages
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.planned import plan_has_semi_join_filters
from slayer.sql.client import ExecutionResult, SlayerSQLClient
from slayer.sql.dialects.clickhouse import _attach_ch_correlated_setting
from slayer.sql.generator import generate_planned_stages
from slayer.sql.session_policy import apply_session_policy
from slayer.storage.yaml_storage import YAMLStorage

from tests._ch_fake_http import FakeClickHouse, fake_ch_engine, route_engine
from tests._dev1840_fixtures import (
    ModelMeasure,
    SPEND_BAND_170,
    bundle,
    dev1840_models,
    gen,
    q,
)

_SETTING = "allow_experimental_correlated_subqueries"

M = ModelMeasure(formula="amount:sum", name="m")
CM = ModelMeasure(formula="customers.spend:sum", name="cm")

PUSHED = q(dimensions=["customers.tier"], measures=[M, CM],
           filters=["channel = 'app'"])

_ORDERS_RULE = JoinFilterRule(
    target_table="orders", join_path=("orders.customer_id = customers.id",),
)


async def _ch_engine(tmp_path, *, policy: SessionPolicy | None = None) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(DatasourceConfig(name="test", type="clickhouse"))
    for model in dev1840_models():
        await storage.save_model(model, _validate=False)
    return SlayerQueryEngine(storage=storage, policy=policy)


class ChServer:
    """A fake ClickHouse answering the profile probes; user statements are recorded, never sent."""

    def __init__(self, *, monkeypatch: pytest.MonkeyPatch, stack: contextlib.ExitStack) -> None:
        self._monkeypatch = monkeypatch
        self._stack = stack
        self.executed: list[str] = []

    def start(self, **kw: Any) -> FakeClickHouse:
        fake = FakeClickHouse(**kw)
        route_engine(self._monkeypatch, self._stack.enter_context(fake_ch_engine(fake)))
        return fake


@pytest.fixture
def ch_server(monkeypatch: pytest.MonkeyPatch) -> Iterator[ChServer]:
    with contextlib.ExitStack() as stack:
        server = ChServer(monkeypatch=monkeypatch, stack=stack)

        async def fake_execute(self, sql, timeout_seconds=120):  # NOSONAR(S7503) — must stay async
            server.executed.append(sql)
            return ExecutionResult(rows=[])

        monkeypatch.setattr(SlayerSQLClient, "execute", fake_execute)
        yield server


async def _dry_sql(engine: SlayerQueryEngine, query) -> str:
    sql = (await engine.execute(query, dry_run=True)).sql
    assert sql is not None
    return sql


def _version_probe(ch_server: ChServer, raw: str | None) -> FakeClickHouse:
    """Serve ``raw`` as the version (readonly 0, setting off); ``None`` fails the probe."""
    return ch_server.start(version=raw or "0", fail_permission=raw is None)


class TestPlanPredicate:
    def test_false_without_any_filter(self):
        planned = plan_query(
            query=q(dimensions=["customers.tier"], measures=[M, CM]),
            bundle=bundle(),
        )
        assert plan_has_semi_join_filters(planned) is False

    def test_false_on_an_inline_filter(self):
        planned = plan_query(
            query=q(dimensions=["customers.tier"], measures=[CM],
                    filters=["customers.regions.name = 'North'"]),
            bundle=bundle(),
        )
        assert plan_has_semi_join_filters(planned) is False

    def test_true_on_a_pushed_filter(self):
        planned = plan_query(query=PUSHED, bundle=bundle())
        assert plan_has_semi_join_filters(planned) is True

    def test_true_when_only_a_nested_producer_pushes(self):
        planned = plan_query(
            query=q(dimensions=[{"expression": SPEND_BAND_170, "name": "sband"}],
                    measures=[M], filters=["channel = 'app'"]),
            bundle=bundle(),
        )
        assert plan_has_semi_join_filters(planned) is True


class TestVersionGate:
    async def test_below_min_version_fails_closed(self, tmp_path, ch_server):
        """Scenario: ClickHouse below 25.4 fails closed."""
        _version_probe(ch_server, "25.3.1.100")
        engine = await _ch_engine(tmp_path)
        with pytest.raises(SlayerError) as ei:
            await engine.execute(PUSHED, dry_run=True)
        assert not isinstance(ei.value, ForcedFilterError)
        message = str(ei.value)
        assert "25.4" in message
        assert "channel" in message

    async def test_unknown_version_fails_closed(self, tmp_path, ch_server):
        _version_probe(ch_server, None)
        engine = await _ch_engine(tmp_path)
        with pytest.raises(SlayerError) as ei:
            await engine.execute(PUSHED, dry_run=True)
        assert "25.4" in str(ei.value)

    async def test_execute_path_is_gated_too(self, tmp_path, ch_server):
        _version_probe(ch_server, "25.3.1.100")
        engine = await _ch_engine(tmp_path)
        with pytest.raises(SlayerError) as ei:
            await engine.execute(PUSHED)
        assert "25.4" in str(ei.value)

    async def test_explain_path_is_gated_too(self, tmp_path, ch_server):
        _version_probe(ch_server, "25.3.1.100")
        engine = await _ch_engine(tmp_path)
        with pytest.raises(SlayerError) as ei:
            await engine.execute(PUSHED, explain=True)
        assert "25.4" in str(ei.value)

    async def test_supported_version_attaches_the_setting(
        self, tmp_path, ch_server,
    ):
        _version_probe(ch_server, "25.4.1.100")
        engine = await _ch_engine(tmp_path)
        sql = await _dry_sql(engine, PUSHED)
        assert "EXISTS" in sql.upper()
        assert sql.count("SETTINGS") == 1
        parsed = sqlglot.parse_one(sql, dialect="clickhouse")
        settings = parsed.args.get("settings") or []
        assert any(_SETTING in s.sql() for s in settings)

    async def test_inline_query_needs_no_probe_or_setting(
        self, tmp_path, ch_server,
    ):
        """No semi-join in the plan: no profile probe, no setting."""
        fake = _version_probe(ch_server, "25.4.1.100")
        engine = await _ch_engine(tmp_path)
        sql = await _dry_sql(
            engine,
            q(dimensions=["customers.tier"], measures=[CM],
              filters=["customers.plans.level = 'basic'"]),
        )
        assert _SETTING not in sql
        assert fake.permission_checks() == 0

    async def test_probe_result_is_cached_across_queries(
        self, tmp_path, ch_server,
    ):
        fake = _version_probe(ch_server, "25.4.1.100")
        engine = await _ch_engine(tmp_path)
        await engine.execute(PUSHED, dry_run=True)
        await engine.execute(PUSHED, dry_run=True)
        assert fake.permission_checks() == 1

    async def test_failed_probe_is_rechecked_by_the_next_query(self, tmp_path, ch_server):
        fake = _version_probe(ch_server, None)
        engine = await _ch_engine(tmp_path)
        for _ in range(2):
            with pytest.raises(SlayerError, match="could not be determined"):
                await engine.execute(PUSHED, dry_run=True)
        assert fake.permission_checks() == 2


def _assert_readonly_refusal(message: str, *, upgrade: bool) -> None:
    assert "channel" in message
    assert _SETTING in message
    assert "readonly" in message
    assert "profile" in message
    assert "readonly=2" in message.replace(" ", "")
    assert ("25.8" in message) is upgrade


class TestReadonlyGate:
    @pytest.mark.parametrize("mode", ["dry_run", "explain", "execute"])
    async def test_setting_off_refuses_before_the_database(self, tmp_path, ch_server, mode):
        """Scenario: ClickHouse readonly user who cannot enable correlated subqueries."""
        fake = ch_server.start(version="25.4.1.1", readonly=1, correlated=False)
        engine = await _ch_engine(tmp_path)
        with pytest.raises(SlayerError) as ei:
            await engine.execute(PUSHED, dry_run=mode == "dry_run", explain=mode == "explain")
        assert not isinstance(ei.value, ForcedFilterError)
        _assert_readonly_refusal(str(ei.value), upgrade=True)
        assert ch_server.executed == []
        assert fake.user_statements() == []

    async def test_from_25_8_the_remedies_omit_the_upgrade(self, tmp_path, ch_server):
        ch_server.start(version="25.8.3.1", readonly=1, correlated=False)
        engine = await _ch_engine(tmp_path)
        with pytest.raises(SlayerError) as ei:
            await engine.execute(PUSHED, dry_run=True)
        _assert_readonly_refusal(str(ei.value), upgrade=False)

    async def test_setting_already_on_executes_with_the_setting(self, tmp_path, ch_server):
        """Scenario: ClickHouse readonly user with correlated subqueries already on."""
        ch_server.start(version="25.8.3.1", readonly=1, correlated=True)
        engine = await _ch_engine(tmp_path)
        await engine.execute(PUSHED)
        (sql,) = ch_server.executed
        assert sql.count("SETTINGS") == 1
        assert f"{_SETTING} = 1" in sql

    async def test_readonly_2_executes_with_the_setting(self, tmp_path, ch_server):
        ch_server.start(version="25.4.1.1", readonly=2, correlated=False)
        engine = await _ch_engine(tmp_path)
        await engine.execute(PUSHED)
        (sql,) = ch_server.executed
        assert f"{_SETTING} = 1" in sql

    async def test_unreadable_setting_fails_closed_and_is_rechecked(self, tmp_path, ch_server):
        """Scenario: Undeterminable correlated-subquery setting fails closed."""
        fake = ch_server.start(version="25.8.3.1", readonly=1, fail_correlated=True)
        engine = await _ch_engine(tmp_path)
        for _ in range(2):
            with pytest.raises(SlayerError) as ei:
                await engine.execute(PUSHED)
            assert _SETTING in str(ei.value)
        assert ch_server.executed == []
        assert fake.correlated_checks() == 2


class TestPolicyNeedsNoProbe:
    @pytest.mark.parametrize(
        "ruleset",
        [
            JoinFilterRuleset(
                table="customers", column="organization_uuid", value="orgA",
                joins=(_ORDERS_RULE,),
            ),
            JoinFilterRuleset(
                table="customers", column="organization_uuid", value="orgA",
                whitelist=("orders",),
            ),
            ColumnFilterRuleset(column="organization_uuid", value="orgA"),
        ],
        ids=["join", "anchor_only", "column"],
    )
    async def test_policy_only_query_sends_nothing(self, tmp_path, ch_server, monkeypatch, ruleset):
        fake = ch_server.start(version="25.4.1.1", readonly=1, correlated=False)
        engine = await _ch_engine(tmp_path, policy=SessionPolicy(ruleset=ruleset))
        monkeypatch.setattr(engine, "_column_present", lambda **_kw: True)
        sql = await _dry_sql(engine, q(measures=[M]))
        assert fake.requests == []
        assert _SETTING not in sql
        assert "EXISTS" not in sql.upper()


class TestSettingsCoexistence:
    async def test_rls_attachment_keeps_one_settings_clause(
        self, tmp_path, ch_server,
    ):
        """Planner-attached setting + RLS join-rule rewrite: one SETTINGS, forced
        to 1; the RLS rewrite adds an IN and leaves the planner's EXISTS alone."""
        _version_probe(ch_server, "25.4.1.100")
        engine = await _ch_engine(tmp_path)
        sql = await _dry_sql(engine, PUSHED)
        assert _SETTING in sql, "precondition: planner attached the setting"
        n_semi_join_exists = sql.upper().count("EXISTS")
        assert n_semi_join_exists >= 1

        def _boom_probe(*_a, **_kw):
            raise AssertionError("join path never probes columns")

        out = apply_session_policy(
            sql,
            dialect="clickhouse",
            policy=SessionPolicy(ruleset=JoinFilterRuleset(
                table="customers", column="organization_uuid", value="orgA",
                joins=(_ORDERS_RULE,),
            )),
            has_column=_boom_probe,
        )
        assert out.count("SETTINGS") == 1
        assert out.count(_SETTING) == 1
        assert f"{_SETTING} = 1" in out
        assert out.upper().count("EXISTS") == n_semi_join_exists
        parsed = sqlglot.parse_one(out, dialect="clickhouse")
        assert any(node.args.get("query") is not None for node in parsed.find_all(exp.In))
        assert "organization_uuid" in out

    def test_shared_attach_helper_handles_a_union(self):
        """The finalization shares the session-policy attachment helper;
        a set operation must round-trip with ONE trailing SETTINGS."""
        ast = sqlglot.parse_one(
            "SELECT id FROM a WHERE EXISTS(SELECT 1 FROM b WHERE b.x = a.x) "
            "UNION ALL SELECT id FROM c",
            dialect="clickhouse",
        )
        _attach_ch_correlated_setting(ast)
        out = ast.sql(dialect="clickhouse")
        assert out.count("SETTINGS") == 1
        assert out.count(f"{_SETTING} = 1") == 1
        sqlglot.parse_one(out, dialect="clickhouse")


def _generated(query, *, dialect: str) -> str:
    b = bundle().model_copy(update={"dialect": dialect})
    return generate_planned_stages(plan_stages(queries=[query], bundle=b), bundle=b, dialect=dialect)


class TestGeneratorAttachesTheSetting:
    def test_semi_join_statement_carries_one_setting(self):
        sql = _generated(PUSHED, dialect="clickhouse")
        assert sql.count("SETTINGS") == 1
        assert sql.count(f"{_SETTING} = 1") == 1

    def test_inline_statement_carries_none(self):
        sql = _generated(q(dimensions=["customers.tier"], measures=[CM]), dialect="clickhouse")
        assert _SETTING not in sql

    async def test_gating_adds_no_reparse_of_the_rendered_statement(self, tmp_path, ch_server, monkeypatch):
        """sql §3.1: the engine attaches nothing by re-parsing; ClickHouse parses no more than DuckDB."""
        ch_server.start(version="25.4.1.1", readonly=0)
        real = sqlglot.parse_one
        parses: list[str] = []

        def spy(sql, *args, **kwargs):
            if isinstance(sql, str) and '"orders.customers.tier"' in sql:
                parses.append(sql)
            return real(sql, *args, **kwargs)

        clickhouse = await _ch_engine(tmp_path)
        monkeypatch.setattr(sqlglot, "parse_one", spy)
        await _dry_sql(clickhouse, PUSHED)
        clickhouse_parses = len(parses)
        parses.clear()
        await gen(PUSHED, dialect="duckdb")
        assert clickhouse_parses <= len(parses)


class TestOtherDialects:
    async def test_non_clickhouse_emits_no_setting(self):
        sql = await gen(PUSHED, dialect="duckdb")
        assert _SETTING not in sql

    async def test_non_gated_dialect_runs_no_probe(self, monkeypatch):
        async def boom(self):  # NOSONAR(S7503) — must stay async
            raise AssertionError("non-gated dialects need no server profile")

        monkeypatch.setattr(SlayerSQLClient, "server_profile", boom)
        await gen(PUSHED, dialect="duckdb")
