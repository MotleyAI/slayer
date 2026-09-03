"""DEV-1840 task 1.7 — ClickHouse gating for semi-join pushdown (design D7).

The recursive plan predicate triggers the version preflight and the settings
finalization on planner-emitted SQL; below 25.4 (or unknown) the query fails
closed with a general error naming the filter and the version requirement —
not the RLS ``ForcedFilterError``.
"""

from __future__ import annotations

import sqlglot

import pytest

import slayer.engine.query_engine as qe
from slayer.core.errors import ForcedFilterError, SlayerError
from slayer.core.models import DatasourceConfig
from slayer.core.policy import JoinFilterRule, JoinFilterRuleset, SessionPolicy
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.stage_planner import plan_query
from slayer.sql.client import SlayerSQLClient
from slayer.sql.session_policy import (
    _attach_ch_correlated_setting,
    apply_session_policy,
)
from slayer.storage.yaml_storage import YAMLStorage

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


async def _ch_engine(tmp_path) -> SlayerQueryEngine:
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(DatasourceConfig(name="test", type="clickhouse"))
    for model in dev1840_models():
        await storage.save_model(model, _validate=False)
    return SlayerQueryEngine(storage=storage)


def _version_probe(monkeypatch, raw: str | None) -> dict:
    """Patch the SQL client's execute: answer the version probe (or fail it
    when ``raw`` is None), swallow anything else."""
    calls = {"n": 0}

    async def fake_execute(self, sql, timeout_seconds=120):  # NOSONAR(S7503) — must stay async
        if "version" not in sql.lower():
            return []
        calls["n"] += 1
        if raw is None:
            raise RuntimeError("cannot reach clickhouse")
        return [{"version()": raw}]

    monkeypatch.setattr(SlayerSQLClient, "execute", fake_execute)
    return calls


class TestPlanPredicate:
    def test_false_without_any_filter(self):
        planned = plan_query(
            query=q(dimensions=["customers.tier"], measures=[M, CM]),
            bundle=bundle(),
        )
        assert qe.plan_has_semi_join_filters(planned) is False

    def test_false_on_an_inline_filter(self):
        planned = plan_query(
            query=q(dimensions=["customers.tier"], measures=[CM],
                    filters=["customers.regions.name = 'North'"]),
            bundle=bundle(),
        )
        assert qe.plan_has_semi_join_filters(planned) is False

    def test_true_on_a_pushed_filter(self):
        planned = plan_query(query=PUSHED, bundle=bundle())
        assert qe.plan_has_semi_join_filters(planned) is True

    def test_true_when_only_a_nested_producer_pushes(self):
        planned = plan_query(
            query=q(dimensions=[{"expression": SPEND_BAND_170, "name": "sband"}],
                    measures=[M], filters=["channel = 'app'"]),
            bundle=bundle(),
        )
        assert qe.plan_has_semi_join_filters(planned) is True


class TestVersionGate:
    async def test_below_min_version_fails_closed(self, tmp_path, monkeypatch):
        """Scenario: ClickHouse below 25.4 fails closed."""
        _version_probe(monkeypatch, "25.3.1.100")
        engine = await _ch_engine(tmp_path)
        with pytest.raises(SlayerError) as ei:
            await engine.execute(PUSHED, dry_run=True)
        assert not isinstance(ei.value, ForcedFilterError)
        message = str(ei.value)
        assert "25.4" in message
        assert "channel" in message

    async def test_unknown_version_fails_closed(self, tmp_path, monkeypatch):
        _version_probe(monkeypatch, None)
        engine = await _ch_engine(tmp_path)
        with pytest.raises(SlayerError) as ei:
            await engine.execute(PUSHED, dry_run=True)
        assert "25.4" in str(ei.value)

    async def test_execute_path_is_gated_too(self, tmp_path, monkeypatch):
        _version_probe(monkeypatch, "25.3.1.100")
        engine = await _ch_engine(tmp_path)
        with pytest.raises(SlayerError) as ei:
            await engine.execute(PUSHED)
        assert "25.4" in str(ei.value)

    async def test_explain_path_is_gated_too(self, tmp_path, monkeypatch):
        _version_probe(monkeypatch, "25.3.1.100")
        engine = await _ch_engine(tmp_path)
        with pytest.raises(SlayerError) as ei:
            await engine.execute(PUSHED, explain=True)
        assert "25.4" in str(ei.value)

    async def test_supported_version_attaches_the_setting(
        self, tmp_path, monkeypatch,
    ):
        _version_probe(monkeypatch, "25.4.1.100")
        engine = await _ch_engine(tmp_path)
        resp = await engine.execute(PUSHED, dry_run=True)
        assert "EXISTS" in resp.sql.upper()
        assert resp.sql.count("SETTINGS") == 1
        parsed = sqlglot.parse_one(resp.sql, dialect="clickhouse")
        settings = parsed.args.get("settings") or []
        assert any(_SETTING in s.sql() for s in settings)

    async def test_inline_query_needs_no_probe_or_setting(
        self, tmp_path, monkeypatch,
    ):
        """No semi-join in the plan: no version probe, no setting."""
        calls = _version_probe(monkeypatch, "25.4.1.100")
        engine = await _ch_engine(tmp_path)
        resp = await engine.execute(
            q(dimensions=["customers.tier"], measures=[CM],
              filters=["customers.plans.level = 'basic'"]),
            dry_run=True,
        )
        assert _SETTING not in resp.sql
        assert calls["n"] == 0

    async def test_probe_result_is_cached_across_queries(
        self, tmp_path, monkeypatch,
    ):
        calls = _version_probe(monkeypatch, "25.4.1.100")
        engine = await _ch_engine(tmp_path)
        await engine.execute(PUSHED, dry_run=True)
        await engine.execute(PUSHED, dry_run=True)
        assert calls["n"] == 1


class TestSettingsCoexistence:
    async def test_rls_attachment_keeps_one_settings_clause(
        self, tmp_path, monkeypatch,
    ):
        """Planner-attached setting + RLS join-rule rewrite: one SETTINGS,
        forced to 1, and BOTH correlated EXISTS survive."""
        _version_probe(monkeypatch, "25.4.1.100")
        engine = await _ch_engine(tmp_path)
        resp = await engine.execute(PUSHED, dry_run=True)
        assert _SETTING in resp.sql, "precondition: planner attached the setting"
        n_semi_join_exists = resp.sql.upper().count("EXISTS")
        assert n_semi_join_exists >= 1

        def _boom_probe(*_a, **_kw):
            raise AssertionError("join path never probes columns")

        out = apply_session_policy(
            resp.sql,
            dialect="clickhouse",
            policy=SessionPolicy(ruleset=JoinFilterRuleset(
                table="customers", column="organization_uuid", value="orgA",
                joins=[JoinFilterRule(
                    target_table="orders",
                    join_path=["orders.customer_id = customers.id"],
                )],
            )),
            has_column=_boom_probe,
        )
        assert out.count("SETTINGS") == 1
        assert out.count(_SETTING) == 1
        assert f"{_SETTING} = 1" in out
        # The RLS rewrite adds its EXISTS without eating the planner's.
        assert out.upper().count("EXISTS") > n_semi_join_exists
        assert "organization_uuid" in out

    def test_shared_attach_helper_handles_a_union(self):
        """D7's finalization shares the session-policy attachment helper;
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


class TestOtherDialects:
    async def test_non_clickhouse_emits_no_setting(self):
        sql = await gen(PUSHED, dialect="duckdb")
        assert _SETTING not in sql
