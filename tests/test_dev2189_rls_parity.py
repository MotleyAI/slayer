"""Forced filters rewrite the statement tree before its single render.

Spec: sql/forced-filters — "Forced filters rewrite the statement before its single render".
The reference is the previous rewrite: the unscoped SQL, parsed, scoped and rendered.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any, Optional

import pytest
import sqlglot
from pytest_postgresql import factories

from slayer.core.errors import ForcedFilterError
from slayer.core.models import DatasourceConfig
from slayer.core.policy import SessionPolicy
from slayer.engine.cache import CacheConfig
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from slayer.sql.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2189_fixtures import (
    EMITTED_DIALECTS,
    ITEMS_RULE,
    ORDERS_RULE,
    POLICIES,
    SHAPES,
    ClientSpy,
    correlating_profile,
    emission_engine,
    file_engine,
    normalized,
    reference_scoped_sql,
    refusal,
    row_values,
    save_models,
    save_query_backed,
    seed_statements,
    shape_supported,
    stub_column_present,
    stub_has_column,
)

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

CASES = [
    pytest.param(policy, shape, id=f"{policy}-{shape}")
    for policy in POLICIES
    for shape in SHAPES
]


async def _assert_parity(
    *, engine: SlayerQueryEngine, ds: DatasourceConfig, dialect: str, policy: str, shape: str,
    monkeypatch: pytest.MonkeyPatch, executed: bool,
) -> None:
    """The scoped SQL equals the reference rewrite of the unscoped SQL; on ``executed`` backends, so do the rows."""
    if not shape_supported(shape=shape, dialect=dialect):
        pytest.skip(f"{dialect} has no native lists")
    query = SHAPES[shape]
    unscoped = (await engine.execute(query, dry_run=True)).sql
    assert unscoped is not None
    scoped = SlayerQueryEngine(storage=engine.storage, policy=POLICIES[policy])
    if executed:
        def has_column(table, column) -> Optional[bool]:
            return scoped._column_present(datasource=ds, scoped_table=table, column=column)
    else:
        monkeypatch.setattr(scoped, "_column_present", stub_column_present)
        has_column = stub_has_column
    try:
        try:
            expected = reference_scoped_sql(sql=unscoped, dialect=dialect, policy=POLICIES[policy],
                                            has_column=has_column)
        except ForcedFilterError as exc:
            with pytest.raises(ForcedFilterError) as got:
                await scoped.execute(query, dry_run=True)
            assert refusal(got.value) == refusal(exc)
            return
        response = await scoped.execute(query, dry_run=True)
        assert response.sql is not None
        assert normalized(response.sql, dialect=dialect) == normalized(expected, dialect=dialect)
        if executed:
            rows = (await scoped.execute(query)).data
            client = SlayerSQLClient(datasource=ds)
            try:
                reference_rows = (await client.execute(sql=expected)).rows
            finally:
                await client.aclose()
            assert row_values(rows) == row_values(reference_rows)
    finally:
        scoped.close()


@pytest.mark.parametrize(("policy", "shape"), CASES)
@pytest.mark.parametrize("backend", ["sqlite", "duckdb"])
async def test_scoped_sql_and_rows_match_the_previous_rewrite(
    backend: str, policy: str, shape: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with file_engine(backend) as (engine, ds):
        await _assert_parity(engine=engine, ds=ds, dialect=backend, policy=policy, shape=shape,
                             monkeypatch=monkeypatch, executed=True)


@pytest.fixture
async def pg_engine(postgresql, tmp_path) -> AsyncGenerator[tuple[SlayerQueryEngine, DatasourceConfig]]:
    cur = postgresql.cursor()
    for stmt in seed_statements("postgres"):
        cur.execute(stmt)
    postgresql.commit()
    info = postgresql.info
    ds = DatasourceConfig(name="test", type="postgres", host=info.host, port=info.port,
                          database=info.dbname, username=info.user, password="")
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(ds)
    await save_models(storage)
    engine = SlayerQueryEngine(storage=storage)
    try:
        await save_query_backed(engine)
        yield engine, ds
    finally:
        engine.close()
        engine_factory.invalidate_engine(ds)


@pytest.mark.parametrize(("policy", "shape"), CASES)
async def test_scoped_sql_and_rows_match_the_previous_rewrite_on_postgres(
    pg_engine: tuple[SlayerQueryEngine, DatasourceConfig], policy: str, shape: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, ds = pg_engine
    await _assert_parity(engine=engine, ds=ds, dialect="postgres", policy=policy, shape=shape,
                         monkeypatch=monkeypatch, executed=True)


@pytest.mark.parametrize(("policy", "shape"), CASES)
@pytest.mark.parametrize("dialect", EMITTED_DIALECTS)
async def test_scoped_sql_matches_the_previous_rewrite_on_emission(
    dialect: str, policy: str, shape: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(SlayerSQLClient, "server_profile", correlating_profile)
    async with emission_engine(dialect) as (engine, ds):
        await _assert_parity(engine=engine, ds=ds, dialect=dialect, policy=policy, shape=shape,
                             monkeypatch=monkeypatch, executed=False)


@pytest.mark.parametrize("policy", ["column_pass", "join_multi_hop"])
@pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
async def test_dotted_result_names_survive_scoping(
    dialect: str, policy: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with emission_engine(dialect) as (engine, _):
        unscoped = await engine.execute(SHAPES["joined_dims"], dry_run=True)
        scoped = SlayerQueryEngine(storage=engine.storage, policy=POLICIES[policy])
        monkeypatch.setattr(scoped, "_column_present", stub_column_present)
        try:
            response = await scoped.execute(SHAPES["joined_dims"], dry_run=True)
        finally:
            scoped.close()
    assert response.sql is not None and unscoped.sql is not None
    emitted = sqlglot.parse_one(response.sql, read=dialect).named_selects
    assert emitted == sqlglot.parse_one(unscoped.sql, read=dialect).named_selects
    assert not any("." in name for name in emitted)
    assert response.columns == unscoped.columns
    assert all("." in name for name in response.columns)


# --------------------------------------------------------------------------- #
# Fail closed, on every statement kind the policy scopes
# --------------------------------------------------------------------------- #
def _join_policy(*, joins: list, whitelist: list[str]) -> SessionPolicy:
    ruleset = POLICIES["join_multi_hop"].ruleset.model_copy(update={"joins": tuple(joins), "whitelist": tuple(whitelist)})
    return POLICIES["join_multi_hop"].model_copy(update={"ruleset": ruleset})


def _never_confirmed(**_kwargs: Any) -> None:
    return None


def _never_present(**_kwargs: Any) -> bool:
    return False


#: refusal kind → (policy, column-presence stub or ``None`` for a join ruleset).
REFUSALS: dict[str, tuple[SessionPolicy, Any]] = {
    "unconfirmable_column": (POLICIES["column_pass"], _never_confirmed),
    "blocked_column": (POLICIES["column_block"], _never_present),
    "uncovered_table": (_join_policy(joins=[], whitelist=[]), None),
    "invalid_join_path": (_join_policy(
        joins=[ORDERS_RULE.model_copy(update={"join_path": ("orders.customer_id = regions.id",)}), ITEMS_RULE],
        whitelist=["regions"],
    ), None),
}


async def _scoped_engine(base: SlayerQueryEngine, kind: str, monkeypatch: pytest.MonkeyPatch) -> SlayerQueryEngine:
    policy, presence = REFUSALS[kind]
    engine = SlayerQueryEngine(storage=base.storage, policy=policy,
                               cache_config=CacheConfig(refresh_keys=[("orders", "COUNT(*)")]))
    if presence is not None:
        monkeypatch.setattr(engine, "_column_present", presence)
    return engine


@pytest.mark.parametrize("mode", [{}, {"dry_run": True}, {"explain": True}, {"cache": True}],
                         ids=["execute", "dry_run", "explain", "cached"])
@pytest.mark.parametrize("kind", sorted(REFUSALS))
async def test_the_data_query_fails_closed(kind: str, mode: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    with ClientSpy().installed(monkeypatch) as spy:
        async with file_engine("sqlite") as (base, _):
            engine = await _scoped_engine(base, kind, monkeypatch)
            try:
                with pytest.raises(ForcedFilterError) as exc:
                    await engine.execute(SHAPES["plain"], **mode)
            finally:
                engine.close()
    assert exc.value.table == "orders"
    assert spy.executed == []


@pytest.mark.parametrize("kind", sorted(REFUSALS))
async def test_the_refresh_key_scan_fails_closed(kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    with ClientSpy().installed(monkeypatch) as spy:
        async with file_engine("sqlite") as (base, ds):
            engine = await _scoped_engine(base, kind, monkeypatch)
            client = SlayerSQLClient(datasource=ds)
            try:
                with pytest.raises(ForcedFilterError):
                    await engine._scan_one_table_values(
                        table="orders", exprs=["COUNT(*)"], dialect="sqlite", datasource=ds, client=client,
                    )
            finally:
                await client.aclose()
                engine.close()
    assert spy.executed == []


async def test_a_refresh_whose_scan_fails_closed_keeps_the_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    confirmed = {"value": True}

    def _presence(**_kwargs: Any) -> Optional[bool]:
        return True if confirmed["value"] else None

    with ClientSpy().installed(monkeypatch) as spy:
        async with file_engine("sqlite") as (base, _):
            engine = SlayerQueryEngine(storage=base.storage, policy=POLICIES["column_pass"],
                                       cache_config=CacheConfig(refresh_keys=[("orders", "COUNT(*)")]))
            monkeypatch.setattr(engine, "_column_present", _presence)
            try:
                await engine.execute(SHAPES["plain"], cache=True)
                sent = len(spy.executed)
                confirmed["value"] = False
                result = await engine.refresh()
            finally:
                engine.close()
    assert [e.phase for e in result.errors] == ["refresh_key_scan"]
    assert "Forced filter" in result.errors[0].message
    assert result.refreshed == [] and result.expired_refreshed == []
    assert len(spy.executed) == sent


@pytest.mark.parametrize("kind", sorted(REFUSALS))
async def test_the_side_statistics_fail_closed(kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    with ClientSpy().installed(monkeypatch) as spy:
        async with file_engine("sqlite") as (base, _):
            engine = await _scoped_engine(base, kind, monkeypatch)
            try:
                with pytest.raises(ForcedFilterError) as exc:
                    await engine.detect_join_cardinality(data_source="test", model="customers")
            finally:
                engine.close()
    refused = exc.value.table
    assert refused is not None
    assert not [sql for sql in spy.executed if refused in sql]


@pytest.mark.parametrize("kind", sorted(REFUSALS))
async def test_the_type_probe_fails_closed_to_no_types(kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    with ClientSpy().installed(monkeypatch) as spy:
        async with file_engine("sqlite") as (base, _):
            engine = await _scoped_engine(base, kind, monkeypatch)
            try:
                types = await engine.get_column_types("orders")
            finally:
                engine.close()
    assert types == {}
    assert spy.probed == []
    assert spy.executed == []


# --------------------------------------------------------------------------- #
# Refresh-key scans and profiling read only the tenant's rows
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("policy", ["column_pass", "join_multi_hop"])
async def test_the_refresh_key_baseline_is_tenant_scoped(policy: str) -> None:
    async with file_engine("sqlite") as (base, _):
        engine = SlayerQueryEngine(storage=base.storage, policy=POLICIES[policy],
                                   cache_config=CacheConfig(refresh_keys=[("orders", "COUNT(*)")]))
        try:
            await engine.execute(SHAPES["plain"], cache=True)
            (entry,) = (await engine._cache.snapshot()).values()
        finally:
            engine.close()
    assert [(v.table, v.value) for v in entry.refresh_key_values] == [("orders", 3)]


@pytest.mark.parametrize("policy", ["column_pass", "join_multi_hop"])
async def test_profiling_reads_only_the_tenants_rows(policy: str) -> None:
    async with file_engine("sqlite") as (base, _):
        engine = SlayerQueryEngine(storage=base.storage, policy=POLICIES[policy])
        try:
            report = await engine.detect_join_cardinality(data_source="test", model="customers")
        finally:
            engine.close()
    (finding,) = [f for f in report.findings if f.target_model == "orders"]
    assert finding.source_side is not None and finding.target_side is not None
    assert (finding.source_side.row_count, finding.target_side.row_count) == (2, 3)
