"""Consumers of a finished statement read its tree: result columns, cache tables, the ClickHouse refusal.

Spec: sql/statement-assembly — "Result columns and cache tables match the rendered statement",
"ClickHouse list refusal is decided from the statement".
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Optional

import pytest
import sqlglot

from slayer.core.errors import SlayerError
from slayer.core.models import DatasourceConfig
from slayer.core.query import SlayerQuery
from slayer.engine.cache import CacheConfig, NormalizedTable, QueryCache
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.database.client import SlayerSQLClient
from slayer.sql.dialects import SqlDialect
from slayer.sql.dialects.base import ServerProfile
from slayer.sql.dialects.clickhouse import ClickhouseDialect
from slayer.sql.physical_tables import physical_tables
from tests._dev2120_fixtures import emit_contractors
from tests._dev2137_fixtures import all_models, measure, query
from tests._dev2189_fixtures import (
    EMITTED_DIALECTS,
    POLICIES,
    SHAPES,
    correlating_profile,
    emission_engine,
    file_engine,
    parsed_query,
    shape_supported,
)

DIALECTS = ("sqlite", "duckdb", "postgres", *EMITTED_DIALECTS)


@asynccontextmanager
async def _engine(dialect: str) -> AsyncGenerator[tuple[SlayerQueryEngine, DatasourceConfig]]:
    if dialect in ("sqlite", "duckdb"):
        async with file_engine(dialect) as pair:
            yield pair
    else:
        async with emission_engine(dialect) as pair:
            yield pair


async def _prepare(engine: SlayerQueryEngine, payload: Any):
    main, named, ds, chain = await engine._normalize_input(payload, runtime_kwarg={}, prefer_data_source=None)
    return await engine._prepare_pipeline(
        query=main, named_queries=named, runtime_kwarg={}, prefer_data_source=ds, splice_chain=chain,
    )


def _tables_of(sql: str, *, dialect: str) -> set[NormalizedTable]:
    tree = sqlglot.parse_one(sql, read=dialect)
    return {QueryCache._normalize_table_expr(t, dialect) for t in physical_tables(tree)}


# --------------------------------------------------------------------------- #
# Cache tables
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("policy", [None, "join_multi_hop"])
@pytest.mark.parametrize("shape", sorted(SHAPES))
@pytest.mark.parametrize("dialect", DIALECTS)
async def test_cache_tables_are_the_executed_statements_physical_tables(
    dialect: str, shape: str, policy: Optional[str], monkeypatch: pytest.MonkeyPatch,
) -> None:
    if not shape_supported(shape=shape, dialect=dialect):
        pytest.skip(f"{dialect} has no native lists")
    monkeypatch.setattr(SlayerSQLClient, "server_profile", correlating_profile)
    async with _engine(dialect) as (base, _):
        engine = SlayerQueryEngine(storage=base.storage, policy=POLICIES[policy] if policy else None)
        try:
            prepared = await _prepare(engine, SHAPES[shape])
        finally:
            engine.close()
    assert set(prepared.tables) == _tables_of(prepared.sql, dialect=prepared.dialect)


async def test_policy_hop_tables_count_for_refresh_keys() -> None:
    async with file_engine("sqlite") as (base, _):
        engine = SlayerQueryEngine(
            storage=base.storage, policy=POLICIES["join_one_hop"],
            cache_config=CacheConfig(refresh_keys=(("customers", "COUNT(*)"), ("regions", "COUNT(*)"))),
        )
        try:
            await engine.execute(SHAPES["plain"], cache=True)
            (entry,) = (await engine._cache.snapshot()).values()
        finally:
            engine.close()
    assert entry.applicable == [("customers", "COUNT(*)")]
    assert [v.value for v in entry.refresh_key_values] == [2]


# --------------------------------------------------------------------------- #
# Result columns
# --------------------------------------------------------------------------- #
JOINED_KEYS = ["orders.customers.name", "orders.customers.regions.name", "orders.amount_sum"]


@pytest.mark.parametrize("policy", [None, "join_multi_hop"])
@pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
async def test_mangled_result_names_come_back_canonical(dialect: str, policy: Optional[str]) -> None:
    async with emission_engine(dialect) as (base, _):
        engine = SlayerQueryEngine(storage=base.storage, policy=POLICIES[policy] if policy else None)
        try:
            response = await engine.execute(SHAPES["joined_dims"], dry_run=True)
        finally:
            engine.close()
    assert response.sql is not None
    assert parsed_query(response.sql, dialect=dialect).named_selects != JOINED_KEYS
    assert response.columns == JOINED_KEYS


@pytest.mark.parametrize("dialect", ["bigquery", "tsql"])
@pytest.mark.parametrize("shape", sorted(SHAPES))
async def test_result_columns_match_an_unmangling_dialect(dialect: str, shape: str) -> None:
    if not shape_supported(shape=shape, dialect=dialect):
        pytest.skip(f"{dialect} has no native lists")
    async with emission_engine("postgres") as (engine, _):
        canonical = (await engine.execute(SHAPES[shape], dry_run=True)).columns
    async with emission_engine(dialect) as (engine, _):
        columns = (await engine.execute(SHAPES[shape], dry_run=True)).columns
    assert columns == canonical


LONG = "a" * 70


@pytest.mark.parametrize("policy", [None, "join_multi_hop"])
async def test_length_fitted_result_names_come_back_canonical(policy: Optional[str]) -> None:
    long_query = SlayerQuery.model_validate({
        "source_model": "orders", "dimensions": ["status"], "measures": [{"formula": "sum(amount)", "name": LONG}],
    })
    async with emission_engine("postgres") as (base, _):
        engine = SlayerQueryEngine(storage=base.storage, policy=POLICIES[policy] if policy else None)
        try:
            response = await engine.execute(long_query, dry_run=True)
        finally:
            engine.close()
    assert response.sql is not None
    emitted = parsed_query(response.sql, dialect="postgres").named_selects
    assert all(len(name.encode()) <= 63 for name in emitted)
    assert response.columns == ["orders.status", f"orders.{LONG}"]


async def test_executed_rows_carry_the_canonical_keys_in_projection_order() -> None:
    async with file_engine("duckdb") as (base, _):
        engine = SlayerQueryEngine(storage=base.storage, policy=POLICIES["join_multi_hop"])
        try:
            response = await engine.execute(SHAPES["joined_dims"])
        finally:
            engine.close()
    assert [list(row) for row in response.data] == [JOINED_KEYS] * len(response.data)


# --------------------------------------------------------------------------- #
# The ClickHouse refusal
# --------------------------------------------------------------------------- #
def _profile(version: tuple[int, int]):
    async def _probe(_self: Any) -> ServerProfile:
        return ServerProfile(version=version, readonly=0, correlated_subqueries=None)
    return _probe


#: Unnests ``skills`` and filters through a correlated semi-join in the same SELECT.
UNNEST_BESIDE_SEMI_JOIN = query(dimensions=["team"], filters=["assignments.id = 1"],
                                measures=[measure("count(skills.*)", "n")])


async def test_unnest_beside_a_semi_join_is_refused_before_25_8(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SlayerSQLClient, "server_profile", _profile((25, 4)))
    models = all_models()
    with pytest.raises(SlayerError, match="25.8") as exc:
        await emit_contractors(UNNEST_BESIDE_SEMI_JOIN, dialect="clickhouse", models=models)
    assert "ARRAY JOIN" in str(exc.value)


async def test_unnest_beside_a_semi_join_runs_from_25_8(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(SlayerSQLClient, "server_profile", _profile((25, 8)))
    assert await emit_contractors(UNNEST_BESIDE_SEMI_JOIN, dialect="clickhouse", models=all_models())


_EXISTS = "EXISTS(SELECT 1 FROM a WHERE a.c = p.id)"
_ARRAY_JOIN_BELOW_EXISTS = f"SELECT x.value FROM (SELECT s.value FROM p ARRAY JOIN p.l AS s) AS x WHERE {_EXISTS}"


def test_unnest_and_semi_join_in_different_selects_run_from_25_4() -> None:
    statement = sqlglot.parse_one(_ARRAY_JOIN_BELOW_EXISTS, read="clickhouse")
    profile = ServerProfile(version=(25, 4), readonly=0, correlated_subqueries=None)
    assert ClickhouseDialect().correlated_subquery_refusal(profile, statement=statement) is None


@pytest.mark.parametrize("dialect", [SqlDialect(), ClickhouseDialect()], ids=["default", "clickhouse"])
def test_the_refusal_hook_takes_the_statement_as_a_required_keyword(dialect: SqlDialect) -> None:
    profile = ServerProfile()
    with pytest.raises(TypeError):
        dialect.correlated_subquery_refusal(profile)  # type: ignore[call-arg]
