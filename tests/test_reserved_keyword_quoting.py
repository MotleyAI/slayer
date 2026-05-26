"""Regression tests for BUG-reserved-keyword-identifiers.

A model named after a reserved SQL keyword (``grant``/``order``/``user``/
``select``) must be emitted QUOTED, else (A) the DB rejects the bare keyword
and (B) a re-parsed ``grant."col"`` string falls back to a ``Command`` parse
(logged on the ``sqlglot`` logger). These tests assert the quoted form, a
clean parse, and no ``Command`` fallback warning.
"""

import logging

import pytest
import sqlglot

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.enrichment import enrich_query
from slayer.sql.generator import SQLGenerator

# A representative spread: SQLite (STRFTIME branch), Postgres (the reported
# dialect), DuckDB, Snowflake (case-folds unquoted → UPPER), MySQL, ClickHouse.
DIALECTS = ["postgres", "sqlite", "duckdb", "snowflake", "mysql", "clickhouse"]
RESERVED_NAMES = ["grant", "order", "user", "select"]

_COMMAND_FALLBACK = "Falling back to parsing as a 'Command'"


async def _noop(**kw):
    return None


def _q(name: str, dialect: str) -> str:
    """Quoted identifier form for ``name`` in ``dialect`` (tracks its quote char)."""
    return sqlglot.exp.to_identifier(name, quoted=True).sql(dialect=dialect)


def _grant_model(name: str = "grant") -> SlayerModel:
    """Reserved-keyword model with a mixed-case column (mirrors the reported schema)."""
    return SlayerModel(
        name=name,
        sql_table=f'"{name}"',
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="idempotencyKey", sql="idempotencyKey", type=DataType.TEXT),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
        ],
    )


async def _enrich(query: SlayerQuery, model: SlayerModel, dialect: str, **resolvers):
    """Enrich with a real ``resolve_model`` (exercises column-expansion, surface
    B) and the generator's ``dialect`` (mirrors the engine's one-dialect flow)."""
    async def resolve_model(model_name=None, named_queries=None, **kw):
        return resolvers.get("models", {}).get(model_name)

    return await enrich_query(
        query=query,
        model=model,
        resolve_dimension_via_joins=_noop,
        resolve_cross_model_measure=_noop,
        resolve_join_target=resolvers.get("resolve_join_target", _noop),
        resolve_model=resolvers.get("resolve_model", resolve_model),
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("name", RESERVED_NAMES)
async def test_reserved_keyword_model_name_is_quoted(dialect, name, caplog):
    model = _grant_model(name)
    query = SlayerQuery(
        source_model=name,
        measures=[{"formula": "amount:sum"}],
        dimensions=[ColumnRef(name="status")],
    )
    enriched = await _enrich(query, model, dialect, models={name: model})
    with caplog.at_level(logging.WARNING, logger="sqlglot"):
        sql = SQLGenerator(dialect=dialect).generate(enriched=enriched)

    # Surface A: quoted identifier present + a clean parse (no bare keyword).
    assert _q(name, dialect) in sql
    assert sqlglot.parse(sql, dialect=dialect)
    # Surface B: no Command-fallback warning.
    assert not any(_COMMAND_FALLBACK in r.getMessage() for r in caplog.records), [
        r.getMessage() for r in caplog.records
    ]


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_surface_b_mixed_case_qualifier_no_command_fallback(dialect, caplog):
    """Reported reproducer: ``grant`` model + mixed-case ``idempotencyKey`` dim —
    quoted qualifier, no ``grant .`` (space-dot) Command artifact."""
    model = _grant_model()
    query = SlayerQuery(source_model="grant", dimensions=[ColumnRef(name="idempotencyKey")])
    enriched = await _enrich(query, model, dialect, models={"grant": model})
    with caplog.at_level(logging.WARNING, logger="sqlglot"):
        sql = SQLGenerator(dialect=dialect).generate(enriched=enriched)

    assert _q("grant", dialect) in sql
    assert "grant ." not in sql  # Command-misparse spacing artifact
    assert sqlglot.parse(sql, dialect=dialect)
    assert not any(_COMMAND_FALLBACK in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_filter_on_reserved_keyword_model(dialect, caplog):
    model = _grant_model()
    query = SlayerQuery(
        source_model="grant",
        measures=[{"formula": "amount:sum"}],
        dimensions=[ColumnRef(name="status")],
        filters=["status = 'completed'"],
    )
    enriched = await _enrich(query, model, dialect, models={"grant": model})
    with caplog.at_level(logging.WARNING, logger="sqlglot"):
        sql = SQLGenerator(dialect=dialect).generate(enriched=enriched)

    q = _q("grant", dialect)
    assert f"{q}.status" in sql or f"{q}.{_q('status', dialect)}" in sql
    assert sqlglot.parse(sql, dialect=dialect)
    assert not any(_COMMAND_FALLBACK in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_join_from_reserved_keyword_model(dialect, caplog):
    """Joined dimension off a reserved-keyword source: quoted JOIN alias + ON."""
    regions = SlayerModel(
        name="regions",
        sql_table="regions",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="population", sql="population", type=DataType.DOUBLE),
        ],
    )
    grant = _grant_model()
    grant.joins = [ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])]

    async def resolve_join_target(target_model_name=None, named_queries=None, **kw):
        return (regions.sql_table, regions) if target_model_name == "regions" else None

    query = SlayerQuery(
        source_model="grant",
        measures=[{"formula": "amount:sum"}],
        dimensions=[ColumnRef(name="regions.name")],
    )
    enriched = await _enrich(
        query, grant, dialect, models={"grant": grant, "regions": regions},
        resolve_join_target=resolve_join_target,
    )
    with caplog.at_level(logging.WARNING, logger="sqlglot"):
        sql = SQLGenerator(dialect=dialect).generate(enriched=enriched)

    assert _q("grant", dialect) in sql
    assert sqlglot.parse(sql, dialect=dialect)
    assert not any(_COMMAND_FALLBACK in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("dialect", DIALECTS)
async def test_first_last_ranked_subquery_on_reserved_keyword_model(dialect, caplog):
    """Ranked subquery emits ``SELECT "grant".*`` — guards star-projection quoting."""
    model = _grant_model()
    query = SlayerQuery(
        source_model="grant",
        measures=[{"formula": "amount:last"}],
        dimensions=[ColumnRef(name="status")],
        time_dimensions=[
            TimeDimension(dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.DAY)
        ],
    )
    enriched = await _enrich(query, model, dialect, models={"grant": model})
    with caplog.at_level(logging.WARNING, logger="sqlglot"):
        sql = SQLGenerator(dialect=dialect).generate(enriched=enriched)

    assert f'{_q("grant", dialect)}.*' in sql
    assert sqlglot.parse(sql, dialect=dialect)
    assert not any(_COMMAND_FALLBACK in r.getMessage() for r in caplog.records)
