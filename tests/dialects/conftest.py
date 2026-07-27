"""Shared fixtures and helpers for dialect-emission tests.

DEV-1542 cleanup: the SQLGenerator-surface tests and multi-dialect end-to-end
tests share helpers and an ``orders_model`` fixture. Consolidating them here
keeps each ``tests/dialects/test_*.py`` file focused on the dialect concern.
"""

from __future__ import annotations

import pytest
import sqlglot

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.enrichment import enrich_query
from slayer.sql.generator import SQLGenerator


async def _noop_async(**kw):  # NOSONAR(S7503) — must remain async to match resolver-callback contract
    """Async no-op used as a resolver-callback fixture. Stays ``async`` so
    callers can ``await`` it through the resolver-callback contract; the
    body has no real awaitable work to do, so it just returns None."""
    return None


def _norm(s: str) -> str:
    return " ".join(s.split())


_SQLGLOT_TYPEERROR_DIALECTS = {"bigquery"}


def _assert_valid_sql(sql: str, dialect: str = "postgres") -> None:
    """Assert generated SQL is structurally valid (parses, no nested WITH)."""
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
        assert statements, f"SQL failed to parse:\n{sql}"
        assert len(statements) == 1, f"Expected 1 SQL statement, got {len(statements)}:\n{sql}"
    except TypeError as exc:
        if dialect not in _SQLGLOT_TYPEERROR_DIALECTS:
            raise AssertionError(
                f"sqlglot TypeError while validating {dialect} SQL:\n{sql}"
            ) from exc
        return  # Known sqlglot limitation for this dialect
    # No nested WITH — only one WITH keyword allowed at the start of a line
    with_lines = [line for line in sql.split("\n") if line.strip().upper().startswith("WITH ")]
    assert len(with_lines) <= 1, f"Nested WITH clauses detected:\n{sql}"


async def _generate(
    generator: SQLGenerator,
    query: SlayerQuery,
    model: SlayerModel,
) -> str:
    """Helper: enrich a query against a model, then generate SQL."""
    enriched = await enrich_query(
        query=query,
        model=model,
        resolve_dimension_via_joins=_noop_async,
        resolve_cross_model_measure=_noop_async,
        resolve_join_target=_noop_async,
    )
    sql = generator.generate(enriched=enriched)
    _assert_valid_sql(sql, dialect=generator.dialect)
    return sql


@pytest.fixture
def orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="revenue", sql="amount", type=DataType.DOUBLE),
            Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
        ],
    )
