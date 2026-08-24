"""Query-backed backing SQL must length-fit identifiers (DEV-1756 class).

The backing SQL of a query-backed (virtual) model is emitted once and then
referenced by outer queries; an over-limit projection alias inside it gets
silently truncated by bounded backends. The fix threads ``projection_aliases``
through ``_expand_query_backed_model`` and teaches the flat-rename wrapper to
decode fitted names and fit its own output aliases; ``Column.sql`` carries the
fitted flat name while ``Column.name`` stays canonical.
"""
from __future__ import annotations

import tempfile

import sqlglot

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import get_dialect
from slayer.storage.yaml_storage import YAMLStorage

from tests.test_dev1756_identifier_length import (
    _FIT_MARKER_RE,
    _assert_within_limit,
    _nbytes,
)

# 73 chars — over the postgres 63-byte limit.
LONGCOL = "a_very_long_column_name_that_certainly_exceeds_sixty_three_bytes_for_fit"
# 310 chars — over both the T-SQL (128) and BigQuery (300) limits.
LONG310 = "c" * 310

# Pre-change expansion of the under-limit stage — pins "no churn for the
# common case" byte for byte.
GOLDEN_UNDER_LIMIT_PG = (
    'SELECT\n  _stage_inner."orders.status" AS "status"\nFROM (\n  SELECT\n'
    '    orders.status AS "orders.status"\n  FROM orders_t AS orders\n'
    "  GROUP BY\n    orders.status\n) AS _stage_inner"
)


def _long_column(name: str) -> list[Column]:
    return [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name=name, sql="amount", type=DataType.DOUBLE),
    ]


async def _expand(
    *,
    ds_type: str,
    columns: list[Column],
    dimensions: list[str],
) -> tuple[SlayerModel, str]:
    """Expand a one-stage query-backed model; returns (model, backing sql)."""
    tmp = tempfile.TemporaryDirectory()
    try:
        storage = YAMLStorage(base_dir=tmp.name)
        await storage.save_datasource(
            DatasourceConfig(name="ds", type=ds_type, database="db")
        )
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders_t", data_source="ds", columns=columns,
        ))
        engine = SlayerQueryEngine(storage=storage)
        stage = SlayerQuery(source_model="orders", dimensions=dimensions)
        model = SlayerModel(name="qb", source_queries=[stage], data_source="ds")
        expanded = await engine._expand_query_backed_model(
            model=model, outer_vars=None, runtime_kwarg=None,
            dry_run_placeholders=False, _resolving=None,
        )
        assert expanded.sql is not None
        return expanded, expanded.sql
    finally:
        tmp.cleanup()


def _outer_aliases(sql: str, dialect: str) -> list[str]:
    return list(sqlglot.parse_one(sql, dialect=dialect).named_selects)


class TestPremise:
    def test_the_long_names_are_over_their_limits(self) -> None:
        assert _nbytes(LONGCOL) > 63
        assert _nbytes(LONG310) > 300


class TestBackingSqlIsLengthFitted:
    async def test_postgres_backing_sql_within_limit(self) -> None:
        _, sql = await _expand(
            ds_type="postgres", columns=_long_column(LONGCOL), dimensions=[LONGCOL],
        )
        _assert_within_limit(sql, 63)
        assert LONGCOL not in sql

    async def test_postgres_column_bridges_canonical_to_fitted(self) -> None:
        expanded, sql = await _expand(
            ds_type="postgres", columns=_long_column(LONGCOL), dimensions=[LONGCOL],
        )
        col = next(c for c in expanded.columns if c.name == LONGCOL)
        fitted = get_dialect("postgres").fit_alias(LONGCOL)
        assert col.sql == fitted
        assert col.sql != col.name
        assert _nbytes(fitted) <= 63
        assert fitted in _outer_aliases(sql, "postgres")

    async def test_tsql_mangled_and_fitted(self) -> None:
        expanded, sql = await _expand(
            ds_type="mssql", columns=_long_column(LONG310), dimensions=[LONG310],
        )
        _assert_within_limit(sql, 128, dialect="tsql")
        assert LONG310 not in sql
        col = next(c for c in expanded.columns if c.name == LONG310)
        assert col.sql == get_dialect("tsql").fit_alias(LONG310)

    async def test_bigquery_mangled_and_fitted(self) -> None:
        expanded, sql = await _expand(
            ds_type="bigquery", columns=_long_column(LONG310), dimensions=[LONG310],
        )
        _assert_within_limit(sql, 300, dialect="bigquery")
        assert LONG310 not in sql
        col = next(c for c in expanded.columns if c.name == LONG310)
        assert col.sql == get_dialect("bigquery").fit_alias(LONG310)

    async def test_under_limit_expansion_is_byte_identical(self) -> None:
        expanded, sql = await _expand(
            ds_type="postgres",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
            dimensions=["status"],
        )
        assert sql == GOLDEN_UNDER_LIMIT_PG
        assert [(c.name, c.sql) for c in expanded.columns] == [("status", "status")]
        assert not _FIT_MARKER_RE.search(sql)

    async def test_unbounded_dialect_keeps_canonical_aliases(self) -> None:
        expanded, sql = await _expand(
            ds_type="sqlite", columns=_long_column(LONGCOL), dimensions=[LONGCOL],
        )
        assert LONGCOL in sql
        assert not _FIT_MARKER_RE.search(sql)
        col = next(c for c in expanded.columns if c.name == LONGCOL)
        assert col.sql == col.name


class TestOuterQueryThroughVirtualModel:
    async def test_outer_query_sql_within_limit_and_keys_canonical(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        try:
            storage = YAMLStorage(base_dir=tmp.name)
            await storage.save_datasource(
                DatasourceConfig(name="ds", type="postgres", database="db")
            )
            await storage.save_model(SlayerModel(
                name="orders", sql_table="orders_t", data_source="ds",
                columns=_long_column(LONGCOL),
            ))
            stage = SlayerQuery(source_model="orders", dimensions=[LONGCOL])
            await storage.save_model(SlayerModel(
                name="qb", source_queries=[stage], data_source="ds",
            ))
            engine = SlayerQueryEngine(storage=storage)
            resp = await engine.execute(
                query=SlayerQuery(source_model="qb", dimensions=[LONGCOL]),
                dry_run=True,
            )
            assert resp.sql is not None
            _assert_within_limit(resp.sql, 63)
            assert LONGCOL not in resp.sql
            assert f"qb.{LONGCOL}" in resp.columns
        finally:
            tmp.cleanup()
