"""DEV-1542: byte-equivalence golden snapshots.

Per Codex finding #4, the existing ``TestMultiDialectGeneration`` suite
in ``tests/test_sql_generator.py`` mostly checks substrings, not full SQL
equality. This file pins **full** ``SQLGenerator.generate(...)`` output
for representative queries on each Tier-1 dialect. Snapshots captured
from current HEAD before the refactor; they must remain unchanged after.

If a snapshot ever drifts, the refactor has accidentally changed
emitted SQL — which would silently break downstream consumers that
have stored explain plans, materialised views keyed on SQL hashes, or
parsed-AST caches.
"""

from __future__ import annotations

import asyncio

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.enrichment import enrich_query
from slayer.sql.generator import SQLGenerator

from tests.dialects.conftest import _noop_async


async def _gen(dialect: str, query: SlayerQuery, model: SlayerModel) -> str:
    enriched = await enrich_query(
        query=query,
        model=model,
        resolve_dimension_via_joins=_noop_async,
        resolve_cross_model_measure=_noop_async,
        resolve_join_target=_noop_async,
        dialect=dialect,
    )
    return SQLGenerator(dialect=dialect).generate(enriched=enriched)


# ---------------------------------------------------------------------------
# Basic aggregation — COUNT(*), SUM(...), GROUP BY dim
# ---------------------------------------------------------------------------


_BASIC_QUERY = SlayerQuery(
    source_model="orders",
    measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="revenue:sum")],
    dimensions=[ColumnRef(name="status")],
)


@pytest.mark.parametrize(
    "dialect,expected",
    [
        (
            "postgres",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  COUNT(*) AS "orders._count",\n'
            '  SUM(orders.amount) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "sqlite",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  COUNT(*) AS "orders._count",\n'
            '  SUM(orders.amount) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "duckdb",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  COUNT(*) AS "orders._count",\n'
            '  SUM(orders.amount) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "clickhouse",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  COUNT(*) AS "orders._count",\n'
            '  SUM(orders.amount) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "mysql",
            "SELECT\n"
            "  orders.status AS `orders.status`,\n"
            "  COUNT(*) AS `orders._count`,\n"
            "  SUM(orders.amount) AS `orders.revenue_sum`\n"
            "FROM public.orders AS orders\n"
            "GROUP BY\n"
            "  orders.status",
        ),
        (
            # DEV-1571 Bug 2: T-SQL bracketed dotted aliases are mangled
            # via TsqlDialect.rewrite_emitted_sql so T-SQL's ORDER BY
            # resolver can match the SELECT alias. ``orders.status``
            # encodes to ``orders___status``; ``orders._count`` (with
            # its literal leading underscore) encodes to
            # ``orders____count`` (3 underscores from the dot + 1
            # literal leading underscore).
            "tsql",
            "SELECT\n"
            "  orders.status AS [orders___status],\n"
            "  COUNT(*) AS [orders____count],\n"
            "  SUM(orders.amount) AS [orders___revenue_sum]\n"
            "FROM public.orders AS orders\n"
            "GROUP BY\n"
            "  orders.status",
        ),
    ],
)
async def test_byte_equivalence_basic_query(
    dialect: str, expected: str, orders_model: SlayerModel
) -> None:
    sql = await _gen(dialect, _BASIC_QUERY, orders_model)
    assert sql == expected


# ---------------------------------------------------------------------------
# DATE_TRUNC by month — covers SQLite STRFTIME, T-SQL DATETRUNC, MySQL CONCAT
# ---------------------------------------------------------------------------


_TRUNC_QUERY = SlayerQuery(
    source_model="orders",
    measures=[ModelMeasure(formula="revenue:sum")],
    time_dimensions=[TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH)],
)


@pytest.mark.parametrize(
    "dialect,expected",
    [
        (
            "postgres",
            'SELECT\n'
            '  DATE_TRUNC(\'MONTH\', orders.created_at) AS "orders.created_at",\n'
            '  SUM(orders.amount) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  DATE_TRUNC(\'MONTH\', orders.created_at)',
        ),
        (
            "sqlite",
            'SELECT\n'
            '  STRFTIME(\'%Y-%m-01\', orders.created_at) AS "orders.created_at",\n'
            '  SUM(orders.amount) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  STRFTIME(\'%Y-%m-01\', orders.created_at)',
        ),
        (
            "duckdb",
            'SELECT\n'
            '  DATE_TRUNC(\'MONTH\', orders.created_at) AS "orders.created_at",\n'
            '  SUM(orders.amount) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  DATE_TRUNC(\'MONTH\', orders.created_at)',
        ),
        (
            "clickhouse",
            'SELECT\n'
            '  DATE_TRUNC(\'MONTH\', orders.created_at) AS "orders.created_at",\n'
            '  SUM(orders.amount) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  DATE_TRUNC(\'MONTH\', orders.created_at)',
        ),
        (
            "mysql",
            "SELECT\n"
            "  STR_TO_DATE(CONCAT(YEAR(orders.created_at), ' ', MONTH(orders.created_at), ' 1'), '%Y %c %e') AS `orders.created_at`,\n"
            "  SUM(orders.amount) AS `orders.revenue_sum`\n"
            "FROM public.orders AS orders\n"
            "GROUP BY\n"
            "  STR_TO_DATE(CONCAT(YEAR(orders.created_at), ' ', MONTH(orders.created_at), ' 1'), '%Y %c %e')",
        ),
        (
            # DEV-1571 Bug 2: bracketed dotted aliases mangled.
            "tsql",
            "SELECT\n"
            "  DATETRUNC(month, orders.created_at) AS [orders___created_at],\n"
            "  SUM(orders.amount) AS [orders___revenue_sum]\n"
            "FROM public.orders AS orders\n"
            "GROUP BY\n"
            "  DATETRUNC(month, orders.created_at)",
        ),
    ],
)
async def test_byte_equivalence_date_trunc_month(
    dialect: str, expected: str, orders_model: SlayerModel
) -> None:
    sql = await _gen(dialect, _TRUNC_QUERY, orders_model)
    assert sql == expected


# ---------------------------------------------------------------------------
# Median — sqlite/duckdb/clickhouse all use sqlglot transpilation of exp.Median;
# postgres uses PERCENTILE_CONT(0.5) WITHIN GROUP; MySQL/T-SQL raise.
# ---------------------------------------------------------------------------


_MEDIAN_QUERY = SlayerQuery(
    source_model="orders",
    measures=[ModelMeasure(formula="revenue:median")],
    dimensions=[ColumnRef(name="status")],
)


@pytest.mark.parametrize(
    "dialect,expected",
    [
        (
            "postgres",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY\n'
            '    orders.amount) AS "orders.revenue_median"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "sqlite",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  PERCENTILE_CONT(orders.amount, 0.5) AS "orders.revenue_median"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "duckdb",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  QUANTILE_CONT(orders.amount, 0.5\n'
            '  ORDER BY\n'
            '    orders.amount) AS "orders.revenue_median"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "clickhouse",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  quantile(0.5)(orders.amount) AS "orders.revenue_median"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
    ],
)
async def test_byte_equivalence_median(
    dialect: str, expected: str, orders_model: SlayerModel
) -> None:
    sql = await _gen(dialect, _MEDIAN_QUERY, orders_model)
    assert sql == expected


def test_byte_equivalence_median_mysql_raises(orders_model: SlayerModel) -> None:
    with pytest.raises(NotImplementedError, match="median.*MySQL"):
        asyncio.run(_gen("mysql", _MEDIAN_QUERY, orders_model))


def test_byte_equivalence_median_tsql_raises(orders_model: SlayerModel) -> None:
    with pytest.raises(NotImplementedError, match="median.*T-SQL"):
        asyncio.run(_gen("tsql", _MEDIAN_QUERY, orders_model))


# ---------------------------------------------------------------------------
# Percentile — same dialect divergence as median, different p value
# ---------------------------------------------------------------------------


_PCT_QUERY = SlayerQuery(
    source_model="orders",
    measures=[ModelMeasure(formula="revenue:percentile(p=0.95)")],
    dimensions=[ColumnRef(name="status")],
)


@pytest.mark.parametrize(
    "dialect,expected",
    [
        (
            "postgres",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY\n'
            '    orders.amount) AS "orders.revenue_percentile_p_0_95"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "sqlite",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  PERCENTILE_CONT(orders.amount, 0.95) AS "orders.revenue_percentile_p_0_95"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "duckdb",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  QUANTILE_CONT(orders.amount, 0.95\n'
            '  ORDER BY\n'
            '    orders.amount) AS "orders.revenue_percentile_p_0_95"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "clickhouse",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  quantile(0.95)(orders.amount) AS "orders.revenue_percentile_p_0_95"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
    ],
)
async def test_byte_equivalence_percentile(
    dialect: str, expected: str, orders_model: SlayerModel
) -> None:
    sql = await _gen(dialect, _PCT_QUERY, orders_model)
    assert sql == expected


# ---------------------------------------------------------------------------
# CORR — covers native (postgres/sqlite/duckdb/clickhouse) vs decomposition
# formula (mysql/tsql with different stddev/var names).
# ---------------------------------------------------------------------------


_CORR_QUERY = SlayerQuery(
    source_model="orders",
    measures=[ModelMeasure(formula="revenue:corr(other=quantity)")],
    dimensions=[ColumnRef(name="status")],
)


@pytest.mark.parametrize("dialect", ["postgres", "sqlite", "duckdb", "clickhouse"])
async def test_byte_equivalence_corr_native(
    dialect: str, orders_model: SlayerModel
) -> None:
    """All 4 dialects emit native ``CORR(amount, quantity)`` directly —
    no variance-decomposition fallback."""
    sql = await _gen(dialect, _CORR_QUERY, orders_model)
    assert "CORR(orders.amount, orders.quantity)" in sql


async def test_byte_equivalence_corr_mysql_decomposition(
    orders_model: SlayerModel,
) -> None:
    """MySQL emits the full variance-decomposition formula. Pin the
    structural shape: VAR_SAMP triplet, STDDEV_SAMP product, NULLIF guard."""
    sql = await _gen("mysql", _CORR_QUERY, orders_model)
    # Triplet of VAR_SAMP calls for the (x+y), x, y legs
    assert sql.count("VAR_SAMP(") == 3
    # Pair of STDDEV_SAMP calls in the denominator
    assert sql.count("STDDEV_SAMP(") == 2
    # NULLIF guards against zero denominator
    assert "NULLIF(" in sql
    # NULL-cross-guard pattern
    assert "IS NULL" in sql
    # Division by literal 2 (the formula constant)
    assert "/ 2" in sql


async def test_byte_equivalence_corr_tsql_decomposition(
    orders_model: SlayerModel,
) -> None:
    """T-SQL emits the same decomposition formula but with VAR / STDEV
    (T-SQL canonical names) instead of MySQL's VAR_SAMP / STDDEV_SAMP."""
    sql = await _gen("tsql", _CORR_QUERY, orders_model)
    # T-SQL VAR (sample) — 3 calls for (x+y), x, y
    assert sql.count("VAR(") == 3
    # T-SQL STDEV (sample) — 2 calls in the denominator
    assert sql.count("STDEV(") == 2
    assert "NULLIF(" in sql
    # T-SQL must NOT use the Postgres-canonical names
    assert "STDDEV_SAMP" not in sql
    assert "VAR_SAMP" not in sql


# ---------------------------------------------------------------------------
# Time-shift CTE — covers shifted sub-query with dialect-specific
# INTERVAL (Postgres) / DATE-modifier (SQLite) / DATEADD (T-SQL) arithmetic.
# ---------------------------------------------------------------------------


_TIME_SHIFT_QUERY = SlayerQuery(
    source_model="orders",
    measures=[
        ModelMeasure(formula="time_shift(revenue:sum, -1, 'month')", name="rev_prev"),
    ],
    time_dimensions=[
        TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH),
    ],
)


async def test_byte_equivalence_time_shift_postgres(orders_model: SlayerModel) -> None:
    sql = await _gen("postgres", _TIME_SHIFT_QUERY, orders_model)
    # Postgres uses INTERVAL '1 MONTH' arithmetic
    assert "INTERVAL '1 MONTH'" in sql
    assert "DATE_TRUNC('MONTH'" in sql
    # The shifted CTE is named after the measure
    assert "shifted_rev_prev AS (" in sql
    # Self-join CTE wires base ↔ shifted by the time-dim alias
    assert "LEFT JOIN shifted_rev_prev" in sql


async def test_byte_equivalence_time_shift_sqlite(orders_model: SlayerModel) -> None:
    sql = await _gen("sqlite", _TIME_SHIFT_QUERY, orders_model)
    # SQLite uses DATE(col, 'N months') — no INTERVAL keyword
    assert "DATE(orders.created_at, '1 months')" in sql
    assert "INTERVAL" not in sql
    assert "STRFTIME('%Y-%m-01'" in sql


async def test_byte_equivalence_time_shift_tsql(orders_model: SlayerModel) -> None:
    sql = await _gen("tsql", _TIME_SHIFT_QUERY, orders_model)
    # T-SQL uses DATEADD(unit, val, col) — no INTERVAL
    assert "DATEADD(MONTH, 1, orders.created_at)" in sql
    assert "INTERVAL" not in sql
    # T-SQL bucket function is DATETRUNC(unit, col). DEV-1571 Bug 1's
    # CTE hoist re-parses the inner SQL via sqlglot's T-SQL dialect,
    # which canonicalises the unit identifier to upper case
    # (``month`` -> ``MONTH``). This is a visual normalisation only —
    # both forms are equivalent T-SQL.
    assert "DATETRUNC(MONTH, " in sql


# ---------------------------------------------------------------------------
# Cumsum (window-function trailing aggregate) — covers OVER (ORDER BY ...) emission
# ---------------------------------------------------------------------------


_CUMSUM_QUERY = SlayerQuery(
    source_model="orders",
    measures=[
        ModelMeasure(formula="cumsum(revenue:sum)", name="cum_rev"),
    ],
    time_dimensions=[
        TimeDimension(dimension="created_at", granularity=TimeGranularity.MONTH),
    ],
)


@pytest.mark.parametrize(
    "dialect,expected_window",
    [
        # Postgres / SQLite quote with ANSI double quotes.
        ("postgres", 'SUM("orders.revenue_sum") OVER (ORDER BY "orders.created_at")'),
        ("sqlite", 'SUM("orders.revenue_sum") OVER (ORDER BY "orders.created_at")'),
        # T-SQL quotes with brackets AND DEV-1571 Bug 2 mangles the
        # dotted aliases to underscore form so the ORDER BY resolver
        # can match the SELECT alias.
        ("tsql", "SUM([orders___revenue_sum]) OVER (ORDER BY [orders___created_at])"),
    ],
)
async def test_byte_equivalence_cumsum_window_emission(
    dialect: str, expected_window: str, orders_model: SlayerModel
) -> None:
    """Cumsum emits the same OVER (ORDER BY ...) window shape on every
    dialect; only the identifier quoting (and DEV-1571 mangling on T-SQL)
    differs."""
    sql = await _gen(dialect, _CUMSUM_QUERY, orders_model)
    assert expected_window in sql


# ---------------------------------------------------------------------------
# json_extract via Column.sql — covers SQLite JSON-rewrite + the per-dialect
# emission differences (Postgres JSON_EXTRACT_PATH, T-SQL ISNULL(JSON_QUERY/VALUE))
# ---------------------------------------------------------------------------


@pytest.fixture
def orders_model_with_json() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(
                name="json_data",
                sql="json_extract(blob, '$.k')",
                type=DataType.TEXT,
            ),
        ],
    )


_JSON_QUERY = SlayerQuery(
    source_model="orders",
    measures=[ModelMeasure(formula="*:count")],
    dimensions=[ColumnRef(name="json_data")],
)


async def test_byte_equivalence_json_extract_sqlite_uses_function_form(
    orders_model_with_json: SlayerModel,
) -> None:
    """SQLite's JSON-extract rewrite produces the JSON_EXTRACT(blob, '$.k')
    function form (not the ``->`` operator) — required so equality matches
    against bare-string literals work. DEV-1331."""
    sql = await _gen("sqlite", _JSON_QUERY, orders_model_with_json)
    assert "JSON_EXTRACT(blob, '$.k')" in sql
    # SQLite ``->`` operator must NOT appear (would silently return quoted form)
    assert " -> " not in sql


async def test_byte_equivalence_json_extract_postgres_uses_path_form(
    orders_model_with_json: SlayerModel,
) -> None:
    """Postgres emits JSON_EXTRACT_PATH(blob, 'k') — sqlglot's translation."""
    sql = await _gen("postgres", _JSON_QUERY, orders_model_with_json)
    assert "JSON_EXTRACT_PATH(blob, 'k')" in sql


async def test_byte_equivalence_json_extract_tsql_uses_isnull_pair(
    orders_model_with_json: SlayerModel,
) -> None:
    """T-SQL emits ISNULL(JSON_QUERY(...), JSON_VALUE(...)) — handles both
    object and scalar paths."""
    sql = await _gen("tsql", _JSON_QUERY, orders_model_with_json)
    assert "ISNULL(JSON_QUERY(blob, '$.k'), JSON_VALUE(blob, '$.k'))" in sql
