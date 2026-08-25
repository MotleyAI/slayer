"""DEV-1542: byte-equivalence golden snapshots.

Per Codex finding #4, the existing ``TestMultiDialectGeneration`` suite
in ``tests/test_sql_generator.py`` mostly checks substrings, not full SQL
equality. This file pins **full** emitted SQL for representative queries
on each Tier-1 dialect.

Snapshots are captured from the **typed pipeline** (``SlayerQueryEngine``
via :func:`tests._engine_helpers._engine_generate`), which is the sole
supported code path now that the legacy generator stack is gone. They
must remain unchanged unless emission is deliberately altered.

If a snapshot ever drifts, something has accidentally changed emitted
SQL — which would silently break downstream consumers that have stored
explain plans, materialised views keyed on SQL hashes, or parsed-AST
caches.
"""

from __future__ import annotations

import asyncio
import re

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension

from tests._engine_helpers import _engine_generate


async def _gen(dialect: str, query: SlayerQuery, model: SlayerModel) -> str:
    return await _engine_generate(query=query, model=model, dialect=dialect)


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
        # The typed pipeline wraps every aggregate in a per-dialect
        # result-type CAST, so ``COUNT(*)`` / ``SUM(...)`` carry the
        # measure's declared type (``*:count`` -> integer,
        # ``revenue:sum`` -> the DOUBLE column's float type).
        (
            "postgres",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  CAST(COUNT(*) AS INT) AS "orders._count",\n'
            '  CAST(SUM(orders.amount) AS DOUBLE PRECISION) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "sqlite",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  CAST(COUNT(*) AS INTEGER) AS "orders._count",\n'
            '  CAST(SUM(orders.amount) AS REAL) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "duckdb",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  CAST(COUNT(*) AS INT) AS "orders._count",\n'
            '  CAST(SUM(orders.amount) AS DOUBLE) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "clickhouse",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  CAST(COUNT(*) AS Nullable(Int32)) AS "orders._count",\n'
            '  CAST(SUM(orders.amount) AS Nullable(Float64)) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "mysql",
            "SELECT\n"
            "  orders.status AS `orders.status`,\n"
            "  CAST(COUNT(*) AS SIGNED) AS `orders._count`,\n"
            "  CAST(SUM(orders.amount) AS DOUBLE) AS `orders.revenue_sum`\n"
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
            "  CAST(COUNT(*) AS INTEGER) AS [orders____count],\n"
            "  CAST(SUM(orders.amount) AS FLOAT) AS [orders___revenue_sum]\n"
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
            '  CAST(SUM(orders.amount) AS DOUBLE PRECISION) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  DATE_TRUNC(\'MONTH\', orders.created_at)',
        ),
        (
            "sqlite",
            'SELECT\n'
            '  STRFTIME(\'%Y-%m-01\', orders.created_at) AS "orders.created_at",\n'
            '  CAST(SUM(orders.amount) AS REAL) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  STRFTIME(\'%Y-%m-01\', orders.created_at)',
        ),
        (
            "duckdb",
            'SELECT\n'
            '  DATE_TRUNC(\'MONTH\', orders.created_at) AS "orders.created_at",\n'
            '  CAST(SUM(orders.amount) AS DOUBLE) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  DATE_TRUNC(\'MONTH\', orders.created_at)',
        ),
        (
            # ClickHouse accepts both ``dateTrunc`` and ``DATE_TRUNC``; sqlglot
            # now emits the canonical lowercase ClickHouse form.
            "clickhouse",
            'SELECT\n'
            '  dateTrunc(\'MONTH\', orders.created_at) AS "orders.created_at",\n'
            '  CAST(SUM(orders.amount) AS Nullable(Float64)) AS "orders.revenue_sum"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  dateTrunc(\'MONTH\', orders.created_at)',
        ),
        (
            "mysql",
            "SELECT\n"
            "  STR_TO_DATE(CONCAT(YEAR(orders.created_at), ' ', MONTH(orders.created_at), ' 1'), '%Y %c %e') AS `orders.created_at`,\n"
            "  CAST(SUM(orders.amount) AS DOUBLE) AS `orders.revenue_sum`\n"
            "FROM public.orders AS orders\n"
            "GROUP BY\n"
            "  STR_TO_DATE(CONCAT(YEAR(orders.created_at), ' ', MONTH(orders.created_at), ' 1'), '%Y %c %e')",
        ),
        (
            # DEV-1571 Bug 2: bracketed dotted aliases mangled.
            "tsql",
            "SELECT\n"
            "  DATETRUNC(month, orders.created_at) AS [orders___created_at],\n"
            "  CAST(SUM(orders.amount) AS FLOAT) AS [orders___revenue_sum]\n"
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
            '  CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY\n'
            '    orders.amount) AS DOUBLE PRECISION) AS "orders.revenue_median"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "sqlite",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  CAST(PERCENTILE_CONT(orders.amount, 0.5) AS REAL) AS "orders.revenue_median"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "duckdb",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  CAST(QUANTILE_CONT(orders.amount, 0.5\n'
            '  ORDER BY\n'
            '    orders.amount) AS DOUBLE) AS "orders.revenue_median"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "clickhouse",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  CAST(quantile(0.5)(orders.amount) AS Nullable(Float64)) AS "orders.revenue_median"\n'
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
            '  CAST(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY\n'
            '    orders.amount) AS DOUBLE PRECISION) AS "orders.revenue_percentile_p_0_95"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "sqlite",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  CAST(PERCENTILE_CONT(orders.amount, 0.95) AS REAL) AS "orders.revenue_percentile_p_0_95"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "duckdb",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  CAST(QUANTILE_CONT(orders.amount, 0.95\n'
            '  ORDER BY\n'
            '    orders.amount) AS DOUBLE) AS "orders.revenue_percentile_p_0_95"\n'
            'FROM public.orders AS orders\n'
            'GROUP BY\n'
            '  orders.status',
        ),
        (
            "clickhouse",
            'SELECT\n'
            '  orders.status AS "orders.status",\n'
            '  CAST(quantile(0.95)(orders.amount) AS Nullable(Float64)) AS "orders.revenue_percentile_p_0_95"\n'
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
    # SQLite uses DATE(col, 'N months') — no INTERVAL keyword. The offset
    # applies to the TRUNCATED bucket start (DEV-1811 period-boundary fix).
    assert "DATE(STRFTIME('%Y-%m-01', orders.created_at), '1 months')" in sql
    assert "INTERVAL" not in sql
    assert "STRFTIME('%Y-%m-01'" in sql


async def test_byte_equivalence_time_shift_tsql(orders_model: SlayerModel) -> None:
    sql = await _gen("tsql", _TIME_SHIFT_QUERY, orders_model)
    # T-SQL uses DATEADD(unit, val, col) — no INTERVAL. The offset applies to
    # the TRUNCATED bucket start (DEV-1811 period-boundary fix).
    assert "DATEADD(MONTH, 1, DATETRUNC(MONTH, orders.created_at))" in sql
    assert "INTERVAL" not in sql
    # DEV-1571 Bug 1: T-SQL accepts WITH only as a statement prefix, so the
    # CTE chain must be hoisted onto the outer statement rather than left
    # inside ``FROM (...) AS _outer``. The hoist re-parses the inner SQL
    # through sqlglot's T-SQL dialect, which upper-cases the datepart on the
    # way through (T-SQL dateparts are case-insensitive keywords, so this is
    # cosmetic). Both facts are pinned here because the typed pipeline
    # briefly regressed the hoist by string-building the wrap instead of
    # delegating to ``SqlDialect.emit_outer_wrap``.
    assert "DATETRUNC(MONTH, " in sql
    assert sql.lstrip().upper().startswith("WITH "), sql
    assert not re.search(r"FROM\s*\(\s*WITH", sql, re.IGNORECASE), sql


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
    """SQLite's JSON-extract rewrite produces the
    JSON_EXTRACT(orders.blob, '$.k') function form (not the ``->``
    operator) — required so equality matches against bare-string literals
    work. DEV-1331.

    The typed pipeline qualifies bare column references inside
    ``Column.sql`` with the owning model's alias, so the argument is
    ``orders.blob`` rather than the legacy generator's unqualified
    ``blob``. Same column, same table — ``FROM public.orders AS orders``
    is the only relation in scope."""
    sql = await _gen("sqlite", _JSON_QUERY, orders_model_with_json)
    assert "JSON_EXTRACT(orders.blob, '$.k')" in sql
    # SQLite ``->`` operator must NOT appear (would silently return quoted form)
    assert " -> " not in sql


async def test_byte_equivalence_json_extract_postgres_uses_path_form(
    orders_model_with_json: SlayerModel,
) -> None:
    """Postgres emits JSON_EXTRACT_PATH(orders.blob, 'k') — sqlglot's
    translation, with the typed pipeline's model-alias qualification on
    the bare ``blob`` reference."""
    sql = await _gen("postgres", _JSON_QUERY, orders_model_with_json)
    assert "JSON_EXTRACT_PATH(orders.blob, 'k')" in sql


async def test_byte_equivalence_json_extract_tsql_uses_isnull_pair(
    orders_model_with_json: SlayerModel,
) -> None:
    """T-SQL emits ISNULL(JSON_QUERY(...), JSON_VALUE(...)) — handles both
    object and scalar paths. As on the other dialects, the typed pipeline
    qualifies the bare ``blob`` reference with the model alias.

    NOTE: the typed pipeline currently emits this pair wrapped in a
    redundant outer ``ISNULL(<pair>, <pair>)``. That doubling is a
    sqlglot T-SQL round-trip artefact (``exp.JSONExtract`` renders to
    ``ISNULL(JSON_QUERY, JSON_VALUE)``, which re-parses to
    ``Coalesce(JSONExtract, JSONExtractScalar)`` and re-renders doubled),
    NOT intended emission — it is deliberately *not* asserted here so a
    fix does not have to touch this test. See the DEV-1703 migration
    report."""
    sql = await _gen("tsql", _JSON_QUERY, orders_model_with_json)
    assert (
        "ISNULL(JSON_QUERY(orders.blob, '$.k'), JSON_VALUE(orders.blob, '$.k'))"
        in sql
    )
