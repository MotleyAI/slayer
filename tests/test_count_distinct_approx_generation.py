"""DEV-1595: end-to-end ``count_distinct_approx`` aggregation.

Covers enum membership / eligibility (incl. on primary-key columns) and the
functional ``count_distinct_approx(col)`` emission through the typed engine
pipeline, on a native-supporting dialect (DuckDB) and an exact-fallback
dialect (Postgres).
"""

from __future__ import annotations

from slayer.core.enums import (
    BUILTIN_AGGREGATIONS,
    DEFAULT_AGGREGATIONS_BY_TYPE,
    PRIMARY_KEY_AGGREGATIONS,
    DataType,
)
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import QueryMetric, SlayerQuery

from tests._engine_helpers import _engine_generate


async def _generate(dialect: str, query: SlayerQuery, dataset: SlayerDataset) -> str:
    return await _engine_generate(query=query, dataset=dataset, dialect=dialect)


def _dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", sql="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="status", sql="status", datatype=DataType.STRING),
        ],
    )


# ---------------------------------------------------------------------------
# Enum membership / eligibility
# ---------------------------------------------------------------------------


def test_is_builtin_aggregation() -> None:
    assert "count_distinct_approx" in BUILTIN_AGGREGATIONS


def test_eligible_on_every_type_like_count_distinct() -> None:
    for dtype, allowed in DEFAULT_AGGREGATIONS_BY_TYPE.items():
        if dtype is DataType.ARRAY:
            # A list counts distinct element sets exactly, never approximately.
            assert "count_distinct" in allowed, allowed
            assert "count_distinct_approx" not in allowed, allowed
            continue
        assert ("count_distinct_approx" in allowed) == ("count_distinct" in allowed), (
            f"count_distinct_approx eligibility must mirror count_distinct for {dtype}"
        )


def test_allowed_on_primary_key_columns() -> None:
    # Decision: approx distinct is allowed on PK columns, like count_distinct.
    assert "count_distinct_approx" in PRIMARY_KEY_AGGREGATIONS


# ---------------------------------------------------------------------------
# End-to-end emission
# ---------------------------------------------------------------------------


async def test_duckdb_emits_native_approx() -> None:
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="count_distinct_approx(orders.customer_id)")],
    )
    sql = (await _generate("duckdb", query, _dataset())).lower()
    assert "approx_count_distinct(" in sql


async def test_postgres_emits_exact_fallback() -> None:
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="count_distinct_approx(orders.customer_id)")],
    )
    sql = (await _generate("postgres", query, _dataset())).upper()
    assert "COUNT(DISTINCT" in sql
    # Exact fallback — no native approximate-distinct *function* is emitted.
    # (The result-column alias still carries the requested aggregation name,
    # ``..._count_distinct_approx``, because SLayer aliases are dialect-
    # independent by design — see CLAUDE.md result-column naming.)
    assert "APPROX_COUNT_DISTINCT" not in sql
    assert "APPROX_DISTINCT" not in sql


async def test_clickhouse_routes_to_native_uniq() -> None:
    """Proves SQLGenerator dispatches count_distinct_approx to the dialect
    override (not just that the override exists)."""
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="count_distinct_approx(orders.customer_id)")],
    )
    sql = (await _generate("clickhouse", query, _dataset())).lower()
    assert "uniq(" in sql


async def test_approx_on_primary_key_column_generates() -> None:
    query = SlayerQuery(
        population="orders",
        metrics=[QueryMetric(metric="count_distinct_approx(orders.id)")],
    )
    # Must not raise an eligibility error on the PK column.
    sql = await _generate("duckdb", query, _dataset())
    assert "approx_count_distinct(" in sql.lower()


async def test_alias_heals_to_count_distinct_approx_and_emits_native() -> None:
    """The aliases for ``count_distinct_approx``
    (``approx_count_distinct`` / ``countdistinctapprox`` — see
    ``slayer/core/enums.py`` ``AGGREGATION_ALIASES``) heal to the canonical
    name and dispatch through the same built-in generator branch, so a native
    dialect emits its approximate-distinct function rather than raising
    'Unknown function' or 'has no formula'."""
    for alias in ("approx_count_distinct", "countdistinctapprox"):
        query = SlayerQuery(
            population="orders",
            metrics=[QueryMetric(metric=f"{alias}(customer_id)")],
        )
        sql = (await _generate("duckdb", query, _dataset())).lower()
        assert "approx_count_distinct(" in sql, alias
