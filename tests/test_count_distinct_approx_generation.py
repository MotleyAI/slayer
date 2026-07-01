"""DEV-1595: end-to-end ``count_distinct_approx`` aggregation.

Covers enum membership / eligibility (incl. on primary-key columns) and the
colon-form ``col:count_distinct_approx`` emission through enrichment + the SQL
generator, on a native-supporting dialect (DuckDB) and an exact-fallback
dialect (Postgres).
"""

from __future__ import annotations

from slayer.core.enums import (
    BUILTIN_AGGREGATIONS,
    DEFAULT_AGGREGATIONS_BY_TYPE,
    PRIMARY_KEY_AGGREGATIONS,
    DataType,
)
from slayer.core.models import Column, ModelMeasure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.enrichment import enrich_query
from slayer.sql.generator import SQLGenerator


async def _noop_async(**kw):  # NOSONAR(S7503) — must be a coroutine; awaited as an enrich_query resolver callback
    return None


async def _generate(dialect: str, query: SlayerQuery, model: SlayerModel) -> str:
    gen = SQLGenerator(dialect=dialect)
    enriched = await enrich_query(
        query=query,
        model=model,
        resolve_dimension_via_joins=_noop_async,
        resolve_cross_model_measure=_noop_async,
        resolve_join_target=_noop_async,
    )
    return gen.generate(enriched=enriched)


def _model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="public.orders",
        data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
            Column(name="status", sql="status", type=DataType.TEXT),
        ],
    )


# ---------------------------------------------------------------------------
# Enum membership / eligibility
# ---------------------------------------------------------------------------


def test_is_builtin_aggregation() -> None:
    assert "count_distinct_approx" in BUILTIN_AGGREGATIONS


def test_eligible_on_every_type_like_count_distinct() -> None:
    for dtype, allowed in DEFAULT_AGGREGATIONS_BY_TYPE.items():
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
        source_model="orders",
        measures=[ModelMeasure(formula="customer_id:count_distinct_approx")],
    )
    sql = (await _generate("duckdb", query, _model())).lower()
    assert "approx_count_distinct(" in sql


async def test_postgres_emits_exact_fallback() -> None:
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="customer_id:count_distinct_approx")],
    )
    sql = (await _generate("postgres", query, _model())).upper()
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
        source_model="orders",
        measures=[ModelMeasure(formula="customer_id:count_distinct_approx")],
    )
    sql = (await _generate("clickhouse", query, _model())).lower()
    assert "uniq(" in sql


async def test_approx_on_primary_key_column_generates() -> None:
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="id:count_distinct_approx")],
    )
    # Must not raise an eligibility error on the PK column.
    sql = await _generate("duckdb", query, _model())
    assert "approx_count_distinct(" in sql.lower()
