"""DEV-1745 (W2) — custom-aggregation template fragments must register the
joins they cross, in EVERY scope that renders them.

The Mode-A custom-aggregation template mechanism (``{value}`` / ``{param}``
substitution) stays as a feature; only its join discovery moves onto the single
Mode-A door, so registration becomes a side effect of resolution (P-A).

Today the fragment scan exists ONLY on the host render path
(``_resolve_agg_inputs_via_scope`` -> ``_resolve_fragment_kwargs``). The
cross-model ``_cm_`` CTE builds its FROM purely from ``cte_scope.join_paths``
and registers only source / positional args / typed column kwargs /
``column_filter_key`` — never string fragments nor the model-default
``AggregationParam.sql`` values. A crossing fragment therefore renders a
reference to a table that is not in the CTE's FROM.

The shifted (``time_shift``) CTE has the same gap but is unreachable today —
``time_shift`` combined with a cross-model aggregate raises first. That half is
tracked separately.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    SlayerModel,
)
from slayer.core.query import SlayerQuery

from tests._engine_helpers import _engine_generate


# ---------------------------------------------------------------------------
# Fixtures — the aggregation is declared on the JOINED model so the planner
# roots a _cm_ CTE at `customers`, and its default param crosses one hop
# further to `regions`.
# ---------------------------------------------------------------------------


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", data_source="test", sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="weight", type=DataType.DOUBLE),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", data_source="test", sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="spend", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        aggregations=[
            Aggregation(
                name="wscaled_sum", formula="SUM({value} * {w})",
                params=[AggregationParam(name="w", sql="regions.weight")],
            ),
        ],
    )


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
        joins=[ModelJoin(
            target_model="customers", join_pairs=[["customer_id", "id"]],
        )],
    )


def _orders_local_agg() -> SlayerModel:
    """Host-rooted variant: the aggregation is declared on the ROOT model with
    a crossing default param. This path already scans fragments today and must
    not regress."""
    return SlayerModel(
        name="orders", data_source="test", sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
        joins=[ModelJoin(
            target_model="customers", join_pairs=[["customer_id", "id"]],
        )],
        aggregations=[
            Aggregation(
                name="wscaled_sum", formula="SUM({value} * {w})",
                params=[AggregationParam(
                    name="w", sql="customers__regions.weight",
                )],
            ),
        ],
    )


async def _sql(query: SlayerQuery, *, model: SlayerModel, dialect="postgres") -> str:
    return await _engine_generate(
        query=query, model=model, dialect=dialect, validate=False,
        extra_models=[_customers(), _regions()],
    )


# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestCrossModelFragmentJoins:
    """The `_cm_` CTE gap — a crossing template fragment must pull its join
    into the CTE's own FROM."""

    async def test_cm_cte_joins_the_fragment_target(self) -> None:
        sql = await _sql(
            SlayerQuery(
                source_model="orders",
                dimensions=[{"formula": "status", "name": "status"}],
                measures=[{
                    "formula": "customers.spend:wscaled_sum", "name": "m0",
                }],
            ),
            model=_orders(),
        )
        # the fragment renders regions.weight ...
        assert "regions.weight" in sql, sql
        # ... so regions MUST be joined in the same scope
        assert "JOIN regions" in sql, (
            f"fragment's crossed join missing from the CTE FROM:\n{sql}"
        )

    async def test_cm_cte_with_sibling_local_measure(self) -> None:
        sql = await _sql(
            SlayerQuery(
                source_model="orders",
                dimensions=[{"formula": "status", "name": "status"}],
                measures=[
                    {"formula": "customers.spend:wscaled_sum", "name": "m0"},
                    {"formula": "amount:sum", "name": "m1"},
                ],
            ),
            model=_orders(),
        )
        assert "regions.weight" in sql, sql
        assert "JOIN regions" in sql, (
            f"fragment's crossed join missing from the CTE FROM:\n{sql}"
        )


@pytest.mark.asyncio
class TestHostPathFragmentJoinsStillWork:
    """Parity guard: the host path already registers fragment joins."""

    async def test_host_rooted_fragment_join_registered(self) -> None:
        sql = await _sql(
            SlayerQuery(
                source_model="orders",
                dimensions=[{"formula": "status", "name": "status"}],
                measures=[{"formula": "amount:wscaled_sum", "name": "m0"}],
            ),
            model=_orders_local_agg(),
        )
        assert "customers__regions.weight" in sql, sql
        assert "JOIN regions" in sql, sql

    async def test_query_time_string_kwarg_override(self) -> None:
        sql = await _sql(
            SlayerQuery(
                source_model="orders",
                dimensions=[{"formula": "status", "name": "status"}],
                measures=[{
                    "formula": "amount:wscaled_sum(w='customers__regions.weight')",
                    "name": "m0",
                }],
            ),
            model=_orders_local_agg(),
        )
        assert "customers__regions.weight" in sql, sql
        assert "JOIN regions" in sql, sql


@pytest.mark.asyncio
async def test_cross_model_fragment_executes_on_duckdb() -> None:
    """A missing join is not a cosmetic difference — the SQL does not bind."""
    import duckdb

    sql = await _sql(
        SlayerQuery(
            source_model="orders",
            dimensions=[{"formula": "status", "name": "status"}],
            measures=[{"formula": "customers.spend:wscaled_sum", "name": "m0"}],
        ),
        model=_orders(), dialect="duckdb",
    )
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE orders(id INT, customer_id INT, amount DOUBLE, status VARCHAR)"
    )
    con.execute("CREATE TABLE customers(id INT, region_id INT, spend DOUBLE)")
    con.execute("CREATE TABLE regions(id INT, weight DOUBLE)")
    con.execute("INSERT INTO orders VALUES (1, 1, 10.0, 'ok')")
    con.execute("INSERT INTO customers VALUES (1, 1, 7.0)")
    con.execute("INSERT INTO regions VALUES (1, 3.0)")

    rows = con.execute(sql).fetchall()
    # SUM(customers.spend * regions.weight) = 7 * 3 = 21
    assert rows == [("ok", 21.0)], f"unexpected rows {rows!r} for SQL:\n{sql}"
