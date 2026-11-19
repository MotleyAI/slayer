"""DEV-1745 (W2) — custom-aggregation template fragments must register the
joins they cross, in EVERY scope that renders them.

The Mode-A custom-aggregation template mechanism (``{value}`` / ``{param}``
substitution) stays as a feature; only its join discovery moves onto the single
Mode-A door, so registration becomes a side effect of resolution (P-A).

Today the fragment scan exists ONLY on the host render path
(``_resolve_agg_inputs_via_scope`` -> ``_resolve_fragment_kwargs``). The
cross-model ``_cm_`` CTE builds its FROM purely from ``cte_scope.join_paths``
and registers only source / positional args / typed column kwargs — never
string fragments nor the model-default
``AggregationParam.sql`` values. A crossing fragment therefore renders a
reference to a table that is not in the CTE's FROM.

The shifted (``time_shift``) CTE has the same gap but is unreachable today —
``time_shift`` combined with a cross-model aggregate raises first. That half is
tracked separately.
"""

from __future__ import annotations

import duckdb
import pytest

from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, FieldKey
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.query import QueryDimension, SlayerQuery
from slayer.core.scope import DatasetScope
from slayer.engine.binding import bind_expr
from slayer.core.syntax import parse_expr
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1746_fixtures import cte_names_in_order, find_cte
from tests._engine_helpers import _engine_generate


# ---------------------------------------------------------------------------
# Fixtures — the aggregation is declared on the JOINED model so the planner
# roots a _cm_ CTE at `customers`, and its default param crosses one hop
# further to `regions`.
# ---------------------------------------------------------------------------


def _regions() -> SlayerDataset:
    return SlayerDataset(
        name="regions", data_source="test", sql_table="regions",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="weight", datatype=DataType.FLOAT),
        ],
    )


def _customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers", data_source="test", sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.INTEGER),
            DatasetField(name="spend", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
        aggregations=[
            Aggregation(
                name="wscaled_sum", expression="SUM({value} * {w})",
                params=[AggregationParam(name="w", sql="regions.weight")],
            ),
        ],
    )


def _orders() -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="status", datatype=DataType.STRING),
        ],
        relationships=[DatasetRelationship(
            to="customers", from_columns=["customer_id"], to_columns=["id"],
        )],
    )


def _orders_local_agg() -> SlayerDataset:
    """Host-rooted variant: the aggregation is declared on the ROOT model with
    a crossing default param. This path already scans fragments today and must
    not regress."""
    return SlayerDataset(
        name="orders", data_source="test", sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="status", datatype=DataType.STRING),
        ],
        relationships=[DatasetRelationship(
            to="customers", from_columns=["customer_id"], to_columns=["id"],
        )],
        aggregations=[
            Aggregation(
                name="wscaled_sum", expression="SUM({value} * {w})",
                params=[AggregationParam(
                    name="w", sql="customers.regions.weight",
                )],
            ),
        ],
    )


async def _sql(
    query: SlayerQuery, *, dataset: SlayerDataset, dialect: str = "postgres",
) -> str:
    return await _engine_generate(
        query=query, dataset=dataset, dialect=dialect, validate=False,
        extra_datasets=[_customers(), _regions()],
    )


# ---------------------------------------------------------------------------


class TestOnlySubstitutedKwargsAreSql:
    """A string kwarg is a SQL fragment only when the aggregation's template
    substitutes it; anything else is a marker, kept as a string on the key and
    never handed to a SQL parser."""

    @staticmethod
    def _kwargs(expression: str) -> dict:
        orders = _orders()
        key = bind_expr(
            parse_expr(expression), scope=DatasetScope(population=orders),
            bundle=ResolvedSourceBundle(dialect="postgres", population=orders,
                                        referenced_datasets=[_customers(), _regions()]),
        ).value_key
        assert isinstance(key, AggregateKey), key
        return dict(key.kwargs)

    def test_reserved_marker_kwarg_is_not_parsed_as_sql(self) -> None:
        kwargs = self._kwargs("wscaled_sum(customers.spend, window='90d')")
        assert kwargs["window"] == "90d"
        assert kwargs["w"] == FieldKey(path=("customers", "regions"), leaf="weight")

    def test_marker_that_is_not_parseable_sql_is_still_skipped(self) -> None:
        assert self._kwargs("wscaled_sum(customers.spend, window='%Y-%m')")["window"] == "%Y-%m"

    def test_a_substituted_kwarg_is_still_bound(self) -> None:
        assert self._kwargs("wscaled_sum(customers.spend, w='customers.regions.weight')") == {
            "w": FieldKey(path=("customers", "regions"), leaf="weight")}


def _cm_body(sql: str, *, dialect: str = "postgres") -> str:
    """The rendered body of the `_cm_` CTE.

    Assertions about which alias the fragment rendered belong to THIS scope:
    a whole-SQL check can be satisfied — or defeated — by a perfectly valid
    alias in the host base or the combined SELECT.
    """
    name = next(
        (n for n in cte_names_in_order(sql, dialect=dialect)
         if n.startswith("_cm_")),
        None,
    )
    assert name is not None, f"no _cm_ CTE in:\n{sql}"
    body = find_cte(sql, name, dialect=dialect)
    assert body is not None, f"no _cm_ CTE in:\n{sql}"
    return body.sql(dialect=dialect)


@pytest.mark.asyncio
class TestCrossModelFragmentJoins:
    """The `_cm_` CTE gap — a crossing template fragment must pull its join
    into the CTE's own FROM."""

    async def test_cm_cte_joins_the_fragment_target(self) -> None:
        sql = await _sql(
            SlayerQuery(
                population="orders",
                dimensions=[QueryDimension(field="orders.status", alias="status")],
                metrics=[{
                    "metric": "wscaled_sum(customers.spend)", "alias": "m0",
                }],
            ),
            dataset=_orders(),
        )
        body = _cm_body(sql)
        # the fragment renders regions.weight, anchored at the CTE's own root ...
        assert "regions.weight" in body, body
        assert "customers__regions.weight" not in body, (
            f"fragment was anchored at the HOST path, not the CTE root:\n{body}"
        )
        # ... so regions MUST be joined in that same scope
        assert "JOIN regions" in body, (
            f"fragment's crossed join missing from the CTE FROM:\n{body}"
        )

    async def test_cm_cte_with_sibling_local_measure(self) -> None:
        sql = await _sql(
            SlayerQuery(
                population="orders",
                dimensions=[QueryDimension(field="orders.status", alias="status")],
                metrics=[
                    {"metric": "wscaled_sum(customers.spend)", "alias": "m0"},
                    {"metric": "sum(orders.amount)", "alias": "m1"},
                ],
            ),
            dataset=_orders(),
        )
        body = _cm_body(sql)
        assert "regions.weight" in body, body
        assert "customers__regions.weight" not in body, (
            f"fragment was anchored at the HOST path, not the CTE root:\n{body}"
        )
        assert "JOIN regions" in body, (
            f"fragment's crossed join missing from the CTE FROM:\n{body}"
        )


@pytest.mark.asyncio
class TestHostPathFragmentJoinsStillWork:
    """Parity guard: the host path already registers fragment joins."""

    async def test_host_rooted_fragment_join_registered(self) -> None:
        sql = await _sql(
            SlayerQuery.model_validate({
                "population": "orders",
                "dimensions": [{"field": "orders.status", "alias": "status"}],
                "metrics": [{"metric": "wscaled_sum(orders.amount)", "alias": "m0"}],
            }),
            dataset=_orders_local_agg(),
        )
        assert "customers__regions.weight" in sql, sql
        assert "JOIN regions" in sql, sql

    async def test_query_time_string_kwarg_override(self) -> None:
        sql = await _sql(
            SlayerQuery(
                population="orders",
                dimensions=[QueryDimension(field="orders.status", alias="status")],
                metrics=[{
                    "metric": "wscaled_sum(orders.amount, w='customers.regions.weight')",
                    "alias": "m0",
                }],
            ),
            dataset=_orders_local_agg(),
        )
        assert "customers__regions.weight" in sql, sql
        assert "JOIN regions" in sql, sql


@pytest.mark.asyncio
async def test_cross_model_fragment_executes_on_duckdb() -> None:
    """A missing join is not a cosmetic difference — the SQL does not bind."""
    sql = await _sql(
        SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": [{"field": "orders.status", "alias": "status"}],
            "metrics": [{"metric": "wscaled_sum(customers.spend)", "alias": "m0"}],
        }),
        dataset=_orders(), dialect="duckdb",
    )
    with duckdb.connect() as con:
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
