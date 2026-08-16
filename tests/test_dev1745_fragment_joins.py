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

import duckdb
import pytest

from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, ColumnKey
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.sql.generator import SQLGenerator

from tests._dev1746_fixtures import cte_names_in_order, find_cte
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


async def _sql(
    query: SlayerQuery, *, model: SlayerModel, dialect: str = "postgres",
) -> str:
    return await _engine_generate(
        query=query, model=model, dialect=dialect, validate=False,
        extra_models=[_customers(), _regions()],
    )


# ---------------------------------------------------------------------------


class TestOnlySubstitutedKwargsAreSql:
    """A string kwarg is a SQL fragment only when the aggregation's template
    substitutes it. Anything else is a marker, and handing a marker to a SQL
    parser is meaningless — harmless while the scan swallowed parse errors,
    query-fatal now that the door raises."""

    @staticmethod
    def _entered_fragments(*, kwargs, agg="sum") -> list:
        gen = SQLGenerator(dialect="postgres")
        seen: list = []
        gen._enter_mode_a_expression = (  # type: ignore[method-assign]
            lambda **kw: seen.append(kw["sql"])
        )
        gen._register_fragment_kwarg_joins(
            key=AggregateKey(
                source=ColumnKey(path=(), leaf="spend"), agg=agg,
                kwargs=kwargs,
            ),
            scope=object(),
            model=_customers(),
        )
        return seen

    def test_reserved_marker_kwarg_is_not_parsed_as_sql(self) -> None:
        """``window='90d'`` is the standing example — a marker whose ``{window}``
        never appears in ``wscaled_sum``'s template, so it is not a fragment.
        Uses a TEMPLATED aggregation so the scan runs PAST the no-template
        guard; the default ``w`` param still contributes ``regions.weight``, so
        the check is that the MARKER's value is absent, not that nothing ran."""
        entered = self._entered_fragments(
            kwargs=(("window", "90d"),), agg="wscaled_sum",
        )
        assert "90d" not in entered, entered

    def test_marker_that_is_not_parseable_sql_is_still_skipped(self) -> None:
        """The failure this guards: a marker whose text sqlglot rejects. Under a
        TEMPLATED aggregation the substitution filter is what skips it, so it
        never reaches the door (which would raise)."""
        entered = self._entered_fragments(
            kwargs=(("fmt", "%Y-%m"),), agg="wscaled_sum",
        )
        assert "%Y-%m" not in entered, entered

    def test_a_substituted_kwarg_is_still_scanned(self) -> None:
        """The counter-case, so the filter is not blanket suppression:
        ``wscaled_sum``'s template does substitute ``{w}``."""
        entered = self._entered_fragments(
            kwargs=(("w", "regions.weight"),), agg="wscaled_sum",
        )
        assert entered == ["regions.weight"], entered


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
                source_model="orders",
                dimensions=[{"formula": "status", "name": "status"}],
                measures=[{
                    "formula": "customers.spend:wscaled_sum", "name": "m0",
                }],
            ),
            model=_orders(),
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
                source_model="orders",
                dimensions=[{"formula": "status", "name": "status"}],
                measures=[
                    {"formula": "customers.spend:wscaled_sum", "name": "m0"},
                    {"formula": "amount:sum", "name": "m1"},
                ],
            ),
            model=_orders(),
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
    sql = await _sql(
        SlayerQuery(
            source_model="orders",
            dimensions=[{"formula": "status", "name": "status"}],
            measures=[{"formula": "customers.spend:wscaled_sum", "name": "m0"}],
        ),
        model=_orders(), dialect="duckdb",
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
