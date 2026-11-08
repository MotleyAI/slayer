"""Carrier × scope acceptance matrix.

* **Layer 1 — invariant sweep** (``TestScopeClosureInvariant``): every
  currently-passing (carrier, scope) combination emits scope-closed SQL. Each
  cell first asserts a *structural marker* proving the intended scope/carrier
  shape was actually exercised (guards against a fixture silently degrading to a
  plain ``_base``), then asserts ``assert_scope_closed`` passes.
* **Layer 2 — scope pins** (``TestScopeDefectPins``): scopes with no other
  passing representative, asserting their intended shape.

Plus the two flagged semantic pins — **F1** (1:N crossing aggregates keep
multiply-per-match) and **F4** (a host-local filter semi-joins into the scalar
cross-model CTE) — as SQL-shape assertions here; their executed-value
counterparts live in ``tests/integration/test_integration_duckdb.py``. And the
**RLS × isolation-CTE** cells, which exercise the validator's post-RLS
``allow_rls_correlation`` allowlist against the session-policy transform.

Carriers exercised: aggregate source (ColumnKey / ColumnSqlKey), explicit-time
positional arg, ``Column.filter``, derived + time dimensions, WHERE filters
(typed + Mode-A), model filters, order refs. Scopes: host base ``_base``,
first/last ranked subquery, forward ``_cm_*`` CTE, ``time_shift`` CTE, windowed
producer CTE. ``_fm_``/``_wm_`` legacy scope shapes get direct validator
coverage in ``tests/test_scope_check.py`` (hand-written SQL).
"""

from __future__ import annotations

import re

import pytest
import sqlglot

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.policy import (
    ColumnFilterRuleset,
    JoinFilterRule,
    JoinFilterRuleset,
    SessionPolicy,
)
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.sql.dialects import get_dialect
from slayer.sql.scope_check import assert_scope_closed
from slayer.sql.session_policy import apply_session_policy
from tests._engine_helpers import _engine_generate

_ALWAYS = lambda *_args, **_kw: True  # noqa: E731 — RLS has_column probe stub


# --------------------------------------------------------------------------- #
# Model builders — the orders → customers → regions chain.
# --------------------------------------------------------------------------- #
def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="population", sql="population", type=DataType.DOUBLE),
        ],
    )


def _customers(extra=None) -> SlayerModel:
    cols = [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
        Column(name="balance", sql="balance", type=DataType.DOUBLE),
    ]
    cols += extra or []
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test", columns=cols,
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _orders(*, extra=None, filters=None) -> SlayerModel:
    cols = [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="status", sql="status", type=DataType.TEXT),
        Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        Column(name="ordered_at", sql="ordered_at", type=DataType.TIMESTAMP),
        Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
        Column(name="amount", sql="amount", type=DataType.DOUBLE),
        Column(name="balance", sql="balance", type=DataType.DOUBLE),
    ]
    cols += extra or []
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at", columns=cols,
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        filters=filters or [],
    )


async def _gen(query, model, *, extra_models=None, dialect="postgres") -> str:
    return await _engine_generate(
        query=query, model=model, extra_models=extra_models or [], dialect=dialect,
    )


def _cte_body(sql: str, cte_name_pattern: str) -> str:
    """Balanced-paren extraction of a single CTE body (local copy of the
    ``test_sql_generator`` helper so this module stays self-contained)."""
    m = re.search(rf"({cte_name_pattern})\s+AS\s*\(", sql)
    assert m, f"No CTE matching {cte_name_pattern!r} in:\n{sql}"
    start = sql.index("(", m.start()) + 1
    depth, i = 1, start
    while i < len(sql):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return sql[start:i]
        i += 1
    raise AssertionError(f"Unbalanced parens for CTE {m.group(1)!r}:\n{sql}")


# --------------------------------------------------------------------------- #
# Layer 1 — invariant sweep: currently-passing combos emit scope-closed SQL.
# Each cell asserts (structural marker exercised) THEN (assert_scope_closed).
# --------------------------------------------------------------------------- #
class TestScopeClosureInvariant:
    async def test_host_base_agg_source_columnkey(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _gen(query, _orders())
        assert "GROUP BY" in sql
        assert "_cm_" not in sql  # host base scope
        assert_scope_closed(sql)

    async def test_host_base_where_filter_typed(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
            filters=["amount > 100"],
        )
        sql = await _gen(query, _orders())
        assert "WHERE" in sql
        assert_scope_closed(sql)

    async def test_host_base_where_filter_mode_a_crosses_join(self) -> None:
        # A Mode-A WHERE filter that crosses the orders→customers join pulls the
        # LEFT JOIN into the host base scope; still closed.
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum")],
            filters=["customers.region_id > 0"],
        )
        sql = await _gen(query, _orders(), extra_models=[_customers(), _regions()])
        assert "LEFT JOIN customers" in sql
        assert_scope_closed(sql)

    async def test_host_base_model_filter(self) -> None:
        query = SlayerQuery(
            source_model="orders", measures=[ModelMeasure(formula="amount:sum")],
        )
        sql = await _gen(query, _orders(filters=["amount > 0"]))
        assert "WHERE" in sql
        assert_scope_closed(sql)

    async def test_cross_model_cm_cte_agg_source(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customers.balance:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _gen(query, _orders(), extra_models=[_customers(), _regions()])
        assert "_cm_" in sql  # forward cross-model CTE scope
        assert_scope_closed(sql)

    async def test_first_last_ranked_explicit_time_arg(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension="created_at", granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="balance:last(ordered_at)")],
        )
        sql = await _gen(query, _orders())
        assert "_cm_" in sql  # ranked producer under the uniform _cm_ scope
        assert "ROW_NUMBER()" in sql
        assert "orders.ordered_at" in sql                    # explicit time carrier
        assert_scope_closed(sql)

    async def test_time_shift_cte(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension="created_at", granularity=TimeGranularity.MONTH)],
            measures=[
                ModelMeasure(formula="amount:sum"),
                ModelMeasure(formula="time_shift(amount:sum, -1)", name="prev"),
            ],
        )
        sql = await _gen(query, _orders())
        assert "shifted_" in sql  # time_shift CTE scope
        assert_scope_closed(sql)

    async def test_order_ref_by_measure(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="amount:sum", name="rev")],
            dimensions=[ColumnRef(name="status")],
            order=[OrderItem(column="rev", direction="desc")],
        )
        sql = await _gen(query, _orders())
        assert "ORDER BY" in sql
        assert_scope_closed(sql)

    async def test_column_filter_carrier_host_base(self) -> None:
        # A per-measure Column.filter (local) on the host base scope.
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="active_amount:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        model = _orders(extra=[
            Column(name="active_amount", sql="amount", filter="status = 'active'",
                   type=DataType.DOUBLE),
        ])
        sql = await _gen(query, model)
        assert "CASE" in sql  # filtered aggregate renders CASE WHEN
        assert_scope_closed(sql)

    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "duckdb", "mysql"])
    async def test_cross_model_closed_across_dialects(self, dialect: str) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customers.balance:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _gen(query, _orders(), extra_models=[_customers(), _regions()],
                         dialect=dialect)
        assert_scope_closed(sql, dialect=dialect)


# --------------------------------------------------------------------------- #
# F1 / F4 semantic pins (SQL-shape). Values pinned in test_integration_duckdb.
# --------------------------------------------------------------------------- #
class TestSemanticPinsSqlShape:
    async def test_f4_scalar_cross_model_agg_semi_joins_host_local_filter(self) -> None:
        # No dimensions ⇒ scalar cross-model aggregate. The
        # host-local filter (orders.status='paid') restricts the target-rooted
        # _cm_ CTE's population via a correlated EXISTS — never via a
        # cardinality-changing join of the CTE body.
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customers.balance:sum")],
            filters=["status = 'paid'"],
        )
        sql = await _gen(query, _orders(), extra_models=[_customers(), _regions()])
        cm_body = _cte_body(sql, r"_cm_\w+")
        assert "EXISTS" in cm_body.upper(), cm_body
        assert "paid" in cm_body, (
            f"the host-local filter must semi-join into the CTE:\n{cm_body}"
        )
        assert_scope_closed(sql)

    async def test_f1_crossing_agg_no_dedup(self) -> None:
        # F1: a crossing aggregate keeps multiply-per-match semantics — its
        # isolated CTE sums over the joined rows with NO EXISTS/DISTINCT dedup
        # (the structural basis of the F1 decision). Executed-value behavior is
        # pinned in test_integration_duckdb.py::TestF1F4SemanticValues.
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customers.balance:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _gen(query, _orders(), extra_models=[_customers(), _regions()])
        cm_body = _cte_body(sql, r"_cm_\w+")
        assert "SUM(" in cm_body.upper()
        assert "EXISTS" not in cm_body.upper(), cm_body
        assert "COUNT(DISTINCT" not in cm_body.upper(), cm_body
        assert_scope_closed(sql)


# --------------------------------------------------------------------------- #
# RLS × isolation-CTE — the validator's post-RLS allowlist against the
# session-policy transform (validator runs pre-RLS by default; post-RLS carries
# the _rls_src correlated-EXISTS allowlist).
# --------------------------------------------------------------------------- #
class TestRlsIsolationCte:
    async def _cross_model_sql(self) -> str:
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customers.balance:sum")],
            dimensions=[ColumnRef(name="status")],
        )
        sql = await _gen(query, _orders(), extra_models=[_customers(), _regions()])
        assert "_cm_" in sql
        return sql

    async def test_column_ruleset_over_isolation_cte_closed(self) -> None:
        sql = await self._cross_model_sql()
        assert_scope_closed(sql)  # pre-RLS
        wrapped = apply_session_policy(
            sqlglot.parse_one(sql, dialect="postgres"), sql_dialect=get_dialect("postgres"),
            policy=SessionPolicy(ruleset=ColumnFilterRuleset(column="org", value="7ef3")),
            has_column=_ALWAYS,
        ).sql(dialect="postgres")
        # Column ruleset wraps each table with a plain WHERE (no correlation);
        # closed even without the allowlist, and with it.
        assert_scope_closed(wrapped)
        assert_scope_closed(wrapped, allow_rls_correlation=True)

    async def test_join_ruleset_correlated_exists_needs_allowlist(self) -> None:
        sql = await self._cross_model_sql()
        wrapped = apply_session_policy(
            sqlglot.parse_one(sql, dialect="postgres"), sql_dialect=get_dialect("postgres"),
            policy=SessionPolicy(ruleset=JoinFilterRuleset(
                table="customers", column="organization_uuid", value="orgA",
                joins=[JoinFilterRule(
                    target_table="orders",
                    join_path=["orders.customer_id = customers.id"])],
            )),
            has_column=_ALWAYS,
        ).sql(dialect="postgres")
        assert "_rls_src" in wrapped  # intentional correlated EXISTS emitted
        # The checker is correlation-aware (planner-emitted semi-joins
        # correlate legally), so the RLS correlation passes with
        # and without the legacy allowlist flag.
        assert_scope_closed(wrapped)
        assert_scope_closed(wrapped, allow_rls_correlation=True)


# --------------------------------------------------------------------------- #
# Layer 2 — scope pins. Rich cross-model / first-last / windowed coverage
# lives in tests/test_sql_generator.py.
# --------------------------------------------------------------------------- #
class TestScopeDefectPins:
    async def test_windowed_measure_emits_wm_cte(self) -> None:
        # Duration-windowed measures emit a range-join producer CTE under the
        # `_cm_` producer scope. Rich coverage lives in
        # tests/test_sql_generator.py (TestFields / TestWindowedMeasureGuards)
        # and tests/integration/test_integration_windowed_measures.py.
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension="created_at", granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="amount:sum(window='90d')", name="rev_w")],
        )
        sql = await _gen(query, _orders())
        assert "_cm_" in sql  # windowed producer under the uniform _cm_ scope

    async def test_cross_model_source_crosses_further_join(self) -> None:
        customers = _customers(extra=[
            Column(name="deep_pop", sql="regions.population", type=DataType.DOUBLE),
        ])
        query = SlayerQuery(
            source_model="orders",
            measures=[ModelMeasure(formula="customers.deep_pop:sum")],
        )
        sql = await _gen(query, _orders(), extra_models=[customers, _regions()])
        cm_body = _cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN regions AS regions" in cm_body

    async def test_time_shift_cross_model_partition(self) -> None:
        # The shifted CTE is a ScopeFrame that pulls the join a cross-model
        # partition crosses. Rich coverage lives in
        # tests/test_dev1474_time_shift_cross_model_partition.py.
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension="created_at", granularity=TimeGranularity.MONTH)],
            dimensions=[ColumnRef(name="customers.region_id")],
            measures=[
                ModelMeasure(formula="amount:sum"),
                ModelMeasure(formula="time_shift(amount:sum, -1)", name="prev"),
            ],
        )
        sql = await _gen(query, _orders(), extra_models=[_customers(), _regions()])
        assert "shifted_" in sql
        assert_scope_closed(sql)
