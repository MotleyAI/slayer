"""DEV-1708 Stage 4 — cross-model / isolation CTE renderer on ScopeFrame.

These tests pin the Stage-4 deliverables NOT already covered by the pinned
``_DEV1526_STAGE4`` xfails in ``tests/test_sql_generator.py`` (which cover the
plain cross-model aggregate-source-crossing shapes and auto-promote when the
CTE ScopeFrame lands). Here we add:

* **DEV-1701** — a shared-grain derived TIME dimension whose ``Column.sql``
  crosses a further join must pull that join into BOTH the host ``_base`` and
  the per-plan ``_cm_*`` CTE (Law 1 in the CTE ScopeFrame + the host-side
  anchoring fix that fixes both from one helper).
* **DEV-1527 cross-model remainder** — a parametric-aggregation kwarg naming a
  DERIVED target column (qualified form) expands its ``Column.sql`` and pulls
  the crossed join into the ``_cm_*`` CTE.
* **DEV-1702 B2 (forward variant)** — a forward first/last whose source
  ``Column.sql`` crosses a join materialises the crossed value as a ``_val_<n>``
  projection INSIDE the ranked subquery (Law 2), and the routed-HAVING variant
  binds to the same alias.
* **Routed WHERE / HAVING** filters on derived crossing target columns pull
  their joins.
* **Null-safe grain join-back (Codex F2)** — grain equality in the combined
  SELECT's LEFT JOIN ON uses dialect-aware null-safe predicates so NULL
  dimension values / nullable truncated time grains join back instead of
  dropping.
* **Plain derived (non-TIME) shared-grain dim → raise** (user-approved: replaces
  today's silently-wrong CROSS-JOIN broadcast; full support = DEV-1495-b1).
* **Generation-wide AliasAllocator determinism** — repeated / multi-CTE renders
  are byte-stable.

Every SQL-shape test additionally asserts ``assert_scope_closed`` on the
emitted statement (Codex F9 — direct per-shape validator evidence, independent
of the suite-wide ``SLAYER_VALIDATE_SCOPES=1`` hook).
"""

from __future__ import annotations

import os
import re as _re
import sqlite3
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import UnknownReferenceError
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.scope_check import assert_scope_closed
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _engine_generate


def _norm(s: str) -> str:
    return " ".join(s.split())


def _split_at_ranked_subquery(norm: str) -> "tuple[str, str]":
    """Split a normalized CTE body into ``(outer, inner)`` at the ranked
    subquery's ``FROM (``. Asserts the marker exists so a shape change (no
    ranked subquery) fails loudly instead of silently slicing at ``-1``."""
    at = norm.find("FROM (")
    assert at != -1, f"no ranked subquery (FROM () in:\n{norm}"
    return norm[:at], norm[at:]


def _joinback_on_predicate(sql: str, *, dialect: str = "postgres") -> str:
    """Return the rendered ON predicate of the combined SELECT's
    ``LEFT JOIN _cm_* ON ...`` grain join-back (the null-safe target)."""
    import sqlglot

    tree = sqlglot.parse_one(sql, dialect=dialect)
    for join in tree.find_all(sqlglot.exp.Join):
        target = join.this
        name = getattr(target, "alias_or_name", "") or ""
        if name.startswith("_cm_") or name.startswith("_fm_"):
            on = join.args.get("on")
            if on is not None:
                return on.sql(dialect=dialect)
    raise AssertionError(f"no LEFT JOIN _cm_*/_fm_* ON predicate in:\n{sql}")


def _extract_cte_body(sql: str, cte_name_pattern: str) -> str:
    """Extract one CTE body by matching ``<cte_name> AS (`` and walking balanced
    parentheses to its closing ``)`` (copied from test_sql_generator.py)."""
    name_match = _re.search(rf"({cte_name_pattern})\s+AS\s*\(", sql)
    assert name_match, f"No CTE matching {cte_name_pattern!r} in:\n{sql}"
    body_start = sql.index("(", name_match.start()) + 1
    depth = 1
    i = body_start
    while i < len(sql) and depth > 0:
        ch = sql[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return sql[body_start:i]
        i += 1
    raise AssertionError(
        f"Unbalanced parens — no closing ) for CTE {name_match.group(1)!r}:\n{sql}"
    )


# --------------------------------------------------------------------------- #
# Model builders — chain: orders_x → customers_v2 → regions → countries.
# Postgres dialect for SQL-shape assertions (mangling is identity).
# --------------------------------------------------------------------------- #
def _countries() -> SlayerModel:
    return SlayerModel(
        name="countries", sql_table="countries", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="gdp", sql="gdp", type=DataType.DOUBLE),
        ],
    )


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="population", sql="population", type=DataType.DOUBLE),
            Column(name="weight", sql="weight", type=DataType.DOUBLE),
            Column(name="country_id", sql="country_id", type=DataType.DOUBLE),
            Column(name="opened_at", sql="opened_at", type=DataType.TIMESTAMP),
        ],
        joins=[ModelJoin(target_model="countries", join_pairs=[["country_id", "id"]])],
    )


def _customers_v2(*, extra_columns=None) -> SlayerModel:
    cols = [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
        Column(name="lifetime_value", sql="lifetime_value", type=DataType.DOUBLE),
        Column(name="signup_at", sql="signup_at", type=DataType.TIMESTAMP),
        Column(name="status", sql="status", type=DataType.TEXT),
        # Derived, one-hop crossing (customers_v2 → regions):
        Column(name="deep_pop", sql="regions.population", type=DataType.DOUBLE),
        Column(name="deep_weight", sql="regions.weight", type=DataType.DOUBLE),
        # Derived, TWO-hop crossing (customers_v2 → regions → countries):
        Column(name="deep_gdp", sql="regions__countries.gdp", type=DataType.DOUBLE),
        # Derived TIME dim whose sql crosses a further join (DEV-1701):
        Column(name="region_opened_eff",
               sql="coalesce(regions.opened_at, signup_at)",
               type=DataType.TIMESTAMP),
    ]
    if extra_columns:
        cols.extend(extra_columns)
    return SlayerModel(
        name="customers_v2", sql_table="customers", data_source="test",
        columns=cols,
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        default_time_dimension="signup_at",
    )


def _orders_x(*, extra_columns=None) -> SlayerModel:
    cols = [
        Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
        Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
        Column(name="amount", sql="amount", type=DataType.DOUBLE),
        Column(name="status", sql="status", type=DataType.TEXT),
        Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
    ]
    if extra_columns:
        cols.extend(extra_columns)
    return SlayerModel(
        name="orders_x", sql_table="orders", data_source="test",
        columns=cols,
        joins=[ModelJoin(target_model="customers_v2", join_pairs=[["customer_id", "id"]])],
        default_time_dimension="created_at",
    )


async def _gen(
    query: SlayerQuery,
    *,
    orders_extra=None,
    customers_extra=None,
    dialect: str = "postgres",
) -> str:
    """Render ``query`` against the orders_x chain and return the SQL."""
    return await _engine_generate(
        query=query,
        model=_orders_x(extra_columns=orders_extra),
        dialect=dialect,
        extra_models=[
            _customers_v2(extra_columns=customers_extra),
            _regions(),
            _countries(),
        ],
    )


# =========================================================================== #
# DEV-1701 — shared-grain derived TIME dimension crossing a further join.
# =========================================================================== #
class TestDev1701SharedGrainDerivedTimeDim:
    """A cross-model aggregate grouped by a joined derived TIME dimension whose
    ``Column.sql`` crosses a FURTHER join must pull that join into BOTH the host
    ``_base`` and the ``_cm_*`` CTE, and anchor the derived expression under the
    canonical path alias — not bare ``regions`` (which is unbound in scope)."""

    async def test_cross_model_agg_grouped_by_crossing_derived_td(self) -> None:
        query = SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="customers_v2.region_opened_eff"),
                granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        base_body = _extract_cte_body(sql, r"_base")
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        # Host base: the derived TD anchors under the path alias + pulls regions.
        assert "customers_v2__regions.opened_at" in base_body, base_body
        assert "LEFT JOIN regions AS customers_v2__regions" in base_body, base_body
        assert "regions.opened_at" not in _norm(base_body).replace(
            "customers_v2__regions.opened_at", ""), base_body
        # CTE: rooted at customers_v2 → the derived TD anchors under the DIRECT
        # regions alias (one hop from the target) and pulls that join.
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "regions.opened_at" in cm_body, cm_body

    async def test_host_base_local_measure_derived_td_crossing(self) -> None:
        """Host-side only (no cross-model agg): a local measure grouped by the
        crossing derived TD must still anchor + pull the join (the host-side
        half of the DEV-1701 fix, unpinned before Stage 4)."""
        query = SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="customers_v2.region_opened_eff"),
                granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="amount:sum")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        assert "LEFT JOIN regions AS customers_v2__regions" in sql, sql
        assert "customers_v2__regions.opened_at" in sql, sql

    async def test_host_base_dim_only_derived_td_crossing(self) -> None:
        """Dim-only query grouped by the crossing derived TD — same anchoring."""
        query = SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="customers_v2.region_opened_eff"),
                granularity=TimeGranularity.MONTH)],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        assert "LEFT JOIN regions AS customers_v2__regions" in sql, sql
        assert "customers_v2__regions.opened_at" in sql, sql


class TestDev1701AnchoringDirectionMatrix:
    """Anchoring-direction coverage (Codex F6): direct / one-hop-joined /
    further-hop-joined / target-rooted derived TIME dims, host + CTE contexts.
    Guards against double-prefixing (``customers_v2__customers_v2__regions``) or
    misrooting an ordinary joined derived TD."""

    async def test_local_derived_td_unaffected(self) -> None:
        """A LOCAL derived TD (no crossing) keeps its host-rooted anchor."""
        orders_extra = [Column(
            name="eff_created", sql="coalesce(created_at, created_at)",
            type=DataType.TIMESTAMP)]
        query = SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="eff_created"),
                granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="amount:sum")],
        )
        sql = await _gen(query, orders_extra=orders_extra)
        assert_scope_closed(sql)
        assert "orders_x.created_at" in sql, sql
        assert "customers_v2__" not in sql, sql

    async def test_one_hop_joined_derived_td_anchors_at_direct_alias(self) -> None:
        """A one-hop joined derived TD (customers_v2.signup_at based) anchors at
        the direct ``customers_v2`` alias, never double-prefixed."""
        customers_extra = [Column(
            name="signup_eff", sql="datetime(signup_at, '+1 month')",
            type=DataType.TIMESTAMP)]
        query = SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="customers_v2.signup_eff"),
                granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="amount:sum")],
        )
        sql = await _gen(query, customers_extra=customers_extra)
        assert_scope_closed(sql)
        assert "customers_v2.signup_at" in sql, sql
        assert "customers_v2__customers_v2" not in sql, sql

    async def test_no_double_prefix_on_further_hop(self) -> None:
        """The DEV-1701 further-hop fix never emits a doubled path alias."""
        query = SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="customers_v2.region_opened_eff"),
                granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="amount:sum")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        # Positive: the corrected expression + join are present (Codex finding 8
        # — a pure negative could pass if the ref vanished entirely).
        assert "customers_v2__regions.opened_at" in sql, sql
        assert "LEFT JOIN regions AS customers_v2__regions" in sql, sql
        # Negative: no doubled path alias.
        assert "customers_v2__customers_v2__regions" not in sql, sql
        assert "regions__regions" not in sql, sql


# =========================================================================== #
# DEV-1527 cross-model remainder — parametric kwarg naming a derived target col.
# =========================================================================== #
class TestDev1527CrossModelKwarg:
    async def test_qualified_derived_crossing_kwarg_expands_and_pulls_join(self) -> None:
        """``weighted_avg(weight=customers_v2.deep_weight)`` where deep_weight's
        sql is ``regions.weight`` — the kwarg expands to the crossing expression
        and the CTE pulls the regions join."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(
                formula="customers_v2.lifetime_value:weighted_avg(weight=customers_v2.deep_weight)")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "regions.weight" in cm_body, cm_body
        # The broken bare-column form must not appear.
        assert "customers_v2.deep_weight" not in cm_body, cm_body

    async def test_qualified_plain_kwarg_unchanged(self) -> None:
        """A non-crossing qualified kwarg (``weight=customers_v2.id``) still
        renders the bare target column with no spurious join."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(
                formula="customers_v2.lifetime_value:weighted_avg(weight=customers_v2.id)")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "customers_v2.id" in cm_body, cm_body
        assert "LEFT JOIN regions" not in cm_body, cm_body

    async def test_bare_crossing_kwarg_still_bind_errors(self) -> None:
        """The bare form ``weight=deep_weight`` (no relation qualifier) resolves
        against the HOST scope by DSL rule and raises at bind time — the
        supported form is the qualified one. Pinned so Stage 4 doesn't silently
        widen the bare-name surface."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(
                formula="customers_v2.lifetime_value:weighted_avg(weight=deep_weight)")],
        )
        with pytest.raises(UnknownReferenceError):
            await _gen(query)


# =========================================================================== #
# DEV-1702 B2 (forward variant) — first/last crossing value materialisation.
# =========================================================================== #
class TestDev1702B2ForwardMaterialization:
    async def test_forward_last_crossing_source_materialized_in_subquery(self) -> None:
        """Forward ``last`` whose source sql crosses a join: the crossed value is
        materialised as a ``_val_<n>`` column INSIDE the ranked subquery, and the
        outer aggregate references the bare alias (never the raw crossing ref)."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(
                formula="customers_v2.deep_pop:last(orders_x.created_at)")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        norm = _norm(cm_body)
        # Ranked subquery present, pulls the further join inside it.
        assert "ROW_NUMBER()" in norm, norm
        outer, inner = _split_at_ranked_subquery(norm)
        assert "LEFT JOIN regions AS regions" in inner, inner
        # Law 2: value materialised as _val_N inside the subquery; the outer
        # MAX(...) references the alias (optionally relation-qualified against
        # the ranked subquery alias), not the crossing ref.
        assert _re.search(r"regions\.population AS _val_\d+", inner), inner
        assert _re.search(
            r"MAX\(CASE WHEN _last_rn = 1 THEN (?:\w+\.)?_val_\d+", outer), outer
        assert "regions.population" not in outer, outer

    async def test_forward_last_crossing_time_arg_registers_join(self) -> None:
        """Codex F1: the explicit positional TIME arg of a first/last can itself
        cross a join (``region_opened_eff`` = ``coalesce(regions.opened_at, …)``).
        The arg must resolve through the ranked subquery's scope so its join is
        pulled INSIDE the subquery and the ORDER BY references the anchored ref
        — not a bare unbound ``regions.opened_at``."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(
                formula="customers_v2.lifetime_value:last(customers_v2.region_opened_eff)")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        norm = _norm(cm_body)
        _outer, inner = _split_at_ranked_subquery(norm)
        # The time arg's crossed join is registered inside the ranked subquery,
        # and the ORDER BY ranks on the anchored expression.
        assert "LEFT JOIN regions AS regions" in inner, inner
        assert "ORDER BY" in inner, inner
        assert "regions.opened_at" in inner, inner

    async def test_forward_last_crossing_value_and_filter(self) -> None:
        """Codex F3: a forward first/last whose source sql crosses one join AND
        whose ``Column.filter`` crosses a DIFFERENT (deeper) join — both are
        independently resolved through the ranked subquery's scope, so BOTH
        joins land inside it and NO raw crossing ref leaks into the CTE's outer
        aggregate scope. Distinct expressions (value = regions.population,
        filter = regions__countries.gdp) prove independent resolution: an
        implementation that resolves only the value would leave the countries
        join unregistered."""
        customers_extra = [Column(
            name="deep_pop_flt", sql="regions.population", type=DataType.DOUBLE,
            filter="regions__countries.gdp > 5")]
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(
                formula="customers_v2.deep_pop_flt:last(orders_x.created_at)")],
        )
        sql = await _gen(query, customers_extra=customers_extra)
        assert_scope_closed(sql)
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        norm = _norm(cm_body)
        outer, inner = _split_at_ranked_subquery(norm)
        # BOTH the value's join (regions) and the filter's deeper join
        # (regions → countries) resolve inside the ranked subquery.
        assert "LEFT JOIN regions AS regions" in inner, inner
        assert "LEFT JOIN countries AS regions__countries" in inner, inner
        # Neither crossing ref leaks into the outer aggregate scope.
        assert "regions.population" not in outer, outer
        assert "regions__countries.gdp" not in outer, outer

    async def test_routed_having_binds_same_val_alias(self) -> None:
        """A HAVING on the forward crossing first/last measure binds to the same
        rn state + materialised value the projection uses (no raw ref leak)."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.status"],
            measures=[ModelMeasure(
                formula="customers_v2.deep_pop:last(customers_v2.signup_at)")],
            filters=["customers_v2.deep_pop:last(customers_v2.signup_at) > 5"],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        norm = _norm(cm_body)
        assert "HAVING" in norm, norm
        # The HAVING aggregate must bind to the materialised value alias, not
        # the raw crossing column (which is out of scope in the CTE's outer).
        having = norm[norm.find("HAVING"):]
        assert "regions.population" not in having, having
        assert _re.search(r"_val_\d+", having), having

    async def test_compound_routed_having_nested_arith_registers_join(self) -> None:
        """Codex F4: the routed-filter pre-pass walks the FULL ValueKey tree.
        A HAVING wrapping the crossing aggregate in a nested arithmetic node
        (``deep_pop:sum + 1 > 5`` → ArithmeticKey(>) → ArithmeticKey(+) →
        AggregateKey) must still discover and register the aggregate leaf's
        crossed join BEFORE the CTE FROM is built."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.status"],
            measures=[ModelMeasure(formula="customers_v2.deep_pop:sum")],
            filters=["customers_v2.deep_pop:sum + 1 > 5"],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        norm = _norm(cm_body)
        assert "LEFT JOIN regions AS regions" in norm, norm
        having = norm[norm.find("HAVING"):]
        assert "SUM(regions.population) + 1 > 5" in having, having

    async def test_local_value_first_last_no_spurious_materialization(self) -> None:
        """A forward first/last whose source is LOCAL to the target emits NO
        ``_val_`` materialisation (the value is already star-projected)."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.status"],
            measures=[ModelMeasure(
                formula="customers_v2.lifetime_value:last(customers_v2.signup_at)")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "_val_" not in cm_body, cm_body
        assert "customers_v2.lifetime_value" in cm_body, cm_body


# =========================================================================== #
# Routed WHERE filter on a derived crossing target column.
# =========================================================================== #
class TestRoutedFilterDerivedCrossing:
    async def test_where_routed_derived_crossing_filter_pulls_join(self) -> None:
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
            filters=["customers_v2.deep_pop > 5"],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "regions.population > 5" in _norm(cm_body), cm_body

    async def test_scalar_call_wrapped_crossing_filter_pulls_join(self) -> None:
        """Codex F4 follow-up: a routed filter wrapping a crossing derived column
        in a scalar call (``abs(customers_v2.deep_pop) > 5``) must expand the arg,
        emit a real ``ABS(regions.population)`` predicate, and pull the join —
        not fall through to a stringified-key literal."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
            filters=["abs(customers_v2.deep_pop) > 5"],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "ABS(regions.population) > 5" in cm_body, cm_body
        # The broken stringified-key literal must not appear.
        assert "ColumnSqlKey" not in cm_body, cm_body


# =========================================================================== #
# Plain derived (non-TIME) shared-grain dim → raise (user-approved).
# =========================================================================== #
class TestDerivedSharedGrainRaises:
    async def test_plain_derived_dim_shared_grain_raises(self) -> None:
        """A PLAIN derived (non-TIME) dimension on the target path used as
        cross-model shared grain raises NotImplementedError (replaces the
        silently-wrong CROSS-JOIN broadcast); full support = DEV-1495-b1."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        with pytest.raises(NotImplementedError, match=r"(?i)shared.grain|derived"):
            await _gen(query)

    async def test_derived_source_unaffected_by_grain_guard(self) -> None:
        """The guard fires ONLY for a shared-grain dim — a derived aggregate
        SOURCE crossing a join is still rendered (DEV-1526 territory)."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.deep_pop:sum")],
        )
        sql = await _gen(query)  # must not raise
        assert_scope_closed(sql)
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "SUM(regions.population)" in cm_body, cm_body

    async def test_derived_filter_unaffected_by_grain_guard(self) -> None:
        """A derived crossing FILTER is not caught by the shared-grain guard —
        and it renders correctly: expanded predicate + pulled join in the CTE."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
            filters=["customers_v2.deep_pop > 1"],
        )
        sql = await _gen(query)  # must not raise
        assert_scope_closed(sql)
        cm_body = _extract_cte_body(sql, r"_cm_\w+")
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "regions.population > 1" in _norm(cm_body), cm_body

    async def test_local_derived_dim_broadcast_unaffected(self) -> None:
        """A HOST-LOCAL derived dim (path=()) still broadcasts via CROSS JOIN —
        the guard is scoped to path-bearing shared-grain dims only."""
        orders_extra = [Column(
            name="amt_bucket", sql="amount * 2", type=DataType.DOUBLE)]
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["amt_bucket"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query, orders_extra=orders_extra)  # must not raise
        assert_scope_closed(sql)
        assert "CROSS JOIN" in sql, sql

    async def test_time_trunc_derived_grain_supported_not_raised(self) -> None:
        """A TimeTrunc-wrapped derived grain gets full support, NOT the raise."""
        query = SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="customers_v2.region_opened_eff"),
                granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query)  # must not raise
        assert_scope_closed(sql)


# =========================================================================== #
# Generation-wide AliasAllocator determinism (Codex F5 mitigation).
# =========================================================================== #
class TestAllocatorDeterminism:
    async def test_repeated_render_byte_identical(self) -> None:
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(
                formula="customers_v2.deep_pop:last(orders_x.created_at)")],
        )
        sql1 = await _gen(query)
        sql2 = await _gen(query)
        assert sql1 == sql2

    async def test_multi_cte_query_stable(self) -> None:
        """A query with two crossing first/last CTEs renders deterministically,
        and no ``_val_`` alias is defined twice within any single CTE body
        (within-scope collision would be a real correctness bug)."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[
                ModelMeasure(formula="customers_v2.deep_pop:last(orders_x.created_at)"),
                ModelMeasure(formula="customers_v2.deep_weight:last(orders_x.created_at)"),
            ],
        )
        sql1 = await _gen(query)
        sql2 = await _gen(query)
        assert sql1 == sql2
        assert_scope_closed(sql1)
        # Within-scope: no CTE body defines the same _val alias twice.
        for pat in (r"_cm_\w*deep_pop\w*", r"_cm_\w*deep_weight\w*"):
            body = _extract_cte_body(sql1, pat)
            vals = _re.findall(r"AS (_val_\d+)", body)
            assert len(vals) == len(set(vals)), f"dup _val in {pat}: {vals}"
        # Design invariant (DEV-1703 D-E generation-wide allocator, per
        # slayer/sql/naming.py): sibling CTEs in one generation never mint the
        # same _val_<n>. Pinned as a regression guard for that locked decision.
        all_vals = _re.findall(r"AS (_val_\d+)", sql1)
        assert len(all_vals) == len(set(all_vals)), f"cross-CTE _val reuse: {all_vals}"


# =========================================================================== #
# Null-safe grain join-back — per-dialect SQL-shape snapshots (Codex F8).
# =========================================================================== #
# Every dialect strategy's expected null-safe form (Codex F8 — classify each).
# ``bigquery`` shares the base ``IS NOT DISTINCT FROM``; it is covered ONLY at
# the strategy level below because its end-to-end join-back alias mangling
# (dotted ``_base."a.b.c"`` → ``_base___a___b.c``) is a separate Stage-9
# (DEV-1713) concern that trips the scope validator regardless of the equality
# form (the same mangling breaks the plain ``=`` join-back).
_NULLSAFE_STRATEGY = {
    "PostgresDialect": "IS NOT DISTINCT FROM",
    "DuckdbDialect": "IS NOT DISTINCT FROM",
    "SnowflakeDialect": "IS NOT DISTINCT FROM",
    "BigqueryDialect": "IS NOT DISTINCT FROM",
    "TrinoDialect": "IS NOT DISTINCT FROM",
    "PrestoDialect": "IS NOT DISTINCT FROM",
    "DatabricksDialect": "IS NOT DISTINCT FROM",
    "SparkDialect": "IS NOT DISTINCT FROM",
    "ClickhouseDialect": "IS NOT DISTINCT FROM",
    "MysqlDialect": "<=>",
    "SqliteDialect": " IS ",
    "TsqlDialect": "= t2.a OR (t1.a IS NULL AND t2.a IS NULL)",
    "OracleDialect": "= t2.a OR (t1.a IS NULL AND t2.a IS NULL)",
    "RedshiftDialect": "= t2.a OR (t1.a IS NULL AND t2.a IS NULL)",
}


class TestNullSafeDialectStrategy:
    """Codex F8: every configured ``SqlDialect`` strategy emits the correct
    null-safe equality from ``build_null_safe_eq`` (unit-level, engine-free —
    covers BigQuery, whose end-to-end join-back is Stage-9)."""

    @pytest.mark.parametrize("cls_name,marker", sorted(_NULLSAFE_STRATEGY.items()))
    def test_strategy_null_safe_form(self, cls_name: str, marker: str) -> None:
        import slayer.sql.dialects as dialects_mod
        from sqlglot import exp

        cls = getattr(dialects_mod, cls_name)
        dialect = cls()
        left = exp.column("a", table="t1")
        right = exp.column("a", table="t2")
        rendered = dialect.build_null_safe_eq(left, right).sql(
            dialect=dialect.sqlglot_name)
        assert marker in rendered, f"{cls_name}: {rendered!r}"


# Native single-token null-safe forms (engine-level end-to-end wiring).
_NULLSAFE_NATIVE = {
    "postgres": "IS NOT DISTINCT FROM",
    "duckdb": "IS NOT DISTINCT FROM",
    "snowflake": "IS NOT DISTINCT FROM",
    "trino": "IS NOT DISTINCT FROM",
    "databricks": "IS NOT DISTINCT FROM",
    "clickhouse": "IS NOT DISTINCT FROM",
    "mysql": "<=>",
    "sqlite": " IS ",
}
# Dialects with no native null-safe equality → expanded predicate.
_NULLSAFE_EXPANDED = ("tsql", "oracle", "redshift")


class TestNullSafeJoinBackDialects:
    @pytest.mark.parametrize("dialect,marker", sorted(_NULLSAFE_NATIVE.items()))
    async def test_join_back_native_null_safe(
        self, dialect: str, marker: str
    ) -> None:
        """The grain join-back ON predicate uses the dialect's native null-safe
        equality (asserted against the extracted ``LEFT JOIN _cm_* ON`` clause,
        not a loose whole-SQL scan — Codex finding 5)."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.status"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query, dialect=dialect)
        on_pred = _joinback_on_predicate(sql, dialect=dialect)
        # The native null-safe token in the ON predicate itself proves the
        # join-back is not a plain `col = col`.
        assert marker in on_pred, f"[{dialect}] ON: {on_pred!r}"

    @pytest.mark.parametrize("dialect", _NULLSAFE_EXPANDED)
    async def test_join_back_expanded_null_safe(self, dialect: str) -> None:
        """Dialects without a native form emit the full expanded predicate
        ``a = b OR (a IS NULL AND b IS NULL)`` (Codex finding 9 — assert the
        whole structure, not just ``IS NULL``)."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.status"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query, dialect=dialect)
        on_pred = _norm(_joinback_on_predicate(sql, dialect=dialect)).upper()
        assert " OR " in on_pred, on_pred
        assert on_pred.count(" IS NULL") == 2, on_pred
        assert " AND " in on_pred, on_pred
        # A bare equality (both sides non-null) is still one arm of the OR.
        assert "=" in on_pred, on_pred


# =========================================================================== #
# Null-safe grain join-back — SQLite EXECUTION (NULL dim retains its row).
# =========================================================================== #
def _seed_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, population REAL)"
    )
    cur.executemany(
        "INSERT INTO regions VALUES (?,?,?)",
        # region 2 has a NULL name — the nullable grain the rerooted join-back
        # test groups on.
        [(1, "NA", 100.0), (2, None, 200.0)],
    )
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "lifetime_value REAL, status TEXT)"
    )
    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?)",
        [
            (1, 1, 10.0, "active"),
            (2, 2, 20.0, None),   # NULL status — the grain value under test
            (3, 1, 30.0, None),   # NULL status
        ],
    )
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "amount REAL, status TEXT)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?)",
        [
            (1, 1, 5.0, "paid"),
            (2, 2, 7.0, "paid"),
            (3, 3, 9.0, "paid"),
        ],
    )
    con.commit()
    con.close()


@pytest.fixture
async def sqlite_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "t.db")
        _seed_sqlite(db_path)
        storage = YAMLStorage(base_dir=os.path.join(d, "store"))
        await storage.save_datasource(
            DatasourceConfig(name="test", type="sqlite", database=db_path)
        )
        # SQLite-shaped models (no schema qualifier).
        await storage.save_model(SlayerModel(
            name="regions", sql_table="regions", data_source="test",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="name", type=DataType.TEXT),
                Column(name="population", type=DataType.DOUBLE),
            ],
        ))
        await storage.save_model(SlayerModel(
            name="customers_v2", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region_id", type=DataType.INT),
                Column(name="lifetime_value", type=DataType.DOUBLE),
                Column(name="status", type=DataType.TEXT),
                Column(name="deep_pop", sql="regions.population", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
        ))
        await storage.save_model(SlayerModel(
            name="orders_x", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="status", type=DataType.TEXT),
            ],
            joins=[ModelJoin(target_model="customers_v2", join_pairs=[["customer_id", "id"]])],
        ))
        yield SlayerQueryEngine(storage=storage)


class TestNullSafeJoinBackExecution:
    async def test_null_grain_retains_aggregate_forward(self, sqlite_engine) -> None:
        """Group by a nullable target dim (customers_v2.status) with a cross-model
        aggregate: the NULL-status group must retain its joined-back aggregate,
        not drop to NULL (the whole point of the null-safe join-back)."""
        resp = await sqlite_engine.execute(SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.status"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        ))
        by_status = {r["orders_x.customers_v2.status"]: r for r in resp.data}
        assert None in by_status, resp.data
        null_row = by_status[None]
        # customers 2 & 3 both have NULL status; their lifetime_values (20 + 30)
        # must survive the join-back.
        assert null_row["orders_x.customers_v2.lifetime_value_sum"] == 50.0, resp.data

    async def test_null_grain_retains_aggregate_filtered_local(self, sqlite_engine) -> None:
        """Same, for a filtered-local (host-rooted, DEV-1503) isolation CTE
        join-back. The filter references a joined table (customers_v2) so the
        measure isolates into a rerooted CTE joined back on the nullable
        ``customers_v2.status`` grain. The NULL-status group (customers 2 & 3,
        both ltv > 15) must retain amount 7+9 = 16 — a plain ``=`` join-back
        drops it to NULL."""
        await sqlite_engine.storage.save_model(SlayerModel(
            name="orders_x", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="status", type=DataType.TEXT),
                # Filter references a JOINED table → DEV-1503 filtered-local
                # isolation. High-ltv customers (2 & 3) have NULL status.
                Column(name="amt_hi", sql="amount", type=DataType.DOUBLE,
                       filter="customers_v2.lifetime_value > 15"),
            ],
            joins=[ModelJoin(target_model="customers_v2", join_pairs=[["customer_id", "id"]])],
        ))
        resp = await sqlite_engine.execute(SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.status"],
            measures=[ModelMeasure(formula="amt_hi:sum")],
        ))
        by_status = {r["orders_x.customers_v2.status"]: r for r in resp.data}
        assert None in by_status, resp.data
        # customers 2 (order amt 7) + 3 (order amt 9), both ltv > 15, NULL status.
        assert by_status[None]["orders_x.amt_hi_sum"] == 16.0, resp.data

    async def test_null_grain_retains_aggregate_rerooted(self, sqlite_engine) -> None:
        """Rerooted (target-rooted, DEV-1450 C1) join-back on a nullable grain.
        Grouping a cross-model aggregate by ``customers_v2.regions.name`` (a
        multi-hop dim reachable from the target's own join graph) re-roots the
        CTE at customers_v2 and joins back on ``regions.name``. Region 2 has a
        NULL name; its customer's lifetime_value (20) must survive the
        null-safe join-back rather than dropping."""
        resp = await sqlite_engine.execute(SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.regions.name"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        ))
        by_name = {
            r["orders_x.customers_v2.regions.name"]: r for r in resp.data
        }
        assert None in by_name, resp.data
        # customer 2 (region 2, NULL name) has lifetime_value 20.
        assert by_name[None]["orders_x.customers_v2.lifetime_value_sum"] == 20.0, resp.data
