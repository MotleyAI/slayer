"""DEV-1728 — render a cross-model aggregate grouped by a joined DERIVED
dimension (shared-grain rendering).

DEV-1708 gated a plain derived (non-time) dimension used as a cross-model
shared grain behind ``NotImplementedError`` (``derived_shared_grain_not_
implemented``): before DEV-1713 fixed the naming half, the host aliased the
grain flat while the CTE join-back expected the dotted form, so silently
excluding it would CROSS-JOIN-broadcast the global aggregate across groups.

DEV-1713 landed the dotted host key (``orders_x.customers_v2.<derived>``), so
the gate is now removable. This module pins FULL support (user-approved scope):

* **Target-local** derived grain (``ltv_x2 = lifetime_value * 2``) — expanded
  inside the ``_cm_*`` CTE rooted at the target, projected under the dotted
  host alias, added to GROUP BY, joined back null-safe.
* **Further-join-crossing** derived grain (``deep_pop = regions.population``,
  ``deep_gdp = regions__countries.gdp``) — the CTE pulls the further join(s)
  into its FROM (reusing the DEV-1701 machinery) and groups by the expanded
  expression.
* **first / last** aggregates over a derived grain — a target-local grain needs
  no materialisation (its refs are re-exported by ``target.*``); a CROSSING
  grain is materialised as a ``_val_<n>`` projection INSIDE the ranked subquery
  (Law 2) so the outer SELECT / GROUP BY reference the alias, not a table bound
  only inside the subquery. This also fixes the confirmed live bug where a
  crossing derived TIME grain + first/last emitted invalid SQL.
* **CAST consistency** — a typed non-bare derived grain (e.g. ``INT``) is
  wrapped identically in ``_base`` and ``_cm_*`` so the join-back compares
  identically-typed values (a mismatch silently drops groups).
* **Sibling paths** — the re-rooted (target-rooted) and filtered-local
  (host-rooted, DEV-1503/1709) CTE variants join back correctly on a derived
  grain (newly reachable once the gate is gone).
* **Intermediate-hop** derived grain still raises the pre-existing 7b.12
  ``NotImplementedError`` (uniform with base columns — out of scope).

Every SQL-shape test asserts ``assert_scope_closed`` on the emitted statement.
Executed-value coverage runs on SQLite (this file, unit suite) with a DuckDB
mirror in ``tests/integration/test_integration_duckdb.py``.
"""

from __future__ import annotations

import os
import re as _re
import sqlite3
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType, TimeGranularity
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

from tests._cross_model_chain import (
    _extract_cte_body,
    _gen,
    _joinback_on_predicate,
    _norm,
    _split_at_ranked_subquery,
)

pytestmark = pytest.mark.asyncio


# =========================================================================== #
# Forward _cm_* CTE — derived shared grain SQL shape.
# =========================================================================== #
class TestForwardDerivedGrainShape:
    async def test_target_local_derived_grain_renders(self) -> None:
        """Target-local derived grain (``ltv_x2 = lifetime_value * 2``): the CTE
        expands it rooted at the target, projects it under the dotted host alias,
        groups by it, and joins back null-safe — no CROSS-JOIN broadcast."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.ltv_x2"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        # Expanded expression present in SELECT + GROUP BY under the dotted alias.
        assert "customers_v2.lifetime_value * 2" in cm_body, cm_body
        assert 'AS "orders_x.customers_v2.ltv_x2"' in cm_body, cm_body
        assert "GROUP BY" in cm_body, cm_body
        # Real shared grain — not the broadcast fallback.
        assert "CROSS JOIN" not in sql, sql
        # Null-safe join-back on the dotted alias.
        on_pred = _joinback_on_predicate(sql)
        assert "orders_x.customers_v2.ltv_x2" in on_pred, on_pred
        assert "IS NOT DISTINCT FROM" in on_pred, on_pred

    async def test_crossing_one_hop_derived_grain_pulls_join(self) -> None:
        """A one-hop crossing derived grain (``deep_pop = regions.population``):
        the CTE pulls ``LEFT JOIN regions`` and groups by ``regions.population``;
        the host base pulls the join under its ``__`` path alias."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "regions.population" in cm_body, cm_body
        assert 'AS "orders_x.customers_v2.deep_pop"' in cm_body, cm_body
        base_body = _norm(_extract_cte_body(sql, r"_base"))
        assert "customers_v2__regions" in base_body, base_body
        assert "CROSS JOIN" not in sql, sql

    async def test_crossing_two_hop_derived_grain_pulls_both_joins(self) -> None:
        """A two-hop crossing derived grain (``deep_gdp = regions__countries.gdp``)
        pulls BOTH joins into the CTE FROM, rooted at the target."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_gdp"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "LEFT JOIN countries AS regions__countries" in cm_body, cm_body
        assert "regions__countries.gdp" in cm_body, cm_body
        assert 'AS "orders_x.customers_v2.deep_gdp"' in cm_body, cm_body

    async def test_typed_derived_grain_cast_consistent_base_and_cte(self) -> None:
        """A typed non-bare derived grain (``ltv_third`` = INT arithmetic) is
        wrapped in the SAME ``CAST(... AS INT)`` in both ``_base`` and ``_cm_*``
        so the join-back compares identically-typed values (Codex F5)."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.ltv_third"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        base_body = _norm(_extract_cte_body(sql, r"_base"))
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        # Both sides CAST the expanded grain expression to INT.
        assert "CAST(customers_v2.lifetime_value / 3.0 AS INT)" in cm_body, cm_body
        assert "AS INT)" in base_body, base_body
        assert "/ 3.0 AS INT)" in base_body, base_body

    async def test_grain_equals_aggregate_source_derived(self) -> None:
        """The derived grain dim and the aggregate source can be the SAME derived
        column (``deep_pop`` dim + ``deep_pop:sum``) — grouped and aggregated on
        the same expanded expression, one pulled join."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="customers_v2.deep_pop:sum")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "SUM(regions.population)" in cm_body, cm_body
        assert 'AS "orders_x.customers_v2.deep_pop"' in cm_body, cm_body
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body

    async def test_non_sum_aggregate_over_derived_grain(self) -> None:
        """A non-sum plain aggregate (``count_distinct``) over a derived grain
        renders through the same path (Codex F3 reduced)."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.ltv_x2"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:count_distinct")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "customers_v2.lifetime_value * 2" in cm_body, cm_body
        assert "COUNT(DISTINCT" in cm_body.upper(), cm_body

    async def test_two_aggregates_share_one_derived_grain(self) -> None:
        """Two cross-model aggregates grouped by the same derived grain produce
        two CTEs, each grouped on the grain and joined back on it."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.ltv_x2"],
            measures=[
                ModelMeasure(formula="customers_v2.lifetime_value:sum"),
                ModelMeasure(formula="customers_v2.region_id:max"),
            ],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        # Two _cm_ CTEs, both grouped by the derived grain.
        cte_names = _re.findall(r"(_cm_\w+) AS \(", sql)
        assert len(cte_names) == 2, cte_names
        for name in cte_names:
            body = _norm(_extract_cte_body(sql, _re.escape(name)))
            assert "customers_v2.lifetime_value * 2" in body, (name, body)
        # Both join back on the dotted derived alias.
        assert sql.count('"orders_x.customers_v2.ltv_x2"') >= 3, sql

    async def test_where_filter_on_derived_grain_dim(self) -> None:
        """A WHERE filter on the projected derived grain dim renders alongside
        the grain (the grain still projects + groups; the query is scope-closed)."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
            filters=["customers_v2.deep_pop > 50"],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "regions.population" in cm_body, cm_body
        assert 'AS "orders_x.customers_v2.deep_pop"' in cm_body, cm_body
        # The filter predicate must actually survive into the rendered SQL —
        # without this the test passes even if the generator drops the filter.
        assert "regions.population > 50" in _norm(sql), sql

    async def test_intermediate_hop_derived_grain_still_raises(self) -> None:
        """A derived grain on an INTERMEDIATE hop of a multi-hop target path
        still hits the pre-existing 7b.12 NotImplementedError (uniform with base
        columns — out of scope for DEV-1728)."""
        # Aggregate targets the TWO-hop path customers_v2 → regions; the grain is
        # a derived dim on the intermediate customers_v2 hop.
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.ltv_x2"],
            measures=[ModelMeasure(formula="customers_v2.regions.population:sum")],
        )
        with pytest.raises(NotImplementedError, match=r"(?i)intermediate|7b\.12"):
            await _gen(query)


# =========================================================================== #
# first / last over a derived shared grain (Law-2 materialisation).
# =========================================================================== #
class TestFirstLastDerivedGrain:
    async def test_target_local_grain_no_materialisation(self) -> None:
        """A first/last aggregate grouped by a TARGET-LOCAL derived grain needs
        NO ``_val_`` materialisation — the grain's refs are re-exported by
        ``target.*`` inside the ranked subquery (byte-parity with a base col)."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.ltv_x2"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:last")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        # The grain expression drives PARTITION BY and outer GROUP BY directly.
        assert "customers_v2.lifetime_value * 2" in cm_body, cm_body
        assert "PARTITION BY" in cm_body, cm_body
        # No spurious grain materialisation for a target-local grain.
        assert "_val_" not in cm_body, cm_body

    async def test_crossing_grain_materialised_in_ranked_subquery(self) -> None:
        """A first/last aggregate grouped by a CROSSING derived grain
        (``deep_pop``): the grain is materialised as a ``_val_<n>`` projection
        INSIDE the ranked subquery; the outer SELECT / GROUP BY reference the
        alias, and PARTITION BY uses the raw crossing expression."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:last")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        outer, inner = _split_at_ranked_subquery(cm_body)
        # F1: the crossed join lives INSIDE the ranked subquery, not outside.
        assert "LEFT JOIN regions AS regions" in inner, inner
        assert "LEFT JOIN regions" not in outer, outer
        # The crossing grain is materialised as a _val_ alias in the subquery,
        # partitioned on the raw expression.
        assert _re.search(r"regions\.population AS _val_\d+", inner), inner
        assert "PARTITION BY" in inner, inner
        assert "regions.population" in inner, inner
        # The outer SELECT/GROUP BY reference the _val_ alias, never bare
        # regions.population (which is out of scope outside the subquery).
        assert _re.search(r"_val_\d+ AS \"orders_x\.customers_v2\.deep_pop\"", outer), outer
        assert "regions.population" not in outer, outer

    async def test_crossing_time_grain_first_last_regression(self) -> None:
        """Regression: a crossing derived TIME grain + first/last previously
        emitted invalid SQL (the ranked subquery re-exported only target.* so the
        outer DATE_TRUNC referenced an unbound ``regions.opened_at``). The grain
        must now be materialised / scope-closed."""
        query = SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="customers_v2.region_opened_eff"),
                granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:last")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        outer, inner = _split_at_ranked_subquery(cm_body)
        assert "LEFT JOIN regions AS regions" in inner, inner
        # The crossing derived TIME grain is materialised as a _val_ projection
        # INSIDE the ranked subquery (its DATE_TRUNC over the coalesce crosses
        # the regions join), so its raw expression stays where regions is bound.
        assert _re.search(r"AS _val_\d+", inner), inner
        assert "regions.opened_at" in inner, inner
        # The outer SELECT / GROUP BY reference the _val_ alias, never the bare
        # crossing ref (the confirmed live-bug leak).
        assert _re.search(
            r"_val_\d+ AS \"orders_x\.customers_v2\.region_opened_eff\"", outer), outer
        assert "regions.opened_at" not in outer, outer

    async def test_typed_crossing_grain_first_last_cast(self) -> None:
        """A TYPED non-bare crossing derived grain (``deep_pop_x2`` = DOUBLE
        arithmetic) + first/last: the materialised ``_val_`` body carries the
        SAME ``CAST(... AS DOUBLE PRECISION)`` wrapper the host base applies
        (Codex F5 typed variant) — not a bare, un-cast expression."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop_x2"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:last")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        outer, inner = _split_at_ranked_subquery(cm_body)
        # The materialised value is the CAST-wrapped arithmetic expression
        # (identical to the host base's wrapping); the outer projection + the
        # subquery PARTITION BY both go through that same cast.
        assert _re.search(
            r"CAST\(regions\.population \* 2 AS DOUBLE PRECISION\) AS _val_\d+",
            inner), inner
        assert "PARTITION BY" in inner, inner
        assert "CAST(regions.population * 2 AS DOUBLE PRECISION)" in inner, inner
        assert _re.search(
            r"_val_\d+ AS \"orders_x\.customers_v2\.deep_pop_x2\"", outer), outer
        # No un-cast bare arithmetic leaked past the materialisation.
        assert "regions.population * 2 AS \"orders_x" not in outer, outer

    async def test_repeated_render_byte_identical(self) -> None:
        """The first/last crossing-grain materialisation is deterministic."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:last")],
        )
        sql1 = await _gen(query)
        sql2 = await _gen(query)
        assert sql1 == sql2

    async def test_val_alias_avoids_target_column_collision(self) -> None:
        """Codex F6: the ranked subquery re-exports ``target.*``. If the target
        model has a real column literally named ``_val_0``, the grain
        materialisation must NOT mint that same name (it would make the outer
        ``_val_0`` reference ambiguous). The allocator reserves the target's
        physical column names, so the minted alias walks past ``_val_0``."""
        customers_extra = [Column(name="_val_0", sql="1", type=DataType.INT)]
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:last")],
        )
        sql = await _gen(query, customers_extra=customers_extra)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        # The materialised grain must use a NON-colliding alias (not _val_0).
        vals = _re.findall(r"AS (_val_\d+)", cm_body)
        assert vals, cm_body
        assert "_val_0" not in vals, cm_body

    async def test_val_alias_avoids_physical_column_collision(self) -> None:
        """The star-projection exports PHYSICAL column names, which differ from
        the semantic name whenever a column renames its source. A column
        ``Column(name="renamed", sql="_val_0")`` exports ``_val_0`` through
        ``target.*``, so the allocator must reserve the physical name too — not
        just ``c.name`` (Codex)."""
        customers_extra = [Column(name="renamed", sql="_val_0", type=DataType.INT)]
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:last")],
        )
        sql = await _gen(query, customers_extra=customers_extra)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        vals = _re.findall(r"AS (_val_\d+)", cm_body)
        assert vals, cm_body
        assert "_val_0" not in vals, cm_body


# =========================================================================== #
# Sibling render paths — re-rooted + filtered-local (newly reachable).
# =========================================================================== #
class TestSiblingPathsDerivedGrain:
    async def test_rerooted_with_derived_on_path_dim(self) -> None:
        """A cross-model aggregate grouped by an ON-PATH derived dim AND an
        OFF-PATH dim re-roots the CTE at the target; both grains join back on
        their dotted aliases (DEV-1713 naming on both sides)."""
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop", "customers_v2.regions.name"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        # BOTH dotted grains participate in the re-rooted join-back: the ON
        # predicate null-safe-compares each (asserting on the extracted ON
        # clause, not a loose whole-SQL scan — Codex F-rerooted).
        on_pred = _joinback_on_predicate(sql)
        assert "orders_x.customers_v2.deep_pop" in on_pred, on_pred
        assert "orders_x.customers_v2.regions.name" in on_pred, on_pred
        assert on_pred.count("IS NOT DISTINCT FROM") >= 2, on_pred

    async def test_filtered_local_with_derived_grain(self) -> None:
        """A DEV-1503 filtered-local (HOST-rooted) isolation grouped by a derived
        joined dim: the host-rooted sub-plan carries the derived grain (rooted at
        the host's ``__`` path alias, ``customers_v2__regions``) and the host
        base joins back on its dotted alias. This path never hit the DEV-1708
        gate, so it is a regression guard — must pass before AND after the fix."""
        # amt_hi's filter references a joined table → filtered-local isolation.
        orders_extra = [Column(
            name="amt_hi", sql="amount", type=DataType.DOUBLE,
            filter="customers_v2.lifetime_value > 15")]
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="amt_hi:sum")],
        )
        sql = await _gen(query, orders_extra=orders_extra)
        assert_scope_closed(sql)
        assert '"orders_x.customers_v2.deep_pop"' in sql, sql
        # Null-safe join-back on the derived grain.
        on_pred = _joinback_on_predicate(sql)
        assert "orders_x.customers_v2.deep_pop" in on_pred, on_pred
        assert "IS NOT DISTINCT FROM" in on_pred, on_pred
        # The filtered-local branch was selected: the measure's Column.filter
        # (referencing the joined customers_v2) rides into the isolated CTE.
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "> 15" in cm_body, cm_body
        # The crossing regions join is pulled under the HOST path alias (the
        # sub-plan is rooted at orders_x, not at customers_v2).
        assert "LEFT JOIN regions AS customers_v2__regions" in cm_body, cm_body


# =========================================================================== #
# Existing-behaviour guards that must stay green after the gate removal.
# =========================================================================== #
class TestUnchangedBehaviourGuards:
    async def test_host_local_derived_dim_still_broadcasts(self) -> None:
        """A HOST-LOCAL derived dim (``path == ()``) still broadcasts via CROSS
        JOIN — the shared-grain rendering is scoped to path-bearing dims."""
        orders_extra = [Column(
            name="amt_bucket", sql="amount * 2", type=DataType.DOUBLE)]
        query = SlayerQuery(
            source_model="orders_x",
            dimensions=["amt_bucket"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query, orders_extra=orders_extra)
        assert_scope_closed(sql)
        assert "CROSS JOIN" in sql, sql

    async def test_derived_source_unaffected(self) -> None:
        """A derived aggregate SOURCE crossing a join (no derived grain) still
        renders (DEV-1526 territory), unchanged by the gate removal."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.deep_pop:sum")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "SUM(regions.population)" in cm_body, cm_body

    async def test_derived_filter_unaffected(self) -> None:
        """A derived crossing FILTER (no derived grain) still renders."""
        query = SlayerQuery(
            source_model="orders_x",
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
            filters=["customers_v2.deep_pop > 1"],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)
        cm_body = _norm(_extract_cte_body(sql, r"_cm_\w+"))
        assert "LEFT JOIN regions AS regions" in cm_body, cm_body
        assert "regions.population > 1" in cm_body, cm_body

    async def test_time_trunc_derived_grain_still_supported(self) -> None:
        """A TimeTrunc-wrapped derived grain keeps full support (unchanged)."""
        query = SlayerQuery(
            source_model="orders_x",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="customers_v2.region_opened_eff"),
                granularity=TimeGranularity.MONTH)],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        )
        sql = await _gen(query)
        assert_scope_closed(sql)


# =========================================================================== #
# SQLite EXECUTION — per-group correctness of the derived-grain join-back.
# =========================================================================== #
def _seed_sqlite(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, population REAL)"
    )
    cur.executemany(
        "INSERT INTO regions VALUES (?,?,?)",
        # region 3 has a NULL population — the nullable derived grain under test.
        [(1, "NA", 100.0), (2, "EU", 200.0), (3, "APAC", None)],
    )
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "lifetime_value REAL, signup_at TEXT)"
    )
    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?)",
        [
            # region 1 (pop 100): two customers, ltv 10 & 30.
            (1, 1, 10.0, "2024-01-01"),
            (2, 1, 30.0, "2024-03-01"),
            # region 2 (pop 200): one customer, ltv 20.
            (3, 2, 20.0, "2024-02-01"),
            # region 3 (pop NULL): one customer, ltv 40 — the NULL-grain group.
            (4, 3, 40.0, "2024-04-01"),
        ],
    )
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "amount REAL, created_at TEXT)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?)",
        [
            (1, 1, 5.0, "2024-01-05"),
            (2, 2, 7.0, "2024-03-05"),
            (3, 3, 9.0, "2024-02-05"),
            (4, 4, 11.0, "2024-04-05"),
        ],
    )
    con.commit()
    con.close()


def _sqlite_models() -> "list[SlayerModel]":
    """SQLite-shaped models (no schema qualifier, no sql= on base columns)."""
    return [
        SlayerModel(
            name="regions", sql_table="regions", data_source="test",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="name", type=DataType.TEXT),
                Column(name="population", type=DataType.DOUBLE),
            ],
        ),
        SlayerModel(
            name="customers_v2", sql_table="customers", data_source="test",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region_id", type=DataType.INT),
                Column(name="lifetime_value", type=DataType.DOUBLE),
                Column(name="signup_at", type=DataType.TIMESTAMP),
                Column(name="ltv_x2", sql="lifetime_value * 2", type=DataType.DOUBLE),
                Column(name="ltv_third", sql="lifetime_value / 3.0", type=DataType.INT),
                Column(name="deep_pop", sql="regions.population", type=DataType.DOUBLE),
            ],
            joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
            default_time_dimension="signup_at",
        ),
        SlayerModel(
            name="orders_x", sql_table="orders", data_source="test",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="created_at", type=DataType.TIMESTAMP),
                # Filter references a JOINED table → DEV-1503 filtered-local
                # isolation (used by the filtered-local + derived-grain test).
                Column(name="amt_hi", sql="amount", type=DataType.DOUBLE,
                       filter="customers_v2.lifetime_value > 15"),
            ],
            joins=[ModelJoin(target_model="customers_v2", join_pairs=[["customer_id", "id"]])],
            default_time_dimension="created_at",
        ),
    ]


@pytest.fixture
async def sqlite_engine() -> AsyncIterator[SlayerQueryEngine]:
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "t.db")
        _seed_sqlite(db_path)
        storage = YAMLStorage(base_dir=os.path.join(d, "store"))
        await storage.save_datasource(
            DatasourceConfig(name="test", type="sqlite", database=db_path)
        )
        for m in _sqlite_models():
            await storage.save_model(m)
        yield SlayerQueryEngine(storage=storage)


class TestDerivedGrainExecution:
    async def test_target_local_grain_per_group_values(self, sqlite_engine) -> None:
        """Group a cross-model sum by a target-local derived grain
        (``ltv_x2``): each distinct ltv*2 is its own group with the right sum."""
        resp = await sqlite_engine.execute(SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.ltv_x2"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        ))
        by_grain = {r["orders_x.customers_v2.ltv_x2"]: r for r in resp.data}
        # Four distinct grains, one per customer — no group swallowed.
        assert len(resp.data) == 4, resp.data
        # ltv 10→grain 20 (customer 1), 30→60 (cust 2), 20→40 (cust 3), 40→80 (cust 4).
        assert by_grain[20.0]["orders_x.customers_v2.lifetime_value_sum"] == 10.0, resp.data
        assert by_grain[60.0]["orders_x.customers_v2.lifetime_value_sum"] == 30.0, resp.data
        assert by_grain[40.0]["orders_x.customers_v2.lifetime_value_sum"] == 20.0, resp.data
        assert by_grain[80.0]["orders_x.customers_v2.lifetime_value_sum"] == 40.0, resp.data

    async def test_crossing_grain_per_group_values(self, sqlite_engine) -> None:
        """Group a cross-model sum by a crossing derived grain (``deep_pop`` =
        regions.population): region 1 (pop 100) has ltv 10+30 = 40."""
        resp = await sqlite_engine.execute(SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        ))
        by_pop = {r["orders_x.customers_v2.deep_pop"]: r for r in resp.data}
        assert by_pop[100.0]["orders_x.customers_v2.lifetime_value_sum"] == 40.0, resp.data
        assert by_pop[200.0]["orders_x.customers_v2.lifetime_value_sum"] == 20.0, resp.data

    async def test_null_grain_retains_aggregate(self, sqlite_engine) -> None:
        """A NULL derived-grain group (region 3, population NULL) retains its
        joined-back aggregate via the null-safe join-back, not dropping to NULL."""
        resp = await sqlite_engine.execute(SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        ))
        by_pop = {r["orders_x.customers_v2.deep_pop"]: r for r in resp.data}
        assert None in by_pop, resp.data
        # customer 4 (region 3, NULL pop) has lifetime_value 40.
        assert by_pop[None]["orders_x.customers_v2.lifetime_value_sum"] == 40.0, resp.data

    async def test_typed_grain_cast_consistency_no_drop(self, sqlite_engine) -> None:
        """A typed INT derived grain (``ltv_third = lifetime_value / 3.0``): the
        CAST must be identical on both join-back sides, else the INT-vs-float
        mismatch silently drops every group. ltv 10/3 → INT 3."""
        resp = await sqlite_engine.execute(SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.ltv_third"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:sum")],
        ))
        by_grain = {r["orders_x.customers_v2.ltv_third"]: r for r in resp.data}
        # ltv 10 → 10/3.0 = 3.33 → INT 3; the group must survive the join-back.
        assert 3 in by_grain, resp.data
        assert by_grain[3]["orders_x.customers_v2.lifetime_value_sum"] == 10.0, resp.data
        # No group's aggregate silently dropped to NULL.
        for grain, row in by_grain.items():
            assert row["orders_x.customers_v2.lifetime_value_sum"] is not None, (grain, resp.data)

    async def test_first_last_crossing_grain_per_group_values(self, sqlite_engine) -> None:
        """first/last over a crossing derived grain returns correct per-group
        values. Group by deep_pop; last(lifetime_value ORDER BY signup_at) picks
        the latest customer in each region-population group."""
        resp = await sqlite_engine.execute(SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:last(customers_v2.signup_at)")],
        ))
        by_pop = {r["orders_x.customers_v2.deep_pop"]: r for r in resp.data}
        key = "orders_x.customers_v2.lifetime_value_last_customers_v2_signup_at"
        # region 1 (pop 100): customers 1 (2024-01) & 2 (2024-03) → latest = 30.
        assert by_pop[100.0][key] == 30.0, resp.data
        # region 2 (pop 200): single customer → 20.
        assert by_pop[200.0][key] == 20.0, resp.data

    async def test_first_crossing_grain_differs_from_last(self, sqlite_engine) -> None:
        """``first`` over a crossing derived grain uses ASC ranking (``_first_rn``)
        and must return a value DIFFERENT from ``last`` — region 1's earliest
        customer is customer 1 (Jan, ltv 10), vs last = 30 — proving the ASC/DESC
        ranking path is exercised for a derived grain too (Codex F3)."""
        resp = await sqlite_engine.execute(SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:first(customers_v2.signup_at)")],
        ))
        by_pop = {r["orders_x.customers_v2.deep_pop"]: r for r in resp.data}
        key = "orders_x.customers_v2.lifetime_value_first_customers_v2_signup_at"
        # region 1 (pop 100): earliest customer is customer 1 (2024-01) → 10.
        assert by_pop[100.0][key] == 10.0, resp.data
        assert by_pop[200.0][key] == 20.0, resp.data

    async def test_filtered_local_derived_grain_filter_changes_result(
        self, sqlite_engine
    ) -> None:
        """Filtered-local (host-rooted, DEV-1503) isolation grouped by a derived
        grain: ``amt_hi`` filters to customers with ltv > 15, so region 1's
        low-ltv customer 1 (ltv 10, order amt 5) is EXCLUDED — region 1's
        amt_hi sum is customer 2's order amount 7 alone, not 5 + 7."""
        resp = await sqlite_engine.execute(SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="amt_hi:sum")],
        ))
        by_pop = {r["orders_x.customers_v2.deep_pop"]: r for r in resp.data}
        key = "orders_x.amt_hi_sum"
        # region 1 (pop 100): only customer 2 (ltv 30) survives → order amt 7.
        assert by_pop[100.0][key] == 7.0, resp.data
        # region 2 (pop 200): customer 3 (ltv 20) → order amt 9.
        assert by_pop[200.0][key] == 9.0, resp.data
        # region 3 (NULL pop): customer 4 (ltv 40) → order amt 11 (null-safe).
        assert by_pop[None][key] == 11.0, resp.data

    async def test_first_last_filter_before_ranking(self, sqlite_engine) -> None:
        """first/last over a crossing grain with a WHERE filter: the filter is
        applied BEFORE ranking (inside the ranked subquery), so a filtered-out
        row cannot win the rank (Codex F4). The filter must remove the row that
        WOULD win — filtering ltv < 25 drops customer 2 (ltv 30, the March
        winner) from region 1, leaving only customer 1 (ltv 10, January). A
        correct pre-ranking filter yields 10; a broken post-ranking filter would
        rank customer 2 first and yield 30 (or drop the group)."""
        resp = await sqlite_engine.execute(SlayerQuery(
            source_model="orders_x",
            dimensions=["customers_v2.deep_pop"],
            measures=[ModelMeasure(formula="customers_v2.lifetime_value:last(customers_v2.signup_at)")],
            filters=["customers_v2.lifetime_value < 25"],
        ))
        by_pop = {r["orders_x.customers_v2.deep_pop"]: r for r in resp.data}
        key = "orders_x.customers_v2.lifetime_value_last_customers_v2_signup_at"
        # region 1: customer 2 (ltv 30) filtered out; only customer 1 (ltv 10)
        # remains → last = 10 (NOT 30, which broken post-rank filtering gives).
        assert by_pop[100.0][key] == 10.0, resp.data
        # region 2: customer 3 (ltv 20) survives → 20.
        assert by_pop[200.0][key] == 20.0, resp.data
