"""time_shift CTEs on ScopeFrame (cross-dataset partitions).

Migrates ``_emit_time_shift_ctes_for_planned`` onto a ``ScopeFrame`` so the
shifted CTE's partition keys and time expressions all enter through
``resolve()`` (Law 1: anchor + register the relationship it crosses) and the CTE's FROM
is built from the scope's registered ``join_paths``. This closes the
silent-broadcast defects the joinless-shifted-CTE limitation caused.

Failure taxonomy this file pins (each verified against the pre-Stage-7 code so
every test fails for the RIGHT reason — feature missing, not setup):

* **cross-dataset ``ColumnKey`` partition** (``stores.name``) → today raises
  ``NotImplementedError: stage 7b.12``. The QoQ demo shape.
* **derived (``ColumnSqlKey``) dim partition** (local ``upper(status)`` or
  joined ``stores.tier``) → today SILENTLY skipped by the auto-partition walk →
  the shifted value broadcasts across the derived-dim groups. ``assert_scope_
  closed`` does NOT catch this (the shifted CTE is internally consistent, just
  missing a partition) — only an sjoin-partition-pair assertion or executed
  values do.
* **secondary ``TimeTruncKey`` dim** → same silent broadcast across the second
  time bucket.
* **joined-field ROW filter** → today raises via ``_guard_no_joined_refs`` (the
  shifted CTE carried no relationships). The guard is lifted: the scope now pulls the
  filter's relationship.
* **plain ``=`` sjoin equality** → NULL dim / NULL time-bucket rows silently
  drop their shifted value. The grain join-back becomes null-safe (Codex F2 —
  the sjoin IS a grain join-back), so a NULL-store group keeps its prev value.

The uniform post-Stage-7 rule (asserted throughout): the sjoin grain is EVERY
projected dimension — joined ``ColumnKey``, derived ``ColumnSqlKey``, and
secondary ``TimeTruncKey`` alike — joined back null-safely.

SQL-shape tests run through the typed engine (``dry_run=True``); execution
ground-truth runs against an in-process DuckDB (the QoQ demo dataset's backend),
following the Stage-4 precedent (``tests/test_dev1708_stage4_cte_scope.py``)
of landing executed-value proofs in the unit suite.
"""

from __future__ import annotations

import os
import re
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.query import OrderItem, QueryDimension, QueryMetric, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.scope_check import assert_scope_closed
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import _engine_generate


# --------------------------------------------------------------------------- #
# Dataset builders — orders → stores → regions (the QoQ demo chain).
# --------------------------------------------------------------------------- #
def _regions() -> SlayerDataset:
    return SlayerDataset(
        name="regions", sql_table="regions", data_source="test",
        fields=[
            DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="name", sql="name", datatype=DataType.STRING),
            DatasetField(name="population", sql="population", datatype=DataType.FLOAT),
        ],
    )


def _stores(*, extra=None) -> SlayerDataset:
    cols = [
        DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="name", sql="name", datatype=DataType.STRING),
        DatasetField(name="opened_at", sql="opened_at", datatype=DataType.DATETIME),
        DatasetField(name="region_id", sql="region_id", datatype=DataType.INTEGER),
    ]
    cols += extra or []
    return SlayerDataset(
        name="stores", sql_table="stores", data_source="test", fields=cols,
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )


def _orders(*, extra=None, filters=None) -> SlayerDataset:
    cols = [
        DatasetField(name="id", sql="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="status", sql="status", datatype=DataType.STRING),
        DatasetField(name="status_up", expression=ExpressionObject(dialects=[DialectEntry(dialect="POSTGRES", expression="upper(status)")]), datatype=DataType.STRING),
        DatasetField(name="ordered_at", sql="ordered_at", datatype=DataType.DATETIME),
        DatasetField(name="delivery_at", sql="delivery_at", datatype=DataType.DATETIME),
        DatasetField(name="store_id", sql="store_id", datatype=DataType.INTEGER),
        DatasetField(name="order_total", sql="order_total", datatype=DataType.FLOAT),
    ]
    cols += extra or []
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="ordered_at", fields=cols,
        relationships=[DatasetRelationship(to="stores", from_columns=["store_id"], to_columns=["id"])],
        filters=filters or [],
    )


async def _gen(query, dataset, *, extra_datasets=None, dialect="postgres") -> str:
    return await _engine_generate(
        query=query, dataset=dataset, extra_datasets=extra_datasets or [],
        dialect=dialect, validate=False,
    )


# --------------------------------------------------------------------------- #
# CTE-body extraction (balanced-paren, self-contained per repo convention).
# --------------------------------------------------------------------------- #
def _cte_body(sql: str, cte_name_pattern: str) -> str:
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


def _shifted_body(sql: str) -> str:
    return _cte_body(sql, r"shifted_\w+")


def _sjoin_body(sql: str) -> str:
    return _cte_body(sql, r"sjoin_\w+")


def _sjoin_on(sql: str) -> str:
    """The sjoin CTE's LEFT JOIN ... ON predicate text (through the end of the
    CTE body)."""
    body = _sjoin_body(sql)
    idx = body.find(" ON ")
    assert idx != -1, f"no ON clause in sjoin body:\n{body}"
    return body[idx + 4:]


def _shifted_where(sql: str) -> str:
    """The shifted CTE's WHERE clause text (between ``WHERE`` and ``GROUP BY``);
    empty string if the shifted CTE has no WHERE."""
    body = _shifted_body(sql)
    m = re.search(r"\bWHERE\b(.*?)(?:\bGROUP BY\b|$)", body, re.DOTALL)
    return m.group(1).strip() if m else ""


# A quoted-identifier char (double-quote for most dialects, backtick for MySQL).
_Q = r"[\"`]"


def _assert_grain_pair(
    on_pred: str, col_regex: str, *, marker: str = "IS NOT DISTINCT FROM",
    lookup: bool = False, population: str = "orders",
) -> None:
    """Assert the sjoin ON contains a genuine null-safe grain pair for the column
    matched by ``col_regex``: ``base."<key>" <marker> shifted_x."<key flattened>"``,
    the shifted side being the key minus the population, ``.`` → ``__``.

    Stronger than a bare substring: it requires the column on BOTH sides, joined
    by the null-safe operator — so an implementation that drops one side, omits
    the pair, or leaves a plain ``=`` cannot pass. ``lookup`` marks the shift
    axis, whose base side carries the offset.
    """
    m = re.escape(marker.strip())
    pat = re.compile(
        r"base\." + _Q + r"([^\"`]*(?:" + col_regex + r")[^\"`]*)" + _Q
        + (r"([^\n]*?)" if lookup else r"()\s*")
        + m + r"\s*shifted_\w+\." + _Q + r"([^\"`]*)" + _Q
    )

    def _flat(key: str) -> str:
        return key.removeprefix(f"{population}.").replace(".", "__")

    found = next((f for f in pat.finditer(on_pred) if f.group(3) == _flat(f.group(1))), None)
    assert found, (
        f"no null-safe grain pair for /{col_regex}/ [{marker}] in ON:\n{on_pred}"
    )
    if lookup:
        line = on_pred[on_pred.rfind("\n", 0, found.start()) + 1:found.end()]
        assert re.search(r"(?i)INTERVAL|DATE_?ADD|addMonths", line), line


def _grain_pair_count(on_pred: str, *, marker: str = "IS NOT DISTINCT FROM") -> int:
    """Number of null-safe equality operators in the sjoin ON — i.e. the count of
    grain columns joined back (time axis + every partition)."""
    return len(re.findall(re.escape(marker.strip()), on_pred))


# --------------------------------------------------------------------------- #
# The QoQ query shapes (reused by shape + execution tests).
# --------------------------------------------------------------------------- #
def _qoq_query(*, metric_expression: str, metric_name: str) -> SlayerQuery:
    return SlayerQuery(
        population="orders",
        time_dimensions=[TimeDimension(
            field="orders.ordered_at",
            granularity=TimeGranularity.QUARTER,
        )],
        dimensions=[QueryDimension(field="stores.name")],
        metrics=[
            QueryMetric(metric="sum(orders.order_total)"),
            QueryMetric(metric=metric_expression, alias=metric_name),
        ],
        order=[OrderItem(field="orders.ordered_at", direction="asc")],
    )


# =========================================================================== #
# SQL-shape — cross-dataset / derived / secondary-TD partitions.
# =========================================================================== #
class TestShiftedCtePartitionShape:
    async def test_qoq_single_hop_partition_joins_and_projects(self) -> None:
        """Core: ``change(sum(order_total))`` with ``stores.name`` — the
        shifted CTE pulls ``LEFT JOIN stores`` (Law 1), projects the joined dim,
        and the sjoin joins back on it. Today: ``NotImplementedError 7b.12``."""
        sql = await _gen(
            _qoq_query(metric_expression="change(sum(orders.order_total))", metric_name="qoq"),
            _orders(), extra_datasets=[_stores(), _regions()],
        )
        shifted = _shifted_body(sql)
        assert "LEFT JOIN stores" in shifted, shifted
        # The partition dim is projected + grouped inside the shifted CTE.
        assert "stores.name" in shifted, shifted
        # The sjoin joins back on EXACTLY two null-safe grain pairs: the shifted
        # time axis and the store partition (same alias both sides — not a loose
        # substring).
        on = _sjoin_on(sql)
        _assert_grain_pair(on, r"ordered_at", lookup=True)
        _assert_grain_pair(on, r"stores[._]+name")
        assert _grain_pair_count(on) == 2, on
        assert_scope_closed(sql)

    async def test_multi_hop_partition_registers_every_hop(self) -> None:
        """A multi-hop partition (``stores.regions.name``) pulls BOTH hops into
        the shifted CTE (``stores`` and ``stores__regions`` aliases)."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.ordered_at",
                granularity=TimeGranularity.QUARTER,
            )],
            dimensions=[QueryDimension(field="stores.regions.name")],
            metrics=[
                QueryMetric(metric="sum(orders.order_total)"),
                QueryMetric(metric="time_shift(sum(orders.order_total), -1)", alias="prev"),
            ],
        )
        sql = await _gen(query, _orders(), extra_datasets=[_stores(), _regions()])
        shifted = _shifted_body(sql)
        # BOTH hops are introduced as real join clauses (not just the alias token
        # appearing somewhere): the direct ``stores`` hop and the second
        # ``regions AS stores__regions`` hop.
        assert "LEFT JOIN stores AS stores" in shifted, shifted
        assert "LEFT JOIN regions AS stores__regions" in shifted, shifted
        on = _sjoin_on(sql)
        _assert_grain_pair(on, r"ordered_at", lookup=True)
        _assert_grain_pair(on, r"stores[._]+regions[._]+name")
        assert _grain_pair_count(on) == 2, on
        assert_scope_closed(sql)

    async def test_derived_local_dim_partition_not_broadcast(self) -> None:
        """A derived LOCAL dim (``status_up = upper(status)``) must partition the
        shifted CTE. Today: silently skipped → the shifted value broadcasts
        across status groups (scope_closed does NOT catch it)."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.ordered_at",
                granularity=TimeGranularity.MONTH,
            )],
            dimensions=[QueryDimension(field="orders.status_up")],
            metrics=[
                QueryMetric(metric="sum(orders.order_total)"),
                QueryMetric(metric="time_shift(sum(orders.order_total), -1)", alias="prev"),
            ],
        )
        sql = await _gen(query, _orders())
        shifted = _shifted_body(sql)
        # The expanded derived expression is both PROJECTED and GROUPED (not just
        # present somewhere) — a broadcast bug would project the shifted value
        # without grouping by the derived dim.
        assert shifted.count("UPPER(orders.status)") >= 2, shifted  # SELECT + GROUP BY
        assert re.search(r"GROUP BY[\s\S]*UPPER\(orders\.status\)", shifted), shifted
        on = _sjoin_on(sql)
        _assert_grain_pair(on, r"ordered_at", lookup=True)
        _assert_grain_pair(on, r"status_up")
        assert _grain_pair_count(on) == 2, on
        assert_scope_closed(sql)

    async def test_derived_joined_dim_partition_not_broadcast(self) -> None:
        """A derived JOINED dim (``stores.tier = upper(name)``) must partition the
        shifted CTE: the relationship is pulled AND the expanded expression is grouped.
        Today: silently skipped → broadcast across tiers (scope_closed passes)."""
        stores = _stores(extra=[
            DatasetField(name="tier", expression="upper(name)", datatype=DataType.STRING),
        ])
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.ordered_at",
                granularity=TimeGranularity.MONTH,
            )],
            dimensions=[QueryDimension(field="stores.tier")],
            metrics=[
                QueryMetric(metric="sum(orders.order_total)"),
                QueryMetric(metric="time_shift(sum(orders.order_total), -1)", alias="prev"),
            ],
        )
        sql = await _gen(query, _orders(), extra_datasets=[stores, _regions()])
        shifted = _shifted_body(sql)
        assert "LEFT JOIN stores AS stores" in shifted, shifted
        # The derived JOINED expression is expanded (UPPER(stores.name), NOT the
        # bare field) and grouped in the shifted CTE.
        assert shifted.count("UPPER(stores.name)") >= 2, shifted  # SELECT + GROUP BY
        assert re.search(r"GROUP BY[\s\S]*UPPER\(stores\.name\)", shifted), shifted
        on = _sjoin_on(sql)
        _assert_grain_pair(on, r"ordered_at", lookup=True)
        _assert_grain_pair(on, r"stores[._]+tier")
        assert _grain_pair_count(on) == 2, on
        assert_scope_closed(sql)

    async def test_secondary_time_dimension_partitions(self) -> None:
        """A second time dimension (``delivery_at`` @quarter, distinct from the
        ``ordered_at`` shift axis) must partition the shifted CTE. Today: silently
        skipped → the shifted value broadcasts across delivery quarters."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(field="orders.ordered_at",
                              granularity=TimeGranularity.MONTH),
                TimeDimension(field="orders.delivery_at",
                              granularity=TimeGranularity.QUARTER),
            ],
            main_time_dimension="orders.ordered_at",
            metrics=[
                QueryMetric(metric="sum(orders.order_total)"),
                QueryMetric(metric="time_shift(sum(orders.order_total), -1)", alias="prev"),
            ],
        )
        sql = await _gen(query, _orders())
        shifted = _shifted_body(sql)
        # The secondary bucket is DATE_TRUNC'd and grouped in the shifted CTE
        # (a broadcast bug omits it from the shifted GROUP BY entirely).
        assert re.search(r"GROUP BY[\s\S]*QUARTER['\"][\s\S]*delivery_at", shifted), shifted
        # BOTH the shift axis and the secondary time dim form null-safe grain
        # pairs — exactly two.
        on = _sjoin_on(sql)
        _assert_grain_pair(on, r"ordered_at", lookup=True)
        _assert_grain_pair(on, r"delivery_at")
        assert _grain_pair_count(on) == 2, on
        assert_scope_closed(sql)

    async def test_explicit_partition_by_rejected_on_time_shift(self) -> None:
        """Explicit ``partition_by=`` on ``time_shift`` is rejected at
        bind time. The auto-include of query dimensions does the shift join-back;
        the explicit form was a no-op under the uniform rule, so it now raises
        rather than silently accepting an inert kwarg."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.ordered_at",
                granularity=TimeGranularity.MONTH,
            )],
            dimensions=[QueryDimension(field="stores.name")],
            metrics=[
                QueryMetric(metric="sum(orders.order_total)"),
                QueryMetric(
                    metric="time_shift(sum(orders.order_total), -1, partition_by=stores.name)",
                    alias="prev",
                ),
            ],
        )
        orders, stores, regions = _orders(), _stores(), _regions()
        with pytest.raises(ValueError, match=r"partition_by"):
            await _gen(query, orders, extra_datasets=[stores, regions])

    async def test_joined_time_axis_pulls_join_into_shifted_cte(self) -> None:
        """When the SHIFT-axis time dimension is itself a joined field
        (``stores.opened_at``), routing it through the scope pulls ``LEFT JOIN
        stores`` into the shifted CTE. Today: the shifted CTE references an
        unbound ``stores`` alias → scope leak."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="stores.opened_at",
                granularity=TimeGranularity.MONTH,
            )],
            metrics=[
                QueryMetric(metric="sum(orders.order_total)"),
                QueryMetric(metric="time_shift(sum(orders.order_total), -1)", alias="prev"),
            ],
        )
        sql = await _gen(query, _orders(), extra_datasets=[_stores(), _regions()])
        shifted = _shifted_body(sql)
        assert "LEFT JOIN stores AS stores" in shifted, shifted
        # The shift-axis time expression IS the joined field — not some unrelated
        # join that happens to be present; the offset lives in the join-back.
        assert "stores.opened_at" in shifted, shifted
        # The sole grain pair is the (joined) shifted time axis.
        on = _sjoin_on(sql)
        _assert_grain_pair(on, r"opened_at", lookup=True)
        assert _grain_pair_count(on) == 1, on
        assert_scope_closed(sql)


# =========================================================================== #
# SQL-shape — joined-field ROW filter (guard lifted).
# =========================================================================== #
class TestShiftedCteJoinedFilter:
    async def test_query_filter_crossing_join_lifts_guard(self) -> None:
        """A query ROW filter crossing the orders→stores relationship (``stores.name =
        'North'``) combined with a time_shift: the shifted CTE now joins stores
        and applies the filter. Today: ``NotImplementedError`` from
        ``_guard_no_joined_refs``."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.ordered_at",
                granularity=TimeGranularity.MONTH,
            )],
            metrics=[
                QueryMetric(metric="sum(orders.order_total)"),
                QueryMetric(metric="time_shift(sum(orders.order_total), -1)", alias="prev"),
            ],
            filter=["stores.name = 'North'"],
        )
        sql = await _gen(query, _orders(), extra_datasets=[_stores(), _regions()])
        shifted = _shifted_body(sql)
        assert "LEFT JOIN stores AS stores" in shifted, shifted
        # The filter predicate is PRESERVED in the shifted CTE's WHERE (not merely
        # the field appearing somewhere) — the re-aggregation runs over the same
        # filtered population as _base.
        where = _shifted_where(sql)
        assert "stores.name" in where, where
        assert "'North'" in where, where
        assert_scope_closed(sql)

    async def test_model_filter_mode_a_crossing_join_lifts_guard(self) -> None:
        """Same, for a Mode-A DATASET filter (``stores.name IS NOT NULL``) that
        crosses the relationship — the shifted CTE re-aggregates over the same filtered
        population, relationships pulled."""
        model = _orders(filters=["stores.name IS NOT NULL"])
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.ordered_at",
                granularity=TimeGranularity.MONTH,
            )],
            metrics=[
                QueryMetric(metric="sum(orders.order_total)"),
                QueryMetric(metric="time_shift(sum(orders.order_total), -1)", alias="prev"),
            ],
        )
        sql = await _gen(query, model, extra_datasets=[_stores(), _regions()])
        shifted = _shifted_body(sql)
        assert "LEFT JOIN stores AS stores" in shifted, shifted
        where = _shifted_where(sql)
        assert "stores.name" in where, where
        # The predicate reaches the shifted CTE as an AST now that Mode-A text
        # enters through the one door, so sqlglot emits its canonical negated
        # form. The host path always re-parsed and rendered this way; the
        # shifted path's raw-text passthrough was the odd one out.
        upper = where.upper()
        assert "IS NOT NULL" in upper or "NOT STORES.NAME IS NULL" in upper, where
        assert_scope_closed(sql)


# =========================================================================== #
# SQL-shape — null-safe sjoin equality (Codex F2, per dialect).
# =========================================================================== #
# Native single-token forms + SQLite's ``IS`` operator; matches the Stage-4
# _NULLSAFE_NATIVE table (tests/test_dev1708_stage4_cte_scope.py).
_NULLSAFE_MARKER = {
    "postgres": "IS NOT DISTINCT FROM",
    "duckdb": "IS NOT DISTINCT FROM",
    "clickhouse": "IS NOT DISTINCT FROM",
    "mysql": "<=>",
    "sqlite": " IS ",
}


class TestShiftedCteNullSafeJoinBack:
    @pytest.mark.parametrize("dialect,marker", sorted(_NULLSAFE_MARKER.items()))
    async def test_sjoin_on_is_null_safe(self, dialect: str, marker: str) -> None:
        """The sjoin grain join-back uses the dialect's null-safe equality on
        BOTH the time axis and every partition pair — a NULL dim / NULL time
        bucket must match, not silently drop. Today: plain ``=``.

        Uses a LOCAL-dim partition so the sjoin is emitted on the pre-Stage-7
        pipeline (a cross-dataset partition raises 7b.12 first) — this pins the
        null-safe EQUALITY spelling specifically, independent of the relationship pull."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.ordered_at",
                granularity=TimeGranularity.MONTH,
            )],
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(metric="sum(orders.order_total)"),
                QueryMetric(metric="time_shift(sum(orders.order_total), -1)", alias="prev"),
            ],
        )
        sql = await _gen(query, _orders(), dialect=dialect)
        on_pred = _sjoin_on(sql)
        # BOTH grain columns (time axis + the local dim) form a null-safe pair
        # (same alias both sides, dialect operator) — not just "the marker appears
        # somewhere".
        _assert_grain_pair(on_pred, r"ordered_at", marker=marker, lookup=True)
        _assert_grain_pair(on_pred, r"[._]status", marker=marker)
        assert _grain_pair_count(on_pred, marker=marker) == 2, on_pred
        # No bare ``=`` equality survives on the grain pairs (a plain ``x = y``
        # would silently drop NULL groups). Strip the null-safe marker first, then
        # assert no standalone ``=`` remains (tolerating ``<=>`` / ``!=``).
        bare_eq = re.search(r"(?<![<>!])=(?!=)", on_pred.replace(marker.strip(), ""))
        assert bare_eq is None, f"[{dialect}] bare '=' left on grain: {on_pred!r}"


# =========================================================================== #
# SQL-shape — local-only regression (no join must never appear).
# =========================================================================== #
class TestLocalOnlyTimeShiftUnchanged:
    async def test_local_partition_stays_joinless(self) -> None:
        """A time_shift partitioned only on a LOCAL bare dimension (``status``)
        must keep the shifted CTE's FROM bare — widening the FROM for the
        cross-dataset case must never leak a join into the local path."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[TimeDimension(
                field="orders.ordered_at",
                granularity=TimeGranularity.MONTH,
            )],
            dimensions=[QueryDimension(field="orders.status")],
            metrics=[
                QueryMetric(metric="sum(orders.order_total)"),
                QueryMetric(metric="time_shift(sum(orders.order_total), -1)", alias="prev"),
            ],
        )
        sql = await _gen(query, _orders(), extra_datasets=[_stores(), _regions()])
        shifted = _shifted_body(sql)
        assert "JOIN" not in shifted.upper(), shifted
        # The local dim still partitions (no regression on the always-worked path)
        # and is joined back null-safely like every other grain column.
        assert "orders.status" in shifted, shifted
        on = _sjoin_on(sql)
        _assert_grain_pair(on, r"ordered_at", lookup=True)
        _assert_grain_pair(on, r"[._]status")
        assert _grain_pair_count(on) == 2, on
        assert_scope_closed(sql)


# =========================================================================== #
# SQL-shape — joined SECONDARY time dimension (Codex test-review #5).
# =========================================================================== #
class TestJoinedSecondaryTimeDimension:
    async def test_joined_secondary_td_registers_join_and_partitions(self) -> None:
        """A LOCAL shift axis (``ordered_at``) with a JOINED secondary time
        dimension (``stores.opened_at`` @quarter): the secondary TimeTruncKey must
        register its crossed relationship into the shifted CTE AND stay part of the grain.
        Exercises the plan's requirement that a secondary TimeTruncKey pulls relationships
        while remaining a shifted-grain partition."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(field="orders.ordered_at",
                              granularity=TimeGranularity.MONTH),
                TimeDimension(field="stores.opened_at",
                              granularity=TimeGranularity.QUARTER),
            ],
            main_time_dimension="orders.ordered_at",
            metrics=[
                QueryMetric(metric="sum(orders.order_total)"),
                QueryMetric(metric="time_shift(sum(orders.order_total), -1)", alias="prev"),
            ],
        )
        sql = await _gen(query, _orders(), extra_datasets=[_stores(), _regions()])
        shifted = _shifted_body(sql)
        assert "LEFT JOIN stores AS stores" in shifted, shifted
        assert re.search(r"GROUP BY[\s\S]*QUARTER['\"][\s\S]*stores\.opened_at", shifted), shifted
        on = _sjoin_on(sql)
        _assert_grain_pair(on, r"ordered_at", lookup=True)
        _assert_grain_pair(on, r"stores[._]+opened_at")
        assert _grain_pair_count(on) == 2, on
        assert_scope_closed(sql)


# =========================================================================== #
# Execution ground-truth — in-process DuckDB, hand-computed QoQ values.
# =========================================================================== #
#
# Seed (order_total by store × quarter):
#            2024-Q1   2024-Q2   2024-Q3
#   North     150       300       250
#   South     200        80       400
#   (NULL)     10        20         5
#
# change(order_total:sum) = current quarter − previous quarter (same store):
#   North:  Q1=None  Q2=+150  Q3=-50
#   South:  Q1=None  Q2=-120  Q3=+320
#   (NULL): Q1=None  Q2=+10   Q3=-15      ← Q2/Q3 require the NULL-safe join-back
#
# change_pct = (current − prev) / prev:
#   North:  Q2=+1.0    Q3=-0.16667
#   South:  Q2=-0.6    Q3=+4.0
#   (NULL): Q2=+1.0    Q3=-0.75
# --------------------------------------------------------------------------- #
_STORES_ROWS = [
    (1, "North", "2020-01-01 00:00:00", 10),
    (2, "South", "2020-01-01 00:00:00", 20),
]
_ORDERS_ROWS = [
    # (id, store_id, ordered_at, order_total)
    (1, 1, "2024-01-15", 100.0), (2, 1, "2024-02-10", 50.0),   # North Q1 = 150
    (3, 1, "2024-04-05", 300.0),                                # North Q2 = 300
    (4, 1, "2024-07-20", 250.0),                                # North Q3 = 250
    (5, 2, "2024-03-01", 200.0),                                # South Q1 = 200
    (6, 2, "2024-05-05", 80.0),                                 # South Q2 = 80
    (7, 2, "2024-08-08", 400.0),                                # South Q3 = 400
    (8, None, "2024-02-02", 10.0),                              # NULL Q1 = 10
    (9, None, "2024-06-06", 20.0),                              # NULL Q2 = 20
    (10, None, "2024-09-09", 5.0),                              # NULL Q3 = 5
]
# For the secondary-time-dimension execution check (single store, two time dims).
_ORDERS2_ROWS = [
    # (id, ordered_at [month axis], delivery_at [quarter partition], order_total)
    (1, "2024-01-10", "2024-02-15", 100.0),  # Jan ordered, Q1 delivered
    (2, "2024-01-20", "2024-05-15", 30.0),   # Jan ordered, Q2 delivered
    (3, "2024-02-10", "2024-02-20", 200.0),  # Feb ordered, Q1 delivered
    (4, "2024-02-25", "2024-05-25", 50.0),   # Feb ordered, Q2 delivered
]


def _seed_duckdb_qoq(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE stores (id INTEGER, name VARCHAR, opened_at TIMESTAMP, "
        "region_id INTEGER)"
    )
    con.executemany("INSERT INTO stores VALUES (?,?,?,?)", _STORES_ROWS)
    con.execute(
        "CREATE TABLE orders (id INTEGER, store_id INTEGER, ordered_at TIMESTAMP, "
        "order_total DOUBLE)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?)", _ORDERS_ROWS)
    con.close()


def _seed_duckdb_secondary_td(db_path: str) -> None:
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect(db_path)
    con.execute(
        "CREATE TABLE orders (id INTEGER, ordered_at TIMESTAMP, "
        "delivery_at TIMESTAMP, order_total DOUBLE)"
    )
    con.executemany("INSERT INTO orders VALUES (?,?,?,?)", _ORDERS2_ROWS)
    con.close()


@pytest.fixture
async def qoq_engine() -> AsyncIterator[SlayerQueryEngine]:
    pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "qoq.duckdb")
        _seed_duckdb_qoq(db_path)
        storage = YAMLStorage(base_dir=os.path.join(d, "store"))
        await storage.save_datasource(
            DatasourceConfig(name="test", type="duckdb", database=db_path)
        )
        await storage.save_dataset(_stores())
        await storage.save_dataset(_regions())
        await storage.save_dataset(_orders())
        yield SlayerQueryEngine(storage=storage)


@pytest.fixture
async def secondary_td_engine() -> AsyncIterator[SlayerQueryEngine]:
    pytest.importorskip("duckdb")
    with tempfile.TemporaryDirectory() as d:
        db_path = os.path.join(d, "std.duckdb")
        _seed_duckdb_secondary_td(db_path)
        storage = YAMLStorage(base_dir=os.path.join(d, "store"))
        await storage.save_datasource(
            DatasourceConfig(name="test", type="duckdb", database=db_path)
        )
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            default_time_dimension="ordered_at",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="ordered_at", datatype=DataType.DATETIME),
                DatasetField(name="delivery_at", datatype=DataType.DATETIME),
                DatasetField(name="order_total", datatype=DataType.FLOAT),
            ],
        ))
        yield SlayerQueryEngine(storage=storage)


def _quarter_key(value) -> str:
    """First 7 chars of the DATE_TRUNC'd quarter timestamp — ``2024-01`` (Q1),
    ``2024-04`` (Q2), ``2024-07`` (Q3) — a stable per-quarter key."""
    return str(value)[:7]


class TestQoQExecutionGroundTruth:
    async def test_change_per_store_matches_hand_computed(self, qoq_engine) -> None:
        """The QoQ ``change(sum(order_total))`` partitioned by ``stores.name``
        matches hand-computed per-store deltas, including the NULL-store group
        (whose non-null prev proves the null-safe join-back)."""
        resp = await qoq_engine.execute(_qoq_query(
            metric_expression="change(sum(orders.order_total))", metric_name="qoq",
        ))
        by = {
            (r["stores.name"], _quarter_key(r["orders.ordered_at"])): r
            for r in resp.data
        }
        # Base per-quarter sums (guards the aggregation under the partition).
        assert float(by[("North", "2024-01")]["orders.order_total_sum"]) == pytest.approx(150.0)
        assert float(by[("South", "2024-04")]["orders.order_total_sum"]) == pytest.approx(80.0)
        assert float(by[(None, "2024-01")]["orders.order_total_sum"]) == pytest.approx(10.0)

        # Q1 (earliest) has no prior quarter → change is NULL for every store.
        assert by[("North", "2024-01")]["qoq"] is None
        assert by[("South", "2024-01")]["qoq"] is None
        assert by[(None, "2024-01")]["qoq"] is None

        # QoQ deltas — each store's series compared only against itself.
        assert float(by[("North", "2024-04")]["qoq"]) == pytest.approx(150.0)
        assert float(by[("North", "2024-07")]["qoq"]) == pytest.approx(-50.0)
        assert float(by[("South", "2024-04")]["qoq"]) == pytest.approx(-120.0)
        assert float(by[("South", "2024-07")]["qoq"]) == pytest.approx(320.0)

        # The NULL-store group: prev is found across the NULL grain (null-safe
        # join-back) — a plain ``=`` join-back would drop these to NULL.
        assert float(by[(None, "2024-04")]["qoq"]) == pytest.approx(10.0)
        assert float(by[(None, "2024-07")]["qoq"]) == pytest.approx(-15.0)

    async def test_change_pct_per_store_matches_hand_computed(self, qoq_engine) -> None:
        """``change_pct`` percentages per store (NULL prior → NULL)."""
        resp = await qoq_engine.execute(_qoq_query(
            metric_expression="change_pct(sum(orders.order_total))", metric_name="qoq_pct",
        ))
        by = {
            (r["stores.name"], _quarter_key(r["orders.ordered_at"])): r
            for r in resp.data
        }
        assert by[("North", "2024-01")]["qoq_pct"] is None
        assert float(by[("North", "2024-04")]["qoq_pct"]) == pytest.approx(1.0)
        assert float(by[("North", "2024-07")]["qoq_pct"]) == pytest.approx(-50.0 / 300.0)
        assert float(by[("South", "2024-04")]["qoq_pct"]) == pytest.approx(-0.6)
        assert float(by[("South", "2024-07")]["qoq_pct"]) == pytest.approx(4.0)
        # NULL-store percentages also require the null-safe join-back.
        assert float(by[(None, "2024-04")]["qoq_pct"]) == pytest.approx(1.0)
        assert float(by[(None, "2024-07")]["qoq_pct"]) == pytest.approx(-0.75)


class TestSecondaryTimeDimensionExecution:
    async def test_prev_does_not_broadcast_across_second_time_dim(
        self, secondary_td_engine
    ) -> None:
        """With two time dimensions, ``time_shift`` on the ``ordered_at`` month
        axis must NOT broadcast the prior-month value across ``delivery_at``
        quarters — each (ordered-month, delivery-quarter) cell gets the prior
        MONTH's value within the SAME delivery quarter."""
        query = SlayerQuery(
            population="orders",
            time_dimensions=[
                TimeDimension(field="orders.ordered_at",
                              granularity=TimeGranularity.MONTH),
                TimeDimension(field="orders.delivery_at",
                              granularity=TimeGranularity.QUARTER),
            ],
            main_time_dimension="orders.ordered_at",
            metrics=[
                QueryMetric(metric="sum(orders.order_total)"),
                QueryMetric(metric="time_shift(sum(orders.order_total), -1)", alias="prev"),
            ],
        )
        resp = await secondary_td_engine.execute(query)
        by = {
            (str(r["orders.ordered_at"])[:7], str(r["orders.delivery_at"])[:7]): r
            for r in resp.data
        }
        # Feb-ordered, Q1-delivered: prev = Jan-ordered / Q1-delivered = 100
        # (NOT 130 = all-of-January broadcast).
        assert float(by[("2024-02", "2024-01")]["prev"]) == pytest.approx(100.0)
        # Feb-ordered, Q2-delivered: prev = Jan-ordered / Q2-delivered = 30.
        assert float(by[("2024-02", "2024-04")]["prev"]) == pytest.approx(30.0)
