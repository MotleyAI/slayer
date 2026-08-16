"""The result-key contract pack.

PR 1's FIRST commit, landing BEFORE any naming work so every later
allocator / renderer change in the consolidation chain is measured against a
written-down contract rather than against whatever the code happened to do.

What this pins, per query family: the EXACT, ORDERED list of public result
keys, asserted twice over —

* on real returned rows (``resp.data[0]`` keys) from a seeded file-backed
  SQLite, so the contract is verified on what a caller actually receives; and
* on the engine's declared metadata (``resp.columns`` / ``resp.attributes``),
  so a passing test cannot be an accident of driver column labelling.

The two producers are independent (``SQLGenerator._full_alias_for_slot`` and
``response_meta._slot_result_keys``); asserting both is what makes this a
contract rather than a snapshot.

Families covered: ordinary, joined dimensions, cross-model aggregates,
windowed measures, hidden / order-only slots, parametric aggregates (including
the DELIBERATE cross-model kwarg-suffix divergence), and the internal-vs-public
identifier separation (``naming.result_key`` dotted keys vs ``naming.flat_name``
``__`` inner-stage bind names).

Column ORDER is pinned as it is TODAY. A later PR in the chain switches
projection to declaration order, which deliberately changes it for some
cross-model / windowed shapes; the affected assertions here are re-surfaced for
approval in that PR. Until then, a failing order assertion is a real regression
rather than expected churn.

File-backed SQLite (never ``:memory:``) — the engine's async connection pool
opens more than one connection, and separate ``:memory:`` connections do not
share a database.
"""

from __future__ import annotations

import os
import sqlite3
from typing import List

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


# ===========================================================================
# Seeded engine: orders -> customers -> regions.
# ===========================================================================


@pytest.fixture
async def engine(tmp_path) -> SlayerQueryEngine:
    d = str(tmp_path)
    db_path = os.path.join(d, "contract.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)"
    )
    cur.executemany(
        "INSERT INTO regions VALUES (?,?)", [(1, "North"), (2, "South")],
    )
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "revenue REAL, signup_at TEXT)"
    )
    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?)",
        [
            (1, 1, 100.0, "2024-01-05"),
            (2, 1, 50.0, "2024-02-10"),
            (3, 2, 70.0, "2024-01-20"),
        ],
    )
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, "
        "status TEXT, amount REAL, created_at TEXT)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?)",
        [
            (1, 1, "new", 10.0, "2024-01-06"),
            (2, 1, "old", 5.0, "2024-02-11"),
            (3, 2, "new", 7.0, "2024-01-21"),
            (4, 3, "new", 3.0, "2024-01-22"),
            (5, 3, "old", 9.0, "2024-02-01"),
        ],
    )
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    await storage.save_model(
        SlayerModel(
            name="regions",
            sql_table="regions",
            data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="name", type=DataType.TEXT),
            ],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="prod",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="region_id", type=DataType.INT),
                Column(name="revenue", type=DataType.DOUBLE),
                Column(name="signup_at", type=DataType.TIMESTAMP),
                Column(
                    name="rev_x2", sql="revenue * 2", type=DataType.DOUBLE,
                ),
            ],
            joins=[
                ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
            ],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="prod",
            default_time_dimension="created_at",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="customer_id", type=DataType.INT),
                Column(name="status", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="created_at", type=DataType.TIMESTAMP),
            ],
            joins=[
                ModelJoin(
                    target_model="customers", join_pairs=[["customer_id", "id"]],
                ),
            ],
        )
    )
    return SlayerQueryEngine(storage=storage)


async def _assert_result_keys(
    engine: SlayerQueryEngine,
    query: SlayerQuery,
    expected: List[str],
) -> None:
    """Assert ``expected`` is the exact, ordered public result-key list, on
    BOTH the declared metadata and the real returned rows.

    ``resp.columns`` is the engine's declared output schema (built by
    ``response_meta.build_response_metadata`` from the SQL projection);
    ``resp.data[0]`` is what the driver actually handed back. Pinning both
    means a passing assertion cannot come from an accidental agreement
    between a wrong alias and a wrong decode.
    """
    resp = await engine.execute(query)
    assert list(resp.columns) == expected, (
        f"declared result keys differ\n  expected: {expected}\n  actual:   "
        f"{list(resp.columns)}"
    )
    assert resp.data, "query returned no rows — contract not verified on data"
    assert list(resp.data[0].keys()) == expected, (
        f"returned-row keys differ\n  expected: {expected}\n  actual:   "
        f"{list(resp.data[0].keys())}"
    )
    # Every declared attribute must be a real projected column (no drift
    # between the two independent key producers).
    attr_keys = set(resp.attributes.dimensions) | set(resp.attributes.measures)
    assert attr_keys <= set(resp.columns), (attr_keys, resp.columns)


# ===========================================================================
# 1 — Ordinary: local dimensions + local measures + star count.
# ===========================================================================


class TestOrdinaryResultKeys:
    async def test_local_dims_and_measures(self, engine) -> None:
        """``<relation>.<column>`` for dimensions; ``<relation>.<measure>_<agg>``
        for aggregates; ``*:count`` collapses the star to a leading underscore
        (``orders._count``). Order follows the query's declaration order."""
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    ModelMeasure(formula="amount:sum"),
                    ModelMeasure(formula="*:count"),
                ],
            ),
            ["orders.status", "orders.amount_sum", "orders._count"],
        )

    async def test_renamed_measure_uses_declared_name(self, engine) -> None:
        """A user-declared ``name`` replaces the canonical aggregate alias in
        the PUBLIC key — the canonical form stays internal."""
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="amount:sum", name="revenue")],
            ),
            ["orders.status", "orders.revenue"],
        )

    async def test_time_dimension_key_has_no_granularity_suffix(
        self, engine,
    ) -> None:
        """A time dimension keys off the bare column — the granularity lives in
        the emitted DATE_TRUNC, never in the result key."""
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                time_dimensions=[
                    TimeDimension(
                        dimension=ColumnRef(name="created_at"),
                        granularity=TimeGranularity.MONTH,
                    ),
                ],
                measures=[ModelMeasure(formula="*:count")],
            ),
            ["orders.created_at", "orders._count"],
        )


# ===========================================================================
# 2 — Joined dimensions keep the full dotted path.
# ===========================================================================


class TestJoinedDimensionResultKeys:
    async def test_single_hop_joined_dimension(self, engine) -> None:
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="customers.region_id")],
                measures=[ModelMeasure(formula="*:count")],
            ),
            ["orders.customers.region_id", "orders._count"],
        )

    async def test_multi_hop_joined_dimension(self, engine) -> None:
        """Two hops keep BOTH hops in the key — the path is not collapsed."""
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="customers.regions.name")],
                measures=[ModelMeasure(formula="*:count")],
            ),
            ["orders.customers.regions.name", "orders._count"],
        )

    async def test_joined_derived_dimension(self, engine) -> None:
        """A DERIVED joined column (``Column.sql`` set — a ``ColumnSqlKey``)
        keys identically to a base joined column."""
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="customers.rev_x2")],
                measures=[ModelMeasure(formula="*:count")],
            ),
            ["orders.customers.rev_x2", "orders._count"],
        )


# ===========================================================================
# 3 — Cross-model aggregates.
# ===========================================================================


class TestCrossModelResultKeys:
    async def test_cross_model_aggregate_keeps_path(self, engine) -> None:
        """A cross-model measure keys as
        ``<relation>.<path>.<canonical_agg_name>`` — the join path is part of
        the public key, not just of the internal CTE name."""
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="customers.revenue:sum")],
            ),
            ["orders.status", "orders.customers.revenue_sum"],
        )

    async def test_cross_model_star_count_keeps_path(self, engine) -> None:
        """``customers.*:count`` keeps its path AND collapses the star:
        ``orders.customers._count``."""
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="customers.*:count")],
            ),
            ["orders.status", "orders.customers._count"],
        )

    async def test_local_and_cross_model_measures_together(self, engine) -> None:
        """Mixed local + cross-model: both key forms coexist, in declaration
        order. (The declaration-order projection change may reorder this
        shape in a later PR — re-approved
        there.)"""
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    ModelMeasure(formula="amount:sum"),
                    ModelMeasure(formula="customers.revenue:sum"),
                ],
            ),
            [
                "orders.status",
                "orders.amount_sum",
                "orders.customers.revenue_sum",
            ],
        )


# ===========================================================================
# 4 — Windowed measures (the ``window=`` reserved kwarg).
# ===========================================================================


class TestWindowedResultKeys:
    async def test_duration_windowed_measure_key(self, engine) -> None:
        """A duration-windowed measure keys off the canonical aggregate name
        INCLUDING the window kwarg suffix, so two different windows over the
        same column do not collide."""
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                time_dimensions=[
                    TimeDimension(
                        dimension=ColumnRef(name="created_at"),
                        granularity=TimeGranularity.MONTH,
                    ),
                ],
                measures=[ModelMeasure(formula="amount:sum(window='90d')")],
            ),
            ["orders.created_at", "orders.amount_sum_window_90d"],
        )

    async def test_two_windows_over_same_column_are_distinct_keys(
        self, engine,
    ) -> None:
        """The window suffix is what keeps them apart — the exact reason the
        suffix is part of the canonical name."""
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                time_dimensions=[
                    TimeDimension(
                        dimension=ColumnRef(name="created_at"),
                        granularity=TimeGranularity.MONTH,
                    ),
                ],
                measures=[
                    ModelMeasure(formula="amount:sum(window='90d')"),
                    ModelMeasure(formula="amount:sum(window='30d')"),
                ],
            ),
            [
                "orders.created_at",
                "orders.amount_sum_window_90d",
                "orders.amount_sum_window_30d",
            ],
        )


# ===========================================================================
# 5 — Hidden / order-only slots are absent from result keys.
# ===========================================================================


class TestHiddenAndOrderOnlySlots:
    """Ordering a GROUPED query by an ungrouped raw column
    makes the planner materialise a hidden ``MAX(...)`` wrap slot. That slot
    must drive ORDER BY without ever surfacing as a public result key."""

    async def test_order_only_slot_is_not_a_result_key(self, engine) -> None:
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="*:count")],
                order=[
                    OrderItem(
                        column=ColumnRef(name="created_at"), direction="desc",
                    ),
                ],
            ),
            ["orders.status", "orders._count"],
        )

    async def test_order_only_slot_still_orders_the_rows(self, engine) -> None:
        """The companion half: absence from the key list must not mean the
        hidden slot was dropped — it still drives ORDER BY.

        Per status, ``MAX(created_at)`` is 2024-01-22 for ``new`` and
        2024-02-11 for ``old``. Descending on that hidden max yields
        ``["old", "new"]`` — the REVERSE of the projected dimension's
        alphabetical order, so a silently-dropped order term flips this
        assertion instead of passing by luck.
        """
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="*:count")],
                order=[
                    OrderItem(
                        column=ColumnRef(name="created_at"), direction="desc",
                    ),
                ],
            )
        )
        assert [r["orders.status"] for r in resp.data] == ["old", "new"]


# ===========================================================================
# 6 — Parametric aggregates, incl. the cross-model kwarg-suffix divergence.
# ===========================================================================


class TestParametricResultKeys:
    async def test_local_parametric_aggregate_suffix(self, engine) -> None:
        """``percentile(p=0.9)`` canonicalises to ``_percentile_p_0_9`` — the
        kwarg name and its value are both sanitised into the key."""
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="amount:percentile(p=0.9)")],
            ),
            ["orders.status", "orders.amount_percentile_p_0_9"],
        )

    async def test_two_local_parametric_variants_are_distinct(
        self, engine,
    ) -> None:
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    ModelMeasure(formula="amount:percentile(p=0.5)"),
                    ModelMeasure(formula="amount:percentile(p=0.9)"),
                ],
            ),
            [
                "orders.status",
                "orders.amount_percentile_p_0_5",
                "orders.amount_percentile_p_0_9",
            ],
        )

    async def test_cross_model_parametric_retains_kwarg_suffix(
        self, engine,
    ) -> None:
        """The DELIBERATE divergence.

        The deleted legacy enrichment path dropped the kwarg suffix from
        cross-model parametric aggregates, which made two variants collide on
        one CTE alias. The typed pipeline RETAINS the suffix: correctness over
        bit-identical legacy output. This test pins the retention as intended
        behavior so a future "restore parity" change has to argue with a test
        rather than with a comment.
        """
        await _assert_result_keys(
            engine,
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    ModelMeasure(formula="customers.revenue:percentile(p=0.5)"),
                    ModelMeasure(formula="customers.revenue:percentile(p=0.9)"),
                ],
            ),
            [
                "orders.status",
                "orders.customers.revenue_percentile_p_0_5",
                "orders.customers.revenue_percentile_p_0_9",
            ],
        )


# ===========================================================================
# 7 — Internal identifiers vs public result keys stay separated.
# ===========================================================================


class TestIdentifierSeparation:
    """``naming.result_key`` (dotted, public) and ``naming.flat_name``
    (``__``-joined, internal inner-stage bind names) own two different forms.
    A ``__`` leaking into a public key means the two mixed — the D3 /
    shape a past bug produced."""

    async def test_no_flattened_key_leaks_from_a_joined_query(
        self, engine,
    ) -> None:
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[
                    ColumnRef(name="customers.regions.name"),
                    ColumnRef(name="customers.rev_x2"),
                ],
                measures=[ModelMeasure(formula="customers.revenue:sum")],
            )
        )
        for key in list(resp.columns) + list(resp.data[0].keys()):
            assert "__" not in key, f"inner-stage flat name leaked publicly: {key}"

    async def test_public_keys_are_dotted_not_flattened(self, engine) -> None:
        """The positive form of the same contract: the dotted key is present
        AND its flattened twin is absent."""
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="customers.regions.name")],
                measures=[ModelMeasure(formula="*:count")],
            )
        )
        assert "orders.customers.regions.name" in resp.columns
        assert "orders.customers__regions__name" not in resp.columns
