"""DEV-1450 follow-up #2 — cross-model re-root strategy is owned by the planner.

Re-rooting (the C1 nested ``rerooted_plan`` that preserves host-dimension
grain instead of CROSS JOINing a scalar) used to be a post-hoc mutation in
``stage_planner.plan_query`` (``_maybe_reroot_cross_model_plan``). It is now
a responsibility of the ``CrossModelPlanner`` strategy: ``plan_query`` calls
``planner.plan(...)`` ONCE per cross-model aggregate, passing ``host_query``,
``public_projection`` and a ``subplan_builder`` callback; the strategy decides
forward-plan vs re-rooted-plan.

This is a behavior-preserving refactor — these tests pin the behavior on both
sides of it:

* a re-root case still groups by the re-rooted grain (not a scalar broadcast);
* ``plan_query`` over a re-root case yields a ``CrossModelAggregatePlan`` with
  ``rerooted_plan`` set (the strategy owns it; no stage_planner post-pass);
* ``IsolatedCteCrossModelPlanner().plan(...)`` called WITHOUT the new kwargs
  returns the forward plan (back-compat for ~30 direct test call sites);
* a forward-path cross-model aggregate is not re-rooted.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from typing import AsyncIterator

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, ColumnKey
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.cross_model_planner import IsolatedCteCrossModelPlanner
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Planner-level fixtures: orders -> customers (agg) -> regions
# ---------------------------------------------------------------------------


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


def _customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers",
        data_source="prod",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="region_id", type=DataType.INT),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="region", type=DataType.TEXT),
        ],
        joins=[
            ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]]),
        ],
    )


def _regions_model() -> SlayerModel:
    return SlayerModel(
        name="regions",
        data_source="prod",
        sql_table="regions",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="name", type=DataType.TEXT),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(
        source_model=_orders_model(),
        referenced_models=[_customers_model(), _regions_model()],
    )


def _reroot_query() -> SlayerQuery:
    # Dimension is two hops out (orders -> customers -> regions); the agg
    # target is one hop (customers). The dim is OFF the host->target forward
    # path, so the forward CTE would CROSS JOIN a scalar -> re-root required.
    return SlayerQuery(
        source_model="orders",
        dimensions=["customers.regions.name"],
        measures=[{"formula": "customers.revenue:sum"}],
    )


def _forward_query() -> SlayerQuery:
    # Dimension is on the host->target forward path (customers.region), so
    # the forward CTE shares grain directly — no re-root.
    return SlayerQuery(
        source_model="orders",
        dimensions=["customers.region"],
        measures=[{"formula": "customers.revenue:sum"}],
    )


# ---------------------------------------------------------------------------
# Planner-level behavior
# ---------------------------------------------------------------------------


class TestRerootStrategyOwnership:
    def test_reroot_case_sets_rerooted_plan(self) -> None:
        planned = plan_query(query=_reroot_query(), bundle=_bundle())
        assert len(planned.cross_model_aggregate_plans) == 1
        plan = planned.cross_model_aggregate_plans[0]
        assert plan.rerooted_plan is not None
        assert plan.rerooted_grain_pairs  # at least one (host, sub) grain pair
        assert plan.rerooted_agg_slot_id is not None

    def test_forward_case_not_rerooted(self) -> None:
        planned = plan_query(query=_forward_query(), bundle=_bundle())
        assert len(planned.cross_model_aggregate_plans) == 1
        plan = planned.cross_model_aggregate_plans[0]
        assert plan.rerooted_plan is None

    def test_planner_plan_without_kwargs_returns_forward_plan(self) -> None:
        # Back-compat: the ~30 direct ``planner.plan(...)`` call sites pass no
        # host_query / public_projection / subplan_builder. Without them the
        # strategy returns the forward plan and never re-roots.
        agg_key = AggregateKey(
            source=ColumnKey(path=("customers",), leaf="revenue"), agg="sum",
        )
        plan = IsolatedCteCrossModelPlanner().plan(
            aggregate_slot_id="s1",
            aggregate_key=agg_key,
            bundle=_bundle(),
            host_slots=[],
            host_filters=[],
        )
        assert plan.target_model == "customers"
        assert plan.rerooted_plan is None


# ---------------------------------------------------------------------------
# Engine end-to-end: behavior is preserved across the refactor
# ---------------------------------------------------------------------------


@pytest.fixture
async def engine() -> AsyncIterator[SlayerQueryEngine]:
    d = tempfile.mkdtemp()
    db_path = os.path.join(d, "t.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)")
    cur.executemany("INSERT INTO regions VALUES (?,?)", [(1, "West"), (2, "East")])
    cur.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, region_id INTEGER, "
        "revenue REAL, region TEXT)"
    )
    cur.executemany(
        "INSERT INTO customers VALUES (?,?,?,?)",
        [(1, 1, 100.0, "West"), (2, 1, 50.0, "West"), (3, 2, 70.0, "East")],
    )
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?)",
        [(1, 1, 10.0), (2, 1, 5.0), (3, 2, 7.0), (4, 3, 3.0), (5, 3, 9.0)],
    )
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type="sqlite", database=db_path)
    )
    await storage.save_model(_regions_model())
    await storage.save_model(_customers_model())
    await storage.save_model(_orders_model())
    yield SlayerQueryEngine(storage=storage)


async def test_reroot_preserves_grain_end_to_end(engine):
    """The re-rooted CTE groups customers.revenue by region (West=150, East=70),
    not the scalar global sum (220) broadcast to every row."""
    resp = await engine.execute(_reroot_query())
    by_region = {
        r["orders.customers.regions.name"]: r["orders.customers.revenue_sum"]
        for r in resp.data
    }
    assert by_region == {"West": 150.0, "East": 70.0}


async def test_forward_path_unchanged_end_to_end(engine):
    """A forward-path cross-model aggregate still groups correctly."""
    resp = await engine.execute(_forward_query())
    by_region = {
        r["orders.customers.region"]: r["orders.customers.revenue_sum"]
        for r in resp.data
    }
    assert by_region == {"West": 150.0, "East": 70.0}
