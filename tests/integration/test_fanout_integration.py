"""Integration tests for DEV-1683 fan-out detection.

End-to-end against a real SQLite database, following the ``cross_model_env`` /
``diamond_env`` fixture pattern. Pins both silent-fan-out failure modes with
concrete numbers, the invariant-aggregation carve-out, the fail-closed rule,
and the no-regression safe direction.

Run with: pytest tests/integration/test_fanout_integration.py -m integration
"""

import sqlite3

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import FanoutError
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, ModelExtension, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

pytestmark = pytest.mark.integration


@pytest.fixture
async def fanout_env(tmp_path):
    """SQLite env with a hand-declared REVERSE (1:N) join.

    customers (the "one" side) declares a reverse join to orders (the "many"
    side) — the shape auto-ingestion never produces. orders also has the normal
    forward many-to-one join back to customers. `notes` has NO declared primary
    key (fail-closed probe).
    """
    db_path = tmp_path / "fanout.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, balance REAL, score REAL)")
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, status TEXT, amount REAL)")
    conn.execute("CREATE TABLE notes (id INTEGER PRIMARY KEY, customer_id INTEGER, body TEXT)")
    conn.executemany("INSERT INTO customers VALUES (?, ?, ?, ?)", [
        (1, "Alice", 1000.0, 90.0),
        (2, "Bob", 500.0, 60.0),
    ])
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", [
        (1, 1, "shipped", 100.0),
        (2, 1, "shipped", 200.0),
        (3, 1, "shipped", 50.0),
        (4, 2, "shipped", 300.0),
        (5, 2, "pending", 10.0),
    ])
    conn.executemany("INSERT INTO notes VALUES (?, ?, ?)", [
        (1, 1, "n1"), (2, 1, "n2"), (3, 2, "n3"),
    ])
    conn.commit()
    conn.close()

    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    await storage.save_datasource(DatasourceConfig(name="db", type="sqlite", database=str(db_path)))

    await storage.save_model(SlayerModel(
        name="customers", sql_table="customers", data_source="db",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="balance", sql="balance", type=DataType.DOUBLE),
            Column(name="score", sql="score", type=DataType.DOUBLE),
        ],
        joins=[
            # REVERSE join: one customer -> many orders (fan-out direction)
            ModelJoin(target_model="orders", join_pairs=[["id", "customer_id"]]),
            # REVERSE join to a target WITHOUT a declared primary key
            ModelJoin(target_model="notes", join_pairs=[["id", "customer_id"]]),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="db",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
        # forward many-to-one join back to customers (safe)
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    ))
    await storage.save_model(SlayerModel(
        name="notes", sql_table="notes", data_source="db",
        columns=[
            # NOTE: no primary_key declared anywhere
            Column(name="id", sql="id", type=DataType.INT),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
            Column(name="body", sql="body", type=DataType.TEXT),
        ],
    ))

    return SlayerQueryEngine(storage=storage)


# ---------------------------------------------------------------------------
# Mode 1 — base same-model measure fan-out
# ---------------------------------------------------------------------------

async def test_mode1_filter_only_fires(fanout_env):
    engine = fanout_env
    query = SlayerQuery(
        source_model="customers",
        measures=[ModelMeasure(formula="balance:sum")],
        filters=["orders.status = 'shipped'"],
    )
    with pytest.raises(FanoutError):
        await engine.execute(query)


async def test_mode1_dimension_fires(fanout_env):
    engine = fanout_env
    query = SlayerQuery(
        source_model="customers",
        measures=[ModelMeasure(formula="balance:sum")],
        dimensions=[ColumnRef(name="orders.status")],
    )
    with pytest.raises(FanoutError):
        await engine.execute(query)


async def test_mode1_star_count_fires(fanout_env):
    """*:count (COUNT(*)) is non-invariant -> fires across the reverse fanout."""
    engine = fanout_env
    query = SlayerQuery(
        source_model="customers",
        measures=[ModelMeasure(formula="*:count")],
        filters=["orders.status = 'shipped'"],
    )
    with pytest.raises(FanoutError):
        await engine.execute(query)


async def test_mode1_dry_run_also_fires(fanout_env):
    engine = fanout_env
    query = SlayerQuery(
        source_model="customers",
        measures=[ModelMeasure(formula="balance:sum")],
        filters=["orders.status = 'shipped'"],
    )
    with pytest.raises(FanoutError):
        await engine.execute(query, dry_run=True)


async def test_mode1_explain_also_fires(fanout_env):
    engine = fanout_env
    query = SlayerQuery(
        source_model="customers",
        measures=[ModelMeasure(formula="balance:sum")],
        filters=["orders.status = 'shipped'"],
    )
    with pytest.raises(FanoutError):
        await engine.execute(query, explain=True)


async def test_mode1_multihop_only_later_hop_fans_fires(fanout_env):
    """orders -> customers (safe PK join) -> notes (1:N, no PK). The base
    orders.amount:sum fans across the SECOND hop only, and still fires."""
    engine = fanout_env
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="amount:sum")],
        dimensions=[ColumnRef(name="customers.notes.body")],
    )
    with pytest.raises(FanoutError):
        await engine.execute(query)


async def test_base_measure_no_fire_when_fanning_join_only_serves_cross_model_cte(fanout_env):
    """A non-invariant base measure does NOT fire when the fanning join is
    isolated into a cross-model CTE (not pulled into the base FROM by any
    dimension/filter). Exercises the shared base_from_join_aliases pruning
    (skip_isolated=True) end-to-end."""
    engine = fanout_env
    query = SlayerQuery(
        source_model="customers",
        measures=[
            ModelMeasure(formula="*:count"),          # base, non-invariant
            ModelMeasure(formula="orders.amount:max"),  # cross-model, invariant -> no Mode 2 fire
        ],
    )
    response = await engine.execute(query)
    # *:count over customers is unaffected by the isolated orders CTE
    assert response.data[0]["customers._count"] == 2


async def test_mode1_count_distinct_does_not_fire(fanout_env):
    """count_distinct on the source PK survives fan-out (invariant) -> no error."""
    engine = fanout_env
    query = SlayerQuery(
        source_model="customers",
        measures=[ModelMeasure(formula="id:count_distinct")],
        filters=["orders.status = 'shipped'"],
    )
    response = await engine.execute(query)
    # Distinct customers with a shipped order = {1, 2} = 2
    assert response.data[0]["customers.id_count_distinct"] == 2


async def test_mode1_max_does_not_fire(fanout_env):
    """max is fan-out-invariant -> no error even across the 1:N join."""
    engine = fanout_env
    query = SlayerQuery(
        source_model="customers",
        measures=[ModelMeasure(formula="balance:max")],
        filters=["orders.status = 'shipped'"],
    )
    response = await engine.execute(query)
    assert response.data[0]["customers.balance_max"] == pytest.approx(1000.0)


# ---------------------------------------------------------------------------
# Mode 2 — cross-model re-rooting grain shift
# ---------------------------------------------------------------------------

async def test_mode2_avg_fires(fanout_env):
    engine = fanout_env
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="customers.score:avg")],
        dimensions=[ColumnRef(name="status")],
    )
    with pytest.raises(FanoutError):
        await engine.execute(query)


async def test_mode2_max_does_not_fire(fanout_env):
    """Invariant agg across the re-rooting 1:N join -> no error."""
    engine = fanout_env
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="customers.score:max")],
        dimensions=[ColumnRef(name="status")],
    )
    response = await engine.execute(query)
    by_status = {r["orders.status"]: r for r in response.data}
    # shipped: customers Alice(90), Bob(60) -> 90 ; pending: Bob(60) -> 60
    assert by_status["shipped"]["orders.customers.score_max"] == pytest.approx(90.0)
    assert by_status["pending"]["orders.customers.score_max"] == pytest.approx(60.0)


# ---------------------------------------------------------------------------
# Fail-closed: target without a declared primary key
# ---------------------------------------------------------------------------

async def test_no_pk_target_fails_closed(fanout_env):
    """A joined target with no declared primary key cannot be proved
    many-to-one, so a non-invariant aggregate across it fires."""
    engine = fanout_env
    query = SlayerQuery(
        source_model="customers",
        measures=[ModelMeasure(formula="balance:sum")],
        dimensions=[ColumnRef(name="notes.body")],
    )
    with pytest.raises(FanoutError):
        await engine.execute(query)


# ---------------------------------------------------------------------------
# No regression: the safe many-to-one direction
# ---------------------------------------------------------------------------

async def test_safe_many_to_one_does_not_fire(fanout_env):
    """Fact measure sliced by a many-to-one dimension (PK join) -> correct, no error."""
    engine = fanout_env
    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="amount:sum")],
        dimensions=[ColumnRef(name="customers.name")],
    )
    response = await engine.execute(query)
    by_name = {r["orders.customers.name"]: r for r in response.data}
    assert by_name["Alice"]["orders.amount_sum"] == pytest.approx(350.0)  # 100+200+50
    assert by_name["Bob"]["orders.amount_sum"] == pytest.approx(310.0)    # 300+10


# ---------------------------------------------------------------------------
# Multi-stage DAG: per-stage detection
# ---------------------------------------------------------------------------

async def test_multistage_inner_fanning_stage_fires(fanout_env):
    """A fanning INNER stage (referenced by a safe outer stage) raises during
    that inner stage's own enrichment — genuine per-stage detection."""
    engine = fanout_env
    inner = SlayerQuery(
        name="cust_by_status",
        source_model="customers",
        measures=[ModelMeasure(formula="balance:sum")],  # fans across reverse orders join
        dimensions=[ColumnRef(name="orders.status")],
    )
    outer = SlayerQuery(
        source_model="cust_by_status",
        measures=[ModelMeasure(formula="balance_sum:sum")],
    )
    with pytest.raises(FanoutError):
        await engine.execute(query=[inner, outer])


async def test_multistage_reaggregation_does_not_fire(fanout_env):
    """Re-aggregating a query-backed stage's own columns never fans (no join)."""
    engine = fanout_env
    inner = SlayerQuery(
        name="order_totals",
        source_model="orders",
        dimensions=[ColumnRef(name="customer_id")],
        measures=[ModelMeasure(formula="amount:sum")],
        filters=["status = 'shipped'"],
    )
    outer = SlayerQuery(
        source_model="order_totals",
        measures=[ModelMeasure(formula="amount_sum:sum")],
    )
    response = await engine.execute(query=[inner, outer])
    # shipped amounts: 100+200+50 (Alice) + 300 (Bob) = 650
    assert response.data[0]["order_totals.amount_sum_sum"] == pytest.approx(650.0)


async def test_query_backed_dimension_join_fails_closed(fanout_env):
    """DEV-1689: a query-backed model used as a base-FROM dimension join has no
    declared primary key, so a non-invariant base aggregate across it fires —
    a documented fail-closed false positive until PK propagation lands."""
    engine = fanout_env
    cust_dim = SlayerQuery(
        name="cust_dim",
        source_model="customers",
        dimensions=[ColumnRef(name="id")],
        measures=[ModelMeasure(formula="score:max")],
    )
    outer = SlayerQuery(
        source_model=ModelExtension(
            source_name="orders",
            joins=[{"target_model": "cust_dim", "join_pairs": [["customer_id", "id"]]}],
        ),
        measures=[ModelMeasure(formula="amount:sum")],
        dimensions=[ColumnRef(name="cust_dim.id")],
    )
    with pytest.raises(FanoutError):
        await engine.execute(query=[cust_dim, outer])
