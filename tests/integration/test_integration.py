"""Integration tests — end-to-end queries against a real SQLite database.

Run with: pytest tests/integration/test_integration.py -m integration
"""

import sqlite3

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import (
    ColumnRef,
    ModelExtension,
    OrderItem,
    SlayerQuery,
    TimeDimension,
)
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.sql.client import _sync_engines
from slayer.storage.yaml_storage import YAMLStorage

pytestmark = pytest.mark.integration


@pytest.fixture
async def integration_env(tmp_path):
    """Create a real SQLite database with test data, configure storage, models, and engine."""

    # -- SQLite database --
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            region TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            amount REAL NOT NULL,
            customer_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        )
        """
    )

    customers = [
        (1, "Alice", "US"),
        (2, "Bob", "EU"),
        (3, "Charlie", "US"),
    ]
    cur.executemany("INSERT INTO customers VALUES (?, ?, ?)", customers)

    orders = [
        (1, "completed", 100.0, 1, "2025-01-15"),
        (2, "completed", 200.0, 2, "2025-01-20"),
        (3, "pending", 50.0, 1, "2025-02-10"),
        (4, "cancelled", 75.0, 3, "2025-02-15"),
        (5, "completed", 300.0, 2, "2025-03-05"),
        (6, "pending", 25.0, 3, "2025-03-20"),
    ]
    cur.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", orders)

    conn.commit()
    conn.close()

    # -- YAML storage --
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))

    # -- Datasource config --
    datasource = DatasourceConfig(
        name="test_sqlite",
        type="sqlite",
        database=str(db_path),
    )
    await storage.save_datasource(datasource)

    # -- Orders model --
    orders_model = SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="test_sqlite",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="status", sql="status", type=DataType.STRING),
            Column(name="customer_id", sql="customer_id", type=DataType.NUMBER),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="amount", sql="amount", type=DataType.NUMBER),

            Column(name="total_amount", sql="amount", type=DataType.NUMBER),
            Column(name="latest_amount", sql="amount", type=DataType.NUMBER),
        ],
    )
    await storage.save_model(orders_model)

    # -- Customers model --
    customers_model = SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="test_sqlite",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="name", sql="name", type=DataType.STRING),
            Column(name="region", sql="region", type=DataType.STRING),

        ],
    )
    await storage.save_model(customers_model)

    engine = SlayerQueryEngine(storage=storage)
    return engine


async def test_count_query(integration_env):
    """Count all orders."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count")],
    )
    response = await engine.execute(query)

    assert isinstance(response, SlayerResponse)
    assert response.row_count == 1
    assert response.data[0]["orders._count"] == 6


async def test_sum_measure(integration_env):
    """Sum of order amounts."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="total_amount:sum")],
    )
    response = await engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders.total_amount_sum"] == pytest.approx(750.0)


async def test_dimensions_groupby(integration_env):
    """Count orders grouped by status."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count")],
        dimensions=[ColumnRef(name="status")],
    )
    response = await engine.execute(query)

    assert response.row_count == 3
    rows_by_status = {row["orders.status"]: row["orders._count"] for row in response.data}
    assert rows_by_status["completed"] == 3
    assert rows_by_status["pending"] == 2
    assert rows_by_status["cancelled"] == 1


async def test_filter_equals(integration_env):
    """Filter orders where status = 'completed'."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count")],
        filters=["status == 'completed'"],
    )
    response = await engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders._count"] == 3


async def test_filter_gt(integration_env):
    """Filter orders where amount > 50."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count")],
        filters=["amount > 50"],
    )
    response = await engine.execute(query)

    assert response.row_count == 1
    # Orders with amount > 50: 100, 200, 75, 300 = 4
    assert response.data[0]["orders._count"] == 4


async def test_order_by(integration_env):
    """Order results by count descending."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count")],
        dimensions=[ColumnRef(name="status")],
        order=[
            OrderItem(column=ColumnRef(name="count"), direction="desc"),
        ],
    )
    response = await engine.execute(query)

    assert response.row_count == 3
    counts = [row["orders._count"] for row in response.data]
    assert counts == sorted(counts, reverse=True)
    # completed=3 is the highest count
    assert response.data[0]["orders.status"] == "completed"


async def test_limit(integration_env):
    """Limit results to 2 rows."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count")],
        dimensions=[ColumnRef(name="status")],
        order=[
            OrderItem(column=ColumnRef(name="count"), direction="desc"),
        ],
        limit=2,
    )
    response = await engine.execute(query)

    assert response.row_count == 2


async def test_multiple_measures(integration_env):
    """Count and sum in the same query."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        measures=[
            ModelMeasure(formula="*:count"),
            ModelMeasure(formula="total_amount:sum"),
        ],
    )
    response = await engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders._count"] == 6
    assert response.data[0]["orders.total_amount_sum"] == pytest.approx(750.0)



async def test_cumsum_change_identity(integration_env):
    """Mathematical identity: cumsum(change(x)) == x - x[0] for all rows after the first."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="*:count"),
            ModelMeasure(formula="cumsum(change(*:count))", name="cumsum_change"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    # 3 months of data: Jan(2), Feb(2), Mar(2)
    assert response.row_count == 3
    assert "orders.cumsum_change" in response.columns

    # First row: change is NULL (no previous period), cumsum(NULL) = NULL
    assert response.data[0]["orders.cumsum_change"] is None

    # Remaining rows: cumsum(change(x)) == x - x[0]
    first_count = response.data[0]["orders._count"]
    for row in response.data[1:]:
        assert row["orders.cumsum_change"] == row["orders._count"] - first_count


async def test_nested_cumsum_of_cumsum(integration_env):
    """Nested transforms: cumsum(cumsum(x)) should produce monotonically increasing values."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="*:count"),
            ModelMeasure(formula="cumsum(*:count)", name="cs"),
            ModelMeasure(formula="cumsum(cumsum(*:count))", name="cs_cs"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 3
    # cumsum(cumsum) should be non-decreasing
    vals = [r["orders.cs_cs"] for r in response.data]
    assert all(a <= b for a, b in zip(vals, vals[1:]))
    # For constant counts (2,2,2): cumsum = (2,4,6), cumsum(cumsum) = (2,6,12)
    assert vals == [2, 6, 12]


async def test_consecutive_periods_counts_trailing_true_run(integration_env):
    """consecutive_periods returns the current trailing run length at the query grain."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="consecutive_periods(total_amount:sum > 200)", name="positive_run"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 3
    # Monthly totals are Jan=300 (true), Feb=125 (false), Mar=325 (true).
    assert [r["orders.positive_run"] for r in response.data] == [1, 0, 1]


async def test_consecutive_periods_with_non_boolean_argument(integration_env):
    """consecutive_periods on a non-boolean argument treats NULL/0 as false.

    The CTE wraps the argument in `IS NOT NULL AND <> 0` so behaviour is
    portable across dialects rather than relying on each engine's
    truthiness coercion in CASE WHEN.
    """
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="consecutive_periods(total_amount:sum)", name="any_revenue_run"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 3
    # Every month has nonzero revenue (Jan=300, Feb=125, Mar=325) so the run
    # is monotonically increasing with no resets.
    assert [r["orders.any_revenue_run"] for r in response.data] == [1, 2, 3]


async def test_consecutive_periods_partitions_by_dimension(integration_env):
    """Runs reset independently for each non-time dimension."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="*:count"),
            ModelMeasure(formula="consecutive_periods(*:count > 0)", name="status_run"),
        ],
        order=[
            OrderItem(column=ColumnRef(name="status"), direction="asc"),
            OrderItem(column=ColumnRef(name="created_at"), direction="asc"),
        ],
    )
    response = await engine.execute(query)

    by_status = {}
    for row in response.data:
        by_status.setdefault(row["orders.status"], []).append(row["orders.status_run"])

    assert by_status["completed"] == [1, 2]
    assert by_status["pending"] == [1, 2]
    assert by_status["cancelled"] == [1]


async def test_consecutive_periods_comparison_is_selectable(integration_env):
    """The integer streak result composes with normal comparison syntax."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="consecutive_periods(total_amount:sum > 0) >= 2", name="has_two_month_run"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert [bool(r["orders.has_two_month_run"]) for r in response.data] == [False, True, True]


async def test_consecutive_periods_comparison_is_filterable(integration_env):
    """Inline consecutive_periods comparisons can be used as post-filters."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="total_amount:sum")],
        filters=["consecutive_periods(total_amount:sum > 0) >= 2"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 2
    assert [r["orders.created_at"] for r in response.data] == ["2025-02-01", "2025-03-01"]


async def test_arithmetic_expression(integration_env):
    """Arithmetic field: total_amount / count = average."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        measures=[
            ModelMeasure(formula="*:count"),
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="total_amount:sum / *:count", name="avg_amount"),
        ],
    )
    response = await engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders._count"] == 6
    assert response.data[0]["orders.avg_amount"] == pytest.approx(125.0)


async def test_time_shift_row_based(integration_env):
    """time_shift(x, -1) without granularity → LAG (previous row)."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="time_shift(total_amount:sum, -1)", name="prev"),
            ModelMeasure(formula="time_shift(total_amount:sum, 1)", name="next"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    # 3 months: Jan(300), Feb(125), Mar(325)
    assert response.row_count == 3

    # Row-based backward shift (LAG): first row has no previous
    assert response.data[0]["orders.prev"] is None
    assert response.data[1]["orders.prev"] == pytest.approx(300.0)  # Feb's prev = Jan
    assert response.data[2]["orders.prev"] == pytest.approx(125.0)  # Mar's prev = Feb

    # Row-based forward shift (LEAD): last row has no next
    assert response.data[0]["orders.next"] == pytest.approx(125.0)  # Jan's next = Feb
    assert response.data[1]["orders.next"] == pytest.approx(325.0)  # Feb's next = Mar
    assert response.data[2]["orders.next"] is None


async def test_time_shift_calendar_based(integration_env):
    """time_shift(x, -1, 'month') with granularity → calendar-based self-join."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="time_shift(total_amount:sum, -1, 'month')", name="prev_month"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    # 3 months: Jan(300), Feb(125), Mar(325)
    assert response.row_count == 3

    # Calendar-based: Jan has no previous month in data → NULL
    assert response.data[0]["orders.prev_month"] is None
    # Feb's previous month is Jan
    assert response.data[1]["orders.prev_month"] == pytest.approx(300.0)
    # Mar's previous month is Feb
    assert response.data[2]["orders.prev_month"] == pytest.approx(125.0)


async def test_time_shift_with_date_range(integration_env):
    """time_shift with date_range should fetch shifted data from outside the filtered range."""
    engine = integration_env

    # Query only March, but ask for previous month's value (February)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2025-03-01", "2025-03-31"],
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="time_shift(total_amount:sum, -1, 'month')", name="prev_month"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    # Only March in the result (date filter)
    assert response.row_count == 1
    assert response.data[0]["orders.total_amount_sum"] == pytest.approx(325.0)
    # Previous month (February) should be fetched from the DB, not NULL
    assert response.data[0]["orders.prev_month"] == pytest.approx(125.0)


async def test_change_with_date_range(integration_env):
    """change() with date_range should fetch previous period from outside the filtered range."""
    engine = integration_env

    # Query only March, change should compare to February
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2025-03-01", "2025-03-31"],
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="change(total_amount:sum)", name="amount_change"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 1
    # March(325) - February(125) = 200
    assert response.data[0]["orders.amount_change"] == pytest.approx(200.0)


async def test_change_pct_with_date_range(integration_env):
    """change_pct() with date_range should compute correct percentage from shifted data."""
    engine = integration_env

    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2025-03-01", "2025-03-31"],
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="change_pct(total_amount:sum)", name="pct"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 1
    # (325 - 125) / 125 = 1.6
    assert response.data[0]["orders.pct"] == pytest.approx(1.6)


async def test_multiple_date_range_shifts(integration_env):
    """Multiple self-join transforms with different offsets should each get correct shifted data."""
    engine = integration_env

    # Query Feb only, ask for both previous (Jan) and next (Mar) month
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2025-02-01", "2025-02-28"],
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="time_shift(total_amount:sum, -1, 'month')", name="prev"),
            ModelMeasure(formula="time_shift(total_amount:sum, 1, 'month')", name="next"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders.total_amount_sum"] == pytest.approx(125.0)
    # Jan = 300
    assert response.data[0]["orders.prev"] == pytest.approx(300.0)
    # Mar = 325
    assert response.data[0]["orders.next"] == pytest.approx(325.0)


async def test_forward_row_shift_with_date_range(integration_env):
    """time_shift(x, 1) (forward, row-based) with date_range should fetch the next period."""
    engine = integration_env

    # Query Feb only, ask for the next period's value (March)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2025-02-01", "2025-02-28"],
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="time_shift(total_amount:sum, 1)", name="next_period"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders.total_amount_sum"] == pytest.approx(125.0)
    # Next period (March) should be fetched from DB = 325
    assert response.data[0]["orders.next_period"] == pytest.approx(325.0)


async def test_post_filter_on_change(integration_env):
    """Filter on a computed column (change) should only return matching rows."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # change values: Jan=NULL, Feb=125-300=-175, Mar=325-125=200
    # Filter: change < 0 → only February
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="change(total_amount:sum)", name="amount_change"),
        ],
        filters=["amount_change < 0"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    # Only February should remain (change = -175)
    assert response.row_count == 1
    assert response.data[0]["orders.amount_change"] == pytest.approx(-175.0)
    assert response.data[0]["orders.total_amount_sum"] == pytest.approx(125.0)


async def test_post_filter_with_base_filter(integration_env):
    """Post-filter and base filter should both be applied correctly."""
    engine = integration_env

    # Without base filter: Jan(300), Feb(125), Mar(325)
    # change: Jan=NULL, Feb=-175, Mar=200
    # Post-filter: amount_change > 0 → only March
    # Base filter: status != 'cancelled' → excludes order 4 (cancelled, 75, Feb)
    # Without cancelled: Jan(300), Feb(50), Mar(325)
    # change: Jan=NULL, Feb=50-300=-250, Mar=325-50=275
    # Post-filter: amount_change > 0 → only March
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="change(total_amount:sum)", name="amount_change"),
        ],
        filters=["status != 'cancelled'", "amount_change > 0"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    # Only March (non-cancelled=325, change=275)
    assert response.row_count == 1
    assert response.data[0]["orders.amount_change"] == pytest.approx(275.0)


async def test_inline_transform_filter(integration_env):
    """Transform expressions can be used directly in filters (auto-extracted as hidden fields)."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # change: Jan=NULL, Feb=-175, Mar=200
    # Filter: change(total_amount) < 0 → only February
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="total_amount:sum")],
        filters=["change(total_amount:sum) < 0"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders.total_amount_sum"] == pytest.approx(125.0)


async def test_inline_last_change_filter(integration_env):
    """last(change(x)) in filter: keep rows only if the most recent period's change matches."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # change: Jan=NULL, Feb=-175, Mar=200
    # last(change) = 200 (March's change, broadcast to all rows)
    # Filter: last(change(total_amount)) > 0 → all rows pass (200 > 0)
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="total_amount:sum")],
        filters=["last(change(total_amount:sum)) > 0"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    # last(change) = 200 > 0, so all 3 rows pass
    assert response.row_count == 3

    # Now filter for < 0 → no rows pass (last change is 200)
    query2 = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="total_amount:sum")],
        filters=["last(change(total_amount:sum)) < 0"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response2 = await engine.execute(query2)
    assert response2.row_count == 0


async def test_arithmetic_transform_filter(integration_env):
    """Arithmetic expressions with transforms in filters: change(x) / x > threshold."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # change: Jan=NULL, Feb=-175, Mar=200
    # change / total_amount: Jan=NULL, Feb=-175/125=-1.4, Mar=200/325≈0.615
    # Filter: change(total_amount) / total_amount > 0 → only March
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="total_amount:sum")],
        filters=["change(total_amount:sum) / total_amount:sum > 0"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    # Only March passes (positive change ratio)
    assert response.row_count == 1
    assert response.data[0]["orders.total_amount_sum"] == pytest.approx(325.0)


async def test_transform_on_filter_rhs(integration_env):
    """Transform expressions work on the RHS of filters too."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # time_shift(total_amount, -1): Jan=NULL, Feb=300, Mar=125
    # Filter: total_amount > time_shift(total_amount, -1) → months where value increased
    # Jan: 300 > NULL → NULL (filtered out), Feb: 125 > 300 → false, Mar: 325 > 125 → true
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="total_amount:sum")],
        filters=["total_amount:sum > time_shift(total_amount:sum, -1)"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    # Only March (325 > 125)
    assert response.row_count == 1
    assert response.data[0]["orders.total_amount_sum"] == pytest.approx(325.0)


async def test_last_measure_type(integration_env):
    """A measure with type=last should return the most recent time bucket's value."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # latest_amount has type=last, so querying it as a bare measure
    # should auto-wrap with last() and return Mar's value (325) for all rows
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="latest_amount:last"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 3
    # type=last returns the latest record's value within each month:
    # Jan: orders on 15th(100) and 20th(200) → latest = 200
    # Feb: orders on 10th(50) and 15th(75) → latest = 75
    # Mar: orders on 5th(300) and 20th(25) → latest = 25
    assert response.data[0]["orders.latest_amount_last"] == pytest.approx(200.0)
    assert response.data[1]["orders.latest_amount_last"] == pytest.approx(75.0)
    assert response.data[2]["orders.latest_amount_last"] == pytest.approx(25.0)


async def test_last_function(integration_env):
    """last() function should broadcast the most recent time bucket's value to all rows."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # last(total_amount) = March's total (325) broadcast to all rows
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="last(total_amount:sum)", name="latest"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 3
    # last() broadcasts the most recent bucket's value to ALL rows
    latest_vals = [r["orders.latest"] for r in response.data]
    assert len(set(latest_vals)) == 1  # Same value everywhere
    assert latest_vals[0] == pytest.approx(325.0)  # March's SUM


async def test_having_filter(integration_env):
    """Filters on measures should use HAVING with the aggregate expression."""
    engine = integration_env

    # Group by status: completed(3 orders), pending(2), cancelled(1)
    # Filter: _count > 1 → only completed and pending
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="*:count")],
        filters=["_count > 1"],
        order=[OrderItem(column=ColumnRef(name="_count"), direction="desc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 2
    assert response.data[0]["orders.status"] == "completed"
    assert response.data[0]["orders._count"] == 3
    assert response.data[1]["orders.status"] == "pending"
    assert response.data[1]["orders._count"] == 2


async def test_having_filter_with_sum(integration_env):
    """HAVING on a SUM measure should use the SUM() expression."""
    engine = integration_env

    # Group by status: completed(100+200+300=600), pending(50+25=75), cancelled(75)
    # Filter: total_amount_sum > 100 → only completed
    query = SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="total_amount:sum")],
        filters=["total_amount_sum > 100"],
        order=[OrderItem(column=ColumnRef(name="total_amount_sum"), direction="desc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders.status"] == "completed"
    assert response.data[0]["orders.total_amount_sum"] == pytest.approx(600.0)


async def test_having_with_non_groupby_dimension_raises(integration_env):
    """HAVING filter referencing a dimension not in GROUP BY should error early."""
    engine = integration_env

    # Filter mixes measure (count) and dimension (status), but status is not in dimensions
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="*:count")],
        filters=["_count > 1 and status == 'completed'"],
    )
    with pytest.raises(ValueError, match="not in the query's dimensions"):
        await engine.execute(query)


# ---------------------------------------------------------------------------
# type=last with joined time dimensions
# ---------------------------------------------------------------------------

@pytest.fixture
async def joined_time_env(tmp_path):
    """Schema: order_items → orders (with created_at) → stores (with opened_at).

    Tests that type=last resolves through join paths correctly.
    """
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE stores (id INTEGER PRIMARY KEY, name TEXT, opened_at TEXT)")
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, store_id INTEGER, amount REAL, created_at TEXT)")
    conn.execute("CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, qty INTEGER)")
    conn.executemany("INSERT INTO stores VALUES (?, ?, ?)", [
        (1, "Downtown", "2020-01-01"), (2, "Uptown", "2021-06-15"),
    ])
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", [
        (1, 1, 100.0, "2025-01-15"), (2, 1, 200.0, "2025-01-20"),
        (3, 2, 50.0, "2025-02-10"), (4, 2, 75.0, "2025-02-15"),
        (5, 1, 300.0, "2025-03-05"), (6, 2, 25.0, "2025-03-20"),
    ])
    conn.executemany("INSERT INTO order_items VALUES (?, ?, ?)", [
        (1, 1, 2), (2, 2, 3), (3, 3, 1),
        (4, 4, 5), (5, 5, 4), (6, 6, 1),
    ])
    conn.commit()
    conn.close()

    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    await storage.save_datasource(DatasourceConfig(name="db", type="sqlite", database=str(db_path)))

    await storage.save_model(SlayerModel(
        name="stores", sql_table="stores", data_source="db",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="name", sql="name", type=DataType.STRING),
            Column(name="opened_at", sql="opened_at", type=DataType.TIMESTAMP),

        ],
    ))
    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="db",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="store_id", sql="store_id", type=DataType.NUMBER),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="amount", sql="amount", type=DataType.NUMBER),

            Column(name="total_amount", sql="amount", type=DataType.NUMBER),
            Column(name="latest_amount", sql="amount", type=DataType.NUMBER),
        ],
        joins=[ModelJoin(target_model="stores", join_pairs=[["store_id", "id"]])],
    ))
    await storage.save_model(SlayerModel(
        name="order_items", sql_table="order_items", data_source="db",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="order_id", sql="order_id", type=DataType.NUMBER),
            Column(name="qty", sql="qty", type=DataType.NUMBER),

            Column(name="qty_sum", sql="qty", type=DataType.NUMBER),
            Column(name="latest_qty", sql="qty", type=DataType.NUMBER),
        ],
        joins=[ModelJoin(target_model="orders", join_pairs=[["order_id", "id"]])],
    ))

    return SlayerQueryEngine(storage=storage)


@pytest.mark.integration
async def test_last_with_joined_time_dimension(joined_time_env):
    """type=last resolves correctly when the time dimension is from a joined model (single hop)."""
    engine = joined_time_env

    # Query orders with stores.opened_at as time dimension and latest_amount (type=last).
    # The ORDER BY for ROW_NUMBER must reference stores.opened_at, not orders.opened_at.
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="stores.opened_at"),
            granularity=TimeGranularity.YEAR,
        )],
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="latest_amount:last"),
        ],
        order=[OrderItem(column=ColumnRef(name="stores.opened_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 2  # 2020 and 2021
    # Verify the SQL references stores.opened_at (not orders.opened_at)
    assert "stores" in response.sql
    # latest_amount should reflect the most recent order per store-year group
    assert response.data[0]["orders.latest_amount_last"] is not None
    assert response.data[1]["orders.latest_amount_last"] is not None


@pytest.mark.integration
async def test_last_with_multihop_joined_time_dimension(joined_time_env):
    """type=last resolves correctly through multi-hop joins (order_items → orders.created_at)."""
    engine = joined_time_env

    # Query order_items with orders.created_at as time dimension and latest_qty (type=last).
    # The ORDER BY for ROW_NUMBER must reference orders.created_at.
    query = SlayerQuery(
        source_model="order_items",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="orders.created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="qty_sum:sum"),
            ModelMeasure(formula="latest_qty:last"),
        ],
        order=[OrderItem(column=ColumnRef(name="orders.created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 3  # Jan, Feb, Mar
    # Verify the SQL references orders.created_at
    assert "orders.created_at" in response.sql or "orders" in response.sql
    # latest_qty per month: Jan has items for orders on 15th and 20th,
    # most recent is 20th (order 2, qty=3)
    assert response.data[0]["order_items.latest_qty_last"] == 3  # Jan: order 2 (20th)
    assert response.data[1]["order_items.latest_qty_last"] == 5  # Feb: order 4 (15th)
    assert response.data[2]["order_items.latest_qty_last"] == 1  # Mar: order 6 (20th)


# ---------------------------------------------------------------------------
# Cross-model measures
# ---------------------------------------------------------------------------

@pytest.fixture
async def cross_model_env(tmp_path):
    """SQLite env with orders + customers models and an explicit join."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL, created_at TEXT)")
    conn.executemany("INSERT INTO customers VALUES (?, ?, ?)", [
        (1, "Alice", 90.0), (2, "Bob", 60.0), (3, "Charlie", 80.0),
    ])
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?, ?)", [
        (1, 1, 100.0, "2025-01-15"), (2, 1, 200.0, "2025-01-20"),
        (3, 2, 50.0, "2025-02-10"), (4, 2, 75.0, "2025-02-15"),
        (5, 3, 300.0, "2025-03-05"), (6, 1, 25.0, "2025-03-20"),
    ])
    conn.commit()
    conn.close()

    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    await storage.save_datasource(DatasourceConfig(name="db", type="sqlite", database=str(db_path)))

    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="db",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.NUMBER),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),

            Column(name="total_amount", sql="amount", type=DataType.NUMBER),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    ))
    await storage.save_model(SlayerModel(
        name="customers", sql_table="customers", data_source="db",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="name", sql="name", type=DataType.STRING),

            Column(name="avg_score", sql="score", type=DataType.NUMBER),
            Column(name="max_score", sql="score", type=DataType.NUMBER),
        ],
    ))

    return SlayerQueryEngine(storage=storage)


async def test_cross_model_measure_monthly(cross_model_env):
    """Cross-model measure: monthly order count + avg customer score from joined model."""
    engine = cross_model_env

    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="*:count"),
            ModelMeasure(formula="customers.avg_score:avg"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    assert response.row_count == 3
    # customers model has no join back to orders, so the time dimension is
    # unreachable from the re-rooted CTE → dropped → scalar AVG CROSS JOINed.
    # Global avg: (90 + 60 + 80) / 3 = 76.67
    global_avg = pytest.approx((90.0 + 60.0 + 80.0) / 3)
    assert response.data[0]["orders.customers.avg_score_avg"] == global_avg
    assert response.data[1]["orders.customers.avg_score_avg"] == global_avg
    assert response.data[2]["orders.customers.avg_score_avg"] == global_avg


async def test_cross_model_measure_no_join_raises(cross_model_env):
    """Referencing a model with no join should raise."""
    engine = cross_model_env

    query = SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="nonexistent.some_measure:sum")],
    )
    with pytest.raises(ValueError, match="has no join to"):
        await engine.execute(query)


async def test_cross_model_measure_with_target_join_filters(cross_model_env):
    """Cross-model measure CTE must include filters reachable from the target model.

    Based on Q9 benchmark: policy_amount.total_policy_amount:sum with filters on
    premium (has_premium='1', constant dim — INNER JOIN does the filtering) and
    agreement_party_role (party_role_code='PH'). Both are reachable from
    policy_amount's join graph. Without the re-rooted subquery fix, these filters
    would be stripped from the cross-model CTE, producing wrong aggregates.
    """
    engine = cross_model_env
    import os
    import tempfile
    tmp = tempfile.mkdtemp()
    db_path = f"{tmp}/test.db"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE policy (policy_identifier INTEGER PRIMARY KEY, policy_number TEXT)")
    conn.execute("CREATE TABLE policy_amount (policy_amount_identifier INTEGER PRIMARY KEY, policy_identifier INTEGER, policy_amount REAL)")
    conn.execute("CREATE TABLE premium (policy_amount_identifier INTEGER PRIMARY KEY)")
    conn.execute("CREATE TABLE agreement_party_role (agreement_identifier INTEGER, party_role_code TEXT)")

    # Policy 1: 2 amounts (100, 200), both have premium rows, PH role
    # Policy 2: 2 amounts (300, 400), only 300 has a premium row, PH role
    # Policy 3: 1 amount (500), has premium, but AG role (not PH)
    conn.executemany("INSERT INTO policy VALUES (?, ?)", [
        (1, "POL-001"), (2, "POL-002"), (3, "POL-003"),
    ])
    conn.executemany("INSERT INTO policy_amount VALUES (?, ?, ?)", [
        (10, 1, 100.0), (11, 1, 200.0),
        (20, 2, 300.0), (21, 2, 400.0),
        (30, 3, 500.0),
    ])
    # Premium rows: existence = is a premium. Amount 21 has no premium row.
    conn.executemany("INSERT INTO premium VALUES (?)", [
        (10,), (11,), (20,), (30,),
    ])
    conn.executemany("INSERT INTO agreement_party_role VALUES (?, ?)", [
        (1, "PH"), (2, "PH"), (3, "AG"),
    ])
    conn.commit()
    conn.close()

    storage_dir = f"{tmp}/storage"
    os.makedirs(storage_dir)
    storage = YAMLStorage(base_dir=storage_dir)
    await storage.save_datasource(DatasourceConfig(name="db", type="sqlite", database=db_path))

    await storage.save_model(SlayerModel(
        name="policy", sql_table="policy", data_source="db",
        columns=[
            Column(name="policy_identifier", type=DataType.NUMBER, primary_key=True),
            Column(name="policy_number", type=DataType.STRING),

        ],
        joins=[
            ModelJoin(target_model="policy_amount", join_pairs=[["policy_identifier", "policy_identifier"]], join_type="inner"),
            ModelJoin(target_model="agreement_party_role", join_pairs=[["policy_identifier", "agreement_identifier"]], join_type="inner"),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="policy_amount", sql_table="policy_amount", data_source="db",
        columns=[
            Column(name="policy_amount_identifier", type=DataType.NUMBER, primary_key=True),
Column(name="total_policy_amount", sql="policy_amount", type=DataType.NUMBER)
        ],
        joins=[
            ModelJoin(target_model="policy", join_pairs=[["policy_identifier", "policy_identifier"]], join_type="inner"),
            ModelJoin(target_model="premium", join_pairs=[["policy_amount_identifier", "policy_amount_identifier"]], join_type="inner"),
            ModelJoin(target_model="agreement_party_role", join_pairs=[["policy_identifier", "agreement_identifier"]], join_type="inner"),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="premium", sql_table="premium", data_source="db",
        columns=[
            Column(name="policy_amount_identifier", type=DataType.NUMBER, primary_key=True),
            Column(name="has_premium", sql="1", type=DataType.STRING),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="agreement_party_role", sql_table="agreement_party_role", data_source="db",
        columns=[
            Column(name="agreement_identifier", type=DataType.NUMBER, primary_key=True),
            Column(name="party_role_code", type=DataType.STRING),
        ],
    ))

    engine = SlayerQueryEngine(storage=storage)

    # Q9-style query: cross-model measure with filters on target's join graph
    query = SlayerQuery(
        source_model="policy",
        measures=[ModelMeasure(formula="policy_amount.total_policy_amount:sum")],
        dimensions=[ColumnRef(name="policy_number")],
        filters=[
            "agreement_party_role.party_role_code = 'PH'",
            "policy_amount.premium.has_premium = 1",
        ],
    )
    response = await engine.execute(query)

    # With both filters applied in the cross-model CTE:
    # POL-001: amounts 100+200=300 (both have premium rows, PH)
    # POL-002: only amount 300 has a premium row (INNER JOIN excludes 400)
    # POL-003: excluded (AG role, not PH)
    assert response.row_count == 2
    data_by_policy = {row["policy.policy_number"]: row for row in response.data}
    assert "POL-001" in data_by_policy
    assert "POL-002" in data_by_policy
    assert "POL-003" not in data_by_policy
    assert data_by_policy["POL-001"]["policy.policy_amount.total_policy_amount_sum"] == pytest.approx(300.0)
    assert data_by_policy["POL-002"]["policy.policy_amount.total_policy_amount_sum"] == pytest.approx(300.0)


async def test_transform_on_cross_model(cross_model_env):
    """Transforms on cross-model measures work (applied after the cross-model join)."""
    engine = cross_model_env

    # cumsum of avg customer score per month
    query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="customers.avg_score:avg"),
            ModelMeasure(formula="cumsum(customers.avg_score:avg)", name="running"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = await engine.execute(query)

    # customers has no join back to orders → time dim dropped → scalar avg
    # CROSS JOINed. cumsum of a constant = constant * row_number.
    global_avg = (90.0 + 60.0 + 80.0) / 3
    assert response.data[0]["orders.running"] == pytest.approx(global_avg)
    assert response.data[1]["orders.running"] == pytest.approx(global_avg * 2)
    assert response.data[2]["orders.running"] == pytest.approx(global_avg * 3)


# ---------------------------------------------------------------------------
# Query as model (multistage queries)
# ---------------------------------------------------------------------------

async def test_query_as_model_count(integration_env):
    """A named query can be used as the model for another query via list."""
    engine = integration_env

    # Inner: monthly order counts (3 months), named for reference
    inner = SlayerQuery(
        name="monthly",
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="total_amount:sum")],
    )

    # Outer: count how many months exist (references "monthly" by name)
    outer = SlayerQuery(source_model="monthly", measures=[ModelMeasure(formula="*:count")])
    response = await engine.execute(query=[inner, outer])

    assert response.row_count == 1
    assert response.data[0]["monthly._count"] == 3


async def test_query_as_model_aggregate(integration_env):
    """Outer query can aggregate over inner query's computed values."""
    engine = integration_env

    inner = SlayerQuery(
        name="monthly",
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="total_amount:sum")],
    )

    outer = SlayerQuery(source_model="monthly", measures=[ModelMeasure(formula="total_amount_sum:sum")])
    response = await engine.execute(query=[inner, outer])

    assert response.row_count == 1
    assert response.data[0]["monthly.total_amount_sum_sum"] == pytest.approx(750.0)


async def test_create_model_from_query(integration_env):
    """A query can be saved as a permanent model and then queried by name."""
    engine = integration_env

    # Create a monthly summary model from a query
    source_query = SlayerQuery(
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="*:count"), ModelMeasure(formula="total_amount:sum")],
    )
    saved = await engine.create_model_from_query(
        query=source_query, name="monthly_summary",
    )

    # Verify model structure
    dim_names = [d.name for d in saved.columns]
    assert "created_at" in dim_names
    assert "_count" in dim_names
    assert "total_amount_sum" in dim_names
    assert saved.source_queries is not None

    # Query the saved model by name
    result = await engine.execute(query=SlayerQuery(
        source_model="monthly_summary", measures=[ModelMeasure(formula="*:count")],
    ))
    assert result.data[0]["monthly_summary._count"] == 3

    # Re-aggregate over saved model
    result2 = await engine.execute(query=SlayerQuery(
        source_model="monthly_summary", measures=[ModelMeasure(formula="total_amount_sum:sum")],
    ))
    assert result2.data[0]["monthly_summary.total_amount_sum_sum"] == pytest.approx(750.0)


async def test_query_list_with_joins(cross_model_env):
    """A query list where the main query joins to a named sub-query."""
    engine = cross_model_env

    # Sub-query: average customer score per customer
    sub = SlayerQuery(
        name="customer_scores",
        source_model="customers",
        dimensions=[ColumnRef(name="id")],
        measures=[ModelMeasure(formula="avg_score:avg")],
    )

    # Main query: monthly orders joined to customer_scores
    # In the virtual model, inner measures become dimensions with auto-generated
    # SUM/AVG measures. Use avg_score_avg to re-average the inner avg_score.
    from slayer.core.query import ModelExtension
    main = SlayerQuery(
        source_model=ModelExtension(
            source_name="orders",
            joins=[{"target_model": "customer_scores", "join_pairs": [["customer_id", "id"]]}],
        ),
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        measures=[
            ModelMeasure(formula="*:count"),
            ModelMeasure(formula="customer_scores.avg_score_avg:avg"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )

    response = await engine.execute(query=[sub, main])

    assert response.row_count == 3
    # customer_scores has no join back to orders → time dim dropped → scalar avg
    global_avg = pytest.approx((90.0 + 60.0 + 80.0) / 3)
    assert response.data[0]["orders.customer_scores.avg_score_avg_avg"] == global_avg
    assert response.data[1]["orders.customer_scores.avg_score_avg_avg"] == global_avg
    assert response.data[2]["orders.customer_scores.avg_score_avg_avg"] == global_avg


# ---------------------------------------------------------------------------
# Expanded dimensions (SQL expressions)
# ---------------------------------------------------------------------------

async def test_sql_dimension_via_model_extension(integration_env):
    """SQL expression dimension via ModelExtension: CASE to bucket amounts."""
    engine = integration_env

    query = SlayerQuery(
        source_model=ModelExtension(
            source_name="orders",
            columns=[{"name": "tier", "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"}],
        ),
        dimensions=[ColumnRef(name="tier")],
        measures=[ModelMeasure(formula="*:count")],
    )
    response = await engine.execute(query)

    by_tier = {r["orders.tier"]: r["orders._count"] for r in response.data}
    assert by_tier["high"] == 2
    assert by_tier["low"] == 4


async def test_sql_dimension_with_regular(integration_env):
    """SQL dimension via ModelExtension mixed with regular dimension."""
    engine = integration_env

    query = SlayerQuery(
        source_model=ModelExtension(
            source_name="orders",
            columns=[{"name": "tier", "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"}],
        ),
        dimensions=[ColumnRef(name="status"), ColumnRef(name="tier")],
        measures=[ModelMeasure(formula="*:count")],
    )
    response = await engine.execute(query)

    # completed has 3 orders: 100(low), 200(high), 300(high)
    data = {(r["orders.status"], r["orders.tier"]): r["orders._count"] for r in response.data}
    assert data[("completed", "high")] == 2
    assert data[("completed", "low")] == 1


async def test_formula_dimension_via_query_list(integration_env):
    """Formula dimensions on aggregates work via multistage query list."""
    engine = integration_env

    # Inner: compute monthly totals
    inner = SlayerQuery(
        name="monthly",
        source_model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        measures=[ModelMeasure(formula="total_amount:sum")],
    )

    # Outer: group by amount tier via ModelExtension on the inner query's result
    outer = SlayerQuery(
        source_model=ModelExtension(
            source_name="monthly",
            columns=[{"name": "amount_tier",
                         "sql": "CASE WHEN total_amount_sum > 200 THEN 'high' ELSE 'low' END"}],
        ),
        dimensions=[ColumnRef(name="amount_tier")],
        measures=[ModelMeasure(formula="*:count")],
    )

    response = await engine.execute(query=[inner, outer])

    # Jan(300)=high, Feb(125)=low, Mar(325)=high
    by_tier = {r["monthly.amount_tier"]: r["monthly._count"] for r in response.data}
    assert by_tier["high"] == 2
    assert by_tier["low"] == 1


async def test_multistage_renamed_measure_returns_non_null(integration_env):
    """DEV-1335 canary. A 2-stage query-backed model where the inner stage
    renames its aggregated measure via ``name=`` must return non-NULL values
    for the renamed column when executed end-to-end.

    Before the fix: the inner-stage wrap silently emits ``amount_sum``
    (canonical) instead of the user-supplied ``rev``, so the outer stage's
    ``rev:sum`` either errors at SQL-gen time or surfaces NULLs in the result
    column. After the fix: the inner stage exposes ``rev`` and the outer
    stage's sum equals the precomputed total.
    """
    engine = integration_env

    saved = SlayerModel(
        name="renamed_metric",
        data_source="test_sqlite",
        source_queries=[
            SlayerQuery(
                name="raw",
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="amount:sum", name="rev")],
            ),
            SlayerQuery(
                source_model="raw",
                measures=[ModelMeasure(formula="rev:sum")],
            ),
        ],
    )
    await engine.save_model(saved)

    response = await engine.execute("renamed_metric")
    assert response.row_count == 1, (
        f"expected single row from outer rev:sum, got {response.row_count}: {response.data}"
    )
    # Outer stage emits ``raw.rev_sum`` (canonical for stage 2).
    row = response.data[0]
    assert "raw.rev_sum" in row, (
        f"expected column 'raw.rev_sum' in result row, got keys: {list(row.keys())}"
    )
    value = row["raw.rev_sum"]
    assert value is not None, (
        f"renamed measure must surface a non-NULL value, got NULL in row: {row}"
    )
    # 100 + 200 + 50 + 75 + 300 + 25 = 750  # NOSONAR(S125) — arithmetic explanation, not commented-out code
    assert value == pytest.approx(750.0), (
        f"sum of inner-stage 'rev' across all status buckets must equal 750, got {value}"
    )


async def test_circular_query_reference_raises(integration_env):
    """Circular references between named queries should error clearly."""
    engine = integration_env

    q1 = SlayerQuery(name="a", source_model="b", measures=[ModelMeasure(formula="*:count")])
    q2 = SlayerQuery(name="b", source_model="a", measures=[ModelMeasure(formula="*:count")])
    main = SlayerQuery(source_model="a", measures=[ModelMeasure(formula="*:count")])
    with pytest.raises(ValueError, match="Circular reference"):
        await engine.execute(query=[q1, q2, main])


async def test_circular_join_graph_raises(tmp_path):
    """Circular joins between stored models should error when walking the join graph."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, b_id INTEGER)")
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)")
    conn.executemany("INSERT INTO a VALUES (?, ?)", [(1, 1)])
    conn.executemany("INSERT INTO b VALUES (?, ?)", [(1, 1)])
    conn.commit()
    conn.close()

    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    await storage.save_datasource(DatasourceConfig(name="db", type="sqlite", database=str(db_path)))

    # Circular joins: a → b → a
    await storage.save_model(SlayerModel(
        name="a", sql_table="a", data_source="db",
        columns=[Column(name="id", sql="id", type=DataType.NUMBER),
                    Column(name="b_id", sql="b_id", type=DataType.NUMBER),

        ],
        joins=[ModelJoin(target_model="b", join_pairs=[["b_id", "id"]])],
    ))
    await storage.save_model(SlayerModel(
        name="b", sql_table="b", data_source="db",
        columns=[Column(name="id", sql="id", type=DataType.NUMBER),
                    Column(name="a_id", sql="a_id", type=DataType.NUMBER),
                    Column(name="unique_b_field", sql="id", type=DataType.NUMBER),

        ],
        joins=[ModelJoin(target_model="a", join_pairs=[["a_id", "id"]])],
    ))

    engine = SlayerQueryEngine(storage=storage)

    # Trying to resolve b.a.unique_b_field — walks a→b→a which is a cycle.
    # "unique_b_field" only exists on model b, so __ translation can't short-circuit.
    query = SlayerQuery(
        source_model="a",
        dimensions=[ColumnRef(name="b.a.unique_b_field")],
        measures=[ModelMeasure(formula="*:count")],
    )
    with pytest.raises(ValueError, match="Circular join"):
        await engine.execute(query)


# ---------------------------------------------------------------------------
# Model filters on joined columns
# ---------------------------------------------------------------------------

async def test_model_filter_on_joined_column(tmp_path):
    """Model-level filter on a joined column applies WHERE correctly."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region TEXT)")
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)")
    conn.executemany("INSERT INTO customers VALUES (?, ?, ?)", [
        (1, "Alice", "US"), (2, "Bob", "EU"), (3, "Charlie", "US")])
    conn.executemany("INSERT INTO orders VALUES (?, ?, ?)", [
        (1, 1, 100), (2, 1, 200), (3, 2, 50), (4, 3, 300)])
    conn.commit()
    conn.close()

    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    await storage.save_datasource(DatasourceConfig(name="db", type="sqlite", database=str(db_path)))
    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="db",
        columns=[
            Column(name="customer_id", sql="customer_id", type=DataType.NUMBER),
Column(name="total", sql="amount", type=DataType.NUMBER)
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        filters=["customers.region == 'US'"],
    ))
    await storage.save_model(SlayerModel(
        name="customers", sql_table="customers", data_source="db",
        columns=[Column(name="id", sql="id", type=DataType.NUMBER),
                    Column(name="name", sql="name", type=DataType.STRING),
                    Column(name="region", sql="region", type=DataType.STRING),

        ],
    ))

    engine = SlayerQueryEngine(storage=storage)

    # Model filter "customers.region == 'US'" should exclude Bob (EU)
    result = await engine.execute(SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="customers.name")],
        measures=[ModelMeasure(formula="*:count")],
    ))

    names = {r["orders.customers.name"] for r in result.data}
    assert "Alice" in names
    assert "Charlie" in names
    assert "Bob" not in names  # Filtered by model filter

    # JOIN must be included even though the filter (not the dimension) needs it
    assert "LEFT JOIN" in result.sql
    assert "customers" in result.sql


# ---------------------------------------------------------------------------
# Diamond joins — same table reached via two different paths
# ---------------------------------------------------------------------------

@pytest.fixture
async def diamond_env(tmp_path):
    """Schema: shipments → customers → regions, shipments → warehouses → regions.

    Two paths to regions, requiring path-based aliases to disambiguate.
    """
    db_path = tmp_path / "diamond.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, region_id INTEGER REFERENCES regions(id))")
    conn.execute("CREATE TABLE warehouses (id INTEGER PRIMARY KEY, name TEXT, region_id INTEGER REFERENCES regions(id))")
    conn.execute("""
        CREATE TABLE shipments (
            id INTEGER PRIMARY KEY,
            amount REAL,
            customer_id INTEGER REFERENCES customers(id),
            warehouse_id INTEGER REFERENCES warehouses(id)
        )
    """)
    conn.executemany("INSERT INTO regions VALUES (?, ?)", [
        (1, "US"), (2, "EU"), (3, "Asia"),
    ])
    conn.executemany("INSERT INTO customers VALUES (?, ?, ?)", [
        (1, "Alice", 1), (2, "Bob", 2),
    ])
    conn.executemany("INSERT INTO warehouses VALUES (?, ?, ?)", [
        (1, "WH-East", 1), (2, "WH-West", 3),
    ])
    conn.executemany("INSERT INTO shipments VALUES (?, ?, ?, ?)", [
        (1, 100, 1, 1),  # Alice(US) from WH-East(US)
        (2, 200, 1, 2),  # Alice(US) from WH-West(Asia)
        (3, 50, 2, 1),   # Bob(EU) from WH-East(US)
        (4, 150, 2, 2),  # Bob(EU) from WH-West(Asia)
    ])
    conn.commit()
    conn.close()

    from slayer.engine.ingestion import ingest_datasource

    storage = YAMLStorage(base_dir=str(tmp_path / "slayer_data"))
    ds = DatasourceConfig(name="diamond_db", type="sqlite", database=str(db_path))
    await storage.save_datasource(ds)
    models = ingest_datasource(datasource=ds)
    for m in models:
        await storage.save_model(m)

    engine = SlayerQueryEngine(storage=storage)
    return engine, storage


async def test_diamond_joins_both_paths(diamond_env):
    """Query both customer region and warehouse region in one query — must not collide."""
    engine, storage = diamond_env

    # Verify the ingested model has its own columns (not flattened joined dims)
    shipments = await storage.get_model("shipments")
    dim_names = {d.name for d in shipments.columns}
    assert "customer_id" in dim_names
    assert "warehouse_id" in dim_names
    # Joined dimensions are resolved via the join graph, not pre-flattened
    assert not any("." in name for name in dim_names)

    # Query both region paths simultaneously — resolved via join graph
    result = await engine.execute(query=SlayerQuery(
        source_model="shipments",
        dimensions=[
            ColumnRef(name="customers.regions.name"),
            ColumnRef(name="warehouses.regions.name"),
        ],
        measures=[ModelMeasure(formula="*:count")],
    ))

    # Should have 4 rows: (US, US), (US, Asia), (EU, US), (EU, Asia)
    rows = {
        (r["shipments.customers.regions.name"], r["shipments.warehouses.regions.name"]): r["shipments._count"]
        for r in result.data
    }
    assert len(rows) == 4
    assert rows[("US", "US")] == 1    # Alice from WH-East
    assert rows[("US", "Asia")] == 1  # Alice from WH-West
    assert rows[("EU", "US")] == 1    # Bob from WH-East
    assert rows[("EU", "Asia")] == 1  # Bob from WH-West

    # SQL must have two different aliases for regions
    assert "customers__regions" in result.sql
    assert "warehouses__regions" in result.sql


async def test_query_filter_on_joined_dimension(diamond_env):
    """Query-level filter on a joined dimension resolves through the model."""
    engine, _ = diamond_env

    result = await engine.execute(query=SlayerQuery(
        source_model="shipments",
        measures=[ModelMeasure(formula="*:count")],
        filters=["customers.regions.name == 'US'"],
    ))

    assert result.data[0]["shipments._count"] == 2  # Alice's 2 shipments
    # Filter must use the path-based alias in SQL
    assert "customers__regions" in result.sql


async def test_diamond_joins_single_path(diamond_env):
    """Query only one path — should work without including the other."""
    engine, _ = diamond_env

    result = await engine.execute(query=SlayerQuery(
        source_model="shipments",
        dimensions=[ColumnRef(name="customers.regions.name")],
        measures=[ModelMeasure(formula="*:count")],
    ))

    by_region = {r["shipments.customers.regions.name"]: r["shipments._count"] for r in result.data}
    assert by_region["US"] == 2  # Alice: 2 shipments
    assert by_region["EU"] == 2  # Bob: 2 shipments


# ---------------------------------------------------------------------------
# Filtered measures
# ---------------------------------------------------------------------------


async def test_filtered_measure_sum(integration_env):
    """Measure with filter produces CASE WHEN — only matching rows aggregated."""
    engine = integration_env
    storage = engine.storage

    # Add a filtered measure: only sum completed orders' amounts
    orders = await storage.get_model("orders")
    orders.columns.append(
        Column(name="completed_revenue", sql="amount", filter="status = 'completed'", type=DataType.NUMBER)
    )
    await storage.save_model(orders)

    result = await engine.execute(query=SlayerQuery(
        source_model="orders",
        measures=[
            ModelMeasure(formula="total_amount:sum"),
            ModelMeasure(formula="completed_revenue:sum"),
        ],
    ))
    assert result.row_count == 1
    row = result.data[0]
    total = row["orders.total_amount_sum"]
    completed = row["orders.completed_revenue_sum"]
    assert total == pytest.approx(750.0)
    assert completed == pytest.approx(600.0)


async def test_filtered_measure_count(integration_env):
    """Filtered count measure counts only matching rows."""
    engine = integration_env
    storage = engine.storage

    orders = await storage.get_model("orders")
    orders.columns.append(
        Column(name="completed_count", sql="id", filter="status = 'completed'", type=DataType.NUMBER)
    )
    await storage.save_model(orders)

    result = await engine.execute(query=SlayerQuery(
        source_model="orders",
        measures=[
            ModelMeasure(formula="*:count"),
            ModelMeasure(formula="completed_count:count"),
        ],
    ))
    row = result.data[0]
    total = row["orders._count"]
    completed = row["orders.completed_count_count"]
    assert total == 6
    assert completed == 3


async def test_filtered_measure_with_dimensions(integration_env):
    """Filtered measure works with GROUP BY dimensions."""
    engine = integration_env
    storage = engine.storage

    orders = await storage.get_model("orders")
    orders.columns.append(
        Column(name="completed_revenue", sql="amount", filter="status = 'completed'", type=DataType.NUMBER)
    )
    await storage.save_model(orders)

    result = await engine.execute(query=SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="completed_revenue:sum")],
        dimensions=[ColumnRef(name="status")],
    ))
    # Completed status row should have a value; others should be NULL
    for row in result.data:
        if row["orders.status"] == "completed":
            assert row["orders.completed_revenue_sum"] is not None
            assert row["orders.completed_revenue_sum"] > 0
        else:
            # Non-completed rows: the CASE WHEN produces NULL, SUM of NULLs is NULL
            assert row["orders.completed_revenue_sum"] is None


async def test_filtered_last_picks_correct_row(integration_env):
    """Filtered last measure picks the latest row that matches the filter,
    not the globally latest row.

    Fixture: orders (1..6), Order 6 (pending, Mar-20) is globally latest,
    Order 5 (completed, 300.0, Mar-5) is the latest completed.
    """
    engine = integration_env
    storage = engine.storage

    orders = await storage.get_model("orders")
    orders.columns.append(
        Column(name="completed_latest", sql="amount", filter="status = 'completed'", type=DataType.NUMBER)
    )
    await storage.save_model(orders)

    # Query with monthly granularity so we get per-month last values
    result = await engine.execute(query=SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            ),
        ],
        measures=[
            ModelMeasure(formula="completed_latest:last"),
            ModelMeasure(formula="latest_amount:last"),
        ],
    ))
    rows_by_month = {row["orders.created_at"]: row for row in result.data}
    # March: globally latest is Order 6 (pending, 25.0), but the latest
    # completed is Order 5 (completed, 300.0). The filter must participate
    # in ranking so the correct row is picked.
    mar = rows_by_month["2025-03-01"]
    assert mar["orders.completed_latest_last"] == pytest.approx(300.0)
    assert mar["orders.latest_amount_last"] == pytest.approx(25.0)  # unfiltered picks Order 6

    # January: latest is Order 2 (completed, 200.0) — passes filter
    jan = rows_by_month["2025-01-01"]
    assert jan["orders.completed_latest_last"] == pytest.approx(200.0)

    # February: no completed orders — should be NULL
    feb = rows_by_month["2025-02-01"]
    assert feb["orders.completed_latest_last"] is None


async def test_time_dimension_label_fallback(integration_env):
    """Time dimension inherits label from model dimension definition."""
    engine = integration_env
    storage = engine.storage

    orders = await storage.get_model("orders")
    for d in orders.columns:
        if d.name == "created_at":
            d.label = "Order Date"
    await storage.save_model(orders)

    # Query with time dimension but no explicit label on TimeDimension
    result = await engine.execute(query=SlayerQuery(
        source_model="orders",
        time_dimensions=[
            TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            ),
        ],
        measures=[ModelMeasure(formula="total_amount:sum")],
    ))
    # The model-level label should propagate through
    td_meta = result.attributes.dimensions.get("orders.created_at")
    assert td_meta is not None
    assert td_meta.label == "Order Date"


async def test_label_propagation_enrichment(integration_env):
    """Model-level labels propagate through enrichment to query results."""
    engine = integration_env
    storage = engine.storage

    orders = await storage.get_model("orders")
    # Add labels to a dimension and measure
    for d in orders.columns:
        if d.name == "status":
            d.label = "Order Status"
    orders.columns.append(
        Column(name="labeled_rev", sql="amount", label="Total Revenue", type=DataType.NUMBER)
    )
    await storage.save_model(orders)

    result = await engine.execute(query=SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="labeled_rev:sum")],
        dimensions=[ColumnRef(name="status")],
    ))
    # Labels should appear in result meta
    status_meta = result.attributes.dimensions.get("orders.status")
    assert status_meta is not None
    assert status_meta.label == "Order Status"
    rev_meta = result.attributes.measures.get("orders.labeled_rev_sum")
    assert rev_meta is not None
    assert rev_meta.label == "Total Revenue"


# ---------------------------------------------------------------------------
# Median / percentile via SQLite Python UDFs
#
# SQLite has no native MEDIAN/PERCENTILE_CONT. SLayer registers Python
# aggregates on each new SQLite connection (slayer/sql/sqlite_udfs.py); these
# tests exercise them end-to-end through the engine.
# ---------------------------------------------------------------------------


async def test_median_sqlite(integration_env):
    """Median of order amounts: [25, 50, 75, 100, 200, 300] -> 87.5."""
    engine = integration_env
    response = await engine.execute(SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="total_amount:median")],
    ))
    assert response.row_count == 1
    assert response.data[0]["orders.total_amount_median"] == pytest.approx(87.5)


async def test_percentile_sqlite_quartiles(integration_env):
    """P25 and P75 of [25, 50, 75, 100, 200, 300] with linear interpolation."""
    engine = integration_env
    response = await engine.execute(SlayerQuery(
        source_model="orders",
        measures=[
            ModelMeasure(formula="total_amount:percentile(p=0.25)"),
            ModelMeasure(formula="total_amount:percentile(p=0.75)"),
        ],
    ))
    assert response.row_count == 1
    row = response.data[0]
    assert row["orders.total_amount_percentile_p_0_25"] == pytest.approx(56.25)
    assert row["orders.total_amount_percentile_p_0_75"] == pytest.approx(175.0)


async def test_median_grouped_sqlite(integration_env):
    """Median per status — confirms UDFs reset state between groups.

    completed: [100, 200, 300] -> 200
    pending:   [25, 50]        -> 37.5
    cancelled: [75]            -> 75
    """
    engine = integration_env
    response = await engine.execute(SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="total_amount:median")],
        dimensions=[ColumnRef(name="status")],
    ))
    by_status = {
        row["orders.status"]: row["orders.total_amount_median"]
        for row in response.data
    }
    assert by_status["completed"] == pytest.approx(200)
    assert by_status["pending"] == pytest.approx(37.5)
    assert by_status["cancelled"] == pytest.approx(75)


async def test_median_empty_result_sqlite(integration_env):
    """Median on a filter that matches no rows yields NULL."""
    engine = integration_env
    response = await engine.execute(SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="total_amount:median")],
        filters=["status == 'nonexistent'"],
    ))
    assert response.row_count == 1
    assert response.data[0]["orders.total_amount_median"] is None


async def test_sqlite_udf_pool_reuse(integration_env):
    """Confirms the connect event re-registers UDFs on every new pooled
    connection (not just the first). We dispose the cached SA engine between
    the two executes so the second one opens a brand-new physical DBAPI
    connection, which forces the connect listener to fire again.
    """
    engine = integration_env
    q = SlayerQuery(source_model="orders", measures=[ModelMeasure(formula="total_amount:median")])
    r1 = await engine.execute(q)
    for sa_engine in _sync_engines.values():
        sa_engine.dispose()
    r2 = await engine.execute(q)
    assert r1.data[0]["orders.total_amount_median"] == pytest.approx(87.5)
    assert r2.data[0]["orders.total_amount_median"] == pytest.approx(87.5)


# ---------------------------------------------------------------------------
# DEV-1317 — math/stat UDFs end-to-end on SQLite
# ---------------------------------------------------------------------------


@pytest.fixture
async def stat_env(tmp_path):
    """Independent fixture with a richer numeric dataset for stat UDFs."""
    import statistics

    db_path = tmp_path / "stat.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE samples (
            id INTEGER PRIMARY KEY,
            x REAL NOT NULL,
            y REAL NOT NULL,
            bucket TEXT NOT NULL
        )
        """
    )
    rows = [
        # bucket "a": linearly correlated x,y (corr ≈ 1)
        (1, 1.0, 2.0, "a"),
        (2, 2.0, 4.0, "a"),
        (3, 3.0, 6.0, "a"),
        (4, 4.0, 8.0, "a"),
        (5, 5.0, 10.0, "a"),
        # bucket "b": noisy positive correlation
        (6, 1.0, 1.9, "b"),
        (7, 2.0, 4.2, "b"),
        (8, 3.0, 5.7, "b"),
        (9, 4.0, 8.1, "b"),
        (10, 5.0, 10.3, "b"),
    ]
    cur.executemany("INSERT INTO samples VALUES (?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()

    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    await storage.save_datasource(
        DatasourceConfig(name="stat_sqlite", type="sqlite", database=str(db_path))
    )
    await storage.save_model(
        SlayerModel(
            name="samples",
            sql_table="samples",
            data_source="stat_sqlite",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="x", sql="x", type=DataType.NUMBER),
                Column(name="y", sql="y", type=DataType.NUMBER),
                Column(name="bucket", sql="bucket", type=DataType.STRING),
                # Column.sql exercising scalar math UDFs
                Column(name="ln_x", sql="ln(x)", type=DataType.NUMBER),
                Column(name="sqrt_x", sql="sqrt(x)", type=DataType.NUMBER),
                Column(name="x_squared", sql="pow(x, 2)", type=DataType.NUMBER),
            ],
        )
    )
    return SlayerQueryEngine(storage=storage), statistics


async def test_stddev_samp_sqlite(stat_env):
    """latency:stddev_samp end-to-end on SQLite — value matches Python."""
    engine, statistics = stat_env
    response = await engine.execute(
        SlayerQuery(
            source_model="samples",
            measures=[ModelMeasure(formula="x:stddev_samp")],
        )
    )
    assert response.data[0]["samples.x_stddev_samp"] == pytest.approx(
        statistics.stdev([1, 2, 3, 4, 5, 1, 2, 3, 4, 5]), rel=1e-9
    )


async def test_stddev_pop_sqlite(stat_env):
    engine, statistics = stat_env
    response = await engine.execute(
        SlayerQuery(
            source_model="samples",
            measures=[ModelMeasure(formula="x:stddev_pop")],
        )
    )
    assert response.data[0]["samples.x_stddev_pop"] == pytest.approx(
        statistics.pstdev([1, 2, 3, 4, 5, 1, 2, 3, 4, 5]), rel=1e-9
    )


async def test_var_samp_sqlite(stat_env):
    engine, statistics = stat_env
    response = await engine.execute(
        SlayerQuery(
            source_model="samples",
            measures=[ModelMeasure(formula="x:var_samp")],
        )
    )
    assert response.data[0]["samples.x_var_samp"] == pytest.approx(
        statistics.variance([1, 2, 3, 4, 5, 1, 2, 3, 4, 5]), rel=1e-9
    )


async def test_var_pop_sqlite(stat_env):
    engine, statistics = stat_env
    response = await engine.execute(
        SlayerQuery(
            source_model="samples",
            measures=[ModelMeasure(formula="x:var_pop")],
        )
    )
    assert response.data[0]["samples.x_var_pop"] == pytest.approx(
        statistics.pvariance([1, 2, 3, 4, 5, 1, 2, 3, 4, 5]), rel=1e-9
    )


async def test_corr_sqlite(stat_env):
    """price:corr(other=quantity) — perfect-positive bucket "a" yields 1.0."""
    engine, _ = stat_env
    response = await engine.execute(
        SlayerQuery(
            source_model="samples",
            measures=[ModelMeasure(formula="x:corr(other=y)")],
            dimensions=[ColumnRef(name="bucket")],
        )
    )
    by_bucket = {
        row["samples.bucket"]: row["samples.x_corr_other_y"]
        for row in response.data
    }
    assert by_bucket["a"] == pytest.approx(1.0, abs=1e-9)
    # bucket "b" should be very close to 1 but not exactly.
    assert 0.99 < by_bucket["b"] < 1.0


async def test_covar_samp_sqlite(stat_env):
    """price:covar_samp(other=quantity) end-to-end on SQLite."""
    engine, _ = stat_env
    response = await engine.execute(
        SlayerQuery(
            source_model="samples",
            measures=[ModelMeasure(formula="x:covar_samp(other=y)")],
            dimensions=[ColumnRef(name="bucket")],
        )
    )
    by_bucket = {
        row["samples.bucket"]: row["samples.x_covar_samp_other_y"]
        for row in response.data
    }
    # Bucket "a" is exactly y = 2x; sample covariance of [1..5] with [2..10]
    # is (5*1 + 4*2 + 3*3 + 2*4 + 1*5) ... compute via Python directly.
    xs_a = [1, 2, 3, 4, 5]
    ys_a = [2, 4, 6, 8, 10]
    mx, my = sum(xs_a) / 5, sum(ys_a) / 5
    expected_a = sum((x - mx) * (y - my) for x, y in zip(xs_a, ys_a)) / 4
    assert by_bucket["a"] == pytest.approx(expected_a, rel=1e-9)


async def test_covar_pop_sqlite(stat_env):
    engine, _ = stat_env
    response = await engine.execute(
        SlayerQuery(
            source_model="samples",
            measures=[ModelMeasure(formula="x:covar_pop(other=y)")],
            dimensions=[ColumnRef(name="bucket")],
        )
    )
    by_bucket = {
        row["samples.bucket"]: row["samples.x_covar_pop_other_y"]
        for row in response.data
    }
    xs_a = [1, 2, 3, 4, 5]
    ys_a = [2, 4, 6, 8, 10]
    mx, my = sum(xs_a) / 5, sum(ys_a) / 5
    expected_a = sum((x - mx) * (y - my) for x, y in zip(xs_a, ys_a)) / 5
    assert by_bucket["a"] == pytest.approx(expected_a, rel=1e-9)


async def test_stat_aggs_per_group_sqlite(stat_env):
    """stddev_samp per bucket — confirms UDFs reset state between groups."""
    engine, statistics = stat_env
    response = await engine.execute(
        SlayerQuery(
            source_model="samples",
            measures=[ModelMeasure(formula="x:stddev_samp")],
            dimensions=[ColumnRef(name="bucket")],
        )
    )
    by_bucket = {
        row["samples.bucket"]: row["samples.x_stddev_samp"]
        for row in response.data
    }
    assert by_bucket["a"] == pytest.approx(statistics.stdev([1, 2, 3, 4, 5]), rel=1e-9)
    assert by_bucket["b"] == pytest.approx(statistics.stdev([1, 2, 3, 4, 5]), rel=1e-9)


async def test_scalar_ln_in_column_sql(stat_env):
    """A Column.sql containing `ln(x)` must execute on SQLite (UDF lookup)
    and return the math.log of every row.
    """
    import math

    engine, _ = stat_env
    response = await engine.execute(
        SlayerQuery(
            source_model="samples",
            measures=[ModelMeasure(formula="ln_x:sum")],
        )
    )
    expected = sum(math.log(v) for v in [1, 2, 3, 4, 5, 1, 2, 3, 4, 5])
    assert response.data[0]["samples.ln_x_sum"] == pytest.approx(expected, rel=1e-9)


async def test_scalar_sqrt_in_column_sql(stat_env):
    import math

    engine, _ = stat_env
    response = await engine.execute(
        SlayerQuery(
            source_model="samples",
            measures=[ModelMeasure(formula="sqrt_x:sum")],
        )
    )
    expected = sum(math.sqrt(v) for v in [1, 2, 3, 4, 5, 1, 2, 3, 4, 5])
    assert response.data[0]["samples.sqrt_x_sum"] == pytest.approx(expected, rel=1e-9)


async def test_scalar_pow_in_column_sql(stat_env):
    """`pow(x, 2)` exercises the 2-arg power UDF."""
    engine, _ = stat_env
    response = await engine.execute(
        SlayerQuery(
            source_model="samples",
            measures=[ModelMeasure(formula="x_squared:sum")],
        )
    )
    expected = sum(v ** 2 for v in [1, 2, 3, 4, 5, 1, 2, 3, 4, 5])
    assert response.data[0]["samples.x_squared_sum"] == pytest.approx(expected, rel=1e-9)


async def test_n_one_bucket_returns_postgres_semantics(tmp_path):
    """With a single sample, stddev_samp/var_samp must be NULL and
    stddev_pop/var_pop must be 0 — matching Postgres."""
    db_path = tmp_path / "single.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE one (id INTEGER PRIMARY KEY, x REAL NOT NULL)")
    cur.execute("INSERT INTO one VALUES (1, 42.0)")
    conn.commit()
    conn.close()

    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    await storage.save_datasource(
        DatasourceConfig(name="one_sqlite", type="sqlite", database=str(db_path))
    )
    await storage.save_model(
        SlayerModel(
            name="one",
            sql_table="one",
            data_source="one_sqlite",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="x", sql="x", type=DataType.NUMBER),
            ],
        )
    )
    engine = SlayerQueryEngine(storage=storage)

    r_samp = await engine.execute(
        SlayerQuery(source_model="one", measures=[ModelMeasure(formula="x:stddev_samp")])
    )
    assert r_samp.data[0]["one.x_stddev_samp"] is None

    r_var_samp = await engine.execute(
        SlayerQuery(source_model="one", measures=[ModelMeasure(formula="x:var_samp")])
    )
    assert r_var_samp.data[0]["one.x_var_samp"] is None

    r_pop = await engine.execute(
        SlayerQuery(source_model="one", measures=[ModelMeasure(formula="x:stddev_pop")])
    )
    assert r_pop.data[0]["one.x_stddev_pop"] == 0

    r_var_pop = await engine.execute(
        SlayerQuery(source_model="one", measures=[ModelMeasure(formula="x:var_pop")])
    )
    assert r_var_pop.data[0]["one.x_var_pop"] == 0


async def test_json_extract_case_when_matches_in_sqlite(tmp_path):
    """DEV-1331 reproduction.

    A derived ``Column.sql`` that uses ``json_extract`` inside a CASE WHEN
    must return the expected aggregate. Pre-fix the SQLite emitter rewrites
    ``json_extract(...)`` to ``col -> '$.path'``, which returns the
    JSON-quoted form (``'"Owned"'``); CASE WHEN against the bare-string
    ``'owned'`` therefore never matches and the sum is 0.
    """
    db_path = tmp_path / "households.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE households (id INTEGER PRIMARY KEY, socioeconomic TEXT NOT NULL)"
    )
    cur.executemany(
        "INSERT INTO households VALUES (?, ?)",
        [
            (1, '{"Tenure_Type": "Owned"}'),
            (2, '{"Tenure_Type": "Rented"}'),
            (3, '{"Tenure_Type": "Owned"}'),
        ],
    )
    conn.commit()
    conn.close()

    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    await storage.save_datasource(
        DatasourceConfig(name="hh_sqlite", type="sqlite", database=str(db_path))
    )
    await storage.save_model(
        SlayerModel(
            name="households",
            sql_table="households",
            data_source="hh_sqlite",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="socioeconomic", sql="socioeconomic", type=DataType.STRING),
                Column(
                    name="is_owner",
                    sql=(
                        "CASE LOWER(TRIM(json_extract(socioeconomic, '$.Tenure_Type'))) "
                        "WHEN 'owned' THEN 1 ELSE 0 END"
                    ),
                    type=DataType.NUMBER,
                ),
            ],
        )
    )
    engine = SlayerQueryEngine(storage=storage)

    response = await engine.execute(
        SlayerQuery(
            source_model="households",
            measures=[ModelMeasure(formula="is_owner:sum")],
        )
    )
    assert response.data[0]["households.is_owner_sum"] == 2


# ---------------------------------------------------------------------------
# DEV-1333: cross-model and local derived ``Column.sql`` chaining (SQLite)
# ---------------------------------------------------------------------------


@pytest.fixture
async def derived_chain_env(tmp_path):
    """Two-table A→B fixture: B has a derived column referenced by A's
    derived columns. Used to verify recursive expansion at execution time.
    """
    db_path = tmp_path / "derived_chain.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE B (id INTEGER PRIMARY KEY, foo_raw REAL)")
    cur.execute(
        "CREATE TABLE A (id INTEGER PRIMARY KEY, bar REAL, b_id INTEGER, raw_a REAL)"
    )
    cur.executemany(
        "INSERT INTO B VALUES (?, ?)",
        [(1, 200.0), (2, 50.0)],
    )
    cur.executemany(
        "INSERT INTO A VALUES (?, ?, ?, ?)",
        [(10, 4.0, 1, 100.0), (11, 1.0, 2, 5.0)],
    )
    conn.commit()
    conn.close()

    storage_dir = tmp_path / "storage_derived"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=str(db_path))
    )
    await storage.save_model(
        SlayerModel(
            name="B",
            data_source="ds",
            sql_table="B",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="foo_raw", sql="foo_raw", type=DataType.NUMBER),
                Column(
                    name="foo_normalized",
                    sql="foo_raw / 100.0",
                    type=DataType.NUMBER,
                ),
            ],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="A",
            data_source="ds",
            sql_table="A",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="bar", sql="bar", type=DataType.NUMBER),
                Column(name="b_id", sql="b_id", type=DataType.NUMBER),
                Column(name="raw_a", sql="raw_a", type=DataType.NUMBER),
                Column(
                    name="ratio_using_base",
                    sql="A.bar / B.foo_raw",
                    type=DataType.NUMBER,
                ),
                Column(
                    name="ratio_using_derived",
                    sql="A.bar / B.foo_normalized",
                    type=DataType.NUMBER,
                ),
                # Local derived chain: c1 derived; c2 references c1.
                Column(name="c1", sql="raw_a + 1", type=DataType.NUMBER),
                Column(name="c2", sql="A.c1 * 2", type=DataType.NUMBER),
            ],
            joins=[ModelJoin(target_model="B", join_pairs=[["b_id", "id"]])],
        )
    )
    engine = SlayerQueryEngine(storage=storage)
    yield engine
    _sync_engines.clear()


async def test_integration_cross_model_derived_columnsql(derived_chain_env):
    """The original DEV-1333 repro must execute and produce the expected ratios."""
    engine = derived_chain_env
    response = await engine.execute(
        SlayerQuery(
            source_model="A",
            dimensions=[
                ColumnRef(name="id"),
                ColumnRef(name="ratio_using_derived"),
            ],
            order=[OrderItem(column=ColumnRef(name="id"), direction="asc")],
        )
    )
    assert response.row_count == 2
    # Row 1: bar=4.0, B.foo_normalized=2.0 → 4.0/2.0 = 2.0
    # Row 2: bar=1.0, B.foo_normalized=0.5 → 1.0/0.5 = 2.0
    assert response.data[0]["A.ratio_using_derived"] == pytest.approx(2.0)
    assert response.data[1]["A.ratio_using_derived"] == pytest.approx(2.0)


async def test_integration_local_derived_chain(derived_chain_env):
    engine = derived_chain_env
    response = await engine.execute(
        SlayerQuery(
            source_model="A",
            dimensions=[ColumnRef(name="id"), ColumnRef(name="c2")],
            order=[OrderItem(column=ColumnRef(name="id"), direction="asc")],
        )
    )
    # c2 = (raw_a + 1) * 2 → for raw_a=100 → 202; raw_a=5 → 12
    assert response.data[0]["A.c2"] == pytest.approx(202.0)
    assert response.data[1]["A.c2"] == pytest.approx(12.0)


# ---------------------------------------------------------------------------
# DEV-1334: filter-only references to derived columns whose sql crosses a
# join must auto-add the join (no need to also list the column in
# ``dimensions``).
# ---------------------------------------------------------------------------


@pytest.fixture
async def orders_customers_env(tmp_path):
    """orders → customers with a derived ``is_eu`` column on orders whose
    SQL references the joined customers table. Used to pin filter-only
    auto-join behavior end-to-end.
    """
    db_path = tmp_path / "orders_customers.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT)")
    cur.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)")
    cur.executemany(
        "INSERT INTO customers VALUES (?, ?)",
        [(1, "EU"), (2, "US"), (3, "EU"), (4, "APAC")],
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?, ?)",
        [(10, 1), (11, 2), (12, 1), (13, 3), (14, 4)],
    )
    conn.commit()
    conn.close()

    storage_dir = tmp_path / "storage_oc"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=str(db_path))
    )
    await storage.save_model(SlayerModel(
        name="customers", data_source="ds", sql_table="customers",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="region", sql="region", type=DataType.STRING),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="orders", data_source="ds", sql_table="orders",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.NUMBER),
            Column(
                name="is_eu",
                sql="CASE WHEN customers.region = 'EU' THEN 1 ELSE 0 END",
                type=DataType.NUMBER,
            ),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    ))
    engine = SlayerQueryEngine(storage=storage)
    yield engine
    _sync_engines.clear()


async def test_filter_on_derived_column_with_cross_table_ref_executes(
    orders_customers_env,
):
    """Filter-only reference to a bare-named derived column whose SQL
    crosses a join must execute end-to-end (no ``no such column`` error)
    and return the right count.
    """
    engine = orders_customers_env
    response = await engine.execute(SlayerQuery(
        source_model="orders",
        measures=[ModelMeasure(formula="*:count", name="n")],
        filters=["is_eu = 1"],
    ))
    # Orders 10, 12, 13 hit EU customers → 3 rows.
    # Explicit ``name="n"`` overrides the canonical alias, so the result
    # key is ``orders.n`` (not ``orders._count``).
    assert response.row_count == 1
    assert response.data[0]["orders.n"] == 3
