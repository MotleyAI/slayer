"""Integration tests — end-to-end queries against a real SQLite database.

Run with: pytest tests/test_integration.py -m integration
"""

import sqlite3

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    DatasourceConfig,
    Dimension,
    Measure,
    SlayerModel,
)
from slayer.core.query import (
    ColumnRef,
    Field,
    OrderItem,
    SlayerQuery,
    TimeDimension,
)
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.yaml_storage import YAMLStorage

pytestmark = pytest.mark.integration


@pytest.fixture
def integration_env(tmp_path):
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
    storage.save_datasource(datasource)

    # -- Orders model --
    orders_model = SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="test_sqlite",
        default_time_dimension="created_at",
        dimensions=[
            Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="status", sql="status", type=DataType.STRING),
            Dimension(name="customer_id", sql="customer_id", type=DataType.NUMBER),
            Dimension(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Dimension(name="amount", sql="amount", type=DataType.NUMBER),
        ],
        measures=[
            Measure(name="count", type=DataType.COUNT),
            Measure(name="total_amount", sql="amount", type=DataType.SUM),
            Measure(name="latest_amount", sql="amount", type=DataType.LAST),
        ],
    )
    storage.save_model(orders_model)

    # -- Customers model --
    customers_model = SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="test_sqlite",
        dimensions=[
            Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="name", sql="name", type=DataType.STRING),
            Dimension(name="region", sql="region", type=DataType.STRING),
        ],
        measures=[
            Measure(name="count", type=DataType.COUNT),
        ],
    )
    storage.save_model(customers_model)

    engine = SlayerQueryEngine(storage=storage)
    return engine


def test_count_query(integration_env):
    """Count all orders."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        fields=[Field(formula="count")],
    )
    response = engine.execute(query)

    assert isinstance(response, SlayerResponse)
    assert response.row_count == 1
    assert response.data[0]["orders.count"] == 6


def test_sum_measure(integration_env):
    """Sum of order amounts."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        fields=[Field(formula="total_amount")],
    )
    response = engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders.total_amount"] == pytest.approx(750.0)


def test_dimensions_groupby(integration_env):
    """Count orders grouped by status."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        fields=[Field(formula="count")],
        dimensions=[ColumnRef(name="status")],
    )
    response = engine.execute(query)

    assert response.row_count == 3
    rows_by_status = {row["orders.status"]: row["orders.count"] for row in response.data}
    assert rows_by_status["completed"] == 3
    assert rows_by_status["pending"] == 2
    assert rows_by_status["cancelled"] == 1


def test_filter_equals(integration_env):
    """Filter orders where status = 'completed'."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        fields=[Field(formula="count")],
        filters=["status == 'completed'"],
    )
    response = engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders.count"] == 3


def test_filter_gt(integration_env):
    """Filter orders where amount > 50."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        fields=[Field(formula="count")],
        filters=["amount > 50"],
    )
    response = engine.execute(query)

    assert response.row_count == 1
    # Orders with amount > 50: 100, 200, 75, 300 = 4
    assert response.data[0]["orders.count"] == 4


def test_order_by(integration_env):
    """Order results by count descending."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        fields=[Field(formula="count")],
        dimensions=[ColumnRef(name="status")],
        order=[
            OrderItem(column=ColumnRef(name="count"), direction="desc"),
        ],
    )
    response = engine.execute(query)

    assert response.row_count == 3
    counts = [row["orders.count"] for row in response.data]
    assert counts == sorted(counts, reverse=True)
    # completed=3 is the highest count
    assert response.data[0]["orders.status"] == "completed"


def test_limit(integration_env):
    """Limit results to 2 rows."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        fields=[Field(formula="count")],
        dimensions=[ColumnRef(name="status")],
        order=[
            OrderItem(column=ColumnRef(name="count"), direction="desc"),
        ],
        limit=2,
    )
    response = engine.execute(query)

    assert response.row_count == 2


def test_multiple_measures(integration_env):
    """Count and sum in the same query."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        fields=[
            Field(formula="count"),
            Field(formula="total_amount"),
        ],
    )
    response = engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders.count"] == 6
    assert response.data[0]["orders.total_amount"] == pytest.approx(750.0)



def test_cumsum_change_identity(integration_env):
    """Mathematical identity: cumsum(change(x)) == x - x[0] for all rows after the first."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[
            Field(formula="count"),
            Field(formula="cumsum(change(count))", name="cumsum_change"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    # 3 months of data: Jan(2), Feb(2), Mar(2)
    assert response.row_count == 3
    assert "orders.cumsum_change" in response.columns

    # First row: change is NULL (no previous period), cumsum(NULL) = NULL
    assert response.data[0]["orders.cumsum_change"] is None

    # Remaining rows: cumsum(change(x)) == x - x[0]
    first_count = response.data[0]["orders.count"]
    for row in response.data[1:]:
        assert row["orders.cumsum_change"] == row["orders.count"] - first_count


def test_nested_cumsum_of_cumsum(integration_env):
    """Nested transforms: cumsum(cumsum(x)) should produce monotonically increasing values."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[
            Field(formula="count"),
            Field(formula="cumsum(count)", name="cs"),
            Field(formula="cumsum(cumsum(count))", name="cs_cs"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    assert response.row_count == 3
    # cumsum(cumsum) should be non-decreasing
    vals = [r["orders.cs_cs"] for r in response.data]
    assert all(a <= b for a, b in zip(vals, vals[1:]))
    # For constant counts (2,2,2): cumsum = (2,4,6), cumsum(cumsum) = (2,6,12)
    assert vals == [2, 6, 12]


def test_arithmetic_expression(integration_env):
    """Arithmetic field: total_amount / count = average."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        fields=[
            Field(formula="count"),
            Field(formula="total_amount"),
            Field(formula="total_amount / count", name="avg_amount"),
        ],
    )
    response = engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders.count"] == 6
    assert response.data[0]["orders.avg_amount"] == pytest.approx(125.0)


def test_time_shift_row_based(integration_env):
    """time_shift(x, -1) without granularity → LAG (previous row)."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[
            Field(formula="total_amount"),
            Field(formula="time_shift(total_amount, -1)", name="prev"),
            Field(formula="time_shift(total_amount, 1)", name="next"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

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


def test_time_shift_calendar_based(integration_env):
    """time_shift(x, -1, 'month') with granularity → calendar-based self-join."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[
            Field(formula="total_amount"),
            Field(formula="time_shift(total_amount, -1, 'month')", name="prev_month"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    # 3 months: Jan(300), Feb(125), Mar(325)
    assert response.row_count == 3

    # Calendar-based: Jan has no previous month in data → NULL
    assert response.data[0]["orders.prev_month"] is None
    # Feb's previous month is Jan
    assert response.data[1]["orders.prev_month"] == pytest.approx(300.0)
    # Mar's previous month is Feb
    assert response.data[2]["orders.prev_month"] == pytest.approx(125.0)


def test_time_shift_with_date_range(integration_env):
    """time_shift with date_range should fetch shifted data from outside the filtered range."""
    engine = integration_env

    # Query only March, but ask for previous month's value (February)
    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2025-03-01", "2025-03-31"],
        )],
        fields=[
            Field(formula="total_amount"),
            Field(formula="time_shift(total_amount, -1, 'month')", name="prev_month"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    # Only March in the result (date filter)
    assert response.row_count == 1
    assert response.data[0]["orders.total_amount"] == pytest.approx(325.0)
    # Previous month (February) should be fetched from the DB, not NULL
    assert response.data[0]["orders.prev_month"] == pytest.approx(125.0)


def test_change_with_date_range(integration_env):
    """change() with date_range should fetch previous period from outside the filtered range."""
    engine = integration_env

    # Query only March, change should compare to February
    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2025-03-01", "2025-03-31"],
        )],
        fields=[
            Field(formula="total_amount"),
            Field(formula="change(total_amount)", name="amount_change"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    assert response.row_count == 1
    # March(325) - February(125) = 200
    assert response.data[0]["orders.amount_change"] == pytest.approx(200.0)


def test_change_pct_with_date_range(integration_env):
    """change_pct() with date_range should compute correct percentage from shifted data."""
    engine = integration_env

    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2025-03-01", "2025-03-31"],
        )],
        fields=[
            Field(formula="total_amount"),
            Field(formula="change_pct(total_amount)", name="pct"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    assert response.row_count == 1
    # (325 - 125) / 125 = 1.6
    assert response.data[0]["orders.pct"] == pytest.approx(1.6)


def test_multiple_date_range_shifts(integration_env):
    """Multiple self-join transforms with different offsets should each get correct shifted data."""
    engine = integration_env

    # Query Feb only, ask for both previous (Jan) and next (Mar) month
    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2025-02-01", "2025-02-28"],
        )],
        fields=[
            Field(formula="total_amount"),
            Field(formula="time_shift(total_amount, -1, 'month')", name="prev"),
            Field(formula="time_shift(total_amount, 1, 'month')", name="next"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders.total_amount"] == pytest.approx(125.0)
    # Jan = 300
    assert response.data[0]["orders.prev"] == pytest.approx(300.0)
    # Mar = 325
    assert response.data[0]["orders.next"] == pytest.approx(325.0)


def test_forward_row_shift_with_date_range(integration_env):
    """time_shift(x, 1) (forward, row-based) with date_range should fetch the next period."""
    engine = integration_env

    # Query Feb only, ask for the next period's value (March)
    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
            date_range=["2025-02-01", "2025-02-28"],
        )],
        fields=[
            Field(formula="total_amount"),
            Field(formula="time_shift(total_amount, 1)", name="next_period"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders.total_amount"] == pytest.approx(125.0)
    # Next period (March) should be fetched from DB = 325
    assert response.data[0]["orders.next_period"] == pytest.approx(325.0)


def test_post_filter_on_change(integration_env):
    """Filter on a computed column (change) should only return matching rows."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # change values: Jan=NULL, Feb=125-300=-175, Mar=325-125=200
    # Filter: change < 0 → only February
    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[
            Field(formula="total_amount"),
            Field(formula="change(total_amount)", name="amount_change"),
        ],
        filters=["amount_change < 0"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    # Only February should remain (change = -175)
    assert response.row_count == 1
    assert response.data[0]["orders.amount_change"] == pytest.approx(-175.0)
    assert response.data[0]["orders.total_amount"] == pytest.approx(125.0)


def test_post_filter_with_base_filter(integration_env):
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
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[
            Field(formula="total_amount"),
            Field(formula="change(total_amount)", name="amount_change"),
        ],
        filters=["status != 'cancelled'", "amount_change > 0"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    # Only March (non-cancelled=325, change=275)
    assert response.row_count == 1
    assert response.data[0]["orders.amount_change"] == pytest.approx(275.0)


def test_inline_transform_filter(integration_env):
    """Transform expressions can be used directly in filters (auto-extracted as hidden fields)."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # change: Jan=NULL, Feb=-175, Mar=200
    # Filter: change(total_amount) < 0 → only February
    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[Field(formula="total_amount")],
        filters=["change(total_amount) < 0"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders.total_amount"] == pytest.approx(125.0)


def test_inline_last_change_filter(integration_env):
    """last(change(x)) in filter: keep rows only if the most recent period's change matches."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # change: Jan=NULL, Feb=-175, Mar=200
    # last(change) = 200 (March's change, broadcast to all rows)
    # Filter: last(change(total_amount)) > 0 → all rows pass (200 > 0)
    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[Field(formula="total_amount")],
        filters=["last(change(total_amount)) > 0"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    # last(change) = 200 > 0, so all 3 rows pass
    assert response.row_count == 3

    # Now filter for < 0 → no rows pass (last change is 200)
    query2 = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[Field(formula="total_amount")],
        filters=["last(change(total_amount)) < 0"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response2 = engine.execute(query2)
    assert response2.row_count == 0


def test_arithmetic_transform_filter(integration_env):
    """Arithmetic expressions with transforms in filters: change(x) / x > threshold."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # change: Jan=NULL, Feb=-175, Mar=200
    # change / total_amount: Jan=NULL, Feb=-175/125=-1.4, Mar=200/325≈0.615
    # Filter: change(total_amount) / total_amount > 0 → only March
    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[Field(formula="total_amount")],
        filters=["change(total_amount) / total_amount > 0"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    # Only March passes (positive change ratio)
    assert response.row_count == 1
    assert response.data[0]["orders.total_amount"] == pytest.approx(325.0)


def test_transform_on_filter_rhs(integration_env):
    """Transform expressions work on the RHS of filters too."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # time_shift(total_amount, -1): Jan=NULL, Feb=300, Mar=125
    # Filter: total_amount > time_shift(total_amount, -1) → months where value increased
    # Jan: 300 > NULL → NULL (filtered out), Feb: 125 > 300 → false, Mar: 325 > 125 → true
    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[Field(formula="total_amount")],
        filters=["total_amount > time_shift(total_amount, -1)"],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    # Only March (325 > 125)
    assert response.row_count == 1
    assert response.data[0]["orders.total_amount"] == pytest.approx(325.0)


def test_last_measure_type(integration_env):
    """A measure with type=last should return the most recent time bucket's value."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # latest_amount has type=last, so querying it as a bare measure
    # should auto-wrap with last() and return Mar's value (325) for all rows
    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[
            Field(formula="total_amount"),
            Field(formula="latest_amount"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    assert response.row_count == 3
    # latest_amount should be the same (most recent) value for all rows
    # Base agg is MAX(amount), March has max single order = 300
    latest_vals = [r["orders.latest_amount"] for r in response.data]
    assert len(set(latest_vals)) == 1  # All rows have the same value
    assert latest_vals[0] == pytest.approx(300.0)  # March's MAX(amount)


def test_having_filter(integration_env):
    """Filters on measures should use HAVING with the aggregate expression."""
    engine = integration_env

    # Group by status: completed(3 orders), pending(2), cancelled(1)
    # Filter: count > 1 → only completed and pending
    query = SlayerQuery(
        model="orders",
        dimensions=[ColumnRef(name="status")],
        fields=[Field(formula="count")],
        filters=["count > 1"],
        order=[OrderItem(column=ColumnRef(name="count"), direction="desc")],
    )
    response = engine.execute(query)

    assert response.row_count == 2
    assert response.data[0]["orders.status"] == "completed"
    assert response.data[0]["orders.count"] == 3
    assert response.data[1]["orders.status"] == "pending"
    assert response.data[1]["orders.count"] == 2


def test_having_filter_with_sum(integration_env):
    """HAVING on a SUM measure should use the SUM() expression."""
    engine = integration_env

    # Group by status: completed(100+200+300=600), pending(50+25=75), cancelled(75)
    # Filter: total_amount > 100 → only completed
    query = SlayerQuery(
        model="orders",
        dimensions=[ColumnRef(name="status")],
        fields=[Field(formula="total_amount")],
        filters=["total_amount > 100"],
        order=[OrderItem(column=ColumnRef(name="total_amount"), direction="desc")],
    )
    response = engine.execute(query)

    assert response.row_count == 1
    assert response.data[0]["orders.status"] == "completed"
    assert response.data[0]["orders.total_amount"] == pytest.approx(600.0)


def test_having_with_non_groupby_dimension_raises(integration_env):
    """HAVING filter referencing a dimension not in GROUP BY should error early."""
    engine = integration_env

    # Filter mixes measure (count) and dimension (status), but status is not in dimensions
    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[Field(formula="count")],
        filters=["count > 1 and status == 'completed'"],
    )
    with pytest.raises(ValueError, match="not in the query's dimensions"):
        engine.execute(query)
