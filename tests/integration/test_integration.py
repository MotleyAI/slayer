"""Integration tests — end-to-end queries against a real SQLite database.

Run with: pytest tests/integration/test_integration.py -m integration
"""

import sqlite3

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import (
    DatasourceConfig,
    Dimension,
    Measure,
    ModelJoin,
    SlayerModel,
)
from slayer.core.query import (
    ColumnRef,
    Field,
    ModelExtension,
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
    # type=last returns the latest record's value within each month:
    # Jan: orders on 15th(100) and 20th(200) → latest = 200
    # Feb: orders on 10th(50) and 15th(75) → latest = 75
    # Mar: orders on 5th(300) and 20th(25) → latest = 25
    assert response.data[0]["orders.latest_amount"] == pytest.approx(200.0)
    assert response.data[1]["orders.latest_amount"] == pytest.approx(75.0)
    assert response.data[2]["orders.latest_amount"] == pytest.approx(25.0)


def test_last_function(integration_env):
    """last() function should broadcast the most recent time bucket's value to all rows."""
    engine = integration_env

    # 3 months: Jan(300), Feb(125), Mar(325)
    # last(total_amount) = March's total (325) broadcast to all rows
    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"),
            granularity=TimeGranularity.MONTH,
        )],
        fields=[
            Field(formula="total_amount"),
            Field(formula="last(total_amount)", name="latest"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    assert response.row_count == 3
    # last() broadcasts the most recent bucket's value to ALL rows
    latest_vals = [r["orders.latest"] for r in response.data]
    assert len(set(latest_vals)) == 1  # Same value everywhere
    assert latest_vals[0] == pytest.approx(325.0)  # March's SUM


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


# ---------------------------------------------------------------------------
# Cross-model measures
# ---------------------------------------------------------------------------

@pytest.fixture
def cross_model_env(tmp_path):
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
    storage.save_datasource(DatasourceConfig(name="db", type="sqlite", database=str(db_path)))

    storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="db",
        default_time_dimension="created_at",
        dimensions=[
            Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="customer_id", sql="customer_id", type=DataType.NUMBER),
            Dimension(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ],
        measures=[
            Measure(name="count", type=DataType.COUNT),
            Measure(name="total_amount", sql="amount", type=DataType.SUM),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    ))
    storage.save_model(SlayerModel(
        name="customers", sql_table="customers", data_source="db",
        dimensions=[
            Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="name", sql="name", type=DataType.STRING),
        ],
        measures=[
            Measure(name="count", type=DataType.COUNT),
            Measure(name="avg_score", sql="score", type=DataType.AVERAGE),
            Measure(name="max_score", sql="score", type=DataType.MAX),
        ],
    ))

    return SlayerQueryEngine(storage=storage)


def test_cross_model_measure_monthly(cross_model_env):
    """Cross-model measure: monthly order count + avg customer score from joined model."""
    engine = cross_model_env

    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        fields=[
            Field(formula="count"),
            Field(formula="customers.avg_score"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    assert response.row_count == 3
    # Jan: Alice (90), Feb: Bob (60), Mar: Charlie(80) + Alice(90) = avg 85
    assert response.data[0]["orders.customers__avg_score"] == pytest.approx(90.0)
    assert response.data[1]["orders.customers__avg_score"] == pytest.approx(60.0)
    assert response.data[2]["orders.customers__avg_score"] == pytest.approx(85.0)


def test_cross_model_measure_no_join_raises(cross_model_env):
    """Referencing a model with no join should raise."""
    engine = cross_model_env

    query = SlayerQuery(
        model="orders",
        fields=[Field(formula="count"), Field(formula="nonexistent.some_measure")],
    )
    with pytest.raises(ValueError, match="has no join to"):
        engine.execute(query)


def test_transform_on_cross_model(cross_model_env):
    """Transforms on cross-model measures work (applied after the cross-model join)."""
    engine = cross_model_env

    # cumsum of avg customer score per month
    query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        fields=[
            Field(formula="customers.avg_score"),
            Field(formula="cumsum(customers.avg_score)", name="running"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )
    response = engine.execute(query)

    # Jan: Alice(90) → cumsum=90, Feb: Bob(60) → cumsum=150, Mar: Charlie(80)+Alice(90)=85 → cumsum=235
    assert response.data[0]["orders.running"] == pytest.approx(90.0)
    assert response.data[1]["orders.running"] == pytest.approx(150.0)
    assert response.data[2]["orders.running"] == pytest.approx(235.0)


# ---------------------------------------------------------------------------
# Query as model (multistage queries)
# ---------------------------------------------------------------------------

def test_query_as_model_count(integration_env):
    """A named query can be used as the model for another query via list."""
    engine = integration_env

    # Inner: monthly order counts (3 months), named for reference
    inner = SlayerQuery(
        name="monthly",
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        fields=[Field(formula="count"), Field(formula="total_amount")],
    )

    # Outer: count how many months exist (references "monthly" by name)
    outer = SlayerQuery(model="monthly", fields=[Field(formula="count")])
    response = engine.execute(query=[inner, outer])

    assert response.row_count == 1
    assert response.data[0]["monthly.count"] == 3


def test_query_as_model_aggregate(integration_env):
    """Outer query can aggregate over inner query's computed values."""
    engine = integration_env

    inner = SlayerQuery(
        name="monthly",
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        fields=[Field(formula="total_amount")],
    )

    outer = SlayerQuery(model="monthly", fields=[Field(formula="total_amount_sum")])
    response = engine.execute(query=[inner, outer])

    assert response.row_count == 1
    assert response.data[0]["monthly.total_amount_sum"] == pytest.approx(750.0)


def test_create_model_from_query(integration_env):
    """A query can be saved as a permanent model and then queried by name."""
    engine = integration_env

    # Create a monthly summary model from a query
    source_query = SlayerQuery(
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        fields=[Field(formula="count"), Field(formula="total_amount")],
    )
    saved = engine.create_model_from_query(
        query=source_query, name="monthly_summary",
    )

    # Verify model structure
    dim_names = [d.name for d in saved.dimensions]
    assert "created_at" in dim_names
    assert "count" in dim_names
    assert "total_amount" in dim_names
    assert saved.source_queries is not None

    # Query the saved model by name
    result = engine.execute(query=SlayerQuery(
        model="monthly_summary", fields=[Field(formula="count")],
    ))
    assert result.data[0]["monthly_summary.count"] == 3

    # Re-aggregate over saved model
    result2 = engine.execute(query=SlayerQuery(
        model="monthly_summary", fields=[Field(formula="total_amount_sum")],
    ))
    assert result2.data[0]["monthly_summary.total_amount_sum"] == pytest.approx(750.0)


def test_query_list_with_joins(cross_model_env):
    """A query list where the main query joins to a named sub-query."""
    engine = cross_model_env

    # Sub-query: average customer score per customer
    sub = SlayerQuery(
        name="customer_scores",
        model="customers",
        dimensions=[ColumnRef(name="id")],
        fields=[Field(formula="avg_score")],
    )

    # Main query: monthly orders joined to customer_scores
    # In the virtual model, inner measures become dimensions with auto-generated
    # SUM/AVG measures. Use avg_score_avg to re-average the inner avg_score.
    from slayer.core.query import ModelExtension
    main = SlayerQuery(
        model=ModelExtension(
            source_name="orders",
            joins=[{"target_model": "customer_scores", "join_pairs": [["customer_id", "id"]]}],
        ),
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        fields=[
            Field(formula="count"),
            Field(formula="customer_scores.avg_score_avg"),
        ],
        order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
    )

    response = engine.execute(query=[sub, main])

    assert response.row_count == 3
    # Jan: Alice(90), Feb: Bob(60), Mar: Charlie(80)+Alice(90)=85
    assert response.data[0]["orders.customer_scores__avg_score_avg"] == pytest.approx(90.0)
    assert response.data[1]["orders.customer_scores__avg_score_avg"] == pytest.approx(60.0)
    assert response.data[2]["orders.customer_scores__avg_score_avg"] == pytest.approx(85.0)


# ---------------------------------------------------------------------------
# Expanded dimensions (SQL expressions)
# ---------------------------------------------------------------------------

def test_sql_dimension_via_model_extension(integration_env):
    """SQL expression dimension via ModelExtension: CASE to bucket amounts."""
    engine = integration_env

    query = SlayerQuery(
        model=ModelExtension(
            source_name="orders",
            dimensions=[{"name": "tier", "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"}],
        ),
        dimensions=[ColumnRef(name="tier")],
        fields=[Field(formula="count")],
    )
    response = engine.execute(query)

    by_tier = {r["orders.tier"]: r["orders.count"] for r in response.data}
    assert by_tier["high"] == 2
    assert by_tier["low"] == 4


def test_sql_dimension_with_regular(integration_env):
    """SQL dimension via ModelExtension mixed with regular dimension."""
    engine = integration_env

    query = SlayerQuery(
        model=ModelExtension(
            source_name="orders",
            dimensions=[{"name": "tier", "sql": "CASE WHEN amount > 100 THEN 'high' ELSE 'low' END"}],
        ),
        dimensions=[ColumnRef(name="status"), ColumnRef(name="tier")],
        fields=[Field(formula="count")],
    )
    response = engine.execute(query)

    # completed has 3 orders: 100(low), 200(high), 300(high)
    data = {(r["orders.status"], r["orders.tier"]): r["orders.count"] for r in response.data}
    assert data[("completed", "high")] == 2
    assert data[("completed", "low")] == 1


def test_formula_dimension_via_query_list(integration_env):
    """Formula dimensions on aggregates work via multistage query list."""
    engine = integration_env

    # Inner: compute monthly totals
    inner = SlayerQuery(
        name="monthly",
        model="orders",
        time_dimensions=[TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )],
        fields=[Field(formula="total_amount")],
    )

    # Outer: group by amount tier via ModelExtension on the inner query's result
    outer = SlayerQuery(
        model=ModelExtension(
            source_name="monthly",
            dimensions=[{"name": "amount_tier",
                         "sql": "CASE WHEN total_amount > 200 THEN 'high' ELSE 'low' END"}],
        ),
        dimensions=[ColumnRef(name="amount_tier")],
        fields=[Field(formula="count")],
    )

    response = engine.execute(query=[inner, outer])

    # Jan(300)=high, Feb(125)=low, Mar(325)=high
    by_tier = {r["monthly.amount_tier"]: r["monthly.count"] for r in response.data}
    assert by_tier["high"] == 2
    assert by_tier["low"] == 1


def test_circular_query_reference_raises(integration_env):
    """Circular references between named queries should error clearly."""
    engine = integration_env

    q1 = SlayerQuery(name="a", model="b", fields=[Field(formula="count")])
    q2 = SlayerQuery(name="b", model="a", fields=[Field(formula="count")])
    main = SlayerQuery(model="a", fields=[Field(formula="count")])
    with pytest.raises(ValueError, match="Circular reference"):
        engine.execute(query=[q1, q2, main])


def test_circular_join_graph_raises(tmp_path):
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
    storage.save_datasource(DatasourceConfig(name="db", type="sqlite", database=str(db_path)))

    # Circular joins: a → b → a
    storage.save_model(SlayerModel(
        name="a", sql_table="a", data_source="db",
        dimensions=[Dimension(name="id", sql="id", type=DataType.NUMBER),
                    Dimension(name="b_id", sql="b_id", type=DataType.NUMBER)],
        measures=[Measure(name="count", type=DataType.COUNT)],
        joins=[ModelJoin(target_model="b", join_pairs=[["b_id", "id"]])],
    ))
    storage.save_model(SlayerModel(
        name="b", sql_table="b", data_source="db",
        dimensions=[Dimension(name="id", sql="id", type=DataType.NUMBER),
                    Dimension(name="a_id", sql="a_id", type=DataType.NUMBER),
                    Dimension(name="unique_b_field", sql="id", type=DataType.NUMBER)],
        measures=[Measure(name="count", type=DataType.COUNT)],
        joins=[ModelJoin(target_model="a", join_pairs=[["a_id", "id"]])],
    ))

    engine = SlayerQueryEngine(storage=storage)

    # Trying to resolve b.a.unique_b_field — walks a→b→a which is a cycle.
    # "unique_b_field" only exists on model b, so __ translation can't short-circuit.
    query = SlayerQuery(
        model="a",
        dimensions=[ColumnRef(name="b.a.unique_b_field")],
        fields=[Field(formula="count")],
    )
    with pytest.raises(ValueError, match="Circular join"):
        engine.execute(query)
