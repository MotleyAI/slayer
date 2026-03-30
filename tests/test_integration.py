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

    # With self-join change, the first row's change is NULL (no previous period),
    # cumsum(NULL) = 0 in SQLite. The identity cumsum(change(x)) == x - x[0]
    # holds for all rows (including the first, where it equals 0).
    first_count = response.data[0]["orders.count"]
    for row in response.data:
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
