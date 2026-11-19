"""Integer result metadata must not narrow the database's aggregate range."""

import pytest

from slayer.core.enums import DataType
from slayer.core.models import DatasetRelationship, SlayerDataset
from slayer.core.query import SlayerQuery
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.plan import plan_query
from slayer.sql.generator import generate_from_planned

duckdb = pytest.importorskip("duckdb")


@pytest.fixture
def integer_orders() -> SlayerDataset:
    return SlayerDataset.model_validate(
        {
            "name": "orders",
            "data_source": "test",
            "sql_table": "orders",
            "default_time_dimension": "created_at",
            "fields": [
                {"name": "id", "datatype": "Integer", "primary_key": True},
                {"name": "amount", "datatype": "Integer"},
                {"name": "created_at", "datatype": "DateTime"},
            ],
            "metrics": [{"name": "as_int", "expression": "sum(amount)", "datatype": "Integer"}],
        }
    )


def _sql(*, dataset, metrics, referenced_datasets=(), **query_kwargs):
    bundle = ResolvedSourceBundle(dialect="duckdb", population=dataset, referenced_datasets=list(referenced_datasets))
    planned = plan_query(
        query=SlayerQuery.model_validate({"population": "orders", "metrics": metrics, **query_kwargs}),
        bundle=bundle,
    )
    return generate_from_planned(planned_query=planned, bundle=bundle, dialect="duckdb"), planned


@pytest.mark.parametrize(
    "sql_type,amount,aggregation,expected",
    [
        ("INT", 1_000_000_000, "sum", 2_000_000_000),
        ("INT", 2_000_000_000, "sum", 4_000_000_000),
        ("INT", -2_000_000_000, "sum", -4_000_000_000),
        ("BIGINT", 2**62, "sum", 2**63),
        ("BIGINT", 4_000_000_001, "max", 4_000_000_001),
        ("BIGINT", -4_000_000_001, "min", -4_000_000_001),
        ("BIGINT", 4_000_000_001, "first", 4_000_000_001),
        ("BIGINT", 4_000_000_001, "last", 4_000_000_001),
    ],
)
def test_inferred_integer_aggregate_preserves_native_range(integer_orders: SlayerDataset, sql_type, amount, aggregation, expected):
    sql, planned = _sql(dataset=integer_orders, metrics=[f"{aggregation}(orders.amount)"])
    # first/last desugar onto the regroup primitive: their public
    # slot is ROW-phase; the other aggregations keep an AGGREGATE-phase slot.
    slot = next(
        s for s in (*planned.aggregate_slots, *planned.row_slots)
        if s.declared_name == f"amount_{aggregation}"
    )
    assert slot.type is DataType.INTEGER
    with duckdb.connect() as connection:
        connection.execute(f"CREATE TABLE orders (id INT, amount {sql_type}, created_at TIMESTAMP)")
        connection.execute(
            query="INSERT INTO orders VALUES (1, ?, '2024-01-01'), (2, ?, '2024-01-02')",
            parameters=[amount, amount],
        )
        value = connection.execute(sql).fetchone()[0]
    assert isinstance(value, int)
    assert value == expected


@pytest.mark.parametrize(
    "measure",
    [{"metric": "sum(orders.amount)", "datatype": "Integer"}, "as_int"],
)
def test_explicit_integer_measure_casts_to_64_bit(integer_orders: SlayerDataset, measure):
    sql, _ = _sql(dataset=integer_orders, metrics=[measure])
    assert "CAST(SUM(orders.amount) AS BIGINT)" in sql
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders (id INT, amount INT)")
        connection.execute("INSERT INTO orders VALUES (1, 2000000000), (2, 2000000000)")
        assert connection.execute(sql).fetchone()[0] == 4_000_000_000


@pytest.mark.parametrize(
    "formula,expected",
    [
        ("sum(orders.amount, window='2d')", [4_000_000_001, 8_000_000_002]),
        ("cumsum(sum(orders.amount))", [4_000_000_001, 8_000_000_002]),
        ("time_shift(sum(orders.amount), periods=-1)", [4_000_000_001]),
    ],
)
def test_integer_range_survives_window_and_shift_paths(integer_orders: SlayerDataset, formula, expected):
    sql, _ = _sql(
        dataset=integer_orders,
        metrics=[{"metric": formula, "alias": "total"}],
        time_dimensions=[{"field": "orders.created_at", "granularity": "day"}],
    )
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders (id INT, amount BIGINT, created_at TIMESTAMP)")
        connection.execute("INSERT INTO orders VALUES (1, 4000000001, '2024-01-01'), (2, 4000000001, '2024-01-02')")
        rows = connection.execute(sql).fetchall()
    values = [row[-1] for row in rows if row[-1] is not None]
    assert all(isinstance(value, int) for value in values)
    assert sorted(values) == expected


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("reroot", [False, True])
def test_joined_integer_aggregate_preserves_cast_intent(integer_orders: SlayerDataset, explicit, reroot):
    integer_orders.relationships = [DatasetRelationship(to="customers", from_columns=["id"], to_columns=["id"])]
    customers = integer_orders.model_copy(
        update={
            "name": "customers",
            "sql_table": "customers",
            "metrics": [],
            "relationships": [],
        }
    )
    measure = {"metric": "sum(customers.amount)", "alias": "total"}
    if explicit:
        measure["datatype"] = "Integer"
    sql, _ = _sql(
        dataset=integer_orders,
        referenced_datasets=[customers],
        metrics=[measure],
        dimensions=["orders.created_at"] if reroot else [],
    )
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders (id INT, amount INT, created_at TIMESTAMP)")
        connection.execute("INSERT INTO orders VALUES (1, 2000000000, '2024-01-01'), (2, 2000000000, '2024-01-01')")
        connection.execute("CREATE TABLE customers AS SELECT * FROM orders")
        assert [row[-1] for row in connection.execute(sql).fetchall()] == [4_000_000_000]
    assert ("AS BIGINT)" in sql) is explicit


@pytest.mark.parametrize("explicit", [False, True])
def test_partitioned_integer_aggregate_preserves_cast_intent(integer_orders: SlayerDataset, explicit):
    measure = {"metric": "sum(orders.amount, partition_by=orders.created_at)", "alias": "total"}
    if explicit:
        measure["datatype"] = "Integer"
    sql, _ = _sql(dataset=integer_orders, metrics=[measure], dimensions=["orders.id", "orders.created_at"])
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders (id INT, amount INT, created_at TIMESTAMP)")
        connection.execute("INSERT INTO orders VALUES (1, 2000000000, '2024-01-01'), (2, 2000000000, '2024-01-01')")
        assert [row[-1] for row in connection.execute(sql).fetchall()] == [4_000_000_000, 4_000_000_000]
    assert ("AS BIGINT)" in sql) is explicit


@pytest.mark.parametrize("values,expected", [([], None), ([None], None), ([None, 4_000_000_001], 4_000_000_001)])
def test_inferred_integer_sum_preserves_null_and_empty_semantics(integer_orders: SlayerDataset, values, expected):
    sql, _ = _sql(dataset=integer_orders, metrics=["sum(orders.amount)"])
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders (amount BIGINT)")
        for value in values:
            connection.execute(query="INSERT INTO orders VALUES (?)", parameters=[value])
        assert connection.execute(sql).fetchall() == [(expected,)]


def test_query_type_override_wins_over_saved_integer_type(integer_orders: SlayerDataset):
    sql, planned = _sql(dataset=integer_orders, metrics=[{"metric": "orders.as_int", "datatype": "Float"}])
    assert planned.aggregate_slots[0].type is DataType.FLOAT
    assert "CAST(SUM(orders.amount) AS DOUBLE)" in sql
