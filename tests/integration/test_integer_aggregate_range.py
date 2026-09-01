"""Integer result metadata must not narrow the database's aggregate range (#347)."""

import pytest

duckdb = pytest.importorskip("duckdb")
pytestmark = pytest.mark.integration

from slayer.core.enums import DataType
from slayer.core.models import ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.engine.stage_planner import plan_query
from slayer.sql.generator import generate_from_planned


@pytest.fixture
def integer_orders():
    return SlayerModel.model_validate(
        {
            "name": "orders",
            "data_source": "test",
            "sql_table": "orders",
            "default_time_dimension": "created_at",
            "columns": [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "amount", "type": "INT"},
                {"name": "created_at", "type": "TIMESTAMP"},
            ],
            "measures": [{"name": "as_int", "formula": "amount:sum", "type": "INT"}],
        }
    )


def _sql(*, model, measures, referenced_models=(), **query_kwargs):
    bundle = ResolvedSourceBundle(source_model=model, referenced_models=list(referenced_models))
    planned = plan_query(
        query=SlayerQuery.model_validate({"source_model": "orders", "measures": measures, **query_kwargs}),
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
def test_inferred_integer_aggregate_preserves_native_range(integer_orders, sql_type, amount, aggregation, expected):
    sql, planned = _sql(model=integer_orders, measures=[f"amount:{aggregation}"])
    # first/last desugar onto the regroup primitive (DEV-1835): their public
    # slot is ROW-phase; the other aggregations keep an AGGREGATE-phase slot.
    slot = next(
        s for s in (*planned.aggregate_slots, *planned.row_slots)
        if s.declared_name == f"amount_{aggregation}"
    )
    assert slot.type is DataType.INT
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
    [{"formula": "amount:sum", "type": "INT"}, "as_int"],
)
def test_explicit_integer_measure_still_casts(integer_orders, measure):
    sql, _ = _sql(model=integer_orders, measures=[measure])
    assert "CAST(SUM(orders.amount) AS INT)" in sql
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders (id INT, amount INT)")
        connection.execute("INSERT INTO orders VALUES (1, 2000000000), (2, 2000000000)")
        with pytest.raises(duckdb.ConversionException, match="out of range"):
            connection.execute(sql)


@pytest.mark.parametrize(
    "formula,expected",
    [
        ("amount:sum(window='2d')", [4_000_000_001, 8_000_000_002]),
        ("cumsum(amount:sum)", [4_000_000_001, 8_000_000_002]),
        ("time_shift(amount:sum, periods=-1)", [4_000_000_001]),
    ],
)
def test_integer_range_survives_window_and_shift_paths(integer_orders, formula, expected):
    sql, _ = _sql(
        model=integer_orders,
        measures=[{"formula": formula, "name": "total"}],
        time_dimensions=[{"dimension": "created_at", "granularity": "day"}],
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
def test_joined_integer_aggregate_preserves_cast_intent(integer_orders, explicit, reroot):
    integer_orders.joins = [ModelJoin(target_model="customers", join_pairs=[["id", "id"]])]
    customers = integer_orders.model_copy(
        update={
            "name": "customers",
            "sql_table": "customers",
            "measures": [],
            "joins": [ModelJoin(target_model="orders", join_pairs=[["id", "id"]])],
        }
    )
    measure = {"formula": "customers.amount:sum", "name": "total"}
    if explicit:
        measure["type"] = "INT"
    sql, _ = _sql(
        model=integer_orders,
        referenced_models=[customers],
        measures=[measure],
        dimensions=["created_at"] if reroot else [],
    )
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders (id INT, amount INT, created_at TIMESTAMP)")
        connection.execute("INSERT INTO orders VALUES (1, 2000000000, '2024-01-01'), (2, 2000000000, '2024-01-01')")
        connection.execute("CREATE TABLE customers AS SELECT * FROM orders")
        if explicit:
            with pytest.raises(duckdb.ConversionException, match="out of range"):
                connection.execute(sql)
        else:
            assert [row[-1] for row in connection.execute(sql).fetchall()] == [4_000_000_000]


@pytest.mark.parametrize("explicit", [False, True])
def test_partitioned_integer_aggregate_preserves_cast_intent(integer_orders, explicit):
    measure = {"formula": "amount:sum(partition_by=created_at)", "name": "total"}
    if explicit:
        measure["type"] = "INT"
    sql, _ = _sql(model=integer_orders, measures=[measure], dimensions=["id", "created_at"])
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders (id INT, amount INT, created_at TIMESTAMP)")
        connection.execute("INSERT INTO orders VALUES (1, 2000000000, '2024-01-01'), (2, 2000000000, '2024-01-01')")
        if explicit:
            with pytest.raises(duckdb.ConversionException, match="out of range"):
                connection.execute(sql)
        else:
            assert [row[-1] for row in connection.execute(sql).fetchall()] == [4_000_000_000, 4_000_000_000]


@pytest.mark.parametrize("values,expected", [([], None), ([None], None), ([None, 4_000_000_001], 4_000_000_001)])
def test_inferred_integer_sum_preserves_null_and_empty_semantics(integer_orders, values, expected):
    sql, _ = _sql(model=integer_orders, measures=["amount:sum"])
    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE orders (amount BIGINT)")
        for value in values:
            connection.execute(query="INSERT INTO orders VALUES (?)", parameters=[value])
        assert connection.execute(sql).fetchall() == [(expected,)]


def test_query_type_override_wins_over_saved_integer_type(integer_orders):
    sql, planned = _sql(model=integer_orders, measures=[{"formula": "as_int", "type": "DOUBLE"}])
    assert planned.aggregate_slots[0].type is DataType.DOUBLE
    assert "CAST(SUM(orders.amount) AS DOUBLE)" in sql
