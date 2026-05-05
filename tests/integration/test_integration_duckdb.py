"""Integration tests using a real DuckDB database (in-process, no Docker needed)."""

import tempfile

import pytest

pytest.importorskip("duckdb")

import duckdb

from slayer.async_utils import run_sync
from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, OrderItem, SlayerQuery, TimeDimension
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture(scope="module")
def _duckdb_env_storage(tmp_path_factory):
    """Module-scoped: per-module DuckDB file with seeded customers/orders, plus
    a YAMLStorage with the orders + customers models pre-saved. The per-test
    ``duckdb_env`` fixture wraps a fresh engine around the returned storage
    (engines bind to their event loop — see slayer/sql/client.py:144)."""
    tmp_path = tmp_path_factory.mktemp("duckdb_env")
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))

    conn.execute("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            region VARCHAR NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            status VARCHAR NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            customer_id INTEGER REFERENCES customers(id),
            created_at TIMESTAMP NOT NULL
        )
    """)
    conn.executemany(
        "INSERT INTO customers VALUES (?, ?, ?)",
        [(1, "Acme Corp", "US"), (2, "Globex", "EU"), (3, "Initech", "US")],
    )
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
        [
            (1, "completed", 100, 1, "2024-01-15 10:00:00"),
            (2, "completed", 200, 1, "2024-01-20 11:00:00"),
            (3, "pending", 50, 2, "2024-02-10 09:00:00"),
            (4, "completed", 150, 2, "2024-02-15 14:00:00"),
            (5, "cancelled", 75, 3, "2024-03-01 08:00:00"),
            (6, "pending", 300, 3, "2024-03-10 16:00:00"),
        ],
    )
    conn.close()

    # Set up SLayer storage
    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))

    run_sync(storage.save_datasource(DatasourceConfig(
        name="testduckdb",
        type="duckdb",
        database=str(db_path),
    )))

    orders_model = SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="testduckdb",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="status", sql="status", type=DataType.STRING),
            Column(name="amount", sql="amount", type=DataType.NUMBER),
            Column(name="customer_id", sql="customer_id", type=DataType.NUMBER),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),

            Column(name="total", sql="amount", type=DataType.NUMBER),
            Column(name="avg_amount", sql="amount", type=DataType.NUMBER),
        ],
    )
    customers_model = SlayerModel(
        name="customers",
        sql_table="customers",
        data_source="testduckdb",
        columns=[
            Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Column(name="name", sql="name", type=DataType.STRING),
            Column(name="region", sql="region", type=DataType.STRING),

        ],
    )
    run_sync(storage.save_model(orders_model))
    run_sync(storage.save_model(customers_model))

    return storage


@pytest.fixture
def duckdb_env(_duckdb_env_storage):
    """Per-test SlayerQueryEngine wrapping the module-scoped storage. The
    engine is recreated per-test because its async SQLAlchemy engine binds to
    the current event loop."""
    return SlayerQueryEngine(storage=_duckdb_env_storage)


@pytest.mark.integration
class TestDuckDBQueries:
    async def test_count_all(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "*:count"}])
        result = await duckdb_env.execute(query=query)
        assert result.row_count == 1
        assert result.data[0]["orders._count"] == 6

    async def test_sum_measure(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "total:sum"}])
        result = await duckdb_env.execute(query=query)
        assert float(result.data[0]["orders.total_sum"]) == 875.0

    async def test_avg_measure(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(source_model="orders", measures=[{"formula": "avg_amount:avg"}])
        result = await duckdb_env.execute(query=query)
        avg = float(result.data[0]["orders.avg_amount_avg"])
        assert abs(avg - 145.83) < 0.1

    async def test_group_by_status(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
        )
        result = await duckdb_env.execute(query=query)
        by_status = {r["orders.status"]: r["orders._count"] for r in result.data}
        assert by_status["completed"] == 3
        assert by_status["pending"] == 2
        assert by_status["cancelled"] == 1

    async def test_filter_equals(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["status == 'completed'"],
        )
        result = await duckdb_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3

    async def test_filter_gt(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["amount > 100"],
        )
        result = await duckdb_env.execute(query=query)
        assert result.data[0]["orders._count"] == 3  # 200, 150, 300

    async def test_order_by_desc(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
            order=[{"column": {"name": "count"}, "direction": "desc"}],
        )
        result = await duckdb_env.execute(query=query)
        assert result.data[0]["orders.status"] == "completed"

    async def test_limit(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "status"}],
            limit=2,
        )
        result = await duckdb_env.execute(query=query)
        assert result.row_count == 2

    async def test_multiple_measures(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}, {"formula": "total:sum"}],
            dimensions=[{"name": "status"}],
        )
        result = await duckdb_env.execute(query=query)
        completed = next(r for r in result.data if r["orders.status"] == "completed")
        assert completed["orders._count"] == 3
        assert float(completed["orders.total_sum"]) == 450.0

    async def test_time_dimension_month_granularity(self, duckdb_env: SlayerQueryEngine) -> None:
        """DuckDB supports DATE_TRUNC natively."""
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            time_dimensions=[{"dimension": {"name": "created_at"}, "granularity": "month"}],
        )
        result = await duckdb_env.execute(query=query)
        assert result.row_count == 3  # Jan, Feb, Mar

    async def test_time_dimension_with_date_range(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            time_dimensions=[{
                "dimension": {"name": "created_at"},
                "granularity": "month",
                "date_range": ["2024-01-01", "2024-02-28"],
            }],
        )
        result = await duckdb_env.execute(query=query)
        # Only Jan and Feb orders (4 orders)
        total = sum(r["orders._count"] for r in result.data)
        assert total == 4

    async def test_composite_filter(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            filters=["status == 'completed' or status == 'pending'"],
        )
        result = await duckdb_env.execute(query=query)
        assert result.data[0]["orders._count"] == 5  # 3 completed + 2 pending

    async def test_time_shift_with_date_range(self, duckdb_env: SlayerQueryEngine) -> None:
        """time_shift with date_range should fetch shifted data from outside the filtered range."""
        # Query only March, ask for previous month (February)
        # Seed: Jan(300), Feb(200), Mar(375)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-03-31"],
            )],
            measures=[
                ModelMeasure(formula="total:sum"),
                ModelMeasure(formula="time_shift(total:sum, -1, 'month')", name="prev_month"),
            ],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        )
        result = await duckdb_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(375.0)
        # Previous month (Feb) fetched from DB, not NULL
        assert float(result.data[0]["orders.prev_month"]) == pytest.approx(200.0)

    async def test_change_with_date_range(self, duckdb_env: SlayerQueryEngine) -> None:
        """change() with date_range should fetch previous period from outside the filtered range."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-03-31"],
            )],
            measures=[
                ModelMeasure(formula="total:sum"),
                ModelMeasure(formula="change(total:sum)", name="amount_change"),
            ],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        )
        result = await duckdb_env.execute(query=query)
        assert result.row_count == 1
        # Mar(375) - Feb(200) = 175
        assert float(result.data[0]["orders.amount_change"]) == pytest.approx(175.0)

    async def test_change_pct_with_date_range(self, duckdb_env: SlayerQueryEngine) -> None:
        """change_pct() with date_range should compute correct percentage from shifted data."""
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
                date_range=["2024-03-01", "2024-03-31"],
            )],
            measures=[
                ModelMeasure(formula="total:sum"),
                ModelMeasure(formula="change_pct(total:sum)", name="pct"),
            ],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        )
        result = await duckdb_env.execute(query=query)
        assert result.row_count == 1
        # (375 - 200) / 200 = 0.875
        assert float(result.data[0]["orders.pct"]) == pytest.approx(0.875)

    async def test_multiple_date_range_shifts(self, duckdb_env: SlayerQueryEngine) -> None:
        """Multiple self-join transforms with different offsets should each get correct data."""
        # Query Feb only, ask for both previous (Jan) and next (Mar)
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
                date_range=["2024-02-01", "2024-02-29"],
            )],
            measures=[
                ModelMeasure(formula="total:sum"),
                ModelMeasure(formula="time_shift(total:sum, -1, 'month')", name="prev"),
                ModelMeasure(formula="time_shift(total:sum, 1, 'month')", name="next"),
            ],
            order=[OrderItem(column=ColumnRef(name="created_at"), direction="asc")],
        )
        result = await duckdb_env.execute(query=query)
        assert result.row_count == 1
        assert float(result.data[0]["orders.total_sum"]) == pytest.approx(200.0)
        assert float(result.data[0]["orders.prev"]) == pytest.approx(300.0)  # Jan
        assert float(result.data[0]["orders.next"]) == pytest.approx(375.0)  # Mar


@pytest.fixture(scope="module")
def duckdb_ingest_env(tmp_path_factory):
    """Set up tables with FK relationships and ingest via rollup.

    Module-scoped: tests destructure ``(models, ds)`` and build their own
    ephemeral storage from the returned models — they never mutate the fixture
    state. Saves the ~0.55s/call SQLAlchemy reflection across 6 tests.
    """
    tmp_path = tmp_path_factory.mktemp("duckdb_ingest")
    db_path = tmp_path / "ingest.duckdb"
    conn = duckdb.connect(str(db_path))

    conn.execute("""
        CREATE TABLE regions (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            region_id INTEGER REFERENCES regions(id)
        )
    """)
    conn.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            amount DECIMAL(10,2) NOT NULL,
            customer_id INTEGER REFERENCES customers(id)
        )
    """)
    conn.executemany("INSERT INTO regions VALUES (?, ?)", [(1, "US"), (2, "EU")])
    conn.executemany(
        "INSERT INTO customers VALUES (?, ?, ?)",
        [(1, "Acme", 1), (2, "Globex", 2), (3, "Initech", 1)],
    )
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?)",
        [(1, 100, 1), (2, 200, 1), (3, 50, 2), (4, 150, 3)],
    )
    conn.close()

    ds = DatasourceConfig(
        name="testduckdb",
        type="duckdb",
        database=str(db_path),
    )

    models = ingest_datasource(datasource=ds)
    return models, ds


@pytest.mark.integration
class TestDuckDBIngestion:
    def test_orders_has_own_columns_only(self, duckdb_ingest_env) -> None:
        """After ingestion, models only have their own columns (no flattened joined dims)."""
        models, _ = duckdb_ingest_env
        orders = next(m for m in models if m.name == "orders")

        col_names = [c.name for c in orders.columns]
        # Should have own columns only.
        assert "id" in col_names
        assert "customer_id" in col_names
        # In v2, every non-joined column appears once (numeric columns included).
        assert "amount" in col_names
        # Joined dimensions are resolved via join graph, not pre-flattened.
        assert not any("." in name for name in col_names)

    def test_orders_uses_sql_table_with_joins(self, duckdb_ingest_env) -> None:
        models, _ = duckdb_ingest_env
        orders = next(m for m in models if m.name == "orders")

        # Models with joins use sql_table + explicit joins (no baked sql)
        assert orders.sql_table is not None
        assert orders.sql is None
        assert len(orders.joins) > 0
        join_targets = [j.target_model for j in orders.joins]
        assert "customers" in join_targets
        # Multi-hop targets (regions) are NOT baked in — resolved at query time
        assert "regions" not in join_targets

    def test_regions_has_no_rollup(self, duckdb_ingest_env) -> None:
        models, _ = duckdb_ingest_env
        regions = next(m for m in models if m.name == "regions")

        # Regions references nothing, should keep sql_table
        assert regions.sql_table is not None
        assert regions.sql is None

    def test_orders_has_no_named_measures_after_ingest(self, duckdb_ingest_env) -> None:
        """After ingestion, model.measures (formula list) is empty in v2.

        v1 generated row-level measures from each non-ID column. In v2, those
        live on ``columns`` instead and ``measures`` (named formulas) is an
        opt-in library users populate themselves.
        """
        models, _ = duckdb_ingest_env
        orders = next(m for m in models if m.name == "orders")
        assert orders.measures == []
        # The 'amount' column lives on .columns now.
        col_names = [c.name for c in orders.columns]
        assert "amount" in col_names

    async def test_rollup_query_group_by_customer(self, duckdb_ingest_env) -> None:
        """Query orders grouped by rolled-up customer name."""
        models, ds = duckdb_ingest_env

        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_model(m)
        engine = SlayerQueryEngine(storage=storage)

        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}],
            dimensions=[{"name": "customers.name"}],
        )
        result = await engine.execute(query=query)

        by_name = {r["orders.customers.name"]: r["orders._count"] for r in result.data}
        assert by_name["Acme"] == 2
        assert by_name["Globex"] == 1
        assert by_name["Initech"] == 1

    async def test_rollup_query_group_by_region(self, duckdb_ingest_env) -> None:
        """Query orders grouped by transitively rolled-up region name."""
        models, ds = duckdb_ingest_env

        tmpdir = tempfile.mkdtemp()
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(ds)
        for m in models:
            await storage.save_model(m)
        engine = SlayerQueryEngine(storage=storage)

        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "*:count"}, {"formula": "amount:sum"}],
            dimensions=[{"name": "customers.regions.name"}],
        )
        result = await engine.execute(query=query)

        by_region = {r["orders.customers.regions.name"]: r for r in result.data}
        assert by_region["US"]["orders._count"] == 3  # Acme(2) + Initech(1)
        assert by_region["EU"]["orders._count"] == 1  # Globex(1)
        assert float(by_region["US"]["orders.amount_sum"]) == 450.0  # 100+200+150
        assert float(by_region["EU"]["orders.amount_sum"]) == 50.0


@pytest.mark.integration
class TestDuckDBMedianPercentile:
    """Lock in DuckDB median/percentile so the static-formula refactor
    (moving percentile out of BUILTIN_AGGREGATION_FORMULAS into the
    dialect-aware _build_percentile method) doesn't silently regress.

    DuckDB receives `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)` from
    the generator, which sqlglot transpiles to `QUANTILE_CONT(x, p ORDER BY x)`.
    """

    async def test_median(self, duckdb_env: SlayerQueryEngine) -> None:
        # amounts = [100, 200, 50, 150, 75, 300] -> median 125
        query = SlayerQuery(source_model="orders", measures=[{"formula": "total:median"}])
        result = await duckdb_env.execute(query=query)
        assert float(result.data[0]["orders.total_median"]) == pytest.approx(125.0)

    async def test_percentile_quartiles(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[
                {"formula": "total:percentile(p=0.25)"},
                {"formula": "total:percentile(p=0.75)"},
            ],
        )
        result = await duckdb_env.execute(query=query)
        row = result.data[0]
        assert float(row["orders.total_percentile_p_0_25"]) == pytest.approx(81.25)
        assert float(row["orders.total_percentile_p_0_75"]) == pytest.approx(187.5)

    async def test_median_grouped(self, duckdb_env: SlayerQueryEngine) -> None:
        # completed: [100, 150, 200] -> 150
        # pending:   [50, 300]       -> 175
        # cancelled: [75]            -> 75
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:median"}],
            dimensions=[{"name": "status"}],
        )
        result = await duckdb_env.execute(query=query)
        by_status = {
            r["orders.status"]: float(r["orders.total_median"]) for r in result.data
        }
        assert by_status["completed"] == pytest.approx(150)
        assert by_status["pending"] == pytest.approx(175)
        assert by_status["cancelled"] == pytest.approx(75)


@pytest.mark.integration
class TestDuckDBStatAggregations:
    """DEV-1317 cross-dialect smoke on DuckDB. DuckDB has native
    STDDEV_SAMP / STDDEV_POP / VAR_SAMP / VAR_POP / CORR (via sqlglot
    transpilation, may emit VARIANCE for var_samp). Same expected
    numeric results as Postgres / SQLite.
    """

    async def test_stddev_samp_native_duckdb(self, duckdb_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:stddev_samp"}],
        )
        result = await duckdb_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_stddev_samp"]) == pytest.approx(
            statistics.stdev(amounts), rel=1e-9
        )

    async def test_var_pop_native_duckdb(self, duckdb_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:var_pop"}],
        )
        result = await duckdb_env.execute(query=query)
        amounts = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        assert float(result.data[0]["orders.total_var_pop"]) == pytest.approx(
            statistics.pvariance(amounts), rel=1e-9
        )

    async def test_corr_native_duckdb(self, duckdb_env: SlayerQueryEngine) -> None:
        import statistics
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:corr(other=customer_id)"}],
        )
        result = await duckdb_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        expected = statistics.correlation(xs, ys)
        assert float(result.data[0]["orders.total_corr_other_customer_id"]) == pytest.approx(
            expected, rel=1e-9
        )

    async def test_covar_samp_native_duckdb(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:covar_samp(other=customer_id)"}],
        )
        result = await duckdb_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        n = len(xs)
        mx, my = sum(xs) / n, sum(ys) / n
        expected = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (n - 1)
        assert float(
            result.data[0]["orders.total_covar_samp_other_customer_id"]
        ) == pytest.approx(expected, rel=1e-9)

    async def test_covar_pop_native_duckdb(self, duckdb_env: SlayerQueryEngine) -> None:
        query = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "total:covar_pop(other=customer_id)"}],
        )
        result = await duckdb_env.execute(query=query)
        xs = [100.0, 200.0, 50.0, 150.0, 75.0, 300.0]
        ys = [1.0, 1.0, 2.0, 2.0, 3.0, 3.0]
        n = len(xs)
        mx, my = sum(xs) / n, sum(ys) / n
        expected = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n
        assert float(
            result.data[0]["orders.total_covar_pop_other_customer_id"]
        ) == pytest.approx(expected, rel=1e-9)

    async def test_log10_round_trip_duckdb(self, duckdb_env: SlayerQueryEngine) -> None:
        """DEV-1337: a `log10(amount)` formula must execute correctly on
        DuckDB (native single-arg LOG10) and the emitted SQL must contain
        `log10(...)` rather than the canonicalised `LOG(10, ...)`."""
        from slayer.core.models import SlayerModel

        existing = await duckdb_env.storage.get_model("orders")
        assert existing is not None
        cols = list(existing.columns) + [
            Column(name="log_amount", sql="log10(amount)", type=DataType.NUMBER),
        ]
        await duckdb_env.save_model(
            SlayerModel(
                name=existing.name,
                sql_table=existing.sql_table,
                data_source=existing.data_source,
                columns=cols,
            )
        )

        result = await duckdb_env.execute(
            SlayerQuery(source_model="orders", measures=[{"formula": "log_amount:max"}])
        )
        import math as _math
        # max(amount) = 300, log10(300) ≈ 2.4771
        assert float(result.data[0]["orders.log_amount_max"]) == pytest.approx(
            _math.log10(300.0), rel=1e-9
        )

        dry = await duckdb_env.execute(
            SlayerQuery(source_model="orders", measures=[{"formula": "log_amount:max"}]),
            dry_run=True,
        )
        assert dry.sql is not None
        sql = dry.sql.lower()
        assert "log10(" in sql, (
            f"Expected literal log10(...) in emitted SQL, got:\n{dry.sql}"
        )
        assert "log(10," not in sql.replace(" ", ""), (
            f"Emitted SQL must not canonicalise log10 to LOG(10, ...):\n{dry.sql}"
        )




# ---------------------------------------------------------------------------
# DEV-1336 — window functions in filters, DuckDB parity
# ---------------------------------------------------------------------------


@pytest.fixture
async def planets_duckdb_env(tmp_path):
    """Planets fixture (DuckDB): a Column.sql with `row_number() over (...)`."""
    db_path = tmp_path / "planets.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE planets (
            id INTEGER PRIMARY KEY,
            name VARCHAR NOT NULL,
            mass DECIMAL(10, 4) NOT NULL
        )
        """
    )
    conn.executemany(
        "INSERT INTO planets VALUES (?, ?, ?)",
        [
            (1, "Mercury", 0.33),
            (2, "Venus", 4.87),
            (3, "Earth", 5.97),
            (4, "Mars", 0.642),
            (5, "Jupiter", 1898.0),
            (6, "Saturn", 568.0),
            (7, "Uranus", 86.8),
            (8, "Neptune", 102.0),
        ],
    )
    conn.close()

    tmpdir = tempfile.mkdtemp()
    storage = YAMLStorage(base_dir=tmpdir)

    await storage.save_datasource(
        DatasourceConfig(name="planets_duckdb", type="duckdb", database=str(db_path))
    )
    await storage.save_model(
        SlayerModel(
            name="planets",
            sql_table="planets",
            data_source="planets_duckdb",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="name", sql="name", type=DataType.STRING),
                Column(name="mass", sql="mass", type=DataType.NUMBER),
                Column(
                    name="rn",
                    sql="row_number() over (order by mass desc)",
                    type=DataType.NUMBER,
                ),
            ],
        )
    )
    return SlayerQueryEngine(storage=storage)


@pytest.mark.integration
async def test_filter_on_windowed_column_duckdb_top_n(planets_duckdb_env):
    """DuckDB parity for DEV-1336: filtering on a `Column.sql` window expression
    must execute and return the top-3 rows by mass."""
    engine = planets_duckdb_env
    query = SlayerQuery(
        source_model="planets",
        dimensions=["name"],
        filters=["rn <= 3"],
        order=[OrderItem(column=ColumnRef(name="rn"), direction="asc")],
    )
    response = await engine.execute(query)
    names = [row["planets.name"] for row in response.data]
    assert names == ["Jupiter", "Saturn", "Neptune"], (
        f"Expected top-3 by mass desc, got {names}"
    )


# ---------------------------------------------------------------------------
# DEV-1333: cross-model derived ``Column.sql`` chaining (DuckDB)
# ---------------------------------------------------------------------------


@pytest.fixture
async def duckdb_derived_chain_env(tmp_path):
    db_path = tmp_path / "derived_chain.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE TABLE b_tbl (id INTEGER PRIMARY KEY, foo_raw DOUBLE)")
    conn.execute(
        "CREATE TABLE a_tbl (id INTEGER PRIMARY KEY, bar DOUBLE, b_id INTEGER, raw_a DOUBLE)"
    )
    conn.execute("INSERT INTO b_tbl VALUES (1, 200.0), (2, 50.0)")
    conn.execute("INSERT INTO a_tbl VALUES (10, 4.0, 1, 100.0), (11, 1.0, 2, 5.0)")
    conn.close()

    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()
    storage = YAMLStorage(base_dir=str(storage_dir))
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="duckdb", database=str(db_path))
    )
    from slayer.core.models import ModelJoin
    await storage.save_model(
        SlayerModel(
            name="b_tbl",
            data_source="ds",
            sql_table="b_tbl",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="foo_raw", sql="foo_raw", type=DataType.NUMBER),
                Column(name="foo_normalized", sql="foo_raw / 100.0", type=DataType.NUMBER),
            ],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="a_tbl",
            data_source="ds",
            sql_table="a_tbl",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="bar", sql="bar", type=DataType.NUMBER),
                Column(name="b_id", sql="b_id", type=DataType.NUMBER),
                Column(
                    name="ratio_using_derived",
                    sql="a_tbl.bar / b_tbl.foo_normalized",
                    type=DataType.NUMBER,
                ),
            ],
            joins=[ModelJoin(target_model="b_tbl", join_pairs=[["b_id", "id"]])],
        )
    )
    return SlayerQueryEngine(storage=storage)


@pytest.mark.integration
async def test_integration_duckdb_cross_model_derived_columnsql(
    duckdb_derived_chain_env: SlayerQueryEngine,
) -> None:
    response = await duckdb_derived_chain_env.execute(
        SlayerQuery(
            source_model="a_tbl",
            dimensions=[
                ColumnRef(name="id"),
                ColumnRef(name="ratio_using_derived"),
            ],
            order=[OrderItem(column=ColumnRef(name="id"), direction="asc")],
        )
    )
    assert response.row_count == 2
    assert float(response.data[0]["a_tbl.ratio_using_derived"]) == pytest.approx(2.0)
    assert float(response.data[1]["a_tbl.ratio_using_derived"]) == pytest.approx(2.0)
