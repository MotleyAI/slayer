"""Integration tests for NamedQuery: save (with dry-run validation), inspect,
run, variable precedence, and bidirectional collision rejection — exercised
end-to-end against an in-process DuckDB database."""

import pytest

pytest.importorskip("duckdb")

import duckdb

from slayer.core.enums import DataType
from slayer.core.models import (
    DatasourceConfig,
    Dimension,
    Measure,
    NamedQuery,
    SlayerModel,
)
from slayer.core.named_query_ops import save_named_query
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
async def duckdb_env(tmp_path):
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            status VARCHAR NOT NULL,
            amount DECIMAL(10,2) NOT NULL,
            created_at TIMESTAMP NOT NULL
        )
    """)
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?)",
        [
            (1, "completed", 100, "2024-01-15"),
            (2, "completed", 200, "2024-02-15"),
            (3, "pending", 50, "2024-03-15"),
            (4, "completed", 150, "2024-04-15"),
        ],
    )
    conn.close()

    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(DatasourceConfig(
        name="duck", type="duckdb", database=str(db_path),
    ))
    await storage.save_model(SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="duck",
        dimensions=[
            Dimension(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="status", sql="status", type=DataType.STRING),
            Dimension(name="amount", sql="amount", type=DataType.NUMBER),
            Dimension(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ],
        measures=[Measure(name="total", sql="amount")],
    ))
    engine = SlayerQueryEngine(storage=storage)
    return engine, storage


@pytest.mark.integration
class TestNamedQueryEndToEnd:
    async def test_save_and_run_simple_two_stage(self, duckdb_env) -> None:
        engine, storage = duckdb_env
        named = NamedQuery(
            name="completed_total_then_avg",
            description="Total of completed orders then average per status.",
            stages=[
                SlayerQuery(
                    name="completed_only",
                    source_model="orders",
                    filters=["status == 'completed'"],
                    fields=[{"formula": "total:sum"}],
                    dimensions=[{"name": "status"}],
                ),
                SlayerQuery(
                    source_model="completed_only",
                    fields=[{"formula": "total_sum:avg"}],
                ),
            ],
        )
        await save_named_query(named, storage=storage, engine=engine)

        # Run by name
        result = await engine.execute(query="completed_total_then_avg")
        assert result.row_count == 1
        # Three completed orders: 100 + 200 + 150 = 450; avg = 450 (single row)
        avg_value = float(list(result.data[0].values())[0])
        assert abs(avg_value - 450.0) < 1e-6

    async def test_save_validates_via_dry_run_with_unresolved_variables(
        self, duckdb_env
    ) -> None:
        """A NamedQuery whose filter references an unsupplied {var} must save
        successfully — placeholder ``0`` is auto-substituted at validation."""
        engine, storage = duckdb_env
        named = NamedQuery(
            name="parameterised",
            stages=[
                SlayerQuery(
                    source_model="orders",
                    fields=[{"formula": "*:count"}],
                    filters=["amount > {min_amount}"],
                ),
            ],
        )
        # No variables anywhere; save must still succeed.
        await save_named_query(named, storage=storage, engine=engine)

        # Run with explicit value
        result = await engine.execute(
            query="parameterised", variables={"min_amount": 100}
        )
        assert int(list(result.data[0].values())[0]) == 2  # 200, 150

        # Run with a different value
        result2 = await engine.execute(
            query="parameterised", variables={"min_amount": 0}
        )
        assert int(list(result2.data[0].values())[0]) == 4

    async def test_save_rejects_invalid_query(self, duckdb_env) -> None:
        """A NamedQuery whose source_model is not resolvable must fail save."""
        engine, storage = duckdb_env
        bad = NamedQuery(
            name="invalid",
            stages=[
                SlayerQuery(
                    source_model="model_that_does_not_exist",
                    fields=[{"formula": "*:count"}],
                ),
            ],
        )
        with pytest.raises(ValueError):
            await save_named_query(bad, storage=storage, engine=engine)
        # And nothing was persisted
        assert await storage.get_query("invalid") is None

    async def test_variable_precedence_stage_overrides_runtime_overrides_top(
        self, duckdb_env
    ) -> None:
        """Stage variables > runtime > NamedQuery.variables."""
        engine, storage = duckdb_env

        # Top-level says 0; runtime overrides to 100. Stage doesn't set it,
        # so runtime wins.
        nq = NamedQuery(
            name="precedence_test",
            variables={"min_amount": 0},
            stages=[
                SlayerQuery(
                    source_model="orders",
                    fields=[{"formula": "*:count"}],
                    filters=["amount > {min_amount}"],
                ),
            ],
        )
        await save_named_query(nq, storage=storage, engine=engine)

        # No runtime → top-level (0) wins → 4 rows
        r1 = await engine.execute(query="precedence_test")
        assert int(list(r1.data[0].values())[0]) == 4

        # Runtime overrides top-level → 100 → 2 rows
        r2 = await engine.execute(
            query="precedence_test", variables={"min_amount": 100}
        )
        assert int(list(r2.data[0].values())[0]) == 2

        # Stage variable trumps runtime: re-save with stage.variables set
        nq2 = NamedQuery(
            name="precedence_test_2",
            variables={"min_amount": 0},
            stages=[
                SlayerQuery(
                    source_model="orders",
                    fields=[{"formula": "*:count"}],
                    filters=["amount > {min_amount}"],
                    variables={"min_amount": 100},  # stage wins
                ),
            ],
        )
        await save_named_query(nq2, storage=storage, engine=engine)
        # Even with runtime=0, stage's 100 wins → 2 rows
        r3 = await engine.execute(query="precedence_test_2", variables={"min_amount": 0})
        assert int(list(r3.data[0].values())[0]) == 2

    async def test_bidirectional_collision_rejected(self, duckdb_env) -> None:
        engine, storage = duckdb_env
        # 'orders' is already a model from the fixture; saving a query named
        # 'orders' must be rejected.
        named = NamedQuery(
            name="orders",
            stages=[SlayerQuery(source_model="orders")],
        )
        with pytest.raises(ValueError, match="already exists|collide"):
            await storage.save_query(named)

        # And vice-versa: save a query, then try to save a model with the
        # same name.
        ok = NamedQuery(
            name="my_query",
            stages=[
                SlayerQuery(
                    source_model="orders",
                    fields=[{"formula": "*:count"}],
                ),
            ],
        )
        await save_named_query(ok, storage=storage, engine=engine)
        colliding = SlayerModel(
            name="my_query",
            sql_table="x",
            data_source="duck",
            dimensions=[Dimension(name="x", sql="x", type=DataType.NUMBER)],
        )
        with pytest.raises(ValueError, match="already exists|collide"):
            await storage.save_model(colliding)

    async def test_engine_execute_string_loads_named_query(self, duckdb_env) -> None:
        engine, storage = duckdb_env
        nq = NamedQuery(
            name="simple_count",
            stages=[
                SlayerQuery(
                    source_model="orders",
                    fields=[{"formula": "*:count"}],
                ),
            ],
        )
        await save_named_query(nq, storage=storage, engine=engine)
        # Pass the *name* — engine resolves it to the saved query.
        result = await engine.execute(query="simple_count")
        assert int(list(result.data[0].values())[0]) == 4

    async def test_engine_execute_unknown_name_raises(self, duckdb_env) -> None:
        engine, _ = duckdb_env
        with pytest.raises(ValueError, match="not found"):
            await engine.execute(query="does_not_exist_anywhere")
