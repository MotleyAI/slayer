"""Tests for ``SchemaDriftError`` query-time wrap. See DEV-1356.

When ``engine.execute()`` raises a DBAPI error, the engine attempts to
attribute it to schema drift via ``validate_models``. If drift is found in
the touched models, the original error is wrapped in ``SchemaDriftError``
(with ``original`` as ``__cause__``); otherwise the original error
re-raises untouched.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import SchemaDriftError
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import WholeModelDelete
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


async def _setup(workspace: Path) -> tuple[SlayerQueryEngine, str]:
    db_path = str(workspace / "live.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT NOT NULL);
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            amount REAL NOT NULL,
            customer_id INTEGER REFERENCES customers(id)
        );
        INSERT INTO customers VALUES (1, 'US');
        INSERT INTO orders VALUES (1, 100.0, 1);
        """
    )
    conn.commit()
    conn.close()

    storage = YAMLStorage(base_dir=str(workspace / "storage"))
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=db_path)
    )
    await storage.save_model(
        SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="region", sql="region", type=DataType.STRING),
            ],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.NUMBER),
                Column(
                    name="customer_id", sql="customer_id", type=DataType.NUMBER
                ),
            ],
            joins=[
                ModelJoin(
                    target_model="customers",
                    join_pairs=[["customer_id", "id"]],
                ),
            ],
        )
    )
    return SlayerQueryEngine(storage=storage), db_path


# ---------------------------------------------------------------------------
# Wrap behaviour
# ---------------------------------------------------------------------------


class TestSchemaDriftErrorWrap:
    async def test_wraps_dbapi_error_when_drift_detected(
        self, workspace: Path
    ) -> None:
        engine, db_path = await _setup(workspace)
        # Drop orders externally — query against it will fail.
        conn = sqlite3.connect(db_path)
        conn.execute("DROP TABLE orders")
        conn.commit()
        conn.close()

        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum", "name": "total"}],
        )
        with pytest.raises(SchemaDriftError) as exc:
            await engine.execute(q)
        # Original error preserved as __cause__
        assert exc.value.__cause__ is not None
        # Touched-models set covers orders
        assert "orders" in exc.value.models
        # to_delete includes a WholeModelDelete on orders
        assert any(
            isinstance(e, WholeModelDelete) and e.model_name == "orders"
            for e in exc.value.to_delete
        )

    async def test_dbapi_error_without_drift_re_raises_original(
        self, workspace: Path
    ) -> None:
        engine, _ = await _setup(workspace)
        # Force a SQL syntax error that's NOT schema drift by injecting
        # invalid SQL into a model — but our control here is to query a
        # nonexistent column via a synthetic enriched failure. Easiest:
        # patch validate_models to return no drift, then invoke a query
        # that fails for a non-drift reason (use a hand-crafted broken
        # column expression).
        broken_model = SlayerModel(
            name="broken",
            sql="SELECT id, this_col_does_not_exist AS amount FROM orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.NUMBER, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.NUMBER),
            ],
        )
        await engine.storage.save_model(broken_model)

        # Force validate_models to attribute no drift to the broken model.
        async def _no_drift(*args, **kwargs):
            return []

        with patch.object(engine, "validate_models", side_effect=_no_drift):
            q = SlayerQuery(
                source_model="broken",
                measures=[{"formula": "amount:sum", "name": "total"}],
            )
            # Original error type should bubble up — NOT SchemaDriftError.
            with pytest.raises(Exception) as exc:
                await engine.execute(q)
            assert not isinstance(exc.value, SchemaDriftError)

    async def test_validate_models_attribution_failure_re_raises_original(
        self, workspace: Path
    ) -> None:
        engine, db_path = await _setup(workspace)
        conn = sqlite3.connect(db_path)
        conn.execute("DROP TABLE orders")
        conn.commit()
        conn.close()

        async def _boom(*args, **kwargs):
            raise RuntimeError("validate_models exploded")

        with patch.object(engine, "validate_models", side_effect=_boom):
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum", "name": "total"}],
            )
            with pytest.raises(Exception) as exc:
                await engine.execute(q)
            # Should NOT be SchemaDriftError nor RuntimeError("validate_models
            # exploded"); should be the original DBAPI error.
            assert not isinstance(exc.value, SchemaDriftError)
            assert "exploded" not in str(exc.value)


class TestHealthyPathNoOverhead:
    async def test_healthy_query_does_not_call_validate_models(
        self, workspace: Path
    ) -> None:
        engine, _ = await _setup(workspace)
        with patch.object(
            engine, "validate_models", side_effect=AssertionError("called!")
        ) as mock:
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum", "name": "total"}],
            )
            resp = await engine.execute(q)
            assert resp.data is not None
            mock.assert_not_called()


class TestModelsTouchedComputation:
    async def test_touched_includes_join_targets(self, workspace: Path) -> None:
        engine, db_path = await _setup(workspace)
        # Drop customers; orders query joins to customers, so customers must
        # appear in the touched-models set even though the query syntactically
        # only names "orders" as source.
        conn = sqlite3.connect(db_path)
        conn.execute("DROP TABLE customers")
        conn.commit()
        conn.close()

        q = SlayerQuery(
            source_model="orders",
            measures=[{"formula": "amount:sum", "name": "total"}],
            dimensions=["customers.region"],
        )
        with pytest.raises(SchemaDriftError) as exc:
            await engine.execute(q)
        # Both source_model and the join target must be reported as touched.
        assert "orders" in exc.value.models
        assert "customers" in exc.value.models
