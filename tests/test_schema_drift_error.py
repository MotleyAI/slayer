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
import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError

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
from slayer.engine.schema_drift import (
    IntrospectionUnavailable,
    WholeModelDelete,
    _collect_sql_table_diffs,
    _live_schema_for_datasource,
)
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
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
            ],
        )
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(
                    name="customer_id", sql="customer_id", type=DataType.DOUBLE
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
        """A model whose own SQL is broken (source tables and columns intact)
        is not drift — the original DBAPI error must reach the caller."""
        engine, _ = await _setup(workspace)
        broken_model = SlayerModel(
            name="broken",
            sql="SELECT id, NO_SUCH_FUNCTION(amount) AS amount FROM orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        await engine.storage.save_model(broken_model)

        q = SlayerQuery(
            source_model="broken",
            measures=[{"formula": "amount:sum", "name": "total"}],
        )
        # Original SQLAlchemy/DBAPI error should bubble up — NOT
        # SchemaDriftError. Assert on the original error class so a
        # regression that re-wraps as something else still fails here.
        with pytest.raises(SQLAlchemyError) as exc:
            await engine.execute(q)
        assert not isinstance(exc.value, SchemaDriftError)
        assert "no_such_function" in str(exc.value).lower()

    async def test_broken_sql_model_reported_as_invalid_sql(
        self, workspace: Path
    ) -> None:
        engine, _ = await _setup(workspace)
        await engine.storage.save_model(
            SlayerModel(
                name="broken",
                sql="SELECT id, NO_SUCH_FUNCTION(amount) AS amount FROM orders",
                data_source="ds",
                columns=[
                    Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                    Column(name="amount", sql="amount", type=DataType.DOUBLE),
                ],
            )
        )
        entries = await engine.validate_models(data_source="ds")
        broken = [e for e in entries if e.model_name == "broken"]
        assert len(broken) == 1
        assert isinstance(broken[0], WholeModelDelete)
        assert broken[0].cause == "invalid_sql"
        assert "source tables and referenced columns exist" in broken[0].reasons[0].reason

    async def test_sql_model_on_dropped_column_still_wraps_as_drift(
        self, workspace: Path
    ) -> None:
        """A dropped source COLUMN (table intact) is real drift, not invalid SQL."""
        engine, db_path = await _setup(workspace)
        await engine.storage.save_model(
            SlayerModel(
                name="orders_view",
                sql="SELECT id, amount FROM orders",
                data_source="ds",
                columns=[
                    Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                    Column(name="amount", sql="amount", type=DataType.DOUBLE),
                ],
            )
        )
        conn = sqlite3.connect(db_path)
        conn.execute("ALTER TABLE orders DROP COLUMN amount")
        conn.commit()
        conn.close()

        q = SlayerQuery(
            source_model="orders_view",
            measures=[{"formula": "amount:sum", "name": "total"}],
        )
        with pytest.raises(SchemaDriftError) as exc:
            await engine.execute(q)
        assert "orders_view" in exc.value.models

    async def test_sql_model_on_dropped_table_still_wraps_as_drift(
        self, workspace: Path
    ) -> None:
        engine, db_path = await _setup(workspace)
        await engine.storage.save_model(
            SlayerModel(
                name="orders_view",
                sql="SELECT id, amount FROM orders",
                data_source="ds",
                columns=[
                    Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                    Column(name="amount", sql="amount", type=DataType.DOUBLE),
                ],
            )
        )
        conn = sqlite3.connect(db_path)
        conn.execute("DROP TABLE orders")
        conn.commit()
        conn.close()

        q = SlayerQuery(
            source_model="orders_view",
            measures=[{"formula": "amount:sum", "name": "total"}],
        )
        with pytest.raises(SchemaDriftError) as exc:
            await engine.execute(q)
        assert "orders_view" in exc.value.models
        # The message carries the underlying DB error for the caller.
        assert "Original error:" in str(exc.value)

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

        from sqlalchemy.exc import SQLAlchemyError

        with patch.object(engine, "validate_models", side_effect=_boom):
            q = SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum", "name": "total"}],
            )
            # The original DBAPI error should bubble up — NOT SchemaDriftError
            # and NOT the RuntimeError from the failing attribution.
            with pytest.raises(SQLAlchemyError) as exc:
                await engine.execute(q)
            assert not isinstance(exc.value, SchemaDriftError)
            assert "exploded" not in str(exc.value)
            assert "orders" in str(exc.value).lower()


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


class TestIntrospectionUnavailable:
    """A datasource whose every table fails to introspect is 'unknown', not
    'everything was dropped' — otherwise ``--force-clean`` deletes a tenant."""

    def _ds_with_one_table(self, tmpdir: str) -> DatasourceConfig:
        db_path = str(Path(tmpdir) / "live.db")
        engine = sa.create_engine(f"sqlite:///{db_path}")
        try:
            with engine.connect() as c:
                c.execute(sa.text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
                c.commit()
        finally:
            engine.dispose()
        return DatasourceConfig(name="live", type="sqlite", database=db_path)

    def test_live_schema_raises_when_every_table_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ds = self._ds_with_one_table(tmpdir)
            with patch(
                "slayer.engine.schema_drift._introspect_one_table",
                side_effect=PermissionError("403 Access Denied"),
            ), pytest.raises(IntrospectionUnavailable):
                _live_schema_for_datasource(datasource=ds)

    async def test_drift_verdict_skipped_when_introspection_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ds = self._ds_with_one_table(tmpdir)
            model = SlayerModel(
                name="t", sql_table="t", data_source="live",
                columns=[Column(name="id", sql="id", type=DataType.INT)],
            )
            with patch(
                "slayer.engine.schema_drift._introspect_one_table",
                side_effect=PermissionError("403 Access Denied"),
            ):
                diffs = await _collect_sql_table_diffs(
                    datasource=ds,
                    sql_table_models=[model],
                    available_in_ds={"t"},
                )
            assert diffs == {}, f"must not recommend deletes: {diffs}"
