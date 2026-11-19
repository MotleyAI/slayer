"""Tests for ``SchemaDriftError`` query-time wrap.

When ``engine.execute()`` raises a DBAPI error, the engine attempts to
attribute it to schema drift via ``validate_models``. If drift is found in
the touched models, the original error is wrapped in ``SchemaDriftError``
(with ``original`` as ``__cause__``); otherwise the original error
re-raises untouched.
"""

from __future__ import annotations

import contextlib
import tempfile
from collections.abc import AsyncGenerator
from pathlib import Path
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import SQLAlchemyError

from slayer.core.enums import DataType
from slayer.core.errors import SchemaDriftError
from slayer.core.models import (
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.schema_drift import (
    IntrospectionUnavailable,
    WholeDatasetDelete,
    _collect_sql_table_diffs,
    _live_schema_for_datasource,
)
from slayer.database.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage
from slayer.storage.sqlite_conn import transaction
from tests._engine_helpers import disposable_engine


@contextlib.asynccontextmanager
async def _client(ds: DatasourceConfig) -> AsyncGenerator[SlayerSQLClient, None]:
    client = SlayerSQLClient(datasource=ds)
    try:
        yield client
    finally:
        await client.aclose()
        client.close()


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


async def _setup(workspace: Path) -> tuple[SlayerQueryEngine, str]:
    db_path = str(workspace / "live.db")
    with transaction(db_path) as conn:
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

    storage = YAMLStorage(base_dir=str(workspace / "storage"))
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=db_path)
    )
    await storage.save_dataset(
        SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
            ],
        )
    )
    await storage.save_dataset(
        SlayerDataset(
            name="orders",
            sql_table="orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(
                    name="customer_id", sql="customer_id", datatype=DataType.FLOAT
                ),
            ],
            relationships=[
                DatasetRelationship(
                    to="customers",
                    from_columns=["customer_id"], to_columns=["id"],
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
        with transaction(db_path) as conn:
            conn.execute("DROP TABLE orders")

        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(amount)", "alias": "total"}],
        })
        with pytest.raises(SchemaDriftError) as exc:
            await engine.execute(q)
        # Original error preserved as __cause__
        assert exc.value.__cause__ is not None
        # Touched-models set covers orders
        assert "orders" in exc.value.datasets
        # to_delete includes a WholeModelDelete on orders
        assert any(
            isinstance(e, WholeDatasetDelete) and e.dataset_name == "orders"
            for e in exc.value.to_delete
        )

    async def test_dbapi_error_without_drift_re_raises_original(
        self, workspace: Path
    ) -> None:
        """A model whose own SQL is broken (source tables and columns intact)
        is not drift — the original DBAPI error must reach the caller."""
        engine, _ = await _setup(workspace)
        broken_model = SlayerDataset(
            name="broken",
            sql="SELECT id, NO_SUCH_FUNCTION(amount) AS amount FROM orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        )
        await engine.storage.save_dataset(broken_model)

        q = SlayerQuery.model_validate({
            "population": "broken",
            "metrics": [{"metric": "sum(amount)", "alias": "total"}],
        })
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
        await engine.storage.save_dataset(
            SlayerDataset(
                name="broken",
                sql="SELECT id, NO_SUCH_FUNCTION(amount) AS amount FROM orders",
                data_source="ds",
                fields=[
                    DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                    DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                ],
            )
        )
        entries = await engine.validate_datasets(data_source="ds")
        broken = [e for e in entries if e.dataset_name == "broken"]
        assert len(broken) == 1
        assert isinstance(broken[0], WholeDatasetDelete)
        assert broken[0].cause == "invalid_sql"
        assert "source tables and referenced columns exist" in broken[0].reasons[0].reason

    async def test_table_less_broken_sql_reported_as_invalid_sql(
        self, workspace: Path
    ) -> None:
        """No source tables ⇒ nothing can have drifted ⇒ invalid_sql."""
        engine, _ = await _setup(workspace)
        await engine.storage.save_dataset(
            SlayerDataset(
                name="broken",
                sql="SELECT NO_SUCH_FUNCTION(1) AS x",
                data_source="ds",
                fields=[
                    DatasetField(name="x", sql="x", datatype=DataType.FLOAT, primary_key=True),
                ],
            )
        )
        entries = await engine.validate_datasets(data_source="ds")
        broken = [e for e in entries if e.dataset_name == "broken"]
        assert len(broken) == 1
        assert isinstance(broken[0], WholeDatasetDelete)
        assert broken[0].cause == "invalid_sql"

    async def test_sql_model_on_dropped_column_still_wraps_as_drift(
        self, workspace: Path
    ) -> None:
        """A dropped source COLUMN (table intact) is real drift, not invalid SQL."""
        engine, db_path = await _setup(workspace)
        await engine.storage.save_dataset(
            SlayerDataset(
                name="orders_view",
                sql="SELECT id, amount FROM orders",
                data_source="ds",
                fields=[
                    DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                    DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                ],
            )
        )
        with transaction(db_path) as conn:
            conn.execute("ALTER TABLE orders DROP COLUMN amount")

        q = SlayerQuery.model_validate({
            "population": "orders_view",
            "metrics": [{"metric": "sum(amount)", "alias": "total"}],
        })
        with pytest.raises(SchemaDriftError) as exc:
            await engine.execute(q)
        assert "orders_view" in exc.value.datasets

    async def test_dropped_using_join_key_still_wraps_as_drift(
        self, workspace: Path
    ) -> None:
        """A dropped ``JOIN .. USING`` key is real drift, not invalid SQL."""
        engine, db_path = await _setup(workspace)
        with transaction(db_path) as conn:
            conn.execute("ALTER TABLE orders ADD COLUMN batch_tag TEXT")
            conn.execute("ALTER TABLE customers ADD COLUMN batch_tag TEXT")
        await engine.storage.save_dataset(
            SlayerDataset(
                name="orders_by_batch",
                sql=(
                    "SELECT o.id, o.amount FROM orders AS o "
                    "JOIN customers AS c USING (batch_tag)"
                ),
                data_source="ds",
                fields=[
                    DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                    DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                ],
            )
        )
        with transaction(db_path) as conn:
            conn.execute("ALTER TABLE orders DROP COLUMN batch_tag")

        q = SlayerQuery.model_validate({
            "population": "orders_by_batch",
            "metrics": [{"metric": "sum(amount)", "alias": "total"}],
        })
        with pytest.raises(SchemaDriftError) as exc:
            await engine.execute(q)
        assert "orders_by_batch" in exc.value.datasets

    async def test_dropped_column_behind_star_cte_still_wraps_as_drift(
        self, workspace: Path
    ) -> None:
        """``SELECT *`` in a CTE hides the physical column from the probe, so
        the failure must stay classified as drift."""
        engine, db_path = await _setup(workspace)
        await engine.storage.save_dataset(
            SlayerDataset(
                name="orders_cte",
                sql=(
                    "WITH src AS (SELECT * FROM orders) "
                    "SELECT src.id, src.amount FROM src"
                ),
                data_source="ds",
                fields=[
                    DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                    DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                ],
            )
        )
        with transaction(db_path) as conn:
            conn.execute("ALTER TABLE orders DROP COLUMN amount")

        q = SlayerQuery.model_validate({
            "population": "orders_cte",
            "metrics": [{"metric": "sum(amount)", "alias": "total"}],
        })
        with pytest.raises(SchemaDriftError) as exc:
            await engine.execute(q)
        assert "orders_cte" in exc.value.datasets

    async def test_sql_model_on_dropped_table_still_wraps_as_drift(
        self, workspace: Path
    ) -> None:
        engine, db_path = await _setup(workspace)
        await engine.storage.save_dataset(
            SlayerDataset(
                name="orders_view",
                sql="SELECT id, amount FROM orders",
                data_source="ds",
                fields=[
                    DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                    DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                ],
            )
        )
        with transaction(db_path) as conn:
            conn.execute("DROP TABLE orders")

        q = SlayerQuery.model_validate({
            "population": "orders_view",
            "metrics": [{"metric": "sum(amount)", "alias": "total"}],
        })
        with pytest.raises(SchemaDriftError) as exc:
            await engine.execute(q)
        assert "orders_view" in exc.value.datasets
        # The message carries the underlying DB error for the caller.
        assert "Original error:" in str(exc.value)

    async def test_validate_models_attribution_failure_re_raises_original(
        self, workspace: Path
    ) -> None:
        engine, db_path = await _setup(workspace)
        with transaction(db_path) as conn:
            conn.execute("DROP TABLE orders")

        async def _boom(*args, **kwargs):
            raise RuntimeError("validate_models exploded")

        with patch.object(engine, "_validate_read_set", side_effect=_boom):
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)", "alias": "total"}],
            })
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
            engine, "validate_datasets", side_effect=AssertionError("called!")
        ) as mock, patch.object(
            engine, "_validate_read_set", side_effect=AssertionError("called!")
        ) as scoped:
            q = SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)", "alias": "total"}],
            })
            resp = await engine.execute(q)
            assert resp.data is not None
            mock.assert_not_called()
            scoped.assert_not_called()


class TestModelsTouchedComputation:
    async def test_touched_includes_join_targets(self, workspace: Path) -> None:
        engine, db_path = await _setup(workspace)
        # Drop customers; orders query joins to customers, so customers must
        # appear in the touched-models set even though the query syntactically
        # only names "orders" as source.
        with transaction(db_path) as conn:
            conn.execute("DROP TABLE customers")

        q = SlayerQuery.model_validate({
            "population": "orders",
            "metrics": [{"metric": "sum(amount)", "alias": "total"}],
            "dimensions": ["customers.region"],
        })
        with pytest.raises(SchemaDriftError) as exc:
            await engine.execute(q)
        # Both source_model and the join target must be reported as touched.
        assert "orders" in exc.value.datasets
        assert "customers" in exc.value.datasets


class TestIntrospectionUnavailable:
    """A datasource whose every table fails to introspect is 'unknown', not
    'everything was dropped' — otherwise ``--force-clean`` deletes a tenant."""

    def _ds_with_one_table(self, tmpdir: str) -> DatasourceConfig:
        db_path = str(Path(tmpdir) / "live.db")
        with disposable_engine(f"sqlite:///{db_path}") as engine:
            with engine.connect() as c:
                c.execute(sa.text("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
                c.commit()
        return DatasourceConfig(name="live", type="sqlite", database=db_path)

    async def test_live_schema_raises_when_every_table_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ds = self._ds_with_one_table(tmpdir)
            async with _client(ds) as client:
                with patch(
                    "slayer.live_schema.schema_drift._introspect_one_table",
                    side_effect=PermissionError("403 Access Denied"),
                ), pytest.raises(IntrospectionUnavailable):
                    await _live_schema_for_datasource(client=client, datasource=ds)

    async def test_drift_verdict_skipped_when_introspection_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            ds = self._ds_with_one_table(tmpdir)
            model = SlayerDataset(
                name="t", sql_table="t", data_source="live",
                fields=[DatasetField(name="id", sql="id", datatype=DataType.INTEGER)],
            )
            async with _client(ds) as client:
                with patch(
                    "slayer.live_schema.schema_drift._introspect_one_table",
                    side_effect=PermissionError("403 Access Denied"),
                ):
                    diffs = await _collect_sql_table_diffs(
                        datasource=ds,
                        client=client,
                        sql_table_datasets=[model],
                        available_in_ds={"t"},
                    )
            assert diffs == {}, f"must not recommend deletes: {diffs}"
