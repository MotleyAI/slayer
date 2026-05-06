"""Tests for ``validate_models`` — read-only diff between persisted SlayerModels
and a live database schema. See DEV-1356.

Two layers are tested here:

* Pure functions in ``slayer.engine.schema_drift`` — exercise each diff /
  cascade rule against synthetic ``LiveTable`` fixtures, no DB involved.
* ``SlayerQueryEngine.validate_models()`` — end-to-end against a real SQLite
  database in a tempfile, verifying the orchestrator wires up live
  introspection, multi-DS gather, and the read-only contract (storage is
  never mutated).

Integration coverage on DuckDB lives in
``tests/integration/test_schema_drift_duckdb.py``.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from typing import Dict, List, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import (
    EditModelDelete,
    LiveTable,
    RemoveSpec,
    WholeModelDelete,
    compute_datasource_drops,
    data_type_bucket,
    diff_sql_model,
    diff_sql_table_model,
)
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _orders_model(
    *,
    data_source: str = "ds",
    columns: Optional[List[Column]] = None,
    joins: Optional[List[ModelJoin]] = None,
    measures: Optional[List[ModelMeasure]] = None,
    filters: Optional[List[str]] = None,
    sql_table: str = "orders",
) -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table=sql_table,
        data_source=data_source,
        columns=columns
        or [
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
        ],
        joins=joins or [],
        measures=measures or [],
        filters=filters or [],
    )


def _live_orders(
    *,
    columns: Optional[Dict[str, DataType]] = None,
    pk_columns: Optional[set[str]] = None,
) -> LiveTable:
    return LiveTable(
        columns=columns
        or {
            "id": DataType.DOUBLE,
            "amount": DataType.DOUBLE,
            "status": DataType.TEXT,
            "customer_id": DataType.DOUBLE,
        },
        pk_columns=pk_columns or {"id"},
    )


def _entry_for(model_name: str, entries: List) -> Optional[object]:
    for e in entries:
        if e.model_name == model_name:
            return e
    return None


# ---------------------------------------------------------------------------
# data_type_bucket
# ---------------------------------------------------------------------------


class TestDataTypeBucket:
    def test_number_string_boolean_temporal(self) -> None:
        # DEV-1361: DataType.DOUBLE → DOUBLE, DataType.TEXT → TEXT.
        assert data_type_bucket(DataType.DOUBLE) == "number"
        assert data_type_bucket(DataType.TEXT) == "string"
        assert data_type_bucket(DataType.BOOLEAN) == "boolean"
        # DATE and TIMESTAMP collapse to "temporal"
        assert data_type_bucket(DataType.DATE) == data_type_bucket(DataType.TIMESTAMP)
        assert data_type_bucket(DataType.DATE) == "temporal"

    def test_number_and_string_are_different_buckets(self) -> None:
        assert data_type_bucket(DataType.DOUBLE) != data_type_bucket(DataType.TEXT)

    def test_int_and_double_share_number_bucket(self) -> None:
        # DEV-1361: critical invariance — drift detection must NOT flag a
        # persisted DOUBLE column against an INT live column (or vice versa)
        # even though they're now distinct enum members.
        assert data_type_bucket(DataType.INT) == "number"
        assert data_type_bucket(DataType.DOUBLE) == "number"
        assert data_type_bucket(DataType.INT) == data_type_bucket(DataType.DOUBLE)


# ---------------------------------------------------------------------------
# diff_sql_table_model — sql_table mode
# ---------------------------------------------------------------------------


class TestDiffSqlTableModel:
    def test_no_drift_when_live_matches(self) -> None:
        model = _orders_model()
        live = _live_orders()
        entry, dropped = diff_sql_table_model(
            model=model, live_table=live, available_models_in_ds={"orders"}
        )
        assert entry is None
        assert dropped == set()

    def test_persisted_column_missing_from_live(self) -> None:
        model = _orders_model()
        live = _live_orders(
            columns={
                "id": DataType.DOUBLE,
                "amount": DataType.DOUBLE,
                # status missing
                "customer_id": DataType.DOUBLE,
            }
        )
        entry, dropped = diff_sql_table_model(
            model=model, live_table=live, available_models_in_ds={"orders"}
        )
        assert isinstance(entry, EditModelDelete)
        assert entry.model_name == "orders"
        assert entry.remove.columns == ["status"]
        assert dropped == {"status"}
        assert any(r.target == "column:status" for r in entry.reasons)

    def test_live_table_missing(self) -> None:
        model = _orders_model()
        entry, dropped = diff_sql_table_model(
            model=model, live_table=None, available_models_in_ds={"orders"}
        )
        assert isinstance(entry, WholeModelDelete)
        assert entry.model_name == "orders"
        assert dropped == {c.name for c in model.columns}
        assert any("missing" in r.reason.lower() or "not found" in r.reason.lower()
                   for r in entry.reasons)

    def test_type_bucket_mismatch_drops_column(self) -> None:
        # Persisted "amount" is NUMBER, live made it STRING.
        model = _orders_model()
        live = _live_orders(
            columns={
                "id": DataType.DOUBLE,
                "amount": DataType.TEXT,  # bucket flip
                "status": DataType.TEXT,
                "customer_id": DataType.DOUBLE,
            }
        )
        entry, dropped = diff_sql_table_model(
            model=model, live_table=live, available_models_in_ds={"orders"}
        )
        assert isinstance(entry, EditModelDelete)
        assert "amount" in entry.remove.columns
        assert dropped == {"amount"}

    def test_integer_vs_float_same_bucket(self) -> None:
        # DEV-1361: INT and DOUBLE are now distinct enum members but share
        # the ``"number"`` bucket so drift detection does not false-positive
        # when a persisted DOUBLE column is reported as INT by live
        # introspection (the v5 refinement step reconciles these without
        # raising drift).
        model = _orders_model()
        live = _live_orders(
            columns={
                "id": DataType.INT,
                "amount": DataType.DOUBLE,
                "status": DataType.TEXT,
                "customer_id": DataType.INT,
            }
        )
        entry, _ = diff_sql_table_model(
            model=model, live_table=live, available_models_in_ds={"orders"}
        )
        assert entry is None

    def test_date_and_timestamp_share_bucket(self) -> None:
        model = SlayerModel(
            name="events",
            sql_table="events",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="happened_at", sql="happened_at", type=DataType.DATE),
            ],
        )
        # Live reports TIMESTAMP — same bucket as DATE.
        live = LiveTable(
            columns={"id": DataType.DOUBLE, "happened_at": DataType.TIMESTAMP},
            pk_columns={"id"},
        )
        entry, _ = diff_sql_table_model(
            model=model, live_table=live, available_models_in_ds={"events"}
        )
        assert entry is None

    def test_join_local_column_missing_drops_join(self) -> None:
        model = _orders_model(
            joins=[
                ModelJoin(
                    target_model="customers",
                    join_pairs=[["customer_id", "id"]],
                ),
            ],
        )
        # Live drops customer_id
        live = _live_orders(
            columns={
                "id": DataType.DOUBLE,
                "amount": DataType.DOUBLE,
                "status": DataType.TEXT,
            }
        )
        entry, _ = diff_sql_table_model(
            model=model, live_table=live, available_models_in_ds={"orders", "customers"}
        )
        assert isinstance(entry, EditModelDelete)
        assert "customer_id" in entry.remove.columns
        assert "customers" in entry.remove.joins  # by target_model name

    def test_join_target_model_not_in_datasource_drops_join(self) -> None:
        model = _orders_model(
            joins=[
                ModelJoin(
                    target_model="warehouses",
                    join_pairs=[["customer_id", "id"]],  # using customer_id as the key
                ),
            ],
        )
        live = _live_orders()
        # warehouses not in this DS
        entry, _ = diff_sql_table_model(
            model=model, live_table=live, available_models_in_ds={"orders", "customers"}
        )
        assert isinstance(entry, EditModelDelete)
        assert "warehouses" in entry.remove.joins


# ---------------------------------------------------------------------------
# diff_sql_model — sql mode
# ---------------------------------------------------------------------------


class TestDiffSqlModel:
    def test_trial_execute_success_matching_columns_is_no_op(self) -> None:
        model = SlayerModel(
            name="archived_orders",
            sql="SELECT id, amount FROM orders WHERE archived = true",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        live = {"id": DataType.DOUBLE, "amount": DataType.DOUBLE}
        entry, dropped = diff_sql_model(model=model, live_columns=live)
        assert entry is None
        assert dropped == set()

    def test_trial_execute_failure_whole_drop(self) -> None:
        model = SlayerModel(
            name="archived_orders",
            sql="SELECT id, amount FROM orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        # live_columns=None signals trial-execute failed
        entry, dropped = diff_sql_model(model=model, live_columns=None)
        assert isinstance(entry, WholeModelDelete)
        assert entry.model_name == "archived_orders"
        assert dropped == {c.name for c in model.columns}

    def test_trial_execute_bucket_mismatch_drops_column(self) -> None:
        model = SlayerModel(
            name="archived_orders",
            sql="SELECT id, amount FROM orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        live = {"id": DataType.DOUBLE, "amount": DataType.TEXT}
        entry, dropped = diff_sql_model(model=model, live_columns=live)
        assert isinstance(entry, EditModelDelete)
        assert "amount" in entry.remove.columns
        assert dropped == {"amount"}

    def test_extra_live_columns_are_ignored(self) -> None:
        model = SlayerModel(
            name="archived_orders",
            sql="SELECT id, amount FROM orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        )
        live = {"id": DataType.DOUBLE, "amount": DataType.DOUBLE, "extra": DataType.TEXT}
        entry, _ = diff_sql_model(model=model, live_columns=live)
        # validate_models reports deletes only — additions are out of scope
        assert entry is None


# ---------------------------------------------------------------------------
# compute_datasource_drops — cascade rules + collapse
# ---------------------------------------------------------------------------


class TestCascadeRules:
    def test_rule_1_derived_column_referencing_dropped_column(self) -> None:
        """A derived ``Column.sql`` referencing a column that was dropped is
        itself dropped. Walks transitively across chains of derived columns.
        """
        model = _orders_model(
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                # derived column referencing 'amount'
                Column(name="amount_x2", sql="amount * 2", type=DataType.DOUBLE),
                # transitively-derived: refs another derived column
                Column(name="amount_x4", sql="amount_x2 * 2", type=DataType.DOUBLE),
            ],
        )
        # Base diff: 'amount' dropped from live
        edit = EditModelDelete(
            model_name="orders",
            data_source="ds",
            remove=RemoveSpec(columns=["amount"]),
            reasons=[],
        )
        out = compute_datasource_drops(
            models=[model],
            sql_table_diffs={"orders": (edit, {"amount"})},
            sql_diffs={},
        )
        entry = _entry_for("orders", out)
        assert isinstance(entry, EditModelDelete)
        assert set(entry.remove.columns) >= {"amount", "amount_x2", "amount_x4"}

    def test_rule_2_measure_referencing_dropped_column(self) -> None:
        """A ``ModelMeasure.formula`` referencing dropped column or dropped
        measure is itself dropped.
        """
        model = _orders_model(
            measures=[
                ModelMeasure(formula="amount:sum", name="total_amount"),
                # transitive: refs total_amount which itself refs dropped 'amount'
                ModelMeasure(formula="total_amount / *:count", name="aov"),
            ],
        )
        edit = EditModelDelete(
            model_name="orders",
            data_source="ds",
            remove=RemoveSpec(columns=["amount"]),
        )
        out = compute_datasource_drops(
            models=[model],
            sql_table_diffs={"orders": (edit, {"amount"})},
            sql_diffs={},
        )
        entry = _entry_for("orders", out)
        assert isinstance(entry, EditModelDelete)
        assert set(entry.remove.measures) >= {"total_amount", "aov"}

    def test_rule_3a_join_local_column_dropped(self) -> None:
        """A ``Join`` whose ``local_column`` (= join_pairs[i][0]) was dropped
        produces a ``drop_join`` on the source model.
        """
        model = _orders_model(
            joins=[
                ModelJoin(
                    target_model="customers",
                    join_pairs=[["customer_id", "id"]],
                ),
            ],
        )
        edit = EditModelDelete(
            model_name="orders",
            data_source="ds",
            remove=RemoveSpec(columns=["customer_id"]),
        )
        out = compute_datasource_drops(
            models=[model],
            sql_table_diffs={"orders": (edit, {"customer_id"})},
            sql_diffs={},
        )
        entry = _entry_for("orders", out)
        assert isinstance(entry, EditModelDelete)
        assert "customers" in entry.remove.joins

    def test_rule_3b_target_foreign_column_dropped(self) -> None:
        """A ``Join`` on model ``K`` whose ``target_model == M`` and whose
        ``foreign_column`` (= join_pairs[i][1]) was dropped on M produces a
        ``drop_join`` on K.
        """
        orders = _orders_model(
            joins=[
                ModelJoin(
                    target_model="customers",
                    join_pairs=[["customer_id", "id"]],
                ),
            ],
        )
        customers = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="ds",
            columns=[
                Column(name="region", sql="region", type=DataType.TEXT),
            ],
        )
        # customers.id was dropped
        edit = EditModelDelete(
            model_name="customers",
            data_source="ds",
            remove=RemoveSpec(columns=["id"]),
        )
        out = compute_datasource_drops(
            models=[orders, customers],
            sql_table_diffs={"customers": (edit, {"id"})},
            sql_diffs={},
        )
        orders_entry = _entry_for("orders", out)
        assert isinstance(orders_entry, EditModelDelete)
        assert "customers" in orders_entry.remove.joins

    def test_rule_4_filter_referencing_dropped_column(self) -> None:
        model = _orders_model(
            filters=["status = 'completed'", "amount > 0"],
        )
        edit = EditModelDelete(
            model_name="orders",
            data_source="ds",
            remove=RemoveSpec(columns=["amount"]),
        )
        out = compute_datasource_drops(
            models=[model],
            sql_table_diffs={"orders": (edit, {"amount"})},
            sql_diffs={},
        )
        entry = _entry_for("orders", out)
        assert isinstance(entry, EditModelDelete)
        assert "amount > 0" in entry.remove_filters
        # Untouched filter stays out of the remove list
        assert "status = 'completed'" not in entry.remove_filters

    def test_rule_5_cross_model_derived_reference(self) -> None:
        """A derived column on model A whose SQL references ``customers.region``
        is cascade-dropped when ``customers.region`` is dropped.
        """
        orders = _orders_model(
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                # derived cross-model ref
                Column(
                    name="customer_region",
                    sql="customers.region",
                    type=DataType.TEXT,
                ),
            ],
            joins=[
                ModelJoin(
                    target_model="customers",
                    join_pairs=[["customer_id", "id"]],
                ),
            ],
        )
        customers = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
            ],
        )
        edit = EditModelDelete(
            model_name="customers",
            data_source="ds",
            remove=RemoveSpec(columns=["region"]),
        )
        out = compute_datasource_drops(
            models=[orders, customers],
            sql_table_diffs={"customers": (edit, {"region"})},
            sql_diffs={},
        )
        orders_entry = _entry_for("orders", out)
        assert isinstance(orders_entry, EditModelDelete)
        assert "customer_region" in orders_entry.remove.columns

    def test_rule_6_query_backed_model_transitive_whole_drop(self) -> None:
        """A query-backed model whose ``source_queries`` chain transitively
        references a dropped column / dropped model gets a ``WholeModelDelete``.
        Multi-stage chain (stage B refs stage A which refs dropped) is covered.
        """
        orders = _orders_model()
        # Two-stage query-backed model: stage A reads from orders, stage B
        # builds on stage A.
        qb = SlayerModel(
            name="qb_orders_summary",
            data_source="ds",
            source_queries=[
                SlayerQuery(
                    name="stage_a",
                    source_model="orders",
                    measures=[{"formula": "amount:sum", "name": "total"}],
                ),
                SlayerQuery(
                    source_model="stage_a",
                    measures=[{"formula": "total:max", "name": "peak"}],
                ),
            ],
        )
        # 'amount' on orders dropped — qb_orders_summary stage_a refs it
        edit = EditModelDelete(
            model_name="orders",
            data_source="ds",
            remove=RemoveSpec(columns=["amount"]),
        )
        out = compute_datasource_drops(
            models=[orders, qb],
            sql_table_diffs={"orders": (edit, {"amount"})},
            sql_diffs={},
        )
        qb_entry = _entry_for("qb_orders_summary", out)
        assert isinstance(qb_entry, WholeModelDelete)

    def test_rule_6_query_backed_whole_drop_when_base_model_whole_dropped(
        self,
    ) -> None:
        """If the underlying model gets a whole-drop (live table missing),
        any query-backed model that references it transitively whole-drops.
        """
        orders = _orders_model()
        qb = SlayerModel(
            name="qb_orders",
            data_source="ds",
            source_queries=[
                SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum", "name": "total"}],
                ),
            ],
        )
        whole = WholeModelDelete(model_name="orders", data_source="ds")
        out = compute_datasource_drops(
            models=[orders, qb],
            sql_table_diffs={"orders": (whole, {c.name for c in orders.columns})},
            sql_diffs={},
        )
        qb_entry = _entry_for("qb_orders", out)
        assert isinstance(qb_entry, WholeModelDelete)

    def test_rule_7_pk_drop_does_not_cascade(self) -> None:
        """PK drops don't cascade into derived columns / measures / filters."""
        model = _orders_model(
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                # Derived column that *would* reference 'id' — but PK drops
                # don't trigger cascade, so this column survives.
                Column(name="id_doubled", sql="id * 2", type=DataType.DOUBLE),
            ],
            measures=[ModelMeasure(formula="id:count_distinct", name="unique_ids")],
        )
        edit = EditModelDelete(
            model_name="orders",
            data_source="ds",
            remove=RemoveSpec(columns=["id"]),
            reasons=[],
        )
        out = compute_datasource_drops(
            models=[model],
            sql_table_diffs={"orders": (edit, {"id"})},
            sql_diffs={},
        )
        entry = _entry_for("orders", out)
        assert isinstance(entry, EditModelDelete)
        assert "id" in entry.remove.columns
        # Cascade has not extended into derived columns / measures / filters
        assert "id_doubled" not in entry.remove.columns
        assert "unique_ids" not in entry.remove.measures


class TestCollapseRule:
    def test_whole_drop_preempts_edit_for_same_model(self) -> None:
        """When a single model receives both an EditModelDelete and a
        WholeModelDelete, only the WholeModelDelete is emitted."""
        model = _orders_model()
        # Live table missing → WholeModelDelete; meanwhile a column-bucket
        # diff would also try to emit EditModelDelete via cascade. The
        # collapse rule should leave only the whole-drop.
        whole = WholeModelDelete(model_name="orders", data_source="ds")
        out = compute_datasource_drops(
            models=[model],
            sql_table_diffs={"orders": (whole, {c.name for c in model.columns})},
            sql_diffs={},
        )
        entries = [e for e in out if e.model_name == "orders"]
        assert len(entries) == 1
        assert isinstance(entries[0], WholeModelDelete)


class TestCrossDatasourceBoundary:
    def test_cascade_does_not_cross_datasource(self) -> None:
        """A cross-DS reference (model in DS A referencing model in DS B) is
        never followed by the cascade walker — DEV-1356 keeps everything
        within the parent model's data_source.
        """
        # Model in ds_a references customers from ds_b — this is invalid
        # cross-DS but at the cascade layer we just don't follow it.
        orders_in_a = _orders_model(data_source="ds_a")
        customers_in_b = SlayerModel(
            name="customers",
            sql_table="customers",
            data_source="ds_b",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="region", sql="region", type=DataType.TEXT),
            ],
        )
        # Drop region in ds_b
        edit = EditModelDelete(
            model_name="customers",
            data_source="ds_b",
            remove=RemoveSpec(columns=["region"]),
        )
        # Run compute scoped to ds_a — orders_in_a is the only model.
        # Cascade should NOT see anything about customers in ds_b.
        out_a = compute_datasource_drops(
            models=[orders_in_a],
            sql_table_diffs={},
            sql_diffs={},
        )
        # Nothing dropped for orders_in_a
        assert _entry_for("orders", out_a) is None

        # And running compute scoped to ds_b — only customers is touched,
        # orders_in_a (which lives in ds_a) is not even passed in.
        out_b = compute_datasource_drops(
            models=[customers_in_b],
            sql_table_diffs={"customers": (edit, {"region"})},
            sql_diffs={},
        )
        # Only customers entry, no cross-DS cascade to orders.
        assert _entry_for("customers", out_b) is not None
        assert _entry_for("orders", out_b) is None


# ---------------------------------------------------------------------------
# engine.validate_models — end-to-end against SQLite
# ---------------------------------------------------------------------------


class TestValidateModelsEndToEnd:
    """Exercise the full orchestrator: live introspection → diff → cascade.

    Uses a real SQLite tempfile so introspection runs through SQLAlchemy.
    """

    @pytest.fixture
    def workspace(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            yield Path(tmp.name)
        finally:
            tmp.cleanup()

    async def _setup(
        self, workspace: Path, *, db_name: str = "live.db"
    ) -> tuple[SlayerQueryEngine, str]:
        """Create a SQLite DB with two tables (orders, customers), persist
        SlayerModels for them, and return the engine + db path."""
        db_path = str(workspace / db_name)
        conn = sqlite3.connect(db_path)
        conn.executescript(
            """
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                region TEXT NOT NULL
            );
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                amount REAL NOT NULL,
                status TEXT NOT NULL,
                customer_id INTEGER REFERENCES customers(id)
            );
            INSERT INTO customers VALUES (1, 'US'), (2, 'EU');
            INSERT INTO orders VALUES (1, 100.0, 'completed', 1);
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
                    Column(name="status", sql="status", type=DataType.TEXT),
                    Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
                ],
                joins=[
                    ModelJoin(
                        target_model="customers",
                        join_pairs=[["customer_id", "id"]],
                    ),
                ],
            )
        )
        engine = SlayerQueryEngine(storage=storage)
        return engine, db_path

    async def test_no_drift_returns_empty_list(self, workspace: Path) -> None:
        engine, _ = await self._setup(workspace)
        result = await engine.validate_models(data_source="ds")
        assert result == []

    async def test_dropped_column_is_reported(self, workspace: Path) -> None:
        engine, db_path = await self._setup(workspace)
        # Externally drop orders.status
        conn = sqlite3.connect(db_path)
        conn.execute("ALTER TABLE orders DROP COLUMN status")
        conn.commit()
        conn.close()

        result = await engine.validate_models(data_source="ds")
        orders_entry = _entry_for("orders", result)
        assert isinstance(orders_entry, EditModelDelete)
        assert "status" in orders_entry.remove.columns

    async def test_dropped_table_yields_whole_model_delete(
        self, workspace: Path
    ) -> None:
        engine, db_path = await self._setup(workspace)
        conn = sqlite3.connect(db_path)
        conn.execute("DROP TABLE orders")
        conn.commit()
        conn.close()

        result = await engine.validate_models(data_source="ds")
        orders_entry = _entry_for("orders", result)
        assert isinstance(orders_entry, WholeModelDelete)

    async def test_validate_models_is_read_only(self, workspace: Path) -> None:
        """The validator must never mutate storage. Persisted models stay byte-
        for-byte unchanged after a validate_models call.
        """
        engine, db_path = await self._setup(workspace)
        before = await engine.storage.get_model("orders", data_source="ds")
        before_dump = before.model_dump()

        # Mutate live DB and re-validate.
        conn = sqlite3.connect(db_path)
        conn.execute("ALTER TABLE orders DROP COLUMN status")
        conn.commit()
        conn.close()
        await engine.validate_models(data_source="ds")

        after = await engine.storage.get_model("orders", data_source="ds")
        assert after.model_dump() == before_dump

    async def test_multi_datasource_concatenates_results(
        self, workspace: Path
    ) -> None:
        """``data_source=None`` runs every datasource concurrently and returns
        a flat list; cascades stay within each DS."""
        # First DS
        engine, _ = await self._setup(workspace, db_name="ds_a.db")
        # Second DS in a separate sqlite file
        db_b = str(workspace / "ds_b.db")
        conn = sqlite3.connect(db_b)
        conn.executescript(
            """
            CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT NOT NULL);
            INSERT INTO products VALUES (1, 'sku-1');
            """
        )
        conn.commit()
        conn.close()
        await engine.storage.save_datasource(
            DatasourceConfig(name="ds_b", type="sqlite", database=db_b)
        )
        await engine.storage.save_model(
            SlayerModel(
                name="products",
                sql_table="products",
                data_source="ds_b",
                columns=[
                    Column(
                        name="id", sql="id", type=DataType.DOUBLE, primary_key=True
                    ),
                    Column(name="sku", sql="sku", type=DataType.TEXT),
                ],
            )
        )
        # Drop a column in each DS
        conn_a = sqlite3.connect(str(workspace / "ds_a.db"))
        conn_a.execute("ALTER TABLE orders DROP COLUMN status")
        conn_a.commit()
        conn_a.close()
        conn_b = sqlite3.connect(db_b)
        conn_b.execute("ALTER TABLE products DROP COLUMN sku")
        conn_b.commit()
        conn_b.close()

        # Default datasource arg → both DSes
        result = await engine.validate_models()
        ds_names = {entry.data_source for entry in result}
        assert ds_names == {"ds", "ds_b"}
        # Cascades are bound per-DS — entries reference their own DS only
        for entry in result:
            assert entry.data_source in {"ds", "ds_b"}
