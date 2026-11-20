"""Tests for ``validate_models`` — read-only diff between persisted SlayerModels
and a live database schema.

Two layers are tested here:

* Pure functions in ``slayer.live_schema.schema_drift`` — exercise each diff /
  cascade rule against synthetic ``LiveTable`` fixtures, no DB involved.
* ``SlayerQueryEngine.validate_datasets()`` — end-to-end against a real SQLite
  database in a tempfile, verifying the orchestrator wires up live
  introspection, multi-DS gather, and the read-only contract (storage is
  never mutated).

Integration coverage on DuckDB lives in
``tests/integration/test_schema_drift_duckdb.py``.
"""

from __future__ import annotations

import tempfile
from collections.abc import Sequence
from pathlib import Path
from unittest.mock import patch

import pytest

try:
    import duckdb
except ImportError:  # optional dependency — tests importorskip at use sites
    duckdb = None

from slayer.core.enums import DataType
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema.schema_drift import (
    DriftReportEntry,
    EditDatasetDelete,
    LiveTable,
    RemoveSpec,
    WholeDatasetDelete,
    _resolve_live_table,
    _sql_dataset_source_refs,
    compute_datasource_drops,
    data_type_bucket,
    diff_sql_dataset,
    diff_sql_table_dataset,
)
from slayer.sql.dialects import get_dialect
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage

POSTGRES = get_dialect("postgres")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _orders_dataset(
    *,
    data_source: str = "ds",
    columns: list[DatasetField] | None = None,
    joins: list[DatasetRelationship] | None = None,
    metrics: list[DatasetMetric] | None = None,
    filters: list[str] | None = None,
    sql_table: str = "orders",
) -> SlayerDataset:
    return SlayerDataset(
        name="orders",
        sql_table=sql_table,
        data_source=data_source,
        fields=columns
        or [
            DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
            DatasetField(name="status", expression="status", datatype=DataType.STRING),
            DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
        ],
        relationships=joins or [],
        metrics=metrics or [],
        filters=filters or [],
    )


def _live_orders(
    *,
    columns: dict[str, DataType] | None = None,
) -> LiveTable:
    return LiveTable(
        columns=columns
        or {
            "id": DataType.FLOAT,
            "amount": DataType.FLOAT,
            "status": DataType.STRING,
            "customer_id": DataType.FLOAT,
        },
    )


def _entry_for(dataset_name: str, entries: Sequence[DriftReportEntry]) -> object | None:
    for e in entries:
        if e.dataset_name == dataset_name:
            return e
    return None


def _delete_for(dataset_name: str, entries: Sequence[DriftReportEntry]) -> object | None:
    return next((e for e in entries if e.dataset_name == dataset_name
                 and isinstance(e, (EditDatasetDelete, WholeDatasetDelete))), None)


def _retype_for(entries: Sequence[DriftReportEntry], column: str) -> dict:
    (finding,) = [d for e in entries if (d := e.model_dump(mode="json")).get("finding") == "retype"
                  and d.get("field") == column]
    return finding


# ---------------------------------------------------------------------------
# data_type_bucket
# ---------------------------------------------------------------------------


class TestDataTypeBucket:
    def test_number_string_boolean_temporal(self) -> None:
        # DataType.FLOAT → DOUBLE, DataType.STRING → TEXT.
        assert data_type_bucket(DataType.FLOAT) == "number"
        assert data_type_bucket(DataType.STRING) == "string"
        assert data_type_bucket(DataType.BOOLEAN) == "boolean"
        # DATE and TIMESTAMP collapse to "temporal"
        assert data_type_bucket(DataType.DATE) == data_type_bucket(DataType.DATETIME)
        assert data_type_bucket(DataType.DATE) == "temporal"

    def test_number_and_string_are_different_buckets(self) -> None:
        assert data_type_bucket(DataType.FLOAT) != data_type_bucket(DataType.STRING)

    def test_int_and_double_share_number_bucket(self) -> None:
        # Critical invariance — drift detection must NOT flag a
        # persisted DOUBLE field against an INT live column (or vice versa)
        # even though they're now distinct enum members.
        assert data_type_bucket(DataType.INTEGER) == "number"
        assert data_type_bucket(DataType.FLOAT) == "number"
        assert data_type_bucket(DataType.INTEGER) == data_type_bucket(DataType.FLOAT)


# ---------------------------------------------------------------------------
# diff_sql_table_model — sql_table mode
# ---------------------------------------------------------------------------


class TestDiffSqlTableModel:
    def test_no_drift_when_live_matches(self) -> None:
        model = _orders_dataset()
        live = _live_orders()
        entry, dropped = diff_sql_table_dataset(
            dataset=model, live_table=live, available_datasets_in_ds={"orders"}
        )
        assert entry is None
        assert dropped == set()

    def test_persisted_column_missing_from_live(self) -> None:
        model = _orders_dataset()
        live = _live_orders(
            columns={
                "id": DataType.FLOAT,
                "amount": DataType.FLOAT,
                # status missing
                "customer_id": DataType.FLOAT,
            }
        )
        entry, dropped = diff_sql_table_dataset(
            dataset=model, live_table=live, available_datasets_in_ds={"orders"}
        )
        assert isinstance(entry, EditDatasetDelete)
        assert entry.dataset_name == "orders"
        assert entry.remove.fields == ["status"]
        assert dropped == {"status"}
        assert any(r.target == "field:status" for r in entry.reasons)

    def test_live_table_missing(self) -> None:
        model = _orders_dataset()
        entry, dropped = diff_sql_table_dataset(
            dataset=model, live_table=None, available_datasets_in_ds={"orders"}
        )
        assert isinstance(entry, WholeDatasetDelete)
        assert entry.dataset_name == "orders"
        assert dropped == {c.name for c in model.fields}
        assert any("missing" in r.reason.lower() or "not found" in r.reason.lower()
                   for r in entry.reasons)

    def test_type_bucket_mismatch_keeps_column(self) -> None:
        # Persisted "amount" is NUMBER, live made it STRING.
        model = _orders_dataset()
        live = _live_orders(
            columns={
                "id": DataType.FLOAT,
                "amount": DataType.STRING,  # bucket flip
                "status": DataType.STRING,
                "customer_id": DataType.FLOAT,
            }
        )
        entry, dropped = diff_sql_table_dataset(
            dataset=model, live_table=live, available_datasets_in_ds={"orders"}
        )
        assert entry is None
        assert dropped == set()

    def test_integer_vs_float_same_bucket(self) -> None:
        # INT and DOUBLE are now distinct enum members but share
        # the ``"number"`` bucket so drift detection does not false-positive
        # when a persisted DOUBLE field is reported as INT by live
        # introspection (the v5 refinement step reconciles these without
        # raising drift).
        model = _orders_dataset()
        live = _live_orders(
            columns={
                "id": DataType.INTEGER,
                "amount": DataType.FLOAT,
                "status": DataType.STRING,
                "customer_id": DataType.INTEGER,
            }
        )
        entry, _ = diff_sql_table_dataset(
            dataset=model, live_table=live, available_datasets_in_ds={"orders"}
        )
        assert entry is None

    def test_date_and_timestamp_share_bucket(self) -> None:
        model = SlayerDataset(
            name="events",
            sql_table="events",
            data_source="ds",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="happened_at", expression="happened_at", datatype=DataType.DATE),
            ],
        )
        # Live reports TIMESTAMP — same bucket as DATE.
        live = LiveTable(
            columns={"id": DataType.FLOAT, "happened_at": DataType.DATETIME},
        )
        entry, _ = diff_sql_table_dataset(
            dataset=model, live_table=live, available_datasets_in_ds={"events"}
        )
        assert entry is None

    def test_join_local_column_missing_drops_join(self) -> None:
        model = _orders_dataset(
            joins=[
                DatasetRelationship(
                    to="customers",
                    from_columns=["customer_id"], to_columns=["id"],
                ),
            ],
        )
        # Live drops customer_id
        live = _live_orders(
            columns={
                "id": DataType.FLOAT,
                "amount": DataType.FLOAT,
                "status": DataType.STRING,
            }
        )
        entry, _ = diff_sql_table_dataset(
            dataset=model, live_table=live, available_datasets_in_ds={"orders", "customers"}
        )
        assert isinstance(entry, EditDatasetDelete)
        assert "customer_id" in entry.remove.fields
        assert "customers" in entry.remove.relationships  # by target_model name

    def test_join_target_model_not_in_datasource_drops_join(self) -> None:
        model = _orders_dataset(
            joins=[
                DatasetRelationship(
                    to="warehouses",
                    from_columns=["customer_id"], to_columns=["id"],  # using customer_id as the key
                ),
            ],
        )
        live = _live_orders()
        # warehouses not in this DS
        entry, _ = diff_sql_table_dataset(
            dataset=model, live_table=live, available_datasets_in_ds={"orders", "customers"}
        )
        assert isinstance(entry, EditDatasetDelete)
        assert "warehouses" in entry.remove.relationships

    def test_join_local_column_resolves_through_column_sql(self) -> None:
        """A relationship's ``from_columns`` entry is a semantic field name; resolve to
        the physical column via ``DatasetField.sql`` before checking against the
        live table. CodeRabbit thread #103/r3196378686.
        """
        # Dataset: ``customer_id`` semantic name maps to physical ``customer_fk``.
        model = _orders_dataset(
            columns=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
                DatasetField(name="status", expression="status", datatype=DataType.STRING),
                DatasetField(name="customer_id", expression="customer_fk", datatype=DataType.FLOAT),
            ],
            joins=[
                DatasetRelationship(
                    to="customers",
                    from_columns=["customer_id"], to_columns=["id"],  # semantic
                ),
            ],
        )
        live = _live_orders(
            columns={
                "id": DataType.FLOAT,
                "amount": DataType.FLOAT,
                "status": DataType.STRING,
                "customer_fk": DataType.FLOAT,  # physical name in the live DB
            }
        )
        entry, _ = diff_sql_table_dataset(
            dataset=model, live_table=live,
            available_datasets_in_ds={"orders", "customers"},
        )
        # No drift — the relationship's local field resolves to ``customer_fk`` which
        # is present in the live table. Without the fix, ``customer_id`` would
        # be flagged as missing and the relationship wrongly dropped.
        assert entry is None


# ---------------------------------------------------------------------------
# diff_sql_model — sql mode
# ---------------------------------------------------------------------------


class TestDiffSqlModel:
    def test_trial_execute_success_matching_columns_is_no_op(self) -> None:
        model = SlayerDataset(
            name="archived_orders",
            sql="SELECT id, amount FROM orders WHERE archived = true",
            data_source="ds",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
            ],
        )
        live = {"id": DataType.FLOAT, "amount": DataType.FLOAT}
        entry, dropped = diff_sql_dataset(dataset=model, live_columns=live)
        assert entry is None
        assert dropped == set()

    def test_trial_execute_failure_whole_drop(self) -> None:
        model = SlayerDataset(
            name="archived_orders",
            sql="SELECT id, amount FROM orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
            ],
        )
        # live_columns=None signals trial-execute failed
        entry, dropped = diff_sql_dataset(dataset=model, live_columns=None)
        assert isinstance(entry, WholeDatasetDelete)
        assert entry.dataset_name == "archived_orders"
        assert entry.cause == "schema_drift"
        assert dropped == {c.name for c in model.fields}

    def test_trial_execute_failure_with_live_tables_marks_invalid_sql(self) -> None:
        model = SlayerDataset(
            name="archived_orders",
            sql="SELECT id, amount FROM orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
            ],
        )
        entry, dropped = diff_sql_dataset(
            dataset=model, live_columns=None, invalid_sql=True
        )
        assert isinstance(entry, WholeDatasetDelete)
        assert entry.cause == "invalid_sql"
        assert "source tables and referenced columns exist" in entry.reasons[0].reason
        assert dropped == {c.name for c in model.fields}

    def test_trial_execute_bucket_mismatch_keeps_column(self) -> None:
        model = SlayerDataset(
            name="archived_orders",
            sql="SELECT id, amount FROM orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
            ],
        )
        live = {"id": DataType.FLOAT, "amount": DataType.STRING}
        entry, dropped = diff_sql_dataset(dataset=model, live_columns=live)
        assert entry is None
        assert dropped == set()

    def test_extra_live_columns_are_ignored(self) -> None:
        model = SlayerDataset(
            name="archived_orders",
            sql="SELECT id, amount FROM orders",
            data_source="ds",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
            ],
        )
        live = {"id": DataType.FLOAT, "amount": DataType.FLOAT, "extra": DataType.STRING}
        entry, _ = diff_sql_dataset(dataset=model, live_columns=live)
        # validate_models reports deletes only — additions are out of scope
        assert entry is None


def _refs(*, sql: str, dialect: str) -> "dict[str, set[str]] | None":
    """``_sql_dataset_source_refs`` with each table rendered and each column named."""
    refs = _sql_dataset_source_refs(sql=sql, dialect=dialect)
    if refs is None:
        return None
    return {table.sql(dialect=dialect): {col.name for col in cols} for table, cols in refs.items()}



class TestSqlModelSourceRefs:
    def test_aliases_attributed_and_ctes_excluded(self) -> None:
        sql = """
        WITH recent AS (
            SELECT o.id, o.store_id FROM orders AS o WHERE o.ts > '2024-01-01'
        )
        SELECT r.id, s.name
        FROM recent AS r
        JOIN analytics.stores AS s ON r.store_id = s.id
        """
        refs = _refs(sql=sql, dialect="postgres")
        assert refs == {
            "orders": {"id", "store_id", "ts"},
            "analytics.stores": {"name", "id"},
        }

    def test_star_behind_derived_ref_returns_none(self) -> None:
        # ``*`` may hide the physical column behind ``src.amount``.
        sql = "WITH src AS (SELECT * FROM orders) SELECT src.amount FROM src"
        assert _refs(sql=sql, dialect="postgres") is None

    def test_bare_columns_attributed_to_single_table(self) -> None:
        refs = _refs(
            sql="SELECT id, amount FROM orders", dialect="postgres"
        )
        assert refs == {"orders": {"id", "amount"}}

    def test_ambiguous_bare_column_returns_none(self) -> None:
        sql = "SELECT amount FROM orders AS o JOIN stores AS s ON o.sid = s.id"
        assert _refs(sql=sql, dialect="postgres") is None

    def test_unparseable_sql_returns_none(self) -> None:
        assert _refs(sql="SELECT FROM FROM", dialect="postgres") is None

    def test_using_join_keys_attributed_to_all_tables(self) -> None:
        # USING identifiers are not exp.Column nodes; a dropped join key must
        # still be probed on every joined table (Codex review on PR #340).
        sql = "SELECT o.amount FROM orders AS o JOIN stores AS s USING (store_id)"
        refs = _refs(sql=sql, dialect="postgres")
        assert refs == {
            "orders": {"amount", "store_id"},
            "stores": {"store_id"},
        }

    def test_alias_shadowing_resolved_per_scope(self) -> None:
        # ``a`` means orders outside and stores inside the subquery.
        sql = (
            "SELECT a.amount FROM orders AS a WHERE EXISTS "
            "(SELECT 1 FROM stores AS a WHERE a.region = 'US')"
        )
        refs = _refs(sql=sql, dialect="postgres")
        assert refs == {"orders": {"amount"}, "stores": {"region"}}

    def test_correlated_outer_ref_attributed(self) -> None:
        sql = (
            "SELECT o.amount FROM orders AS o WHERE EXISTS "
            "(SELECT 1 FROM stores AS s WHERE s.oid = o.id)"
        )
        refs = _refs(sql=sql, dialect="postgres")
        assert refs == {"orders": {"amount", "id"}, "stores": {"oid"}}

    def test_using_join_on_cte_with_star_returns_none(self) -> None:
        # The USING key's presence in the CTE cannot be verified behind ``*``.
        sql = (
            "WITH c AS (SELECT * FROM stores) "
            "SELECT o.amount FROM orders AS o JOIN c USING (store_id)"
        )
        assert _refs(sql=sql, dialect="postgres") is None

    def test_union_bare_columns_attributed_per_branch(self) -> None:
        sql = "SELECT a FROM t1 UNION ALL SELECT b FROM t2"
        refs = _refs(sql=sql, dialect="postgres")
        assert refs == {"t1": {"a"}, "t2": {"b"}}

    def test_table_less_statement_returns_empty_refs(self) -> None:
        # {} (verifies vacuously — invalid_sql) is distinct from None
        # (cannot verify — schema drift).
        refs = _refs(
            sql="SELECT no_such_function(1)", dialect="postgres"
        )
        assert refs == {}

    def test_using_keys_not_attributed_to_later_joins(self) -> None:
        # ``c`` is not an operand of the USING join, so it must not be
        # probed for ``k``.
        sql = "SELECT a.x FROM a JOIN b USING (k) JOIN c ON c.id = a.cid"
        refs = _refs(sql=sql, dialect="postgres")
        assert refs == {"a": {"x", "k", "cid"}, "b": {"k"}, "c": {"id"}}


# ---------------------------------------------------------------------------
# compute_datasource_drops — cascade rules + collapse
# ---------------------------------------------------------------------------


class TestCascadeRules:
    def test_rule_1_derived_column_referencing_dropped_column(self) -> None:
        """A derived ``DatasetField.sql`` referencing a field that was dropped is
        itself dropped. Walks transitively across chains of derived fields.
        """
        model = _orders_dataset(
            columns=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
                DatasetField(name="status", expression="status", datatype=DataType.STRING),
                DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
                # derived field referencing 'amount'
                DatasetField(name="amount_x2", expression="amount * 2", datatype=DataType.FLOAT),
                # transitively-derived: refs another derived field
                DatasetField(name="amount_x4", expression="orders.amount_x2 * 2", datatype=DataType.FLOAT),
            ],
        )
        # Base diff: 'amount' dropped from live
        edit = EditDatasetDelete(
            dataset_name="orders",
            data_source="ds",
            remove=RemoveSpec(fields=["amount"]),
            reasons=[],
        )
        out = compute_datasource_drops(
            datasets=[model],
            sql_table_diffs={"orders": (edit, {"amount"})},
            sql_diffs={},
        )
        entry = _entry_for("orders", out)
        assert isinstance(entry, EditDatasetDelete)
        assert set(entry.remove.fields) >= {"amount", "amount_x2", "amount_x4"}

    def test_rule_2_measure_referencing_dropped_column(self) -> None:
        """A ``DatasetMetric.expression`` referencing a dropped field or dropped
        metric is itself dropped.
        """
        model = _orders_dataset(
            metrics=[
                DatasetMetric(expression="sum(amount)", name="total_amount"),
                # transitive: refs total_amount which itself refs dropped 'amount'
                DatasetMetric(expression="total_amount / count(*)", name="aov"),
            ],
        )
        edit = EditDatasetDelete(
            dataset_name="orders",
            data_source="ds",
            remove=RemoveSpec(fields=["amount"]),
        )
        out = compute_datasource_drops(
            datasets=[model],
            sql_table_diffs={"orders": (edit, {"amount"})},
            sql_diffs={},
        )
        entry = _entry_for("orders", out)
        assert isinstance(entry, EditDatasetDelete)
        assert set(entry.remove.metrics) >= {"total_amount", "aov"}

    def test_rule_2_funcstyle_joined_custom_agg_cascades(self) -> None:
        """A ``DatasetMetric.expression`` using funcstyle over a custom
        aggregation defined on a JOINED dataset (``rolling_avg(customers.score)``
        where ``rolling_avg`` lives on ``customers``) must extract
        ``customers.score`` as a ref, so dropping that field cascades the
        metric away. The cascade now walks the reachable relationship graph for
        custom aggregation names instead of using the dataset's own aggs only.
        """
        orders = _orders_dataset(
            joins=[
                DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
            ],
            metrics=[
                DatasetMetric(expression="rolling_avg(customers.score)", name="ravg"),
            ],
        )
        customers = SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="ds",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="score", expression="score", datatype=DataType.FLOAT),
            ],
            aggregations=[
                Aggregation(name="rolling_avg", expression="AVG({value})"),
            ],
        )
        # Live diff: ``customers.score`` was dropped.
        customers_edit = EditDatasetDelete(
            dataset_name="customers",
            data_source="ds",
            remove=RemoveSpec(fields=["score"]),
        )
        out = compute_datasource_drops(
            datasets=[orders, customers],
            sql_table_diffs={"customers": (customers_edit, {"score"})},
            sql_diffs={},
        )
        # orders.ravg must cascade because its expression references the
        # dropped customers.score.
        orders_entry = _entry_for("orders", out)
        assert isinstance(orders_entry, EditDatasetDelete), out
        assert "ravg" in set(orders_entry.remove.metrics), orders_entry

    def test_rule_3a_join_local_column_dropped(self) -> None:
        """A ``DatasetRelationship`` whose ``from_columns`` field was dropped
        produces a relationship removal on the source dataset.
        """
        model = _orders_dataset(
            joins=[
                DatasetRelationship(
                    to="customers",
                    from_columns=["customer_id"], to_columns=["id"],
                ),
            ],
        )
        edit = EditDatasetDelete(
            dataset_name="orders",
            data_source="ds",
            remove=RemoveSpec(fields=["customer_id"]),
        )
        out = compute_datasource_drops(
            datasets=[model],
            sql_table_diffs={"orders": (edit, {"customer_id"})},
            sql_diffs={},
        )
        entry = _entry_for("orders", out)
        assert isinstance(entry, EditDatasetDelete)
        assert "customers" in entry.remove.relationships

    def test_rule_3b_target_foreign_column_dropped(self) -> None:
        """A ``DatasetRelationship`` on dataset ``K`` whose ``to == M`` and whose
        ``to_columns`` field was dropped on M produces a
        relationship removal on K.
        """
        orders = _orders_dataset(
            joins=[
                DatasetRelationship(
                    to="customers",
                    from_columns=["customer_id"], to_columns=["id"],
                ),
            ],
        )
        customers = SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="ds",
            fields=[
                DatasetField(name="region", expression="region", datatype=DataType.STRING),
            ],
        )
        # customers.id was dropped
        edit = EditDatasetDelete(
            dataset_name="customers",
            data_source="ds",
            remove=RemoveSpec(fields=["id"]),
        )
        out = compute_datasource_drops(
            datasets=[orders, customers],
            sql_table_diffs={"customers": (edit, {"id"})},
            sql_diffs={},
        )
        orders_entry = _entry_for("orders", out)
        assert isinstance(orders_entry, EditDatasetDelete)
        assert "customers" in orders_entry.remove.relationships

    def test_rule_4_filter_referencing_dropped_column(self) -> None:
        model = _orders_dataset(
            filters=["status = 'completed'", "amount > 0"],
        )
        edit = EditDatasetDelete(
            dataset_name="orders",
            data_source="ds",
            remove=RemoveSpec(fields=["amount"]),
        )
        out = compute_datasource_drops(
            datasets=[model],
            sql_table_diffs={"orders": (edit, {"amount"})},
            sql_diffs={},
        )
        entry = _entry_for("orders", out)
        assert isinstance(entry, EditDatasetDelete)
        assert "amount > 0" in entry.remove_filters
        # Untouched filter stays out of the remove list
        assert "status = 'completed'" not in entry.remove_filters

    def test_rule_5_cross_model_derived_reference(self) -> None:
        """A derived field on dataset A whose SQL references ``customers.region``
        is cascade-dropped when ``customers.region`` is dropped.
        """
        orders = _orders_dataset(
            columns=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
                DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
                # derived cross-dataset ref
                DatasetField(
                    name="customer_region",
                    expression="customers.region",
                    datatype=DataType.STRING,
                ),
            ],
            joins=[
                DatasetRelationship(
                    to="customers",
                    from_columns=["customer_id"], to_columns=["id"],
                ),
            ],
        )
        customers = SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="ds",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", expression="region", datatype=DataType.STRING),
            ],
        )
        edit = EditDatasetDelete(
            dataset_name="customers",
            data_source="ds",
            remove=RemoveSpec(fields=["region"]),
        )
        out = compute_datasource_drops(
            datasets=[orders, customers],
            sql_table_diffs={"customers": (edit, {"region"})},
            sql_diffs={},
        )
        orders_entry = _entry_for("orders", out)
        assert isinstance(orders_entry, EditDatasetDelete)
        assert "customer_region" in orders_entry.remove.fields

    def test_rule_6_query_backed_model_transitive_whole_drop(self) -> None:
        """A query-backed dataset whose ``source_queries`` chain transitively
        references a dropped field / dropped dataset gets a ``WholeDatasetDelete``.
        Multi-stage chain (stage B refs stage A which refs dropped) is covered.
        """
        orders = _orders_dataset()
        # Two-stage query-backed dataset: stage A reads from orders, stage B
        # builds on stage A.
        qb = SlayerDataset(
            name="qb_orders_summary",
            data_source="ds",
            source_queries=[
                SlayerQuery.model_validate({
                    "name": "stage_a",
                    "population": "orders",
                    "metrics": [{"metric": "sum(orders.amount)", "alias": "total"}],
                }),
                SlayerQuery.model_validate({
                    "population": "stage_a",
                    "metrics": [{"metric": "max(stage_a.total)", "alias": "peak"}],
                }),
            ],
        )
        # 'amount' on orders dropped — qb_orders_summary stage_a refs it
        edit = EditDatasetDelete(
            dataset_name="orders",
            data_source="ds",
            remove=RemoveSpec(fields=["amount"]),
        )
        out = compute_datasource_drops(
            datasets=[orders, qb],
            sql_table_diffs={"orders": (edit, {"amount"})},
            sql_diffs={},
        )
        qb_entry = _entry_for("qb_orders_summary", out)
        assert isinstance(qb_entry, WholeDatasetDelete)

    def test_rule_6_query_backed_stage_filter_with_aggregation_cascades(
        self,
    ) -> None:
        """A query-backed dataset whose stage filter uses Mode B DSL constructs
        (``sum(revenue) > 100``, ``change(...) > 0``) must still trigger a
        cascade drop when the underlying field is dropped. Regression for
        Stage filters were once routed through the SQL-only ref
        extractor and returned ``[]``, so drift cascades were missed.
        """
        orders = _orders_dataset()
        qb = SlayerDataset(
            name="qb_orders_filtered",
            data_source="ds",
            source_queries=[
                SlayerQuery.model_validate({
                    "population": "orders",
                    "metrics": [{"metric": "sum(orders.amount)", "alias": "total"}],
                    "filter": ["sum(orders.amount) > 100"],
                }),
            ],
        )
        edit = EditDatasetDelete(
            dataset_name="orders",
            data_source="ds",
            remove=RemoveSpec(fields=["amount"]),
        )
        out = compute_datasource_drops(
            datasets=[orders, qb],
            sql_table_diffs={"orders": (edit, {"amount"})},
            sql_diffs={},
        )
        qb_entry = _entry_for("qb_orders_filtered", out)
        assert isinstance(qb_entry, WholeDatasetDelete)

    def test_rule_6_query_backed_whole_drop_when_base_model_whole_dropped(
        self,
    ) -> None:
        """If the underlying dataset gets a whole-drop (live table missing),
        any query-backed dataset that references it transitively whole-drops.
        """
        orders = _orders_dataset()
        qb = SlayerDataset(
            name="qb_orders",
            data_source="ds",
            source_queries=[
                SlayerQuery.model_validate({
                    "population": "orders",
                    "metrics": [{"metric": "sum(orders.amount)", "alias": "total"}],
                }),
            ],
        )
        whole = WholeDatasetDelete(dataset_name="orders", data_source="ds")
        out = compute_datasource_drops(
            datasets=[orders, qb],
            sql_table_diffs={"orders": (whole, {c.name for c in orders.fields})},
            sql_diffs={},
        )
        qb_entry = _entry_for("qb_orders", out)
        assert isinstance(qb_entry, WholeDatasetDelete)

    def test_rule_7_pk_drop_does_not_cascade(self) -> None:
        """PK drops don't cascade into derived fields / metrics / filters."""
        model = _orders_dataset(
            columns=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
                DatasetField(name="status", expression="status", datatype=DataType.STRING),
                DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
                # Derived field that *would* reference 'id' — but PK drops
                # don't trigger cascade, so this field survives.
                DatasetField(name="id_doubled", expression="id * 2", datatype=DataType.FLOAT),
            ],
            metrics=[DatasetMetric(expression="count_distinct(id)", name="unique_ids")],
        )
        edit = EditDatasetDelete(
            dataset_name="orders",
            data_source="ds",
            remove=RemoveSpec(fields=["id"]),
            reasons=[],
        )
        out = compute_datasource_drops(
            datasets=[model],
            sql_table_diffs={"orders": (edit, {"id"})},
            sql_diffs={},
        )
        entry = _entry_for("orders", out)
        assert isinstance(entry, EditDatasetDelete)
        assert "id" in entry.remove.fields
        # Cascade has not extended into derived fields / metrics / filters
        assert "id_doubled" not in entry.remove.fields
        assert "unique_ids" not in entry.remove.metrics


class TestCollapseRule:
    def test_whole_drop_preempts_edit_for_same_model(self) -> None:
        """When a single dataset receives both an EditDatasetDelete and a
        WholeDatasetDelete, only the WholeDatasetDelete is emitted."""
        model = _orders_dataset()
        # Live table missing → WholeDatasetDelete; meanwhile a column-bucket
        # diff would also try to emit EditDatasetDelete via cascade. The
        # collapse rule should leave only the whole-drop.
        whole = WholeDatasetDelete(dataset_name="orders", data_source="ds")
        out = compute_datasource_drops(
            datasets=[model],
            sql_table_diffs={"orders": (whole, {c.name for c in model.fields})},
            sql_diffs={},
        )
        entries = [e for e in out if e.dataset_name == "orders"]
        assert len(entries) == 1
        assert isinstance(entries[0], WholeDatasetDelete)


class TestCrossDatasourceBoundary:
    def test_cascade_does_not_cross_datasource(self) -> None:
        """A cross-DS reference (dataset in DS A referencing dataset in DS B) is
        never followed by the cascade walker — drift keeps everything
        within the parent dataset's data_source.
        """
        # Dataset in ds_a references customers from ds_b — this is invalid
        # cross-DS but at the cascade layer we just don't follow it.
        orders_in_a = _orders_dataset(data_source="ds_a")
        customers_in_b = SlayerDataset(
            name="customers",
            sql_table="customers",
            data_source="ds_b",
            fields=[
                DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="region", expression="region", datatype=DataType.STRING),
            ],
        )
        # Drop region in ds_b
        edit = EditDatasetDelete(
            dataset_name="customers",
            data_source="ds_b",
            remove=RemoveSpec(fields=["region"]),
        )
        # Run compute scoped to ds_a — orders_in_a is the only dataset.
        # Cascade should NOT see anything about customers in ds_b.
        out_a = compute_datasource_drops(
            datasets=[orders_in_a],
            sql_table_diffs={},
            sql_diffs={},
        )
        # Nothing dropped for orders_in_a
        assert _entry_for("orders", out_a) is None

        # And running compute scoped to ds_b — only customers is touched,
        # orders_in_a (which lives in ds_a) is not even passed in.
        out_b = compute_datasource_drops(
            datasets=[customers_in_b],
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
        with transaction(db_path) as conn:
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
                    DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                    DatasetField(name="region", expression="region", datatype=DataType.STRING),
                ],
            )
        )
        await storage.save_dataset(
            SlayerDataset(
                name="orders",
                sql_table="orders",
                data_source="ds",
                fields=[
                    DatasetField(name="id", expression="id", datatype=DataType.FLOAT, primary_key=True),
                    DatasetField(name="amount", expression="amount", datatype=DataType.FLOAT),
                    DatasetField(name="status", expression="status", datatype=DataType.STRING),
                    DatasetField(name="customer_id", expression="customer_id", datatype=DataType.FLOAT),
                ],
                relationships=[
                    DatasetRelationship(
                        to="customers",
                        from_columns=["customer_id"], to_columns=["id"],
                    ),
                ],
            )
        )
        engine = SlayerQueryEngine(storage=storage)
        return engine, db_path

    async def test_no_drift_returns_empty_list(self, workspace: Path) -> None:
        engine, _ = await self._setup(workspace)
        result = await engine.validate_datasets(data_source="ds")
        assert result == []

    async def test_dropped_column_is_reported(self, workspace: Path) -> None:
        engine, db_path = await self._setup(workspace)
        # Externally drop orders.status
        with transaction(db_path) as conn:
            conn.execute("ALTER TABLE orders DROP COLUMN status")

        result = await engine.validate_datasets(data_source="ds")
        orders_entry = _entry_for("orders", result)
        assert isinstance(orders_entry, EditDatasetDelete)
        assert "status" in orders_entry.remove.fields

    async def test_dropped_table_yields_whole_model_delete(
        self, workspace: Path
    ) -> None:
        engine, db_path = await self._setup(workspace)
        with transaction(db_path) as conn:
            conn.execute("DROP TABLE orders")

        result = await engine.validate_datasets(data_source="ds")
        orders_entry = _entry_for("orders", result)
        assert isinstance(orders_entry, WholeDatasetDelete)

    async def test_validate_models_is_read_only(self, workspace: Path) -> None:
        """The validator must never mutate storage. Persisted datasets stay byte-
        for-byte unchanged after a validate_datasets call.
        """
        engine, db_path = await self._setup(workspace)
        before = await engine.storage.get_dataset("orders", data_source="ds")
        before_dump = before.model_dump()

        # Mutate live DB and re-validate.
        with transaction(db_path) as conn:
            conn.execute("ALTER TABLE orders DROP COLUMN status")
        await engine.validate_datasets(data_source="ds")

        after = await engine.storage.get_dataset("orders", data_source="ds")
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
        with transaction(db_b) as conn:
            conn.executescript(
                """
                CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT NOT NULL);
                INSERT INTO products VALUES (1, 'sku-1');
                """
            )
        await engine.storage.save_datasource(
            DatasourceConfig(name="ds_b", type="sqlite", database=db_b)
        )
        await engine.storage.save_dataset(
            SlayerDataset(
                name="products",
                sql_table="products",
                data_source="ds_b",
                fields=[
                    DatasetField(
                        name="id", expression="id", datatype=DataType.FLOAT, primary_key=True
                    ),
                    DatasetField(name="sku", expression="sku", datatype=DataType.STRING),
                ],
            )
        )
        # Drop a column in each DS
        with transaction(str(workspace / "ds_a.db")) as conn_a:
            conn_a.execute("ALTER TABLE orders DROP COLUMN status")
        with transaction(db_b) as conn_b:
            conn_b.execute("ALTER TABLE products DROP COLUMN sku")

        # Default datasource arg → both DSes
        result = await engine.validate_datasets()
        ds_names = {entry.data_source for entry in result}
        assert ds_names == {"ds", "ds_b"}
        # Cascades are bound per-DS — entries reference their own DS only
        for entry in result:
            assert entry.data_source in {"ds", "ds_b"}


# ---------------------------------------------------------------------------
# validate_models — the SQLite affinity probe reports a re-type finding when
# persisted INT disagrees with the value-level probe verdict.
# ---------------------------------------------------------------------------


def _create_sqlite_with_mixed_storage(
    db_path: str, table: str, column: str, values: list
) -> None:
    with transaction(db_path) as conn:
        conn.execute(
            f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, "{column}" INTEGER)'
        )
        for i, v in enumerate(values, start=1):
            conn.execute(
                f'INSERT INTO "{table}" VALUES (?, ?)', (i, v),
            )


class TestValidateModelsSqliteProbe:
    """validate_datasets on SQLite: the per-field probe runs against each persisted INT base field and
    reports a re-type finding when it disagrees with the persisted type."""

    @pytest.fixture
    def workspace(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            yield Path(tmp.name)
        finally:
            tmp.cleanup()

    async def _setup_sensordata(
        self,
        workspace: Path,
        *,
        values: list,
        columns: list[DatasetField],
        metrics: list[DatasetMetric] | None = None,
        ds_type: str = "sqlite",
    ) -> tuple[SlayerQueryEngine, str]:
        """Persist a sensordata dataset whose ``tempstabidx`` field is
        declared INT but whose live values are controlled by ``values``."""
        db_path = str(workspace / "live.db")
        if ds_type == "sqlite":
            _create_sqlite_with_mixed_storage(
                db_path, "sensordata", "tempstabidx", values,
            )

        storage = YAMLStorage(base_dir=str(workspace / "storage"))
        await storage.save_datasource(
            DatasourceConfig(name="ds", type=ds_type, database=db_path)
        )
        await storage.save_dataset(
            SlayerDataset(
                name="sensordata",
                sql_table="sensordata",
                data_source="ds",
                fields=columns,
                metrics=metrics or [],
            )
        )
        return SlayerQueryEngine(storage=storage), db_path

    async def test_reports_a_retype_when_storage_widens_to_double(
        self, workspace: Path
    ) -> None:
        engine, _ = await self._setup_sensordata(
            workspace,
            values=[1, 0.5, 0.7, 0.9, 0.99],
            columns=[
                DatasetField(
                    name="id", expression="id", datatype=DataType.INTEGER, primary_key=True,
                ),
                DatasetField(name="tempstabidx", expression="tempstabidx", datatype=DataType.INTEGER),
            ],
        )
        result = await engine.validate_datasets(data_source="ds")
        assert _delete_for("sensordata", result) is None, result
        finding = _retype_for(result, "tempstabidx")
        assert (finding["stored_type"], finding["live_type"]) == ("Integer", "Float"), finding
        assert "slayer ingest" in finding["remedy"], finding

    async def test_reports_a_retype_when_storage_widens_to_text(
        self, workspace: Path
    ) -> None:
        """Same shape as the DOUBLE drift but verdict TEXT — non-coercible
        text storage in a declared-INT column."""
        engine, _ = await self._setup_sensordata(
            workspace,
            values=[1, 2, "abc", "xyz"],
            columns=[
                DatasetField(
                    name="id", expression="id", datatype=DataType.INTEGER, primary_key=True,
                ),
                DatasetField(name="tempstabidx", expression="tempstabidx", datatype=DataType.INTEGER),
            ],
        )
        result = await engine.validate_datasets(data_source="ds")
        assert _delete_for("sensordata", result) is None, result
        finding = _retype_for(result, "tempstabidx")
        assert (finding["stored_type"], finding["live_type"]) == ("Integer", "String"), finding
        assert "slayer ingest" in finding["remedy"], finding

    async def test_no_drift_when_probe_agrees(
        self, workspace: Path
    ) -> None:
        """All-integer storage → probe returns INT → no drift entry."""
        engine, _ = await self._setup_sensordata(
            workspace,
            values=[1, 2, 3, 4, 5],
            columns=[
                DatasetField(
                    name="id", expression="id", datatype=DataType.INTEGER, primary_key=True,
                ),
                DatasetField(name="tempstabidx", expression="tempstabidx", datatype=DataType.INTEGER),
            ],
        )
        result = await engine.validate_datasets(data_source="ds")
        assert result == []

    async def test_non_sqlite_skips_probe(self, workspace: Path) -> None:
        """Non-SQLite datasources never run the probe at validate time."""
        pytest.importorskip("duckdb")

        db_path = str(workspace / "live.duckdb")
        con = duckdb.connect(db_path)
        con.execute(
            "CREATE TABLE sensordata (id INTEGER PRIMARY KEY, tempstabidx INTEGER)"
        )
        con.execute("INSERT INTO sensordata VALUES (1, 10), (2, 20)")
        con.close()

        storage = YAMLStorage(base_dir=str(workspace / "storage"))
        await storage.save_datasource(
            DatasourceConfig(name="ds", type="duckdb", database=db_path)
        )
        await storage.save_dataset(
            SlayerDataset(
                name="sensordata",
                sql_table="sensordata",
                data_source="ds",
                fields=[
                    DatasetField(
                        name="id", expression="id", datatype=DataType.INTEGER, primary_key=True,
                    ),
                    DatasetField(
                        name="tempstabidx", expression="tempstabidx", datatype=DataType.INTEGER,
                    ),
                ],
            )
        )
        engine = SlayerQueryEngine(storage=storage)

        with patch(
            "slayer.database.sqlite_introspect.probe_sqlite_integer_column",
            side_effect=AssertionError("probe must not fire on non-SQLite"),
        ):
            result = await engine.validate_datasets(data_source="ds")
        # No probe-related drift; the test mainly asserts the patch never
        # fires.
        assert all(e.dataset_name != "sensordata" or
                   "tempstabidx" not in (e.remove if isinstance(e, EditDatasetDelete) else RemoveSpec()).fields
                   for e in result)

    async def test_probe_failure_no_entry(self, workspace: Path) -> None:
        """A failing probe returns None — validate_datasets must not emit a
        drift entry for that field, and only log a WARNING."""
        engine, _ = await self._setup_sensordata(
            workspace,
            values=[1, 0.5, 0.7],
            columns=[
                DatasetField(
                    name="id", expression="id", datatype=DataType.INTEGER, primary_key=True,
                ),
                DatasetField(name="tempstabidx", expression="tempstabidx", datatype=DataType.INTEGER),
            ],
        )
        with patch(
            "slayer.database.sqlite_introspect.probe_sqlite_integer_column",
            return_value=None,
        ):
            result = await engine.validate_datasets(data_source="ds")
        # No drift entry for tempstabidx via the probe path.
        entry = _entry_for("sensordata", result)
        assert entry is None or "tempstabidx" not in (
            entry.remove.fields if isinstance(entry, EditDatasetDelete) else []
        )

    async def test_probe_retype_keeps_the_dependent_measure(
        self, workspace: Path
    ) -> None:
        engine, _ = await self._setup_sensordata(
            workspace,
            values=[1, 0.5, 0.7, 0.9],
            columns=[
                DatasetField(
                    name="id", expression="id", datatype=DataType.INTEGER, primary_key=True,
                ),
                DatasetField(name="tempstabidx", expression="tempstabidx", datatype=DataType.INTEGER),
            ],
            metrics=[
                DatasetMetric(
                    expression="sum(tempstabidx)",
                    name="stab_total",
                ),
            ],
        )
        result = await engine.validate_datasets(data_source="ds")
        assert _delete_for("sensordata", result) is None, result
        assert _retype_for(result, "tempstabidx")

    async def test_probe_retype_keeps_the_derived_column(
        self, workspace: Path
    ) -> None:
        engine, _ = await self._setup_sensordata(
            workspace,
            values=[1, 0.5, 0.7, 0.9],
            columns=[
                DatasetField(
                    name="id", expression="id", datatype=DataType.INTEGER, primary_key=True,
                ),
                DatasetField(name="tempstabidx", expression="tempstabidx", datatype=DataType.INTEGER),
                DatasetField(name="doubled", expression=ExpressionObject(dialects=[DialectEntry(dialect="SQLITE", expression="tempstabidx * 2")]), datatype=DataType.INTEGER),
            ],
        )
        result = await engine.validate_datasets(data_source="ds")
        assert _delete_for("sensordata", result) is None, result


class TestResolveLiveTable:
    """Live-table lookup must handle schema-qualified and quoted identifiers."""

    @staticmethod
    def _live(name: str) -> LiveTable:
        return LiveTable(columns={"id": DataType.FLOAT})

    def test_bare_table_matches(self) -> None:
        live = self._live("orders")
        assert _resolve_live_table(
            sql_table="orders", live_tables={"orders": live}, dialect=POSTGRES
        ) is live

    def test_schema_qualified_matches_the_qualified_key(self) -> None:
        # The live map is keyed by the qualified form
        # (``_collect_live_tables``), and a qualified sql_table is NO LONGER
        # stripped to a bare same-named object — that masked a dropped
        # non-default table behind its default twin. It matches the qualified
        # key directly.
        live = self._live("orders")
        assert _resolve_live_table(
            sql_table="public.orders", live_tables={"public.orders": live}, dialect=POSTGRES
        ) is live

    def test_schema_qualified_does_not_fall_back_to_a_bare_twin(self) -> None:
        # The removed bug: public.orders must NOT resolve to a bare ``orders``
        # key (a different, default-schema table).
        live = self._live("orders")
        assert _resolve_live_table(
            sql_table="public.orders", live_tables={"orders": live}, dialect=POSTGRES
        ) is None

    def test_quoted_table_part_strips_quotes(self) -> None:
        live = self._live("Company")
        # prod."Company" resolves against the per-segment-unquoted qualified key.
        assert _resolve_live_table(
            sql_table='prod."Company"', live_tables={"prod.Company": live}, dialect=POSTGRES
        ) is live

    def test_both_parts_quoted(self) -> None:
        live = self._live("Company")
        assert _resolve_live_table(
            sql_table='"prod"."Company"', live_tables={"prod.Company": live}, dialect=POSTGRES
        ) is live

    def test_escaped_inner_quote_in_table(self) -> None:
        live = self._live('Comp"any')
        assert _resolve_live_table(
            sql_table='prod."Comp""any"', live_tables={'prod."Comp""any"': live}, dialect=POSTGRES
        ) is live

    def test_missing_returns_none(self) -> None:
        assert _resolve_live_table(
            sql_table='prod."Missing"', live_tables={"orders": self._live("orders")}, dialect=POSTGRES
        ) is None
