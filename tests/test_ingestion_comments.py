"""Tests for importing DB column/table/dataset comments during ingestion.

Column comments land on ``Column.description``, table comments on
``SlayerModel.description``, and (BigQuery only) the dataset description on
``DatasourceConfig.description`` — always fill-if-empty, never overwriting.
"""

from __future__ import annotations

import argparse
import io
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb
import pytest
import sqlalchemy as sa

import slayer.engine.ingestion as ingestion_mod
from slayer.async_utils import run_sync
from slayer.cli import _run_datasources_create
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine.ingestion import (
    DatasourceIngestOutput,
    IntrospectedColumn,
    _additive_merge_existing,
    _fetch_bigquery_dataset_description,
    _ingest_datasource_full,
    _print_ingest_addition,
    _safe_get_table_comment,
    _sqlite_probe_integer_columns,
    ingest_datasource,
    ingest_datasource_idempotent,
    introspect_table_to_model,
)
from slayer.engine.introspect_utils import (
    _clean_comment,
    _get_column_comments_fallback,
    _get_columns_fallback,
)
from slayer.engine.schema_drift import IdempotentIngestResult, ModelAddition
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_engine(dialect_name: str = "postgresql") -> MagicMock:
    # No spec: sa.Engine sets ``dialect`` in __init__, so a spec'd mock
    # rejects it.
    engine = MagicMock()
    engine.dialect.name = dialect_name
    return engine


def _mock_conn_engine(dialect_name: str, execute_side_effect) -> tuple[MagicMock, MagicMock]:
    """Engine whose ``connect()`` context yields a conn with stubbed execute."""
    engine = _mock_engine(dialect_name)
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    if callable(execute_side_effect):
        conn.execute.side_effect = execute_side_effect
    else:
        conn.execute.return_value.fetchall.return_value = execute_side_effect
    return engine, conn


def _mock_inspector(
    columns: list[dict],
    *,
    pk: list[str] | None = None,
    table_comment: object = None,
) -> MagicMock:
    inspector = MagicMock(spec=sa.engine.Inspector)
    inspector.get_columns.return_value = columns
    inspector.get_pk_constraint.return_value = {"constrained_columns": pk or []}
    if isinstance(table_comment, Exception):
        inspector.get_table_comment.side_effect = table_comment
    else:
        inspector.get_table_comment.return_value = {"text": table_comment}
    return inspector


def _commented_duckdb(path: Path) -> None:
    conn = duckdb.connect(str(path))
    conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, region VARCHAR)")
    conn.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            amount DOUBLE,
            status VARCHAR,
            customer_id INTEGER REFERENCES customers(id)
        )
        """
    )
    conn.execute("COMMENT ON TABLE orders IS 'All orders'")
    conn.execute("COMMENT ON COLUMN orders.amount IS 'Order amount in USD'")
    conn.execute("COMMENT ON COLUMN customers.region IS 'Sales region'")
    conn.execute("INSERT INTO customers VALUES (1, 'US')")
    conn.execute("INSERT INTO orders VALUES (1, 100.0, 'completed', 1)")
    conn.close()


def _model_by_name(models: list[SlayerModel], name: str) -> SlayerModel:
    return next(m for m in models if m.name == name)


def _col(model: SlayerModel, name: str) -> Column:
    return next(c for c in model.columns if c.name == name)


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


class TestCleanComment:
    def test_strips_whitespace(self) -> None:
        assert _clean_comment("  order id  ") == "order id"

    def test_empty_string_is_none(self) -> None:
        assert _clean_comment("") is None

    def test_whitespace_only_is_none(self) -> None:
        assert _clean_comment("   \n\t ") is None

    def test_none_passthrough(self) -> None:
        assert _clean_comment(None) is None


# ---------------------------------------------------------------------------
# Inspector path: column + table comments
# ---------------------------------------------------------------------------


class TestInspectorPathComments:
    def test_column_comments_land_on_descriptions(self) -> None:
        inspector = _mock_inspector(
            [
                {"name": "id", "type": sa.INTEGER(), "comment": "row id"},
                {"name": "amount", "type": sa.NUMERIC(10, 2), "comment": None},
                {"name": "note", "type": sa.TEXT()},  # no comment key at all
            ],
            pk=["id"],
            table_comment="Orders table",
        )
        model = introspect_table_to_model(
            sa_engine=_mock_engine(),
            inspector=inspector,
            table_name="orders",
            schema=None,
            data_source="ds",
        )
        assert model.description == "Orders table"
        assert _col(model, "id").description == "row id"
        assert _col(model, "amount").description is None
        assert _col(model, "note").description is None

    def test_whitespace_comment_normalized_to_none(self) -> None:
        inspector = _mock_inspector(
            [{"name": "id", "type": sa.INTEGER(), "comment": "   "}],
            table_comment="  ",
        )
        model = introspect_table_to_model(
            sa_engine=_mock_engine(),
            inspector=inspector,
            table_name="t",
            schema=None,
            data_source="ds",
        )
        assert model.description is None
        assert _col(model, "id").description is None

    def test_count_rename_keeps_description(self) -> None:
        inspector = _mock_inspector(
            [{"name": "_count", "type": sa.INTEGER(), "comment": "collides"}],
        )
        model = introspect_table_to_model(
            sa_engine=_mock_engine(),
            inspector=inspector,
            table_name="t",
            schema=None,
            data_source="ds",
        )
        assert _col(model, "count_col").description == "collides"


class TestSafeGetTableComment:
    def test_returns_text(self) -> None:
        inspector = _mock_inspector([], table_comment="hello")
        assert _safe_get_table_comment(inspector, "t", None) == "hello"

    def test_none_text(self) -> None:
        inspector = _mock_inspector([], table_comment=None)
        assert _safe_get_table_comment(inspector, "t", None) is None

    def test_not_implemented_is_skipped(self) -> None:
        inspector = _mock_inspector([], table_comment=NotImplementedError())
        assert _safe_get_table_comment(inspector, "t", None) is None

    def test_arbitrary_error_is_skipped(self) -> None:
        inspector = _mock_inspector([], table_comment=RuntimeError("boom"))
        assert _safe_get_table_comment(inspector, "t", None) is None

    def test_whitespace_normalized(self) -> None:
        inspector = _mock_inspector([], table_comment="  x ")
        assert _safe_get_table_comment(inspector, "t", None) == "x"


class TestSqliteProbePreservesComments:
    def test_non_sqlite_engine_keeps_comments(self) -> None:
        cols = [IntrospectedColumn(name="a", type=DataType.INT, comment="kept")]
        out = _sqlite_probe_integer_columns(
            sa_engine=_mock_engine("postgresql"), sql_table="t", columns=cols
        )
        assert out[0].comment == "kept"

    def test_widened_column_keeps_comment(self) -> None:
        engine = sa.create_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            conn.execute(sa.text("CREATE TABLE t (a INTEGER)"))
            conn.execute(sa.text("INSERT INTO t VALUES (1.5)"))
            conn.commit()
        cols = [IntrospectedColumn(name="a", type=DataType.INT, comment="probed")]
        out = _sqlite_probe_integer_columns(sa_engine=engine, sql_table="t", columns=cols)
        assert out[0].type is DataType.DOUBLE
        assert out[0].comment == "probed"


# ---------------------------------------------------------------------------
# information_schema fallback: per-dialect comment SQL
# ---------------------------------------------------------------------------


class TestColumnCommentsFallback:
    @pytest.mark.parametrize(
        ("dialect", "source_marker"),
        [
            ("mysql", "column_comment"),
            ("mariadb", "column_comment"),
            ("snowflake", "information_schema"),
            ("clickhouse", "system.columns"),
            ("duckdb", "duckdb_columns"),
            ("postgresql", "col_description"),
        ],
    )
    def test_dialect_query_shape(self, dialect: str, source_marker: str) -> None:
        engine, conn = _mock_conn_engine(dialect, [("id", "row id"), ("x", None)])
        result = _get_column_comments_fallback(
            sa_engine=engine, table_name="orders", schema=None
        )
        assert result == {"id": "row id"}
        args, kwargs = conn.execute.call_args
        sql_str = str(args[0]).lower()
        assert source_marker in sql_str
        params = args[1] if len(args) > 1 else kwargs
        assert "orders" in params.values()
        assert "orders" not in sql_str

    @pytest.mark.parametrize(
        "dialect", ["mysql", "snowflake", "clickhouse", "duckdb", "postgresql"]
    )
    def test_schema_is_bound_not_interpolated(self, dialect: str) -> None:
        engine, conn = _mock_conn_engine(dialect, [("id", "row id")])
        result = _get_column_comments_fallback(
            sa_engine=engine, table_name="orders", schema="s1"
        )
        assert result == {"id": "row id"}
        args, kwargs = conn.execute.call_args
        sql_str = str(args[0])
        params = args[1] if len(args) > 1 else kwargs
        assert "s1" in params.values()
        assert "s1" not in sql_str

    def test_clickhouse_defaults_to_current_database(self) -> None:
        engine, conn = _mock_conn_engine("clickhouse", [("id", "c")])
        _get_column_comments_fallback(sa_engine=engine, table_name="t", schema=None)
        sql_str = str(conn.execute.call_args[0][0]).lower()
        assert "currentdatabase()" in sql_str

    def test_no_literal_interpolation(self) -> None:
        engine, conn = _mock_conn_engine("mysql", [])
        _get_column_comments_fallback(
            sa_engine=engine,
            table_name="'; DROP TABLE users;--",
            schema="'; DROP TABLE users;--",
        )
        args, _ = conn.execute.call_args
        assert "DROP TABLE" not in str(args[0])

    def test_unknown_dialect_returns_empty(self) -> None:
        engine, conn = _mock_conn_engine("mssql", [("id", "x")])
        assert _get_column_comments_fallback(
            sa_engine=engine, table_name="t", schema=None
        ) == {}
        conn.execute.assert_not_called()

    def test_query_error_returns_empty(self) -> None:
        def _boom(*args, **kwargs):
            raise RuntimeError("no such view")

        engine, _ = _mock_conn_engine("duckdb", _boom)
        assert _get_column_comments_fallback(
            sa_engine=engine, table_name="t", schema=None
        ) == {}

    def test_blank_comments_filtered(self) -> None:
        engine, _ = _mock_conn_engine("mysql", [("a", ""), ("b", "  "), ("c", "ok")])
        assert _get_column_comments_fallback(
            sa_engine=engine, table_name="t", schema=None
        ) == {"c": "ok"}

    def test_generic_fallback_merges_comments(self) -> None:
        def _dispatch(clause, params=None):
            res = MagicMock()
            if "duckdb_columns" in str(clause):
                res.fetchall.return_value = [("id", "row id")]
            else:
                res.fetchall.return_value = [("id", "INTEGER"), ("x", "VARCHAR")]
            return res

        engine, _ = _mock_conn_engine("duckdb", _dispatch)
        cols = _get_columns_fallback(sa_engine=engine, table_name="t", schema=None)
        by_name = {c["name"]: c for c in cols}
        assert by_name["id"]["comment"] == "row id"
        assert by_name["x"].get("comment") is None


# ---------------------------------------------------------------------------
# DuckDB end-to-end (real engine — exercises the genuine fallback path)
# ---------------------------------------------------------------------------


class TestDuckDBEndToEnd:
    @pytest.fixture()
    def duckdb_models(self, tmp_path: Path) -> list[SlayerModel]:
        db_path = tmp_path / "commented.duckdb"
        _commented_duckdb(db_path)
        ds = DatasourceConfig(name="ds", type="duckdb", database=str(db_path))
        return ingest_datasource(datasource=ds)

    def test_table_comment_becomes_model_description(self, duckdb_models) -> None:
        assert _model_by_name(duckdb_models, "orders").description == "All orders"

    def test_column_comments_become_descriptions(self, duckdb_models) -> None:
        orders = _model_by_name(duckdb_models, "orders")
        assert _col(orders, "amount").description == "Order amount in USD"
        customers = _model_by_name(duckdb_models, "customers")
        assert _col(customers, "region").description == "Sales region"

    def test_uncommented_stays_none(self, duckdb_models) -> None:
        orders = _model_by_name(duckdb_models, "orders")
        assert _col(orders, "status").description is None
        assert _model_by_name(duckdb_models, "customers").description is None


# ---------------------------------------------------------------------------
# Idempotent merge: fill-if-empty + reporting
# ---------------------------------------------------------------------------


async def _duckdb_idempotent_setup(tmp_path: Path) -> tuple[YAMLStorage, DatasourceConfig]:
    db_path = tmp_path / "live.duckdb"
    _commented_duckdb(db_path)
    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    ds = DatasourceConfig(name="ds", type="duckdb", database=str(db_path))
    await storage.save_datasource(ds)
    return storage, ds


def _addition_for(name: str, additions) -> ModelAddition | None:
    return next((a for a in additions if a.model_name == name), None)


class TestIdempotentDescriptionFill:
    async def test_created_model_reports_descriptions(self, tmp_path: Path) -> None:
        storage, ds = await _duckdb_idempotent_setup(tmp_path)
        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        addition = _addition_for("orders", result.additions)
        assert addition is not None and addition.created
        assert "amount" in addition.described_columns
        assert addition.model_described is True
        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded.description == "All orders"
        assert _col(loaded, "amount").description == "Order amount in USD"

    async def test_fill_if_empty_and_preserve_user_text(self, tmp_path: Path) -> None:
        storage, ds = await _duckdb_idempotent_setup(tmp_path)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        loaded = await storage.get_model("orders", data_source="ds")
        loaded.description = None
        for c in loaded.columns:
            if c.name == "amount":
                c.description = None
            if c.name == "status":
                c.description = "hand-authored"
        await storage.save_model(loaded)

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        addition = _addition_for("orders", result.additions)
        assert addition is not None and not addition.created
        assert addition.described_columns == ["amount"]
        assert addition.model_described is True
        assert addition.new_columns == []

        loaded2 = await storage.get_model("orders", data_source="ds")
        assert loaded2.description == "All orders"
        assert _col(loaded2, "amount").description == "Order amount in USD"
        assert _col(loaded2, "status").description == "hand-authored"

    async def test_reingest_is_noop_when_descriptions_present(self, tmp_path: Path) -> None:
        storage, ds = await _duckdb_idempotent_setup(tmp_path)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        for addition in result.additions:
            assert addition.described_columns == []
            assert addition.model_described is False

    async def test_new_commented_column_arrives_with_description(
        self, tmp_path: Path
    ) -> None:
        storage, ds = await _duckdb_idempotent_setup(tmp_path)
        await ingest_datasource_idempotent(datasource=ds, storage=storage)
        conn = duckdb.connect(ds.database)
        conn.execute("ALTER TABLE orders ADD COLUMN discount DOUBLE")
        conn.execute("COMMENT ON COLUMN orders.discount IS 'Discount applied'")
        conn.close()

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        addition = _addition_for("orders", result.additions)
        assert addition is not None
        assert "discount" in addition.new_columns
        # New columns carry their description implicitly — not double-counted.
        assert "discount" not in addition.described_columns
        loaded = await storage.get_model("orders", data_source="ds")
        assert _col(loaded, "discount").description == "Discount applied"


class TestAdditiveMergeDescriptions:
    def _persisted(self, description=None, col_description=None) -> SlayerModel:
        return SlayerModel(
            name="t",
            sql_table="t",
            data_source="ds",
            description=description,
            columns=[
                Column(name="a", sql="a", type=DataType.INT, description=col_description)
            ],
        )

    def _fresh(self) -> SlayerModel:
        return SlayerModel(
            name="t",
            sql_table="t",
            data_source="ds",
            description="fresh model desc",
            columns=[
                Column(name="a", sql="a", type=DataType.INT, description="fresh col desc")
            ],
        )

    def test_fills_empty_descriptions(self) -> None:
        merged, new_cols, new_joins, widened, described, model_described = (
            _additive_merge_existing(persisted=self._persisted(), fresh=self._fresh())
        )
        assert described == ["a"]
        assert model_described is True
        assert merged.description == "fresh model desc"
        assert _col(merged, "a").description == "fresh col desc"
        assert new_cols == [] and new_joins == [] and widened == []

    def test_existing_descriptions_untouched(self) -> None:
        merged, _, _, _, described, model_described = _additive_merge_existing(
            persisted=self._persisted(description="mine", col_description="my col"),
            fresh=self._fresh(),
        )
        assert described == []
        assert model_described is False
        assert merged.description == "mine"
        assert _col(merged, "a").description == "my col"


# ---------------------------------------------------------------------------
# BigQuery dataset description
# ---------------------------------------------------------------------------


def _bq_engine(dataset_id: str | None, description: str | None = "ds desc"):
    """Mock BigQuery engine exposing the client the way sqlalchemy-bigquery does."""
    engine = _mock_engine("bigquery")
    engine.dialect.dataset_id = dataset_id
    client = MagicMock()
    client.get_dataset.return_value = SimpleNamespace(description=description)
    conn = MagicMock()
    conn.connection._client = client
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine, client


class TestFetchBigQueryDatasetDescription:
    def test_non_bigquery_dialect_returns_none(self) -> None:
        ds = DatasourceConfig(name="d", type="duckdb", database="x.db")
        assert (
            _fetch_bigquery_dataset_description(
                sa_engine=_mock_engine("duckdb"), datasource=ds, schema=None
            )
            is None
        )

    def test_explicit_schema_wins(self) -> None:
        engine, client = _bq_engine(dataset_id="dialect_ds")
        ds = DatasourceConfig(name="d", type="bigquery", schema_name="cfg_ds")
        out = _fetch_bigquery_dataset_description(
            sa_engine=engine, datasource=ds, schema="explicit_ds"
        )
        assert out == "ds desc"
        assert client.get_dataset.call_args[0][0] == "explicit_ds"

    def test_dialect_default_beats_schema_name(self) -> None:
        engine, client = _bq_engine(dataset_id="dialect_ds")
        ds = DatasourceConfig(name="d", type="bigquery", schema_name="cfg_ds")
        _fetch_bigquery_dataset_description(sa_engine=engine, datasource=ds, schema=None)
        assert client.get_dataset.call_args[0][0] == "dialect_ds"

    def test_schema_name_is_last_resort(self) -> None:
        engine, client = _bq_engine(dataset_id=None)
        ds = DatasourceConfig(name="d", type="bigquery", schema_name="cfg_ds")
        _fetch_bigquery_dataset_description(sa_engine=engine, datasource=ds, schema=None)
        assert client.get_dataset.call_args[0][0] == "cfg_ds"

    def test_unresolvable_dataset_returns_none(self) -> None:
        engine, client = _bq_engine(dataset_id=None)
        ds = DatasourceConfig(name="d", type="bigquery")
        assert (
            _fetch_bigquery_dataset_description(
                sa_engine=engine, datasource=ds, schema=None
            )
            is None
        )
        client.get_dataset.assert_not_called()

    def test_client_error_returns_none(self) -> None:
        engine, client = _bq_engine(dataset_id="d1")
        client.get_dataset.side_effect = RuntimeError("403")
        ds = DatasourceConfig(name="d", type="bigquery")
        assert (
            _fetch_bigquery_dataset_description(
                sa_engine=engine, datasource=ds, schema=None
            )
            is None
        )

    def test_whitespace_description_is_none(self) -> None:
        engine, _ = _bq_engine(dataset_id="d1", description="   ")
        ds = DatasourceConfig(name="d", type="bigquery")
        assert (
            _fetch_bigquery_dataset_description(
                sa_engine=engine, datasource=ds, schema=None
            )
            is None
        )


class TestIngestDatasourceFull:
    def test_returns_models_and_no_description_for_duckdb(self, tmp_path: Path) -> None:
        db_path = tmp_path / "x.duckdb"
        _commented_duckdb(db_path)
        ds = DatasourceConfig(name="ds", type="duckdb", database=str(db_path))
        out = _ingest_datasource_full(datasource=ds)
        assert isinstance(out, DatasourceIngestOutput)
        assert {m.name for m in out.models} == {"customers", "orders"}
        assert out.schema_description is None

    def test_wrapper_returns_plain_model_list(self, tmp_path: Path) -> None:
        db_path = tmp_path / "x.duckdb"
        _commented_duckdb(db_path)
        ds = DatasourceConfig(name="ds", type="duckdb", database=str(db_path))
        models = ingest_datasource(datasource=ds)
        assert isinstance(models, list)
        assert all(isinstance(m, SlayerModel) for m in models)

    def test_fetch_skipped_when_description_set(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fetch = MagicMock(return_value="never used")
        monkeypatch.setattr(
            ingestion_mod, "_fetch_bigquery_dataset_description", fetch
        )
        db_path = tmp_path / "x.duckdb"
        _commented_duckdb(db_path)
        ds = DatasourceConfig(
            name="ds", type="duckdb", database=str(db_path), description="already set"
        )
        out = _ingest_datasource_full(datasource=ds)
        fetch.assert_not_called()
        assert out.schema_description is None

    def test_fetch_called_when_description_empty(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fetch = MagicMock(return_value="dataset says hi")
        monkeypatch.setattr(
            ingestion_mod, "_fetch_bigquery_dataset_description", fetch
        )
        db_path = tmp_path / "x.duckdb"
        _commented_duckdb(db_path)
        ds = DatasourceConfig(name="ds", type="duckdb", database=str(db_path))
        out = _ingest_datasource_full(datasource=ds)
        fetch.assert_called_once()
        assert out.schema_description == "dataset says hi"


# ---------------------------------------------------------------------------
# Idempotent path: datasource description persistence + failure isolation
# ---------------------------------------------------------------------------


def _sqlite_live_db(tmp_path: Path) -> str:
    db_path = str(tmp_path / "live.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()
    return db_path


def _patch_full_ingest(monkeypatch: pytest.MonkeyPatch, description: str | None):
    real = ingestion_mod._ingest_datasource_full

    def _fake(*args, **kwargs):
        out = real(*args, **kwargs)
        return DatasourceIngestOutput(
            models=out.models, schema_description=description
        )

    monkeypatch.setattr(ingestion_mod, "_ingest_datasource_full", _fake)


class TestIdempotentDatasourceDescription:
    async def test_description_persisted_and_reported(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        db_path = _sqlite_live_db(tmp_path)
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
        await storage.save_datasource(ds)
        _patch_full_ingest(monkeypatch, "From the dataset")

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        assert result.datasource_described is True
        loaded = await storage.get_datasource("ds")
        assert loaded.description == "From the dataset"

        # Re-ingest with the reloaded datasource (as the CLI does): the
        # persisted description must make the second pass a no-op.
        result2 = await ingest_datasource_idempotent(datasource=loaded, storage=storage)
        assert result2.datasource_described is False
        reloaded = await storage.get_datasource("ds")
        assert reloaded.description == "From the dataset"

    async def test_existing_description_never_overwritten(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        db_path = _sqlite_live_db(tmp_path)
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        ds = DatasourceConfig(
            name="ds", type="sqlite", database=db_path, description="user text"
        )
        await storage.save_datasource(ds)
        _patch_full_ingest(monkeypatch, "From the dataset")

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        assert result.datasource_described is False
        loaded = await storage.get_datasource("ds")
        assert loaded.description == "user text"

    async def test_save_failure_is_isolated(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        db_path = _sqlite_live_db(tmp_path)

        class _FailingSave(YAMLStorage):
            async def save_datasource(self, datasource: DatasourceConfig) -> None:
                if getattr(self, "_armed", False):
                    raise RuntimeError("disk full")
                await super().save_datasource(datasource)

        storage = _FailingSave(base_dir=str(tmp_path / "storage"))
        ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
        await storage.save_datasource(ds)
        storage._armed = True
        _patch_full_ingest(monkeypatch, "From the dataset")

        result = await ingest_datasource_idempotent(datasource=ds, storage=storage)
        assert result.datasource_described is False
        assert any(
            e.model_name == "" and "datasource" in e.error.lower()
            for e in result.errors
        )
        # Model ingestion itself still succeeded.
        assert _addition_for("t", result.additions) is not None


# ---------------------------------------------------------------------------
# Report shape + rendering
# ---------------------------------------------------------------------------


class TestReportShape:
    def test_model_addition_defaults(self) -> None:
        addition = ModelAddition(model_name="m", data_source="ds")
        assert addition.described_columns == []
        assert addition.model_described is False

    def test_result_default(self) -> None:
        assert IdempotentIngestResult().datasource_described is False

    def test_print_updated_with_descriptions(self) -> None:
        addition = ModelAddition(
            model_name="orders",
            data_source="ds",
            described_columns=["amount", "status"],
            model_described=True,
        )
        buf = io.StringIO()
        _print_ingest_addition(addition, file=buf)
        out = buf.getvalue()
        assert "+descriptions: amount, status" in out
        assert "+model description" in out

    def test_print_created_with_descriptions(self) -> None:
        addition = ModelAddition(
            model_name="orders",
            data_source="ds",
            created=True,
            new_columns=["a", "b", "c"],
            described_columns=["a", "b"],
        )
        buf = io.StringIO()
        _print_ingest_addition(addition, file=buf)
        assert "Created: orders (3 columns, 2 described)" in buf.getvalue()

    def test_print_created_without_descriptions_unchanged(self) -> None:
        addition = ModelAddition(
            model_name="orders", data_source="ds", created=True, new_columns=["a"]
        )
        buf = io.StringIO()
        _print_ingest_addition(addition, file=buf)
        assert "Created: orders (1 columns)" in buf.getvalue()

    def test_description_only_update_is_printed(self) -> None:
        addition = ModelAddition(
            model_name="orders", data_source="ds", described_columns=["amount"]
        )
        buf = io.StringIO()
        _print_ingest_addition(addition, file=buf)
        assert "Updated: orders" in buf.getvalue()


# ---------------------------------------------------------------------------
# CLI `datasources create --ingest` wiring
# ---------------------------------------------------------------------------


class TestCliDatasourcesCreateIngest:
    def _args(self, db_path: str, description: str | None = None) -> argparse.Namespace:
        return argparse.Namespace(
            connection_string=f"sqlite:///{db_path}",
            name="ds",
            description=description,
            yes=True,
            ingest=True,
            include=None,
            exclude=None,
            schema=None,
        )

    def test_dataset_description_imported(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        db_path = _sqlite_live_db(tmp_path)
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        _patch_full_ingest(monkeypatch, "Imported dataset description")

        _run_datasources_create(self._args(db_path), storage)
        loaded = run_sync(storage.get_datasource("ds"))
        assert loaded.description == "Imported dataset description"
        assert run_sync(storage.get_model("t", data_source="ds")) is not None

    def test_user_description_wins(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        db_path = _sqlite_live_db(tmp_path)
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        _patch_full_ingest(monkeypatch, "Imported dataset description")

        _run_datasources_create(self._args(db_path, description="user says"), storage)
        loaded = run_sync(storage.get_datasource("ds"))
        assert loaded.description == "user says"


# ---------------------------------------------------------------------------
# BigQuery driver contract (credential-free — pins the installed package)
# ---------------------------------------------------------------------------


class TestBigQueryDriverContract:
    def test_get_columns_carries_field_description(self) -> None:
        pytest.importorskip("sqlalchemy_bigquery")
        from google.cloud.bigquery import SchemaField
        from sqlalchemy_bigquery._types import get_columns as bq_get_columns

        cols = bq_get_columns([SchemaField("id", "INTEGER", description="row id")])
        assert cols[0]["comment"] == "row id"

    def test_get_table_comment_returns_table_description(self) -> None:
        pytest.importorskip("sqlalchemy_bigquery")
        from sqlalchemy_bigquery import BigQueryDialect

        dialect = BigQueryDialect()
        dialect._get_table = MagicMock(
            return_value=SimpleNamespace(description="tbl desc")
        )
        out = dialect.get_table_comment(MagicMock(), "t")
        assert out == {"text": "tbl desc"}

    def test_real_dialect_reflection_flows_into_slayer_model(self) -> None:
        """Run the REAL BigQueryDialect reflection over a locally built
        Table — only the network call is mocked — and feed its genuine
        output through introspect_table_to_model."""
        pytest.importorskip("sqlalchemy_bigquery")
        from google.cloud.bigquery import SchemaField, Table
        from sqlalchemy_bigquery import BigQueryDialect

        table = Table("proj.dset.orders", schema=[
            SchemaField("id", "INTEGER", description="row id"),
            SchemaField("amount", "FLOAT", description="order amount"),
            SchemaField("status", "STRING"),
        ])
        table.description = "All orders"

        dialect = BigQueryDialect()
        with patch.object(BigQueryDialect, "_get_table", return_value=table):
            real_cols = dialect.get_columns(MagicMock(), "orders")
            real_comment = dialect.get_table_comment(MagicMock(), "orders")

        inspector = MagicMock(spec=sa.engine.Inspector)
        inspector.get_columns.return_value = real_cols
        inspector.get_pk_constraint.return_value = {"constrained_columns": []}
        inspector.get_table_comment.return_value = real_comment

        model = introspect_table_to_model(
            sa_engine=_mock_engine("bigquery"),
            inspector=inspector,
            table_name="orders",
            schema="dset",
            data_source="bq",
        )
        assert model.description == "All orders"
        cols = {c.name: c for c in model.columns}
        assert cols["id"].description == "row id"
        assert cols["id"].type is DataType.INT
        assert cols["amount"].description == "order amount"
        assert cols["amount"].type is DataType.DOUBLE
        assert cols["status"].description is None
