"""Ingestion, drift, and MCP listing must all see views."""
from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import sqlalchemy as sa

from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.enums import DataType
from slayer.engine.ingestion import (
    IngestableObject,
    ingest_datasource,
    list_ingestable_objects,
)
from slayer.engine.schema_drift import (
    ModelAddition,
    WholeModelDelete,
    validate_datasource,
)
from slayer.engine.ingestion import _print_ingest_addition


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _db_with_view(workspace: Path) -> tuple[str, DatasourceConfig]:
    """A table plus a view over it — the dbt staging-model shape."""
    db_path = str(workspace / "live.db")
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            amount REAL NOT NULL,
            status TEXT NOT NULL
        );
        INSERT INTO orders VALUES (1, 100.0, 'completed');
        CREATE VIEW stg_orders AS
            SELECT id, amount, status FROM orders WHERE status = 'completed';
        """
    )
    conn.commit()
    conn.close()
    return db_path, DatasourceConfig(name="ds", type="sqlite", database=db_path)


def _mock_inspector(
    *,
    tables: list[str],
    views: list[str] | Exception | None = None,
    matviews: list[str] | Exception | None = None,
) -> MagicMock:
    """An Inspector stub whose view accessors can raise, for dialects lacking them."""
    insp = MagicMock(spec=sa.engine.Inspector)
    insp.get_table_names.return_value = list(tables)

    def _maybe(value):
        def _call(*_args, **_kwargs):
            if isinstance(value, Exception):
                raise value
            # ``or []``: an omitted accessor means "no objects", not list(None)
            return list(value or [])
        return _call

    insp.get_view_names.side_effect = _maybe(views)
    insp.get_materialized_view_names.side_effect = _maybe(matviews)
    return insp


def _names(objects: list[IngestableObject]) -> list[str]:
    return [o.name for o in objects]


def _kind_of(objects: list[IngestableObject], name: str) -> str | None:
    for o in objects:
        if o.name == name:
            return o.kind
    return None


# ---------------------------------------------------------------------------
# views are ingested by default, suppressed by include_views=False
# ---------------------------------------------------------------------------


class TestViewIngestion:
    def test_view_is_ingested_by_default(self, workspace: Path) -> None:
        """Headline regression: before the fix this returned only ``orders``."""
        _, ds = _db_with_view(workspace)
        models = ingest_datasource(datasource=ds)
        by_name = {m.name: m for m in models}
        assert "orders" in by_name
        assert "stg_orders" in by_name, "view was not ingested"
        assert by_name["stg_orders"].sql_table == "stg_orders"

    def test_include_views_false_suppresses_views(self, workspace: Path) -> None:
        """the ``--no-views`` escape hatch."""
        _, ds = _db_with_view(workspace)
        models = ingest_datasource(datasource=ds, include_views=False)
        names = {m.name for m in models}
        assert "orders" in names
        assert "stg_orders" not in names

    def test_view_model_has_columns(self, workspace: Path) -> None:
        """A view-backed model is a real model, not an empty shell."""
        _, ds = _db_with_view(workspace)
        models = ingest_datasource(datasource=ds)
        view_model = next(m for m in models if m.name == "stg_orders")
        assert {c.name for c in view_model.columns} == {"id", "amount", "status"}

    def test_view_model_has_no_joins(self, workspace: Path) -> None:
        """Views carry no FK metadata, so no joins can be derived."""
        _, ds = _db_with_view(workspace)
        models = ingest_datasource(datasource=ds)
        view_model = next(m for m in models if m.name == "stg_orders")
        assert view_model.joins == []


# ---------------------------------------------------------------------------
# dialect tolerance — unimplemented accessors and duplicate names
# ---------------------------------------------------------------------------


class TestDialectTolerance:
    def test_get_view_names_not_implemented_is_survivable(self) -> None:
        """Base Inspector raises NotImplementedError; tables must still list."""
        insp = _mock_inspector(tables=["orders"], views=NotImplementedError())
        objects = list_ingestable_objects(inspector=insp, schema=None)
        assert _names(objects) == ["orders"]

    def test_get_materialized_view_names_not_implemented_is_survivable(self) -> None:
        """The base get_materialized_view_names raises rather than returning []."""
        insp = _mock_inspector(
            tables=["orders"], views=["v_orders"], matviews=NotImplementedError()
        )
        objects = list_ingestable_objects(inspector=insp, schema=None)
        assert _names(objects) == ["orders", "v_orders"]

    def test_arbitrary_accessor_failure_is_survivable(self) -> None:
        """A non-NotImplementedError from an accessor must not abort the scan."""
        insp = _mock_inspector(
            tables=["orders"], views=RuntimeError("no view privilege")
        )
        objects = list_ingestable_objects(inspector=insp, schema=None)
        assert _names(objects) == ["orders"]

    def test_name_returned_as_both_table_and_view_appears_once(self) -> None:
        """A name from both table and view accessors: the view listing is authoritative."""
        insp = _mock_inspector(tables=["orders", "v_dup"], views=["v_dup"])
        objects = list_ingestable_objects(inspector=insp, schema=None)
        assert _names(objects) == ["orders", "v_dup"]
        assert _kind_of(objects, "v_dup") == "view"

    def test_matview_also_listed_as_view_appears_once(self) -> None:
        """Most-specific wins: a view+matview dup is a materialized view."""
        insp = _mock_inspector(tables=[], views=["mv"], matviews=["mv"])
        objects = list_ingestable_objects(inspector=insp, schema=None)
        assert _names(objects) == ["mv"]
        assert _kind_of(objects, "mv") == "materialized_view"

    def test_name_in_all_three_listings_is_a_materialized_view(self) -> None:
        """Some dialects list a matview from every accessor; kind must not degrade."""
        insp = _mock_inspector(tables=["mv"], views=["mv"], matviews=["mv"])
        objects = list_ingestable_objects(inspector=insp, schema=None)
        assert _names(objects) == ["mv"]
        assert _kind_of(objects, "mv") == "materialized_view"

    def test_include_views_false_keeps_table_kind_for_dup(self) -> None:
        """Without view discovery there is nothing to reclassify against."""
        insp = _mock_inspector(tables=["v_dup"], views=["v_dup"])
        objects = list_ingestable_objects(
            inspector=insp, schema=None, include_views=False
        )
        assert _kind_of(objects, "v_dup") == "table"

    def test_ordering_is_tables_then_views_then_matviews(self) -> None:
        """Deterministic order matters: name-collision reservation needs a stable scan order."""
        insp = _mock_inspector(
            tables=["t1", "t2"], views=["v1"], matviews=["m1"]
        )
        objects = list_ingestable_objects(inspector=insp, schema=None)
        assert _names(objects) == ["t1", "t2", "v1", "m1"]
        assert [o.kind for o in objects] == [
            "table", "table", "view", "materialized_view",
        ]

    def test_include_views_false_skips_accessors_entirely(self) -> None:
        insp = _mock_inspector(tables=["orders"], views=["v_orders"])
        objects = list_ingestable_objects(
            inspector=insp, schema=None, include_views=False
        )
        assert _names(objects) == ["orders"]
        insp.get_view_names.assert_not_called()
        insp.get_materialized_view_names.assert_not_called()

    def test_schema_is_forwarded_to_every_accessor(self) -> None:
        """Schema-qualified scans must not silently fall back to the default
        schema for views."""
        insp = _mock_inspector(tables=["orders"], views=["v"], matviews=["m"])
        list_ingestable_objects(inspector=insp, schema="analytics")
        insp.get_table_names.assert_called_once_with(schema="analytics")
        insp.get_view_names.assert_called_once_with(schema="analytics")
        insp.get_materialized_view_names.assert_called_once_with(schema="analytics")


# ---------------------------------------------------------------------------
# include/exclude filters apply to views identically
# ---------------------------------------------------------------------------


class TestFilters:
    def test_include_tables_filters_views_too(self, workspace: Path) -> None:
        _, ds = _db_with_view(workspace)
        models = ingest_datasource(datasource=ds, include_tables=["stg_orders"])
        assert {m.name for m in models} == {"stg_orders"}

    def test_exclude_tables_filters_views_too(self, workspace: Path) -> None:
        _, ds = _db_with_view(workspace)
        models = ingest_datasource(datasource=ds, exclude_tables=["stg_orders"])
        assert {m.name for m in models} == {"orders"}


# ---------------------------------------------------------------------------
# drift must resolve view-backed models — the pre-existing data-loss bug
# ---------------------------------------------------------------------------


class TestDriftSeesViews:
    async def test_hand_authored_view_model_is_not_marked_for_deletion(
        self, workspace: Path
    ) -> None:
        """A model pointing at a view resolved to live_table=None and got a WholeModelDelete."""
        _, ds = _db_with_view(workspace)
        model = SlayerModel(
            name="stg_orders",
            sql_table="stg_orders",
            data_source="ds",
            columns=[
                Column(name="id", sql="id", type=DataType.INT, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
        )
        entries = await validate_datasource(datasource=ds, models=[model])
        whole = [e for e in entries if isinstance(e, WholeModelDelete)]
        assert whole == [], f"view-backed model marked for deletion: {whole}"

    async def test_model_pointing_at_a_genuinely_missing_object_still_deletes(
        self, workspace: Path
    ) -> None:
        """The fix must not blind drift to genuinely dropped objects."""
        _, ds = _db_with_view(workspace)
        model = SlayerModel(
            name="gone",
            sql_table="gone",
            data_source="ds",
            columns=[Column(name="id", sql="id", type=DataType.INT)],
        )
        entries = await validate_datasource(datasource=ds, models=[model])
        assert any(isinstance(e, WholeModelDelete) for e in entries)

    def test_drift_sees_views_unconditionally(self, workspace: Path) -> None:
        """The drift side takes no include_views flag, so --no-views can't re-arm the bug."""
        import inspect as _inspect

        from slayer.engine.schema_drift import _live_schema_for_datasource

        params = _inspect.signature(_live_schema_for_datasource).parameters
        assert "include_views" not in params, (
            "drift introspection must not be gated on the ingest views flag"
        )

        _, ds = _db_with_view(workspace)
        live = _live_schema_for_datasource(datasource=ds)
        assert "stg_orders" in live


# ---------------------------------------------------------------------------
# MCP listing surface
# ---------------------------------------------------------------------------


class TestMcpListing:
    def test_fetch_tables_includes_views(self, workspace: Path) -> None:
        """describe_datasource hid views; the empty-ingest probe shares this helper."""
        from slayer.mcp.server import _fetch_tables

        _, ds = _db_with_view(workspace)
        objects, err = _fetch_tables(ds=ds)
        assert err is None
        assert objects is not None
        by_name = {o.name: o.kind for o in objects}
        assert "orders" in by_name
        assert "stg_orders" in by_name
        # Each object keeps its kind so describe_datasource can label a view
        # instead of presenting it as a bare table name.
        assert by_name["orders"] == "table"
        assert by_name["stg_orders"] == "view"


# ---------------------------------------------------------------------------
# ingest output labels non-table objects
# ---------------------------------------------------------------------------


class TestOutputLabel:
    def test_created_view_is_labelled(self, capsys) -> None:
        addition = ModelAddition(
            model_name="stg_orders",
            data_source="ds",
            created=True,
            new_columns=["id", "amount"],
            source_kind="view",
        )
        _print_ingest_addition(addition)
        out = capsys.readouterr().out
        assert "stg_orders" in out
        assert "[view]" in out

    def test_created_matview_is_labelled(self, capsys) -> None:
        addition = ModelAddition(
            model_name="mv_orders",
            data_source="ds",
            created=True,
            new_columns=["id"],
            source_kind="materialized_view",
        )
        _print_ingest_addition(addition)
        assert "[materialized view]" in capsys.readouterr().out

    def test_created_table_is_not_labelled(self, capsys) -> None:
        addition = ModelAddition(
            model_name="orders",
            data_source="ds",
            created=True,
            new_columns=["id"],
            source_kind="table",
        )
        _print_ingest_addition(addition)
        out = capsys.readouterr().out
        assert "[view]" not in out
        assert "[materialized view]" not in out

    def test_unknown_source_kind_is_not_labelled(self, capsys) -> None:
        """source_kind=None (e.g. a pre-existing model) renders as before."""
        addition = ModelAddition(
            model_name="orders", data_source="ds", created=True, new_columns=["id"]
        )
        _print_ingest_addition(addition)
        assert "[" not in capsys.readouterr().out
