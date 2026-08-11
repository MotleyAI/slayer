"""`slayer ingest` must not be silent: skips or an empty scan exit 1, but `POST /ingest` keeps 422 for errors only."""
from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from slayer.engine.ingestion import (
    SkippedTable,
    _empty_ingest_message,
    _print_ingest_drift_and_errors,
)
from slayer.engine.schema_drift import IdempotentIngestResult, ModelAddition


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


def _args(workspace: Path, **overrides) -> SimpleNamespace:
    base = dict(
        datasource="ds",
        schema=None,
        include=None,
        exclude=None,
        include_views=True,
        storage=str(workspace / "storage"),
        models_dir=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _patch_ingest(monkeypatch, result: IdempotentIngestResult) -> None:
    """Stub the engine call so these tests pin CLI behaviour only."""
    async def _fake(**_kwargs):
        return result

    monkeypatch.setattr(
        "slayer.engine.ingestion.ingest_datasource_idempotent", _fake
    )
    monkeypatch.setattr(
        "slayer.cli._resolve_storage",
        lambda _args: SimpleNamespace(
            get_datasource=_async_return(
                SimpleNamespace(name="ds", type="sqlite", database=":memory:")
            )
        ),
    )


def _async_return(value):
    async def _call(*_args, **_kwargs):
        return value
    return _call


# ---------------------------------------------------------------------------
# slayer ingest exit codes
# ---------------------------------------------------------------------------


class TestIngestExitCodes:
    def test_skipped_objects_exit_nonzero(
        self, workspace: Path, monkeypatch, capsys
    ) -> None:
        """a skipped object is a valid table we declined to model."""
        from slayer.cli import _run_ingest

        _patch_ingest(
            monkeypatch,
            IdempotentIngestResult(
                additions=[
                    ModelAddition(
                        model_name="orders", data_source="ds", created=True,
                        new_columns=["id"],
                    )
                ],
                to_delete=[],
                errors=[],
                skipped=[
                    SkippedTable(
                        table_name="reports__patient__drug",
                        reason="name collision with existing table 'reports_patient_drug'",
                    )
                ],
            ),
        )

        args = _args(workspace)
        with pytest.raises(SystemExit) as exc:
            _run_ingest(args)
        assert exc.value.code == 1

        out = capsys.readouterr().out
        assert "reports__patient__drug" in out
        assert "--exclude" in out, "the skip message must name the remedy"

    def test_clean_run_exits_zero_with_no_skip_section(
        self, workspace: Path, monkeypatch, capsys
    ) -> None:
        from slayer.cli import _run_ingest

        _patch_ingest(
            monkeypatch,
            IdempotentIngestResult(
                additions=[
                    ModelAddition(
                        model_name="orders", data_source="ds", created=True,
                        new_columns=["id"],
                    )
                ],
                to_delete=[],
                errors=[],
                skipped=[],
            ),
        )
        _run_ingest(_args(workspace))
        out = capsys.readouterr().out
        assert "Created: orders" in out
        assert "Skipped" not in out

    def test_zero_objects_prints_message_and_exits_nonzero(
        self, workspace: Path, monkeypatch, capsys
    ) -> None:
        """the reported silence. Before the fix: no output, exit 0."""
        from slayer.cli import _run_ingest

        _patch_ingest(
            monkeypatch,
            IdempotentIngestResult(
                additions=[], to_delete=[], errors=[], skipped=[], objects=[]
            ),
        )

        args = _args(workspace, schema="analytics")
        with pytest.raises(SystemExit) as exc:
            _run_ingest(args)
        assert exc.value.code == 1

        out = capsys.readouterr().out
        assert out.strip(), "ingest must not be silent on an empty result"
        assert "analytics" in out
        assert "no tables or views found" in out.lower()

    def test_errors_still_exit_nonzero(
        self, workspace: Path, monkeypatch
    ) -> None:
        """Pre-existing behaviour must not regress."""
        from slayer.cli import _run_ingest
        from slayer.engine.schema_drift import IngestionError

        _patch_ingest(
            monkeypatch,
            IdempotentIngestResult(
                additions=[],
                to_delete=[],
                errors=[
                    IngestionError(
                        model_name="orders", data_source="ds", error="boom"
                    )
                ],
                skipped=[],
                objects=[],
            ),
        )
        args = _args(workspace)
        with pytest.raises(SystemExit) as exc:
            _run_ingest(args)
        assert exc.value.code == 1


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------


class TestRenderers:
    def test_skipped_section_lists_every_entry_with_its_reason(
        self, capsys
    ) -> None:
        result = IdempotentIngestResult(
            additions=[],
            to_delete=[],
            errors=[],
            skipped=[
                SkippedTable(table_name="a__b", reason="name collision with 'a_b'"),
                SkippedTable(
                    table_name="weird.table",
                    reason="'.' in name is ambiguous with schema qualification",
                ),
            ],
        )
        _print_ingest_drift_and_errors(result)
        out = capsys.readouterr().out
        assert "a__b" in out
        assert "name collision" in out
        assert "weird.table" in out
        assert "ambiguous" in out

    def test_skipped_and_errors_are_reported_separately(self, capsys) -> None:
        """A skip and an error have different causes and fixes, so report them separately."""
        from slayer.engine.schema_drift import IngestionError

        result = IdempotentIngestResult(
            additions=[],
            to_delete=[],
            errors=[
                IngestionError(
                    model_name="orders", data_source="ds", error="disk full"
                )
            ],
            skipped=[SkippedTable(table_name="a__b", reason="name collision")],
        )
        _print_ingest_drift_and_errors(result)
        out = capsys.readouterr().out
        assert "disk full" in out
        assert "a__b" in out
        skipped_at = out.lower().index("skipped")
        errors_at = out.lower().index("error")
        assert skipped_at != errors_at

    def test_empty_message_wording_covers_views(self) -> None:
        """The empty-scan message must mention views, not just tables."""
        ds = SimpleNamespace(name="ds", type="sqlite", database=":memory:")
        msg = _empty_ingest_message(schema_name="analytics", ds=ds)
        assert "views" in msg.lower()
        assert "analytics" in msg


# ---------------------------------------------------------------------------
# REST divergence
# ---------------------------------------------------------------------------


class TestRestExitSemantics:
    def test_skipped_only_returns_200_with_skipped_in_body(
        self, monkeypatch, workspace: Path
    ) -> None:
        """Skips must NOT turn into 422; the body carries them alongside the successful additions."""
        from fastapi.testclient import TestClient

        from slayer.api.server import create_app
        from slayer.storage.yaml_storage import YAMLStorage

        storage = YAMLStorage(base_dir=str(workspace / "storage"))
        app = create_app(storage=storage)
        client = TestClient(app)
        client.post(
            "/datasources",
            json={"name": "ds", "type": "sqlite", "database": ":memory:"},
        )

        async def _fake(**_kwargs):
            return IdempotentIngestResult(
                additions=[
                    ModelAddition(
                        model_name="orders", data_source="ds", created=True,
                        new_columns=["id"],
                    )
                ],
                to_delete=[],
                errors=[],
                skipped=[
                    SkippedTable(table_name="a__b", reason="name collision")
                ],
            )

        monkeypatch.setattr(
            "slayer.engine.ingestion.ingest_datasource_idempotent", _fake
        )

        resp = client.post("/ingest", json={"datasource": "ds"})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["skipped"]
        assert body["skipped"][0]["table_name"] == "a__b"

    def test_errors_still_return_422(self, monkeypatch, workspace: Path) -> None:
        """the existing contract at api/server.py:675 is unchanged."""
        from fastapi.testclient import TestClient

        from slayer.api.server import create_app
        from slayer.engine.schema_drift import IngestionError
        from slayer.storage.yaml_storage import YAMLStorage

        storage = YAMLStorage(base_dir=str(workspace / "storage"))
        app = create_app(storage=storage)
        client = TestClient(app)
        client.post(
            "/datasources",
            json={"name": "ds", "type": "sqlite", "database": ":memory:"},
        )

        async def _fake(**_kwargs):
            return IdempotentIngestResult(
                additions=[],
                to_delete=[],
                errors=[
                    IngestionError(
                        model_name="orders", data_source="ds", error="boom"
                    )
                ],
                skipped=[],
            )

        monkeypatch.setattr(
            "slayer.engine.ingestion.ingest_datasource_idempotent", _fake
        )

        resp = client.post("/ingest", json={"datasource": "ds"})
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# datasources create --ingest carve-out
# ---------------------------------------------------------------------------


class TestDatasourcesCreateCarveOut:
    def test_empty_db_still_exits_zero(self, workspace: Path, capsys) -> None:
        """Creating the datasource succeeded, so an empty database must not fail this command."""
        import sqlite3

        from slayer.cli import _run_datasources_create
        from slayer.storage.yaml_storage import YAMLStorage

        db_path = str(workspace / "empty.db")
        sqlite3.connect(db_path).close()
        storage = YAMLStorage(base_dir=str(workspace / "storage"))

        args = SimpleNamespace(
            connection_string=f"sqlite:///{db_path}",
            name="ds",
            description=None,
            ingest=True,
            include=None,
            exclude=None,
            schema=None,
            include_views=True,
            yes=True,
            storage=str(workspace / "storage"),
            models_dir=None,
        )
        _run_datasources_create(args, storage)
        assert "No models were generated." in capsys.readouterr().out

class TestViewsFlagWiring:
    """The parser is built inline in ``main()``, so drive it through ``main()`` with a stubbed handler that captures the parsed args."""

    @staticmethod
    def _capture(monkeypatch, argv: list[str], handler: str) -> SimpleNamespace:
        import sys

        captured: dict[str, SimpleNamespace] = {}

        def _stub(args, *_rest, **_kwargs):
            captured["args"] = args

        monkeypatch.setattr(f"slayer.cli.{handler}", _stub)
        monkeypatch.setattr(sys, "argv", ["slayer", *argv])

        from slayer.cli import main

        main()
        return captured["args"]

    def test_ingest_defaults_to_views_on(self, monkeypatch) -> None:
        args = self._capture(
            monkeypatch, ["ingest", "--datasource", "ds"], "_run_ingest"
        )
        assert args.include_views is True

    def test_ingest_accepts_no_views_flag(self, monkeypatch) -> None:
        args = self._capture(
            monkeypatch,
            ["ingest", "--datasource", "ds", "--no-views"],
            "_run_ingest",
        )
        assert args.include_views is False

    def test_datasources_create_defaults_to_views_on(self, monkeypatch) -> None:
        """The flag exists on this subcommand too, consistent with `slayer ingest`."""
        args = self._capture(
            monkeypatch,
            ["datasources", "create", "sqlite:///x.db", "--ingest"],
            "_run_datasources",
        )
        assert args.include_views is True

    def test_datasources_create_accepts_no_views_flag(self, monkeypatch) -> None:
        args = self._capture(
            monkeypatch,
            ["datasources", "create", "sqlite:///x.db", "--ingest", "--no-views"],
            "_run_datasources",
        )
        assert args.include_views is False
