"""Tests for `--ingest-on-startup` orchestration (DEV-1392).

Covers:
* `ingest_all_datasources_idempotent` orchestrator (zero/all-succeed/one-fail/
  all-fail/drift/list-raises/get-returns-None/stream-routing).
* CLI flag plumbing on `slayer serve` and `slayer mcp` — argparse accepts
  `--ingest-on-startup`, the boolean threads through `_run_serve`/`_run_mcp`
  into `create_app` / `create_mcp_server`.
* `SLAYER_INGEST_ON_STARTUP` env var, truthy/falsy semantics, and the
  "flag wins over env" precedence rule.
* Programmatic `create_app(ingest_on_startup=True)` and
  `create_mcp_server(ingest_on_startup=True)` actually trigger the
  orchestrator (and the negative — `False` does not).
"""

from __future__ import annotations

import argparse
import io
import sqlite3
import sys
import tempfile
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from slayer import cli
from slayer.core.models import DatasourceConfig
from slayer.embeddings import client as embedding_client
from slayer.engine import ingestion as ingestion_module
from slayer.engine.ingestion import (
    StartupIngestSummary,
    ingest_all_datasources_idempotent,
)
from slayer.engine.schema_drift import (
    IdempotentIngestResult,
    IngestionError,
    ModelAddition,
    WholeModelDelete,
)
from slayer.storage.yaml_storage import YAMLStorage


# ─────────────────────────────────────────────────────────────────────────────  # NOSONAR(S125) — section separator, not commented-out code
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _ds(name: str) -> DatasourceConfig:
    return DatasourceConfig(
        name=name,
        type="sqlite",
        database=":memory:",
    )


def _stub_storage(
    *,
    names: list[str],
    list_raises: BaseException | None = None,
    missing: list[str] | None = None,
) -> MagicMock:
    """Build a storage stub with async `list_datasources` and `get_datasource`.

    `missing` names will resolve to `None` (simulating concurrent delete).
    """
    missing_set = set(missing or [])
    storage = MagicMock()
    if list_raises is not None:
        storage.list_datasources = AsyncMock(side_effect=list_raises)
    else:
        storage.list_datasources = AsyncMock(return_value=list(names))

    async def _get(name: str) -> DatasourceConfig | None:  # NOSONAR(S7503) — must be `async def` to be a valid AsyncMock.side_effect
        if name in missing_set:
            return None
        if name in names:
            return _ds(name)
        return None

    storage.get_datasource = AsyncMock(side_effect=_get)
    return storage


def _ok_result() -> IdempotentIngestResult:
    return IdempotentIngestResult(additions=[], to_delete=[], errors=[])


def _result_with_addition(model_name: str, data_source: str) -> IdempotentIngestResult:
    return IdempotentIngestResult(
        additions=[
            ModelAddition(
                model_name=model_name,
                data_source=data_source,
                created=True,
                new_columns=["a", "b"],
                new_joins=[],
            )
        ],
        to_delete=[],
        errors=[],
    )


def _result_with_drift(data_source: str) -> IdempotentIngestResult:
    return IdempotentIngestResult(
        additions=[],
        to_delete=[
            WholeModelDelete(
                model_name="dropped_table",
                data_source=data_source,
                reasons=[],
            )
        ],
        errors=[],
    )


def _result_with_per_table_errors(data_source: str) -> IdempotentIngestResult:
    return IdempotentIngestResult(
        additions=[],
        to_delete=[],
        errors=[
            IngestionError(
                model_name="widgets",
                data_source=data_source,
                error="Connection timed out",
            )
        ],
    )


def _patch_ingester(monkeypatch, behaviour):
    """Patch `ingest_datasource_idempotent` with a side-effect-driven stub.

    `behaviour` is a callable `(name, ds) -> IdempotentIngestResult` (or
    `(name, ds) -> raise`). Awaited by the orchestrator.
    """
    call_log: list[str] = []

    async def fake(*, datasource, storage, schema, include_tables, exclude_tables):  # NOSONAR(S7503) — must be `async def` to replace the real async ingest_datasource_idempotent
        call_log.append(datasource.name)
        out = behaviour(datasource.name, datasource)
        if isinstance(out, BaseException):
            raise out
        return out

    monkeypatch.setattr(
        ingestion_module,
        "ingest_datasource_idempotent",
        fake,
    )
    return call_log


def _serve_args(**overrides) -> argparse.Namespace:
    base = {
        "host": "h",
        "port": 1,
        "storage": None,
        "models_dir": None,
        "demo": False,
        "ingest_on_startup": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def _mcp_args(**overrides) -> argparse.Namespace:
    base = {
        "storage": None,
        "models_dir": None,
        "demo": False,
        "ingest_on_startup": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def _patch_serve_dependencies(monkeypatch, *, capture: list, app_obj="APP"):
    """Stub out _resolve_storage, _prepare_demo, create_app, and uvicorn.run
    so `_run_serve` can be called without touching real storage/network."""
    monkeypatch.setattr(cli, "_resolve_storage", lambda *a, **kw: "STORAGE")
    monkeypatch.setattr(
        cli, "_prepare_demo", lambda *a, **kw: capture.append(("prepare_demo",))
    )

    def fake_create_app(*args, **kwargs):
        capture.append(("create_app", kwargs))
        return app_obj

    fake_api = types.ModuleType("slayer.api.server")
    fake_api.create_app = fake_create_app
    monkeypatch.setitem(sys.modules, "slayer.api.server", fake_api)

    fake_uvicorn = types.ModuleType("uvicorn")
    fake_uvicorn.run = lambda *a, **kw: capture.append(("uvicorn_run", kw))
    monkeypatch.setitem(sys.modules, "uvicorn", fake_uvicorn)


def _patch_mcp_dependencies(monkeypatch, *, capture: list):
    monkeypatch.setattr(cli, "_resolve_storage", lambda *a, **kw: "STORAGE")
    monkeypatch.setattr(
        cli, "_prepare_demo", lambda *a, **kw: capture.append(("prepare_demo",))
    )

    class _FakeMCP:
        def run(self):
            capture.append(("mcp_run",))

    def fake_create_mcp_server(*args, **kwargs):
        capture.append(("create_mcp_server", kwargs))
        return _FakeMCP()

    fake_mcp = types.ModuleType("slayer.mcp.server")
    fake_mcp.create_mcp_server = fake_create_mcp_server
    monkeypatch.setitem(sys.modules, "slayer.mcp.server", fake_mcp)


# ─────────────────────────────────────────────────────────────────────────────
# 1–5, 10, 12 — orchestrator behaviour
# ─────────────────────────────────────────────────────────────────────────────


class TestOrchestrator:
    async def test_zero_datasources(self, monkeypatch, capsys):
        storage = _stub_storage(names=[])
        called = _patch_ingester(monkeypatch, lambda n, ds: _ok_result())
        stream = io.StringIO()

        summary = await ingest_all_datasources_idempotent(
            storage=storage, stream=stream
        )

        assert isinstance(summary, StartupIngestSummary)
        assert summary.succeeded == []
        assert summary.failures == []
        assert summary.drift_pending == []
        assert "no datasources configured" in stream.getvalue()
        # The per-datasource ingester must not have been called.
        assert called == []
        # Nothing leaked to real stdout/stderr.
        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == ""

    async def test_all_succeed(self, monkeypatch):
        storage = _stub_storage(names=["a", "b", "c"])
        called = _patch_ingester(
            monkeypatch, lambda n, ds: _result_with_addition(f"{n}_model", n)
        )
        stream = io.StringIO()

        summary = await ingest_all_datasources_idempotent(
            storage=storage, stream=stream
        )

        assert summary.succeeded == ["a", "b", "c"]
        assert summary.failures == []
        assert called == ["a", "b", "c"]
        out = stream.getvalue()
        assert "Ingest-on-startup: 3/3 datasources ingested" in out
        # No parenthetical when nothing failed.
        assert "failed" not in out

    async def test_one_fails(self, monkeypatch):
        storage = _stub_storage(names=["a", "b", "c"])

        def behaviour(name, ds):
            if name == "b":
                return RuntimeError("boom-b")
            return _result_with_addition(f"{name}_model", name)

        _patch_ingester(monkeypatch, behaviour)
        stream = io.StringIO()

        summary = await ingest_all_datasources_idempotent(
            storage=storage, stream=stream
        )

        assert summary.succeeded == ["a", "c"]
        assert [f.name for f in summary.failures] == ["b"]
        assert "boom-b" in summary.failures[0].error
        out = stream.getvalue()
        assert "Ingest-on-startup: 2/3 datasources ingested (1 failed: b)" in out

    async def test_all_fail(self, monkeypatch):
        storage = _stub_storage(names=["a", "b", "c"])
        _patch_ingester(monkeypatch, lambda n, ds: RuntimeError(f"boom-{n}"))
        stream = io.StringIO()

        summary = await ingest_all_datasources_idempotent(
            storage=storage, stream=stream
        )

        assert summary.succeeded == []
        assert [f.name for f in summary.failures] == ["a", "b", "c"]
        out = stream.getvalue()
        assert "0/3 datasources ingested" in out
        assert "3 failed: a, b, c" in out

    async def test_drift_accumulated_and_not_applied(self, monkeypatch):
        storage = _stub_storage(names=["a", "b"])
        _patch_ingester(monkeypatch, lambda n, ds: _result_with_drift(n))
        stream = io.StringIO()

        # Sentinel: if anything tries to call apply_drift_deletes, blow up.
        apply_mock = AsyncMock(
            side_effect=AssertionError("apply_drift_deletes must NOT be called")
        )
        # Patch wherever it could be imported from — both engine and a
        # potential future re-export. The orchestrator must never reach it.
        try:
            import slayer.engine.schema_drift as _sd

            if hasattr(_sd, "apply_drift_deletes"):
                monkeypatch.setattr(_sd, "apply_drift_deletes", apply_mock)
        except Exception:
            pass
        try:
            from slayer.engine.query_engine import SlayerQueryEngine

            if hasattr(SlayerQueryEngine, "apply_drift_deletes"):
                monkeypatch.setattr(
                    SlayerQueryEngine, "apply_drift_deletes", apply_mock
                )
        except Exception:
            pass

        summary = await ingest_all_datasources_idempotent(
            storage=storage, stream=stream
        )

        assert len(summary.drift_pending) == 2
        names = {entry.model_name for entry in summary.drift_pending}
        assert names == {"dropped_table"}
        sources = {entry.data_source for entry in summary.drift_pending}
        assert sources == {"a", "b"}
        apply_mock.assert_not_called()
        # Drift line should be visible in stream.
        assert "drift" in stream.getvalue().lower() or "delete" in stream.getvalue().lower()

    async def test_list_datasources_raises_propagates(self, monkeypatch):
        storage = _stub_storage(
            names=[], list_raises=RuntimeError("storage offline")
        )
        _patch_ingester(monkeypatch, lambda n, ds: _ok_result())
        stream = io.StringIO()

        with pytest.raises(RuntimeError, match="storage offline"):
            await ingest_all_datasources_idempotent(
                storage=storage, stream=stream
            )

    async def test_get_datasource_returns_none_mid_iteration(self, monkeypatch):
        storage = _stub_storage(names=["a", "b", "c"], missing=["b"])
        called = _patch_ingester(
            monkeypatch, lambda n, ds: _result_with_addition(f"{n}_model", n)
        )
        stream = io.StringIO()

        summary = await ingest_all_datasources_idempotent(
            storage=storage, stream=stream
        )

        # `a` and `c` go through, `b` becomes a failure.
        assert called == ["a", "c"]
        assert summary.succeeded == ["a", "c"]
        assert [f.name for f in summary.failures] == ["b"]
        assert "disappeared" in summary.failures[0].error

    async def test_get_datasource_raises_does_not_abort_iteration(self, monkeypatch):
        """A YAML parse error / invalid-config raise on a single datasource
        must not abort the whole startup pass — only `list_datasources()`
        raising is supposed to prevent boot."""
        names = ["a", "b", "c"]
        storage = MagicMock()
        storage.list_datasources = AsyncMock(return_value=list(names))

        async def _get(name: str):  # NOSONAR(S7503) — must be `async def` to be a valid AsyncMock.side_effect
            if name == "b":
                raise RuntimeError("invalid YAML in datasources/b.yaml")
            return _ds(name)

        storage.get_datasource = AsyncMock(side_effect=_get)
        called = _patch_ingester(
            monkeypatch, lambda n, ds: _result_with_addition(f"{n}_model", n)
        )
        stream = io.StringIO()

        summary = await ingest_all_datasources_idempotent(
            storage=storage, stream=stream
        )

        assert called == ["a", "c"]
        assert summary.succeeded == ["a", "c"]
        assert [f.name for f in summary.failures] == ["b"]
        assert "invalid YAML" in summary.failures[0].error

    async def test_output_routed_to_stream_not_stdout(self, monkeypatch, capsys):
        storage = _stub_storage(names=["a"])
        _patch_ingester(
            monkeypatch, lambda n, ds: _result_with_addition("a_model", "a")
        )
        stream = io.StringIO()

        await ingest_all_datasources_idempotent(
            storage=storage, stream=stream
        )

        # Stream got the output; stdout/stderr are clean.
        assert "Ingest-on-startup" in stream.getvalue()
        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == ""

    async def test_default_stream_is_stderr(self, monkeypatch, capsys):
        storage = _stub_storage(names=["only"])
        _patch_ingester(
            monkeypatch, lambda n, ds: _result_with_addition("only_model", "only")
        )

        # No `stream=` kwarg → defaults to sys.stderr.
        await ingest_all_datasources_idempotent(storage=storage)

        captured = capsys.readouterr()
        assert captured.out == ""
        assert "Ingest-on-startup" in captured.err

    async def test_per_table_errors_still_count_as_succeeded(self, monkeypatch):
        """When `ingest_datasource_idempotent` returns normally but with
        `result.errors` non-empty, the datasource still counts as succeeded
        (the call itself didn't raise). Per the spec."""
        storage = _stub_storage(names=["a"])
        _patch_ingester(monkeypatch, lambda n, ds: _result_with_per_table_errors(n))
        stream = io.StringIO()

        summary = await ingest_all_datasources_idempotent(
            storage=storage, stream=stream
        )

        assert summary.succeeded == ["a"]
        assert summary.failures == []
        # Per-table error message is rendered into the stream.
        assert "Connection timed out" in stream.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# 7 — CLI flag plumbing (argparse + dispatcher)
# ─────────────────────────────────────────────────────────────────────────────


class TestCliFlagPlumbing:
    def test_argparse_accepts_serve_flag(self, monkeypatch):
        """`slayer serve --ingest-on-startup` parses without error and
        produces `args.ingest_on_startup == True`."""
        captured = []

        def stub_run_serve(args):
            captured.append(args)

        monkeypatch.setattr(cli, "_run_serve", stub_run_serve)
        monkeypatch.setattr(sys, "argv", ["slayer", "serve", "--ingest-on-startup"])

        cli.main()

        assert len(captured) == 1
        assert captured[0].ingest_on_startup is True

    def test_argparse_accepts_mcp_flag(self, monkeypatch):
        captured = []

        def stub_run_mcp(args):
            captured.append(args)

        monkeypatch.setattr(cli, "_run_mcp", stub_run_mcp)
        monkeypatch.setattr(sys, "argv", ["slayer", "mcp", "--ingest-on-startup"])

        cli.main()

        assert len(captured) == 1
        assert captured[0].ingest_on_startup is True

    def test_argparse_default_is_false_serve(self, monkeypatch):
        captured = []
        monkeypatch.setattr(cli, "_run_serve", lambda a: captured.append(a))
        monkeypatch.setattr(sys, "argv", ["slayer", "serve"])
        cli.main()
        assert len(captured) == 1
        assert captured[0].ingest_on_startup is False

    def test_argparse_default_is_false_mcp(self, monkeypatch):
        captured = []
        monkeypatch.setattr(cli, "_run_mcp", lambda a: captured.append(a))
        monkeypatch.setattr(sys, "argv", ["slayer", "mcp"])
        cli.main()
        assert len(captured) == 1
        assert captured[0].ingest_on_startup is False

    def test_serve_flag_threads_to_create_app(self, monkeypatch):
        capture: list = []
        _patch_serve_dependencies(monkeypatch, capture=capture)
        monkeypatch.delenv("SLAYER_INGEST_ON_STARTUP", raising=False)

        cli._run_serve(_serve_args(ingest_on_startup=True))

        create_app_kwargs = next(c[1] for c in capture if c[0] == "create_app")
        assert create_app_kwargs.get("ingest_on_startup") is True

    def test_mcp_flag_threads_to_create_mcp_server(self, monkeypatch):
        capture: list = []
        _patch_mcp_dependencies(monkeypatch, capture=capture)
        monkeypatch.delenv("SLAYER_INGEST_ON_STARTUP", raising=False)

        cli._run_mcp(_mcp_args(ingest_on_startup=True))

        create_kwargs = next(c[1] for c in capture if c[0] == "create_mcp_server")
        assert create_kwargs.get("ingest_on_startup") is True

    def test_serve_no_flag_passes_false(self, monkeypatch):
        capture: list = []
        _patch_serve_dependencies(monkeypatch, capture=capture)
        monkeypatch.delenv("SLAYER_INGEST_ON_STARTUP", raising=False)

        cli._run_serve(_serve_args(ingest_on_startup=False))

        create_app_kwargs = next(c[1] for c in capture if c[0] == "create_app")
        assert create_app_kwargs.get("ingest_on_startup") is False

    def test_mcp_no_flag_passes_false(self, monkeypatch):
        capture: list = []
        _patch_mcp_dependencies(monkeypatch, capture=capture)
        monkeypatch.delenv("SLAYER_INGEST_ON_STARTUP", raising=False)

        cli._run_mcp(_mcp_args(ingest_on_startup=False))

        create_kwargs = next(c[1] for c in capture if c[0] == "create_mcp_server")
        assert create_kwargs.get("ingest_on_startup") is False


# ─────────────────────────────────────────────────────────────────────────────
# 8 — env var precedence
# ─────────────────────────────────────────────────────────────────────────────


class TestEnvVar:
    @pytest.mark.parametrize("value", ["1", "true", "yes", "TRUE", "Yes", "YES"])
    def test_truthy_enables(self, monkeypatch, value):
        monkeypatch.setenv("SLAYER_INGEST_ON_STARTUP", value)
        capture: list = []
        _patch_serve_dependencies(monkeypatch, capture=capture)

        cli._run_serve(_serve_args(ingest_on_startup=False))

        kwargs = next(c[1] for c in capture if c[0] == "create_app")
        assert kwargs.get("ingest_on_startup") is True

    @pytest.mark.parametrize("value", ["0", "false", "no", "FALSE", "", "garbage", "off"])
    def test_falsy_disables(self, monkeypatch, value):
        monkeypatch.setenv("SLAYER_INGEST_ON_STARTUP", value)
        capture: list = []
        _patch_serve_dependencies(monkeypatch, capture=capture)

        cli._run_serve(_serve_args(ingest_on_startup=False))

        kwargs = next(c[1] for c in capture if c[0] == "create_app")
        assert kwargs.get("ingest_on_startup") is False

    def test_unset_disables(self, monkeypatch):
        monkeypatch.delenv("SLAYER_INGEST_ON_STARTUP", raising=False)
        capture: list = []
        _patch_serve_dependencies(monkeypatch, capture=capture)

        cli._run_serve(_serve_args(ingest_on_startup=False))

        kwargs = next(c[1] for c in capture if c[0] == "create_app")
        assert kwargs.get("ingest_on_startup") is False

    def test_flag_wins_when_env_false(self, monkeypatch):
        # Flag True, env "0" → effective True (flag set wins).
        monkeypatch.setenv("SLAYER_INGEST_ON_STARTUP", "0")
        capture: list = []
        _patch_serve_dependencies(monkeypatch, capture=capture)

        cli._run_serve(_serve_args(ingest_on_startup=True))

        kwargs = next(c[1] for c in capture if c[0] == "create_app")
        assert kwargs.get("ingest_on_startup") is True

    def test_env_wins_when_flag_unset(self, monkeypatch):
        monkeypatch.setenv("SLAYER_INGEST_ON_STARTUP", "1")
        capture: list = []
        _patch_serve_dependencies(monkeypatch, capture=capture)

        cli._run_serve(_serve_args(ingest_on_startup=False))

        kwargs = next(c[1] for c in capture if c[0] == "create_app")
        assert kwargs.get("ingest_on_startup") is True

    def test_env_also_threads_through_mcp(self, monkeypatch):
        monkeypatch.setenv("SLAYER_INGEST_ON_STARTUP", "yes")
        capture: list = []
        _patch_mcp_dependencies(monkeypatch, capture=capture)

        cli._run_mcp(_mcp_args(ingest_on_startup=False))

        kwargs = next(c[1] for c in capture if c[0] == "create_mcp_server")
        assert kwargs.get("ingest_on_startup") is True


# ─────────────────────────────────────────────────────────────────────────────
# 9 — --demo + --ingest-on-startup ordering
# ─────────────────────────────────────────────────────────────────────────────


class TestDemoIngestOrdering:
    def test_demo_then_ingest_serve(self, monkeypatch):
        capture: list = []
        _patch_serve_dependencies(monkeypatch, capture=capture)
        monkeypatch.delenv("SLAYER_INGEST_ON_STARTUP", raising=False)

        cli._run_serve(_serve_args(demo=True, ingest_on_startup=True))

        order = [c[0] for c in capture]
        # _prepare_demo before create_app; create_app got ingest_on_startup=True;
        # uvicorn last.
        assert order.index("prepare_demo") < order.index("create_app")
        assert order.index("create_app") < order.index("uvicorn_run")
        kwargs = next(c[1] for c in capture if c[0] == "create_app")
        assert kwargs.get("ingest_on_startup") is True

    def test_demo_then_ingest_mcp(self, monkeypatch):
        capture: list = []
        _patch_mcp_dependencies(monkeypatch, capture=capture)
        monkeypatch.delenv("SLAYER_INGEST_ON_STARTUP", raising=False)

        cli._run_mcp(_mcp_args(demo=True, ingest_on_startup=True))

        order = [c[0] for c in capture]
        assert order.index("prepare_demo") < order.index("create_mcp_server")
        assert order.index("create_mcp_server") < order.index("mcp_run")
        kwargs = next(c[1] for c in capture if c[0] == "create_mcp_server")
        assert kwargs.get("ingest_on_startup") is True


# ─────────────────────────────────────────────────────────────────────────────
# 11 — programmatic create_app / create_mcp_server
# ─────────────────────────────────────────────────────────────────────────────


class TestProgrammaticKwarg:
    @pytest.fixture(autouse=True)
    def _no_seed_help(self, monkeypatch):
        # DEV-1658: create_app / create_mcp_server now seed help.* memories on
        # startup. These tests pass a MagicMock stub storage (no async memory
        # API), and are about ingest orchestration, not seeding — so no-op the
        # seed in both server modules.
        async def _noop(*, storage):  # noqa: ANN001, ANN002
            return 0

        import slayer.api.server as api_server
        import slayer.mcp.server as mcp_server_mod

        monkeypatch.setattr(api_server, "seed_help_memories", _noop)
        monkeypatch.setattr(mcp_server_mod, "seed_help_memories", _noop)

    def test_create_app_with_kwarg_triggers_orchestrator(self, monkeypatch):
        calls: list = []

        async def fake_orchestrator(*, storage, stream=None):  # NOSONAR(S7503) — must be `async def`; replaces the async ingest_all_datasources_idempotent which create_app/create_mcp_server awaits via run_sync
            calls.append((storage, stream))
            return StartupIngestSummary()

        monkeypatch.setattr(
            ingestion_module,
            "ingest_all_datasources_idempotent",
            fake_orchestrator,
        )

        storage = _stub_storage(names=[])
        from slayer.api.server import create_app

        app = create_app(storage=storage, ingest_on_startup=True)

        assert app is not None
        assert len(calls) == 1
        captured_storage, captured_stream = calls[0]
        assert captured_storage is storage
        # Default stream from create_app is sys.stderr.
        assert captured_stream is sys.stderr

    def test_create_app_without_kwarg_does_not_trigger(self, monkeypatch):
        calls: list = []

        async def fake_orchestrator(*, storage, stream=None):  # NOSONAR(S7503) — must be `async def`; replaces the async ingest_all_datasources_idempotent which create_app/create_mcp_server awaits via run_sync
            calls.append((storage, stream))
            return StartupIngestSummary()

        monkeypatch.setattr(
            ingestion_module,
            "ingest_all_datasources_idempotent",
            fake_orchestrator,
        )

        storage = _stub_storage(names=[])
        from slayer.api.server import create_app

        create_app(storage=storage)
        create_app(storage=storage, ingest_on_startup=False)

        assert calls == []

    def test_create_mcp_server_with_kwarg_triggers_orchestrator(self, monkeypatch):
        calls: list = []

        async def fake_orchestrator(*, storage, stream=None):  # NOSONAR(S7503) — must be `async def`; replaces the async ingest_all_datasources_idempotent which create_app/create_mcp_server awaits via run_sync
            calls.append((storage, stream))
            return StartupIngestSummary()

        monkeypatch.setattr(
            ingestion_module,
            "ingest_all_datasources_idempotent",
            fake_orchestrator,
        )

        storage = _stub_storage(names=[])
        from slayer.mcp.server import create_mcp_server

        server = create_mcp_server(storage=storage, ingest_on_startup=True)

        assert server is not None
        assert len(calls) == 1
        captured_storage, captured_stream = calls[0]
        assert captured_storage is storage
        assert captured_stream is sys.stderr

    def test_create_mcp_server_without_kwarg_does_not_trigger(self, monkeypatch):
        calls: list = []

        async def fake_orchestrator(*, storage, stream=None):  # NOSONAR(S7503) — must be `async def`; replaces the async ingest_all_datasources_idempotent which create_app/create_mcp_server awaits via run_sync
            calls.append((storage, stream))
            return StartupIngestSummary()

        monkeypatch.setattr(
            ingestion_module,
            "ingest_all_datasources_idempotent",
            fake_orchestrator,
        )

        storage = _stub_storage(names=[])
        from slayer.mcp.server import create_mcp_server

        create_mcp_server(storage=storage)
        create_mcp_server(storage=storage, ingest_on_startup=False)

        assert calls == []

    def test_create_app_propagates_list_datasources_error(self, monkeypatch):
        """When `storage.list_datasources()` raises, the exception
        propagates through `create_app(ingest_on_startup=True)` and the app
        is never returned."""
        storage = _stub_storage(
            names=[], list_raises=RuntimeError("storage offline")
        )

        from slayer.api.server import create_app

        with pytest.raises(RuntimeError, match="storage offline"):
            create_app(storage=storage, ingest_on_startup=True)

    def test_create_mcp_server_propagates_list_datasources_error(self, monkeypatch):
        storage = _stub_storage(
            names=[], list_raises=RuntimeError("storage offline")
        )

        from slayer.mcp.server import create_mcp_server

        with pytest.raises(RuntimeError, match="storage offline"):
            create_mcp_server(storage=storage, ingest_on_startup=True)


# ─────────────────────────────────────────────────────────────────────────────
# 6 — list_datasources raise via _run_serve / _run_mcp prevents uvicorn/mcp.run
# ─────────────────────────────────────────────────────────────────────────────


class TestListDatasourcesRaiseAtBoot:
    def test_serve_does_not_run_uvicorn_when_orchestrator_raises(self, monkeypatch):
        capture: list = []
        _patch_serve_dependencies(monkeypatch, capture=capture)
        monkeypatch.delenv("SLAYER_INGEST_ON_STARTUP", raising=False)

        # Swap the stub create_app for one that raises (simulating
        # orchestrator propagation through create_app).
        fake_api = sys.modules["slayer.api.server"]

        def raising_create_app(*a, **kw):
            capture.append(("create_app", kw))
            raise RuntimeError("storage offline")

        fake_api.create_app = raising_create_app

        with pytest.raises(RuntimeError, match="storage offline"):
            cli._run_serve(_serve_args(ingest_on_startup=True))

        # uvicorn must NOT have been called.
        assert not any(c[0] == "uvicorn_run" for c in capture)

    def test_mcp_does_not_run_when_orchestrator_raises(self, monkeypatch):
        capture: list = []
        _patch_mcp_dependencies(monkeypatch, capture=capture)
        monkeypatch.delenv("SLAYER_INGEST_ON_STARTUP", raising=False)

        fake_mcp = sys.modules["slayer.mcp.server"]

        def raising_create_mcp_server(*a, **kw):
            capture.append(("create_mcp_server", kw))
            raise RuntimeError("storage offline")

        fake_mcp.create_mcp_server = raising_create_mcp_server

        with pytest.raises(RuntimeError, match="storage offline"):
            cli._run_mcp(_mcp_args(ingest_on_startup=True))

        assert not any(c[0] == "mcp_run" for c in capture)


# ─────────────────────────────────────────────────────────────────────────────
# DEV-1416 — memory embeddings refresh on --ingest-on-startup
# ─────────────────────────────────────────────────────────────────────────────


class TestMemoryEmbeddingsOnStartup:
    """End-to-end smoke test: a stale `embeddings.db` (zero memory rows)
    gets populated by a single `--ingest-on-startup` pass through the
    real `ingest_all_datasources_idempotent` orchestrator."""

    async def test_orchestrator_refreshes_memory_embeddings_for_each_datasource(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = str(tmp_path / "live.db")
            conn = sqlite3.connect(db_path)
            conn.executescript(
                """
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    amount REAL NOT NULL
                );
                INSERT INTO orders VALUES (1, 100.0);
                """
            )
            conn.commit()
            conn.close()

            storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
            ds = DatasourceConfig(name="ds", type="sqlite", database=db_path)
            await storage.save_datasource(ds)

            saved = await storage.save_memory(
                learning="orders.amount is in USD cents",
                entities=[f"{ds.name}.orders.amount"],
            )

            # Enable the embedding channel (overriding the conftest
            # autouse fixture which forces is_available=False) and stub
            # embed_batch.
            monkeypatch.setattr(
                embedding_client, "is_available", lambda: True,
            )

            async def fake_embed_batch(  # NOSONAR(S7503) — must be `async def` to match the patched embed_batch signature
                texts: list[str], *, model: str | None = None,
            ) -> list[list[float] | None]:
                return [[0.1, 0.2, 0.3] for _ in texts]

            monkeypatch.setattr(
                "slayer.search.retrievers.embeddings.embed_batch",
                fake_embed_batch,
            )

            stream = io.StringIO()
            summary = await ingest_all_datasources_idempotent(
                storage=storage, stream=stream,
            )
            assert summary.succeeded == ["ds"]
            assert summary.failures == []

            rows = await storage.list_embeddings(
                embedding_model_name=embedding_client.current_model(),
            )
            assert any(
                r.canonical_id == f"memory:{saved.id}"
                and r.entity_kind == "memory"
                for r in rows
            ), f"memory embedding not written; rows={[r.canonical_id for r in rows]}"
