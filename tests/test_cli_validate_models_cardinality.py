"""CLI ``slayer validate-models``, including the merged cardinality profiling."""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from slayer.async_utils import run_sync
from slayer.cli import _run_validate_models
from slayer.cli import main as cli_main
from slayer.core.enums import DataType, JoinCardinality
from slayer.core.models import Column, DatasourceConfig, ModelJoin, SlayerModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def workspace():
    tmp = tempfile.TemporaryDirectory()
    try:
        yield Path(tmp.name)
    finally:
        tmp.cleanup()


# ---------------------------------------------------------------------------
# Fixture: one datasource, two models, drift optionally seeded on each
# ---------------------------------------------------------------------------


def _seed_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE customers (id INTEGER PRIMARY KEY, region TEXT);
        CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER);
        INSERT INTO customers VALUES (1,'US'),(2,'EU');
        INSERT INTO orders VALUES (1,1),(2,1),(3,2);
        """
    )
    conn.commit()
    conn.close()


def _customers_model(*, data_source: str, drift: bool) -> SlayerModel:
    columns = [
        Column(name="id", sql="id", type=DataType.INT, primary_key=True),
        Column(name="region", sql="region", type=DataType.TEXT),
    ]
    if drift:
        # Not in the live table -> validate_models emits an EditModelDelete.
        columns.append(Column(name="ghost_c", sql="ghost_c", type=DataType.TEXT))
    return SlayerModel(
        name="customers", sql_table="customers",
        data_source=data_source, columns=columns,
    )


def _orders_model(*, data_source: str, drift: bool) -> SlayerModel:
    columns = [
        Column(name="id", sql="id", type=DataType.INT, primary_key=True),
        Column(name="customer_id", sql="customer_id", type=DataType.INT),
    ]
    if drift:
        columns.append(Column(name="ghost_o", sql="ghost_o", type=DataType.TEXT))
    return SlayerModel(
        name="orders", sql_table="orders",
        data_source=data_source, columns=columns,
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def _setup(
    workspace: Path,
    *,
    ds_name: str = "ds",
    drift_orders: bool = False,
    drift_customers: bool = False,
    store: str | None = None,
    db_name: str | None = None,
) -> str:
    db = str(workspace / (db_name or f"{ds_name}.db"))
    _seed_db(db)
    store = store or str(workspace / "store")
    storage = YAMLStorage(base_dir=store)
    run_sync(storage.save_datasource(
        DatasourceConfig(name=ds_name, type="sqlite", database=db)
    ))
    run_sync(storage.save_model(
        _customers_model(data_source=ds_name, drift=drift_customers)
    ))
    run_sync(storage.save_model(
        _orders_model(data_source=ds_name, drift=drift_orders)
    ))
    return store


def _args(store: str, **kw) -> SimpleNamespace:
    base = dict(
        datasource="ds",
        model=None,
        cardinality=False,
        persist_cardinality=False,
        format="text",
        force_clean=False,
        yes=False,
        storage=store,
        models_dir=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


@contextmanager
def _argv(*argv: str):
    original = sys.argv
    sys.argv = ["slayer", *argv]
    try:
        yield
    finally:
        sys.argv = original


def _exit_code(*argv: str) -> int:
    with _argv(*argv):
        try:
            cli_main()
        except SystemExit as exc:  # NOSONAR(S5754) — capturing the CLI exit code
            return int(exc.code or 0)
    return 0


# ---------------------------------------------------------------------------
# Default output is unchanged (no cardinality work at all)
# ---------------------------------------------------------------------------


def test_default_output_has_no_section_headers(workspace: Path, capsys) -> None:
    store = _setup(workspace)
    _run_validate_models(_args(store))
    out = capsys.readouterr().out
    assert "Join cardinality" not in out
    assert "Schema drift" not in out
    assert "No drift detected." in out


def test_default_run_never_profiles(workspace: Path, monkeypatch, capsys) -> None:
    store = _setup(workspace)
    calls: list[str] = []

    async def _spy(self, **kwargs):
        calls.append("called")
        raise AssertionError("cardinality profiling must not run by default")

    monkeypatch.setattr(SlayerQueryEngine, "detect_join_cardinality", _spy)
    _run_validate_models(_args(store))
    capsys.readouterr()
    assert calls == []


# ---------------------------------------------------------------------------
# --cardinality
# ---------------------------------------------------------------------------


def test_cardinality_text_output_has_both_sections(workspace: Path, capsys) -> None:
    store = _setup(workspace)
    _run_validate_models(_args(store, cardinality=True))
    out = capsys.readouterr().out
    assert "Schema drift" in out
    assert "Join cardinality" in out
    assert "many_to_one" in out.split("Join cardinality", 1)[1]


def test_cardinality_leaves_storage_untouched(workspace: Path, capsys) -> None:
    store = _setup(workspace)
    _run_validate_models(_args(store, cardinality=True))
    capsys.readouterr()
    orders = run_sync(YAMLStorage(base_dir=store).get_model("orders", data_source="ds"))
    assert orders.joins[0].cardinality is None


def test_persist_cardinality_implies_cardinality(workspace: Path, capsys) -> None:
    store = _setup(workspace)
    _run_validate_models(_args(store, persist_cardinality=True))
    out = capsys.readouterr().out
    assert "Join cardinality" in out
    orders = run_sync(YAMLStorage(base_dir=store).get_model("orders", data_source="ds"))
    assert orders.joins[0].cardinality is not None


# ---------------------------------------------------------------------------
# --format json
# ---------------------------------------------------------------------------


def test_json_with_cardinality(workspace: Path, capsys) -> None:
    store = _setup(workspace)
    _run_validate_models(_args(store, cardinality=True, format="json"))
    data = json.loads(capsys.readouterr().out)
    assert isinstance(data["drift"], list)
    finding = next(f for f in data["cardinality"]["findings"] if f["model"] == "orders")
    assert finding["detected"] == "many_to_one"


def test_json_without_cardinality_is_null(workspace: Path, capsys) -> None:
    store = _setup(workspace)
    _run_validate_models(_args(store, format="json"))
    data = json.loads(capsys.readouterr().out)
    assert data["cardinality"] is None
    assert data["drift"] == []


def test_json_includes_join_safety_key(workspace: Path, capsys) -> None:
    """Automation reading JSON must see the same audit the text output prints.
    The fixture's join is structurally proven → an empty list, not a missing key."""
    store = _setup(workspace)
    _run_validate_models(_args(store, format="json"))
    data = json.loads(capsys.readouterr().out)
    assert data["join_safety"] == []


def test_json_join_safety_reports_unproven_join(workspace: Path, capsys) -> None:
    store = _setup(workspace)
    run_sync(YAMLStorage(base_dir=store).save_model(SlayerModel(
        name="risky", sql_table="orders", data_source="ds",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.INT),
        ],
        # region is neither a PK nor unique on customers → unproven hop.
        joins=[ModelJoin(target_model="customers",
                         join_pairs=[["customer_id", "region"]])],
    )))
    _run_validate_models(_args(store, format="json"))
    data = json.loads(capsys.readouterr().out)
    (finding,) = data["join_safety"]
    assert finding["data_source"] == "ds"
    assert finding["model"] == "risky"
    assert finding["target_model"] == "customers"
    assert "unproven" in finding["message"]


def test_json_serialises_drift_entries(workspace: Path, capsys) -> None:
    """Both ToDeleteEntry variants must round-trip through json.dumps."""
    store = _setup(workspace, drift_orders=True, drift_customers=True)
    # A model whose whole table is gone -> WholeModelDelete.
    run_sync(YAMLStorage(base_dir=store).save_model(SlayerModel(
        name="vanished", sql_table="vanished", data_source="ds",
        columns=[Column(name="id", sql="id", type=DataType.INT)],
    )))
    _run_validate_models(_args(store, format="json"))
    data = json.loads(capsys.readouterr().out)
    tools = {e["tool"] for e in data["drift"]}
    assert "edit_model" in tools
    assert "delete_model" in tools
    edit = next(e for e in data["drift"] if e["tool"] == "edit_model")
    assert "ghost_o" in edit["remove"]["columns"] or "ghost_c" in edit["remove"]["columns"]
    assert isinstance(edit["reasons"], list)


# ---------------------------------------------------------------------------
# --model scoping
# ---------------------------------------------------------------------------


def test_model_scopes_both_sections(workspace: Path, capsys) -> None:
    store = _setup(workspace, drift_orders=True, drift_customers=True)
    _run_validate_models(_args(store, model="orders", cardinality=True, format="json"))
    data = json.loads(capsys.readouterr().out)
    assert {e["model_name"] for e in data["drift"]} == {"orders"}
    assert {f["model"] for f in data["cardinality"]["findings"]} == {"orders"}


def test_model_scopes_force_clean_mutations(workspace: Path, capsys) -> None:
    store = _setup(workspace, drift_orders=True, drift_customers=True)
    _run_validate_models(_args(store, model="orders", force_clean=True, yes=True))
    capsys.readouterr()
    storage = YAMLStorage(base_dir=store)
    orders = run_sync(storage.get_model("orders", data_source="ds"))
    customers = run_sync(storage.get_model("customers", data_source="ds"))
    assert [c.name for c in orders.columns] == ["id", "customer_id"]
    assert "ghost_c" in [c.name for c in customers.columns]


def test_force_clean_ignores_out_of_scope_residual(workspace: Path, capsys) -> None:
    """Drift on a model outside --model must not print, nor force exit 1."""
    store = _setup(workspace, drift_orders=True, drift_customers=True)
    _run_validate_models(_args(store, model="orders", force_clean=True, yes=True))
    out = capsys.readouterr().out
    assert "ghost_c" not in out


def test_unknown_model_fails_fast(workspace: Path, capsys) -> None:
    store = _setup(workspace)
    args = _args(store, model="nope")
    with pytest.raises(SystemExit) as exc:
        _run_validate_models(args)
    assert exc.value.code == 1
    assert "nope" in capsys.readouterr().err


def test_unknown_datasource_fails_fast(workspace: Path, capsys) -> None:
    store = _setup(workspace)
    args = _args(store, datasource="nope")
    with pytest.raises(SystemExit) as exc:
        _run_validate_models(args)
    assert exc.value.code == 1
    assert "nope" in capsys.readouterr().err


def test_model_resolves_across_datasources(workspace: Path, capsys) -> None:
    store = _setup(workspace, ds_name="ds")
    _setup(workspace, ds_name="ds2", store=store)
    _run_validate_models(
        _args(store, datasource=None, model="orders", cardinality=True, format="json")
    )
    data = json.loads(capsys.readouterr().out)
    sources = {f["data_source"] for f in data["cardinality"]["findings"]}
    assert sources == {"ds", "ds2"}


def test_model_present_in_only_one_datasource_does_not_fail(
    workspace: Path, capsys
) -> None:
    store = _setup(workspace, ds_name="ds")
    _setup(workspace, ds_name="ds2", store=store)
    storage = YAMLStorage(base_dir=store)
    run_sync(storage.save_model(SlayerModel(
        name="solo", sql_table="customers", data_source="ds2",
        columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
    )))
    _run_validate_models(
        _args(store, datasource=None, model="solo", cardinality=True, format="json")
    )
    data = json.loads(capsys.readouterr().out)
    assert data["cardinality"]["findings"] == []


# ---------------------------------------------------------------------------
# Exit codes and failure handling
# ---------------------------------------------------------------------------


def test_contradicts_hard_still_exits_zero(workspace: Path, capsys) -> None:
    store = _setup(workspace)
    storage = YAMLStorage(base_dir=store)
    orders = run_sync(storage.get_model("orders", data_source="ds"))
    # customer_id has duplicates, so a stored one_to_many is hard-contradicted.
    orders.joins[0].cardinality = JoinCardinality.ONE_TO_MANY
    run_sync(storage.save_model(orders))

    _run_validate_models(_args(store, cardinality=True, format="json"))
    data = json.loads(capsys.readouterr().out)
    finding = next(f for f in data["cardinality"]["findings"] if f["model"] == "orders")
    assert finding["verdict"] == "contradicts_hard"


def test_datasource_failure_in_unscoped_run_exits_one(
    workspace: Path, monkeypatch, capsys
) -> None:
    store = _setup(workspace, ds_name="ds")
    _setup(workspace, ds_name="ds2", store=store)

    async def _flaky(self, data_source=None):
        if data_source == "ds2":
            raise RuntimeError("boom")
        return []

    monkeypatch.setattr(SlayerQueryEngine, "validate_models", _flaky)

    async def _never(self, **kwargs):
        raise AssertionError("must not profile after a datasource failure")

    monkeypatch.setattr(SlayerQueryEngine, "detect_join_cardinality", _never)

    args = _args(store, datasource=None, cardinality=True)
    with pytest.raises(SystemExit) as exc:
        _run_validate_models(args)
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "ds2" in err
    assert "boom" in err


def test_every_datasource_failure_is_reported(
    workspace: Path, monkeypatch, capsys
) -> None:
    """The per-datasource loop must not short-circuit on the first failure."""
    store = _setup(workspace, ds_name="ds")
    _setup(workspace, ds_name="ds2", store=store)

    async def _boom(self, data_source=None):
        raise RuntimeError(f"{data_source} exploded")

    monkeypatch.setattr(SlayerQueryEngine, "validate_models", _boom)

    args = _args(store, datasource=None)
    with pytest.raises(SystemExit):
        _run_validate_models(args)
    err = capsys.readouterr().err
    assert "ds exploded" in err
    assert "ds2 exploded" in err


def test_datasource_failure_keeps_json_parseable(
    workspace: Path, monkeypatch, capsys
) -> None:
    store = _setup(workspace, ds_name="ds")
    _setup(workspace, ds_name="ds2", store=store)

    async def _flaky(self, data_source=None):
        if data_source == "ds2":
            raise RuntimeError("boom")
        return []

    monkeypatch.setattr(SlayerQueryEngine, "validate_models", _flaky)

    args = _args(store, datasource=None, format="json")
    with pytest.raises(SystemExit):
        _run_validate_models(args)
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {
        "drift": [], "join_safety": [], "cardinality": None,
    }
    assert "ds2" in captured.err


def test_drift_failure_skips_profiling(workspace: Path, monkeypatch, capsys) -> None:
    store = _setup(workspace)

    async def _boom(self, data_source=None):
        raise RuntimeError("drift exploded")

    monkeypatch.setattr(SlayerQueryEngine, "validate_models", _boom)

    async def _never(self, **kwargs):
        raise AssertionError("must not profile when drift validation failed")

    monkeypatch.setattr(SlayerQueryEngine, "detect_join_cardinality", _never)

    args = _args(store, cardinality=True)
    with pytest.raises(SystemExit) as exc:
        _run_validate_models(args)
    assert exc.value.code == 1
    assert "drift exploded" in capsys.readouterr().err


def test_cardinality_failure_exits_one(workspace: Path, monkeypatch, capsys) -> None:
    store = _setup(workspace)

    async def _boom(self, **kwargs):
        raise RuntimeError("profiling exploded")

    monkeypatch.setattr(SlayerQueryEngine, "detect_join_cardinality", _boom)

    args = _args(store, cardinality=True)
    with pytest.raises(SystemExit) as exc:
        _run_validate_models(args)
    assert exc.value.code == 1
    err = capsys.readouterr().err
    assert "cardinality" in err.lower()
    assert "profiling exploded" in err


def test_scan_failure_reports_fully_but_exits_one(
    workspace: Path, monkeypatch, capsys
) -> None:
    """A contained scan failure is still work the command did not do."""
    store = _setup(workspace)

    async def _boom(self, **kwargs):
        raise RuntimeError("table is on fire")

    monkeypatch.setattr(SlayerQueryEngine, "_side_stats", _boom)

    args = _args(store, cardinality=True)
    with pytest.raises(SystemExit) as exc:
        _run_validate_models(args)
    assert exc.value.code == 1
    captured = capsys.readouterr()
    # The report is still printed in full — containment is not silence.
    assert "scan_failed" in captured.out
    assert "could not be profiled" in captured.err


def test_profiling_runs_after_the_force_clean_apply(
    workspace: Path, monkeypatch, capsys
) -> None:
    store = _setup(workspace, drift_orders=True)
    order: list[str] = []

    real_apply = SlayerQueryEngine.apply_drift_deletes
    real_detect = SlayerQueryEngine.detect_join_cardinality

    async def _apply(self, deletes):
        order.append("apply")
        return await real_apply(self, deletes)

    async def _detect(self, **kwargs):
        order.append("detect")
        return await real_detect(self, **kwargs)

    monkeypatch.setattr(SlayerQueryEngine, "apply_drift_deletes", _apply)
    monkeypatch.setattr(SlayerQueryEngine, "detect_join_cardinality", _detect)

    _run_validate_models(
        _args(store, cardinality=True, force_clean=True, yes=True)
    )
    capsys.readouterr()
    assert order == ["apply", "detect"]


# ---------------------------------------------------------------------------
# Parser-level contracts
# ---------------------------------------------------------------------------


def test_json_with_force_clean_is_a_usage_error(workspace: Path) -> None:
    store = _setup(workspace)
    assert _exit_code(
        "validate-models", "--storage", store, "--format", "json", "--force-clean"
    ) == 2


def test_joins_command_is_gone(workspace: Path) -> None:
    store = _setup(workspace)
    assert _exit_code(
        "joins", "detect-cardinality", "--storage", store
    ) == 2
