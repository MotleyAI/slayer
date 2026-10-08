"""Query feature counters, environment and context fields."""

from __future__ import annotations

import asyncio
import json
import platform
import sys
from collections import Counter
from importlib.metadata import version as pkg_version
from typing import Any

import pytest

import slayer.cli
from slayer.core.models import DatasourceConfig
from slayer.sql import engine_factory
from slayer.storage.yaml_storage import YAMLStorage
from tests._saved_query_refinement_fixtures import AVG_CUSTOMER_REVENUE, MONTH_TD
from tests._telemetry_helpers import (
    build_storage,
    cli,
    quiet_config,
    run_python,
    save_models,
    show,
    usage,
)

_PLAIN = {"source_model": "orders", "measures": ["sum(amount)"]}
_FLAGS = (
    "multistage", "inline_source", "time_dimensions", "filters",
    "computed_dimensions", "saved_query", "to_many_handling",
)


@pytest.fixture
def store(telemetry_env) -> str:
    quiet_config()
    return build_storage(telemetry_env.root)


def _run_query(store: str, query: Any) -> dict:
    arg = query if isinstance(query, str) else json.dumps(query)
    cli(["query", arg, "--storage", store])
    return show()


# --- structural flags ------------------------------------------------------------------------


def test_plain_query_sets_no_flag(store: str) -> None:
    flags = _run_query(store, _PLAIN)["features"]["flags"]
    assert all(flags.get(flag, 0) == 0 for flag in _FLAGS)


@pytest.mark.parametrize(("query", "flag"), [
    (AVG_CUSTOMER_REVENUE, "multistage"),
    ({"source_model": {"source_name": "orders", "columns": [{"name": "double_amount", "sql": "amount * 2", "type": "DOUBLE"}]},
      "measures": ["sum(double_amount)"]}, "inline_source"),
    ({"source_model": {"name": "orders_inline", "data_source": "test", "sql_table": "orders",
                       "columns": [{"name": "id", "type": "INT", "primary_key": True},
                                   {"name": "amount", "type": "DOUBLE"}]},
      "measures": ["sum(amount)"]}, "inline_source"),
    ({**_PLAIN, "time_dimensions": [MONTH_TD]}, "time_dimensions"),
    ({**_PLAIN, "filters": ["status = 'paid'"]}, "filters"),
    ({**_PLAIN, "dimensions": [{"expression": "amount > 50", "name": "big"}]}, "computed_dimensions"),
    ("monthly_revenue", "saved_query"),
    ({**_PLAIN, "dimensions": ["region"], "to_many_handling": "associate"}, "to_many_handling"),
])
def test_structural_flag_counted_once(store: str, query: Any, flag: str) -> None:
    assert _run_query(store, query)["features"]["flags"][flag] == 1


def test_three_stage_list_counts_multistage_once(store: str) -> None:
    stages = [
        {"name": "s1", "source_model": "orders", "dimensions": ["customer_id"], "measures": [{"formula": "sum(amount)", "name": "rev"}]},
        {"name": "s2", "source_model": "s1", "dimensions": ["customer_id"], "measures": [{"formula": "max(rev)", "name": "top"}]},
        {"source_model": "s2", "measures": ["avg(top)"]},
    ]
    features = _run_query(store, stages)["features"]
    assert features["flags"]["multistage"] == 1
    assert features["aggregations"]["sum"] == 1
    assert features["aggregations"]["max"] == 1
    assert features["aggregations"]["avg"] == 1


# --- built-in and custom names ---------------------------------------------------------------------


def test_builtin_names_counted_by_name_and_custom_as_custom(store: str) -> None:
    features = _run_query(store, {
        "source_model": "orders",
        "time_dimensions": [MONTH_TD],
        "measures": ["time_shift(sum(amount), -1)", "dsum(amount)"],
    })["features"]
    assert features["transforms"]["time_shift"] == 1
    assert features["aggregations"]["sum"] == 1
    assert features["aggregations"]["custom"] == 1
    assert "dsum" not in features["aggregations"]


def test_features_counted_per_attempt_even_on_failure(store: str) -> None:
    report = _run_query(store, {"source_model": "orders", "measures": ["sum(no_such_column)"]})
    assert report["features"]["aggregations"]["sum"] == 1
    assert usage(report, "cli:query")["ok"] == 0


def test_features_counted_per_query(store: str) -> None:
    cli(["query", json.dumps(_PLAIN), "--storage", store])
    report = _run_query(store, _PLAIN)
    assert report["features"]["aggregations"]["sum"] == 2


# --- dialects --------------------------------------------------------------------------------------------


def test_queries_counted_per_dialect(store: str) -> None:
    assert _run_query(store, _PLAIN)["dialects"]["queries"] == {"sqlite": 1}


@pytest.mark.parametrize(("extra", "bucket"), [(0, "1"), (1, "2-5"), (4, "2-5"), (5, "6+")])
def test_datasources_per_dialect_bucketed(store: str, telemetry_env, extra: int, bucket: str) -> None:
    async def add() -> None:
        storage = YAMLStorage(base_dir=store)
        for i in range(extra):
            await storage.save_datasource(DatasourceConfig(
                name=f"extra_{i}", type="sqlite", database=str(telemetry_env.root / f"extra_{i}.db"),
            ))

    asyncio.run(add())
    cli(["datasources", "--storage", store, "list"])
    assert show()["dialects"]["datasources"] == {"sqlite": {bucket: 1}}


# --- context ------------------------------------------------------------------------------------------------


def test_model_count_is_bucketed(store: str) -> None:
    save_models(store, count=32)
    cli(["models", "--storage", store, "list"])
    context = show()["context"]
    assert context["model_count"] == {"11-50": 1}
    assert context["storage"] == {"yaml": 1}


def test_demo_datasource_in_use(telemetry_env) -> None:
    quiet_config()
    store = str(telemetry_env.storage_dir)
    cli(["datasources", "--storage", store, "list"])
    assert show()["context"]["demo"] == 0
    cli(["datasources", "--storage", store, "create", "demo", "--years", "1", "--ingest", "-y"])
    cli(["query", json.dumps({"source_model": "orders", "measures": ["count(*)"]}), "--storage", store])
    assert show()["context"]["demo"] >= 1


class _CountingStorage:
    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.calls: Counter[str] = Counter()

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if not callable(attr):
            return attr

        def counted(*args: Any, **kwargs: Any) -> Any:
            self.calls[name] += 1
            return attr(*args, **kwargs)

        return counted


def _storage_and_connection_calls(args: list[str], monkeypatch: pytest.MonkeyPatch) -> tuple[Counter, int]:
    spies: list[_CountingStorage] = []
    connections = 0
    real_resolve = slayer.cli.resolve_storage
    real_get_engine = engine_factory.get_engine

    def resolve(path: str) -> _CountingStorage:
        spy = _CountingStorage(real_resolve(path))
        spies.append(spy)
        return spy

    def get_engine(*a: Any, **k: Any) -> Any:
        nonlocal connections
        connections += 1
        return real_get_engine(*a, **k)

    with monkeypatch.context() as mp:
        mp.setattr(slayer.cli, "resolve_storage", resolve)
        mp.setattr(engine_factory, "get_engine", get_engine)
        cli(args)
    return sum((s.calls for s in spies), Counter()), connections


@pytest.mark.parametrize("command", [
    ["query", json.dumps(_PLAIN)],
    ["models", "list"],
    ["datasources", "list"],
    ["serve"],
])
def test_telemetry_makes_no_storage_call_or_connection(
    store: str, monkeypatch: pytest.MonkeyPatch, command: list[str],
) -> None:
    fake_uvicorn = type(sys)("uvicorn")
    fake_uvicorn.run = lambda **kwargs: None  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "uvicorn", fake_uvicorn)
    args = [*command[:1], "--storage", store, *command[1:]] if command[0] in ("models", "datasources") \
        else [*command, "--storage", store]
    on = _storage_and_connection_calls(args, monkeypatch)
    monkeypatch.setenv("SLAYER_TELEMETRY", "off")
    off = _storage_and_connection_calls(args, monkeypatch)
    assert on == off


# --- environment -------------------------------------------------------------------------------------------------


def test_environment_fields(store: str) -> None:
    env = _run_query(store, _PLAIN)["env"]
    assert env["slayer_version"] == pkg_version("motley-slayer")
    assert env["python"] == f"{sys.version_info.major}.{sys.version_info.minor}"
    assert env["os"] == {"linux": "linux", "darwin": "darwin", "win32": "windows"}.get(sys.platform, "other")
    assert env["arch"] in ("x86_64", "arm64", "other")
    if platform.machine().lower() in ("x86_64", "amd64"):
        assert env["arch"] == "x86_64"
    assert isinstance(env["in_container"], bool)
    assert "flight" in env["extras"]


_IMPORTS_SCRIPT = """
import contextlib, io, json, sys
import slayer.cli

def run(*args):
    sys.argv = ["slayer", *args]
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        try:
            slayer.cli.main()
        except SystemExit:
            pass
    return out.getvalue()

run("models", "--storage", {store!r}, "list")
shown = run("telemetry", "show")
print(json.dumps({{"modules": sorted(sys.modules), "shown": shown}}))
"""


def _imports_and_show(store: str, env: dict[str, str]) -> dict:
    result = run_python(_IMPORTS_SCRIPT.format(store=store), env=env)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_extras_detected_without_importing_them(store: str, telemetry_env) -> None:
    on = _imports_and_show(store, telemetry_env.subprocess_env())
    off = _imports_and_show(store, telemetry_env.subprocess_env(SLAYER_TELEMETRY="off"))
    added = set(on["modules"]) - set(off["modules"])
    foreign = sorted(m for m in added if m.split(".")[0] not in sys.stdlib_module_names | {"slayer"})
    assert foreign == []
    assert "flight" in json.loads(on["shown"])["env"]["extras"]
