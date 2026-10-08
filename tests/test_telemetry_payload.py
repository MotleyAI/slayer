"""system.arc42.md principle 17: a usage report carries only SLayer-defined vocabulary."""

from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import re
from pathlib import Path

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from mcp.shared.memory import create_connected_server_and_client_session
from mcp.types import Implementation

import slayer.cli
from slayer import telemetry
from slayer.api.server import create_app
from slayer.core.enums import BUILTIN_AGGREGATIONS
from slayer.core.formula import ALL_TRANSFORMS
from slayer.flight.translator import TranslationError
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage
from slayer.telemetry.payload import SCHEMA_VERSION, VERSION_PATTERN, UsageReport
from tests._telemetry_helpers import (
    HOSTILE,
    SENT,
    assert_clean_everywhere,
    assert_no_sentinel,
    build_sentinel_storage,
    cli,
    end_process,
    enum_values,
    make_due,
    quiet_config,
    show,
    usage,
    usage_key_vocabulary,
)
from tests.flight.test_handlers import _make_handlers
from tests.pg_facade.test_connection import _parse, _bind, _execute, _query, _run, _startup, _sync, _terminate

# --- the schema policy ----------------------------------------------------------------


def _resolve(schema: dict, node: dict) -> dict:
    while "$ref" in node:
        node = schema["$defs"][node["$ref"].rsplit("/", 1)[-1]]
    return node


def _check_leaf(schema: dict, node: dict, path: str) -> None:
    node = _resolve(schema, node)
    if "enum" in node or "const" in node:
        return
    branches = node.get("anyOf") or node.get("oneOf")
    if branches:
        for i, branch in enumerate(branches):
            _check_leaf(schema, branch, f"{path}|{i}")
        return
    kind = node.get("type")
    if kind in ("null", "boolean"):
        return
    if kind == "integer":
        assert node.get("minimum", -1) >= 0, f"{path}: integer may be negative"
        return
    if kind == "string":
        allowed = node.get("format") in ("date", "uuid") or node.get("pattern") == VERSION_PATTERN
        assert allowed, f"{path}: free-form string"
        return
    if kind == "array":
        _check_leaf(schema, node["items"], f"{path}[]")
        return
    if kind == "object":
        for name, sub in node.get("properties", {}).items():
            _check_leaf(schema, sub, f"{path}.{name}")
        extra = node.get("additionalProperties")
        assert extra is not True, f"{path}: open object"
        if isinstance(extra, dict):
            assert enum_values(schema, node.get("propertyNames", {})), f"{path}: dict keys are not vocabulary"
            _check_leaf(schema, extra, f"{path}[*]")
        return
    raise AssertionError(f"{path}: disallowed type {kind!r}")


def test_every_report_leaf_is_vocabulary() -> None:
    schema = UsageReport.model_json_schema()
    _check_leaf(schema, schema, "report")


@pytest.mark.parametrize("value", ["1.1.2", "3.12", "0.0.0+unknown", "1.2.0rc1", "1.1.2.dev3+g1a2b3c"])
def test_version_pattern_accepts_versions(value: str) -> None:
    assert re.fullmatch(VERSION_PATTERN, value)


@pytest.mark.parametrize("value", [SENT, HOSTILE, "1.2.3 extra", "x1.2", "1.2;DROP", "", "../1.2"])
def test_version_pattern_rejects_free_text(value: str) -> None:
    assert not re.fullmatch(VERSION_PATTERN, value)


# --- vocabulary covers every surface's fixed tokens ----------------------------------------


class _Captured(Exception):
    def __init__(self, parser: argparse.ArgumentParser) -> None:
        super().__init__()
        self.parser = parser


def _cli_parser(monkeypatch: pytest.MonkeyPatch) -> argparse.ArgumentParser:
    def capture(self: argparse.ArgumentParser, *args, **kwargs):
        raise _Captured(self)

    monkeypatch.setattr(argparse.ArgumentParser, "parse_args", capture)
    with pytest.raises(_Captured) as info:
        slayer.cli.main()
    return info.value.parser


def _sub_parsers(parser: argparse.ArgumentParser, path: tuple[str, ...] = ()):
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for name, sub in action.choices.items():
                yield path + (name,), sub
                yield from _sub_parsers(sub, path + (name,))


def _runs_bare(parser: argparse.ArgumentParser) -> bool:
    """A leaf, or a group that runs without a subcommand (``slayer search``)."""
    has_subcommands = any(isinstance(a, argparse._SubParsersAction) for a in parser._actions)
    return not has_subcommands or parser.prog.endswith(" search")


def test_every_cli_command_has_a_vocabulary_token(monkeypatch: pytest.MonkeyPatch) -> None:
    vocabulary = usage_key_vocabulary()
    tokens = {}
    for path, parser in _sub_parsers(_cli_parser(monkeypatch)):
        if not _runs_bare(parser):
            continue
        token = parser.get_default("telemetry_token")
        assert isinstance(token, str) and token, f"slayer {' '.join(path)} has no telemetry token"
        assert f"cli:{token}" in vocabulary, token
        tokens[path] = token
    assert len(set(tokens.values())) == len(tokens)
    assert {("telemetry", "show"), ("search",), ("search", "refresh-samples")} <= set(tokens)


def test_every_mcp_tool_has_a_vocabulary_token(tmp_path: Path) -> None:
    server = create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path)), _seed_help=False)
    names = {tool.name for tool in asyncio.run(server.list_tools())}
    vocabulary = usage_key_vocabulary()
    assert names
    assert {f"mcp:{name}" for name in names} <= vocabulary
    assert "mcp:other" in vocabulary


def test_every_rest_route_has_a_vocabulary_token(tmp_path: Path) -> None:
    app = create_app(storage=YAMLStorage(base_dir=str(tmp_path)))
    paths = {route.path for route in app.routes if isinstance(route, APIRoute)}
    assert {f"rest:{path}" for path in paths} <= usage_key_vocabulary()


def test_protocol_tokens_in_vocabulary() -> None:
    assert {"flight:query", "pg:query"} <= usage_key_vocabulary()


def test_feature_vocabulary_covers_builtins() -> None:
    schema = UsageReport.model_json_schema()
    features = _resolve(schema, schema["properties"]["features"])
    transforms = _resolve(schema, features["properties"]["transforms"])
    aggregations = _resolve(schema, features["properties"]["aggregations"])
    assert set(ALL_TRANSFORMS) | {"custom"} <= enum_values(schema, transforms["propertyNames"])
    assert set(BUILTIN_AGGREGATIONS) | {"custom"} <= enum_values(schema, aggregations["propertyNames"])


# --- sentinel fuzz: hostile user strings never leave ----------------------------------------


@pytest.fixture
def sentinel_store(telemetry_env) -> str:
    quiet_config()
    return build_sentinel_storage(telemetry_env.root)


def test_cli_arguments_and_names_never_leave(sentinel_store: str, telemetry_env) -> None:
    query = {
        "source_model": f"{SENT}_model",
        "dimensions": [{"expression": f"{SENT}_col * 2", "name": SENT}],
        "measures": [f"{SENT}_agg({SENT}_col)", {"formula": f"sum({SENT}_col)", "name": f"{SENT}_m"}],
        "filters": [f"{SENT}_col > 0 or '{HOSTILE}' == 'x'"],
    }
    runs = [
        ["query", json.dumps(query), "--storage", sentinel_store],
        ["query", json.dumps({**query, "source_model": HOSTILE}), "--storage", sentinel_store],
        ["query", HOSTILE, "--storage", sentinel_store],
        ["models", "--storage", sentinel_store, "show", HOSTILE],
        ["models", "--storage", sentinel_store, "list"],
        ["datasources", "--storage", sentinel_store, "list"],
        ["datasources", "--storage", sentinel_store, "show", f"{SENT}_ds"],
        ["memory", "--storage", sentinel_store, "save", "--learning", HOSTILE, "--entities", f"{SENT}_ds.{SENT}_model"],
        ["inspect", f"{SENT}_model", "--type", "model", "--storage", sentinel_store],
        ["search", "--question", HOSTILE, "--storage", sentinel_store],
        ["recommend-root-model", f"{SENT}_model.{SENT}_col", "--storage", sentinel_store],
    ]
    for args in runs:
        cli(args)
    assert_clean_everywhere()


async def test_mcp_arguments_errors_and_client_never_leave(sentinel_store: str) -> None:
    telemetry.start()
    server = create_mcp_server(storage=YAMLStorage(base_dir=sentinel_store), _seed_help=False)
    client = Implementation(name=HOSTILE, version=HOSTILE)
    async with create_connected_server_and_client_session(server, client_info=client) as session:
        await session.call_tool("query", {"query": {
            "source_model": f"{SENT}_model",
            "measures": [f"{SENT}_agg({SENT}_col)"],
            "filters": [f"'{HOSTILE}' == '{HOSTILE}'"],
        }})
        await session.call_tool("inspect_model", {"model_name": HOSTILE})
        await session.call_tool("save_memory", {"learning": HOSTILE, "linked_entities": [f"{SENT}_ds.{SENT}_model"]})
        await session.call_tool("search", {"question": HOSTILE})
        await session.call_tool("describe_datasource", {"name": HOSTILE})
        await session.call_tool(SENT, {"x": HOSTILE})
    end_process()
    assert_clean_everywhere()


def test_rest_paths_bodies_and_errors_never_leave(sentinel_store: str) -> None:
    telemetry.start()
    client = TestClient(create_app(storage=YAMLStorage(base_dir=sentinel_store)))
    client.get(f"/models/{SENT}_model")
    client.get(f"/models/{SENT}missing")
    client.get(f"/{SENT}/unmatched")
    client.post("/query", json={"source_model": f"{SENT}_model", "measures": [f"{SENT}_agg({SENT}_col)"]})
    client.post("/query", json={"source_model": HOSTILE, "measures": ["count(*)"]})
    client.post(f"/mcp/messages/?session_id={SENT}", json={"x": HOSTILE})
    end_process()
    assert_clean_everywhere()


class _HostileError(Exception):
    pass


class _HostileEngine:
    async def execute(self, *, query=None, data_source=None):  # NOSONAR(S7503) — awaited interface
        raise _HostileError(f"SELECT {HOSTILE} FROM {SENT}_table")


async def test_pg_sql_and_engine_errors_never_leave(telemetry_env) -> None:
    quiet_config()
    telemetry.start()
    await _run(
        _startup(user=SENT, database=SENT)
        + _query(f"SELECT revenue_sum FROM orders WHERE status = '{HOSTILE}'")
        + _query(f"SELECT {SENT} FROM {SENT}")
        + _parse("", f"SELECT revenue_sum FROM orders WHERE status = '{SENT}'")
        + _bind("", "")
        + _execute("")
        + _sync()
        + _terminate(),
        engine=_HostileEngine(),
    )
    end_process()
    assert_clean_everywhere()


def test_flight_sql_never_leaves(telemetry_env) -> None:
    quiet_config()
    telemetry.start()
    handlers = _make_handlers()
    handlers.do_get_for_sql(f"SELECT revenue_sum FROM jaffle.orders WHERE status = '{HOSTILE}'")
    with pytest.raises(TranslationError):
        handlers.do_get_for_sql(f"SELECT {SENT} FROM {SENT}")
    end_process()
    assert_clean_everywhere()


def test_sent_report_never_carries_a_sentinel(sentinel_store: str, telemetry_env) -> None:
    cli(["query", json.dumps({"source_model": HOSTILE, "measures": ["count(*)"]}), "--storage", sentinel_store])
    cli(["models", "--storage", sentinel_store, "show", HOSTILE])
    make_due()
    cli(["models", "--storage", sentinel_store, "list"])
    requests = telemetry_env.capture.wait_for(count=1)
    assert len(requests) == 1
    assert_no_sentinel(requests[0].body)


# --- error tokens --------------------------------------------------------------------------


class _UnlistedError(Exception):
    pass


def test_unknown_exception_class_counts_as_other(telemetry_env) -> None:
    quiet_config()
    telemetry.start()
    telemetry.record(surface="mcp", token="query", error=_UnlistedError("boom"))
    end_process()
    assert usage(show(), "mcp:query")["errors"] == {"other": 1}


def test_error_message_is_never_sent(telemetry_env) -> None:
    quiet_config()
    telemetry.start()
    telemetry.record(surface="mcp", token="query", error=ValueError(f"no such table {SENT}_table: SELECT {HOSTILE}"))
    end_process()
    report = show()
    entry = usage(report, "mcp:query")
    assert entry["ok"] == 0
    assert sum(entry["errors"].values()) == 1
    assert_no_sentinel(json.dumps(report))


# --- identity and time resolution ----------------------------------------------------------------


def test_report_identity_fields(telemetry_env) -> None:
    quiet_config()
    telemetry.start()
    telemetry.record(surface="mcp", token="query")
    end_process()
    report = show()
    assert report["schema"] == SCHEMA_VERSION
    assert report["install_id"] and report["batch_id"]
    assert report["install_id"] != report["batch_id"]
    assert show()["batch_id"] != report["batch_id"]


def test_period_holds_dates_only(telemetry_env) -> None:
    day_one = datetime.datetime(2026, 3, 1, 9, 30, tzinfo=datetime.timezone.utc)
    day_two = datetime.datetime(2026, 3, 2, 17, 45, tzinfo=datetime.timezone.utc)
    quiet_config(last_sent=day_one)
    for day in (day_one, day_two):
        telemetry_env.set_now(day)
        telemetry.start()
        telemetry.record(surface="mcp", token="query")
        end_process()
    report = show()
    assert report["period"] == {"first": "2026-03-01", "last": "2026-03-02"}
    assert not re.search(r"\d{2}:\d{2}", json.dumps(report))
