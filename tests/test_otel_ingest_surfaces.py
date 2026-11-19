"""Every surface that ingests exports the same ``slayer.ingest`` span tree and ingest metrics."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from slayer.core.models import DatasourceConfig
from slayer.mcp.server import create_mcp_server
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._otel_helpers import OtelCapture
from tests._usage_reporting_helpers import cli

_TABLES = ("a", "b", "c")
_INGEST_CHILDREN = ["slayer.ingest.scan.schema", "slayer.ingest.merge", "slayer.ingest.drift", "slayer.ingest.embeddings"]


def _three_tables(tmp_path: Path) -> str:
    db = str(tmp_path / "app.db")
    with transaction(db) as conn:
        for table in _TABLES:
            conn.execute(f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, amount REAL)")
    return db


def _cli_create(tmp_path: Path, *extra: str) -> None:
    db = _three_tables(tmp_path)
    result = cli(["datasources", "--storage", str(tmp_path / "store"), "create", f"sqlite:///{db}", "--name", "app",
                  *extra])
    assert result.returncode == 0, result.stderr


def _ingest_spans(otel_env: OtelCapture) -> int:
    return otel_env.span_names().count("slayer.ingest")


def _duration_counts(otel_env: OtelCapture) -> list[int]:
    return [point.count for point in otel_env.metric("slayer.ingest.duration")]


def _dataset_counts(otel_env: OtelCapture) -> dict[str, float]:
    return {point.attributes["slayer.ingest.change"]: point.value for point in otel_env.metric("slayer.ingest.datasets")}


# --- CLI datasources create --ingest --------------------------------------------------------------


def test_cli_create_with_ingest_is_one_ingest_span_with_its_outcome(otel_env: OtelCapture, tmp_path: Path) -> None:
    _cli_create(tmp_path, "--ingest")
    attributes = otel_env.attributes("slayer.ingest")
    assert (_ingest_spans(otel_env), attributes["slayer.ingest.datasets.added"],
            attributes["slayer.datasource.name"], attributes["slayer.datasource.type"]) == (1, 3, "app", "sqlite")


@pytest.mark.parametrize("child", _INGEST_CHILDREN)
def test_cli_create_ingest_steps_nest_under_the_ingest_span(
    otel_env: OtelCapture, tmp_path: Path, child: str,
) -> None:
    _cli_create(tmp_path, "--ingest")
    assert otel_env.parent_name(child) == "slayer.ingest"


def test_cli_create_ingest_span_is_a_child_of_the_cli_request(otel_env: OtelCapture, tmp_path: Path) -> None:
    _cli_create(tmp_path, "--ingest")
    request = otel_env.attributes("slayer.request")
    assert (otel_env.span_names().count("slayer.request"), request["slayer.surface"], request["slayer.operation"],
            otel_env.parent_name("slayer.ingest")) == (1, "cli", "datasources.create", "slayer.request")


def test_cli_create_with_ingest_records_the_ingest_metrics(otel_env: OtelCapture, tmp_path: Path) -> None:
    _cli_create(tmp_path, "--ingest")
    assert (_duration_counts(otel_env), _dataset_counts(otel_env)) == ([1], {"added": 3})


def test_cli_create_without_ingest_is_a_request_span_alone(otel_env: OtelCapture, tmp_path: Path) -> None:
    _cli_create(tmp_path)
    request = otel_env.attributes("slayer.request")
    assert (request["slayer.surface"], request["slayer.operation"], _ingest_spans(otel_env)) == (
        "cli", "datasources.create", 0,
    )


# --- MCP create_datasource with auto-ingestion ---------------------------------------------------


async def _mcp_create(tmp_path: Path) -> None:
    db = _three_tables(tmp_path)
    server = create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path / "store")), _seed_help=False)
    await server.call_tool(name="create_datasource", arguments={"name": "app", "type": "sqlite", "database": db})


async def test_mcp_create_with_ingest_is_one_ingest_span_with_its_outcome(
    otel_env: OtelCapture, tmp_path: Path,
) -> None:
    await _mcp_create(tmp_path)
    attributes = otel_env.attributes("slayer.ingest")
    assert (_ingest_spans(otel_env), attributes["slayer.ingest.datasets.added"],
            attributes["slayer.datasource.name"], attributes["slayer.datasource.type"]) == (1, 3, "app", "sqlite")


@pytest.mark.parametrize("child", _INGEST_CHILDREN)
async def test_mcp_create_ingest_steps_nest_under_the_ingest_span(
    otel_env: OtelCapture, tmp_path: Path, child: str,
) -> None:
    await _mcp_create(tmp_path)
    assert otel_env.parent_name(child) == "slayer.ingest"


async def test_mcp_create_with_ingest_records_the_ingest_metrics(otel_env: OtelCapture, tmp_path: Path) -> None:
    await _mcp_create(tmp_path)
    assert (_duration_counts(otel_env), _dataset_counts(otel_env)) == ([1], {"added": 3})


# --- slayer ingest ---------------------------------------------------------------------------


async def _stored_datasource(tmp_path: Path) -> str:
    store = str(tmp_path / "store")
    await YAMLStorage(base_dir=store).save_datasource(
        DatasourceConfig(name="app", type="sqlite", database=_three_tables(tmp_path)),
    )
    return store


def test_slayer_ingest_exports_exactly_one_ingest_span(otel_env: OtelCapture, tmp_path: Path) -> None:
    store = asyncio.run(_stored_datasource(tmp_path))
    result = cli(["ingest", "--storage", store, "--datasource", "app"])
    assert (result.returncode, _ingest_spans(otel_env), _duration_counts(otel_env)) == (0, 1, [1])
