"""MCP query-tool response row cap: default 20-row cap, LIMIT push-down,
truncation notice via the warnings channel (spec: mcp/response-row-cap)."""

from __future__ import annotations

import copy
import csv
import io
import json
import sqlite3
from pathlib import Path
from typing import Any

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.core.warnings import NormalizationWarning, ResponseTruncationWarning
from slayer.engine.query_engine import (
    FieldMetadata,
    ResponseAttributes,
    SlayerQueryEngine,
    SlayerResponse,
)
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage

CAP = 20
NOTICE = f"showing first {CAP} rows — more rows exist"


def _make_db(workspace: Path, *, rows: int = 30) -> Path:
    db = workspace / "live.db"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE nums (id INTEGER PRIMARY KEY, v INTEGER)")
    conn.executemany(
        "INSERT INTO nums (id, v) VALUES (?, ?)",
        [(i, i * 10) for i in range(1, rows + 1)],
    )
    conn.commit()
    conn.close()
    return db


async def _seed_storage(workspace: Path) -> YAMLStorage:
    """A ``lite`` sqlite datasource with a 30-row ``nums`` model plus two
    stored query-backed models returning 25 (``qb_over``) and 20 (``qb_at_cap``) rows."""
    db = _make_db(workspace)
    storage = YAMLStorage(base_dir=str(workspace / "store"))
    await storage.save_datasource(DatasourceConfig(name="lite", type="sqlite", database=str(db)))
    await storage.save_model(SlayerModel(
        name="nums", data_source="lite", sql_table="nums",
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="v", sql="v", type=DataType.INT),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="qb_over", data_source="lite",
        source_queries=[SlayerQuery(
            source_model="nums", dimensions=["id"], filters=["id <= 25"],
        )],
    ))
    await storage.save_model(SlayerModel(
        name="qb_at_cap", data_source="lite",
        source_queries=[SlayerQuery(
            source_model="nums", dimensions=["id"], filters=["id <= 20"],
        )],
    ))
    return storage


async def _make_server(tmp_path: Path):
    storage = await _seed_storage(tmp_path)
    return create_mcp_server(storage=storage)


async def _call(server, *, name: str, arguments: dict[str, Any] | None = None) -> str:
    content_blocks, _ = await server.call_tool(name=name, arguments=arguments or {})
    return content_blocks[0].text


def _json_payload(text: str) -> Any:
    """Decode the leading JSON value, ignoring any trailing footer text."""
    payload, _ = json.JSONDecoder().raw_decode(text)
    return payload


def _json_after_plan(text: str) -> Any:
    return _json_payload(text.split("Query Plan:\n", 1)[1])


def _canned_response(n: int, *, warnings: list | None = None) -> SlayerResponse:
    return SlayerResponse(
        data=[{"nums.id": i} for i in range(1, n + 1)],
        columns=["nums.id"],
        sql="SELECT 1",
        warnings=warnings or [],
    )


def _patch_execute(monkeypatch, *, make_response) -> None:
    """Replace engine execution with a canned-response factory."""
    async def fake_execute(self, *args: Any, **kwargs: Any) -> SlayerResponse:
        return make_response()

    monkeypatch.setattr(SlayerQueryEngine, "execute", fake_execute)


def _assert_truncated(payload: Any, *, rows: int = CAP) -> dict:
    """Payload is {"data", "warnings"} with ``rows`` rows and a last
    truncation warning; returns that warning entry."""
    assert isinstance(payload, dict), f"expected truncated shape, got {type(payload)}"
    assert len(payload["data"]) == rows
    warning = payload["warnings"][-1]
    assert warning["kind"] == "truncated"
    assert warning["returned_rows"] == rows
    return warning


def _assert_untruncated(payload: Any, *, rows: int) -> None:
    assert isinstance(payload, list), f"expected bare array, got {type(payload)}"
    assert len(payload) == rows


class TestDefaultCap:
    """Requirement: default row cap on MCP query responses."""

    async def test_uncapped_query_over_large_result(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"], "format": "json",
        })
        _assert_truncated(_json_payload(result))

    async def test_result_exactly_at_cap(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"],
            "filters": ["id <= 20"], "format": "json",
        })
        _assert_untruncated(_json_payload(result), rows=20)

    async def test_result_one_past_cap(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"],
            "filters": ["id <= 21"], "format": "json",
        })
        _assert_truncated(_json_payload(result))


class TestExplicitLimitTrusted:
    """Requirement: an explicit ``limit`` is trusted verbatim — no slice, no notice."""

    async def test_explicit_limit_honored(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"],
            "limit": 25, "format": "json",
        })
        _assert_untruncated(_json_payload(result), rows=25)

    async def test_explicit_small_limit(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"],
            "limit": 5, "format": "json",
        })
        _assert_untruncated(_json_payload(result), rows=5)

    async def test_rows_exceeding_explicit_limit_not_sliced(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """The cap decision keys on caller args, never on the executed row count."""
        server = await _make_server(tmp_path)
        _patch_execute(monkeypatch, make_response=lambda: _canned_response(30))
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"],
            "limit": 5, "format": "json",
        })
        _assert_untruncated(_json_payload(result), rows=30)

    async def test_explain_with_explicit_limit_untouched(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        server = await _make_server(tmp_path)
        _patch_execute(monkeypatch, make_response=lambda: _canned_response(25))
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"],
            "limit": 5, "explain": True, "format": "json",
        })
        _assert_untruncated(_json_after_plan(result), rows=25)


class TestPushDown:
    """Requirement: cap push-down into the generated query (cap + 1)."""

    async def test_dry_run_sql_carries_limit_21(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"], "dry_run": True,
        })
        assert "LIMIT 21" in result
        assert "LIMIT 20" not in result

    async def test_show_sql_carries_limit_21(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"],
            "show_sql": True, "format": "json",
        })
        assert "LIMIT 21" in result
        assert "LIMIT 20" not in result

    async def test_run_by_name_stored_sql_untouched(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "qb_over", "dry_run": True,
        })
        assert "SQL:" in result
        assert "LIMIT" not in result.upper()


class TestRunByNameCap:
    """Requirement: run-by-name responses are capped response-side."""

    async def test_stored_query_above_cap(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "qb_over", "format": "json",
        })
        warning = _assert_truncated(_json_payload(result))
        # Uniform hint even though passing `limit` switches execution paths.
        assert "limit" in warning["hint"]

    async def test_stored_query_at_cap(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "qb_at_cap", "format": "json",
        })
        _assert_untruncated(_json_payload(result), rows=20)


class TestQueryNestedCap:
    """Requirement: query_nested capped by the root stage's limit only."""

    async def test_root_without_limit_capped(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query_nested", arguments={
            "queries": [{"source_model": "nums", "dimensions": ["id"]}],
            "format": "json",
        })
        warning = _assert_truncated(_json_payload(result))
        assert "root" in warning["hint"]
        assert "limit" in warning["hint"]

    async def test_non_root_limit_does_not_lift_cap(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query_nested", arguments={
            "queries": [
                {"name": "base", "source_model": "nums",
                 "dimensions": ["id"], "limit": 30},
                {"source_model": "base", "dimensions": ["id"]},
            ],
            "format": "json",
        })
        _assert_truncated(_json_payload(result))

    async def test_root_limit_trusted(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query_nested", arguments={
            "queries": [
                {"source_model": "nums", "dimensions": ["id"], "limit": 25},
            ],
            "format": "json",
        })
        _assert_untruncated(_json_payload(result), rows=25)

    async def test_rows_exceeding_root_limit_not_sliced(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        server = await _make_server(tmp_path)
        _patch_execute(monkeypatch, make_response=lambda: _canned_response(30))
        result = await _call(server, name="query_nested", arguments={
            "queries": [
                {"source_model": "nums", "dimensions": ["id"], "limit": 5},
            ],
            "format": "json",
        })
        _assert_untruncated(_json_payload(result), rows=30)

    async def test_root_dry_run_sql_carries_limit_21(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query_nested", arguments={
            "queries": [{"source_model": "nums", "dimensions": ["id"]}],
            "dry_run": True,
        })
        assert "LIMIT 21" in result
        assert "LIMIT 20" not in result

    async def test_caller_dicts_not_mutated(self, tmp_path: Path) -> None:
        """Push-down must copy the root dict, not add ``limit`` to caller input."""
        server = await _make_server(tmp_path)
        # Direct closure call: the wire path would copy the dicts anyway.
        fn = server._tool_manager.get_tool("query_nested").fn
        queries = [{"source_model": "nums", "dimensions": ["id"]}]
        snapshot = copy.deepcopy(queries)
        await fn(queries=queries, format="json")
        assert queries == snapshot


class TestNoticeRendering:
    """Requirement: truncation notice content and rendering in all formats."""

    async def test_markdown_notice(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"],
        })
        assert "Warnings:" in result
        assert NOTICE in result
        assert "limit" in result.split("Warnings:", 1)[1]
        # The Warnings block ends the output, notice last.
        assert NOTICE in result.rstrip().splitlines()[-1]
        assert result.index("Warnings:") > result.index("| ")
        table_lines = [ln for ln in result.splitlines() if ln.startswith("|")]
        assert len(table_lines) == 2 + CAP  # header + separator + rows

    async def test_csv_notice(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id", "v"], "format": "csv",
        })
        lines = result.splitlines()
        assert lines[0].startswith("#")
        assert NOTICE in lines[0]
        data_lines = [ln for ln in lines if not ln.startswith("#")]
        rows = list(csv.reader(io.StringIO("\n".join(data_lines))))
        assert len(rows) == 1 + CAP  # header + rows
        assert all(len(r) == 2 for r in rows)

    async def test_json_notice(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"], "format": "json",
        })
        warning = _assert_truncated(_json_payload(result))
        assert "limit" in warning["hint"]

    async def test_coexists_with_other_warnings(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        server = await _make_server(tmp_path)
        norm = NormalizationWarning(
            rule_id="R1", original="a", normalized="b", location="filters[0]",
        )
        _patch_execute(
            monkeypatch,
            make_response=lambda: _canned_response(25, warnings=[norm]),
        )
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"], "format": "json",
        })
        payload = _json_payload(result)
        _assert_truncated(payload)
        kinds = [w["kind"] for w in payload["warnings"]]
        assert kinds == ["normalization", "truncated"]

        md = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"],
        })
        assert "[R1]" in md
        assert NOTICE in md
        assert md.index("[R1]") < md.index(NOTICE)

        csv_out = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"], "format": "csv",
        })
        comment_lines = [ln for ln in csv_out.splitlines() if ln.startswith("#")]
        assert len(comment_lines) == 2
        assert "[R1]" in comment_lines[0]
        assert NOTICE in comment_lines[1]

    async def test_markdown_notice_trails_attributes(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        """Field attributes render before the trailing Warnings block, so the
        notice stays last even when the response carries metadata."""
        server = await _make_server(tmp_path)
        attrs = ResponseAttributes(dimensions={"nums.id": FieldMetadata(label="ID")})
        _patch_execute(
            monkeypatch,
            make_response=lambda: SlayerResponse(
                data=[{"nums.id": i} for i in range(1, 25)],
                columns=["nums.id"], sql="SELECT 1", attributes=attrs,
            ),
        )
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"],
        })
        assert "Dimension attributes:" in result
        assert result.index("Dimension attributes:") < result.index("Warnings:")
        assert NOTICE in result.rstrip().splitlines()[-1]

    def test_union_round_trip(self) -> None:
        resp = SlayerResponse(
            data=[],
            warnings=[ResponseTruncationWarning(
                returned_rows=CAP, hint="pass a higher 'limit'",
            )],
        )
        parsed = SlayerResponse.model_validate_json(resp.model_dump_json())
        warning = parsed.warnings[0]
        assert isinstance(warning, ResponseTruncationWarning)
        assert warning.kind == "truncated"
        assert warning.returned_rows == CAP
        assert warning.hint == "pass a higher 'limit'"


class TestExplainDryRun:
    """Requirement: explain plans capped without a limit; dry_run never truncates."""

    async def test_large_explain_plan_capped(
        self, tmp_path: Path, monkeypatch,
    ) -> None:
        server = await _make_server(tmp_path)
        _patch_execute(monkeypatch, make_response=lambda: _canned_response(25))
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"],
            "explain": True, "format": "json",
        })
        _assert_truncated(_json_after_plan(result))

    async def test_dry_run_never_truncates(self, tmp_path: Path) -> None:
        server = await _make_server(tmp_path)
        result = await _call(server, name="query", arguments={
            "source_model": "nums", "dimensions": ["id"], "dry_run": True,
        })
        assert "SQL:" in result
        assert "truncated" not in result
        assert "Warnings:" not in result
