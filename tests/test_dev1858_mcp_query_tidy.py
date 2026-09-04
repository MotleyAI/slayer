"""DEV-1858 — the MCP ``query`` tool is a thin ``engine.execute`` wrapper.

Spec: openspec/changes/dev-1858-tidy-up-mcp-query-tools/specs/mcp/query-tool.
One polymorphic ``query`` argument (string / object / list) mirroring the
engine's accepted union, plus only the execution-wrapper arguments; the
``query_nested`` tool and the per-field arguments are retired.
"""

import json
import sqlite3
from typing import Any

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage
from tests import _dev1836_fixtures as fx

RETIRED_ARGS = {
    "source_model", "measures", "dimensions", "filters", "time_dimensions",
    "order", "limit", "offset", "whole_periods_only", "strict",
    "distinct_dimension_values",
}
UNIFIED_ARGS = {"query", "variables", "show_sql", "dry_run", "explain", "format"}


def _is_object_shaped(schema: dict) -> bool:
    """A JSON-schema node that resolves to an object: inline ``type: object``,
    a ``$ref`` to a model def, or a union with such a branch."""
    if schema.get("type") == "object" or "$ref" in schema:
        return True
    return any(
        _is_object_shaped(b) for b in (schema.get("anyOf") or schema.get("oneOf") or [])
    )


def _seed_orders_db(db_path: str) -> None:
    """6-row ``orders`` table: 3 distinct statuses (completed x3, pending x2,
    cancelled x1) so dim-only dedup is observable in the row count."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT NOT NULL, "
        "amount REAL NOT NULL, created_at TEXT NOT NULL)"
    )
    conn.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?)",
        [
            (1, "completed", 100.0, "2025-01-15"),
            (2, "completed", 200.0, "2025-01-20"),
            (3, "pending", 50.0, "2025-02-10"),
            (4, "cancelled", 75.0, "2025-02-15"),
            (5, "completed", 300.0, "2025-03-05"),
            (6, "pending", 25.0, "2025-03-20"),
        ],
    )
    conn.commit()
    conn.close()


@pytest.fixture
async def orders_server(tmp_path):
    """MCP server over a live SQLite ``orders`` model plus a query-backed
    ``qb_by_status`` model. ``status`` carries a label so the run-by-name
    result exposes dimension attribute metadata."""
    db_path = str(tmp_path / "orders.db")
    _seed_orders_db(db_path)
    storage = YAMLStorage(base_dir=str(tmp_path / "store"))
    await storage.save_datasource(
        DatasourceConfig(name="ds", type="sqlite", database=db_path)
    )
    await storage.save_model(SlayerModel(
        name="orders", sql_table="orders", data_source="ds",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT, label="Order Status"),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ],
    ))
    await storage.save_model(SlayerModel(
        name="qb_by_status", data_source="ds",
        source_queries=[SlayerQuery(
            source_model="orders",
            dimensions=["status"],
            measures=[{"formula": "amount:sum"}],
        )],
    ))
    return create_mcp_server(storage=storage)


async def _call(server, *, name: str = "query", **arguments: Any) -> str:
    content_blocks, _ = await server.call_tool(name=name, arguments=arguments)
    return content_blocks[0].text


def _rows(out: str) -> list:
    """Decode the leading JSON array, ignoring any trailing attributes footer
    (json output appends a 'Measure attributes:' block when a measure carries
    a display format)."""
    decoded, _ = json.JSONDecoder().raw_decode(out)
    return decoded


# ---------------------------------------------------------------------------
# 1.1 — schema regression
# ---------------------------------------------------------------------------


class TestQueryToolSchema:
    async def test_exposes_only_the_unified_arguments(self, orders_server) -> None:
        tools = {t.name: t for t in await orders_server.list_tools()}
        schema = tools["query"].inputSchema
        assert set(schema.get("properties", {})) == UNIFIED_ARGS
        # Only `query` is required — the wrappers must all carry defaults.
        assert set(schema.get("required", [])) == {"query"}
        for retired in RETIRED_ARGS:
            assert retired not in schema.get("properties", {}), retired

    async def test_query_argument_accepts_string_object_and_array(
        self, orders_server,
    ) -> None:
        """Structural, not substring: the union must keep a string branch, an
        object branch (inline ``type: object`` or a ``$ref`` model), and an
        array-of-objects branch — so it fails if any input form regresses (e.g.
        the list narrowing to ``list[str]``)."""
        tools = {t.name: t for t in await orders_server.list_tools()}
        q_schema = tools["query"].inputSchema["properties"]["query"]
        branches = q_schema.get("anyOf") or q_schema.get("oneOf") or [q_schema]
        assert any(b.get("type") == "string" for b in branches), q_schema
        assert any(_is_object_shaped(b) for b in branches), q_schema
        array_branches = [b for b in branches if b.get("type") == "array"]
        assert array_branches, q_schema
        # The list form is a list of query OBJECTS, not list[str].
        assert any(
            _is_object_shaped(b.get("items", {})) for b in array_branches
        ), q_schema

    async def test_no_query_nested_tool_registered(self, orders_server) -> None:
        names = {t.name for t in await orders_server.list_tools()}
        assert "query_nested" not in names
        assert "query" in names


# ---------------------------------------------------------------------------
# 1.2 — input dispatch (str / dict / list), mirroring engine.execute
# ---------------------------------------------------------------------------


class TestQueryDispatch:
    async def test_dict_runs_a_single_query(self, orders_server) -> None:
        out = await _call(
            orders_server,
            query={
                "source_model": "orders",
                "measures": [{"formula": "*:count"}],
                "dimensions": ["status"],
            },
            format="json",
        )
        rows = _rows(out)
        assert isinstance(rows, list)
        assert len(rows) == 3  # three distinct statuses

    async def test_list_runs_a_two_stage_dag(self, orders_server) -> None:
        out = await _call(
            orders_server,
            query=[
                {
                    "name": "monthly",
                    "source_model": "orders",
                    "measures": [{"formula": "amount:sum"}],
                    "time_dimensions": [
                        {"dimension": "created_at", "granularity": "month"},
                    ],
                },
                {"source_model": "monthly", "measures": [{"formula": "*:count"}]},
            ],
            dry_run=True,
        )
        assert "SQL:" in out
        assert "COUNT(" in out.upper()

    async def test_query_backed_string_runs_by_name(self, orders_server) -> None:
        out = await _call(orders_server, query="qb_by_status", format="json")
        rows = _rows(out)
        assert isinstance(rows, list)
        assert len(rows) == 3  # grouped by the three distinct statuses
        # Result keys keep the underlying model prefix; totals sum to 750.0.
        assert sum(float(r["orders.amount_sum"]) for r in rows) == pytest.approx(750.0)

    async def test_non_query_backed_string_raises_not_query_backed(
        self, orders_server,
    ) -> None:
        """A bare table-backed model name is exact engine string semantics:
        it raises the engine's 'not query-backed' error (which names the
        ``source_model=`` remedy), not a silent wrap."""
        with pytest.raises(ToolError) as ei:
            await _call(orders_server, query="orders")
        message = str(ei.value)
        assert "not query-backed" in message
        assert "source_model='orders'" in message

    async def test_empty_list_is_rejected(self, orders_server) -> None:
        with pytest.raises(ToolError, match="non-empty"):
            await _call(orders_server, query=[])

    async def test_object_honors_order_and_limit(self, orders_server) -> None:
        """``order``/``limit``/``offset`` are retired as tool arguments; they now
        live inside the query object and still shape the result."""
        out = await _call(
            orders_server,
            query={
                "source_model": "orders",
                "dimensions": ["status"],
                "measures": [{"formula": "*:count"}],
                "order": [{"column": "*:count", "direction": "desc"}],
                "limit": 1,
            },
            format="json",
        )
        rows = _rows(out)
        assert len(rows) == 1  # limit honored
        # Ordered by count desc → 'completed' (3) is the top group.
        assert rows[0]["orders.status"] == "completed"


# ---------------------------------------------------------------------------
# 1.3 — in-query control fields (strict / distinct_dimension_values)
# ---------------------------------------------------------------------------


class TestInQueryControlFields:
    async def test_distinct_dimension_values_false_in_json_returns_raw_rows(
        self, orders_server,
    ) -> None:
        out = await _call(
            orders_server,
            query={
                "source_model": "orders",
                "dimensions": ["status"],
                "distinct_dimension_values": False,
            },
            format="json",
        )
        rows = _rows(out)
        assert isinstance(rows, list)
        assert len(rows) == 6  # raw rows, no dedup


@pytest.fixture
async def broadcast_server(tmp_path):
    """MCP server over the DEV-1836 cross-model fixture, where an implicit-grain
    broadcast warns in lenient mode and errors under in-query ``strict``."""
    db_path = str(tmp_path / "data.sqlite")
    fx._seed_sqlite(db_path)
    engine = await fx._engine_for(dialect="sqlite", db_path=db_path)
    return create_mcp_server(storage=engine.storage)


_BROADCAST_QUERY = {
    "source_model": "orders",
    "dimensions": ["status"],
    "measures": [
        {"formula": "amount:sum", "name": "m"},
        {"formula": "customers.spend:sum", "name": "cm"},
    ],
}


class TestInQueryStrict:
    async def test_strict_true_in_json_errors_on_broadcast(
        self, broadcast_server,
    ) -> None:
        with pytest.raises(ToolError, match="cardinality|unique|status"):
            await _call(broadcast_server, query={**_BROADCAST_QUERY, "strict": True})

    async def test_without_strict_the_same_query_returns_rows_with_warning(
        self, broadcast_server,
    ) -> None:
        """Lenient mode: the broadcast is a warning, not an error. JSON output
        carrying warnings is the ``{"data": [...], "warnings": [...]}`` shape."""
        out = await _call(broadcast_server, query=_BROADCAST_QUERY, format="json")
        payload, _ = json.JSONDecoder().raw_decode(out)
        assert isinstance(payload, dict)
        assert payload["data"]      # rows returned, not an error
        assert payload["warnings"]  # broadcast surfaced as a warning


# ---------------------------------------------------------------------------
# 1.4 — execution wrappers and output shaping on the new shape
# ---------------------------------------------------------------------------


class TestExecutionWrappers:
    async def test_runtime_variables_override_in_query_variables(
        self, orders_server,
    ) -> None:
        """Runtime ``variables=`` wins over the query json's own ``variables``."""
        out = await _call(
            orders_server,
            query={
                "source_model": "orders",
                "measures": [{"formula": "*:count"}],
                "filters": ["status == '{s}'"],
                "variables": {"s": "pending"},
            },
            variables={"s": "completed"},
            dry_run=True,
        )
        assert "SQL:" in out
        assert "completed" in out
        assert "pending" not in out

    async def test_dry_run_returns_sql_only(self, orders_server) -> None:
        out = await _call(
            orders_server,
            query={
                "source_model": "orders",
                "dimensions": ["status"],
                "measures": [{"formula": "*:count"}],
            },
            dry_run=True,
        )
        assert out.startswith("SQL:")
        # No result rows: the status data values only appear in executed output,
        # never in the generated SQL.
        for value in ("completed", "pending", "cancelled"):
            assert value not in out

    async def test_explain_returns_sql_and_plan(self, orders_server) -> None:
        out = await _call(
            orders_server,
            query={"source_model": "orders", "measures": [{"formula": "*:count"}]},
            explain=True,
        )
        assert "SQL:" in out
        assert "Query Plan:" in out

    async def test_show_sql_prefixes_results(self, orders_server) -> None:
        out = await _call(
            orders_server,
            query={"source_model": "orders", "measures": [{"formula": "*:count"}]},
            show_sql=True,
        )
        assert out.startswith("SQL:")
        assert "orders._count" in out

    async def test_invalid_format_is_rejected_naming_all_valid_formats(
        self, orders_server,
    ) -> None:
        with pytest.raises(ToolError) as ei:
            await _call(
                orders_server,
                query={"source_model": "orders", "measures": [{"formula": "*:count"}]},
                format="xml",
            )
        message = str(ei.value)
        for valid in ("json", "csv", "markdown"):
            assert valid in message

    async def test_format_is_case_insensitive(self, orders_server) -> None:
        """The spec requires case-insensitive format handling; ``JSON`` works."""
        out = await _call(
            orders_server,
            query={
                "source_model": "orders",
                "dimensions": ["status"],
                "measures": [{"formula": "*:count"}],
            },
            format="JSON",
        )
        assert isinstance(_rows(out), list)

    async def test_attributes_block_appended_on_run_by_name(
        self, orders_server,
    ) -> None:
        """Regression: the old run-by-name branch skipped the attributes block;
        unified output appends it whenever the result carries attribute
        metadata (here the labeled ``status`` dimension). The block is the
        tail of the output."""
        out = await _call(orders_server, query="qb_by_status")
        assert "Dimension attributes:" in out
        # The populated block ends the output (the label is its last line).
        assert out.rstrip().endswith("Order Status")
