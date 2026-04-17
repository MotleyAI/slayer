"""Tests for the MCP server tools."""

import json
import os
import tempfile
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    DatasourceConfig,
    Dimension,
    Measure,
    ModelJoin,
    SlayerModel,
)
from slayer.mcp.server import (
    _build_sample_query_args,
    _collect_reachable_fields,
    _escape_md_cell,
    _format_table,
    _friendly_db_error,
    _markdown_table,
    _strip_model_prefix,
    create_mcp_server,
)
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def storage() -> YAMLStorage:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


@pytest.fixture
def mcp_server(storage: YAMLStorage):
    return create_mcp_server(storage=storage)


async def _call(mcp_server, *, name: str, arguments: Optional[dict[str, Any]] = None) -> str:
    """Call an MCP tool and return the text result."""
    content_blocks, result_dict = await mcp_server.call_tool(name=name, arguments=arguments or {})
    return content_blocks[0].text


class TestModelsSummary:
    async def test_datasource_not_found(self, mcp_server) -> None:
        result = await _call(mcp_server, name="models_summary", arguments={"datasource_name": "nope"})
        assert "not found" in result

    async def test_empty_when_datasource_has_no_models(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="h"))
        result = await _call(mcp_server, name="models_summary", arguments={"datasource_name": "mydb"})
        assert "has no models" in result

    async def test_filters_by_datasource(self, mcp_server, storage: YAMLStorage) -> None:
        """Only models whose ``data_source`` matches the arg appear in the output."""
        await storage.save_datasource(DatasourceConfig(name="ds_a", type="postgres", host="h"))
        await storage.save_datasource(DatasourceConfig(name="ds_b", type="postgres", host="h"))
        await storage.save_model(SlayerModel(name="orders_a", sql_table="t", data_source="ds_a"))
        await storage.save_model(SlayerModel(name="orders_b", sql_table="t", data_source="ds_b"))
        result = await _call(mcp_server, name="models_summary", arguments={"datasource_name": "ds_a"})
        assert "orders_a" in result
        assert "orders_b" not in result

    async def test_markdown_structure(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="h"))
        await storage.save_model(SlayerModel(
            name="orders",
            sql_table="t",
            data_source="mydb",
            description="Orders fact table.",
            dimensions=[Dimension(name="status", type=DataType.STRING, description="Order state")],
            measures=[Measure(name="revenue", sql="amount", description="USD")],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        ))
        result = await _call(mcp_server, name="models_summary", arguments={"datasource_name": "mydb"})
        assert result.startswith("# Datasource: `mydb` — 1 model(s)")
        assert "## `orders`" in result
        assert "Orders fact table." in result
        assert "**Dimensions (1):**" in result
        assert "| status |" in result
        assert "Order state" in result
        assert "**Measures (1):**" in result
        assert "| revenue |" in result
        assert "USD" in result
        assert "**Joins to:** `customers`" in result

    async def test_hidden_models_excluded(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="h"))
        await storage.save_model(SlayerModel(name="visible", sql_table="t", data_source="mydb"))
        await storage.save_model(SlayerModel(name="hidden_m", sql_table="t", data_source="mydb", hidden=True))
        result = await _call(mcp_server, name="models_summary", arguments={"datasource_name": "mydb"})
        assert "visible" in result
        assert "hidden_m" not in result

    async def test_joins_none_marker(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="h"))
        await storage.save_model(SlayerModel(name="solo", sql_table="t", data_source="mydb"))
        result = await _call(mcp_server, name="models_summary", arguments={"datasource_name": "mydb"})
        assert "**Joins to:** _(none)_" in result

    async def test_single_surviving_column_collapses_to_comma_list(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """When a model has no descriptions at all, the Dimensions table — which
        would otherwise be just the ``name`` column — auto-collapses into a
        comma-separated backticked list rather than a degenerate one-column
        table. Same applies to Measures."""
        await storage.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="h"))
        await storage.save_model(SlayerModel(
            name="m", sql_table="t", data_source="mydb",
            dimensions=[
                Dimension(name="x", type=DataType.STRING),
                Dimension(name="y", type=DataType.STRING),
            ],
        ))
        result = await _call(mcp_server, name="models_summary", arguments={"datasource_name": "mydb"})
        dim_section = result.split("**Dimensions")[1].split("**Measures")[0]
        assert "`x`, `y`" in dim_section
        # And no markdown-table scaffolding made it in:
        assert "| x |" not in dim_section
        assert "| --- |" not in dim_section


class TestInspectModel:
    async def test_not_found(self, mcp_server) -> None:
        result = await _call(mcp_server, name="inspect_model", arguments={"model_name": "nonexistent"})
        assert "not found" in result

    async def test_markdown_structure(self, mcp_server, storage: YAMLStorage) -> None:
        """Every expected section header appears and the response is no longer JSON."""
        await storage.save_model(SlayerModel(
            name="test",
            sql_table="public.t",
            data_source="test",
            description="A test model used in unit tests.",
            dimensions=[
                Dimension(name="status", type=DataType.STRING, label="Status", description="Order state"),
                Dimension(name="id", type=DataType.NUMBER, primary_key=True),
            ],
            measures=[Measure(name="revenue", sql="amount", label="Revenue", description="USD total")],
            filters=["deleted_at IS NULL"],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={"model_name": "test"})

        assert result.startswith("# Model: `test`")
        assert "A test model used in unit tests." in result
        assert "**data_source:** `test`" in result
        assert "**sql_table:** `public.t`" in result
        assert "## Filters (model-level)" in result
        assert "`deleted_at IS NULL`" in result
        assert "## Dimensions (2)" in result
        assert "| status |" in result
        assert "Order state" in result
        assert "## Measures (1)" in result
        assert "Revenue" in result
        assert "USD total" in result
        assert "## Joins (1)" in result
        assert "customers" in result
        assert "direct" in result

        # No longer JSON
        with pytest.raises(json.JSONDecodeError):
            json.loads(result)

    async def test_custom_sql_section_only_when_sql_is_set(self, mcp_server, storage: YAMLStorage) -> None:
        """## SQL fenced block appears only when model.sql is populated."""
        await storage.save_model(SlayerModel(name="plain", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="inspect_model", arguments={"model_name": "plain"})
        assert "## SQL" not in result

        await storage.save_model(SlayerModel(
            name="querybacked", sql="SELECT 1 AS x", data_source="test",
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={"model_name": "querybacked"})
        assert "## SQL" in result
        assert "```sql" in result
        assert "SELECT 1 AS x" in result

    async def test_measure_filter_and_aggregations(self, mcp_server, storage: YAMLStorage) -> None:
        """Measure.filter and custom Aggregation entries both surface."""
        await storage.save_model(SlayerModel(
            name="t", sql_table="t", data_source="test",
            measures=[Measure(
                name="completed_rev", sql="amount",
                filter="status = 'completed'",
                allowed_aggregations=["sum", "avg"],
            )],
            aggregations=[Aggregation(
                name="wavg",
                formula="SUM({sql} * {weight}) / NULLIF(SUM({weight}), 0)",
                description="Weighted average",
            )],
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={"model_name": "t"})
        assert "status = 'completed'" in result  # measure filter surfaces
        assert "sum, avg" in result              # allowed_aggregations rendered
        assert "## Aggregations (1)" in result
        assert "wavg" in result
        assert "Weighted average" in result

    async def test_joins_kind_labels(self, mcp_server, storage: YAMLStorage) -> None:
        """Joins table marks direct vs multi-hop joins."""
        await storage.save_model(SlayerModel(
            name="order_items", sql_table="order_items", data_source="test",
            joins=[
                ModelJoin(target_model="orders", join_pairs=[["order_id", "id"]]),
                ModelJoin(target_model="customers", join_pairs=[["orders.customer_id", "id"]]),
            ],
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={"model_name": "order_items"})
        assert "| orders |" in result
        assert "| customers |" in result
        # The "direct" label should appear on the orders row, "multi-hop" on the customers row.
        orders_line = next(line for line in result.splitlines() if "| orders |" in line)
        customers_line = next(line for line in result.splitlines() if "| customers |" in line)
        assert "direct" in orders_line
        assert "multi-hop" in customers_line


class TestBuildSampleQueryArgs:
    def test_avg_when_allowed(self) -> None:
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            dimensions=[
                Dimension(name="status"),
                Dimension(name="region"),
                Dimension(name="id", primary_key=True),
            ],
            measures=[
                Measure(name="rev", sql="amt"),
                Measure(name="qty", sql="quantity"),
            ],
        )
        args = _build_sample_query_args(model=model, num_rows=7)
        assert [f["formula"] for f in args["fields"]] == ["*:count", "rev:avg", "qty:avg"]
        assert [d["name"] for d in args["dimensions"]] == ["status", "region"]
        assert args["limit"] == 7
        assert args["source_model"] == "t"

    def test_fallback_to_first_allowed_when_avg_not_permitted(self) -> None:
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            measures=[Measure(name="rev", sql="amt", allowed_aggregations=["sum", "max"])],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        assert [f["formula"] for f in args["fields"]] == ["*:count", "rev:sum"]

    def test_prefers_safe_agg_over_first_allowed(self) -> None:
        """When the allowed list starts with a non-safe aggregation (e.g. first,
        last), _build_sample_query_args should skip it and pick the first safe
        zero-arg aggregation from the list."""
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            measures=[Measure(name="rev", sql="amt", allowed_aggregations=["last", "first", "min", "max"])],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        assert [f["formula"] for f in args["fields"]] == ["*:count", "rev:min"]

    def test_falls_back_to_first_allowed_when_no_safe_agg(self) -> None:
        """When the allowed list contains no safe aggregation, fall back to the
        first entry (even if it requires extra context like a time column)."""
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            measures=[Measure(name="rev", sql="amt", allowed_aggregations=["last", "first"])],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        assert [f["formula"] for f in args["fields"]] == ["*:count", "rev:last"]

    def test_skip_when_allowed_is_empty(self) -> None:
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            measures=[Measure(name="rev", sql="amt", allowed_aggregations=[])],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        assert [f["formula"] for f in args["fields"]] == ["*:count"]

    def test_dims_cap_at_two_and_exclude_pk_and_hidden(self) -> None:
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            dimensions=[
                Dimension(name="id", primary_key=True),
                Dimension(name="hidden_d", hidden=True),
                Dimension(name="a"),
                Dimension(name="b"),
                Dimension(name="c"),
            ],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        assert [d["name"] for d in args["dimensions"]] == ["a", "b"]

    def test_hidden_measure_skipped(self) -> None:
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            measures=[Measure(name="rev", sql="amt", hidden=True), Measure(name="qty", sql="quantity")],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        assert [f["formula"] for f in args["fields"]] == ["*:count", "qty:avg"]

    def test_count_distinct_fallback_for_non_numeric_same_named_dim(self) -> None:
        """Auto-ingestion generates a measure for every non-ID column — including
        string columns like `sku`. AVG(VARCHAR) is invalid SQL, so when a measure
        shares its name with a non-numeric dimension and avg is permitted, we
        fall back to count_distinct."""
        model = SlayerModel(
            name="order_items", sql_table="order_items", data_source="ds",
            dimensions=[
                Dimension(name="id", type=DataType.STRING, primary_key=True),
                Dimension(name="sku", type=DataType.STRING),
                Dimension(name="is_flagged", type=DataType.BOOLEAN),
                Dimension(name="quantity", type=DataType.NUMBER),
            ],
            measures=[
                Measure(name="sku", sql="sku"),                 # string — use count_distinct
                Measure(name="is_flagged", sql="is_flagged"),   # boolean — use count_distinct
                Measure(name="quantity", sql="quantity"),       # numeric — use avg
            ],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        assert [f["formula"] for f in args["fields"]] == [
            "*:count", "sku:count_distinct", "is_flagged:count_distinct", "quantity:avg",
        ]


class TestMarkdownHelpers:
    def test_escape_none_and_empty(self) -> None:
        assert _escape_md_cell(None) == "—"
        assert _escape_md_cell("") == "—"
        assert _escape_md_cell("   ") == "—"

    def test_escape_pipes_and_newlines(self) -> None:
        assert _escape_md_cell("a|b") == "a\\|b"
        assert _escape_md_cell("line1\nline2") == "line1 line2"
        assert _escape_md_cell("line1\r\nline2") == "line1 line2"

    def test_table_empty(self) -> None:
        assert _markdown_table(rows=[], columns=["x"]) == "_(none)_"

    def test_table_renders(self) -> None:
        out = _markdown_table(rows=[{"x": 1, "y": "hi"}, {"x": 2, "y": None}], columns=["x", "y"])
        assert out.splitlines() == [
            "| x | y |",
            "| --- | --- |",
            "| 1 | hi |",
            "| 2 | — |",
        ]

    def test_table_prunes_all_empty_columns(self) -> None:
        """A column where every row is None/empty is dropped from the output."""
        out = _markdown_table(
            rows=[
                {"name": "a", "label": None, "desc": "foo"},
                {"name": "b", "label": "", "desc": "bar"},
            ],
            columns=["name", "label", "desc"],
        )
        assert "label" not in out
        assert out.splitlines() == [
            "| name | desc |",
            "| --- | --- |",
            "| a | foo |",
            "| b | bar |",
        ]

    def test_table_single_column_collapses_to_comma_list(self) -> None:
        """After pruning, a lone remaining column renders as ``\\`a\\`, \\`b\\`
        ... — no table scaffolding."""
        out = _markdown_table(
            rows=[
                {"name": "x", "desc": None},
                {"name": "y", "desc": ""},
                {"name": "z", "desc": None},
            ],
            columns=["name", "desc"],
        )
        assert out == "`x`, `y`, `z`"

    def test_single_column_escapes_backticks(self) -> None:
        """Backticks inside values must be escaped when the single-column branch
        wraps them in backtick delimiters, otherwise malformed markdown results."""
        out = _markdown_table(
            rows=[
                {"name": "no`ticks", "desc": None},
                {"name": "plain", "desc": None},
            ],
            columns=["name", "desc"],
        )
        assert out == r"`no\`ticks`, `plain`"

    def test_table_all_columns_pruned_returns_none_marker(self) -> None:
        out = _markdown_table(
            rows=[{"a": None, "b": ""}, {"a": "", "b": None}],
            columns=["a", "b"],
        )
        assert out == "_(none)_"

    def test_strip_model_prefix(self) -> None:
        cols, data = _strip_model_prefix(
            columns=["orders.count", "orders.revenue_avg", "other.field"],
            data=[{"orders.count": 5, "orders.revenue_avg": 12.5, "other.field": "x"}],
            model_name="orders",
        )
        assert cols == ["count", "revenue_avg", "other.field"]
        assert data == [{"count": 5, "revenue_avg": 12.5, "other.field": "x"}]


class TestReachableFields:
    async def test_empty_when_no_joins(self, storage: YAMLStorage) -> None:
        model = SlayerModel(name="solo", sql_table="t", data_source="ds")
        await storage.save_model(model)
        dims, measures = await _collect_reachable_fields(model=model, storage=storage)
        assert dims == []
        assert measures == []

    async def test_direct_join_exposes_target_fields(self, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="customers", sql_table="customers", data_source="ds",
            dimensions=[
                Dimension(name="id", primary_key=True),
                Dimension(name="name"),
                Dimension(name="region"),
            ],
            measures=[Measure(name="lifetime_value", sql="ltv")],
        ))
        orders = SlayerModel(
            name="orders", sql_table="orders", data_source="ds",
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )
        await storage.save_model(orders)
        dims, measures = await _collect_reachable_fields(model=orders, storage=storage)
        assert dims == ["customers.name", "customers.region"]
        assert measures == ["customers.lifetime_value"]

    async def test_auto_ingested_multi_hop_path(self, storage: YAMLStorage) -> None:
        """A baked-in multi-hop join (source col 'orders.customer_id') should
        produce the path 'orders.customers.<field>' from the root."""
        await storage.save_model(SlayerModel(
            name="customers", sql_table="customers", data_source="ds",
            dimensions=[Dimension(name="id", primary_key=True), Dimension(name="name")],
        ))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="ds",
            dimensions=[Dimension(name="id", primary_key=True), Dimension(name="customer_id")],
        ))
        root = SlayerModel(
            name="order_items", sql_table="order_items", data_source="ds",
            joins=[
                ModelJoin(target_model="orders", join_pairs=[["order_id", "id"]]),
                ModelJoin(target_model="customers", join_pairs=[["orders.customer_id", "id"]]),
            ],
        )
        await storage.save_model(root)
        dims, _ = await _collect_reachable_fields(model=root, storage=storage)
        assert "orders.customer_id" in dims
        assert "orders.customers.name" in dims

    async def test_recursive_walk_via_targets_joins(self, storage: YAMLStorage) -> None:
        """Hand-built shallow joins (root -> A -> B): recursion reaches B via A."""
        await storage.save_model(SlayerModel(
            name="b", sql_table="b", data_source="ds",
            dimensions=[Dimension(name="id", primary_key=True), Dimension(name="code")],
        ))
        await storage.save_model(SlayerModel(
            name="a", sql_table="a", data_source="ds",
            dimensions=[Dimension(name="id", primary_key=True), Dimension(name="x")],
            joins=[ModelJoin(target_model="b", join_pairs=[["b_id", "id"]])],
        ))
        root = SlayerModel(
            name="root", sql_table="root", data_source="ds",
            joins=[ModelJoin(target_model="a", join_pairs=[["a_id", "id"]])],
        )
        await storage.save_model(root)
        dims, _ = await _collect_reachable_fields(model=root, storage=storage)
        assert "a.x" in dims
        assert "a.b.code" in dims

    async def test_depth_cap(self, storage: YAMLStorage) -> None:
        """Paths with more than max_depth segments are excluded."""
        # Build a chain root -> a -> b -> c -> d
        await storage.save_model(SlayerModel(
            name="d", sql_table="d", data_source="ds",
            dimensions=[Dimension(name="id", primary_key=True), Dimension(name="val")],
        ))
        await storage.save_model(SlayerModel(
            name="c", sql_table="c", data_source="ds",
            dimensions=[Dimension(name="id", primary_key=True), Dimension(name="val")],
            joins=[ModelJoin(target_model="d", join_pairs=[["d_id", "id"]])],
        ))
        await storage.save_model(SlayerModel(
            name="b", sql_table="b", data_source="ds",
            dimensions=[Dimension(name="id", primary_key=True), Dimension(name="val")],
            joins=[ModelJoin(target_model="c", join_pairs=[["c_id", "id"]])],
        ))
        await storage.save_model(SlayerModel(
            name="a", sql_table="a", data_source="ds",
            dimensions=[Dimension(name="id", primary_key=True), Dimension(name="val")],
            joins=[ModelJoin(target_model="b", join_pairs=[["b_id", "id"]])],
        ))
        root = SlayerModel(
            name="root", sql_table="root", data_source="ds",
            joins=[ModelJoin(target_model="a", join_pairs=[["a_id", "id"]])],
        )
        await storage.save_model(root)
        dims, _ = await _collect_reachable_fields(model=root, storage=storage, max_depth=2)
        # max_depth caps the model-path length (segments), so max_depth=2 admits
        # paths "a" (1 segment → field `a.val`) and "a.b" (2 segments → field
        # `a.b.val`). Paths of 3+ segments ("a.b.c" etc.) are excluded.
        assert "a.val" in dims
        assert "a.b.val" in dims
        assert not any(d.startswith("a.b.c.") for d in dims)

    async def test_cycles_dont_infinite_loop(self, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="b", sql_table="b", data_source="ds",
            dimensions=[Dimension(name="id", primary_key=True), Dimension(name="val")],
            joins=[ModelJoin(target_model="a", join_pairs=[["a_id", "id"]])],
        ))
        a = SlayerModel(
            name="a", sql_table="a", data_source="ds",
            dimensions=[Dimension(name="id", primary_key=True), Dimension(name="val")],
            joins=[ModelJoin(target_model="b", join_pairs=[["b_id", "id"]])],
        )
        await storage.save_model(a)
        dims, _ = await _collect_reachable_fields(model=a, storage=storage)
        # Should complete without hanging; a.b.val and a.b.a.val are distinct paths
        assert "b.val" in dims


class TestCreateModel:
    async def test_create(self, mcp_server, storage: YAMLStorage) -> None:
        result = await _call(mcp_server, name="create_model", arguments={
            "name": "orders",
            "sql_table": "public.orders",
            "data_source": "test_ds",
            "dimensions": [
                {"name": "id", "sql": "id", "type": "number", "primary_key": "true"},
                {"name": "status", "sql": "status", "type": "string"},
            ],
            "measures": [
                {"name": "revenue", "sql": "amount"},
            ],
        })
        assert "orders" in result
        assert "created" in result
        assert await storage.get_model("orders") is not None

    async def test_create_with_allowed_aggregations(self, mcp_server, storage: YAMLStorage) -> None:
        result = await _call(mcp_server, name="create_model", arguments={
            "name": "orders",
            "sql_table": "public.orders",
            "data_source": "test_ds",
            "measures": [
                {"name": "revenue", "sql": "amount", "allowed_aggregations": ["sum", "avg"]},
            ],
        })
        assert "created" in result
        model = await storage.get_model("orders")
        assert model.measures[0].allowed_aggregations == ["sum", "avg"]

    async def test_create_reports_replaced(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="create_model", arguments={"name": "orders", "sql_table": "t2", "data_source": "test"})
        assert "replaced" in result

    async def test_create_from_query_rejects_mixed_params(self, mcp_server) -> None:
        result = await _call(mcp_server, name="create_model", arguments={
            "name": "bad",
            "query": {"source_model": "orders", "fields": ["*:count"]},
            "sql_table": "public.orders",
        })
        assert "Error" in result
        assert "query" in result
        assert "sql_table" in result

    async def test_create_from_query_rejects_data_source(self, mcp_server) -> None:
        result = await _call(mcp_server, name="create_model", arguments={
            "name": "bad",
            "query": {"source_model": "orders", "fields": ["*:count"]},
            "data_source": "mydb",
        })
        assert "Error" in result
        assert "data_source" in result

    async def test_create_from_query_ignores_empty_placeholders(self, mcp_server, storage: YAMLStorage) -> None:
        """Empty lists/strings should not trigger the mixed-parameter error."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="test_ds",
            measures=[Measure(name="amount", sql="amount")],
        ))
        result = await _call(mcp_server, name="create_model", arguments={
            "name": "summary",
            "query": {"source_model": "orders", "fields": ["amount:sum"]},
            "dimensions": [],
            "measures": [],
        })
        # Should route to query path (fails on missing datasource), not mixed-param error
        assert "Error:" not in result or "Datasource" in result

    async def test_create_from_query_routes_to_engine(self, mcp_server, storage: YAMLStorage) -> None:
        # Without a real datasource/data, the engine will return a friendly error —
        # but the error message proves we routed to the query path.
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="test_ds",
            measures=[Measure(name="amount", sql="amount")],
        ))
        result = await _call(mcp_server, name="create_model", arguments={
            "name": "summary",
            "query": {"source_model": "orders", "fields": ["amount:sum"]},
        })
        # Should fail on missing datasource, not on "missing sql_table"
        assert "Datasource" in result


class TestEditModel:
    """Tests for the edit_model MCP tool with upsert semantics."""

    # --- Measure upserts ---

    async def test_upsert_new_measure(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount")],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "measures": [{"name": "total", "sql": "amount"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("created measure 'total'" in c for c in parsed["changes"])
        model = await storage.get_model("orders")
        assert len(model.measures) == 2

    async def test_upsert_measure_with_allowed_aggregations(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount")],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "measures": [{"name": "total", "sql": "amount", "allowed_aggregations": ["sum", "avg"]}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_model("orders")
        total = next(m for m in model.measures if m.name == "total")
        assert total.allowed_aggregations == ["sum", "avg"]

    async def test_upsert_existing_measure(self, mcp_server, storage: YAMLStorage) -> None:
        """Upserting an existing measure updates it instead of erroring."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount", description="old")],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "measures": [{"name": "revenue", "sql": "price"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("updated measure 'revenue'" in c for c in parsed["changes"])
        model = await storage.get_model("orders")
        assert len(model.measures) == 1
        assert model.measures[0].sql == "price"

    async def test_upsert_existing_measure_partial_update(self, mcp_server, storage: YAMLStorage) -> None:
        """Partial upsert: only specified fields change, others are preserved."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount", description="Total revenue")],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "measures": [{"name": "revenue", "description": "Updated description"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        m = (await storage.get_model("orders")).measures[0]
        assert m.description == "Updated description"
        assert m.sql == "amount"  # unchanged

    # --- Dimension upserts ---

    async def test_upsert_new_dimension(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "dimensions": [{"name": "region", "sql": "region", "type": "string"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("created dimension 'region'" in c for c in parsed["changes"])
        assert any(d.name == "region" for d in (await storage.get_model("orders")).dimensions)

    async def test_upsert_existing_dimension_partial_update(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            dimensions=[Dimension(name="status", sql="status", type=DataType.STRING)],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "dimensions": [{"name": "status", "description": "Order status"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        d = (await storage.get_model("orders")).dimensions[0]
        assert d.description == "Order status"
        assert d.sql == "status"  # unchanged
        assert d.type == DataType.STRING  # unchanged

    async def test_upsert_multiple_mixed_create_update(self, mcp_server, storage: YAMLStorage) -> None:
        """One new + one existing entity in the same call."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount")],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "measures": [
                {"name": "revenue", "description": "Updated"},
                {"name": "profit", "sql": "revenue - cost"},
            ],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("updated measure 'revenue'" in c for c in parsed["changes"])
        assert any("created measure 'profit'" in c for c in parsed["changes"])
        model = await storage.get_model("orders")
        assert len(model.measures) == 2

    async def test_invalid_dimension_type_on_upsert(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "dimensions": [{"name": "bad", "type": "invalid_type"}],
        })
        assert "Invalid" in result

    # --- Aggregation upserts ---

    async def test_upsert_new_aggregation(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "aggregations": [{"name": "my_agg", "formula": "SUM({value})"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_model("orders")
        assert len(model.aggregations) == 1
        assert model.aggregations[0].name == "my_agg"

    async def test_upsert_existing_aggregation(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            aggregations=[Aggregation(name="my_agg", formula="SUM({value})")],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "aggregations": [{"name": "my_agg", "formula": "AVG({value})"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_model("orders")
        assert model.aggregations[0].formula == "AVG({value})"

    async def test_remove_aggregation(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            aggregations=[Aggregation(name="my_agg", formula="SUM({value})")],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "remove": {"aggregations": ["my_agg"]},
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert len((await storage.get_model("orders")).aggregations) == 0

    # --- Join upserts ---

    async def test_upsert_new_join(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "joins": [{"target_model": "customers", "join_pairs": [["customer_id", "id"]]}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_model("orders")
        assert len(model.joins) == 1
        assert model.joins[0].target_model == "customers"

    async def test_upsert_existing_join(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "joins": [{"target_model": "customers", "join_pairs": [["buyer_id", "id"]]}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_model("orders")
        assert len(model.joins) == 1
        assert model.joins[0].join_pairs == [["buyer_id", "id"]]

    async def test_remove_join(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "remove": {"joins": ["customers"]},
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert len((await storage.get_model("orders")).joins) == 0

    # --- Filter management ---

    async def test_add_filter(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "add_filters": ["deleted_at IS NULL"],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert "deleted_at IS NULL" in (await storage.get_model("orders")).filters

    async def test_add_duplicate_filter_skipped(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            filters=["deleted_at IS NULL"],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "add_filters": ["deleted_at IS NULL"],
        })
        # No changes because the filter already exists
        assert "No changes" in result

    async def test_remove_filter(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            filters=["deleted_at IS NULL"],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "remove_filters": ["deleted_at IS NULL"],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert len((await storage.get_model("orders")).filters) == 0

    async def test_remove_filter_not_found(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "remove_filters": ["nonexistent"],
        })
        assert "Filter not found" in result

    # --- Scalar metadata ---

    async def test_update_description(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "description": "Updated",
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert (await storage.get_model("orders")).description == "Updated"

    async def test_set_sql_table(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "sql_table": "public.orders",
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert (await storage.get_model("orders")).sql_table == "public.orders"

    async def test_set_hidden(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "hidden": True,
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert (await storage.get_model("orders")).hidden is True

    # --- Multiple changes ---

    async def test_multiple_changes(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount")],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "description": "Orders table",
            "measures": [{"name": "total", "sql": "amount"}],
            "dimensions": [{"name": "status", "sql": "status", "type": "string"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert len(parsed["changes"]) == 3
        model = await storage.get_model("orders")
        assert model.description == "Orders table"
        assert len(model.measures) == 2
        assert any(d.name == "status" for d in model.dimensions)

    # --- Typed remove ---

    async def test_remove_measure_typed(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            measures=[Measure(name="revenue", sql="amount"), Measure(name="total", sql="x")],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "remove": {"measures": ["total"]},
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_model("orders")
        assert len(model.measures) == 1

    async def test_remove_dimension_not_found(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "remove": {"dimensions": ["nonexistent"]},
        })
        assert "not found" in result

    async def test_remove_invalid_key(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "remove": {"invalid": ["x"]},
        })
        assert "Invalid remove key" in result

    async def test_remove_then_recreate_same_call(self, mcp_server, storage: YAMLStorage) -> None:
        """Remove a dimension then upsert one with the same name in the same call."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            dimensions=[Dimension(name="status", sql="old_col", type=DataType.STRING)],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "remove": {"dimensions": ["status"]},
            "dimensions": [{"name": "status", "sql": "new_col", "type": "string"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        d = (await storage.get_model("orders")).dimensions[0]
        assert d.sql == "new_col"

    # --- Error cases ---

    async def test_model_not_found(self, mcp_server) -> None:
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "nope",
            "description": "test",
        })
        assert "not found" in result

    async def test_no_changes(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={"model_name": "orders"})
        assert "No changes" in result

    async def test_cross_field_validation_error(self, mcp_server, storage: YAMLStorage) -> None:
        """allowed_aggregations referencing a non-existent aggregation should fail."""
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "measures": [{"name": "rev", "sql": "amount", "allowed_aggregations": ["nonexistent_agg"]}],
        })
        assert "Validation error" in result or "not a built-in aggregation" in result


class TestDatasources:
    async def test_list_empty(self, mcp_server) -> None:
        result = await _call(mcp_server, name="list_datasources")
        assert "No datasources configured" in result
        assert "create_datasource" in result

    async def test_create_and_list(self, mcp_server, storage: YAMLStorage) -> None:
        result = await _call(mcp_server, name="create_datasource", arguments={
            "name": "mydb",
            "type": "postgres",
            "host": "localhost",
        })
        assert "mydb" in result
        assert await storage.get_datasource("mydb") is not None

        result = await _call(mcp_server, name="list_datasources")
        assert "mydb" in result
        assert "postgres" in result

    async def test_create_reports_connection_failure(self, mcp_server) -> None:
        result = await _call(mcp_server, name="create_datasource", arguments={
            "name": "bad",
            "type": "postgres",
            "host": "localhost",
            "port": 59999,
            "database": "nonexistent",
        })
        assert "created" in result
        assert "connection test failed" in result.lower()

    async def test_create_reports_replaced(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
        result = await _call(mcp_server, name="create_datasource", arguments={"name": "ds", "type": "sqlite", "database": ":memory:"})
        assert "replaced" in result

    async def test_list_with_malformed_datasource(self, mcp_server, storage: YAMLStorage) -> None:
        # A valid datasource alongside a malformed one
        await storage.save_datasource(DatasourceConfig(name="good", type="sqlite", database=":memory:"))
        path = os.path.join(storage.datasources_dir, "bad.yaml")
        with open(path, "w") as f:
            f.write("name: bad\ntype: [unclosed\n")
        result = await _call(mcp_server, name="list_datasources")
        assert "good (sqlite)" in result
        assert "bad" in result
        assert "ERROR" in result

    async def test_models_summary_with_malformed_datasource(self, mcp_server, storage: YAMLStorage) -> None:
        """Asking for a datasource whose YAML config is broken surfaces the
        invalid-config error rather than raising."""
        path = os.path.join(storage.datasources_dir, "bad.yaml")
        with open(path, "w") as f:
            f.write("name: bad\ntype: [unclosed\n")
        result = await _call(mcp_server, name="models_summary", arguments={"datasource_name": "bad"})
        assert "invalid" in result.lower()

    async def test_describe_malformed_datasource(self, mcp_server, storage: YAMLStorage) -> None:
        path = os.path.join(storage.datasources_dir, "bad.yaml")
        with open(path, "w") as f:
            f.write("name: bad\ntype: [unclosed\n")
        result = await _call(mcp_server, name="describe_datasource", arguments={"name": "bad"})
        assert "invalid" in result.lower()

    async def test_describe_not_found(self, mcp_server) -> None:
        result = await _call(mcp_server, name="describe_datasource", arguments={"name": "nope"})
        assert "not found" in result

    async def test_describe_shows_details(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(
            name="testds", type="postgres", host="localhost", port=5432, database="testdb", username="user",
        ))
        result = await _call(mcp_server, name="describe_datasource", arguments={"name": "testds"})
        assert "Datasource: testds" in result
        assert "Type: postgres" in result
        assert "Host: localhost" in result
        assert "Database: testdb" in result
        assert "Connection:" in result


class TestDeleteTools:
    async def test_delete_model(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="delete_model", arguments={"name": "orders"})
        assert "deleted" in result
        assert await storage.get_model("orders") is None

    async def test_delete_model_not_found(self, mcp_server) -> None:
        result = await _call(mcp_server, name="delete_model", arguments={"name": "nope"})
        assert "not found" in result

    async def test_delete_datasource(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
        result = await _call(mcp_server, name="delete_datasource", arguments={"name": "ds"})
        assert "deleted" in result
        assert await storage.get_datasource("ds") is None

    async def test_delete_datasource_not_found(self, mcp_server) -> None:
        result = await _call(mcp_server, name="delete_datasource", arguments={"name": "nope"})
        assert "not found" in result



class TestIngestionIdSkipping:
    def test_id_columns_skip_sum_avg(self) -> None:
        from slayer.engine.ingestion import _is_id_column
        assert _is_id_column("id") is True
        assert _is_id_column("user_id") is True
        assert _is_id_column("customer_key") is True
        assert _is_id_column("role_fk") is True
        assert _is_id_column("primary_pk") is True
        assert _is_id_column("amount") is False
        assert _is_id_column("quantity") is False
        assert _is_id_column("price") is False
        assert _is_id_column("width") is False


class TestFriendlyErrors:
    def test_password_error(self) -> None:
        msg = _friendly_db_error(Exception("password authentication failed for user"))
        assert "Database error:" in msg
        assert "Check that username and password" in msg

    def test_database_not_found(self) -> None:
        msg = _friendly_db_error(Exception('database "foo" does not exist'))
        assert "Verify the database name" in msg

    def test_connection_refused(self) -> None:
        msg = _friendly_db_error(Exception("connection refused"))
        assert "Check that the database server is running" in msg

    def test_unknown_error(self) -> None:
        msg = _friendly_db_error(Exception("something weird"))
        assert "Database error:" in msg
        assert "Hint:" not in msg


class TestFormatTable:
    def test_empty(self) -> None:
        assert _format_table(data=[], columns=[]) == "No results."

    def test_basic(self) -> None:
        data = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
        result = _format_table(data=data, columns=["a", "b"])
        assert "a | b" in result
        assert "1 | x" in result
        assert "2 | y" in result

    def test_truncation(self) -> None:
        data = [{"x": i} for i in range(100)]
        result = _format_table(data=data, columns=["x"], max_rows=10)
        assert "100 total rows" in result
        assert "showing first 10" in result


class TestHelp:
    async def test_no_arg_returns_intro(self, mcp_server) -> None:
        result = await _call(mcp_server, name="help")
        assert "SLayer" in result
        # At least one of the landing-page invariants should be present
        assert any(
            phrase in result
            for phrase in (
                "Measures are not aggregates",
                "Joined data is reached",
                "Filters on measures",
            )
        )

    async def test_valid_topic_returns_body(self, mcp_server) -> None:
        result = await _call(mcp_server, name="help", arguments={"topic": "transforms"})
        # The transforms topic should mention the key transform names
        assert "cumsum" in result
        assert "change" in result
        assert "time_shift" in result

    async def test_invalid_topic_returns_friendly_error(self, mcp_server) -> None:
        result = await _call(mcp_server, name="help", arguments={"topic": "bogus"})
        assert "Unknown help topic" in result
        assert "bogus" in result
        # The error should list every valid topic
        for name in ("queries", "formulas", "transforms", "workflow"):
            assert name in result

    async def test_tool_description_carries_topic_list(self, mcp_server) -> None:
        tools = await mcp_server.list_tools()
        help_tool = next(t for t in tools if t.name == "help")
        assert help_tool.description is not None
        assert "Available help topics:" in help_tool.description
        for name in ("queries", "formulas", "transforms", "workflow"):
            assert name in help_tool.description
