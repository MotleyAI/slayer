"""Tests for the MCP server tools."""

import json
import os
import shutil
import tempfile
from typing import Any
from collections.abc import Generator

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.mcp.server import (
    _build_sample_query_args,
    _escape_md_cell,
    _format_table,
    _friendly_db_error,
    _markdown_table,
    _md_code_span,
    _strip_model_prefix,
    create_mcp_server,
)
from slayer.storage.yaml_storage import YAMLStorage


# `create_mcp_server` is expensive (~35 ms each, dominated by FastMCP tool
# registration / pydantic schema gen). With ~160 tests in this file that
# was ~5–6s of pure fixture overhead per run. The MCP server captures the
# storage instance at construction time, so we share *both* across the
# session and reset the underlying YAML files in a per-test fixture.
@pytest.fixture(scope="session")
def _shared_storage() -> Generator[YAMLStorage, None, None]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


@pytest.fixture(scope="session")
def _shared_mcp_server(_shared_storage: YAMLStorage):
    return create_mcp_server(storage=_shared_storage)


def _reset_yaml_storage(storage: YAMLStorage) -> None:
    """Wipe model + datasource files between tests so the session-scoped
    storage looks fresh to every test. v4 nests models under
    ``models/<data_source>/`` so we recurse rather than just unlinking
    top-level entries. Also clears the ``priority.yaml`` written by
    ``set_datasource_priority`` — without this, priority leaks between
    session-scoped tests and any ambiguity-related test would be
    order-dependent (PR #92 thread #13).
    """
    for sub in ("models", "datasources"):
        d = os.path.join(storage.base_dir, sub)
        if os.path.isdir(d):
            for entry in os.listdir(d):
                path = os.path.join(d, entry)
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
    priority_path = os.path.join(storage.base_dir, "priority.yaml")
    if os.path.exists(priority_path):
        os.remove(priority_path)


@pytest.fixture
def storage(_shared_storage: YAMLStorage) -> YAMLStorage:
    _reset_yaml_storage(_shared_storage)
    return _shared_storage


@pytest.fixture
def mcp_server(_shared_mcp_server, storage: YAMLStorage):
    # Depending on `storage` ensures the per-test reset runs before any
    # test exercises the MCP server. `mcp_server` itself is the same
    # session-scoped instance every call.
    return _shared_mcp_server


async def _call(mcp_server, *, name: str, arguments: dict[str, Any] | None = None) -> str:
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
            columns=[Column(name="status", type=DataType.TEXT, description="Order state"),
Column(name="revenue", sql="amount", description="USD", type=DataType.DOUBLE)
            ],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        ))
        # DEV-1549: pin the verbose markdown shape; compact-by-default
        # drops the per-column table this test was written for.
        result = await _call(mcp_server, name="models_summary", arguments={
            "datasource_name": "mydb", "compact": False,
        })
        assert result.startswith("# Datasource: `mydb` — 1 model(s)")
        assert "## `orders`" in result
        assert "Orders fact table." in result
        assert "**Columns (2):**" in result
        assert "| status |" in result
        assert "Order state" in result
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
        # DEV-1549: verbose markdown contract.
        result = await _call(mcp_server, name="models_summary", arguments={
            "datasource_name": "mydb", "compact": False,
        })
        assert "**Joins to:** _(none)_" in result

    async def test_columns_table_includes_type(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """v2 columns table always shows the type column, even without descriptions."""
        await storage.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="h"))
        await storage.save_model(SlayerModel(
            name="m", sql_table="t", data_source="mydb",
            columns=[
                Column(name="x", type=DataType.TEXT),
                Column(name="y", type=DataType.DOUBLE),
            ],
        ))
        # DEV-1549: verbose markdown contract.
        result = await _call(mcp_server, name="models_summary", arguments={
            "datasource_name": "mydb", "compact": False,
        })
        col_section = result.split("**Columns")[1].split("**Measures")[0]
        assert "| x | TEXT |" in col_section
        assert "| y | DOUBLE |" in col_section


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
            columns=[
                Column(name="status", type=DataType.TEXT, label="Status", description="Order state"),
                Column(name="id", type=DataType.DOUBLE, primary_key=True),
Column(name="revenue", sql="amount", label="Revenue", description="USD total", type=DataType.DOUBLE)
            ],
            filters=["deleted_at IS NULL"],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={"model_name": "test", "show_sql": True})

        assert result.startswith("# Model: `test`")
        assert "A test model used in unit tests." in result
        assert "**data_source:** `test`" in result
        assert "**sql_table:** `public.t`" in result
        assert "## Filters (model-level)" in result
        assert "`deleted_at IS NULL`" in result
        assert "## Columns (3)" in result
        assert "| status |" in result
        assert "Order state" in result
        assert "Revenue" in result
        assert "USD total" in result
        assert "## Joins (1)" in result
        assert "customers" in result
        assert "customer_id = id" in result

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
        result = await _call(mcp_server, name="inspect_model", arguments={"model_name": "querybacked", "show_sql": True})
        assert "## SQL" in result
        assert "```sql" in result
        assert "SELECT 1 AS x" in result

    async def test_measure_filter_and_aggregations(self, mcp_server, storage: YAMLStorage) -> None:
        """Measure.filter and custom Aggregation entries both surface."""
        await storage.save_model(SlayerModel(
            name="t", sql_table="t", data_source="test",
            columns=[Column(
                name="completed_rev", sql="amount",
                filter="status = 'completed'",
                allowed_aggregations=["sum", "avg"], type=DataType.DOUBLE)],
            aggregations=[Aggregation(
                name="wavg",
                formula="SUM({sql} * {weight}) / NULLIF(SUM({weight}), 0)",
                description="Weighted average",
            )],
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={"model_name": "t", "show_sql": True})
        assert "status = 'completed'" in result  # measure filter surfaces
        assert "sum, avg" in result              # allowed_aggregations rendered
        assert "## Aggregations (1)" in result
        assert "wavg" in result
        assert "Weighted average" in result

    async def test_joins_table_rendered(self, mcp_server, storage: YAMLStorage) -> None:
        """Joins table renders direct joins without kind labels."""
        await storage.save_model(SlayerModel(
            name="order_items", sql_table="order_items", data_source="test",
            joins=[
                ModelJoin(target_model="orders", join_pairs=[["order_id", "id"]]),
                ModelJoin(target_model="products", join_pairs=[["product_id", "id"]]),
            ],
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={"model_name": "order_items"})
        assert "| orders |" in result
        assert "| products |" in result
        assert "kind" not in result

    # --- meta rendering (DEV-1332) ---

    async def test_inspect_renders_meta_on_columns(self, mcp_server, storage: YAMLStorage) -> None:
        """Column.meta surfaces in both markdown and JSON inspect_model output.

        Pins the user-visible bug from DEV-1332: the storage layer round-trips
        meta correctly, but inspect_model never rendered it, so agents couldn't
        verify their bookkeeping was persisted.
        """
        await storage.save_model(SlayerModel(
            name="m", sql_table="t", data_source="test",
            columns=[Column(name="amount", type=DataType.DOUBLE, meta={"kb_id": 7})],
        ))
        # Markdown
        md = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "m", "sections": ["columns"],
        })
        assert "kb_id" in md
        assert "7" in md
        # JSON
        js = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "m", "sections": ["columns"], "format": "json",
        })
        payload = json.loads(js)
        assert payload["columns"][0]["meta"] == {"kb_id": 7}

    async def test_inspect_renders_meta_on_measures(self, mcp_server, storage: YAMLStorage) -> None:
        """ModelMeasure.meta surfaces in both markdown and JSON inspect_model output."""
        await storage.save_model(SlayerModel(
            name="m", sql_table="t", data_source="test",
            columns=[Column(name="revenue", type=DataType.DOUBLE)],
            measures=[ModelMeasure(
                name="aov", formula="revenue:sum / *:count",
                meta={"kb_id": "abc-123"},
            )],
        ))
        md = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "m", "sections": ["measures"],
        })
        assert "kb_id" in md
        assert "abc-123" in md
        js = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "m", "sections": ["measures"], "format": "json",
        })
        payload = json.loads(js)
        assert payload["measures"][0]["meta"] == {"kb_id": "abc-123"}

    async def test_inspect_renders_meta_on_aggregations(self, mcp_server, storage: YAMLStorage) -> None:
        """Aggregation.meta surfaces in both markdown and JSON inspect_model output."""
        await storage.save_model(SlayerModel(
            name="m", sql_table="t", data_source="test",
            aggregations=[Aggregation(
                name="trimmed_mean",
                formula="AVG(CASE WHEN {expr} BETWEEN {low} AND {high} THEN {expr} END)",
                meta={"owner": "analytics"},
            )],
        ))
        md = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "m", "sections": ["aggregations"], "show_sql": True,
        })
        assert "owner" in md
        assert "analytics" in md
        js = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "m", "sections": ["aggregations"], "format": "json",
        })
        payload = json.loads(js)
        assert payload["aggregations"][0]["meta"] == {"owner": "analytics"}

    async def test_inspect_renders_meta_on_model_header(self, mcp_server, storage: YAMLStorage) -> None:
        """SlayerModel.meta surfaces in the markdown header bullets and at the
        top level of the JSON payload."""
        await storage.save_model(SlayerModel(
            name="m", sql_table="t", data_source="test",
            meta={"source": "CRM"},
        ))
        md = await _call(mcp_server, name="inspect_model", arguments={"model_name": "m"})
        assert "**meta:**" in md
        assert "source" in md
        assert "CRM" in md
        js = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "m", "format": "json",
        })
        payload = json.loads(js)
        assert payload["meta"] == {"source": "CRM"}

    async def test_inspect_omits_meta_column_when_no_entity_has_meta(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """When no column/measure/aggregation has meta set, the meta column is
        pruned from the markdown table — keeps existing output unchanged for
        users who don't use meta. Relies on _markdown_table's all-empty-column
        pruning.
        """
        await storage.save_model(SlayerModel(
            name="m", sql_table="t", data_source="test",
            columns=[Column(name="amount", type=DataType.DOUBLE)],
            measures=[ModelMeasure(name="aov", formula="amount:sum")],
            aggregations=[Aggregation(name="my_agg", formula="SUM({expr})")],
        ))
        md = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "m", "sections": ["columns", "measures", "aggregations"],
            "show_sql": True,
        })
        # No meta column header should be emitted in any of the three tables.
        assert "| meta |" not in md
        assert "**meta:**" not in md  # also no model-header meta bullet


class TestMdCodeSpan:
    def test_plain_value(self) -> None:
        assert _md_code_span("hello") == "`hello`"

    def test_value_with_single_backtick(self) -> None:
        result = _md_code_span("no`ticks")
        assert result == "``no`ticks``"

    def test_value_with_double_backticks(self) -> None:
        result = _md_code_span("a``b")
        assert result == "```a``b```"

    def test_value_starting_with_backtick(self) -> None:
        result = _md_code_span("`start")
        assert result == "`` `start ``"

    def test_value_ending_with_backtick(self) -> None:
        result = _md_code_span("end`")
        assert result == "`` end` ``"

    def test_pipe_is_escaped(self) -> None:
        assert _md_code_span("a|b") == r"`a\|b`"

    def test_newlines_collapsed(self) -> None:
        assert _md_code_span("a\nb\rc\r\nd") == "`a b c d`"

    def test_empty_string(self) -> None:
        assert _md_code_span("") == "` `"

    def test_non_string_value(self) -> None:
        assert _md_code_span(42) == "`42`"


    async def test_measure_type_column_in_schema(self, mcp_server, storage: YAMLStorage) -> None:
        """Measure type column is included when type inference succeeds.

        Without a real DB, get_column_types returns {} and the type column
        is auto-pruned. This test verifies the column appears in the schema
        by checking _build_sample_query_args uses inferred types.
        """
        from slayer.mcp.server import _build_sample_query_args

        model = SlayerModel(
            name="typed",
            sql_table="t",
            data_source="test",
            columns=[Column(name="status", type=DataType.TEXT),

                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="label", sql="label", type=DataType.DOUBLE),
            ],
        )
        # Without types: both get avg (label has no matching dim to trigger heuristic)
        args_no_types = _build_sample_query_args(model=model, num_rows=3)
        formulas = [f["formula"] for f in args_no_types["measures"]]
        assert "label:avg" in formulas

        # With inferred types: label is string → count_distinct
        args_with_types = _build_sample_query_args(
            model=model, num_rows=3, measure_types={"amount": "number", "label": "string"},
        )
        formulas = [f["formula"] for f in args_with_types["measures"]]
        assert "amount:avg" in formulas
        assert "label:count_distinct" in formulas


class TestInspectModelQueryBacked:
    """inspect_model output for query-backed models — backing_query section,
    source_type, and (with show_sql) backing_query_sql.
    """

    async def _setup(self, storage: YAMLStorage) -> None:
        from slayer.core.models import DatasourceConfig
        from slayer.core.query import SlayerQuery
        from slayer.engine.query_engine import SlayerQueryEngine
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_model(SlayerModel(
            name="upstream", sql_table="t", data_source="test",
            columns=[
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="region", sql="region", type=DataType.TEXT),
            ],
        ))
        # Route the query-backed model through engine.save_model so the cache
        # (columns + backing_query_sql) is populated at save time. Read paths
        # never write to storage (issue #74), so cache must be warmed here.
        engine = SlayerQueryEngine(storage=storage)
        await engine.save_model(SlayerModel(
            name="qb",
            data_source="test",
            source_queries=[SlayerQuery(
                source_model="upstream",
                measures=[{"formula": "amount:sum"}],
                dimensions=["region"],
                filters=["amount > {threshold}"],
                dry_run=True,
            )],
            query_variables={"threshold": 100},
        ))

    async def test_json_includes_backing_query(self, mcp_server, storage: YAMLStorage) -> None:
        await self._setup(storage)
        result = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "qb", "format": "json",
        })
        parsed = json.loads(result)
        assert parsed["source_type"] == "query"
        assert "backing_query" in parsed
        bq = parsed["backing_query"]
        assert bq["variables"] == {"threshold": 100}
        assert bq["required_variables"] == []  # threshold has a default
        assert len(bq["stages"]) == 1
        # backing_query_sql is gated by show_sql
        assert "backing_query_sql" not in parsed

    async def test_json_show_sql_includes_backing_query_sql(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        """After save populates the cache, ``backing_query_sql`` is included
        when ``show_sql=True``.
        """
        await self._setup(storage)  # populates cache at save time
        result = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "qb", "format": "json", "show_sql": True,
        })
        parsed = json.loads(result)
        assert "backing_query_sql" in parsed
        assert parsed["backing_query_sql"]
        assert "amount" in parsed["backing_query_sql"].lower()

    async def test_required_variables_reported(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        from slayer.core.models import DatasourceConfig
        from slayer.core.query import SlayerQuery
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_model(SlayerModel(
            name="upstream", sql_table="t", data_source="test",
            columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
        ))
        await storage.save_model(SlayerModel(
            name="qb_missing_default",
            data_source="test",
            source_queries=[SlayerQuery(
                source_model="upstream",
                measures=[{"formula": "amount:sum"}],
                filters=["amount > {threshold}"],
                dry_run=True,
            )],
            # No query_variables → 'threshold' is required
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "qb_missing_default", "format": "json",
        })
        parsed = json.loads(result)
        assert "threshold" in parsed["backing_query"]["required_variables"]

    async def test_table_backed_model_has_no_backing_query(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await storage.save_model(SlayerModel(
            name="plain", sql_table="t", data_source="test",
            columns=[Column(name="x", sql="x", type=DataType.TEXT)],
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "plain", "format": "json",
        })
        parsed = json.loads(result)
        assert parsed["source_type"] == "table"
        assert "backing_query" not in parsed

    async def test_markdown_includes_backing_query_section(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await self._setup(storage)
        result = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "qb", "format": "markdown",
        })
        assert "## Backing Query" in result
        assert "threshold" in result


class TestInspectModelJsonFormat:
    async def test_json_format_includes_sample_data(self, mcp_server, storage: YAMLStorage) -> None:
        """inspect_model(format='json') must include sample_data and sample_data_error keys."""
        await storage.save_model(SlayerModel(
            name="jtest",
            sql_table="t",
            data_source="test",
            columns=[Column(name="x"),
Column(name="m", sql="val", type=DataType.DOUBLE)
            ],
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "jtest", "format": "json",
        })
        parsed = json.loads(result)
        assert "sample_data" in parsed
        assert "sample_data_error" in parsed
        assert parsed["model_name"] == "jtest"
        # sample_sql should NOT appear when show_sql is not requested
        assert "sample_sql" not in parsed


class TestInspectModelShowSQL:
    """show_sql parameter must control visibility of all SQL in inspect_model output."""

    async def test_hides_sql_by_default_markdown(self, mcp_server, storage: YAMLStorage) -> None:
        """Without show_sql, markdown output has no ## SQL section and no sql column."""
        await storage.save_model(SlayerModel(
            name="sqlt",
            sql="SELECT id, val FROM raw_table",
            data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
Column(name="val", sql="val", type=DataType.DOUBLE)
            ],
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "sqlt", "num_rows": 0,
        })
        assert "## SQL" not in result
        # Dimension table should not have an "sql" column header
        assert "| sql " not in result and "| sql|" not in result

    async def test_shows_sql_when_requested_markdown(self, mcp_server, storage: YAMLStorage) -> None:
        """With show_sql=True, markdown output includes ## SQL section and sql columns."""
        await storage.save_model(SlayerModel(
            name="sqlshow",
            sql="SELECT id, val FROM raw_table",
            data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
Column(name="val", sql="val", type=DataType.DOUBLE)
            ],
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "sqlshow", "num_rows": 0, "show_sql": True,
        })
        assert "## SQL" in result
        assert "SELECT id, val FROM raw_table" in result

    async def test_hides_sql_by_default_json(self, mcp_server, storage: YAMLStorage) -> None:
        """JSON format without show_sql excludes sql keys from dimensions and measures."""
        await storage.save_model(SlayerModel(
            name="jsqlt",
            sql="SELECT id, val FROM raw_table",
            sql_table=None,
            data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
Column(name="val", sql="val", filter="val > 0", type=DataType.DOUBLE)
            ],
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "jsqlt", "format": "json", "num_rows": 0,
        })
        parsed = json.loads(result)
        # Model-level sql and sql_table should be absent
        assert "sql" not in parsed
        assert "sql_table" not in parsed
        # Column dicts should not have sql/filter keys when show_sql=False
        for c in parsed["columns"]:
            assert "sql" not in c
            assert "filter" not in c

    async def test_shows_sql_when_requested_json(self, mcp_server, storage: YAMLStorage) -> None:
        """JSON format with show_sql=True includes sql keys everywhere."""
        await storage.save_model(SlayerModel(
            name="jsqlshow",
            sql="SELECT id, val FROM raw_table",
            sql_table=None,
            data_source="test",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
Column(name="val", sql="val", filter="val > 0", type=DataType.DOUBLE)
            ],
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "jsqlshow", "format": "json", "num_rows": 0, "show_sql": True,
        })
        parsed = json.loads(result)
        assert "sql" in parsed
        assert parsed["sql"] == "SELECT id, val FROM raw_table"
        for c in parsed["columns"]:
            assert "sql" in c
            assert "filter" in c


class TestInspectModelSectionGating:
    """``sections`` parameter on inspect_model — markdown rendering."""

    async def _save_rich_model(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
        await storage.save_model(SlayerModel(
            name="rich", sql_table="t", data_source="test",
            description="A rich model used to exercise inspect_model section gating.",
            columns=[
                Column(name="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", type=DataType.TEXT, description="Order state"),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
            measures=[
                ModelMeasure(name="aov", formula="amount:sum / *:count", description="Average order value"),
                ModelMeasure(name="rev", formula="amount:sum"),
            ],
            aggregations=[
                Aggregation(
                    name="wavg",
                    formula="SUM({sql} * {weight}) / NULLIF(SUM({weight}), 0)",
                    description="Weighted average",
                ),
            ],
            joins=[
                ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
                ModelJoin(target_model="products", join_pairs=[["product_id", "id"]]),
            ],
        ))

    async def test_default_renders_all_sections_no_footer(self, mcp_server, storage: YAMLStorage) -> None:
        """Default call (no `sections=`) emits all sections and no footer."""
        await self._save_rich_model(storage)
        result = await _call(mcp_server, name="inspect_model", arguments={"model_name": "rich"})
        assert "## Columns (3)" in result
        assert "## Measures (2)" in result
        assert "## Joins (2)" in result
        # Default does not opt into show_sql, so aggregations table appears with name/params/description
        assert "## Aggregations (1)" in result
        # The footer only renders when something was trimmed/unknown
        assert "Sections shown:" not in result
        assert "Names-only:" not in result
        assert "Omitted:" not in result

    async def test_columns_only_collapses_others(self, mcp_server, storage: YAMLStorage) -> None:
        """sections=['columns'] keeps columns full; other parts collapse to names-only or vanish."""
        await self._save_rich_model(storage)
        result = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "rich", "sections": ["columns"]},
        )
        # Columns full
        assert "## Columns (3)" in result
        # Measures, Aggregations, Joins → names-only (CSV under heading)
        assert "## Measures (2 — names only)" in result
        assert "`aov`, `rev`" in result
        assert "## Aggregations (1 — names only)" in result
        assert "`wavg`" in result
        assert "## Joins (2 — names only)" in result
        assert "`customers`, `products`" in result
        # Reachable fields and samples fully omitted
        assert "## Reachable" not in result
        assert "## Sample Data" not in result
        # Footer present
        assert "> Sections shown: columns." in result
        assert "> Names-only: measures, aggregations, joins." in result
        assert "> Omitted: samples, learnings." in result
        assert "Re-call inspect_model" in result

    async def test_omitted_sections_with_no_entities_render_nothing(self, mcp_server, storage: YAMLStorage) -> None:
        """A names-only section with no entities does not emit a heading."""
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
        await storage.save_model(SlayerModel(
            name="bare", sql_table="t", data_source="test",
            columns=[Column(name="id", type=DataType.DOUBLE, primary_key=True)],
        ))
        result = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "bare", "sections": ["columns"]},
        )
        assert "## Columns" in result
        # No measures, aggregations, or joins → no headings for them at all
        assert "## Measures" not in result
        assert "## Aggregations" not in result
        assert "## Joins" not in result

    async def test_unknown_section_emits_warning(self, mcp_server, storage: YAMLStorage) -> None:
        """An unknown section name produces a warning line in the footer."""
        await self._save_rich_model(storage)
        result = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "rich", "sections": ["columns", "fish"]},
        )
        assert "> Warning: ignored unknown sections: 'fish'." in result
        assert "Valid: columns, measures, aggregations, joins, samples, learnings." in result
        # Valid section still rendered
        assert "## Columns (3)" in result

    async def test_all_unknown_resolves_to_no_sections(self, mcp_server, storage: YAMLStorage) -> None:
        """When every supplied section is unknown, no sections are selected
        (the explicit ``None``/``[]`` form is reserved for "all six"). The
        agent gets the always-on header + every gated section in either
        names-only or fully-omitted form, plus the warning. They can correct
        and re-call with valid names."""
        await self._save_rich_model(storage)
        result = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "rich", "sections": ["fish", "bird"]},
        )
        assert "Warning: ignored unknown sections: 'fish', 'bird'." in result
        # Always-on header still renders
        assert "# Model: `rich`" in result
        # Names-only collapse for the four gateable sections — no full tables
        assert "## Columns (3 — names only)" in result
        assert "## Measures (2 — names only)" in result
        assert "## Joins (2 — names only)" in result
        # The full-table heading must NOT appear
        assert "## Columns (3)\n\n|" not in result
        # samples and learnings are fully omitted (no heading at all)
        assert "## Sample" not in result
        # Reachable-via-joins section was removed entirely in DEV-1560 — it
        # must never appear regardless of input.
        assert "## Reachable" not in result
        # Footer summarises what was dropped — caller can re-call with a
        # corrected sections= list.
        assert "Sections shown: (none)" in result
        assert "Names-only: columns, measures, aggregations, joins" in result
        assert "Omitted: samples, learnings" in result
        assert "Re-call inspect_model with `sections=[...]`" in result

    async def test_canonical_order_regardless_of_input(self, mcp_server, storage: YAMLStorage) -> None:
        """Sections render in canonical order regardless of caller order."""
        await self._save_rich_model(storage)
        result = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "rich", "sections": ["measures", "columns"]},
        )
        assert result.find("## Columns (3)") < result.find("## Measures (2)")

    # ---- DEV-1560: reachable_fields surface fully removed ----

    async def test_old_reachable_fields_token_is_unknown(self, mcp_server, storage: YAMLStorage) -> None:
        """``sections=["reachable_fields"]`` post-removal flows through the
        existing unknown-section branch (NOT silently filtered with a
        fallback to full output): footer warning lists the bad token with
        the current ``Valid:`` set, the all-unknown-resolves-to-no-sections
        semantics kick in (collapsible sections render as names-only,
        ``samples``/``learnings`` are omitted), the markdown body emits no
        ``## Reachable via joins`` heading, and the JSON payload has no
        ``reachable_dimensions`` / ``reachable_measures`` keys regardless of
        format."""
        await self._save_rich_model(storage)
        result_md = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "rich", "sections": ["reachable_fields"]},
        )
        # Exact one-line warning contract — fragments split could otherwise
        # admit a malformed two-line footer.
        assert (
            "> Warning: ignored unknown sections: 'reachable_fields'. "
            "Valid: columns, measures, aggregations, joins, samples, learnings."
        ) in result_md
        # The reachable-via-joins heading is gone for good — it can never
        # render again, regardless of the section token.
        assert "## Reachable" not in result_md
        # All-unknown ⇒ no full sections selected: collapsible sections
        # render names-only, samples + learnings are fully omitted.
        # A silent-drop implementation (filter the bad token, fall back to
        # the full default) would emit the full Columns table — this
        # assertion pins that the unknown-branch wins.
        assert "## Columns (3)\n\n|" not in result_md
        assert "## Columns (3 — names only)" in result_md
        assert "> Sections shown: (none)" in result_md
        assert "> Names-only: columns, measures, aggregations, joins." in result_md
        assert "> Omitted: samples, learnings." in result_md

        result_json = await _call(
            mcp_server, name="inspect_model",
            arguments={
                "model_name": "rich",
                "sections": ["reachable_fields"],
                "format": "json",
            },
        )
        parsed = json.loads(result_json)
        assert parsed["unknown_sections"] == ["reachable_fields"]
        assert "reachable_dimensions" not in parsed
        assert "reachable_measures" not in parsed
        # Full "columns" key must NOT be present — that would mean the
        # unknown-branch was silently bypassed.
        assert "columns" not in parsed
        # The collapsed names-only form lives under <section>_names siblings.
        assert parsed["names_only_sections"] == ["columns", "measures", "aggregations", "joins"]
        assert parsed["omitted_sections"] == ["samples", "learnings"]

    async def test_old_reachable_fields_token_with_other_sections(self, mcp_server, storage: YAMLStorage) -> None:
        """``sections=["reachable_fields", "columns"]`` renders the columns
        table fully, warns about the unknown ``reachable_fields`` token, and
        emits no reachable-via-joins markdown heading or JSON keys."""
        await self._save_rich_model(storage)
        result_md = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "rich", "sections": ["reachable_fields", "columns"]},
        )
        # Columns table renders in full.
        assert "## Columns (3)" in result_md
        # Footer warning calls out the bad token by repr.
        assert "> Warning: ignored unknown sections: 'reachable_fields'." in result_md
        assert "## Reachable" not in result_md

        result_json = await _call(
            mcp_server, name="inspect_model",
            arguments={
                "model_name": "rich",
                "sections": ["reachable_fields", "columns"],
                "format": "json",
            },
        )
        parsed = json.loads(result_json)
        assert "columns" in parsed
        assert parsed["unknown_sections"] == ["reachable_fields"]
        assert "reachable_dimensions" not in parsed
        assert "reachable_measures" not in parsed

    async def test_reachable_fields_depth_kwarg_has_no_effect(self, mcp_server, storage: YAMLStorage) -> None:
        """DEV-1560 removed the ``reachable_fields_depth`` kwarg from the
        ``inspect_model`` signature. FastMCP's input-schema validation
        uses pydantic's default ``extra="ignore"`` — unknown kwargs are
        silently dropped at the tool boundary rather than raised. The
        caller-facing regression-pin is therefore the descriptor schema
        (see ``test_tool_descriptor_omits_reachable_fields_depth_arg``)
        plus this companion test, which proves the silently-dropped
        kwarg has no behavioural effect: the rendered output is
        byte-identical to the call without it, AND no
        ``## Reachable via joins`` heading sneaks back in via any code
        path that might re-read the bogus value."""
        await self._save_rich_model(storage)
        result_with = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "rich", "reachable_fields_depth": 5},
        )
        result_without = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "rich"},
        )
        assert result_with == result_without
        assert "## Reachable" not in result_with
        assert "Reachable via joins" not in result_with

    async def test_no_reachable_via_joins_heading_anywhere(self, mcp_server, storage: YAMLStorage) -> None:
        """Invariant: ``## Reachable via joins`` and the
        ``reachable_dimensions`` / ``reachable_measures`` JSON keys never
        appear in inspect_model output. Exercises the default
        ``sections=None`` path and the explicit full-section-list path."""
        await self._save_rich_model(storage)
        for sections_arg in (
            None,
            ["columns", "measures", "aggregations", "joins", "samples", "learnings"],
        ):
            args: dict = {"model_name": "rich"}
            if sections_arg is not None:
                args["sections"] = sections_arg
            md = await _call(mcp_server, name="inspect_model", arguments=args)
            assert "## Reachable" not in md, sections_arg
            assert "Reachable via joins" not in md, sections_arg

            args_json = dict(args, format="json")
            raw = await _call(mcp_server, name="inspect_model", arguments=args_json)
            parsed = json.loads(raw)
            assert "reachable_dimensions" not in parsed, sections_arg
            assert "reachable_measures" not in parsed, sections_arg


class TestInspectModelDescriptionsMaxChars:
    """``descriptions_max_chars`` parameter — truncation behaviour."""

    async def _save_with_long_descriptions(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
        await storage.save_model(SlayerModel(
            name="m", sql_table="t", data_source="test",
            description="A" * 100,
            columns=[Column(name="status", type=DataType.TEXT, description="B" * 100)],
            measures=[ModelMeasure(name="rev", formula="*:count", description="C" * 100)],
            aggregations=[Aggregation(
                name="wavg", formula="SUM({x})", description="D" * 100,
            )],
        ))

    async def test_no_truncation_by_default(self, mcp_server, storage: YAMLStorage) -> None:
        """descriptions_max_chars=None leaves descriptions untouched."""
        await self._save_with_long_descriptions(storage)
        result = await _call(mcp_server, name="inspect_model", arguments={"model_name": "m"})
        assert "A" * 100 in result
        assert "B" * 100 in result
        assert "[truncated]" not in result

    async def test_truncates_descriptions_when_set(self, mcp_server, storage: YAMLStorage) -> None:
        """All four description fields are truncated to max_chars + marker."""
        await self._save_with_long_descriptions(storage)
        result = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "m", "descriptions_max_chars": 10, "show_sql": True},
        )
        # Marker present
        assert "[truncated]" in result
        # No 50+ run of the same letter (those would only exist if untruncated)
        assert "A" * 50 not in result
        assert "B" * 50 not in result
        assert "C" * 50 not in result
        assert "D" * 50 not in result

    async def test_short_descriptions_unchanged(self, mcp_server, storage: YAMLStorage) -> None:
        """Descriptions shorter than max_chars are not modified."""
        await storage.save_model(SlayerModel(
            name="short", sql_table="t", data_source="test",
            description="hi",
            columns=[Column(name="x", type=DataType.TEXT, description="ok")],
        ))
        result = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "short", "descriptions_max_chars": 100},
        )
        assert "hi" in result
        assert "ok" in result
        assert "[truncated]" not in result

    async def test_zero_yields_marker_only(self, mcp_server, storage: YAMLStorage) -> None:
        """descriptions_max_chars=0 truncates every non-empty description to just the marker."""
        await self._save_with_long_descriptions(storage)
        result = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "m", "descriptions_max_chars": 0},
        )
        assert "[truncated]" in result
        assert "A" * 5 not in result

    async def test_negative_rejected(self, mcp_server, storage: YAMLStorage) -> None:
        """Negative values would silently produce ``str[:-N] + marker`` (i.e.
        "all but the last N chars" instead of "first N chars"). Rejected at
        the tool boundary."""
        from mcp.server.fastmcp.exceptions import ToolError
        await self._save_with_long_descriptions(storage)
        with pytest.raises(ToolError, match="descriptions_max_chars must be >= 0"):
            await _call(
                mcp_server, name="inspect_model",
                arguments={"model_name": "m", "descriptions_max_chars": -1},
            )


class TestInspectModelAggregationsShowSql:
    """show_sql gating on the aggregations section."""

    async def _save_with_aggs(self, storage: YAMLStorage) -> None:
        from slayer.core.models import AggregationParam
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
        await storage.save_model(SlayerModel(
            name="m", sql_table="t", data_source="test",
            aggregations=[Aggregation(
                name="wavg",
                formula="SUM({sql} * {weight}) / NULLIF(SUM({weight}), 0)",
                params=[AggregationParam(name="weight", sql="quantity")],
                description="Weighted avg",
            )],
        ))

    async def test_aggregations_full_with_show_sql_true(self, mcp_server, storage: YAMLStorage) -> None:
        """show_sql=True keeps formula and full params."""
        await self._save_with_aggs(storage)
        result = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "m", "sections": ["aggregations"], "show_sql": True},
        )
        assert "## Aggregations (1)" in result
        assert "SUM({sql} * {weight})" in result
        assert "weight=quantity" in result

    async def test_aggregations_show_sql_false_drops_formula_and_param_sql(self, mcp_server, storage: YAMLStorage) -> None:
        """show_sql=False drops formula column and the param SQL (param names only)."""
        await self._save_with_aggs(storage)
        result = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "m", "sections": ["aggregations"], "show_sql": False},
        )
        assert "## Aggregations (1)" in result
        # Formula content is gone (the SQL fragment doesn't appear anywhere)
        assert "SUM({sql} * {weight})" not in result
        # `quantity` (the SQL of the param) is gone, but the param `name` should be present
        assert "quantity" not in result
        assert "weight" in result

    async def test_aggregations_names_only_when_omitted(self, mcp_server, storage: YAMLStorage) -> None:
        await self._save_with_aggs(storage)
        result = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "m", "sections": ["columns"]},
        )
        assert "## Aggregations (1 — names only)" in result
        assert "`wavg`" in result
        # No param SQL or formula leaks
        assert "quantity" not in result
        assert "SUM(" not in result


class TestInspectModelJsonGating:
    """JSON parity for section gating, descriptions, and observability arrays."""

    async def _save_rich(self, storage: YAMLStorage) -> None:
        from slayer.core.models import AggregationParam
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
        await storage.save_model(SlayerModel(
            name="rich", sql_table="t", data_source="test",
            description="X" * 50,
            columns=[
                Column(name="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="status", type=DataType.TEXT),
            ],
            measures=[ModelMeasure(name="aov", formula="*:count")],
            aggregations=[Aggregation(
                name="wavg",
                formula="SUM({a})",
                params=[AggregationParam(name="a", sql="amount")],
            )],
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        ))

    async def test_json_columns_only_uses_names_keys(self, mcp_server, storage: YAMLStorage) -> None:
        await self._save_rich(storage)
        raw = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "rich", "sections": ["columns"], "format": "json"},
        )
        parsed = json.loads(raw)
        # Columns key present (full)
        assert "columns" in parsed
        # Measures/aggregations/joins → names-only siblings
        assert parsed["measures_names"] == ["aov"]
        assert parsed["aggregations_names"] == ["wavg"]
        assert parsed["joins_names"] == ["customers"]
        assert "measures" not in parsed
        assert "aggregations" not in parsed
        assert "joins" not in parsed
        # Reachable / samples → fully absent
        assert "reachable_dimensions" not in parsed
        assert "reachable_measures" not in parsed
        assert "sample_data" not in parsed
        assert "sample_data_error" not in parsed
        # Top-level state arrays
        assert parsed["names_only_sections"] == ["measures", "aggregations", "joins"]
        assert parsed["omitted_sections"] == ["samples", "learnings"]
        assert "unknown_sections" not in parsed

    async def test_json_unknown_sections_array(self, mcp_server, storage: YAMLStorage) -> None:
        await self._save_rich(storage)
        raw = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "rich", "sections": ["columns", "fish"], "format": "json"},
        )
        parsed = json.loads(raw)
        assert parsed["unknown_sections"] == ["fish"]

    async def test_json_aggregations_show_sql_false_drops_formula_and_param_sql(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        await self._save_rich(storage)
        raw = await _call(
            mcp_server, name="inspect_model",
            arguments={
                "model_name": "rich",
                "sections": ["aggregations"],
                "show_sql": False,
                "format": "json",
            },
        )
        parsed = json.loads(raw)
        agg = parsed["aggregations"][0]
        assert "formula" not in agg
        # Each param dict has only `name`
        assert agg["params"] == [{"name": "a"}]

    async def test_json_aggregations_show_sql_true_includes_formula_and_param_sql(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        await self._save_rich(storage)
        raw = await _call(
            mcp_server, name="inspect_model",
            arguments={
                "model_name": "rich",
                "sections": ["aggregations"],
                "show_sql": True,
                "format": "json",
            },
        )
        parsed = json.loads(raw)
        agg = parsed["aggregations"][0]
        assert agg["formula"] == "SUM({a})"
        assert agg["params"] == [{"name": "a", "sql": "amount"}]

    async def test_json_descriptions_truncated(self, mcp_server, storage: YAMLStorage) -> None:
        await self._save_rich(storage)
        raw = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "rich", "descriptions_max_chars": 5, "format": "json"},
        )
        parsed = json.loads(raw)
        assert parsed["description"].startswith("XXXXX")
        assert parsed["description"].endswith("[truncated]")

    async def test_json_default_no_gating_arrays(self, mcp_server, storage: YAMLStorage) -> None:
        """Default call should not include the gating arrays (nothing was trimmed)."""
        await self._save_rich(storage)
        raw = await _call(
            mcp_server, name="inspect_model",
            arguments={"model_name": "rich", "format": "json"},
        )
        parsed = json.loads(raw)
        assert "names_only_sections" not in parsed
        assert "omitted_sections" not in parsed
        assert "unknown_sections" not in parsed


class TestInspectModelHelpers:
    """Direct unit tests for the section-budgeting helpers."""

    def test_truncate_description_none(self) -> None:
        from slayer.mcp.server import _truncate_description
        assert _truncate_description(None, 10) is None
        assert _truncate_description("anything", None) == "anything"

    def test_truncate_description_short_unchanged(self) -> None:
        from slayer.mcp.server import _truncate_description
        assert _truncate_description("hello", 10) == "hello"
        # Boundary: exactly equal to max_chars should not truncate
        assert _truncate_description("hello", 5) == "hello"

    def test_truncate_description_marks_truncation(self) -> None:
        from slayer.mcp.server import _truncate_description
        assert _truncate_description("hello world", 5) == "hello ... [truncated]"

    def test_truncate_description_zero_yields_marker(self) -> None:
        from slayer.mcp.server import _truncate_description
        assert _truncate_description("hello", 0) == " ... [truncated]"

    def test_resolve_inspect_sections_none(self) -> None:
        from slayer.mcp.server import _resolve_inspect_sections
        resolved, unknown = _resolve_inspect_sections(None)
        assert resolved == ["columns", "measures", "aggregations", "joins", "samples", "learnings"]
        assert unknown == []

    def test_resolve_inspect_sections_empty(self) -> None:
        from slayer.mcp.server import _resolve_inspect_sections
        resolved, unknown = _resolve_inspect_sections([])
        # Same expansion as the ``None`` case — pin the order so a
        # missing or duplicated section name fails.
        assert resolved == [
            "columns",
            "measures",
            "aggregations",
            "joins",
            "samples",
            "learnings",
        ]
        assert unknown == []

    def test_resolve_inspect_sections_subset_canonical_order(self) -> None:
        from slayer.mcp.server import _resolve_inspect_sections
        resolved, unknown = _resolve_inspect_sections(["measures", "columns"])
        # Canonical order, not caller order
        assert resolved == ["columns", "measures"]
        assert unknown == []

    def test_resolve_inspect_sections_unknowns_filtered(self) -> None:
        from slayer.mcp.server import _resolve_inspect_sections
        resolved, unknown = _resolve_inspect_sections(["columns", "fish", "samples"])
        assert resolved == ["columns", "samples"]
        assert unknown == ["fish"]

    def test_resolve_inspect_sections_all_unknown_returns_empty(self) -> None:
        """A non-empty list of only-unknown names resolves to no sections.
        Reserves "all six" for the explicit ``None`` / ``[]`` forms so a typo
        like ``sections=["sample"]`` can't silently trigger the full expensive
        payload. The footer's warning + names-only listing tells the caller
        what they have to work with.
        """
        from slayer.mcp.server import _resolve_inspect_sections
        resolved, unknown = _resolve_inspect_sections(["fish", "bird"])
        assert resolved == []
        assert unknown == ["fish", "bird"]

    def test_render_inspect_footer_sanitizes_unknown_section_names(self) -> None:
        """Caller-supplied unknown names containing newlines / quote-prefix
        characters must not forge additional footer lines. ``repr()`` escapes
        control chars so the warning stays a single line.
        """
        from slayer.mcp.server import _render_inspect_footer
        result = _render_inspect_footer(
            included=["columns", "measures", "aggregations", "joins", "samples", "learnings"],
            names_only=[], omitted=[], unknown=["foo\n> evil-injected"],
        )
        assert result is not None
        # Newline never reaches the rendered output
        assert "\n> evil" not in result
        # The escaped form does
        assert "\\n" in result and "evil-injected" in result

    def test_render_inspect_footer_none_when_no_trim(self) -> None:
        from slayer.mcp.server import _render_inspect_footer
        result = _render_inspect_footer(
            included=["columns", "measures", "aggregations", "joins", "samples", "learnings"],
            names_only=[], omitted=[], unknown=[],
        )
        assert result is None

    def test_render_inspect_footer_warning_only(self) -> None:
        """All sections shown but unknown names → warning line only, no other footer text."""
        from slayer.mcp.server import _render_inspect_footer
        result = _render_inspect_footer(
            included=["columns", "measures", "aggregations", "joins", "samples", "learnings"],
            names_only=[], omitted=[], unknown=["fish"],
        )
        assert result is not None
        assert "Warning" in result
        assert "Sections shown:" not in result

    def test_render_inspect_footer_full(self) -> None:
        from slayer.mcp.server import _render_inspect_footer
        result = _render_inspect_footer(
            included=["columns"],
            names_only=["measures", "joins"],
            omitted=["samples"],
            unknown=["fish"],
        )
        assert result is not None
        assert "Warning: ignored unknown sections: 'fish'." in result
        assert "Sections shown: columns." in result
        assert "Names-only: measures, joins." in result
        assert "Omitted: samples." in result
        assert "Re-call inspect_model" in result


class TestBuildSampleQueryArgs:
    def test_avg_when_allowed(self) -> None:
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            columns=[
                Column(name="status", type=DataType.TEXT),
                Column(name="region", type=DataType.TEXT),
                Column(name="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="rev", sql="amt", type=DataType.DOUBLE),
                Column(name="qty", sql="quantity", type=DataType.DOUBLE),
            ],
        )
        args = _build_sample_query_args(model=model, num_rows=7)
        assert [f["formula"] for f in args["measures"]] == ["*:count", "rev:avg", "qty:avg"]
        assert [d["name"] for d in args["dimensions"]] == ["status", "region"]
        assert args["limit"] == 7
        assert args["source_model"] == "t"

    def test_fallback_to_first_allowed_when_avg_not_permitted(self) -> None:
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            columns=[Column(name="rev", sql="amt", allowed_aggregations=["sum", "max"], type=DataType.DOUBLE)],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        assert [f["formula"] for f in args["measures"]] == ["*:count", "rev:sum"]

    def test_prefers_safe_agg_over_first_allowed(self) -> None:
        """When the allowed list starts with a non-safe aggregation (e.g. first,
        last), _build_sample_query_args should skip it and pick the first safe
        zero-arg aggregation from the list."""
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            columns=[Column(name="rev", sql="amt", allowed_aggregations=["last", "first", "min", "max"], type=DataType.DOUBLE)],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        assert [f["formula"] for f in args["measures"]] == ["*:count", "rev:min"]

    def test_falls_back_to_first_allowed_when_no_safe_agg(self) -> None:
        """When the allowed list contains no safe aggregation, fall back to the
        first entry (even if it requires extra context like a time column)."""
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            columns=[Column(name="rev", sql="amt", allowed_aggregations=["last", "first"], type=DataType.DOUBLE)],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        assert [f["formula"] for f in args["measures"]] == ["*:count", "rev:last"]

    def test_skip_when_allowed_is_empty(self) -> None:
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            columns=[Column(name="rev", sql="amt", allowed_aggregations=[], type=DataType.DOUBLE)],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        assert [f["formula"] for f in args["measures"]] == ["*:count"]

    def test_dims_cap_at_two_and_exclude_pk_and_hidden(self) -> None:
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            columns=[
                Column(name="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="hidden_d", type=DataType.TEXT, hidden=True),
                Column(name="a", type=DataType.TEXT),
                Column(name="b", type=DataType.TEXT),
                Column(name="c", type=DataType.TEXT),
            ],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        assert [d["name"] for d in args["dimensions"]] == ["a", "b"]

    def test_hidden_column_skipped(self) -> None:
        model = SlayerModel(
            name="t", sql_table="t", data_source="ds",
            columns=[
                Column(name="rev", sql="amt", hidden=True, type=DataType.DOUBLE),
                Column(name="qty", sql="quantity", type=DataType.DOUBLE),
            ],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        assert [f["formula"] for f in args["measures"]] == ["*:count", "qty:avg"]

    def test_count_distinct_fallback_for_non_numeric_columns(self) -> None:
        """Non-numeric columns (string, boolean, date) get ``:count_distinct`` in
        the sample query when not already used as group-by dimensions; numeric
        columns get ``:avg``."""
        model = SlayerModel(
            name="order_items", sql_table="order_items", data_source="ds",
            columns=[
                Column(name="id", type=DataType.TEXT, primary_key=True),
                Column(name="sku", sql="sku", type=DataType.TEXT),
                Column(name="is_flagged", sql="is_flagged", type=DataType.BOOLEAN),
                Column(name="extra_string", sql="extra_string", type=DataType.TEXT),
                Column(name="quantity", sql="quantity", type=DataType.DOUBLE),
            ],
        )
        args = _build_sample_query_args(model=model, num_rows=3)
        # First two categorical columns (sku, is_flagged) become group-by dims.
        # Remaining non-numeric (extra_string) is aggregated as count_distinct;
        # numeric (quantity) is aggregated as avg.
        assert [d["name"] for d in args["dimensions"]] == ["sku", "is_flagged"]
        assert [f["formula"] for f in args["measures"]] == [
            "*:count", "extra_string:count_distinct", "quantity:avg",
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
        """Backticks inside values use a longer fence instead of backslash escaping,
        per CommonMark inline code span rules."""
        out = _markdown_table(
            rows=[
                {"name": "no`ticks", "desc": None},
                {"name": "plain", "desc": None},
            ],
            columns=["name", "desc"],
        )
        assert out == "``no`ticks``, `plain`"

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
        """``allowed_aggregations`` lives on a Column in v2, not a measure formula."""
        result = await _call(mcp_server, name="create_model", arguments={
            "name": "orders",
            "sql_table": "public.orders",
            "data_source": "test_ds",
            "columns": [
                {"name": "revenue", "sql": "amount", "type": "number",
                 "allowed_aggregations": ["sum", "avg"]},
            ],
        })
        assert "created" in result
        model = await storage.get_model("orders")
        assert model.columns[0].allowed_aggregations == ["sum", "avg"]

    async def test_create_with_measure_meta_dict(self, mcp_server, storage: YAMLStorage) -> None:
        # Pins create_model.measures' parameter type at List[Dict[str, Any]] —
        # FastMCP introspects the annotation to build its tool schema, so a
        # narrower Dict[str, str] would reject this dict-typed `meta` value at
        # call time before the function body ever runs.
        result = await _call(mcp_server, name="create_model", arguments={
            "name": "orders",
            "sql_table": "public.orders",
            "data_source": "test_ds",
            "columns": [
                {"name": "revenue", "sql": "amount", "type": "number"},
            ],
            "measures": [
                {"name": "aov", "formula": "revenue:sum / *:count",
                 "meta": {"kb_id": "abc-123", "tags": ["finance", "kpi"]}},
            ],
        })
        assert "created" in result
        model = await storage.get_model("orders")
        assert model.measures[0].meta == {"kb_id": "abc-123", "tags": ["finance", "kpi"]}

    async def test_create_reports_replaced(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="create_model", arguments={"name": "orders", "sql_table": "t2", "data_source": "test"})
        assert "replaced" in result

    async def test_create_from_query_rejects_mixed_params(self, mcp_server) -> None:
        result = await _call(mcp_server, name="create_model", arguments={
            "name": "bad",
            "query": {"source_model": "orders", "measures": ["*:count"]},
            "sql_table": "public.orders",
        })
        assert "Error" in result
        assert "query" in result
        assert "sql_table" in result

    async def test_create_from_query_rejects_data_source(self, mcp_server) -> None:
        result = await _call(mcp_server, name="create_model", arguments={
            "name": "bad",
            "query": {"source_model": "orders", "measures": ["*:count"]},
            "data_source": "mydb",
        })
        assert "Error" in result
        assert "data_source" in result

    async def test_create_from_query_ignores_empty_placeholders(self, mcp_server, storage: YAMLStorage) -> None:
        """Empty lists/strings should not trigger the mixed-parameter error."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="test_ds",
            columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
        ))
        result = await _call(mcp_server, name="create_model", arguments={
            "name": "summary",
            "query": {"source_model": "orders", "measures": ["amount:sum"]},
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
            columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
        ))
        result = await _call(mcp_server, name="create_model", arguments={
            "name": "summary",
            "query": {"source_model": "orders", "measures": ["amount:sum"]},
        })
        # Should fail on missing datasource, not on "missing sql_table"
        assert "Datasource" in result

    async def test_create_from_query_populates_backing_query_sql(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        """End-to-end MCP write path: ``create_model(query=...)`` must route
        through ``engine.save_model`` so the persisted query-backed model has
        ``backing_query_sql`` populated. Read paths no longer warm the cache
        (issue #74) — a regression that drops the engine routing in
        ``slayer/mcp/server.py`` would silently leave the cache empty.
        """
        from slayer.core.models import DatasourceConfig
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
        ))
        result = await _call(mcp_server, name="create_model", arguments={
            "name": "summary",
            "query": {
                "source_model": "orders",
                "measures": ["amount:sum"],
            },
        })
        assert "Error" not in result, result
        persisted = await storage.get_model("summary")
        assert persisted is not None
        assert persisted.backing_query_sql, (
            "MCP create_model(query=...) must populate backing_query_sql"
        )
        assert "amount" in persisted.backing_query_sql.lower()
        assert persisted.columns, "MCP create_model(query=...) must populate columns"


class TestEditModel:
    """Tests for the edit_model MCP tool with upsert semantics."""

    # --- Measure upserts ---

    async def test_upsert_new_column(self, mcp_server, storage: YAMLStorage) -> None:
        """Upserting a new column adds it to ``columns``."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "columns": [{"name": "total", "sql": "amount", "type": "number"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("created column 'total'" in c for c in parsed["changes"])
        model = await storage.get_model("orders")
        assert len(model.columns) == 2

    async def test_upsert_column_with_allowed_aggregations(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "columns": [{"name": "total", "sql": "amount", "type": "number", "allowed_aggregations": ["sum", "avg"]}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_model("orders")
        total = next(c for c in model.columns if c.name == "total")
        assert total.allowed_aggregations == ["sum", "avg"]

    async def test_upsert_existing_column(self, mcp_server, storage: YAMLStorage) -> None:
        """Upserting an existing column updates it instead of erroring."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="revenue", sql="amount", description="old", type=DataType.DOUBLE)],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "columns": [{"name": "revenue", "sql": "price"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("updated column 'revenue'" in c for c in parsed["changes"])
        model = await storage.get_model("orders")
        assert len(model.columns) == 1
        assert model.columns[0].sql == "price"

    async def test_upsert_existing_column_partial_update(self, mcp_server, storage: YAMLStorage) -> None:
        """Partial upsert: only specified fields change, others are preserved."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="revenue", sql="amount", description="Total revenue", type=DataType.DOUBLE)],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "columns": [{"name": "revenue", "description": "Updated description"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        c = (await storage.get_model("orders")).columns[0]
        assert c.description == "Updated description"
        assert c.sql == "amount"  # unchanged

    # --- New ``measures`` (ModelMeasure formulas) upserts ---

    async def test_upsert_new_measure_formula(self, mcp_server, storage: YAMLStorage) -> None:
        """Upserting a new model-level ModelMeasure adds it to ``measures``."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "measures": [{"name": "aov", "formula": "revenue:sum / *:count"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("created measure 'aov'" in c for c in parsed["changes"])
        model = await storage.get_model("orders")
        assert len(model.measures) == 1
        assert model.measures[0].formula == "revenue:sum / *:count"

    async def test_upsert_existing_measure_formula(self, mcp_server, storage: YAMLStorage) -> None:

        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            measures=[ModelMeasure(name="aov", formula="revenue:sum / *:count")],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "measures": [{"name": "aov", "formula": "revenue:avg"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_model("orders")
        assert model.measures[0].formula == "revenue:avg"

    async def test_upsert_multiple_mixed_create_update(self, mcp_server, storage: YAMLStorage) -> None:
        """One new + one existing column in the same call."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "columns": [
                {"name": "revenue", "description": "Updated"},
                {"name": "profit", "sql": "revenue - cost", "type": "number"},
            ],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("updated column 'revenue'" in c for c in parsed["changes"])
        assert any("created column 'profit'" in c for c in parsed["changes"])
        model = await storage.get_model("orders")
        assert len(model.columns) == 2

    async def test_invalid_column_type_on_upsert(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "columns": [{"name": "bad", "type": "invalid_type"}],
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
            columns=[Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "description": "Orders table",
            "columns": [
                {"name": "total", "sql": "amount", "type": "number"},
                {"name": "status", "sql": "status", "type": "string"},
            ],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert len(parsed["changes"]) == 3
        model = await storage.get_model("orders")
        assert model.description == "Orders table"
        assert any(c.name == "status" for c in model.columns)
        assert any(c.name == "total" for c in model.columns)

    # --- Typed remove ---

    async def test_remove_column_typed(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="revenue", sql="amount", type=DataType.DOUBLE), Column(name="total", sql="x", type=DataType.DOUBLE)],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "remove": {"columns": ["total"]},
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_model("orders")
        assert len(model.columns) == 1

    async def test_remove_column_not_found(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_model(SlayerModel(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "remove": {"columns": ["nonexistent"]},
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
        """Remove a column then upsert one with the same name in the same call."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="status", sql="old_col", type=DataType.TEXT)],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "remove": {"columns": ["status"]},
            "columns": [{"name": "status", "sql": "new_col", "type": "string"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        c = (await storage.get_model("orders")).columns[0]
        assert c.sql == "new_col"

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
            "columns": [{"name": "rev", "sql": "amount", "type": "number", "allowed_aggregations": ["nonexistent_agg"]}],
        })
        assert "Validation error" in result or "not a built-in aggregation" in result

    # --- Query-backed model edits ---

    async def test_edit_set_source_queries_makes_model_query_backed(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        """Switching source mode via edit_model: sql_table → source_queries.
        Persisted state must show source_queries set and sql_table cleared.
        """
        from slayer.core.models import DatasourceConfig
        # Engine save path needs a datasource to dry-run validate the new
        # backing query.
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        # Separate upstream model so the edited model's backing query has a
        # non-cyclic source.
        await storage.save_model(SlayerModel(
            name="orders_source", sql_table="orders_t", data_source="test",
            columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
        ))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "source_queries": [{
                "source_model": "orders_source",
                "measures": [{"formula": "amount:sum"}],
            }],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        # Persisted state: source_queries set, sql_table cleared.
        reloaded = await storage.get_model("orders")
        assert reloaded is not None
        assert reloaded.source_queries is not None
        assert len(reloaded.source_queries) == 1
        assert reloaded.source_queries[0].source_model == "orders_source"
        assert not reloaded.sql_table
        # Cache must be populated by the MCP write path (issue #74) — read
        # paths no longer warm it.
        assert reloaded.backing_query_sql, (
            "MCP edit_model(source_queries=...) must populate backing_query_sql"
        )
        assert "amount" in reloaded.backing_query_sql.lower()

    async def test_edit_query_variables_on_query_backed_model(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        from slayer.core.models import DatasourceConfig
        from slayer.core.query import SlayerQuery
        # Engine save path resolves datasource during dry-run validation;
        # provide one.
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        # Set up a query-backed model
        await storage.save_model(SlayerModel(
            name="upstream", sql_table="t", data_source="test",
            columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
        ))
        await storage.save_model(SlayerModel(
            name="qb",
            data_source="test",
            source_queries=[SlayerQuery(
                source_model="upstream",
                measures=[{"formula": "amount:sum"}],
                filters=["amount > {threshold}"],
                dry_run=True,
            )],
            query_variables={"threshold": 100},
        ))

        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "qb",
            "query_variables": {"threshold": 500},
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("query_variables" in c for c in parsed["changes"])
        reloaded = await storage.get_model("qb")
        assert reloaded.query_variables == {"threshold": 500}

    async def test_edit_rejects_simultaneous_source_modes(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test"
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "sql_table": "new_t",
            "sql": "SELECT 1",
        })
        assert "mutually exclusive" in result or "Specify at most one" in result

    # --- meta round-trip pins (DEV-1332) ---

    async def test_edit_persists_measure_meta_create_path(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """Adding a brand-new measure with meta via edit_model — meta survives
        storage round-trip. Mirrors the existing TestCreateModel pin but for
        the edit_model surface that DEV-1332 reported as broken.
        """
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "measures": [{
                "name": "aov", "formula": "revenue:sum / *:count",
                "meta": {"kb_id": 1},
            }],
        })
        assert json.loads(result)["success"] is True
        model = await storage.get_model("orders")
        assert model.measures[0].meta == {"kb_id": 1}

    async def test_edit_persists_measure_meta_update_path(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """Updating an existing measure to add meta — meta survives storage
        round-trip. The existing measure had no meta; the edit adds it."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            measures=[ModelMeasure(name="aov", formula="revenue:sum / *:count")],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "measures": [{
                "name": "aov", "formula": "revenue:sum / *:count",
                "meta": {"kb_id": 1},
            }],
        })
        assert json.loads(result)["success"] is True
        model = await storage.get_model("orders")
        assert model.measures[0].meta == {"kb_id": 1}

    async def test_edit_persists_aggregation_meta_create_path(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """Adding a brand-new aggregation with meta via edit_model — meta and
        formula both survive storage round-trip. Uses a custom aggregation
        name + formula so the test pins both fields, not just meta on a
        built-in override.
        """
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "aggregations": [{
                "name": "my_agg",
                "formula": "SUM({expr})",
                "meta": {"owner": "x"},
            }],
        })
        assert json.loads(result)["success"] is True
        model = await storage.get_model("orders")
        assert model.aggregations[0].meta == {"owner": "x"}
        assert model.aggregations[0].formula == "SUM({expr})"

    async def test_edit_persists_aggregation_meta_update_path(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """Updating an existing aggregation to add meta — meta survives
        storage round-trip."""
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            aggregations=[Aggregation(name="my_agg", formula="SUM({expr})")],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "aggregations": [{"name": "my_agg", "meta": {"owner": "x"}}],
        })
        assert json.loads(result)["success"] is True
        model = await storage.get_model("orders")
        assert model.aggregations[0].meta == {"owner": "x"}
        # And the formula on the existing aggregation is preserved (partial update).
        assert model.aggregations[0].formula == "SUM({expr})"

    async def test_edit_measure_meta_replaced_not_merged(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """Updating meta replaces the whole dict (no deep merge), consistent
        with the documented top-level model.meta replacement semantics.
        """
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            measures=[ModelMeasure(
                name="aov", formula="revenue:sum / *:count",
                meta={"a": 1},
            )],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "measures": [{"name": "aov", "meta": {"b": 2}}],
        })
        assert json.loads(result)["success"] is True
        model = await storage.get_model("orders")
        assert model.measures[0].meta == {"b": 2}

    async def test_edit_omitting_meta_key_preserves_existing_meta(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """When the edit spec omits the `meta` key entirely, the existing
        meta on the entity is preserved — _upsert_entity flat-merge semantics.
        """
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[Column(name="revenue", sql="amount", type=DataType.DOUBLE)],
            measures=[ModelMeasure(
                name="aov", formula="revenue:sum / *:count",
                meta={"kb_id": 9},
            )],
        ))
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "measures": [{"name": "aov", "description": "Average order value"}],
        })
        assert json.loads(result)["success"] is True
        model = await storage.get_model("orders")
        assert model.measures[0].description == "Average order value"
        assert model.measures[0].meta == {"kb_id": 9}


class TestEditModelDatasourceMoveSafety:
    """Moving a model to a different ``data_source`` must be atomic and
    collision-safe (PR #92 thread #8). Failure modes to pin:
    1. If a model with the same name already exists at the target
       ``(new_data_source, name)`` key, the move must refuse and the
       source model must remain intact.
    2. If validation/save fails after the data_source is updated, the
       source model must remain intact (no delete-before-save).
    """

    async def test_move_collision_with_target_refuses_and_preserves_source(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        from slayer.core.models import DatasourceConfig
        for n in ("db_a", "db_b"):
            await storage.save_datasource(DatasourceConfig(
                name=n, type="sqlite", database=":memory:"
            ))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders_a", data_source="db_a",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True)],
            description="source",
        ))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders_b", data_source="db_b",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True)],
            description="target",
        ))

        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "data_source": "db_a",
            "new_data_source": "db_b",
        })
        # Refusal mentions the collision so the agent can fix it.
        assert "exist" in result.lower() or "collid" in result.lower() or "already" in result.lower(), (
            f"expected collision rejection, got: {result}"
        )

        # Both models still in their original places, untouched.
        src = await storage.get_model("orders", data_source="db_a")
        tgt = await storage.get_model("orders", data_source="db_b")
        assert src is not None and src.description == "source"
        assert tgt is not None and tgt.description == "target"

    async def test_move_query_backed_when_engine_recomputes_data_source_back(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """For query-backed models, ``engine.save_model`` recomputes
        ``data_source`` from the backing query. If the user requests a move
        but the query still resolves back to the *original* datasource, the
        post-save delete must NOT remove the row we just saved at the
        original key. See PR #92 thread (post-merge, critical).
        """
        from slayer.core.models import DatasourceConfig
        from slayer.core.query import SlayerQuery
        from slayer.engine.query_engine import SlayerQueryEngine

        for n in ("db_a", "db_b"):
            await storage.save_datasource(DatasourceConfig(
                name=n, type="sqlite", database=":memory:"
            ))
        # Upstream lives in db_a. The query-backed model's backing query
        # references this upstream, so its resolved data_source is db_a.
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="db_a",
            columns=[
                Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)
        await engine.save_model(SlayerModel(
            name="qb",
            data_source="db_a",
            source_queries=[SlayerQuery(
                source_model="orders",
                measures=[{"formula": "amount:sum"}],
            )],
        ))
        # Sanity: the model is at (db_a, qb).
        assert await storage.get_model("qb", data_source="db_a") is not None
        assert await storage.get_model("qb", data_source="db_b") is None

        # Ask for a move qb: db_a -> db_b. The backing query still
        # resolves to db_a, so the engine cache populator will overwrite
        # ``new_data_source`` and the model should land back at db_a.
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "qb",
            "data_source": "db_a",
            "new_data_source": "db_b",
        })
        # Edit either succeeds (engine silently rerouted) or returns a
        # clear error explaining the override; either way, the model
        # must NOT be deleted from storage entirely.
        del result
        # If we silently deleted (db_a, qb) thinking the model moved to
        # (db_b, qb), then both lookups would now miss.
        landed_a = await storage.get_model("qb", data_source="db_a")
        landed_b = await storage.get_model("qb", data_source="db_b")
        assert landed_a is not None or landed_b is not None, (
            "edit_model deleted the model entirely after a no-op move "
            "of a query-backed entity"
        )

    async def test_move_save_failure_preserves_source(
        self, mcp_server, storage: YAMLStorage, monkeypatch
    ) -> None:
        from slayer.core.models import DatasourceConfig
        for n in ("db_a", "db_b"):
            await storage.save_datasource(DatasourceConfig(
                name=n, type="sqlite", database=":memory:"
            ))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders_a", data_source="db_a",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True)],
            description="source",
        ))

        # Force the save under the new datasource to fail. The implementation
        # must not have deleted the source row by then.
        original_save = storage.save_model

        async def _failing_save(model):
            if model.data_source == "db_b":
                raise RuntimeError("simulated storage failure on new key")
            return await original_save(model)

        monkeypatch.setattr(storage, "save_model", _failing_save)

        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "orders",
            "data_source": "db_a",
            "new_data_source": "db_b",
        })
        # Some kind of error surfaces (the simulated failure).
        assert (
            "fail" in result.lower()
            or "error" in result.lower()
            or "simulated" in result.lower()
        ), f"expected save-failure to surface, got: {result}"

        # The source model is still here.
        src = await storage.get_model("orders", data_source="db_a")
        assert src is not None
        assert src.data_source == "db_a"
        assert src.description == "source"


class TestEditModelMultiStageRename:
    """DEV-1335: editing a multi-stage query-backed model so an inner stage's
    measure is renamed (or the stage shape changes) must refresh the cached
    SQL/columns to reflect the new names. Outer-stage references to the new
    name must resolve cleanly.
    """

    async def _setup_orders_with_two_stage_model(
        self, storage: YAMLStorage, *, inner_measures: list, outer_measures: list,
    ) -> None:
        """Save a datasource, an upstream `orders` table-model, and a saved
        2-stage query-backed model whose inner stage is named ``raw``.
        """
        from slayer.core.query import SlayerQuery
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="t", data_source="test",
            columns=[
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="region", sql="region", type=DataType.TEXT),
            ],
        ))
        # Build initial source_queries via the engine save path so the cache
        # reflects the initial state.
        from slayer.engine.query_engine import SlayerQueryEngine
        engine = SlayerQueryEngine(storage=storage)
        await engine.save_model(SlayerModel(
            name="qb",
            data_source="test",
            source_queries=[
                SlayerQuery(
                    name="raw",
                    source_model="orders",
                    dimensions=["region"],
                    measures=inner_measures,
                ),
                SlayerQuery(
                    source_model="raw",
                    measures=outer_measures,
                ),
            ],
        ))

    async def test_edit_model_renames_inner_stage_measure(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """Initial: stage 1 names a measure ``old``. Edit replaces source_queries
        with ``new`` everywhere. The cached SQL must reference the new name and
        not the old one; cached columns must follow.
        """
        await self._setup_orders_with_two_stage_model(
            storage,
            inner_measures=[{"formula": "amount:sum", "name": "old"}],
            outer_measures=[{"formula": "old:sum"}],
        )
        # Sanity: initial cache reflects 'old'.
        before = await storage.get_model("qb")
        assert before is not None
        assert before.backing_query_sql is not None
        assert "old" in before.backing_query_sql
        assert "old_sum" in [c.name for c in before.columns]

        # Edit: rename inner-stage measure to 'new' (and update outer to match).
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "qb",
            "source_queries": [
                {
                    "name": "raw",
                    "source_model": "orders",
                    "dimensions": ["region"],
                    "measures": [{"formula": "amount:sum", "name": "new"}],
                },
                {
                    "source_model": "raw",
                    "measures": [{"formula": "new:sum"}],
                },
            ],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True, result

        after = await storage.get_model("qb")
        assert after is not None
        assert after.backing_query_sql is not None
        col_names = [c.name for c in after.columns]
        assert "new_sum" in col_names, (
            f"cache must reflect renamed inner measure, got: {col_names}"
        )
        assert "old_sum" not in col_names, (
            f"stale 'old_sum' must be evicted, got: {col_names}"
        )
        sql = after.backing_query_sql
        assert "new" in sql, f"backing_query_sql must contain new name:\n{sql}"
        # The stale name must not survive in the wrap aliases or measure refs.
        assert " AS old " not in sql and 'AS "old"' not in sql, (
            f"backing_query_sql must not retain stale 'old' alias:\n{sql}"
        )

    async def test_edit_model_stage_shape_change_drops_and_adds_measure(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """Initial stage 1 has two measures (``rev``, ``n``). Edit drops ``n``
        and adds ``avg_amount`` instead. Outer stage now references the new
        name; cached SQL must reflect the swap.
        """
        await self._setup_orders_with_two_stage_model(
            storage,
            inner_measures=[
                {"formula": "amount:sum", "name": "rev"},
                {"formula": "*:count", "name": "n"},
            ],
            outer_measures=[{"formula": "rev:sum"}],
        )
        before = await storage.get_model("qb")
        assert before is not None
        before_cols = [c.name for c in before.columns]
        assert "rev_sum" in before_cols
        # Inner-stage `n` is not directly emitted on the outer model (the
        # outer only takes rev:sum), but it must appear in backing_query_sql
        # as the inner stage's wrap rename.
        before_sql = before.backing_query_sql or ""
        assert " AS n " in before_sql or 'AS "n"' in before_sql or before_sql.find("AS n\n") >= 0, (
            f"initial cache should expose inner 'n' alias:\n{before_sql}"
        )

        # Edit: swap n → avg_amount in stage 1; outer now sums avg_amount.
        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "qb",
            "source_queries": [
                {
                    "name": "raw",
                    "source_model": "orders",
                    "dimensions": ["region"],
                    "measures": [
                        {"formula": "amount:sum", "name": "rev"},
                        {"formula": "amount:avg", "name": "avg_amount"},
                    ],
                },
                {
                    "source_model": "raw",
                    "measures": [
                        {"formula": "rev:sum"},
                        {"formula": "avg_amount:avg"},
                    ],
                },
            ],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True, result

        after = await storage.get_model("qb")
        assert after is not None
        after_sql = after.backing_query_sql or ""
        # Stale `n` must be gone from the inner wrap.
        assert " AS n " not in after_sql and 'AS "n"' not in after_sql, (
            f"stale inner 'n' alias must be evicted:\n{after_sql}"
        )
        # New `avg_amount` must be present.
        assert "avg_amount" in after_sql, (
            f"new 'avg_amount' alias must appear in backing_query_sql:\n{after_sql}"
        )
        # Cached outer columns reflect the new outer measures.
        after_cols = [c.name for c in after.columns]
        assert "rev_sum" in after_cols
        assert "avg_amount_avg" in after_cols, (
            f"outer column for avg_amount:avg missing: {after_cols}"
        )


class TestEditModelColumnsRejected:
    """edit_model on a query-backed model must explicitly reject ``columns``
    (which are engine-managed cache) instead of silently dropping them.
    """

    async def test_columns_on_query_backed_edit_returns_error(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        from slayer.core.models import DatasourceConfig
        from slayer.core.query import SlayerQuery
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_model(SlayerModel(
            name="upstream", sql_table="t", data_source="test",
            columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
        ))
        await storage.save_model(SlayerModel(
            name="qb",
            data_source="test",
            source_queries=[SlayerQuery(
                source_model="upstream",
                measures=[{"formula": "amount:sum"}],
                dry_run=True,
            )],
        ))
        # Snapshot the model state before the bad edit.
        before = await storage.get_model("qb")
        before_columns = list(before.columns)

        result = await _call(mcp_server, name="edit_model", arguments={
            "model_name": "qb",
            "columns": [{"name": "x", "sql": "x", "type": "string"}],
        })
        # Should be a clear error, not a silent success.
        assert (
            "engine-managed" in result
            or "auto-generated" in result
            or "Cannot supply columns" in result
            or "must not be supplied" in result
        ), f"expected explicit rejection, got: {result}"
        # Stored model is unchanged.
        after = await storage.get_model("qb")
        assert [c.name for c in after.columns] == [c.name for c in before_columns]


class TestInspectModelRequiredVariables:
    """``required_variables`` must exclude placeholders that have a default at
    either ``model.query_variables`` OR a stage's own ``variables`` block.
    """

    async def test_stage_scoped_default_not_required(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        from slayer.core.models import DatasourceConfig
        from slayer.core.query import SlayerQuery
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_model(SlayerModel(
            name="upstream", sql_table="t", data_source="test",
            columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
        ))
        # Stage has its own variables={"x": 1}, so {x} placeholder is NOT
        # required from outside.
        await storage.save_model(SlayerModel(
            name="qb_stage_default",
            data_source="test",
            source_queries=[SlayerQuery(
                source_model="upstream",
                measures=[{"formula": "amount:sum"}],
                filters=["amount > {x}"],
                variables={"x": 1},
                dry_run=True,
            )],
            # No model-level query_variables.
        ))
        result = await _call(mcp_server, name="inspect_model", arguments={
            "model_name": "qb_stage_default", "format": "json",
        })
        parsed = json.loads(result)
        assert "x" not in parsed["backing_query"]["required_variables"]


class TestRunByNamePlanFlagsMCP:
    """``query(source_model="qb_model", dry_run=True)`` should return SQL
    without executing the backing query.
    """

    async def test_dry_run_run_by_name_returns_sql_without_executing(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        from slayer.core.models import DatasourceConfig
        from slayer.core.query import SlayerQuery
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_model(SlayerModel(
            name="upstream", sql_table="t", data_source="test",
            columns=[Column(name="amount", sql="amount", type=DataType.DOUBLE)],
        ))
        await storage.save_model(SlayerModel(
            name="qb_dr",
            data_source="test",
            source_queries=[SlayerQuery(
                source_model="upstream",
                measures=[{"formula": "amount:sum"}],
                # NOTE: no dry_run on the stage; only the caller asks.
            )],
        ))
        from slayer.sql.client import SlayerSQLClient
        execute_calls = 0
        real_execute = SlayerSQLClient.execute

        async def counting_execute(self, *a, **kw):
            nonlocal execute_calls
            execute_calls += 1
            return await real_execute(self, *a, **kw)

        SlayerSQLClient.execute = counting_execute  # type: ignore[method-assign]
        try:
            result = await _call(mcp_server, name="query", arguments={
                "source_model": "qb_dr",
                "dry_run": True,
            })
        finally:
            SlayerSQLClient.execute = real_execute  # type: ignore[method-assign]
        assert "SQL:" in result
        assert "amount" in result.lower()
        assert execute_calls == 0, "dry_run=True must not execute SQL"


class TestQueryAcceptsInlineSourceModel:
    """DEV-1372: the MCP ``query`` tool must accept ``source_model`` as a
    string (model name), an inline ``ModelExtension`` dict, or an inline
    ``SlayerModel`` dict — matching ``SlayerQuery.source_model``'s native
    polymorphism. Previously typed ``str``, which forced agents to JSON-
    encode dicts and tripped name validation.
    """

    async def _setup_orders(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
            ],
        ))

    async def test_string_source_model_still_works(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await self._setup_orders(storage)
        result = await _call(mcp_server, name="query", arguments={
            "source_model": "orders",
            "measures": [{"formula": "*:count"}],
            "dry_run": True,
        })
        assert "Invalid model name" not in result
        assert "SQL:" in result
        assert "orders" in result.lower()

    async def test_inline_model_extension_dict(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await self._setup_orders(storage)
        result = await _call(mcp_server, name="query", arguments={
            "source_model": {
                "source_name": "orders",
                "columns": [
                    {"name": "double_amount", "sql": "amount * 2", "type": "DOUBLE"},
                ],
            },
            "measures": [{"formula": "double_amount:sum"}],
            "dry_run": True,
        })
        assert "Invalid model name" not in result
        assert "SQL:" in result
        # The inline column's SQL expression must surface in the generated SQL.
        assert "amount * 2" in result.lower() or "amount*2" in result.lower()

    async def test_inline_slayer_model_dict(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        # Datasource must exist for SQL-client routing, but the table need not.
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        result = await _call(mcp_server, name="query", arguments={
            "source_model": {
                "name": "ad_hoc",
                "sql_table": "things",
                "data_source": "test",
                "columns": [
                    {"name": "x", "sql": "x", "type": "DOUBLE"},
                ],
            },
            "measures": [{"formula": "x:sum"}],
            "dry_run": True,
        })
        assert "Invalid model name" not in result
        assert "SQL:" in result
        assert "things" in result.lower()


class TestQueryNested:
    """MCP ``query_nested`` tool — DAG list of stages where earlier
    entries are named sub-queries that later entries can reference via
    ``source_model: "<sibling_name>"``. Mirrors ``engine.execute(list)``
    1:1; the regular ``query`` tool stays single-stage.
    """

    async def _setup_orders(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders", data_source="test",
            columns=[
                Column(name="amount", sql="amount", type=DataType.DOUBLE),
                Column(name="status", sql="status", type=DataType.TEXT),
                Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            ],
        ))

    async def test_two_stage_dag_dry_run(self, mcp_server, storage: YAMLStorage) -> None:
        """Stage 2 references prior named sibling 'monthly' via source_model."""
        await self._setup_orders(storage)
        result = await _call(mcp_server, name="query_nested", arguments={
            "queries": [
                {
                    "name": "monthly",
                    "source_model": "orders",
                    "measures": [{"formula": "amount:sum"}],
                    "time_dimensions": [
                        {"dimension": "created_at", "granularity": "month"},
                    ],
                },
                {
                    "source_model": "monthly",
                    "measures": [{"formula": "amount_sum:avg"}],
                },
            ],
            "dry_run": True,
        })
        assert "SQL:" in result
        # The earlier stage emits a CTE / sub-select named after its alias,
        # and the outer stage's AVG aggregation must surface.
        assert "AVG(" in result.upper()

    async def test_empty_list_rejected(self, mcp_server, storage: YAMLStorage) -> None:
        from mcp.server.fastmcp.exceptions import ToolError
        await self._setup_orders(storage)
        with pytest.raises(ToolError, match="non-empty list"):
            await _call(mcp_server, name="query_nested", arguments={
                "queries": [],
                "dry_run": True,
            })

    async def test_out_of_order_dag_works(self, mcp_server, storage: YAMLStorage) -> None:
        """Caller submits stages in non-topological order — engine auto-sorts.

        Order here: 'a' references 'b' (forward in the input list), but
        'b' has no deps. Topo-sort produces [b, a, final], and the SQL
        emits cleanly.
        """
        await self._setup_orders(storage)
        result = await _call(mcp_server, name="query_nested", arguments={
            "queries": [
                {
                    "name": "a",
                    "source_model": "b",
                    "measures": [{"formula": "amount_sum:avg"}],
                },
                {
                    "name": "b",
                    "source_model": "orders",
                    "measures": [{"formula": "amount:sum"}],
                },
                {
                    "source_model": "a",
                    "measures": [{"formula": "amount_sum_avg:max"}],
                },
            ],
            "dry_run": True,
        })
        assert "SQL:" in result
        assert "MAX(" in result.upper()

    async def test_cycle_raises(self, mcp_server, storage: YAMLStorage) -> None:
        """A cycle between stages must surface a clear error naming the cycle members."""
        from mcp.server.fastmcp.exceptions import ToolError
        await self._setup_orders(storage)
        with pytest.raises(ToolError, match=r"[Cc]ycle"):
            await _call(mcp_server, name="query_nested", arguments={
                "queries": [
                    {"name": "a", "source_model": "b", "measures": [{"formula": "amount:sum"}]},
                    {"name": "b", "source_model": "a", "measures": [{"formula": "amount:sum"}]},
                    {"source_model": "orders", "measures": [{"formula": "amount:sum"}]},
                ],
                "dry_run": True,
            })

    async def test_invalid_format_rejected(self, mcp_server, storage: YAMLStorage) -> None:
        from mcp.server.fastmcp.exceptions import ToolError
        await self._setup_orders(storage)
        with pytest.raises(ToolError, match="Invalid format"):
            await _call(mcp_server, name="query_nested", arguments={
                "queries": [
                    {"source_model": "orders", "measures": [{"formula": "*:count"}]},
                ],
                "format": "xml",
            })


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


# DEV-1658: the `help` MCP tool was removed. Conceptual help now ships as
# seeded `memory:help.*` memories read via `inspect(entity_type="memory")`;
# see tests/test_help_seed.py.


