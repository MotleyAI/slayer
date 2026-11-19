"""Tests for the MCP server tools."""

import json
import os
import shutil
import tempfile
from typing import Any
from collections.abc import AsyncIterator, Generator

import pytest
from mcp.server.fastmcp.exceptions import ToolError
from mcp.shared.memory import create_connected_server_and_client_session
from mcp.types import TextContent
from sqlalchemy import exc as sa_exc

import slayer.live_schema.ingestion as ingestion_mod
from slayer.core.enums import DataType
from slayer.core.query import SlayerQuery
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.live_schema.ingestion import _is_id_field
from slayer.database.client import DatabaseUnavailable, ResultColumnType, SlayerSQLClient
from slayer.storage.sqlite_conn import transaction
from slayer.inspect.dataset_render import (
    _choose_sample_agg,
    render_dataset_inspection,
)
from slayer.mcp.server import (
    _build_sample_query_args,
    _escape_md_cell,
    _format_table,
    _friendly_db_error,
    _markdown_table,
    _md_code_span,
    _render_inspect_footer,
    _resolve_inspect_sections,
    _strip_dataset_prefix,
    _truncate_description,
    create_mcp_server,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage
from tests._usage_reporting_helpers import free_port


# create_mcp_server is expensive, so share it (and storage) session-wide and
# reset the YAML files per-test.
@pytest.fixture(scope="session")
def _shared_storage() -> Generator[YAMLStorage, None, None]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


@pytest.fixture(scope="session")
def _shared_mcp_server(_shared_storage: YAMLStorage):
    return create_mcp_server(storage=_shared_storage)


def _reset_yaml_storage(storage: YAMLStorage) -> None:
    """Wipe model/datasource files (recursively) plus ``priority.yaml`` so the
    session-scoped storage looks fresh; priority leakage makes tests order-dependent."""
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
async def mcp_server(_shared_mcp_server, storage: YAMLStorage) -> AsyncIterator[Any]:
    # Depend on `storage` so the per-test reset runs first.
    yield _shared_mcp_server
    await _shared_mcp_server._slayer_engine.aclose()


async def _dry_run_sql(storage: YAMLStorage, name: str) -> str:
    """The SQL a dry run of the saved model ``name`` renders."""
    engine = SlayerQueryEngine(storage=storage)
    try:
        sql = (await engine.execute(name, dry_run=True)).sql
    finally:
        engine.close()
    assert sql is not None
    return sql


async def _call(mcp_server, *, name: str, arguments: dict[str, Any] | None = None) -> str:
    """Call an MCP tool and return the text result."""
    content_blocks, result_dict = await mcp_server.call_tool(name=name, arguments=arguments or {})
    return content_blocks[0].text


class TestModelsSummary:
    async def test_datasource_not_found(self, mcp_server) -> None:
        result = await _call(mcp_server, name="datasets_summary", arguments={"datasource_name": "nope"})
        assert "not found" in result

    async def test_only_the_spine_when_datasource_has_no_models(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="h"))
        result = await _call(mcp_server, name="datasets_summary", arguments={"datasource_name": "mydb"})
        assert result.startswith("# Datasource: `mydb` — 1 dataset(s)")
        assert "## `time_spine`" in result

    async def test_filters_by_datasource(self, mcp_server, storage: YAMLStorage) -> None:
        """Only models whose ``data_source`` matches the arg appear in the output."""
        await storage.save_datasource(DatasourceConfig(name="ds_a", type="postgres", host="h"))
        await storage.save_datasource(DatasourceConfig(name="ds_b", type="postgres", host="h"))
        await storage.save_dataset(SlayerDataset(name="orders_a", sql_table="t", data_source="ds_a"))
        await storage.save_dataset(SlayerDataset(name="orders_b", sql_table="t", data_source="ds_b"))
        result = await _call(mcp_server, name="datasets_summary", arguments={"datasource_name": "ds_a"})
        assert "orders_a" in result
        assert "orders_b" not in result

    async def test_markdown_structure(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="h"))
        await storage.save_dataset(SlayerDataset(
            name="orders",
            sql_table="t",
            data_source="mydb",
            description="Orders fact table.",
            fields=[DatasetField(name="status", datatype=DataType.STRING, description="Order state"),
DatasetField(name="revenue", sql="amount", description="USD", datatype=DataType.FLOAT),
                     DatasetField(name="customer_id", datatype=DataType.INTEGER, hidden=True),
            ],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        ))
        # compact=False: compact-by-default drops the per-column table under test.
        result = await _call(mcp_server, name="datasets_summary", arguments={
            "datasource_name": "mydb", "compact": False,
        })
        assert result.startswith("# Datasource: `mydb` — 2 dataset(s)")
        assert "## `orders`" in result
        assert "Orders fact table." in result
        assert "**Fields (2):**" in result
        assert "| status |" in result
        assert "Order state" in result
        assert "| revenue |" in result
        assert "USD" in result
        assert "**Relationships to:** `customers`" in result

    async def test_hidden_models_excluded(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="h"))
        await storage.save_dataset(SlayerDataset(name="visible", sql_table="t", data_source="mydb"))
        await storage.save_dataset(SlayerDataset(name="hidden_m", sql_table="t", data_source="mydb", hidden=True))
        result = await _call(mcp_server, name="datasets_summary", arguments={"datasource_name": "mydb"})
        assert "visible" in result
        assert "hidden_m" not in result

    async def test_joins_none_marker(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="h"))
        await storage.save_dataset(SlayerDataset(name="solo", sql_table="t", data_source="mydb"))
        result = await _call(mcp_server, name="datasets_summary", arguments={
            "datasource_name": "mydb", "compact": False,
        })
        assert "**Relationships to:** _(none)_" in result

    async def test_columns_table_includes_type(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """v2 columns table always shows the type column, even without descriptions."""
        await storage.save_datasource(DatasourceConfig(name="mydb", type="postgres", host="h"))
        await storage.save_dataset(SlayerDataset(
            name="m", sql_table="t", data_source="mydb",
            fields=[
                DatasetField(name="x", datatype=DataType.STRING),
                DatasetField(name="y", datatype=DataType.FLOAT),
            ],
        ))
        result = await _call(mcp_server, name="datasets_summary", arguments={
            "datasource_name": "mydb", "compact": False,
        })
        col_section = result.split("**Fields")[1].split("**Metrics")[0]
        assert "| x | String |" in col_section
        assert "| y | Float |" in col_section


class TestInspectModel:
    async def test_not_found(self, mcp_server) -> None:
        result = await _call(mcp_server, name="inspect", arguments={"entity_type": "dataset", "reference": "nonexistent", "compact": False})
        assert "does not resolve to a dataset" in result

    async def test_markdown_structure(self, mcp_server, storage: YAMLStorage) -> None:
        """Every expected section header appears and the response is no longer JSON."""
        await storage.save_dataset(SlayerDataset(
            name="test",
            sql_table="public.t",
            data_source="test",
            description="A test model used in unit tests.",
            fields=[
                DatasetField(name="status", datatype=DataType.STRING, label="Status", description="Order state"),
                DatasetField(name="id", datatype=DataType.FLOAT, primary_key=True),
DatasetField(name="revenue", sql="amount", label="Revenue", description="USD total", datatype=DataType.FLOAT),
                DatasetField(name="customer_id", datatype=DataType.INTEGER, hidden=True),
            ],
            filters=["deleted_at IS NULL"],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        ))
        result = await _call(mcp_server, name="inspect", arguments={"entity_type": "dataset", "reference": "test", "compact": False, "show_sql": True})

        assert result.startswith("# Dataset: `test`")
        assert "A test model used in unit tests." in result
        assert "**data_source:** `test`" in result
        assert "**sql_table:** `public.t`" in result
        assert "## Filters (dataset-level)" in result
        assert "`deleted_at IS NULL`" in result
        assert "## Fields (3)" in result
        assert "| status |" in result
        assert "Order state" in result
        assert "Revenue" in result
        assert "USD total" in result
        assert "## Relationships (1)" in result
        assert "customers" in result
        assert "customer_id = id" in result

        with pytest.raises(json.JSONDecodeError):
            json.loads(result)

    async def test_custom_sql_section_only_when_sql_is_set(self, mcp_server, storage: YAMLStorage) -> None:
        """## SQL fenced block appears only when model.sql is populated."""
        await storage.save_dataset(SlayerDataset(name="plain", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="inspect", arguments={"entity_type": "dataset", "reference": "plain", "compact": False})
        assert "## SQL" not in result

        await storage.save_dataset(SlayerDataset(
            name="querybacked", sql="SELECT 1 AS x", data_source="test",
        ))
        result = await _call(mcp_server, name="inspect", arguments={"entity_type": "dataset", "reference": "querybacked", "compact": False, "show_sql": True})
        assert "## SQL" in result
        assert "```sql" in result
        assert "SELECT 1 AS x" in result

    async def test_measure_filter_and_aggregations(self, mcp_server, storage: YAMLStorage) -> None:
        """Measure.filter and custom Aggregation entries both surface."""
        await storage.save_dataset(SlayerDataset(
            name="t", sql_table="t", data_source="test",
            fields=[DatasetField(
                name="completed_rev", sql="amount",
                filter="status = 'completed'",
                allowed_aggregations=["sum", "avg"], datatype=DataType.FLOAT)],
            aggregations=[Aggregation(
                name="wavg",
                expression="SUM({sql} * {weight}) / NULLIF(SUM({weight}), 0)",
                description="Weighted average",
            )],
        ))
        result = await _call(mcp_server, name="inspect", arguments={"entity_type": "dataset", "reference": "t", "compact": False, "show_sql": True})
        assert "status = 'completed'" in result  # measure filter surfaces
        assert "sum, avg" in result              # allowed_aggregations rendered
        assert "## Aggregations (1)" in result
        assert "wavg" in result
        assert "Weighted average" in result

    async def test_joins_table_rendered(self, mcp_server, storage: YAMLStorage) -> None:
        """Joins table renders direct joins without kind labels."""
        await storage.save_dataset(SlayerDataset(
            name="order_items", sql_table="order_items", data_source="test",
            fields=[
                DatasetField(name="order_id", datatype=DataType.INTEGER, hidden=True),
                DatasetField(name="product_id", datatype=DataType.INTEGER, hidden=True),
            ],
            relationships=[
                DatasetRelationship(to="orders", from_columns=["order_id"], to_columns=["id"]),
                DatasetRelationship(to="products", from_columns=["product_id"], to_columns=["id"]),
            ],
        ))
        result = await _call(mcp_server, name="inspect", arguments={"entity_type": "dataset", "reference": "order_items", "compact": False})
        assert "| orders |" in result
        assert "| products |" in result
        assert "kind" not in result

    # --- meta rendering ---

    async def test_inspect_renders_meta_on_columns(self, mcp_server, storage: YAMLStorage) -> None:
        """Column.meta surfaces in both markdown and JSON model inspect output."""
        await storage.save_dataset(SlayerDataset(
            name="m", sql_table="t", data_source="test",
            fields=[DatasetField(name="amount", datatype=DataType.FLOAT, meta={"kb_id": 7})],
        ))
        md = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "m", "compact": False, "sections": ["fields"],
        })
        assert "kb_id" in md
        assert "7" in md
        js = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "m", "compact": False, "sections": ["fields"], "format": "json",
        })
        payload = json.loads(js)
        assert payload["fields"][0]["meta"] == {"kb_id": 7}

    async def test_inspect_renders_meta_on_measures(self, mcp_server, storage: YAMLStorage) -> None:
        """ModelMeasure.meta surfaces in both markdown and JSON model inspect output."""
        await storage.save_dataset(SlayerDataset(
            name="m", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", datatype=DataType.FLOAT)],
            metrics=[DatasetMetric(
                name="aov", expression="sum(revenue) / count(*)",
                meta={"kb_id": "abc-123"},
            )],
        ))
        md = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "m", "compact": False, "sections": ["metrics"],
        })
        assert "kb_id" in md
        assert "abc-123" in md
        js = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "m", "compact": False, "sections": ["metrics"], "format": "json",
        })
        payload = json.loads(js)
        assert payload["metrics"][0]["meta"] == {"kb_id": "abc-123"}

    async def test_inspect_renders_meta_on_aggregations(self, mcp_server, storage: YAMLStorage) -> None:
        """Aggregation.meta surfaces in both markdown and JSON model inspect output."""
        await storage.save_dataset(SlayerDataset(
            name="m", sql_table="t", data_source="test",
            aggregations=[Aggregation(
                name="trimmed_mean",
                expression="AVG(CASE WHEN {expr} BETWEEN {low} AND {high} THEN {expr} END)",
                meta={"owner": "analytics"},
            )],
        ))
        md = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "m", "compact": False, "sections": ["aggregations"], "show_sql": True,
        })
        assert "owner" in md
        assert "analytics" in md
        js = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "m", "compact": False, "sections": ["aggregations"], "format": "json",
        })
        payload = json.loads(js)
        assert payload["aggregations"][0]["meta"] == {"owner": "analytics"}

    async def test_inspect_renders_meta_on_model_header(self, mcp_server, storage: YAMLStorage) -> None:
        """SlayerModel.meta surfaces in the markdown header and the JSON payload."""
        await storage.save_dataset(SlayerDataset(
            name="m", sql_table="t", data_source="test",
            meta={"source": "CRM"},
        ))
        md = await _call(mcp_server, name="inspect", arguments={"entity_type": "dataset", "reference": "m", "compact": False})
        assert "**meta:**" in md
        assert "source" in md
        assert "CRM" in md
        js = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "m", "compact": False, "format": "json",
        })
        payload = json.loads(js)
        assert payload["meta"] == {"source": "CRM"}

    async def test_inspect_omits_meta_column_when_no_entity_has_meta(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """No entity with meta ⇒ the meta column is pruned from every table."""
        await storage.save_dataset(SlayerDataset(
            name="m", sql_table="t", data_source="test",
            fields=[DatasetField(name="amount", datatype=DataType.FLOAT)],
            metrics=[DatasetMetric(name="aov", expression="sum(amount)")],
            aggregations=[Aggregation(name="my_agg", expression="SUM({expr})")],
        ))
        md = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "m", "compact": False, "sections": ["fields", "metrics", "aggregations"],
            "show_sql": True,
        })
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
        """Inferred types drive the sample-query aggregations in _build_sample_query_args."""
        model = SlayerDataset(
            name="typed",
            sql_table="t",
            data_source="test",
            fields=[DatasetField(name="status", datatype=DataType.STRING),

                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="label", sql="label", datatype=DataType.FLOAT),
            ],
        )
        # Without types, label falls back to avg (no dim to trigger the heuristic).
        args_no_types = _build_sample_query_args(dataset=model, num_rows=3)
        formulas = [f["metric"] for f in args_no_types["metrics"]]
        assert "avg(label)" in formulas

        args_with_types = _build_sample_query_args(
            dataset=model, num_rows=3,
            metric_types={"amount": ResultColumnType(type=DataType.FLOAT), "label": ResultColumnType(type=DataType.STRING)},
        )
        formulas = [f["metric"] for f in args_with_types["metrics"]]
        assert "avg(amount)" in formulas
        assert "count_distinct(label)" in formulas


class TestInspectModelQueryBacked:
    """Model inspect for query-backed models: backing_query, source_type, SQL."""
    async def _setup(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_dataset(SlayerDataset(
            name="upstream", sql_table="t", data_source="test",
            fields=[
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
            ],
        ))
        # engine.save_model warms the cache (columns + SQL); read paths don't.
        engine = SlayerQueryEngine(storage=storage)
        await engine.save_dataset(SlayerDataset(
            name="qb",
            data_source="test",
            source_queries=[SlayerQuery(
                population="upstream",
                metrics=[{"metric": "sum(amount)"}],
                dimensions=["region"],
                filter=["amount > {threshold}"],
                dry_run=True,
            )],
            query_variables={"threshold": 100},
        ))

    async def test_json_includes_backing_query(self, mcp_server, storage: YAMLStorage) -> None:
        await self._setup(storage)
        result = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "qb", "compact": False, "format": "json",
        })
        parsed = json.loads(result)
        assert parsed["source_type"] == "query"
        assert "backing_query" in parsed
        bq = parsed["backing_query"]
        assert bq["variables"] == {"threshold": 100}
        assert bq["required_variables"] == []  # threshold has a default
        assert len(bq["stages"]) == 1
        assert "backing_query_sql" not in parsed

    async def test_json_show_sql_omits_backing_query_sql(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        """No backing SQL even with ``show_sql=True``; a dry run renders it."""
        await self._setup(storage)
        result = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "qb", "compact": False, "format": "json", "show_sql": True,
        })
        assert "backing_query_sql" not in json.loads(result)

    async def test_required_variables_reported(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_dataset(SlayerDataset(
            name="upstream", sql_table="t", data_source="test",
            fields=[DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT)],
        ))
        await storage.save_dataset(SlayerDataset(
            name="qb_missing_default",
            data_source="test",
            source_queries=[SlayerQuery(
                population="upstream",
                metrics=[{"metric": "sum(amount)"}],
                filter=["amount > {threshold}"],
                dry_run=True,
            )],
            # No query_variables → 'threshold' is required
        ))
        result = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "qb_missing_default", "compact": False, "format": "json",
        })
        parsed = json.loads(result)
        assert "threshold" in parsed["backing_query"]["required_variables"]

    async def test_table_backed_model_has_no_backing_query(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await storage.save_dataset(SlayerDataset(
            name="plain", sql_table="t", data_source="test",
            fields=[DatasetField(name="x", sql="x", datatype=DataType.STRING)],
        ))
        result = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "plain", "compact": False, "format": "json",
        })
        parsed = json.loads(result)
        assert parsed["source_type"] == "table"
        assert "backing_query" not in parsed

    async def test_markdown_includes_backing_query_section(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await self._setup(storage)
        result = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "qb", "compact": False, "format": "markdown",
        })
        assert "## Backing Query" in result
        assert "threshold" in result


class TestInspectModelJsonFormat:
    async def test_json_format_includes_sample_data(self, mcp_server, storage: YAMLStorage) -> None:
        """Model inspect with format='json' must include sample_data and sample_data_error keys."""
        await storage.save_dataset(SlayerDataset(
            name="jtest",
            sql_table="t",
            data_source="test",
            fields=[DatasetField(name="x"),
DatasetField(name="m", sql="val", datatype=DataType.FLOAT)
            ],
        ))
        result = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "jtest", "compact": False, "format": "json",
        })
        parsed = json.loads(result)
        assert "sample_data" in parsed
        assert "sample_data_error" in parsed
        assert parsed["dataset_name"] == "jtest"
        # sample_sql should NOT appear when show_sql is not requested
        assert "sample_sql" not in parsed
        # A complete profile must advertise itself as not reduced, so a JSON
        # caller can distinguish it from the count-only fallback.
        assert parsed["sample_data_reduced"] is False
        assert parsed["sample_data_reduced_reason"] is None


class TestInspectModelShowSQL:
    """show_sql parameter must control visibility of all SQL in model inspect output."""
    async def test_hides_sql_by_default_markdown(self, mcp_server, storage: YAMLStorage) -> None:
        """Without show_sql, markdown output has no ## SQL section and no sql column."""
        await storage.save_dataset(SlayerDataset(
            name="sqlt",
            sql="SELECT id, val FROM raw_table",
            data_source="test",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
DatasetField(name="val", sql="val", datatype=DataType.FLOAT)
            ],
        ))
        result = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "sqlt", "compact": False, "num_rows": 0,
        })
        assert "## SQL" not in result
        # Dimension table should not have an "sql" column header
        assert "| sql " not in result
        assert "| sql|" not in result

    async def test_shows_sql_when_requested_markdown(self, mcp_server, storage: YAMLStorage) -> None:
        """With show_sql=True, markdown output includes ## SQL section and sql columns."""
        await storage.save_dataset(SlayerDataset(
            name="sqlshow",
            sql="SELECT id, val FROM raw_table",
            data_source="test",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
DatasetField(name="val", sql="val", datatype=DataType.FLOAT)
            ],
        ))
        result = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "sqlshow", "compact": False, "num_rows": 0, "show_sql": True,
        })
        assert "## SQL" in result
        assert "SELECT id, val FROM raw_table" in result

    async def test_hides_sql_by_default_json(self, mcp_server, storage: YAMLStorage) -> None:
        """JSON format without show_sql excludes sql keys from dimensions and measures."""
        await storage.save_dataset(SlayerDataset(
            name="jsqlt",
            sql="SELECT id, val FROM raw_table",
            sql_table=None,
            data_source="test",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
DatasetField(name="val", sql="val", filter="val > 0", datatype=DataType.FLOAT)
            ],
        ))
        result = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "jsqlt", "compact": False, "format": "json", "num_rows": 0,
        })
        parsed = json.loads(result)
        # Model-level sql and sql_table should be absent
        assert "sql" not in parsed
        assert "sql_table" not in parsed
        # Column dicts should not have sql/filter keys when show_sql=False
        for c in parsed["fields"]:
            assert "sql" not in c
            assert "filter" not in c

    async def test_shows_sql_when_requested_json(self, mcp_server, storage: YAMLStorage) -> None:
        """JSON format with show_sql=True includes sql keys everywhere."""
        await storage.save_dataset(SlayerDataset(
            name="jsqlshow",
            sql="SELECT id, val FROM raw_table",
            sql_table=None,
            data_source="test",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
DatasetField(name="val", sql="val", filter="val > 0", datatype=DataType.FLOAT)
            ],
        ))
        result = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "jsqlshow", "compact": False, "format": "json", "num_rows": 0, "show_sql": True,
        })
        parsed = json.loads(result)
        assert "sql" in parsed
        assert parsed["sql"] == "SELECT id, val FROM raw_table"
        for c in parsed["fields"]:
            assert "sql" in c
            assert "filter" in c


class TestInspectModelSectionGating:
    """``sections`` parameter on the model inspect — markdown rendering."""
    async def _save_rich_dataset(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
        await storage.save_dataset(SlayerDataset(
            name="rich", sql_table="t", data_source="test",
            description="A rich model used to exercise model inspect section gating.",
            fields=[
                DatasetField(name="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", datatype=DataType.STRING, description="Order state"),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="customer_id", datatype=DataType.INTEGER, hidden=True),
                DatasetField(name="product_id", datatype=DataType.INTEGER, hidden=True),
            ],
            metrics=[
                DatasetMetric(name="aov", expression="sum(amount) / count(*)", description="Average order value"),
                DatasetMetric(name="rev", expression="sum(amount)"),
            ],
            aggregations=[
                Aggregation(
                    name="wavg",
                    expression="SUM({sql} * {weight}) / NULLIF(SUM({weight}), 0)",
                    description="Weighted average",
                ),
            ],
            relationships=[
                DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"]),
                DatasetRelationship(to="products", from_columns=["product_id"], to_columns=["id"]),
            ],
        ))

    async def test_default_renders_all_sections_no_footer(self, mcp_server, storage: YAMLStorage) -> None:
        """Default call (no `sections=`) emits all sections and no footer."""
        await self._save_rich_dataset(storage)
        result = await _call(mcp_server, name="inspect", arguments={"entity_type": "dataset", "reference": "rich", "compact": False})
        assert "## Fields (3)" in result
        assert "## Metrics (2)" in result
        assert "## Relationships (2)" in result
        # Default does not opt into show_sql, so aggregations table appears with name/params/description
        assert "## Aggregations (1)" in result
        # The footer only renders when something was trimmed/unknown
        assert "Sections shown:" not in result
        assert "Names-only:" not in result
        assert "Omitted:" not in result

    async def test_columns_only_collapses_others(self, mcp_server, storage: YAMLStorage) -> None:
        """sections=['columns'] keeps columns full; other parts collapse to names-only or vanish."""
        await self._save_rich_dataset(storage)
        result = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "rich", "compact": False, "sections": ["fields"]},
        )
        # Columns full
        assert "## Fields (3)" in result
        # Measures, Aggregations, Joins → names-only (CSV under heading)
        assert "## Metrics (2 — names only)" in result
        assert "`aov`, `rev`" in result
        assert "## Aggregations (1 — names only)" in result
        assert "`wavg`" in result
        assert "## Relationships (2 — names only)" in result
        assert "`customers`, `products`" in result
        # Reachable fields and samples fully omitted
        assert "## Reachable" not in result
        assert "## Data Profile" not in result
        # Footer present
        assert "> Sections shown: fields." in result
        assert "> Names-only: metrics, aggregations, relationships." in result
        assert "> Omitted: samples, learnings, saved_queries." in result
        assert "Re-call inspect with" in result

    async def test_omitted_sections_with_no_entities_render_nothing(self, mcp_server, storage: YAMLStorage) -> None:
        """A names-only section with no entities does not emit a heading."""
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
        await storage.save_dataset(SlayerDataset(
            name="bare", sql_table="t", data_source="test",
            fields=[DatasetField(name="id", datatype=DataType.FLOAT, primary_key=True)],
        ))
        result = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "bare", "compact": False, "sections": ["fields"]},
        )
        assert "## Fields" in result
        # No measures, aggregations, or joins → no headings for them at all
        assert "## Metrics" not in result
        assert "## Aggregations" not in result
        assert "## Relationships" not in result

    async def test_unknown_section_emits_warning(self, mcp_server, storage: YAMLStorage) -> None:
        """An unknown section name produces a warning line in the footer."""
        await self._save_rich_dataset(storage)
        result = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "rich", "compact": False, "sections": ["fields", "fish"]},
        )
        assert "> Warning: ignored unknown sections: 'fish'." in result
        assert "Valid: fields, metrics, aggregations, relationships, samples, learnings, saved_queries." in result
        # Valid section still rendered
        assert "## Fields (3)" in result

    async def test_all_unknown_resolves_to_no_sections(self, mcp_server, storage: YAMLStorage) -> None:
        """All-unknown sections ⇒ none selected (None/[] alone means "all six")."""
        await self._save_rich_dataset(storage)
        result = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "rich", "compact": False, "sections": ["fish", "bird"]},
        )
        assert "Warning: ignored unknown sections: 'fish', 'bird'." in result
        # Always-on header still renders
        assert "# Dataset: `rich`" in result
        # Names-only collapse for the four gateable sections — no full tables
        assert "## Fields (3 — names only)" in result
        assert "## Metrics (2 — names only)" in result
        assert "## Relationships (2 — names only)" in result
        # The full-table heading must NOT appear
        assert "## Fields (3)\n\n|" not in result
        # samples and learnings are fully omitted (no heading at all)
        assert "## Sample" not in result
        # Reachable-via-joins section was removed entirely — it
        # must never appear regardless of input.
        assert "## Reachable" not in result
        # Footer summarises what was dropped — caller can re-call with a
        # corrected sections= list.
        assert "Sections shown: (none)" in result
        assert "Names-only: fields, metrics, aggregations, relationships" in result
        assert "Omitted: samples, learnings, saved_queries" in result
        assert "Re-call inspect with `sections=[...]`" in result

    async def test_canonical_order_regardless_of_input(self, mcp_server, storage: YAMLStorage) -> None:
        """Sections render in canonical order regardless of caller order."""
        await self._save_rich_dataset(storage)
        result = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "rich", "compact": False, "sections": ["metrics", "fields"]},
        )
        assert result.find("## Fields (3)") < result.find("## Metrics (2)")

    # ---- reachable_fields surface fully removed ----

    async def test_old_reachable_fields_token_is_unknown(self, mcp_server, storage: YAMLStorage) -> None:
        """Removed ``reachable_fields`` token flows through the unknown-section
        branch: names-only collapse, no ## Reachable heading, no JSON reachable_* keys."""
        await self._save_rich_dataset(storage)
        result_md = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "rich", "compact": False, "sections": ["reachable_fields"]},
        )
        assert (
            "> Warning: ignored unknown sections: 'reachable_fields'. "
            "Valid: fields, metrics, aggregations, relationships, samples, learnings, saved_queries."
        ) in result_md
        assert "## Reachable" not in result_md
        # Unknown-branch wins over a silent-drop fallback to the full Columns table.
        assert "## Fields (3)\n\n|" not in result_md
        assert "## Fields (3 — names only)" in result_md
        assert "> Sections shown: (none)" in result_md
        assert "> Names-only: fields, metrics, aggregations, relationships." in result_md
        assert "> Omitted: samples, learnings, saved_queries." in result_md

        result_json = await _call(
            mcp_server, name="inspect",
            arguments={
                "entity_type": "dataset", "reference": "rich", "compact": False,
                "sections": ["reachable_fields"],
                "format": "json",
            },
        )
        parsed = json.loads(result_json)
        assert parsed["unknown_sections"] == ["reachable_fields"]
        assert "reachable_dimensions" not in parsed
        assert "reachable_measures" not in parsed
        assert "fields" not in parsed  # full key absent ⇒ unknown-branch not bypassed
        assert parsed["names_only_sections"] == ["fields", "metrics", "aggregations", "relationships"]
        assert parsed["omitted_sections"] == ["samples", "learnings", "saved_queries"]

    async def test_old_reachable_fields_token_with_other_sections(self, mcp_server, storage: YAMLStorage) -> None:
        """A mix of ``reachable_fields`` + ``columns`` renders columns fully and
        still warns about the unknown token, with no reachable-via-joins output."""
        await self._save_rich_dataset(storage)
        result_md = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "rich", "compact": False, "sections": ["reachable_fields", "fields"]},
        )
        # Columns table renders in full.
        assert "## Fields (3)" in result_md
        # Footer warning calls out the bad token by repr.
        assert "> Warning: ignored unknown sections: 'reachable_fields'." in result_md
        assert "## Reachable" not in result_md

        result_json = await _call(
            mcp_server, name="inspect",
            arguments={
                "entity_type": "dataset", "reference": "rich", "compact": False,
                "sections": ["reachable_fields", "fields"],
                "format": "json",
            },
        )
        parsed = json.loads(result_json)
        assert "fields" in parsed
        assert parsed["unknown_sections"] == ["reachable_fields"]
        assert "reachable_dimensions" not in parsed
        assert "reachable_measures" not in parsed

    async def test_reachable_fields_depth_kwarg_has_no_effect(self, mcp_server, storage: YAMLStorage) -> None:
        """The removed ``reachable_fields_depth`` kwarg is silently dropped and has
        no effect: output is byte-identical to the call without it, no ## Reachable."""
        await self._save_rich_dataset(storage)
        result_with = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "rich", "compact": False, "reachable_fields_depth": 5},
        )
        result_without = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "rich", "compact": False},
        )
        assert result_with == result_without
        assert "## Reachable" not in result_with
        assert "Reachable via joins" not in result_with

    async def test_no_reachable_via_joins_heading_anywhere(self, mcp_server, storage: YAMLStorage) -> None:
        """Invariant: ## Reachable via joins and reachable_* JSON keys never appear,
        for both the default and explicit full-section-list paths."""
        await self._save_rich_dataset(storage)
        for sections_arg in (
            None,
            ["fields", "metrics", "aggregations", "relationships", "samples", "learnings"],
        ):
            args: dict = {"entity_type": "dataset", "reference": "rich", "compact": False}
            if sections_arg is not None:
                args["sections"] = sections_arg
            md = await _call(mcp_server, name="inspect", arguments=args)
            assert "## Reachable" not in md, sections_arg
            assert "Reachable via joins" not in md, sections_arg

            args_json = dict(args, format="json")
            raw = await _call(mcp_server, name="inspect", arguments=args_json)
            parsed = json.loads(raw)
            assert "reachable_dimensions" not in parsed, sections_arg
            assert "reachable_measures" not in parsed, sections_arg


class TestInspectModelDescriptionsMaxChars:
    """``descriptions_max_chars`` parameter — truncation behaviour."""
    async def _save_with_long_descriptions(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
        await storage.save_dataset(SlayerDataset(
            name="m", sql_table="t", data_source="test",
            description="A" * 100,
            fields=[DatasetField(name="status", datatype=DataType.STRING, description="B" * 100)],
            metrics=[DatasetMetric(name="rev", expression="count(*)", description="C" * 100)],
            aggregations=[Aggregation(
                name="wavg", expression="SUM({x})", description="D" * 100,
            )],
        ))

    async def test_no_truncation_by_default(self, mcp_server, storage: YAMLStorage) -> None:
        """descriptions_max_chars=None leaves descriptions untouched."""
        await self._save_with_long_descriptions(storage)
        result = await _call(mcp_server, name="inspect", arguments={"entity_type": "dataset", "reference": "m", "compact": False})
        assert "A" * 100 in result
        assert "B" * 100 in result
        assert "[truncated]" not in result

    async def test_truncates_descriptions_when_set(self, mcp_server, storage: YAMLStorage) -> None:
        """All four description fields are truncated to max_chars + marker."""
        await self._save_with_long_descriptions(storage)
        result = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "m", "compact": False, "descriptions_max_chars": 10, "show_sql": True},
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
        await storage.save_dataset(SlayerDataset(
            name="short", sql_table="t", data_source="test",
            description="hi",
            fields=[DatasetField(name="x", datatype=DataType.STRING, description="ok")],
        ))
        result = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "short", "compact": False, "descriptions_max_chars": 100},
        )
        assert "hi" in result
        assert "ok" in result
        assert "[truncated]" not in result

    async def test_zero_yields_marker_only(self, mcp_server, storage: YAMLStorage) -> None:
        """descriptions_max_chars=0 truncates every non-empty description to just the marker."""
        await self._save_with_long_descriptions(storage)
        result = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "m", "compact": False, "descriptions_max_chars": 0},
        )
        assert "[truncated]" in result
        assert "A" * 5 not in result

    async def test_negative_rejected(self, mcp_server, storage: YAMLStorage) -> None:
        """Negative max_chars is rejected (would slice ``str[:-N]``, not first-N)."""
        await self._save_with_long_descriptions(storage)
        with pytest.raises(ToolError, match="descriptions_max_chars must be >= 0"):
            await _call(
                mcp_server, name="inspect",
                arguments={"entity_type": "dataset", "reference": "m", "compact": False, "descriptions_max_chars": -1},
            )


class TestInspectModelAggregationsShowSql:
    """show_sql gating on the aggregations section."""
    async def _save_with_aggs(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
        await storage.save_dataset(SlayerDataset(
            name="m", sql_table="t", data_source="test",
            aggregations=[Aggregation(
                name="wavg",
                expression="SUM({sql} * {weight}) / NULLIF(SUM({weight}), 0)",
                params=[AggregationParam(name="weight", sql="quantity")],
                description="Weighted avg",
            )],
        ))

    async def test_aggregations_full_with_show_sql_true(self, mcp_server, storage: YAMLStorage) -> None:
        """show_sql=True keeps formula and full params."""
        await self._save_with_aggs(storage)
        result = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "m", "compact": False, "sections": ["aggregations"], "show_sql": True},
        )
        assert "## Aggregations (1)" in result
        assert "SUM({sql} * {weight})" in result
        assert "weight=quantity" in result

    async def test_aggregations_show_sql_false_drops_formula_and_param_sql(self, mcp_server, storage: YAMLStorage) -> None:
        """show_sql=False drops formula column and the param SQL (param names only)."""
        await self._save_with_aggs(storage)
        result = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "m", "compact": False, "sections": ["aggregations"], "show_sql": False},
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
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "m", "compact": False, "sections": ["fields"]},
        )
        assert "## Aggregations (1 — names only)" in result
        assert "`wavg`" in result
        # No param SQL or formula leaks
        assert "quantity" not in result
        assert "SUM(" not in result


class TestInspectModelJsonGating:
    """JSON parity for section gating, descriptions, and observability arrays."""
    async def _save_rich(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="test", type="sqlite", database=":memory:"))
        await storage.save_dataset(SlayerDataset(
            name="rich", sql_table="t", data_source="test",
            description="X" * 50,
            fields=[
                DatasetField(name="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="status", datatype=DataType.STRING),
                DatasetField(name="customer_id", datatype=DataType.INTEGER, hidden=True),
            ],
            metrics=[DatasetMetric(name="aov", expression="count(*)")],
            aggregations=[Aggregation(
                name="wavg",
                expression="SUM({a})",
                params=[AggregationParam(name="a", sql="amount")],
            )],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        ))

    async def test_json_columns_only_uses_names_keys(self, mcp_server, storage: YAMLStorage) -> None:
        await self._save_rich(storage)
        raw = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "rich", "compact": False, "sections": ["fields"], "format": "json"},
        )
        parsed = json.loads(raw)
        # Columns key present (full)
        assert "fields" in parsed
        # Measures/aggregations/joins → names-only siblings
        assert parsed["metrics_names"] == ["aov"]
        assert parsed["aggregations_names"] == ["wavg"]
        assert parsed["relationships_names"] == ["customers"]
        assert "metrics" not in parsed
        assert "aggregations" not in parsed
        assert "relationships" not in parsed
        # Reachable / samples → fully absent
        assert "reachable_dimensions" not in parsed
        assert "reachable_measures" not in parsed
        assert "sample_data" not in parsed
        assert "sample_data_error" not in parsed
        # Top-level state arrays
        assert parsed["names_only_sections"] == ["metrics", "aggregations", "relationships"]
        assert parsed["omitted_sections"] == ["samples", "learnings", "saved_queries"]
        assert "unknown_sections" not in parsed

    async def test_json_unknown_sections_array(self, mcp_server, storage: YAMLStorage) -> None:
        await self._save_rich(storage)
        raw = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "rich", "compact": False, "sections": ["fields", "fish"], "format": "json"},
        )
        parsed = json.loads(raw)
        assert parsed["unknown_sections"] == ["fish"]

    async def test_json_aggregations_show_sql_false_drops_formula_and_param_sql(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        await self._save_rich(storage)
        raw = await _call(
            mcp_server, name="inspect",
            arguments={
                "entity_type": "dataset", "reference": "rich", "compact": False,
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
            mcp_server, name="inspect",
            arguments={
                "entity_type": "dataset", "reference": "rich", "compact": False,
                "sections": ["aggregations"],
                "show_sql": True,
                "format": "json",
            },
        )
        parsed = json.loads(raw)
        agg = parsed["aggregations"][0]
        assert agg["expression"] == "SUM({a})"
        assert agg["params"] == [{"name": "a", "sql": "amount"}]

    async def test_json_descriptions_truncated(self, mcp_server, storage: YAMLStorage) -> None:
        await self._save_rich(storage)
        raw = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "rich", "compact": False, "descriptions_max_chars": 5, "format": "json"},
        )
        parsed = json.loads(raw)
        assert parsed["description"].startswith("XXXXX")
        assert parsed["description"].endswith("[truncated]")

    async def test_json_default_no_gating_arrays(self, mcp_server, storage: YAMLStorage) -> None:
        """Default call should not include the gating arrays (nothing was trimmed)."""
        await self._save_rich(storage)
        raw = await _call(
            mcp_server, name="inspect",
            arguments={"entity_type": "dataset", "reference": "rich", "compact": False, "format": "json"},
        )
        parsed = json.loads(raw)
        assert "names_only_sections" not in parsed
        assert "omitted_sections" not in parsed
        assert "unknown_sections" not in parsed


class TestInspectModelHelpers:
    """Direct unit tests for the section-budgeting helpers."""
    def test_truncate_description_none(self) -> None:
        assert _truncate_description(None, 10) is None
        assert _truncate_description("anything", None) == "anything"

    def test_truncate_description_short_unchanged(self) -> None:
        assert _truncate_description("hello", 10) == "hello"
        # Boundary: exactly equal to max_chars should not truncate
        assert _truncate_description("hello", 5) == "hello"

    def test_truncate_description_marks_truncation(self) -> None:
        assert _truncate_description("hello world", 5) == "hello ... [truncated]"

    def test_truncate_description_zero_yields_marker(self) -> None:
        assert _truncate_description("hello", 0) == " ... [truncated]"

    def test_resolve_inspect_sections_none(self) -> None:
        resolved, unknown = _resolve_inspect_sections(None)
        assert resolved == ["fields", "metrics", "aggregations", "relationships", "samples", "learnings", "saved_queries"]
        assert unknown == []

    def test_resolve_inspect_sections_empty(self) -> None:
        resolved, unknown = _resolve_inspect_sections([])
        # Same expansion as the ``None`` case — pin the order so a
        # missing or duplicated section name fails.
        assert resolved == [
            "fields",
            "metrics",
            "aggregations",
            "relationships",
            "samples",
            "learnings",
            "saved_queries",
        ]
        assert unknown == []

    def test_resolve_inspect_sections_subset_canonical_order(self) -> None:
        resolved, unknown = _resolve_inspect_sections(["metrics", "fields"])
        # Canonical order, not caller order
        assert resolved == ["fields", "metrics"]
        assert unknown == []

    def test_resolve_inspect_sections_unknowns_filtered(self) -> None:
        resolved, unknown = _resolve_inspect_sections(["fields", "fish", "samples"])
        assert resolved == ["fields", "samples"]
        assert unknown == ["fish"]

    def test_resolve_inspect_sections_all_unknown_returns_empty(self) -> None:
        """Only-unknown names resolve to no sections (a typo can't trigger the
        full payload; None/[] alone means "all six")."""
        resolved, unknown = _resolve_inspect_sections(["fish", "bird"])
        assert resolved == []
        assert unknown == ["fish", "bird"]

    def test_render_inspect_footer_sanitizes_unknown_section_names(self) -> None:
        """``repr()`` escapes control chars so unknown names can't forge extra footer lines."""
        result = _render_inspect_footer(
            included=["fields", "metrics", "aggregations", "relationships", "samples", "learnings"],
            names_only=[], omitted=[], unknown=["foo\n> evil-injected"],
        )
        assert result is not None
        # Newline never reaches the rendered output
        assert "\n> evil" not in result
        # The escaped form does
        assert "\\n" in result
        assert "evil-injected" in result

    def test_render_inspect_footer_none_when_no_trim(self) -> None:
        result = _render_inspect_footer(
            included=["fields", "metrics", "aggregations", "relationships", "samples", "learnings"],
            names_only=[], omitted=[], unknown=[],
        )
        assert result is None

    def test_render_inspect_footer_warning_only(self) -> None:
        """All sections shown but unknown names → warning line only, no other footer text."""
        result = _render_inspect_footer(
            included=["fields", "metrics", "aggregations", "relationships", "samples", "learnings"],
            names_only=[], omitted=[], unknown=["fish"],
        )
        assert result is not None
        assert "Warning" in result
        assert "Sections shown:" not in result

    def test_render_inspect_footer_full(self) -> None:
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
        assert "Re-call inspect with" in result


class TestBuildSampleQueryArgs:
    def test_avg_when_allowed(self) -> None:
        model = SlayerDataset(
            name="t", sql_table="t", data_source="ds",
            fields=[
                DatasetField(name="status", datatype=DataType.STRING),
                DatasetField(name="region", datatype=DataType.STRING),
                DatasetField(name="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="rev", sql="amt", datatype=DataType.FLOAT),
                DatasetField(name="qty", sql="quantity", datatype=DataType.FLOAT),
            ],
        )
        args = _build_sample_query_args(dataset=model, num_rows=7)
        assert [f["metric"] for f in args["metrics"]] == ["count(*)", "avg(rev)", "avg(qty)"]
        assert args["dimensions"] == ["status", "region"]
        assert args["limit"] == 7
        assert args["population"] == "t"

    def test_fallback_to_first_allowed_when_avg_not_permitted(self) -> None:
        model = SlayerDataset(
            name="t", sql_table="t", data_source="ds",
            fields=[DatasetField(name="rev", sql="amt", allowed_aggregations=["sum", "max"], datatype=DataType.FLOAT)],
        )
        args = _build_sample_query_args(dataset=model, num_rows=3)
        assert [f["metric"] for f in args["metrics"]] == ["count(*)", "sum(rev)"]

    def test_prefers_safe_agg_over_first_allowed(self) -> None:
        """A non-safe first aggregation is skipped for the first safe zero-arg one."""
        model = SlayerDataset(
            name="t", sql_table="t", data_source="ds",
            fields=[DatasetField(name="rev", sql="amt", allowed_aggregations=["last", "first", "min", "max"], datatype=DataType.FLOAT)],
        )
        args = _build_sample_query_args(dataset=model, num_rows=3)
        assert [f["metric"] for f in args["metrics"]] == ["count(*)", "min(rev)"]

    def test_falls_back_to_first_allowed_when_no_safe_agg(self) -> None:
        """No safe aggregation ⇒ fall back to the first allowed entry."""
        model = SlayerDataset(
            name="t", sql_table="t", data_source="ds",
            fields=[DatasetField(name="rev", sql="amt", allowed_aggregations=["last", "first"], datatype=DataType.FLOAT)],
        )
        args = _build_sample_query_args(dataset=model, num_rows=3)
        assert [f["metric"] for f in args["metrics"]] == ["count(*)", "last(rev)"]

    def test_skip_when_allowed_is_empty(self) -> None:
        model = SlayerDataset(
            name="t", sql_table="t", data_source="ds",
            fields=[DatasetField(name="rev", sql="amt", allowed_aggregations=[], datatype=DataType.FLOAT)],
        )
        args = _build_sample_query_args(dataset=model, num_rows=3)
        assert [f["metric"] for f in args["metrics"]] == ["count(*)"]

    def test_dims_cap_at_two_and_exclude_pk_and_hidden(self) -> None:
        model = SlayerDataset(
            name="t", sql_table="t", data_source="ds",
            fields=[
                DatasetField(name="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="hidden_d", datatype=DataType.STRING, hidden=True),
                DatasetField(name="a", datatype=DataType.STRING),
                DatasetField(name="b", datatype=DataType.STRING),
                DatasetField(name="c", datatype=DataType.STRING),
            ],
        )
        args = _build_sample_query_args(dataset=model, num_rows=3)
        assert args["dimensions"] == ["a", "b"]

    def test_hidden_column_skipped(self) -> None:
        model = SlayerDataset(
            name="t", sql_table="t", data_source="ds",
            fields=[
                DatasetField(name="rev", sql="amt", hidden=True, datatype=DataType.FLOAT),
                DatasetField(name="qty", sql="quantity", datatype=DataType.FLOAT),
            ],
        )
        args = _build_sample_query_args(dataset=model, num_rows=3)
        assert [f["metric"] for f in args["metrics"]] == ["count(*)", "avg(qty)"]

    def test_count_distinct_fallback_for_non_numeric_columns(self) -> None:
        """Non-numeric columns not used as dims get count_distinct; numeric get avg."""
        model = SlayerDataset(
            name="order_items", sql_table="order_items", data_source="ds",
            fields=[
                DatasetField(name="id", datatype=DataType.STRING, primary_key=True),
                DatasetField(name="sku", sql="sku", datatype=DataType.STRING),
                DatasetField(name="is_flagged", sql="is_flagged", datatype=DataType.BOOLEAN),
                DatasetField(name="extra_string", sql="extra_string", datatype=DataType.STRING),
                DatasetField(name="quantity", sql="quantity", datatype=DataType.FLOAT),
            ],
        )
        args = _build_sample_query_args(dataset=model, num_rows=3)
        assert args["dimensions"] == ["sku", "is_flagged"]
        assert [f["metric"] for f in args["metrics"]] == [
            "count(*)", "count_distinct(extra_string)", "avg(quantity)",
        ]

    def test_opaque_column_excluded_from_data_profile(self) -> None:
        """An opaque (Opaque) column has no equality operator, so it's skipped as
        both measure and dimension (else count_distinct/min/max sinks the profile)."""
        opaque = DatasetField(name="loc", datatype=DataType.OPAQUE, db_type="point")
        assert _choose_sample_agg(opaque, metric_types={}) is None
        # ...even when the driver reports a categorical-looking cursor type.
        assert _choose_sample_agg(opaque, metric_types={"loc": ResultColumnType(type=DataType.STRING)}) is None

        model = SlayerDataset(
            name="places", sql_table="places", data_source="ds",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="city", datatype=DataType.STRING),
                opaque,
                DatasetField(name="area", datatype=DataType.FLOAT),
            ],
        )
        args = _build_sample_query_args(dataset=model, num_rows=3)
        assert [f["metric"] for f in args["metrics"]] == ["count(*)", "avg(area)"]
        assert args["dimensions"] == ["city"]


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
        assert _markdown_table(rows=[], fields=["x"]) == "_(none)_"

    def test_table_renders(self) -> None:
        out = _markdown_table(rows=[{"x": 1, "y": "hi"}, {"x": 2, "y": None}], fields=["x", "y"])
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
            fields=["name", "label", "desc"],
        )
        assert "label" not in out
        assert out.splitlines() == [
            "| name | desc |",
            "| --- | --- |",
            "| a | foo |",
            "| b | bar |",
        ]

    def test_table_single_column_collapses_to_comma_list(self) -> None:
        """A lone remaining column renders as a comma list, no table scaffolding."""
        out = _markdown_table(
            rows=[
                {"name": "x", "desc": None},
                {"name": "y", "desc": ""},
                {"name": "z", "desc": None},
            ],
            fields=["name", "desc"],
        )
        assert out == "`x`, `y`, `z`"

    def test_single_column_escapes_backticks(self) -> None:
        """Backticks in values use a longer fence (CommonMark code-span rules)."""
        out = _markdown_table(
            rows=[
                {"name": "no`ticks", "desc": None},
                {"name": "plain", "desc": None},
            ],
            fields=["name", "desc"],
        )
        assert out == "``no`ticks``, `plain`"

    def test_table_all_columns_pruned_returns_none_marker(self) -> None:
        out = _markdown_table(
            rows=[{"a": None, "b": ""}, {"a": "", "b": None}],
            fields=["a", "b"],
        )
        assert out == "_(none)_"

    def test_strip_model_prefix(self) -> None:
        cols, data = _strip_dataset_prefix(
            fields=["orders.count", "orders.revenue_avg", "other.field"],
            data=[{"orders.count": 5, "orders.revenue_avg": 12.5, "other.field": "x"}],
            dataset_name="orders",
        )
        assert cols == ["count", "revenue_avg", "other.field"]
        assert data == [{"count": 5, "revenue_avg": 12.5, "other.field": "x"}]


class TestCreateModel:
    async def test_create(self, mcp_server, storage: YAMLStorage) -> None:
        result = await _call(mcp_server, name="create_dataset", arguments={
            "name": "orders",
            "sql_table": "public.orders",
            "data_source": "test_ds",
            "dimensions": [
                {"name": "id", "sql": "id", "type": "number", "primary_key": "true"},
                {"name": "status", "sql": "status", "type": "string"},
            ],
            "metrics": [
                {"name": "revenue", "sql": "amount"},
            ],
        })
        assert "orders" in result
        assert "created" in result
        assert await storage.get_dataset("orders") is not None

    async def test_create_with_allowed_aggregations(self, mcp_server, storage: YAMLStorage) -> None:
        """``allowed_aggregations`` lives on a Column in v2, not a measure formula."""
        result = await _call(mcp_server, name="create_dataset", arguments={
            "name": "orders",
            "sql_table": "public.orders",
            "data_source": "test_ds",
            "fields": [
                {"name": "revenue", "sql": "amount", "datatype": "number",
                 "allowed_aggregations": ["sum", "avg"]},
            ],
        })
        assert "created" in result
        model = await storage.get_dataset("orders")
        assert model.fields[0].allowed_aggregations == ["sum", "avg"]

    async def test_create_with_measure_meta_dict(self, mcp_server, storage: YAMLStorage) -> None:
        # Pins measures as List[Dict[str, Any]]: a narrower type would reject this
        # dict-valued meta at the FastMCP schema boundary.
        result = await _call(mcp_server, name="create_dataset", arguments={
            "name": "orders",
            "sql_table": "public.orders",
            "data_source": "test_ds",
            "fields": [
                {"name": "revenue", "sql": "amount", "datatype": "number"},
            ],
            "metrics": [
                {"name": "aov", "expression": "sum(revenue) / count(*)",
                 "meta": {"kb_id": "abc-123", "tags": ["finance", "kpi"]}},
            ],
        })
        assert "created" in result
        model = await storage.get_dataset("orders")
        assert model.metrics[0].meta == {"kb_id": "abc-123", "tags": ["finance", "kpi"]}

    async def test_create_reports_replaced(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="create_dataset", arguments={"name": "orders", "sql_table": "t2", "data_source": "test"})
        assert "replaced" in result

    async def test_create_from_query_rejects_mixed_params(self, mcp_server) -> None:
        result = await _call(mcp_server, name="create_dataset", arguments={
            "name": "bad",
            "query": {"population": "orders", "metrics": ["count(*)"]},
            "sql_table": "public.orders",
        })
        assert "Error" in result
        assert "query" in result
        assert "sql_table" in result

    async def test_create_from_query_rejects_data_source(self, mcp_server) -> None:
        result = await _call(mcp_server, name="create_dataset", arguments={
            "name": "bad",
            "query": {"population": "orders", "metrics": ["count(*)"]},
            "data_source": "mydb",
        })
        assert "Error" in result
        assert "data_source" in result

    async def test_create_from_query_ignores_empty_placeholders(self, mcp_server, storage: YAMLStorage) -> None:
        """Empty lists/strings should not trigger the mixed-parameter error."""
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="orders", data_source="test_ds",
            fields=[DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="create_dataset", arguments={
            "name": "summary",
            "query": {"population": "orders", "metrics": ["sum(amount)"]},
            "dimensions": [],
            "metrics": [],
        })
        # Should route to query path (fails on missing datasource), not mixed-param error
        assert "Error:" not in result or "Datasource" in result

    async def test_create_from_query_routes_to_engine(self, mcp_server, storage: YAMLStorage) -> None:
        # The datasource error proves routing to the query path.
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="orders", data_source="test_ds",
            fields=[DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="create_dataset", arguments={
            "name": "summary",
            "query": {"population": "orders", "metrics": ["sum(amount)"]},
        })
        # Should fail on missing datasource, not on "missing sql_table"
        assert "Datasource" in result

    async def test_create_from_query_populates_columns(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        """create_model(query=...) routes through engine.save_model so the persisted
        model has its columns populated (read paths don't warm the cache)."""
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="create_dataset", arguments={
            "name": "summary",
            "query": {
                "population": "orders",
                "metrics": ["sum(amount)"],
            },
        })
        assert "Error" not in result, result
        persisted = await storage.get_dataset("summary")
        assert persisted is not None
        assert "amount" in (await _dry_run_sql(storage, "summary")).lower()
        assert persisted.fields, "MCP create_model(query=...) must populate columns"


class TestEditModel:
    """Tests for the edit_model MCP tool with upsert semantics."""
    # --- Measure upserts ---

    async def test_upsert_new_column(self, mcp_server, storage: YAMLStorage) -> None:
        """Upserting a new column adds it to ``columns``."""
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "fields": [{"name": "total", "sql": "amount", "datatype": "number"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("created field 'total'" in c for c in parsed["changes"])
        model = await storage.get_dataset("orders")
        assert len(model.fields) == 2

    async def test_upsert_column_with_allowed_aggregations(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "fields": [{"name": "total", "sql": "amount", "datatype": "number", "allowed_aggregations": ["sum", "avg"]}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_dataset("orders")
        total = next(c for c in model.fields if c.name == "total")
        assert total.allowed_aggregations == ["sum", "avg"]

    async def test_upsert_list_column_with_element_type(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "fields": [{"name": "tags", "sql": "tags", "datatype": "Array", "element_type": "String"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True, result
        stored = await storage.get_dataset("orders")
        assert stored is not None
        tags = next(c for c in stored.fields if c.name == "tags")
        assert (tags.datatype, tags.element_type) == (DataType.ARRAY, DataType.STRING)

    async def test_upsert_existing_column(self, mcp_server, storage: YAMLStorage) -> None:
        """Upserting an existing column updates it instead of erroring."""
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", description="old", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "fields": [{"name": "revenue", "sql": "price"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("updated field 'revenue'" in c for c in parsed["changes"])
        model = await storage.get_dataset("orders")
        assert len(model.fields) == 1
        assert model.fields[0].expression == "price"

    async def test_upsert_existing_column_partial_update(self, mcp_server, storage: YAMLStorage) -> None:
        """Partial upsert: only specified fields change, others are preserved."""
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", description="Total revenue", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "fields": [{"name": "revenue", "description": "Updated description"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        c = (await storage.get_dataset("orders")).fields[0]
        assert c.description == "Updated description"
        assert c.expression == "amount"  # unchanged

    # --- New ``measures`` (ModelMeasure formulas) upserts ---

    async def test_upsert_new_measure_formula(self, mcp_server, storage: YAMLStorage) -> None:
        """Upserting a new model-level ModelMeasure adds it to ``measures``."""
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "metrics": [{"name": "aov", "expression": "sum(revenue) / count(*)"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("created metric 'aov'" in c for c in parsed["changes"])
        model = await storage.get_dataset("orders")
        assert len(model.metrics) == 1
        assert model.metrics[0].expression == "sum(revenue) / count(*)"

    async def test_upsert_existing_measure_formula(self, mcp_server, storage: YAMLStorage) -> None:

        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT)],
            metrics=[DatasetMetric(name="aov", expression="sum(revenue) / count(*)")],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "metrics": [{"name": "aov", "expression": "avg(revenue)"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_dataset("orders")
        assert model.metrics[0].expression == "avg(revenue)"

    async def test_upsert_multiple_mixed_create_update(self, mcp_server, storage: YAMLStorage) -> None:
        """One new + one existing column in the same call."""
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "fields": [
                {"name": "revenue", "description": "Updated"},
                {"name": "profit", "sql": "revenue - cost", "datatype": "number"},
            ],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("updated field 'revenue'" in c for c in parsed["changes"])
        assert any("created field 'profit'" in c for c in parsed["changes"])
        model = await storage.get_dataset("orders")
        assert len(model.fields) == 2

    async def test_invalid_column_type_on_upsert(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "fields": [{"name": "bad", "datatype": "invalid_type"}],
        })
        assert "Invalid" in result

    # --- Aggregation upserts ---

    async def test_upsert_new_aggregation(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "aggregations": [{"name": "my_agg", "expression": "SUM({value})"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_dataset("orders")
        assert len(model.aggregations) == 1
        assert model.aggregations[0].name == "my_agg"

    async def test_upsert_existing_aggregation(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            aggregations=[Aggregation(name="my_agg", expression="SUM({value})")],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "aggregations": [{"name": "my_agg", "expression": "AVG({value})"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_dataset("orders")
        assert model.aggregations[0].expression == "AVG({value})"

    async def test_remove_aggregation(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            aggregations=[Aggregation(name="my_agg", expression="SUM({value})")],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "remove": {"aggregations": ["my_agg"]},
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert len((await storage.get_dataset("orders")).aggregations) == 0

    # --- Join upserts ---

    async def test_upsert_new_join(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="customer_id", datatype=DataType.INTEGER, hidden=True)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "relationships": [{"to": "customers", "from_columns": ["customer_id"], "to_columns": ["id"]}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_dataset("orders")
        assert len(model.relationships) == 1
        assert model.relationships[0].to == "customers"

    async def test_upsert_existing_join(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[
                DatasetField(name="customer_id", datatype=DataType.INTEGER, hidden=True),
                DatasetField(name="buyer_id", datatype=DataType.INTEGER, hidden=True),
            ],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "relationships": [{"to": "customers", "from_columns": ["buyer_id"], "to_columns": ["id"]}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_dataset("orders")
        assert len(model.relationships) == 1
        assert model.relationships[0].column_pairs == [("buyer_id", "id")]

    async def test_remove_join(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="customer_id", datatype=DataType.INTEGER, hidden=True)],
            relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "remove": {"relationships": ["customers"]},
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert len((await storage.get_dataset("orders")).relationships) == 0

    # --- Filter management ---

    async def test_add_filter(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "add_filters": ["deleted_at IS NULL"],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert "deleted_at IS NULL" in (await storage.get_dataset("orders")).filters

    async def test_add_duplicate_filter_skipped(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            filters=["deleted_at IS NULL"],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "add_filters": ["deleted_at IS NULL"],
        })
        # No changes because the filter already exists
        assert "No changes" in result

    async def test_remove_filter(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            filters=["deleted_at IS NULL"],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "remove_filters": ["deleted_at IS NULL"],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert len((await storage.get_dataset("orders")).filters) == 0

    async def test_remove_filter_not_found(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "remove_filters": ["nonexistent"],
        })
        assert "Filter not found" in result

    # --- Scalar metadata ---

    async def test_update_description(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "description": "Updated",
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert (await storage.get_dataset("orders")).description == "Updated"

    async def test_set_sql_table(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "sql_table": "public.orders",
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert (await storage.get_dataset("orders")).sql_table == "public.orders"

    async def test_set_hidden(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "hidden": True,
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert (await storage.get_dataset("orders")).hidden is True

    # --- Multiple changes ---

    async def test_multiple_changes(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "description": "Orders table",
            "fields": [
                {"name": "total", "sql": "amount", "datatype": "number"},
                {"name": "status", "sql": "status", "datatype": "string"},
            ],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert len(parsed["changes"]) == 3
        model = await storage.get_dataset("orders")
        assert model.description == "Orders table"
        assert any(c.name == "status" for c in model.fields)
        assert any(c.name == "total" for c in model.fields)

    # --- Typed remove ---

    async def test_remove_column_typed(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT), DatasetField(name="total", sql="x", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "remove": {"fields": ["total"]},
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        model = await storage.get_dataset("orders")
        assert len(model.fields) == 1

    async def test_remove_column_not_found(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "remove": {"fields": ["nonexistent"]},
        })
        assert "not found" in result

    async def test_remove_invalid_key(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "remove": {"invalid": ["x"]},
        })
        assert "Invalid remove key" in result

    async def test_remove_then_recreate_same_call(self, mcp_server, storage: YAMLStorage) -> None:
        """Remove a column then upsert one with the same name in the same call."""
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="status", sql="old_col", datatype=DataType.STRING)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "remove": {"fields": ["status"]},
            "fields": [{"name": "status", "sql": "new_col", "datatype": "string"}],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        c = (await storage.get_dataset("orders")).fields[0]
        assert c.expression == "new_col"

    # --- Error cases ---

    async def test_model_not_found(self, mcp_server) -> None:
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "nope",
            "description": "test",
        })
        assert "not found" in result

    async def test_no_changes(self, mcp_server, storage: YAMLStorage) -> None:
        await storage.save_dataset(SlayerDataset(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_dataset", arguments={"dataset_name": "orders"})
        assert "No changes" in result

    async def test_cross_field_validation_error(self, mcp_server, storage: YAMLStorage) -> None:
        """allowed_aggregations referencing a non-existent aggregation should fail."""
        await storage.save_dataset(SlayerDataset(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "fields": [{"name": "rev", "sql": "amount", "datatype": "number", "allowed_aggregations": ["nonexistent_agg"]}],
        })
        assert "Validation error" in result or "not a built-in aggregation" in result

    # --- Query-backed model edits ---

    async def test_edit_set_source_queries_makes_model_query_backed(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        """edit_model switches source mode: source_queries set, sql_table cleared."""
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        # Separate upstream so the backing query has a non-cyclic source.
        await storage.save_dataset(SlayerDataset(
            name="orders_source", sql_table="orders_t", data_source="test",
            fields=[DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT)],
        ))
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "source_queries": [{
                "population": "orders_source",
                "metrics": [{"metric": "sum(amount)"}],
            }],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        reloaded = await storage.get_dataset("orders")
        assert reloaded is not None
        assert reloaded.source_queries is not None
        assert len(reloaded.source_queries) == 1
        assert reloaded.source_queries[0].population == "orders_source"
        assert not reloaded.sql_table
        assert reloaded.fields
        assert "amount" in (await _dry_run_sql(storage, "orders")).lower()

    async def test_edit_query_variables_on_query_backed_model(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_dataset(SlayerDataset(
            name="upstream", sql_table="t", data_source="test",
            fields=[DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT)],
        ))
        await storage.save_dataset(SlayerDataset(
            name="qb",
            data_source="test",
            source_queries=[SlayerQuery(
                population="upstream",
                metrics=[{"metric": "sum(amount)"}],
                filter=["amount > {threshold}"],
                dry_run=True,
            )],
            query_variables={"threshold": 100},
        ))

        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "qb",
            "query_variables": {"threshold": 500},
        })
        parsed = json.loads(result)
        assert parsed["success"] is True
        assert any("query_variables" in c for c in parsed["changes"])
        reloaded = await storage.get_dataset("qb")
        assert reloaded.query_variables == {"threshold": 500}

    async def test_edit_rejects_simultaneous_source_modes(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test"
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "sql_table": "new_t",
            "sql": "SELECT 1",
        })
        assert "mutually exclusive" in result or "Specify at most one" in result

    # --- meta round-trip pins ---

    async def test_edit_persists_measure_meta_create_path(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """New measure with meta via edit_model survives the storage round-trip."""
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "metrics": [{
                "name": "aov", "expression": "sum(revenue) / count(*)",
                "meta": {"kb_id": 1},
            }],
        })
        assert json.loads(result)["success"] is True
        model = await storage.get_dataset("orders")
        assert model.metrics[0].meta == {"kb_id": 1}

    async def test_edit_persists_measure_meta_update_path(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """Adding meta to an existing measure via edit_model survives the round-trip."""
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT)],
            metrics=[DatasetMetric(name="aov", expression="sum(revenue) / count(*)")],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "metrics": [{
                "name": "aov", "expression": "sum(revenue) / count(*)",
                "meta": {"kb_id": 1},
            }],
        })
        assert json.loads(result)["success"] is True
        model = await storage.get_dataset("orders")
        assert model.metrics[0].meta == {"kb_id": 1}

    async def test_edit_persists_aggregation_meta_create_path(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """New aggregation with meta+formula via edit_model survives the round-trip."""
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "aggregations": [{
                "name": "my_agg",
                "expression": "SUM({expr})",
                "meta": {"owner": "x"},
            }],
        })
        assert json.loads(result)["success"] is True
        model = await storage.get_dataset("orders")
        assert model.aggregations[0].meta == {"owner": "x"}
        assert model.aggregations[0].expression == "SUM({expr})"

    async def test_edit_persists_aggregation_meta_update_path(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """Adding meta to an existing aggregation survives the round-trip."""
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            aggregations=[Aggregation(name="my_agg", expression="SUM({expr})")],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "aggregations": [{"name": "my_agg", "meta": {"owner": "x"}}],
        })
        assert json.loads(result)["success"] is True
        model = await storage.get_dataset("orders")
        assert model.aggregations[0].meta == {"owner": "x"}
        assert model.aggregations[0].expression == "SUM({expr})"  # preserved (partial update)

    async def test_edit_measure_meta_replaced_not_merged(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """Updating meta replaces the whole dict (no deep merge)."""
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT)],
            metrics=[DatasetMetric(
                name="aov", expression="sum(revenue) / count(*)",
                meta={"a": 1},
            )],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "metrics": [{"name": "aov", "meta": {"b": 2}}],
        })
        assert json.loads(result)["success"] is True
        model = await storage.get_dataset("orders")
        assert model.metrics[0].meta == {"b": 2}

    async def test_edit_omitting_meta_key_preserves_existing_meta(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """Omitting the meta key preserves existing meta (flat-merge upsert)."""
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[DatasetField(name="revenue", sql="amount", datatype=DataType.FLOAT)],
            metrics=[DatasetMetric(
                name="aov", expression="sum(revenue) / count(*)",
                meta={"kb_id": 9},
            )],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "metrics": [{"name": "aov", "description": "Average order value"}],
        })
        assert json.loads(result)["success"] is True
        model = await storage.get_dataset("orders")
        assert model.metrics[0].description == "Average order value"
        assert model.metrics[0].meta == {"kb_id": 9}


class TestEditModelDatasourceMoveSafety:
    """Moving a model to a new data_source must be atomic and collision-safe:
    refuse on a target collision or a save failure, leaving the source intact."""
    async def test_move_collision_with_target_refuses_and_preserves_source(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        for n in ("db_a", "db_b"):
            await storage.save_datasource(DatasourceConfig(
                name=n, type="sqlite", database=":memory:"
            ))
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="orders_a", data_source="db_a",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True)],
            description="source",
        ))
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="orders_b", data_source="db_b",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True)],
            description="target",
        ))

        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "data_source": "db_a",
            "new_data_source": "db_b",
        })
        # Refusal mentions the collision so the agent can fix it.
        assert "exist" in result.lower() or "collid" in result.lower() or "already" in result.lower(), (
            f"expected collision rejection, got: {result}"
        )

        # Both models still in their original places, untouched.
        src = await storage.get_dataset("orders", data_source="db_a")
        tgt = await storage.get_dataset("orders", data_source="db_b")
        assert src is not None
        assert src.description == "source"
        assert tgt is not None
        assert tgt.description == "target"

    async def test_move_query_backed_when_engine_recomputes_data_source_back(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """A no-op move of a query-backed model (its query resolves back to the
        original datasource) must not delete the row just saved at that key."""
        for n in ("db_a", "db_b"):
            await storage.save_datasource(DatasourceConfig(
                name=n, type="sqlite", database=":memory:"
            ))
        # Upstream in db_a ⇒ qb's backing query resolves to db_a.
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="orders", data_source="db_a",
            fields=[
                DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True),
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
            ],
        ))
        engine = SlayerQueryEngine(storage=storage)
        await engine.save_dataset(SlayerDataset(
            name="qb",
            data_source="db_a",
            source_queries=[SlayerQuery.model_validate({
                "population": "orders",
                "metrics": [{"metric": "sum(amount)"}],
            })],
        ))
        assert await storage.get_dataset("qb", data_source="db_a") is not None
        assert await storage.get_dataset("qb", data_source="db_b") is None

        # Move db_a -> db_b, but the query still resolves to db_a, so the model
        # should land back at db_a and never be deleted outright.
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "qb",
            "data_source": "db_a",
            "new_data_source": "db_b",
        })
        del result
        landed_a = await storage.get_dataset("qb", data_source="db_a")
        landed_b = await storage.get_dataset("qb", data_source="db_b")
        assert landed_a is not None or landed_b is not None, (
            "edit_model deleted the model entirely after a no-op move "
            "of a query-backed entity"
        )

    async def test_move_save_failure_preserves_source(
        self, mcp_server, storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        for n in ("db_a", "db_b"):
            await storage.save_datasource(DatasourceConfig(
                name=n, type="sqlite", database=":memory:"
            ))
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="orders_a", data_source="db_a",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True)],
            description="source",
        ))

        # Force the save under the new datasource to fail; the source row must survive.
        # The engine's storage view writes through the store's ``_save_model_impl``.
        original_save = storage._save_dataset_impl

        async def _failing_save(model):
            if model.data_source == "db_b":
                raise RuntimeError("simulated storage failure on new key")
            return await original_save(model)

        monkeypatch.setattr(storage, "_save_dataset_impl", _failing_save)

        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "orders",
            "data_source": "db_a",
            "new_data_source": "db_b",
        })
        assert (
            "fail" in result.lower()
            or "error" in result.lower()
            or "simulated" in result.lower()
        ), f"expected save-failure to surface, got: {result}"

        src = await storage.get_dataset("orders", data_source="db_a")
        assert src is not None
        assert src.data_source == "db_a"
        assert src.description == "source"


class TestEditModelMultiStageRename:
    """Editing an inner stage's measure (rename or shape change) must refresh the
    cached SQL/columns so outer-stage references resolve."""
    async def _setup_orders_with_two_stage_dataset(
        self, storage: YAMLStorage, *, inner_metrics: list, outer_metrics: list,
    ) -> None:
        """Save a datasource, an ``orders`` table-model, and a 2-stage qb model
        (inner stage ``raw``)."""
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="t", data_source="test",
            fields=[
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="region", sql="region", datatype=DataType.STRING),
            ],
        ))
        # Build initial source_queries via the engine save path so the cache
        # reflects the initial state.
        engine = SlayerQueryEngine(storage=storage)
        await engine.save_dataset(SlayerDataset(
            name="qb",
            data_source="test",
            source_queries=[
                SlayerQuery(
                    name="raw",
                    population="orders",
                    dimensions=["region"],
                    metrics=inner_metrics,
                ),
                SlayerQuery(
                    population="raw",
                    metrics=outer_metrics,
                ),
            ],
        ))

    async def test_edit_model_renames_inner_stage_measure(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """Renaming an inner-stage measure old→new refreshes the cached SQL/columns."""
        await self._setup_orders_with_two_stage_dataset(
            storage,
            inner_metrics=[{"metric": "sum(amount)", "alias": "old"}],
            outer_metrics=[{"metric": "sum(old)"}],
        )
        # Sanity: initial cache reflects 'old'.
        before = await storage.get_dataset("qb")
        assert before is not None
        assert "old" in await _dry_run_sql(storage, "qb")
        assert "old_sum" in [c.name for c in before.fields]

        # Edit: rename inner-stage measure to 'new' (and update outer to match).
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "qb",
            "source_queries": [
                {
                    "name": "raw",
                    "population": "orders",
                    "dimensions": ["region"],
                    "metrics": [{"metric": "sum(amount)", "alias": "new"}],
                },
                {
                    "population": "raw",
                    "metrics": [{"metric": "sum(new)"}],
                },
            ],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True, result

        after = await storage.get_dataset("qb")
        assert after is not None
        col_names = [c.name for c in after.fields]
        assert "new_sum" in col_names, (
            f"cache must reflect renamed inner measure, got: {col_names}"
        )
        assert "old_sum" not in col_names, (
            f"stale 'old_sum' must be evicted, got: {col_names}"
        )
        sql = await _dry_run_sql(storage, "qb")
        assert "new" in sql, f"the SQL must contain the new name:\n{sql}"
        # The stale name must not survive in the wrap aliases or measure refs.
        assert " AS old " not in sql, (
            f"the SQL must not retain the stale 'old' alias:\n{sql}"
        )
        assert 'AS "old"' not in sql, (
            f"the SQL must not retain the stale 'old' alias:\n{sql}"
        )

    async def test_edit_model_stage_shape_change_drops_and_adds_measure(
        self, mcp_server, storage: YAMLStorage,
    ) -> None:
        """Dropping inner ``n`` and adding ``avg_amount`` refreshes the cached SQL."""
        await self._setup_orders_with_two_stage_dataset(
            storage,
            inner_metrics=[
                {"metric": "sum(amount)", "alias": "rev"},
                {"metric": "count(*)", "alias": "n"},
            ],
            outer_metrics=[{"metric": "sum(rev)"}],
        )
        before = await storage.get_dataset("qb")
        assert before is not None
        before_cols = [c.name for c in before.fields]
        assert "rev_sum" in before_cols
        # Inner-stage `n` is not directly emitted on the outer model (the
        # outer only takes rev:sum), but it must appear in the SQL
        # as the inner stage's wrap rename.
        before_sql = await _dry_run_sql(storage, "qb")
        assert " AS n " in before_sql or 'AS "n"' in before_sql or before_sql.find("AS n\n") >= 0, (
            f"initial cache should expose inner 'n' alias:\n{before_sql}"
        )

        # Edit: swap n → avg_amount in stage 1; outer now sums avg_amount.
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "qb",
            "source_queries": [
                {
                    "name": "raw",
                    "population": "orders",
                    "dimensions": ["region"],
                    "metrics": [
                        {"metric": "sum(amount)", "alias": "rev"},
                        {"metric": "avg(amount)", "alias": "avg_amount"},
                    ],
                },
                {
                    "population": "raw",
                    "metrics": [
                        {"metric": "sum(rev)"},
                        {"metric": "avg(avg_amount)"},
                    ],
                },
            ],
        })
        parsed = json.loads(result)
        assert parsed["success"] is True, result

        after = await storage.get_dataset("qb")
        assert after is not None
        after_sql = await _dry_run_sql(storage, "qb")
        # Stale `n` must be gone from the inner wrap.
        assert " AS n " not in after_sql, (
            f"stale inner 'n' alias must be evicted:\n{after_sql}"
        )
        assert 'AS "n"' not in after_sql, (
            f"stale inner 'n' alias must be evicted:\n{after_sql}"
        )
        # New `avg_amount` must be present.
        assert "avg_amount" in after_sql, (
            f"new 'avg_amount' alias must appear in the SQL:\n{after_sql}"
        )
        # Cached outer columns reflect the new outer measures.
        after_cols = [c.name for c in after.fields]
        assert "rev_sum" in after_cols
        assert "avg_amount_avg" in after_cols, (
            f"outer column for avg_amount:avg missing: {after_cols}"
        )


class TestEditModelColumnsRejected:
    """edit_model must reject ``columns`` on a query-backed model (engine-managed cache)."""
    async def test_columns_on_query_backed_edit_returns_error(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_dataset(SlayerDataset(
            name="upstream", sql_table="t", data_source="test",
            fields=[DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT)],
        ))
        await storage.save_dataset(SlayerDataset(
            name="qb",
            data_source="test",
            source_queries=[SlayerQuery(
                population="upstream",
                metrics=[{"metric": "sum(amount)"}],
                dry_run=True,
            )],
        ))
        # Snapshot the model state before the bad edit.
        before = await storage.get_dataset("qb")
        before_columns = list(before.fields)

        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "qb",
            "fields": [{"name": "x", "sql": "x", "datatype": "string"}],
        })
        # Should be a clear error, not a silent success.
        assert (
            "engine-managed" in result
            or "auto-generated" in result
            or "Cannot supply columns" in result
            or "must not be supplied" in result
        ), f"expected explicit rejection, got: {result}"
        # Stored model is unchanged.
        after = await storage.get_dataset("qb")
        assert [c.name for c in after.fields] == [c.name for c in before_columns]


class TestInspectModelRequiredVariables:
    """``required_variables`` excludes placeholders with a default at model- or stage-level."""
    async def test_stage_scoped_default_not_required(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_dataset(SlayerDataset(
            name="upstream", sql_table="t", data_source="test",
            fields=[DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT)],
        ))
        # Stage has its own variables={"x": 1}, so {x} placeholder is NOT
        # required from outside.
        await storage.save_dataset(SlayerDataset(
            name="qb_stage_default",
            data_source="test",
            source_queries=[SlayerQuery(
                population="upstream",
                metrics=[{"metric": "sum(amount)"}],
                filter=["amount > {x}"],
                variables={"x": 1},
                dry_run=True,
            )],
            # No model-level query_variables.
        ))
        result = await _call(mcp_server, name="inspect", arguments={
            "entity_type": "dataset", "reference": "qb_stage_default", "compact": False, "format": "json",
        })
        parsed = json.loads(result)
        assert "x" not in parsed["backing_query"]["required_variables"]


class TestRunByNamePlanFlagsMCP:
    """``query(query="qb", dry_run=True)`` returns SQL without executing."""
    async def test_dry_run_run_by_name_returns_sql_without_executing(
        self, mcp_server, storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_dataset(SlayerDataset(
            name="upstream", sql_table="t", data_source="test",
            fields=[DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT)],
        ))
        await storage.save_dataset(SlayerDataset(
            name="qb_dr",
            data_source="test",
            source_queries=[SlayerQuery.model_validate({
                "population": "upstream",
                "metrics": [{"metric": "sum(amount)"}],
            })],
        ))
        execute_calls = 0
        real_execute = SlayerSQLClient.execute

        async def counting_execute(self, *a, **kw):
            nonlocal execute_calls
            execute_calls += 1
            return await real_execute(self, *a, **kw)

        monkeypatch.setattr(SlayerSQLClient, "execute", counting_execute)
        result = await _call(mcp_server, name="query", arguments={
            "query": "qb_dr",
            "dry_run": True,
        })
        assert "SQL:" in result
        assert "amount" in result.lower()
        assert execute_calls == 0, "dry_run=True must not execute SQL"


class TestQueryAcceptsInlineSourceModel:
    """The MCP ``query`` tool accepts ``source_model`` as a name and refuses extension columns and inline models."""
    async def _setup_orders(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            fields=[
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
            ],
        ))

    async def test_string_source_model_still_works(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await self._setup_orders(storage)
        result = await _call(mcp_server, name="query", arguments={
            "query": {
                "population": "orders",
                "metrics": [{"metric": "count(*)"}],
            },
            "dry_run": True,
        })
        assert "Invalid model name" not in result
        assert "SQL:" in result
        assert "orders" in result.lower()

    async def test_inline_model_extension_dict(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        await self._setup_orders(storage)
        with pytest.raises(ToolError, match="fields"):
            await _call(mcp_server, name="query", arguments={
                "query": {
                    "population": "orders",
                    "extensions": [{
                        "dataset": "orders",
                        "fields": [
                            {"name": "double_amount", "sql": "amount * 2", "type": "DOUBLE"},
                        ],
                    }],
                    "metrics": [{"metric": "sum(double_amount)"}],
                },
                "dry_run": True,
            })

    async def test_inline_slayer_model_dict(
        self, mcp_server, storage: YAMLStorage
    ) -> None:
        # Datasource must exist for SQL-client routing, but the table need not.
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        with pytest.raises(ToolError, match="(?i)inline"):
            await _call(mcp_server, name="query", arguments={
                "query": {
                    "population": {
                        "name": "ad_hoc",
                        "sql_table": "things",
                        "data_source": "test",
                        "fields": [
                            {"name": "x", "sql": "x", "datatype": "Float"},
                        ],
                    },
                    "metrics": [{"metric": "sum(x)"}],
                },
                "dry_run": True,
            })


class TestQueryListMultiStage:
    """MCP ``query`` with a list argument: a DAG of named stages later entries
    reference by name; mirrors ``engine.execute(list)`` (the list form absorbed
    the retired standalone nested-query tool)."""
    async def _setup_orders(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(
            name="test", type="sqlite", database=":memory:"
        ))
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="orders", data_source="test",
            fields=[
                DatasetField(name="amount", sql="amount", datatype=DataType.FLOAT),
                DatasetField(name="status", sql="status", datatype=DataType.STRING),
                DatasetField(name="created_at", sql="created_at", datatype=DataType.DATETIME),
            ],
        ))

    async def test_two_stage_dag_dry_run(self, mcp_server, storage: YAMLStorage) -> None:
        """Stage 2 references prior named sibling 'monthly' via source_model."""
        await self._setup_orders(storage)
        result = await _call(mcp_server, name="query", arguments={
            "query": [
                {
                    "name": "monthly",
                    "population": "orders",
                    "metrics": [{"metric": "sum(amount)"}],
                    "time_dimensions": [
                        {"field": "created_at", "granularity": "month"},
                    ],
                },
                {
                    "population": "monthly",
                    "metrics": [{"metric": "avg(amount_sum)"}],
                },
            ],
            "dry_run": True,
        })
        assert "SQL:" in result
        # The earlier stage emits a CTE / sub-select named after its alias,
        # and the outer stage's AVG aggregation must surface.
        assert "AVG(" in result.upper()

    async def test_empty_list_rejected(self, mcp_server, storage: YAMLStorage) -> None:
        await self._setup_orders(storage)
        with pytest.raises(ToolError, match="non-empty"):
            await _call(mcp_server, name="query", arguments={
                "query": [],
                "dry_run": True,
            })

    async def test_out_of_order_dag_works(self, mcp_server, storage: YAMLStorage) -> None:
        """Stages submitted out of topological order are auto-sorted before emission."""
        await self._setup_orders(storage)
        result = await _call(mcp_server, name="query", arguments={
            "query": [
                {
                    "name": "a",
                    "population": "b",
                    "metrics": [{"metric": "avg(amount_sum)"}],
                },
                {
                    "name": "b",
                    "population": "orders",
                    "metrics": [{"metric": "sum(amount)"}],
                },
                {
                    "population": "a",
                    "metrics": [{"metric": "max(amount_sum_avg)"}],
                },
            ],
            "dry_run": True,
        })
        assert "SQL:" in result
        assert "MAX(" in result.upper()

    async def test_cycle_raises(self, mcp_server, storage: YAMLStorage) -> None:
        """A cycle between stages must surface a clear error naming the cycle members."""
        await self._setup_orders(storage)
        with pytest.raises(ToolError, match=r"[Cc]ycle"):
            await _call(mcp_server, name="query", arguments={
                "query": [
                    {"name": "a", "population": "b", "metrics": [{"metric": "sum(amount)"}]},
                    {"name": "b", "population": "a", "metrics": [{"metric": "sum(amount)"}]},
                    {"population": "orders", "metrics": [{"metric": "sum(amount)"}]},
                ],
                "dry_run": True,
            })

    async def test_invalid_format_rejected(self, mcp_server, storage: YAMLStorage) -> None:
        await self._setup_orders(storage)
        with pytest.raises(ToolError, match="Invalid format"):
            await _call(mcp_server, name="query", arguments={
                "query": [
                    {"population": "orders", "metrics": [{"metric": "count(*)"}]},
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

    async def test_create_auto_ingest_imports_dataset_description(
        self, mcp_server, storage: YAMLStorage, tmp_path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        db_path = str(tmp_path / "live.db")
        with transaction(db_path) as conn:
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")

        real = ingestion_mod.ingest_datasource_report_async

        async def _fake(*args, **kwargs):
            out = await real(*args, **kwargs)
            return out.model_copy(update={"schema_description": "From the dataset"})

        monkeypatch.setattr(ingestion_mod, "ingest_datasource_report_async", _fake)
        result = await _call(mcp_server, name="create_datasource", arguments={
            "name": "ds", "type": "sqlite", "database": db_path,
        })
        assert "ds" in result
        loaded = await storage.get_datasource("ds")
        assert loaded.description == "From the dataset"
        assert await storage.get_dataset("t", data_source="ds") is not None

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
        """A broken datasource YAML surfaces the invalid-config error, not a raise."""
        path = os.path.join(storage.datasources_dir, "bad.yaml")
        with open(path, "w") as f:
            f.write("name: bad\ntype: [unclosed\n")
        result = await _call(mcp_server, name="datasets_summary", arguments={"datasource_name": "bad"})
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
        await storage.save_dataset(SlayerDataset(name="orders", sql_table="t", data_source="test"))
        result = await _call(mcp_server, name="delete_dataset", arguments={"name": "orders"})
        assert "deleted" in result
        assert await storage.get_dataset("orders") is None

    async def test_delete_model_not_found(self, mcp_server) -> None:
        result = await _call(mcp_server, name="delete_dataset", arguments={"name": "nope"})
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
        assert _is_id_field("id") is True
        assert _is_id_field("user_id") is True
        assert _is_id_field("customer_key") is True
        assert _is_id_field("role_fk") is True
        assert _is_id_field("primary_pk") is True
        assert _is_id_field("amount") is False
        assert _is_id_field("quantity") is False
        assert _is_id_field("price") is False
        assert _is_id_field("width") is False


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


# The `help` MCP tool was removed. Conceptual help now ships as
# seeded `memory:help.*` memories read via `inspect(entity_type="memory")`;
# see tests/test_help_seed.py.




class TestDataProfileRetryScope:
    """The count-only retry fires only for a grouping/dedup failure; other failures
    keep their own cause. Fakes key off ``query.dimensions`` to target the full profile."""
    @staticmethod
    def _dataset() -> SlayerDataset:
        return SlayerDataset(
            name="profile_scope",
            sql_table="t",
            data_source="test",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="status", datatype=DataType.STRING),
            ],
        )

    class _CountResult:
        sql = "SELECT COUNT(*) FROM t"
        columns = ["profile_scope._count"]
        data = [{"profile_scope._count": 7}]

        def to_markdown(self) -> str:
            return "| _count |\n|---|\n| 7 |"

    async def _render(self, storage, engine) -> dict:
        out = await render_dataset_inspection(
            dataset=self._dataset(), storage=storage, engine=engine,
            format="json", sections=["samples"],
        )
        return json.loads(out)

    async def test_unrelated_failure_is_not_relabeled_as_reduced(
        self, storage: YAMLStorage
    ) -> None:
        outer = self

        class _Engine:
            async def get_result_column_types(self, **kwargs):
                return {}

            async def execute(self, *, query, **kwargs):
                if query.dimensions:
                    raise RuntimeError("permission denied for table t")
                return outer._CountResult()

        payload = await self._render(storage, _Engine())
        # A permission failure must NOT masquerade as a type/grouping problem.
        assert payload["sample_data_reduced"] is False
        assert payload["sample_data_reduced_reason"] is None
        assert "permission denied" in (payload["sample_data_error"] or "")

    async def test_grouping_failure_retries_and_reports_reduced(
        self, storage: YAMLStorage
    ) -> None:
        outer = self

        class _Engine:
            async def get_result_column_types(self, **kwargs):
                return {}

            async def execute(self, *, query, **kwargs):
                if query.dimensions:
                    raise RuntimeError(
                        "could not identify an equality operator for type point"
                    )
                return outer._CountResult()

        payload = await self._render(storage, _Engine())
        assert payload["sample_data_reduced"] is True
        assert "does not support" in payload["sample_data_reduced_reason"]
        assert payload["sample_data_error"] is None

    async def test_original_cause_survives_a_failing_retry(
        self, storage: YAMLStorage
    ) -> None:
        """When the reduced profile also fails, report the first cause."""
        class _Engine:
            def __init__(self) -> None:
                self.dimensioned_seen = False

            async def get_result_column_types(self, **kwargs):
                return {}

            async def execute(self, *, query, **kwargs):
                if query.dimensions:
                    self.dimensioned_seen = True
                    raise RuntimeError(
                        "could not identify an equality operator for type point"
                    )
                if self.dimensioned_seen:
                    # This is the count-only retry — fail it too.
                    raise RuntimeError("connection closed unexpectedly")
                raise RuntimeError("row count unavailable")

        payload = await self._render(storage, _Engine())
        assert "equality operator" in (payload["sample_data_error"] or "")
        assert "connection closed" not in (payload["sample_data_error"] or "")
        assert payload["sample_data_reduced"] is False


def _seed_live_db(path: str) -> None:
    """A real SQLite file with an ``orders`` table for save-time trial-execute."""
    with transaction(path) as conn:
        conn.executescript(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL, status TEXT);"
            "INSERT INTO orders VALUES (1, 100.0, 'ok');"
        )


async def _register_live_ds(storage: YAMLStorage, db_path: str) -> None:
    await storage.save_datasource(
        DatasourceConfig(name="livedb", type="sqlite", database=db_path)
    )


class TestSaveTimeSqlValidationMcp:
    """The MCP create/edit doors reroute through ``engine.save_model``
    so a raw-sql model with backend-invalid SQL is rejected before it persists."""

    async def test_create_sql_mode_valid_persists(
        self, mcp_server, storage: YAMLStorage, tmp_path
    ) -> None:
        db = str(tmp_path / "live.db")
        _seed_live_db(db)
        await _register_live_ds(storage, db)
        result = await _call(mcp_server, name="create_dataset", arguments={
            "name": "good_model", "sql": "SELECT id FROM orders",
            "data_source": "livedb",
            "fields": [{"name": "id", "sql": "id", "datatype": "number"}],
        })
        assert "created" in result
        assert await storage.get_dataset("good_model", data_source="livedb") is not None

    async def test_create_sql_mode_invalid_rejected(
        self, mcp_server, storage: YAMLStorage, tmp_path
    ) -> None:
        db = str(tmp_path / "live.db")
        _seed_live_db(db)
        await _register_live_ds(storage, db)
        result = await _call(mcp_server, name="create_dataset", arguments={
            "name": "bad_model", "sql": "SELECT nope FROM ghosts",
            "data_source": "livedb",
            "fields": [{"name": "id", "sql": "id", "datatype": "number"}],
        })
        assert "created" not in result
        assert "bad_model" in result
        assert await storage.get_dataset("bad_model", data_source="livedb") is None

    async def test_edit_sql_to_invalid_rejected_and_original_intact(
        self, mcp_server, storage: YAMLStorage, tmp_path
    ) -> None:
        db = str(tmp_path / "live.db")
        _seed_live_db(db)
        await _register_live_ds(storage, db)
        await storage.save_dataset(SlayerDataset(
            name="m", sql="SELECT id FROM orders", data_source="livedb",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "m", "data_source": "livedb",
            "sql": "SELECT nope FROM ghosts",
        })
        assert "error" in result.lower()
        model = await storage.get_dataset("m", data_source="livedb")
        assert model.sql == "SELECT id FROM orders"

    async def test_edit_sql_to_valid_succeeds(
        self, mcp_server, storage: YAMLStorage, tmp_path
    ) -> None:
        db = str(tmp_path / "live.db")
        _seed_live_db(db)
        await _register_live_ds(storage, db)
        await storage.save_dataset(SlayerDataset(
            name="m", sql="SELECT id FROM orders", data_source="livedb",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT)],
        ))
        result = await _call(mcp_server, name="edit_dataset", arguments={
            "dataset_name": "m", "data_source": "livedb",
            "sql": "SELECT amount FROM orders",
        })
        assert '"success": true' in result
        model = await storage.get_dataset("m", data_source="livedb")
        assert model.sql == "SELECT amount FROM orders"


class TestDatabaseFailuresAreToolErrors:
    """A database failure in these tools is an MCP tool error carrying the friendly text."""

    @pytest.fixture
    async def db_storage(self, tmp_path) -> YAMLStorage:
        db = str(tmp_path / "shop.db")
        with transaction(db) as con:
            con.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount REAL)")
            con.execute("INSERT INTO orders VALUES (1, 2.5)")
        storage = YAMLStorage(base_dir=str(tmp_path / "store"))
        await storage.save_datasource(DatasourceConfig(name="shop", type="sqlite", database=db))
        await storage.save_datasource(DatasourceConfig(
            name="down", type="postgres", host="127.0.0.1", port=free_port(), database="x", username="u", password="p",
        ))
        await storage.save_dataset(SlayerDataset(
            name="orders", sql_table="orders", data_source="shop",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="broken", sql="no_such_fn(amount)", datatype=DataType.FLOAT),
            ],
        ))
        await storage.save_dataset(SlayerDataset(
            name="remote", sql_table="remote", data_source="down",
            fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
        ))
        return storage

    async def _call_tool(self, storage: YAMLStorage, name: str, arguments: dict[str, Any]):
        server: Any = create_mcp_server(storage=storage, _seed_help=False)
        try:
            async with create_connected_server_and_client_session(server) as session:
                return await session.call_tool(name, arguments)
        finally:
            await server._slayer_engine.aclose()

    @staticmethod
    def _assert_database_tool_error(result) -> None:
        assert result.isError
        assert "Database error:" in result.content[0].text

    async def test_query_rejected_by_the_database(self, db_storage: YAMLStorage) -> None:
        result = await self._call_tool(db_storage, "query", {"query": {"population": "orders", "metrics": ["sum(broken)"]}})
        self._assert_database_tool_error(result)

    async def test_validate_models_database_unreachable(self, db_storage: YAMLStorage) -> None:
        self._assert_database_tool_error(await self._call_tool(db_storage, "validate_datasets", {"data_source": "down"}))

    async def test_validate_models_rejected_sql_is_a_finding(self, db_storage: YAMLStorage) -> None:
        await db_storage.save_dataset(SlayerDataset(
            name="bad", sql="SELECT id, NO_SUCH_FUNCTION(amount) AS amount FROM orders", data_source="shop",
            fields=[DatasetField(name="id", sql="id", datatype=DataType.FLOAT, primary_key=True)],
        ))
        result = await self._call_tool(db_storage, "validate_datasets", {"data_source": "shop"})
        assert not result.isError
        content = result.content[0]
        assert isinstance(content, TextContent)
        causes = {entry.get("dataset_name"): entry.get("cause") for entry in json.loads(content.text)}
        assert causes["bad"] == "invalid_sql"

    async def test_ingest_database_unreachable(self, db_storage: YAMLStorage) -> None:
        self._assert_database_tool_error(
            await self._call_tool(db_storage, "ingest_datasource", {"datasource_name": "down"}),
        )

    @pytest.mark.parametrize(("method", "arguments"), [
        ("save_dataset", {"name": "m", "sql_table": "orders", "data_source": "down",
                        "fields": [{"name": "id", "sql": "id", "datatype": "number"}]}),
        ("create_dataset_from_query", {"name": "m", "query": {"population": "orders", "metrics": ["count(*)"]}}),
    ], ids=["table", "query"])
    async def test_create_model_database_unreachable(
        self, db_storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch, method: str, arguments: dict[str, Any],
    ) -> None:
        async def unreachable(*args, **kwargs):
            try:
                raise sa_exc.OperationalError("SELECT 1", {}, Exception("connection refused"))
            except sa_exc.OperationalError as exc:
                raise DatabaseUnavailable("connection refused") from exc

        monkeypatch.setattr(SlayerQueryEngine, method, unreachable)
        self._assert_database_tool_error(await self._call_tool(db_storage, "create_dataset", arguments))
