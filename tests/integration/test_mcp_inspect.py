"""Integration tests for MCP ``inspect_model`` against a real SQLite database.

Exercises the bits of ``inspect_model`` that need a live DB: row count, the
per-dim profile (distinct values + batched min/max), and end-to-end markdown
output.

Run with: poetry run pytest tests/integration/test_mcp_inspect.py -m integration
"""

import sqlite3
from typing import Any, Optional

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    DatasourceConfig,
    Dimension,
    Measure,
    SlayerModel,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import (
    _collect_dim_profile,
    _get_row_count,
    create_mcp_server,
)
from slayer.storage.yaml_storage import YAMLStorage

pytestmark = pytest.mark.integration


@pytest.fixture
async def env(tmp_path):
    """Real SQLite DB + YAMLStorage + a saved ``orders`` model."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            status TEXT NOT NULL,
            is_paid INTEGER NOT NULL,
            amount REAL NOT NULL,
            quantity INTEGER NOT NULL,
            ordered_at TEXT NOT NULL,
            notes TEXT
        )
        """
    )
    rows = [
        (1, "completed", 1, 100.0, 2, "2025-01-15 09:00:00", "first"),
        (2, "completed", 1, 250.0, 5, "2025-01-20 14:30:00", "second"),
        (3, "pending",   0, 50.0,  1, "2025-02-10 11:15:00", None),
        (4, "cancelled", 0, 75.0,  3, "2025-02-15 16:45:00", "cancelled"),
        (5, "completed", 1, 300.0, 6, "2025-03-05 08:00:00", None),
        (6, "pending",   0, 25.0,  1, "2025-03-20 20:10:00", "small"),
    ]
    cur.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
    conn.commit()
    conn.close()

    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    ds = DatasourceConfig(name="test_sqlite", type="sqlite", database=str(db_path))
    await storage.save_datasource(ds)

    model = SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="test_sqlite",
        description="Orders model used in integration tests.",
        dimensions=[
            Dimension(name="id", type=DataType.NUMBER, primary_key=True),
            Dimension(name="status", type=DataType.STRING, label="Status", description="Order state"),
            Dimension(name="is_paid", type=DataType.BOOLEAN),
            Dimension(name="amount", type=DataType.NUMBER),
            Dimension(name="quantity", type=DataType.NUMBER),
            Dimension(name="ordered_at", type=DataType.TIMESTAMP),
            Dimension(name="notes", type=DataType.STRING),
        ],
        measures=[
            Measure(name="amount", sql="amount", description="Revenue per order"),
            Measure(name="quantity", sql="quantity"),
        ],
    )
    await storage.save_model(model)

    engine = SlayerQueryEngine(storage=storage)
    return {"storage": storage, "engine": engine, "model": model}


class TestDescribeDatasourceTables:
    """Integration-level tests for the table-listing behaviour that's now part
    of describe_datasource (was formerly the separate list_tables tool)."""

    async def _call_describe(
        self, server, *, name: str, list_tables: bool = True, schema_name: str = "",
    ) -> str:
        content, _ = await server.call_tool(
            name="describe_datasource",
            arguments={"name": name, "list_tables": list_tables, "schema_name": schema_name},
        )
        return content[0].text

    async def test_tables_appear_by_default(self, env) -> None:
        server = create_mcp_server(storage=env["storage"])
        out = await self._call_describe(server, name="test_sqlite")
        assert "Tables (1):" in out
        assert "  - orders" in out

    async def test_list_tables_false_suppresses_section(self, env) -> None:
        server = create_mcp_server(storage=env["storage"])
        out = await self._call_describe(server, name="test_sqlite", list_tables=False)
        assert "Tables" not in out.split("Connection:")[1]

    async def test_schema_name_is_forwarded(self, env) -> None:
        """An unknown schema name is tolerated — error surfaces inline, rest of
        the response still renders."""
        server = create_mcp_server(storage=env["storage"])
        out = await self._call_describe(server, name="test_sqlite", schema_name="nope")
        # Still got the connection header
        assert "Datasource: test_sqlite" in out
        assert "Connection: OK" in out
        # And something table-related — either "No tables found in schema 'nope'"
        # or a DB-specific error; both are acceptable outcomes of the probe.
        assert "nope" in out


class TestGetRowCount:
    async def test_non_empty_table(self, env) -> None:
        count = await _get_row_count(model=env["model"], engine=env["engine"])
        assert count == 6

    async def test_empty_table(self, tmp_path) -> None:
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(str(db_path))
        conn.cursor().execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        await storage.save_datasource(DatasourceConfig(
            name="empty_ds", type="sqlite", database=str(db_path),
        ))
        model = SlayerModel(
            name="t", sql_table="t", data_source="empty_ds",
            dimensions=[Dimension(name="id", type=DataType.NUMBER, primary_key=True)],
        )
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        assert await _get_row_count(model=model, engine=engine) == 0


class TestCollectDimProfile:
    async def test_categorical_enumerated(self, env) -> None:
        """string/boolean dims get distinct values with counts."""
        profile = await _collect_dim_profile(model=env["model"], engine=env["engine"])
        by_name = {e.name: e for e in profile}

        status = by_name["status"]
        assert status.type_str == "string"
        assert status.distinct_count == 3
        assert set(status.values or []) == {"completed", "pending", "cancelled"}
        assert status.min_value is None
        assert status.max_value is None

        is_paid = by_name["is_paid"]
        assert is_paid.type_str == "boolean"
        assert is_paid.distinct_count == 2

    async def test_numeric_and_temporal_min_max(self, env) -> None:
        """number/date/time dims get min/max via the batched query."""
        profile = await _collect_dim_profile(model=env["model"], engine=env["engine"])
        by_name = {e.name: e for e in profile}

        amt = by_name["amount"]
        assert amt.type_str == "number"
        assert amt.values is None
        assert float(amt.min_value) == 25.0
        assert float(amt.max_value) == 300.0

        ordered_at = by_name["ordered_at"]
        assert ordered_at.type_str == "time"
        # SQLite returns strings for TEXT timestamps — both ends populate
        assert str(ordered_at.min_value).startswith("2025-01-15")
        assert str(ordered_at.max_value).startswith("2025-03-20")

    async def test_high_cardinality_overflow(self, tmp_path) -> None:
        """A string dim with > 20 distinct values yields the overflow marker."""
        db_path = tmp_path / "hc.db"
        conn = sqlite3.connect(str(db_path))
        conn.cursor().execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO t(label) VALUES (?)",
            [(f"v{i}",) for i in range(50)],
        )
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        await storage.save_datasource(DatasourceConfig(
            name="hc_ds", type="sqlite", database=str(db_path),
        ))
        model = SlayerModel(
            name="t", sql_table="t", data_source="hc_ds",
            dimensions=[
                Dimension(name="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="label", type=DataType.STRING),
            ],
        )
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        profile = await _collect_dim_profile(model=model, engine=engine)
        label_entry = next(e for e in profile if e.name == "label")
        assert label_entry.values is None
        assert label_entry.distinct_count is None  # overflow signal

    async def test_empty_table_produces_no_entries(self, tmp_path) -> None:
        db_path = tmp_path / "empty.db"
        conn = sqlite3.connect(str(db_path))
        conn.cursor().execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT, amount REAL)"
        )
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        await storage.save_datasource(DatasourceConfig(
            name="empty_ds2", type="sqlite", database=str(db_path),
        ))
        model = SlayerModel(
            name="t", sql_table="t", data_source="empty_ds2",
            dimensions=[
                Dimension(name="id", type=DataType.NUMBER, primary_key=True),
                Dimension(name="status", type=DataType.STRING),
                Dimension(name="amount", type=DataType.NUMBER),
            ],
        )
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)

        profile = await _collect_dim_profile(model=model, engine=engine)
        # Categorical dim on empty table returns 0 distinct values (not overflow).
        status_entries = [e for e in profile if e.name == "status"]
        assert len(status_entries) == 1
        assert status_entries[0].distinct_count == 0
        assert status_entries[0].values == []
        # Numeric min/max against empty table → both None → entry omitted.
        assert not any(e.name == "amount" for e in profile)


class TestInspectModelEndToEnd:
    """Full ``inspect_model`` run against the SQLite fixture — confirms every
    section of the markdown output is produced end-to-end."""

    async def _call(self, server: Any, *, name: str, arguments: Optional[dict] = None) -> str:
        content, _ = await server.call_tool(name=name, arguments=arguments or {})
        return content[0].text

    async def test_full_response(self, env) -> None:
        server = create_mcp_server(storage=env["storage"])
        result = await self._call(server, name="inspect_model", arguments={"model_name": "orders", "num_rows": 5})

        # Header + description + metadata
        assert result.startswith("# Model: `orders`")
        assert "Orders model used in integration tests." in result
        assert "**data_source:** `test_sqlite`" in result
        assert "**sql_table:** `orders`" in result
        assert "**row_count:** 6" in result

        # Dimensions table (7 dims declared; pk excluded from sample but still listed).
        # The `sampled` column is folded in, so the same section now carries the
        # enumerated values for string/boolean dims and `min .. max` for numeric
        # and temporal dims.
        assert "## Dimensions (7)" in result
        assert "| status |" in result

        dim_section = result.split("## Dimensions")[1].split("## Measures")[0]
        # The sampled column carries the profile data inline now.
        # status (string, 3 distinct) enumerates its values
        assert "completed" in dim_section
        assert "pending" in dim_section
        assert "cancelled" in dim_section
        # is_paid (boolean) is in the dim table; sample values render
        assert "| is_paid |" in dim_section
        # amount (number) shows as "<min> .. <max>"
        assert " .. " in dim_section
        # ordered_at (timestamp) is present in the dimensions section
        assert "ordered_at" in dim_section
        # The sampled column header appears
        assert "| sampled |" in dim_section

        # Measures table
        assert "## Measures (2)" in result
        assert "Revenue per order" in result

        # Joins table (empty model has no joins but header always rendered)
        assert "## Joins (0)" in result

        # No standalone dim-profile section anymore
        assert "## Dimension profile" not in result

        # Sample data table: count + amount_avg + quantity_avg + 2 dim columns
        # SLayer names the *:count output '_count' when grouped by dimensions.
        assert "## Sample Data" in result
        sample_section = result.split("## Sample Data", 1)[1]
        assert "_count" in sample_section
        assert "amount_avg" in sample_section
        assert "quantity_avg" in sample_section

        # No leaked error artefacts from the old implementation
        assert "sample_data_error" not in result
        assert "Bare measure name" not in result

    async def test_no_longer_json(self, env) -> None:
        server = create_mcp_server(storage=env["storage"])
        result = await self._call(server, name="inspect_model", arguments={"model_name": "orders"})
        import json as _json
        with pytest.raises(_json.JSONDecodeError):
            _json.loads(result)


class TestMeasureTypeInference:
    """get_column_types infers measure types via LIMIT 0 against a real DB."""

    async def test_infers_numeric_types(self, env) -> None:
        """amount (REAL) and quantity (INTEGER) both infer as 'number'."""
        engine = env["engine"]
        types = await engine.get_column_types(model_name="orders")
        assert types["amount"] == "number"
        assert types["quantity"] == "number"

    async def test_string_measure_inferred(self, tmp_path) -> None:
        """A VARCHAR/TEXT measure infers as 'string'."""
        import sqlite3 as _sqlite3

        db_path = tmp_path / "types.db"
        conn = _sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT, price REAL)")
        conn.execute("INSERT INTO t VALUES (1, 'hello', 9.99)")
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        await storage.save_datasource(DatasourceConfig(
            name="types_ds", type="sqlite", database=str(db_path),
        ))
        model = SlayerModel(
            name="t", sql_table="t", data_source="types_ds",
            dimensions=[Dimension(name="id", type=DataType.NUMBER, primary_key=True)],
            measures=[
                Measure(name="label", sql="label"),
                Measure(name="price", sql="price"),
            ],
        )
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)

        types = await engine.get_column_types(model_name="t")
        assert types["label"] == "string"
        assert types["price"] == "number"

    async def test_type_appears_in_inspect_model(self, env) -> None:
        """inspect_model measures table includes a type column with inferred types."""
        server = create_mcp_server(storage=env["storage"])
        content, _ = await server.call_tool(
            name="inspect_model", arguments={"model_name": "orders", "num_rows": 0},
        )
        result = content[0].text
        measures_section = result.split("## Measures")[1].split("##")[0]
        # The type column should show "number" for both measures
        assert "| type |" in measures_section or "| number |" in measures_section
        assert "number" in measures_section


class TestStringAggregationRejection:
    """Validation: numeric-only aggregations on string measures are rejected
    during query enrichment, before SQL is generated or executed.

    The orders fixture has a string column `status` that auto-ingestion also
    exposes as a measure (one measure per non-ID column is the default).
    Triggering ``status:sum`` / ``status:avg`` etc. should produce a clear
    ValueError, not a database-level type error."""

    async def _run(self, env, formula: str) -> None:
        from slayer.core.query import SlayerQuery

        # Ensure the `status` column is also exposed as a measure (auto-ingest
        # convention). The fixture only declares amount/quantity as measures,
        # so add `status` explicitly.
        env["model"].measures.append(Measure(name="status", sql="status"))
        await env["storage"].save_model(env["model"])

        q = SlayerQuery.model_validate({
            "source_model": "orders",
            "fields": [{"formula": formula}],
        })
        await env["engine"].execute(query=q)

    @pytest.mark.parametrize("agg", ["sum", "avg", "median"])
    async def test_numeric_only_aggregations_rejected_on_string(self, env, agg: str) -> None:
        with pytest.raises(ValueError, match="is not applicable to string measure"):
            await self._run(env, f"status:{agg}")

    async def test_min_max_allowed_on_string(self, env) -> None:
        """min/max work on strings (alphabetical ordering) — should pass."""
        from slayer.core.query import SlayerQuery

        env["model"].measures.append(Measure(name="status", sql="status"))
        await env["storage"].save_model(env["model"])

        q = SlayerQuery.model_validate({
            "source_model": "orders",
            "fields": [{"formula": "status:min"}, {"formula": "status:max"}],
        })
        result = await env["engine"].execute(query=q)
        assert result.data  # executed without error

    async def test_count_and_count_distinct_allowed_on_string(self, env) -> None:
        """count/count_distinct always work regardless of type."""
        from slayer.core.query import SlayerQuery

        env["model"].measures.append(Measure(name="status", sql="status"))
        await env["storage"].save_model(env["model"])

        q = SlayerQuery.model_validate({
            "source_model": "orders",
            "fields": [{"formula": "status:count"}, {"formula": "status:count_distinct"}],
        })
        result = await env["engine"].execute(query=q)
        assert result.data

    async def test_numeric_aggregations_allowed_on_numeric_measure(self, env) -> None:
        """avg/sum on the numeric `amount` measure must still work."""
        from slayer.core.query import SlayerQuery

        q = SlayerQuery.model_validate({
            "source_model": "orders",
            "fields": [{"formula": "amount:sum"}, {"formula": "amount:avg"}],
        })
        result = await env["engine"].execute(query=q)
        assert result.data
