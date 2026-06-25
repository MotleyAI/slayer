"""Integration tests for MCP ``inspect_model`` against a real SQLite database.

Exercises the bits of ``inspect_model`` that need a live DB: row count, the
per-dim profile (distinct values + batched min/max), and end-to-end markdown
output.

Run with: poetry run pytest tests/integration/test_mcp_inspect.py -m integration
"""

import sqlite3
from typing import Any

import pytest

from slayer.core.enums import DataType
from slayer.core.models import (
    Column,
    DatasourceConfig,
    SlayerModel,
)
from slayer.engine.profiling import _collect_dim_profile
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import (
    _collect_measure_profile,
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
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT, label="Status", description="Order state"),
            Column(name="is_paid", type=DataType.BOOLEAN),
            Column(name="amount", sql="amount", type=DataType.DOUBLE, description="Revenue per order"),
            Column(name="quantity", sql="quantity", type=DataType.INT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            Column(name="notes", type=DataType.TEXT),
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
            columns=[Column(name="id", type=DataType.INT, primary_key=True)],
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
        assert status.type_str == "TEXT"
        assert status.distinct_count == 3
        assert set(status.values or []) == {"completed", "pending", "cancelled"}
        assert status.min_value is None
        assert status.max_value is None

        is_paid = by_name["is_paid"]
        assert is_paid.type_str == "BOOLEAN"
        assert is_paid.distinct_count == 2

    async def test_numeric_and_temporal_min_max(self, env) -> None:
        """number/date/time dims get min/max via the batched query."""
        profile = await _collect_dim_profile(model=env["model"], engine=env["engine"])
        by_name = {e.name: e for e in profile}

        amt = by_name["amount"]
        # DEV-1361: SQLite REAL columns now narrow to DOUBLE.
        assert amt.type_str == "DOUBLE"
        assert amt.values is None
        assert float(amt.min_value) == 25.0
        assert float(amt.max_value) == 300.0

        ordered_at = by_name["ordered_at"]
        assert ordered_at.type_str == "TIMESTAMP"
        # SQLite returns strings for TEXT timestamps — both ends populate
        assert str(ordered_at.min_value).startswith("2025-01-15")
        assert str(ordered_at.max_value).startswith("2025-03-20")

    async def test_high_cardinality_overflow(self, tmp_path) -> None:
        """A string dim with > 50 distinct values yields the overflow marker.

        DEV-1480: cap raised from 20 to 50, so this test now uses 60 rows so
        it remains in the overflow regime.
        """
        db_path = tmp_path / "hc.db"
        conn = sqlite3.connect(str(db_path))
        conn.cursor().execute("CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)")
        conn.executemany(
            "INSERT INTO t(label) VALUES (?)",
            [(f"v{i:03d}",) for i in range(60)],
        )
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        await storage.save_datasource(DatasourceConfig(
            name="hc_ds", type="sqlite", database=str(db_path),
        ))
        model = SlayerModel(
            name="t", sql_table="t", data_source="hc_ds",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="label", type=DataType.TEXT),
            ],
        )
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)
        profile = await _collect_dim_profile(model=model, engine=engine)
        label_entry = next(e for e in profile if e.name == "label")
        # Overflow contract on the internal _DimProfileEntry shape:
        # values=None, distinct_count=None signal overflow to the caller.
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
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="status", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
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

    async def _call(self, server: Any, *, name: str, arguments: dict | None = None) -> str:
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

        # Columns table (7 declared; v2 has a unified columns table).
        # The `sampled` column is folded in, so the same section now carries the
        # enumerated values for string/boolean cols and `min .. max` for numeric
        # and temporal cols.
        assert "## Columns (7)" in result
        assert "| status |" in result

        col_section = result.split("## Columns")[1].split("## Measures")[0]
        # The sampled column carries the profile data inline now.
        # status (string, 3 distinct) enumerates its values
        assert "completed" in col_section
        assert "pending" in col_section
        assert "cancelled" in col_section
        # is_paid (boolean) is in the column table; sample values render
        assert "| is_paid |" in col_section
        # amount (number) shows as "<min> .. <max>"
        assert " .. " in col_section
        # ordered_at (timestamp) is present in the columns section
        assert "ordered_at" in col_section
        # The sampled column header appears
        assert "| sampled |" in col_section

        # Measures table (formula list — empty by default in v2)
        assert "## Measures (0)" in result
        # Per-column description for revenue lives in the Columns table now.
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


class TestInspectModelSectionGatingIntegration:
    """End-to-end ``sections``/``descriptions_max_chars`` against a real DB.

    These tests confirm the gating semantics work against a live datasource
    (not just mocked storage), so the column-profile / sample-query
    short-circuits actually take effect when sections are dropped.
    """

    async def _call(self, server: Any, *, name: str, arguments: dict | None = None) -> str:
        content, _ = await server.call_tool(name=name, arguments=arguments or {})
        return content[0].text

    async def test_columns_only_short_circuits_samples(self, env) -> None:
        """sections=['columns'] keeps the columns table populated and skips
        sample-data entirely; the footer documents what was trimmed."""
        server = create_mcp_server(storage=env["storage"])
        result = await self._call(
            server, name="inspect_model",
            arguments={"model_name": "orders", "sections": ["columns"]},
        )
        # Columns full table is rendered — `sampled` column populated by the
        # live profile query (proves columns-section is fully included).
        assert "## Columns (7)" in result
        col_section = result.split("## Columns")[1]
        # Profile data still in the columns table when columns is included
        assert "completed" in col_section
        # No sample data section; the reachable-via-joins heading was removed
        # entirely in DEV-1560 and must never appear.
        assert "## Sample Data" not in result
        assert "## Reachable" not in result
        # Empty list-only headings for the rest are OK (model has no measures /
        # aggregations / joins, so they render nothing); footer should still
        # document what was omitted.
        assert "> Sections shown: columns." in result
        # ``learnings`` joined the omittable section list when DEV-1357
        # landed; ``reachable_fields`` was removed in DEV-1560.
        assert "> Omitted: samples, learnings." in result

    async def test_descriptions_max_chars_truncates_in_columns_table(self, env) -> None:
        """descriptions_max_chars trims long descriptions and appends the marker."""
        server = create_mcp_server(storage=env["storage"])
        result = await self._call(
            server, name="inspect_model",
            arguments={
                "model_name": "orders",
                "descriptions_max_chars": 5,
                "sections": ["columns"],
            },
        )
        # Long description "Revenue per order" (17 chars) → truncates to 5 + marker
        assert "Reven ... [truncated]" in result
        # The column itself still renders
        assert "| amount |" in result


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
            columns=[Column(name="id", type=DataType.INT, primary_key=True),

                Column(name="label", sql="label", type=DataType.TEXT),
                Column(name="price", sql="price", type=DataType.DOUBLE),
            ],
        )
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)

        types = await engine.get_column_types(model_name="t")
        assert types["label"] == "string"
        assert types["price"] == "number"

    async def test_type_appears_in_inspect_model(self, env) -> None:
        """inspect_model columns table includes a type column with declared types."""
        server = create_mcp_server(storage=env["storage"])
        content, _ = await server.call_tool(
            name="inspect_model", arguments={"model_name": "orders", "num_rows": 0},
        )
        result = content[0].text
        columns_section = result.split("## Columns")[1].split("##")[0]
        assert "| type |" in columns_section
        # DEV-1361: number → DOUBLE in the new sqlglot-aligned vocabulary.
        assert "DOUBLE" in columns_section

    async def test_measure_sampled_shows_min_max(self, env) -> None:
        """Measures with data show min .. max in the sampled column."""
        from slayer.mcp.server import _collect_measure_profile

        profile = await _collect_measure_profile(model=env["model"], engine=env["engine"])
        # amount: REAL values 25.0 .. 300.0
        assert "25" in profile["amount"]
        assert "300" in profile["amount"]
        assert ".." in profile["amount"]
        # quantity: INTEGER values 1 .. 6
        assert "1" in profile["quantity"]
        assert "6" in profile["quantity"]

    async def test_measure_sampled_all_null(self, tmp_path) -> None:
        """Measures with all-NULL data show 'all NULL' in the sampled column."""
        import sqlite3 as _sqlite3
        from slayer.mcp.server import _collect_measure_profile

        db_path = tmp_path / "nulls.db"
        conn = _sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val REAL)")
        conn.execute("INSERT INTO t VALUES (1, NULL)")
        conn.execute("INSERT INTO t VALUES (2, NULL)")
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        await storage.save_datasource(DatasourceConfig(
            name="null_ds", type="sqlite", database=str(db_path),
        ))
        model = SlayerModel(
            name="t", sql_table="t", data_source="null_ds",
            columns=[Column(name="id", type=DataType.INT, primary_key=True),
Column(name="val", sql="val", type=DataType.DOUBLE)
            ],
        )
        await storage.save_model(model)
        engine = SlayerQueryEngine(storage=storage)

        profile = await _collect_measure_profile(model=model, engine=engine)
        assert profile["val"] == "all NULL"


class TestStringAggregationRejection:
    """Validation: numeric-only aggregations on string measures are rejected
    during query enrichment, before SQL is generated or executed.

    The orders fixture has a string column `status` that auto-ingestion also
    exposes as a measure (one measure per non-ID column is the default).
    Triggering ``status:sum`` / ``status:avg`` etc. should produce a clear
    ValueError, not a database-level type error."""

    async def _run(self, env, formula: str) -> None:
        from slayer.core.query import SlayerQuery

        q = SlayerQuery.model_validate({
            "source_model": "orders",
            "measures": [{"formula": formula}],
        })
        await env["engine"].execute(query=q)

    @pytest.mark.parametrize("agg", ["sum", "avg", "median"])
    async def test_numeric_only_aggregations_rejected_on_string(self, env, agg: str) -> None:
        with pytest.raises(ValueError, match="is not applicable to TEXT column"):
            await self._run(env, f"status:{agg}")

    async def test_min_max_allowed_on_string(self, env) -> None:
        """min/max work on strings (alphabetical ordering) — should pass."""
        from slayer.core.query import SlayerQuery

        q = SlayerQuery.model_validate({
            "source_model": "orders",
            "measures": [{"formula": "status:min"}, {"formula": "status:max"}],
        })
        result = await env["engine"].execute(query=q)
        assert result.data  # executed without error

    async def test_count_and_count_distinct_allowed_on_string(self, env) -> None:
        """count/count_distinct always work regardless of type."""
        from slayer.core.query import SlayerQuery

        q = SlayerQuery.model_validate({
            "source_model": "orders",
            "measures": [{"formula": "status:count"}, {"formula": "status:count_distinct"}],
        })
        result = await env["engine"].execute(query=q)
        assert result.data

    async def test_numeric_aggregations_allowed_on_numeric_measure(self, env) -> None:
        """avg/sum on the numeric `amount` measure must still work."""
        from slayer.core.query import SlayerQuery

        q = SlayerQuery.model_validate({
            "source_model": "orders",
            "measures": [{"formula": "amount:sum"}, {"formula": "amount:avg"}],
        })
        result = await env["engine"].execute(query=q)
        assert result.data


class TestPrimaryKeyAggregationRule:
    """v2 contract: primary-key columns are restricted to count/count_distinct
    regardless of type or any explicit ``allowed_aggregations`` whitelist."""

    async def test_sum_on_pk_rejected(self, env) -> None:
        """`:sum` on a numeric primary-key column is rejected at enrichment."""
        from slayer.core.query import SlayerQuery

        q = SlayerQuery.model_validate({
            "source_model": "orders",
            "measures": [{"formula": "id:sum"}],
        })
        with pytest.raises(ValueError, match="primary-key column"):
            await env["engine"].execute(query=q)

    async def test_count_on_pk_allowed(self, env) -> None:
        """`:count` on a primary-key column is always allowed."""
        from slayer.core.query import SlayerQuery

        q = SlayerQuery.model_validate({
            "source_model": "orders",
            "measures": [{"formula": "id:count"}],
        })
        result = await env["engine"].execute(query=q)
        assert result.data


class TestIngestDatasourceModelsTool:
    """Pin the success-message format of the ``ingest_datasource_models`` MCP
    tool. Pre-v2 the message read ``X dims`` and referenced the long-removed
    ``SlayerModel.dimensions`` attribute, which would AttributeError on every
    successful ingest. v2 unifies dims+measures into ``columns``.
    """

    async def test_success_message_uses_columns_not_dims(self, tmp_path) -> None:
        db_path = tmp_path / "ingest.db"
        conn = sqlite3.connect(str(db_path))
        conn.cursor().execute(
            "CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT, qty INTEGER)"
        )
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        await storage.save_datasource(DatasourceConfig(
            name="ingest_ds", type="sqlite", database=str(db_path),
        ))

        server = create_mcp_server(storage=storage)
        content, _ = await server.call_tool(
            name="ingest_datasource_models",
            arguments={"datasource_name": "ingest_ds"},
        )
        text = content[0].text

        # DEV-1356 idempotent ingest renders "Created N new model(s):" for new
        # tables (replaces the legacy "Ingested N model(s):" wording).
        assert "Created 1 new model(s):" in text
        assert "widgets" in text
        assert "columns" in text  # v2 wording
        assert "dims" not in text  # v1 leftover would crash before reaching here


# ---------------------------------------------------------------------------
# DEV-1480: structured sampled_values + distinct_count
# ---------------------------------------------------------------------------


class TestInspectModelSampledValuesAndDistinctCount:
    """End-to-end DEV-1480 contract through ``inspect_model``."""

    async def _call_json(self, server, *, model_name: str) -> dict:
        import json as _json
        content, _ = await server.call_tool(
            name="inspect_model",
            arguments={"model_name": model_name, "format": "json"},
        )
        return _json.loads(content[0].text)

    async def test_json_output_includes_sampled_values_and_distinct_count(
        self, env,
    ) -> None:
        """JSON inspect_model output carries both new fields per column."""
        server = create_mcp_server(storage=env["storage"])
        payload = await self._call_json(server, model_name="orders")
        cols_by_name = {c["name"]: c for c in payload["columns"]}
        # Categorical column: both new fields populated.
        status = cols_by_name["status"]
        assert "sampled_values" in status
        assert "distinct_count" in status
        assert isinstance(status["sampled_values"], list)
        assert status["distinct_count"] == 3
        # Numeric column: both new fields None.
        amount = cols_by_name["amount"]
        assert amount["sampled_values"] is None
        assert amount["distinct_count"] is None

    async def test_markdown_table_unchanged_no_new_column(self, env) -> None:
        """The markdown ``## Columns`` table must not gain new headers — the
        issue explicitly says the text format does not change. New data is
        carried only in JSON (and in the persisted model)."""
        server = create_mcp_server(storage=env["storage"])
        content, _ = await server.call_tool(
            name="inspect_model",
            arguments={"model_name": "orders"},  # markdown by default
        )
        text = content[0].text
        col_section = text.split("## Columns")[1].split("## Measures")[0]
        # No new column headers in the markdown table.
        assert "| sampled_values |" not in col_section
        assert "| distinct_count |" not in col_section
        # The single ``sampled`` header is still present.
        assert "| sampled |" in col_section

    async def test_v6_stale_cache_re_profiles_to_populate_sampled_values(
        self, env,
    ) -> None:
        """A v6 model with ``sampled`` set but no ``sampled_values`` is
        cache-miss and re-profiles on next ``inspect_model``. Pins
        ``_is_sample_cached`` validity rule."""
        storage = env["storage"]
        # Simulate a v6-era persisted state: ``sampled`` set, structured field
        # absent. Via storage so the on-disk dict matches.
        await storage.update_column_sampled(
            data_source="test_sqlite", model_name="orders",
            column_name="status",
            sampled="legacy text from v6",
            sampled_values=None,
            distinct_count=None,
        )
        server = create_mcp_server(storage=storage)
        await self._call_json(server, model_name="orders")
        # After inspect_model runs, the structured field has been populated.
        reloaded = await storage.get_model("orders", data_source="test_sqlite")
        status = reloaded.get_column("status")
        assert status.sampled_values is not None
        assert status.distinct_count == 3

    async def test_all_null_categorical_outputs_empty_string_not_all_null(
        self, tmp_path,
    ) -> None:
        """All-NULL categorical column: text ``sampled=""``, NOT ``"all NULL"``
        (the latter is the numeric-fallback marker). JSON shows the structured
        ``sampled_values=[]``."""
        db_path = tmp_path / "nulls.db"
        conn = sqlite3.connect(str(db_path))
        conn.cursor().execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, notes TEXT)"
        )
        conn.executemany(
            "INSERT INTO t(id, notes) VALUES (?, ?)",
            [(i, None) for i in range(1, 6)],
        )
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        await storage.save_datasource(DatasourceConfig(
            name="nulls_ds", type="sqlite", database=str(db_path),
        ))
        model = SlayerModel(
            name="t", sql_table="t", data_source="nulls_ds",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="notes", type=DataType.TEXT),
            ],
        )
        await storage.save_model(model)

        server = create_mcp_server(storage=storage)
        import json as _json
        content, _ = await server.call_tool(
            name="inspect_model",
            arguments={"model_name": "t", "format": "json"},
        )
        payload = _json.loads(content[0].text)
        notes = next(c for c in payload["columns"] if c["name"] == "notes")
        # All-NULL categorical column contract:
        assert notes["sampled_values"] == []
        assert notes["distinct_count"] == 0
        # Text sampled is empty string, NOT the numeric-fallback "all NULL".
        assert notes["sampled"] == ""

    async def test_markdown_table_caps_sampled_at_20_values(
        self, tmp_path,
    ) -> None:
        """DEV-1516: per-column markdown rendering must still show at most
        20 values per column (via the persisted ``Column.sampled`` text)
        even when ``sampled_values`` holds the full 50. The markdown table
        is the all-columns-at-once surface and stays readable."""
        db_path = tmp_path / "wide_values.db"
        conn = sqlite3.connect(str(db_path))
        conn.cursor().execute(
            "CREATE TABLE many_values (id INTEGER PRIMARY KEY, kind TEXT)"
        )
        # Insert 30 distinct categorical values. <= 50 so no overflow, but
        # > 20 so the markdown text cap kicks in.
        conn.executemany(
            "INSERT INTO many_values VALUES (?, ?)",
            [(i, f"k_{i:02d}") for i in range(1, 31)],
        )
        conn.commit()
        conn.close()
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        await storage.save_datasource(DatasourceConfig(
            name="mv_ds", type="sqlite", database=str(db_path),
        ))
        await storage.save_model(SlayerModel(
            name="many_values", sql_table="many_values", data_source="mv_ds",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="kind", type=DataType.TEXT),
            ],
        ))
        server = create_mcp_server(storage=storage)
        content, _ = await server.call_tool(
            name="inspect_model",
            arguments={"model_name": "many_values"},  # markdown
        )
        text = content[0].text
        col_section = text.split("## Columns")[1]
        kind_row_start = col_section.find("| kind ")
        if kind_row_start < 0:
            pytest.fail("``kind`` row not found in markdown ## Columns table")
        # Scan only the kind row (terminated by newline).
        kind_row = col_section[kind_row_start:col_section.find("\n", kind_row_start)]
        # Codex round-3 finding #9: positive AND negative assertions.
        # Values within the cap MUST appear (the cell isn't empty).
        assert "k_01" in kind_row, (
            "markdown table is missing in-cap sample values — sampled cell "
            "appears empty"
        )
        assert "k_20" in kind_row, (
            "markdown table dropped a top-20 sample value"
        )
        # Values past the 20-cap MUST NOT appear.
        assert "k_21" not in kind_row, (
            "DEV-1516 regression: markdown table leaked the 21st sample value "
            "for one column — the 20-cap is broken."
        )
        assert "k_29" not in kind_row, (
            "DEV-1516 regression: markdown table leaked the 29th sample value "
            "for one column. The full 50 belongs only on per-column search "
            "hits, not the all-columns inspect_model markdown surface."
        )
        # And the persisted ``sampled_values`` still carries the full set.
        reloaded = await storage.get_model("many_values", data_source="mv_ds")
        assert reloaded is not None
        kind_col = reloaded.get_column("kind")
        assert kind_col is not None
        assert kind_col.sampled_values is not None
        assert len(kind_col.sampled_values) == 30

    async def test_legacy_sampled_text_preserved_in_markdown_on_profile_failure(
        self, env, monkeypatch,
    ) -> None:
        """Codex finding #6: when ``inspect_model``'s refactor delegates to
        ``ensure_column_sample_fresh`` and ``profile_column`` raises, the
        markdown column for ``sampled`` must still show the LEGACY persisted
        text. The helper returns the input column on failure (which still
        carries the legacy ``sampled`` set), so the markdown rendering
        reads the legacy value from there. Don't surface an empty cell."""
        storage = env["storage"]
        # Pre-populate a v6-style legacy state: ``sampled`` set, no list.
        await storage.update_column_sampled(
            data_source="test_sqlite", model_name="orders",
            column_name="status",
            sampled="legacy text from v6",
            sampled_values=None,
            distinct_count=None,
        )

        # Force every profile_column call to fail.
        async def explodes(**_kwargs):
            raise RuntimeError("simulated profile failure")

        monkeypatch.setattr(
            "slayer.engine.profiling.profile_column", explodes,
        )

        server = create_mcp_server(storage=storage)
        content, _ = await server.call_tool(
            name="inspect_model",
            arguments={"model_name": "orders"},  # markdown by default
        )
        text = content[0].text
        col_section = text.split("## Columns")[1]
        status_row_start = col_section.find("| status ")
        assert status_row_start >= 0
        status_row = col_section[status_row_start:col_section.find("\n", status_row_start)]
        # Legacy text MUST appear in the cell so the agent still sees data.
        assert "legacy text from v6" in status_row, (
            "Codex finding #6: legacy ``Column.sampled`` text must survive "
            "a profile_column failure in inspect_model. The refactored "
            "helper returns the INPUT column on failure (which carries "
            "the legacy text); the markdown rendering must read from "
            "there. An empty cell is a regression."
        )

    async def test_profiles_more_than_10_categorical_columns(
        self, tmp_path,
    ) -> None:
        """``inspect_model`` must persist ``sampled`` for every categorical
        column, not just the first 10. Pins the ``max_dims`` cap removal on
        the persistence path."""
        db_path = tmp_path / "wide.db"
        col_count = 15
        col_defs = ", ".join(f"c{i} TEXT" for i in range(col_count))
        conn = sqlite3.connect(str(db_path))
        conn.cursor().execute(
            f"CREATE TABLE wide (id INTEGER PRIMARY KEY, {col_defs})"
        )
        placeholders = ", ".join(["?"] * (col_count + 1))
        row_value = ("val",) * col_count
        conn.execute(
            f"INSERT INTO wide VALUES ({placeholders})",
            (1, *row_value),
        )
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        await storage.save_datasource(DatasourceConfig(
            name="wide_ds", type="sqlite", database=str(db_path),
        ))
        cols = [Column(name="id", type=DataType.INT, primary_key=True)]
        cols.extend(
            Column(name=f"c{i}", type=DataType.TEXT) for i in range(col_count)
        )
        await storage.save_model(SlayerModel(
            name="wide", sql_table="wide", data_source="wide_ds",
            columns=cols,
        ))

        server = create_mcp_server(storage=storage)
        await self._call_json(server, model_name="wide")

        reloaded = await storage.get_model("wide", data_source="wide_ds")
        # Every one of the 15 categorical columns has structured profile.
        for i in range(col_count):
            col = reloaded.get_column(f"c{i}")
            assert col.sampled_values is not None, (
                f"c{i}.sampled_values is None — max_dims cap not removed"
            )


class TestCollectMeasureProfileTypeRestriction:
    """DEV-1480: ``_collect_measure_profile`` is restricted to numeric/temporal
    columns. Text/boolean columns no longer appear in its output — they're
    served exclusively by the categorical dim profile, which now produces both
    ``sampled`` and ``sampled_values``. Mixing the two paths for the same
    column would otherwise create a permanent cache miss (cached text from
    measure_profile without the structured field)."""

    async def test_text_and_boolean_columns_skipped(self, env) -> None:
        result = await _collect_measure_profile(
            model=env["model"], engine=env["engine"],
        )
        # Text/boolean columns are not in the measure_profile output.
        assert "status" not in result
        assert "notes" not in result
        assert "is_paid" not in result
        # Numeric/temporal columns still appear.
        assert "amount" in result
        assert "quantity" in result
        assert "ordered_at" in result


class TestInspectModelEmptyStringSampledNotClobberedByFallback:
    """DEV-1480: pin the row-construction "key-presence not truthiness" fix.

    An all-NULL categorical column produces ``sampled=""`` from the dim profile.
    If row construction uses ``profile_by_name.get(c.name) or measure_profile.get(c.name)``,
    the empty string falls through to the measure_profile fallback. Even with
    the type-restriction on ``_collect_measure_profile``, we test the row
    construction in isolation by monkeypatching the fallback to inject a value
    for the same column — the empty string must survive.
    """

    async def test_empty_string_sampled_survives_fallback_injection(
        self, tmp_path, monkeypatch,
    ) -> None:
        db_path = tmp_path / "nulls.db"
        conn = sqlite3.connect(str(db_path))
        conn.cursor().execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, notes TEXT)"
        )
        conn.executemany(
            "INSERT INTO t(id, notes) VALUES (?, ?)",
            [(i, None) for i in range(1, 6)],
        )
        conn.commit()
        conn.close()

        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        await storage.save_datasource(DatasourceConfig(
            name="nulls_ds", type="sqlite", database=str(db_path),
        ))
        await storage.save_model(SlayerModel(
            name="t", sql_table="t", data_source="nulls_ds",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="notes", type=DataType.TEXT),
            ],
        ))

        # Inject a "fallback" value for the categorical column so the
        # truthiness ``or`` bug would silently swap "" → "FALLBACK".
        from slayer.mcp import server as mcp_server

        async def injected_measure_profile(*, model, engine):  # noqa: ARG001  # NOSONAR(S7503) — must be async to replace _collect_measure_profile which the production caller awaits
            return {"notes": "FALLBACK_VALUE_SHOULD_NOT_APPEAR"}

        monkeypatch.setattr(
            mcp_server, "_collect_measure_profile",
            injected_measure_profile,
        )

        server = create_mcp_server(storage=storage)
        import json as _json
        content, _ = await server.call_tool(
            name="inspect_model",
            arguments={"model_name": "t", "format": "json"},
        )
        payload = _json.loads(content[0].text)
        notes = next(c for c in payload["columns"] if c["name"] == "notes")
        # The empty-string ``sampled`` from the dim profile must NOT be
        # overwritten by the injected fallback. Pins the key-presence fix.
        assert notes["sampled"] == ""
        assert "FALLBACK" not in str(notes["sampled"] or "")
