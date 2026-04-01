"""MCP server for SLayer."""

import json
import logging
from typing import Any, Dict, List, Optional

import sqlalchemy as sa

from slayer.core.models import DatasourceConfig, Dimension, Measure, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)

VALID_MEASURE_TYPES = {"count", "count_distinct", "sum", "avg", "min", "max", "last"}
VALID_DIMENSION_TYPES = {"string", "time", "date", "boolean", "number"}


def _parse_column_ref(value: Any) -> dict:
    """Parse a string or dict into a ColumnRef-compatible dict.

    Accepts: "name", "model.name", or {"name": "x", "model": "y"}.
    """
    if isinstance(value, dict):
        return value
    s = str(value)
    if "." in s:
        model, name = s.split(".", 1)
        return {"name": name, "model": model}
    return {"name": s}



def _normalize_time_dimensions(tds: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize time dimension dicts: convert string 'dimension' to ColumnRef dict."""
    result = []
    for td in tds:
        td = dict(td)
        if "dimension" in td and isinstance(td["dimension"], str):
            td["dimension"] = _parse_column_ref(td["dimension"])
        result.append(td)
    return result


def _normalize_order(items: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Normalize order dicts: convert string 'column' to ColumnRef dict."""
    result = []
    for item in items:
        item = dict(item)
        if "column" in item and isinstance(item["column"], str):
            item["column"] = _parse_column_ref(item["column"])
        result.append(item)
    return result


def _test_connection(ds: DatasourceConfig) -> tuple[bool, str]:
    """Test a datasource connection. Returns (success, message)."""
    try:
        conn_str = ds.resolve_env_vars().get_connection_string()
        engine = sa.create_engine(conn_str)
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        engine.dispose()
        return True, "Connection successful."
    except Exception as e:
        return False, _friendly_db_error(e)


def _get_schemas(ds: DatasourceConfig) -> list[str]:
    """List available schemas for a datasource."""
    try:
        conn_str = ds.resolve_env_vars().get_connection_string()
        engine = sa.create_engine(conn_str)
        inspector = sa.inspect(engine)
        schemas = inspector.get_schema_names()
        engine.dispose()
        return schemas
    except Exception:
        return []


def _friendly_db_error(exc: Exception) -> str:
    """Convert a database exception into a user-friendly message with hints."""
    msg = str(exc)
    # Extract the core error from SQLAlchemy wrapper
    if hasattr(exc, "orig") and exc.orig:
        msg = str(exc.orig)

    hints = []
    msg_lower = msg.lower()
    if "no password supplied" in msg_lower or "password authentication failed" in msg_lower:
        hints.append("Check that username and password are correct.")
    elif "does not exist" in msg_lower and "database" in msg_lower:
        hints.append("Verify the database name is correct.")
    elif "could not translate host" in msg_lower or "name or service not known" in msg_lower:
        hints.append("Check that the host address is correct.")
    elif "connection refused" in msg_lower:
        hints.append("Check that the database server is running and the port is correct.")
    elif "timeout" in msg_lower:
        hints.append("The database server is not responding. Check host/port and network access.")

    result = f"Database error: {msg}"
    if hints:
        result += "\nHint: " + " ".join(hints)
    return result


def _model_to_summary(model: SlayerModel) -> dict:
    """Convert a SlayerModel to a summary dict."""
    return {
        "name": model.name,
        "description": model.description,
        "dimensions": [
            {"name": d.name, "type": str(d.type), "description": d.description}
            for d in model.dimensions if not d.hidden
        ],
        "measures": [
            {"name": m.name, "type": str(m.type), "description": m.description}
            for m in model.measures if not m.hidden
        ],
    }


def create_mcp_server(storage: StorageBackend):
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError:
        raise ImportError("MCP support requires the 'mcp' extra: pip install semantic-slayer[mcp]")

    mcp = FastMCP(
        "SLayer",
        instructions=(
            "SLayer is a semantic layer for querying databases. "
            "Instead of writing SQL, describe what data you want using models, measures, dimensions, and filters. "
            "Typical workflow: datasource_summary → inspect_model → query. "
            "To connect a new database: create_datasource → describe_datasource (to verify) → ingest_datasource_models → datasource_summary."
        ),
    )
    engine = SlayerQueryEngine(storage=storage)

    @mcp.tool()
    def query(
        model: str,
        fields: Optional[List[Dict[str, str]]] = None,
        dimensions: Optional[List[str]] = None,
        filters: Optional[List[str]] = None,
        time_dimensions: Optional[List[Dict[str, Any]]] = None,
        order: Optional[List[Dict[str, str]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        whole_periods_only: bool = False,
        show_sql: bool = False,
    ) -> str:
        """Query data from a semantic model. Call inspect_model first to see available fields and dimensions.

        Args:
            model: Name of the model to query (from datasource_summary).
            fields: Data columns to return. Each is a formula: {"formula": "count"} (measure),
                {"formula": "revenue / count", "name": "aov"} (arithmetic),
                {"formula": "cumsum(revenue)"} (cumulative sum), {"formula": "change(revenue)"} (diff from previous row),
                {"formula": "change_pct(revenue)"} (% change), {"formula": "time_shift(revenue, -1)"} (previous period via self-join),
                {"formula": "time_shift(revenue, -1, 'year')"} (year-over-year), {"formula": "lag(revenue, 1)"} (previous row via window function),
                {"formula": "lead(revenue, 1)"} (next row via window function), {"formula": "last(revenue)"} (most recent),
                {"formula": "rank(revenue)"} (ranking).
            dimensions: List of dimension names to group by, e.g. ["status", "region"].
            filters: Filter conditions as formula strings. Examples: "status == 'completed'",
                "amount > 100", "status in ('a', 'b')", "status is None",
                "name like '%acme%'". Filters on measures are automatically routed to HAVING.
                Supports and/or: "status == 'a' or status == 'b'".
                Filters can also reference computed field names or contain inline transforms:
                "change(revenue) > 0", "last(change(revenue)) < 0".
            time_dimensions: Time grouping. Format: {"dimension": "created_at", "granularity": "day|week|month|quarter|year", "date_range": ["2024-01-01", "2024-12-31"]}.
            order: Sorting. Format: {"column": "field_name", "direction": "asc|desc"}.
            limit: Max rows to return.
            offset: Number of rows to skip.
            whole_periods_only: When true, snap date filters to time bucket boundaries based on granularity, exclude the current incomplete time bucket.
            show_sql: When true, include the generated SQL in the response for debugging.

        Example: query(model="orders", fields=[{"formula": "count"}], dimensions=["status"], filters=["status == 'completed'"])
        """
        data: Dict[str, Any] = {"model": model}
        if dimensions:
            data["dimensions"] = [_parse_column_ref(d) for d in dimensions]
        if filters:
            data["filters"] = filters
        if time_dimensions:
            data["time_dimensions"] = _normalize_time_dimensions(time_dimensions)
        if order:
            data["order"] = _normalize_order(order)
        if limit is not None:
            data["limit"] = limit
        if offset is not None:
            data["offset"] = offset
        if whole_periods_only:
            data["whole_periods_only"] = True
        if fields:
            data["fields"] = fields
        try:
            slayer_query = SlayerQuery.model_validate(data)
            result = engine.execute(query=slayer_query)
            output = _format_table(data=result.data, columns=result.columns)
            if show_sql and result.sql:
                output = f"SQL:\n{result.sql}\n\n{output}"
            return output
        except Exception as e:
            if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                return _friendly_db_error(e)
            raise

    # -----------------------------------------------------------------------
    # Model discovery
    # -----------------------------------------------------------------------

    @mcp.tool()
    def datasource_summary() -> str:
        """List all datasources and their models with schemas (dimensions, measures). Does not include sample data — use inspect_model for that."""
        # Datasources
        ds_names = storage.list_datasources()
        datasources = []
        for name in ds_names:
            ds = storage.get_datasource(name)
            if ds:
                entry = {"name": name, "type": ds.type}
                if ds.description:
                    entry["description"] = ds.description
                datasources.append(entry)

        # Models
        model_names = storage.list_models()
        models = []
        for name in model_names:
            model = storage.get_model(name)
            if model and not model.hidden:
                models.append(_model_to_summary(model))

        if not datasources and not models:
            return "No datasources or models configured. Use create_datasource to connect a database."

        result = {}
        if datasources:
            result["datasources"] = datasources
        result["models"] = models
        result["model_count"] = len(models)

        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    def inspect_model(
        model_name: str,
        num_rows: int = 3,
        show_sql: bool = False,
    ) -> str:
        """Get detailed information about a specific model including sample data.

        Args:
            model_name: Name of the model to inspect.
            num_rows: Number of sample rows to include (default: 3).
            show_sql: Whether to include the SQL query/table definition in the response.
        """
        model = storage.get_model(model_name)
        if model is None:
            available = sorted([
                n for n in storage.list_models()
                if not (storage.get_model(n) or SlayerModel(name="", data_source="")).hidden
            ])
            return f"Model '{model_name}' not found. Available models: {', '.join(available)}"

        result = _model_to_summary(model)

        if show_sql:
            result["sql"] = model.sql
            result["sql_table"] = model.sql_table

        # Include sample data
        try:
            sample_query = SlayerQuery(
                model=model_name,
                fields=[{"formula": m.name} for m in model.measures if not m.hidden][:3],
                dimensions=[
                    {"name": d.name}
                    for d in model.dimensions
                    if not d.hidden and not d.primary_key
                ][:2],
                limit=num_rows,
            )
            sample_result = engine.execute(query=sample_query)
            result["sample_data"] = _format_table(
                data=sample_result.data, columns=sample_result.columns,
            )
        except Exception as e:
            result["sample_data_error"] = str(e)

        return json.dumps(result, indent=2, default=str)

    # -----------------------------------------------------------------------
    # Model creation and editing
    # -----------------------------------------------------------------------

    @mcp.tool()
    def create_model(
        name: str,
        sql_table: Optional[str] = None,
        sql: Optional[str] = None,
        data_source: Optional[str] = None,
        description: Optional[str] = None,
        dimensions: Optional[List[Dict[str, str]]] = None,
        measures: Optional[List[Dict[str, str]]] = None,
    ) -> str:
        """Create a new semantic model that maps to a database table.

        Args:
            name: Unique model name (lowercase, underscores).
            sql_table: Database table name, e.g. "public.orders".
            sql: Alternative to sql_table — a custom SQL expression for the model's source.
            data_source: Name of the datasource (from list_datasources).
            description: What this model represents.
            dimensions: List of dimension definitions. Each: {"name": "col", "sql": "col", "type": "string"}.
                Types: string, number, time, date, boolean.
            measures: List of measure definitions. Each: {"name": "total", "sql": "amount", "type": "sum"}.
                Types: count, count_distinct, sum, avg, min, max.
        """
        data = _build_dict(
            name=name, sql_table=sql_table, sql=sql, data_source=data_source,
            description=description, dimensions=dimensions, measures=measures,
        )
        model = SlayerModel.model_validate(data)
        existed = storage.get_model(name) is not None
        storage.save_model(model)
        verb = "replaced" if existed else "created"
        return f"Model '{model.name}' {verb}."

    @mcp.tool()
    def edit_model(
        model_name: str,
        description: Optional[str] = None,
        data_source: Optional[str] = None,
        default_time_dimension: Optional[str] = None,
        add_measures: Optional[List[Dict[str, str]]] = None,
        add_dimensions: Optional[List[Dict[str, str]]] = None,
        remove: Optional[List[str]] = None,
    ) -> str:
        """Edit an existing model — update metadata, add/remove measures and dimensions in a single call.

        Args:
            model_name: Name of the model to edit.
            description: New description for the model.
            data_source: New data source name.
            default_time_dimension: Default time dimension for transforms.
            add_measures: Measures to add. Each: {"name": "total", "sql": "amount", "type": "sum", "description": "..."}.
                Types: count, count_distinct, sum, avg, min, max.
            add_dimensions: Dimensions to add. Each: {"name": "region", "sql": "region", "type": "string", "description": "..."}.
                Types: string, number, time, date, boolean.
            remove: Names of measures or dimensions to remove.
        """
        model = storage.get_model(model_name)
        if model is None:
            return f"Model '{model_name}' not found."

        changes = []

        # Update metadata
        if description is not None:
            model.description = description
            changes.append("updated description")
        if data_source is not None:
            model.data_source = data_source
            changes.append(f"set data_source to '{data_source}'")
        if default_time_dimension is not None:
            model.default_time_dimension = default_time_dimension
            changes.append(f"set default_time_dimension to '{default_time_dimension}'")

        # Remove measures/dimensions
        if remove:
            for name in remove:
                match = [m for m in model.measures if m.name == name]
                if match:
                    model.measures.remove(match[0])
                    changes.append(f"removed measure '{name}'")
                    continue
                match = [d for d in model.dimensions if d.name == name]
                if match:
                    model.dimensions.remove(match[0])
                    changes.append(f"removed dimension '{name}'")
                    continue
                return f"'{name}' not found as a measure or dimension on model '{model_name}'."

        # Add measures
        existing_measure_names = {m.name for m in model.measures}
        for spec in (add_measures or []):
            name = spec.get("name", "")
            if name in existing_measure_names:
                return f"Measure '{name}' already exists on model '{model_name}'."
            measure_type = spec.get("type", "")
            if measure_type not in VALID_MEASURE_TYPES:
                return f"Invalid measure type '{measure_type}'. Must be one of: {', '.join(sorted(VALID_MEASURE_TYPES))}"
            model.measures.append(Measure(
                name=name, sql=spec.get("sql"), type=measure_type, description=spec.get("description"),
            ))
            existing_measure_names.add(name)
            changes.append(f"added measure '{name}'")

        # Add dimensions
        existing_dim_names = {d.name for d in model.dimensions}
        for spec in (add_dimensions or []):
            name = spec.get("name", "")
            if name in existing_dim_names:
                return f"Dimension '{name}' already exists on model '{model_name}'."
            dim_type = spec.get("type", "")
            if dim_type not in VALID_DIMENSION_TYPES:
                return f"Invalid dimension type '{dim_type}'. Must be one of: {', '.join(sorted(VALID_DIMENSION_TYPES))}"
            model.dimensions.append(Dimension(
                name=name, sql=spec.get("sql"), type=dim_type, description=spec.get("description"),
            ))
            existing_dim_names.add(name)
            changes.append(f"added dimension '{name}'")

        if not changes:
            return f"No changes specified for model '{model_name}'."

        storage.save_model(model)
        return json.dumps({
            "success": True,
            "model_name": model_name,
            "changes": changes,
            "message": f"Applied {len(changes)} change(s) to '{model_name}'",
        }, indent=2)

    # -----------------------------------------------------------------------
    # Datasource management
    # -----------------------------------------------------------------------

    @mcp.tool()
    def create_datasource(
        name: str,
        type: str,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        connection_string: Optional[str] = None,
        schema_name: Optional[str] = None,
        auto_ingest: bool = True,
    ) -> str:
        """Create a database connection, verify it, and auto-ingest models. Use ${ENV_VAR} syntax in credentials to reference environment variables.

        Args:
            name: Unique datasource name.
            type: Database type — postgres, mysql, sqlite, bigquery, or snowflake.
            host: Database host (default: localhost).
            port: Database port (e.g. 5432 for Postgres).
            database: Database name.
            username: Database username.
            password: Database password.
            connection_string: Full connection string as alternative to individual fields.
            schema_name: Default schema name. Also used as the schema for auto-ingestion.
            auto_ingest: Automatically ingest models from the database schema (default: true). Set to false to skip.

        Example: create_datasource(name="mydb", type="postgres", host="localhost", port=5432, database="app", username="user", password="pass")
        """
        from slayer.engine.ingestion import ingest_datasource as _ingest

        data = _build_dict(
            name=name, type=type, host=host, port=port, database=database,
            username=username, password=password, connection_string=connection_string,
            schema_name=schema_name,
        )
        ds = DatasourceConfig.model_validate(data)
        existed = storage.get_datasource(name) is not None
        storage.save_datasource(ds)
        verb = "replaced" if existed else "created"

        ok, msg = _test_connection(ds)
        if not ok:
            return f"Datasource '{ds.name}' {verb}, but connection test failed.\n{msg}"

        lines = [f"Datasource '{ds.name}' {verb}. {msg}"]

        if not auto_ingest:
            return "\n".join(lines)

        # Auto-ingest models
        try:
            models = _ingest(datasource=ds, schema=schema_name or None)
        except Exception as e:
            if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                lines.append(f"Auto-ingestion failed: {_friendly_db_error(e)}")
                return "\n".join(lines)
            raise

        for model in models:
            storage.save_model(model)

        if not models:
            lines.append("No tables found to ingest.")
            schemas = _get_schemas(ds)
            if schemas:
                lines.append(f"Available schemas: {', '.join(schemas)}")
        else:
            lines.append(f"Ingested {len(models)} model(s):")
            for m in models:
                lines.append(f"- {m.name} ({len(m.dimensions)} dims, {len(m.measures)} measures)")
            lines.append("")
            lines.append("Use datasource_summary and inspect_model to explore, then query to fetch data.")

        return "\n".join(lines)

    @mcp.tool()
    def list_datasources() -> str:
        """List all configured database connections (names and types only, credentials are not shown). Use describe_datasource for connection details and status."""
        names = storage.list_datasources()
        if not names:
            return "No datasources configured. Use create_datasource to add a database connection."
        lines = []
        for name in names:
            ds = storage.get_datasource(name)
            ds_type = ds.type if ds else "unknown"
            lines.append(f"- {name} ({ds_type})")
        return "\n".join(lines)

    @mcp.tool()
    def describe_datasource(name: str) -> str:
        """Show datasource details including connection status and available schemas. Use this to verify a datasource works before ingesting.

        Args:
            name: Datasource name (from list_datasources).
        """
        ds = storage.get_datasource(name)
        if ds is None:
            return f"Datasource '{name}' not found."

        lines = [f"Datasource: {ds.name}"]
        if ds.type:
            lines.append(f"Type: {ds.type}")
        if ds.host:
            lines.append(f"Host: {ds.host}")
        if ds.port:
            lines.append(f"Port: {ds.port}")
        if ds.database:
            lines.append(f"Database: {ds.database}")
        if ds.username:
            lines.append(f"Username: {ds.username}")
        if ds.connection_string:
            lines.append("Connection string: (set)")

        ok, msg = _test_connection(ds)
        lines.append(f"\nConnection: {'OK' if ok else 'FAILED'}")
        if not ok:
            lines.append(msg)
            return "\n".join(lines)

        schemas = _get_schemas(ds)
        if schemas:
            lines.append(f"Available schemas: {', '.join(schemas)}")

        return "\n".join(lines)

    @mcp.tool()
    def list_tables(datasource_name: str, schema_name: str = "") -> str:
        """List tables in a database. Use this to explore what's available before ingesting.

        Args:
            datasource_name: Name of an existing datasource (from list_datasources).
            schema_name: Database schema (e.g. "public"). If empty, uses the default schema.
        """
        ds = storage.get_datasource(datasource_name)
        if ds is None:
            return f"Datasource '{datasource_name}' not found."
        try:
            conn_str = ds.resolve_env_vars().get_connection_string()
            sa_engine = sa.create_engine(conn_str)
            inspector = sa.inspect(sa_engine)
            schema = schema_name or None
            tables = inspector.get_table_names(schema=schema)
            sa_engine.dispose()
        except Exception as e:
            if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                return _friendly_db_error(e)
            raise

        if not tables:
            schema_label = f" in schema '{schema_name}'" if schema_name else ""
            lines = [f"No tables found{schema_label}."]
            schemas = _get_schemas(ds)
            if schemas:
                lines.append(f"Available schemas: {', '.join(schemas)}")
            return "\n".join(lines)

        lines = [f"Tables ({len(tables)}):"]
        for t in sorted(tables):
            lines.append(f"  - {t}")
        lines.append("\nUse ingest_datasource_models to create models from these tables.")
        return "\n".join(lines)

    @mcp.tool()
    def edit_datasource(
        name: str,
        description: Optional[str] = None,
    ) -> str:
        """Update a datasource's metadata.

        Args:
            name: Datasource name to update.
            description: New description for the datasource.
        """
        ds = storage.get_datasource(name)
        if ds is None:
            return f"Datasource '{name}' not found."

        if description is not None:
            ds.description = description

        storage.save_datasource(ds)
        return f"Datasource '{name}' updated."

    # -----------------------------------------------------------------------
    # Delete operations
    # -----------------------------------------------------------------------

    @mcp.tool()
    def delete_model(name: str) -> str:
        """Delete a semantic model.

        Args:
            name: Model name to delete.
        """
        if storage.delete_model(name):
            return f"Model '{name}' deleted."
        return f"Model '{name}' not found."

    @mcp.tool()
    def delete_datasource(name: str) -> str:
        """Delete a datasource configuration.

        Args:
            name: Datasource name to delete.
        """
        if storage.delete_datasource(name):
            return f"Datasource '{name}' deleted."
        return f"Datasource '{name}' not found."

    # -----------------------------------------------------------------------
    # Ingestion
    # -----------------------------------------------------------------------

    @mcp.tool()
    def ingest_datasource_models(datasource_name: str, include_tables: str = "", schema_name: str = "") -> str:
        """Auto-discover tables in a database and create semantic models from them. Inspects the schema and generates one model per table with dimensions and measures inferred from column types.

        Args:
            datasource_name: Name of an existing datasource (from list_datasources).
            include_tables: Comma-separated list of table names to include. If empty, all tables are ingested.
            schema_name: Database schema to inspect (e.g. "public"). If empty, uses the default schema.
        """
        from slayer.engine.ingestion import ingest_datasource as _ingest

        ds = storage.get_datasource(datasource_name)
        if ds is None:
            return f"Datasource '{datasource_name}' not found."

        try:
            include = [t.strip() for t in include_tables.split(",") if t.strip()] or None
            models = _ingest(datasource=ds, include_tables=include, schema=schema_name or None)
        except Exception as e:
            if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                return _friendly_db_error(e)
            raise

        for model in models:
            storage.save_model(model)

        if not models:
            schema_label = f" in schema '{schema_name}'" if schema_name else ""
            lines = [f"No tables found{schema_label}."]
            schemas = _get_schemas(ds)
            if schemas:
                lines.append(f"Available schemas: {', '.join(schemas)}")
                lines.append("Try: ingest_datasource_models with schema_name set to one of these.")
            return "\n".join(lines)

        lines = [f"Ingested {len(models)} model(s):"]
        for m in models:
            lines.append(f"- {m.name} ({len(m.dimensions)} dims, {len(m.measures)} measures)")
        lines.append("")
        lines.append("Use datasource_summary and inspect_model to explore, then query to fetch data.")
        return "\n".join(lines)

    return mcp


def _build_dict(**kwargs: Any) -> Dict[str, Any]:
    """Build a dict from keyword arguments, excluding None values."""
    return {k: v for k, v in kwargs.items() if v is not None}


def _format_table(data: List[Dict[str, Any]], columns: List[str], max_rows: int = 50) -> str:
    if not data:
        return "No results."

    truncated = len(data) > max_rows
    rows = data[:max_rows]

    header = " | ".join(columns)
    separator = " | ".join("-" * len(c) for c in columns)
    body_lines = []
    for row in rows:
        body_lines.append(" | ".join(str(row.get(c, "")) for c in columns))

    result = f"{header}\n{separator}\n" + "\n".join(body_lines)
    if truncated:
        result += f"\n... ({len(data)} total rows, showing first {max_rows})"
    return result
