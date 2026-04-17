"""MCP server for SLayer."""

import json
import logging
from typing import Any, Dict, List, Optional, Union

import sqlalchemy as sa

from slayer.core.models import (
    Aggregation,
    DatasourceConfig,
    Dimension,
    Measure,
    ModelJoin,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)

VALID_DIMENSION_TYPES = {"string", "time", "date", "boolean", "number"}
_UNSET = object()  # Sentinel to distinguish "not provided" from "explicitly set to None"


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
    dims = []
    for d in model.dimensions:
        if d.hidden:
            continue
        entry = {"name": d.name, "type": str(d.type)}
        if d.label:
            entry["label"] = d.label
        if d.description:
            entry["description"] = d.description
        dims.append(entry)

    measures = []
    for m in model.measures:
        if m.hidden:
            continue
        entry: dict = {"name": m.name}
        if m.label:
            entry["label"] = m.label
        if m.description:
            entry["description"] = m.description
        if m.filter:
            entry["filter"] = m.filter
        measures.append(entry)

    return {
        "name": model.name,
        "description": model.description,
        "dimensions": dims,
        "measures": measures,
    }


def create_mcp_server(storage: StorageBackend):
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError:
        raise ImportError("MCP package not found. Reinstall SLayer: pip install motley-slayer")

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
    async def query(
        source_model: str,
        fields: Optional[List[Dict[str, str]]] = None,
        dimensions: Optional[List[str]] = None,
        filters: Optional[List[str]] = None,
        time_dimensions: Optional[List[Dict[str, Any]]] = None,
        order: Optional[List[Dict[str, str]]] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        whole_periods_only: bool = False,
        show_sql: bool = False,
        dry_run: bool = False,
        explain: bool = False,
        format: str = "markdown",
    ) -> str:
        """Query data from a semantic model. Call inspect_model first to see available fields and dimensions.

        Args:
            source_model: Name of the model to query (from datasource_summary).
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
            dry_run: When true, generate and return the SQL without executing it.
            explain: When true, run EXPLAIN ANALYZE and return the query plan.
            format: Output format — "markdown" (default, compact and LLM-friendly), "json" (structured), or "csv" (most compact). Case-insensitive.

        Example: query(source_model="orders", fields=[{"formula": "count"}], dimensions=["status"], filters=["status == 'completed'"])
        """
        data: Dict[str, Any] = {"source_model": source_model}
        if dimensions:
            data["dimensions"] = list(dimensions)
        if filters:
            data["filters"] = filters
        if time_dimensions:
            data["time_dimensions"] = list(time_dimensions)
        if order:
            data["order"] = list(order)
        if limit is not None:
            data["limit"] = limit
        if offset is not None:
            data["offset"] = offset
        if whole_periods_only:
            data["whole_periods_only"] = True
        if dry_run:
            data["dry_run"] = True
        if explain:
            data["explain"] = True
        if fields:
            data["fields"] = fields
        try:
            fmt = format.lower().strip()
            if fmt not in ("json", "csv", "markdown"):
                raise ValueError(f"Invalid format '{format}'. Must be one of: json, csv, markdown")
            slayer_query = SlayerQuery.model_validate(data)
            result = await engine.execute(query=slayer_query)
            if dry_run:
                return f"SQL:\n{result.sql}"
            if explain:
                output = f"SQL:\n{result.sql}\n\nQuery Plan:\n"
                output += _format_output(result=result, fmt=fmt)
                return output
            output = _format_output(result=result, fmt=fmt)
            if show_sql and result.sql:
                output = f"SQL:\n{result.sql}\n\n{output}"
            if result.attributes and (result.attributes.dimensions or result.attributes.measures):
                output += "\n\n" + _format_attributes(attributes=result.attributes)
            return output
        except Exception as e:
            if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                return _friendly_db_error(e)
            raise

    # -----------------------------------------------------------------------
    # Model discovery
    # -----------------------------------------------------------------------

    @mcp.tool()
    async def datasource_summary() -> str:
        """List all datasources and their models with schemas (dimensions, measures). Does not include sample data — use inspect_model for that."""
        # Datasources
        ds_names = await storage.list_datasources()
        datasources = []
        for name in ds_names:
            try:
                ds = await storage.get_datasource(name)
                if ds:
                    entry: Dict[str, Any] = {"name": name, "type": ds.type}
                    if ds.description:
                        entry["description"] = ds.description
                    datasources.append(entry)
            except Exception as exc:
                logger.warning("Failed to load datasource '%s': %s", name, exc)
                datasources.append({"name": name, "error": "invalid datasource config"})

        # Models
        model_names = await storage.list_models()
        models = []
        for name in model_names:
            try:
                model = await storage.get_model(name)
                if model and not model.hidden:
                    models.append(_model_to_summary(model))
            except Exception:
                logger.warning("Failed to load model '%s', skipping", name, exc_info=True)

        if not datasources and not models:
            return json.dumps({"datasources": [], "models": [], "model_count": 0})

        result = {
            "datasources": datasources,
            "models": models,
            "model_count": len(models),
        }

        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    async def inspect_model(
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
        model = await storage.get_model(model_name)
        if model is None:
            all_names = await storage.list_models()
            available = []
            for n in all_names:
                m = await storage.get_model(n)
                if m is not None and not m.hidden:
                    available.append(n)
            available.sort()
            return f"Model '{model_name}' not found. Available models: {', '.join(available)}"

        result = _model_to_summary(model)

        if show_sql:
            result["sql"] = model.sql
            result["sql_table"] = model.sql_table

        # Include sample data
        try:
            sample_query = SlayerQuery(
                source_model=model_name,
                fields=[{"formula": m.name} for m in model.measures if not m.hidden][:3],
                dimensions=[{"name": d.name} for d in model.dimensions if not d.hidden and not d.primary_key][:2],
                limit=num_rows,
            )
            sample_result = await engine.execute(query=sample_query)
            result["sample_data"] = _format_table(
                data=sample_result.data,
                columns=sample_result.columns,
            )
        except Exception as e:
            result["sample_data_error"] = str(e)

        return json.dumps(result, indent=2, default=str)

    # -----------------------------------------------------------------------
    # Model creation and editing
    # -----------------------------------------------------------------------

    @mcp.tool()
    async def create_model(
        name: str,
        sql_table: Optional[str] = None,
        sql: Optional[str] = None,
        data_source: Optional[str] = None,
        description: Optional[str] = None,
        dimensions: Optional[List[Dict[str, str]]] = None,
        measures: Optional[List[Dict[str, Union[str, List[str]]]]] = None,
        query: Optional[Dict] = None,
    ) -> str:
        """Create a new semantic model, either from a database table or from a query.

        **From a table** (provide sql_table or sql):
            create_model(name="orders", sql_table="public.orders", data_source="mydb",
                         dimensions=[...], measures=[...])

        **From a query** (provide query):
            create_model(name="monthly_summary", query={"source_model": "orders",
                         "fields": ["*:count", "amount:sum"],
                         "time_dimensions": [{"dimension": "created_at", "granularity": "month"}]})
            Dimensions and measures are auto-introspected from the query result.

        Args:
            name: Unique model name (lowercase, underscores).
            sql_table: Database table name, e.g. "public.orders".
            sql: Alternative to sql_table — a custom SQL expression for the model's source.
            data_source: Name of the datasource (from list_datasources).
            description: What this model represents.
            dimensions: List of dimension definitions. Each: {"name": "col", "sql": "col", "type": "string"}.
                Types: string, number, time, date, boolean.
            measures: List of measure definitions. Each: {"name": "total", "sql": "amount"}.
                Optional: "allowed_aggregations": ["sum", "avg"] to restrict usable aggregations.
            query: A SLayer query dict. When provided, the query's SQL becomes the model source
                and dimensions/measures are auto-introspected. Mutually exclusive with
                sql_table, sql, dimensions, and measures.
        """
        if query is not None:
            table_params = {
                k: v for k, v in {
                    "sql_table": sql_table, "sql": sql, "data_source": data_source,
                    "dimensions": dimensions, "measures": measures,
                }.items()
                if v
            }
            if table_params:
                return (
                    f"Error: 'query' cannot be combined with {', '.join(table_params.keys())}. "
                    "Use 'query' alone to create from a query, or provide table details without 'query'."
                )
            try:
                parsed_query = SlayerQuery.model_validate(query)
                model = await engine.create_model_from_query(
                    query=parsed_query, name=name, description=description,
                )
            except Exception as e:
                if isinstance(e, (sa.exc.OperationalError, sa.exc.DatabaseError)):
                    return _friendly_db_error(e)
                return f"Error creating model from query: {e}"
            dims = [d.name for d in model.dimensions]
            meas = [m.name for m in model.measures]
            return (
                f"Model '{name}' created from query. "
                f"Dimensions: {dims}. Measures: {meas}."
            )

        data = _build_dict(
            name=name,
            sql_table=sql_table,
            sql=sql,
            data_source=data_source,
            description=description,
            dimensions=dimensions,
            measures=measures,
        )
        model = SlayerModel.model_validate(data)
        existed = await storage.get_model(name) is not None
        await storage.save_model(model)
        verb = "replaced" if existed else "created"
        return f"Model '{model.name}' {verb}."

    def _upsert_entity(
        entity_list: list,
        spec: dict,
        entity_cls: type,
        id_field: str,
        changes: list,
        label: str,
    ) -> Optional[str]:
        """Upsert a named entity in *entity_list*.

        Returns an error string on validation failure, ``None`` on success.
        """
        entity_id = spec.get(id_field, "")
        if not entity_id:
            return f"Missing '{id_field}' in {label} specification."

        existing = next((e for e in entity_list if getattr(e, id_field) == entity_id), None)
        if existing is not None:
            merged = existing.model_dump()
            for k, v in spec.items():
                merged[k] = v
            try:
                updated = entity_cls.model_validate(merged)
            except Exception as exc:
                return f"Invalid {label} '{entity_id}': {exc}"
            idx = entity_list.index(existing)
            entity_list[idx] = updated
            changes.append(f"updated {label} '{entity_id}'")
        else:
            try:
                new_entity = entity_cls.model_validate(spec)
            except Exception as exc:
                return f"Invalid {label} '{entity_id}': {exc}"
            entity_list.append(new_entity)
            changes.append(f"created {label} '{entity_id}'")
        return None

    VALID_REMOVE_KEYS = {"dimensions", "measures", "aggregations", "joins"}

    @mcp.tool()
    async def edit_model(
        model_name: str,
        description: Optional[str] = None,
        data_source: Optional[str] = None,
        default_time_dimension: Optional[str] = None,
        sql_table: Optional[str] = None,
        sql: Optional[str] = None,
        hidden: Optional[bool] = None,
        dimensions: Optional[List[Dict[str, Any]]] = None,
        measures: Optional[List[Dict[str, Any]]] = None,
        aggregations: Optional[List[Dict[str, Any]]] = None,
        joins: Optional[List[Dict[str, Any]]] = None,
        add_filters: Optional[List[str]] = None,
        remove_filters: Optional[List[str]] = None,
        remove: Optional[Dict[str, List[str]]] = None,
        meta: Optional[Dict[str, Any]] = _UNSET,
    ) -> str:
        """Edit an existing model in a single call — update metadata, upsert dimensions/measures/aggregations/joins,
        manage filters, and remove entities.

        Args:
            model_name: Name of the model to edit.
            description: New model description.
            data_source: New data source name.
            default_time_dimension: Default time dimension for time-dependent transforms.
            sql_table: Database table name.
            sql: Custom SQL expression for the model source.
            hidden: Whether this model is hidden from discovery.
            meta: Arbitrary JSON metadata for the model (replaces existing meta). Pass null/None to clear.
            dimensions: Dimensions to create or update (upsert by name). Each dict: {"name": "col", "type": "string", "sql": "col", "description": "...", "primary_key": false, "hidden": false}.
                If a dimension with this name exists, only the provided fields are updated; omitted fields keep current values.
                Types: string, number, time, date, boolean.
            measures: Measures to create or update (upsert by name). Each dict: {"name": "total", "sql": "amount", "description": "...", "hidden": false, "allowed_aggregations": ["sum", "avg"]}.
                If a measure with this name exists, only the provided fields are updated.
            aggregations: Aggregations to create or update (upsert by name). Each dict: {"name": "weighted_avg", "formula": "SUM({value} * {weight}) / NULLIF(SUM({weight}), 0)", "params": [{"name": "weight", "sql": "quantity"}], "description": "..."}.
                If an aggregation with this name exists, only the provided fields are updated.
            joins: Joins to create or update (upsert by target_model). Each dict: {"target_model": "customers", "join_pairs": [["customer_id", "id"]]}.
                If a join to this target_model exists, its join_pairs are updated.
            add_filters: SQL filter strings to add (e.g. ["deleted_at IS NULL"]). Duplicates are ignored.
            remove_filters: SQL filter strings to remove (exact match).
            remove: Named entities to delete, keyed by type: {"dimensions": ["name1"], "measures": ["name2"], "aggregations": ["name3"], "joins": ["target_model_name"]}.
                Removals are processed before upserts, so you can remove and re-add in one call.

        Example — update a dimension and add a measure:
            edit_model(model_name="orders", dimensions=[{"name": "status", "type": "string"}], measures=[{"name": "profit", "sql": "revenue - cost"}])
        Example — remove a measure:
            edit_model(model_name="orders", remove={"measures": ["old_metric"]})
        """
        model = await storage.get_model(model_name)
        if model is None:
            return f"Model '{model_name}' not found."

        changes: List[str] = []

        # --- Phase 1: Scalar metadata ---
        if description is not None:
            model.description = description
            changes.append("updated description")
        if data_source is not None:
            model.data_source = data_source
            changes.append(f"set data_source to '{data_source}'")
        if default_time_dimension is not None:
            model.default_time_dimension = default_time_dimension
            changes.append(f"set default_time_dimension to '{default_time_dimension}'")
        if sql_table is not None and sql is not None:
            return "Specify only one of 'sql_table' or 'sql' when editing a model."

        if sql_table is not None:
            model.sql_table = sql_table
            model.sql = None
            changes.append(f"set sql_table to '{sql_table}'")
        if sql is not None:
            model.sql = sql
            model.sql_table = None
            changes.append(f"set sql to '{sql}'")
        if hidden is not None:
            model.hidden = hidden
            changes.append(f"set hidden to {hidden}")
        if meta is not _UNSET:
            model.meta = meta
            changes.append("updated meta" if meta is not None else "cleared meta")

        # --- Phase 2: Removals ---
        if remove:
            for key in remove:
                if key not in VALID_REMOVE_KEYS:
                    return (
                        f"Invalid remove key '{key}'. "
                        f"Must be one of: {', '.join(sorted(VALID_REMOVE_KEYS))}."
                    )

            for name in remove.get("dimensions", []):
                match = next((d for d in model.dimensions if d.name == name), None)
                if match is None:
                    return f"Dimension '{name}' not found on model '{model_name}'."
                model.dimensions.remove(match)
                changes.append(f"removed dimension '{name}'")

            for name in remove.get("measures", []):
                match = next((m for m in model.measures if m.name == name), None)
                if match is None:
                    return f"Measure '{name}' not found on model '{model_name}'."
                model.measures.remove(match)
                changes.append(f"removed measure '{name}'")

            for name in remove.get("aggregations", []):
                match = next((a for a in model.aggregations if a.name == name), None)
                if match is None:
                    return f"Aggregation '{name}' not found on model '{model_name}'."
                model.aggregations.remove(match)
                changes.append(f"removed aggregation '{name}'")

            for target in remove.get("joins", []):
                match = next((j for j in model.joins if j.target_model == target), None)
                if match is None:
                    return f"Join to '{target}' not found on model '{model_name}'."
                model.joins.remove(match)
                changes.append(f"removed join to '{target}'")

        # --- Phase 3: Entity upserts ---
        for spec in dimensions or []:
            err = _upsert_entity(
                entity_list=model.dimensions, spec=spec, entity_cls=Dimension,
                id_field="name", changes=changes, label="dimension",
            )
            if err:
                return err

        for spec in measures or []:
            err = _upsert_entity(
                entity_list=model.measures, spec=spec, entity_cls=Measure,
                id_field="name", changes=changes, label="measure",
            )
            if err:
                return err

        for spec in aggregations or []:
            err = _upsert_entity(
                entity_list=model.aggregations, spec=spec, entity_cls=Aggregation,
                id_field="name", changes=changes, label="aggregation",
            )
            if err:
                return err

        for spec in joins or []:
            err = _upsert_entity(
                entity_list=model.joins, spec=spec, entity_cls=ModelJoin,
                id_field="target_model", changes=changes, label="join",
            )
            if err:
                return err

        # --- Phase 4: Filters ---
        if add_filters:
            existing_filters = set(model.filters)
            for f in add_filters:
                if f not in existing_filters:
                    model.filters.append(f)
                    existing_filters.add(f)
                    changes.append(f"added filter '{f}'")

        if remove_filters:
            for f in remove_filters:
                if f not in model.filters:
                    return f"Filter not found on model '{model_name}': {f}"
                model.filters.remove(f)
                changes.append(f"removed filter '{f}'")

        if not changes:
            return f"No changes specified for model '{model_name}'."

        # --- Phase 5: Validate and save ---
        try:
            validated = SlayerModel.model_validate(model.model_dump(mode="json"))
        except Exception as exc:
            return f"Validation error: {exc}"

        await storage.save_model(validated)
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
    async def create_datasource(
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
            name=name,
            type=type,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            schema_name=schema_name,
        )
        ds = DatasourceConfig.model_validate(data)
        existed = await storage.get_datasource(name) is not None
        await storage.save_datasource(ds)
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
            await storage.save_model(model)

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
    async def list_datasources() -> str:
        """List all configured database connections (names and types only, credentials are not shown). Use describe_datasource for connection details and status."""
        names = await storage.list_datasources()
        if not names:
            return "No datasources configured. Use create_datasource to add a database connection."
        lines = []
        for name in names:
            try:
                ds = await storage.get_datasource(name)
                ds_type = ds.type if ds else "unknown"
                lines.append(f"- {name} ({ds_type})")
            except Exception as exc:
                logger.warning("Failed to load datasource '%s': %s", name, exc)
                lines.append(f"- {name} (ERROR: invalid datasource config)")
        return "\n".join(lines)

    @mcp.tool()
    async def describe_datasource(name: str) -> str:
        """Show datasource details including connection status and available schemas. Use this to verify a datasource works before ingesting.

        Args:
            name: Datasource name (from list_datasources).
        """
        try:
            ds = await storage.get_datasource(name)
        except Exception as exc:
            logger.warning("Failed to load datasource '%s': %s", name, exc)
            return f"Datasource '{name}' has an invalid config."
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
    async def list_tables(datasource_name: str, schema_name: str = "") -> str:
        """List tables in a database. Use this to explore what's available before ingesting.

        Args:
            datasource_name: Name of an existing datasource (from list_datasources).
            schema_name: Database schema (e.g. "public"). If empty, uses the default schema.
        """
        try:
            ds = await storage.get_datasource(datasource_name)
        except Exception as exc:
            logger.warning("Failed to load datasource '%s': %s", datasource_name, exc)
            return f"Datasource '{datasource_name}' has an invalid config."
        if ds is None:
            return f"Datasource '{datasource_name}' not found."
        try:
            conn_str = ds.get_connection_string()
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
    async def edit_datasource(
        name: str,
        description: Optional[str] = None,
    ) -> str:
        """Update a datasource's metadata.

        Args:
            name: Datasource name to update.
            description: New description for the datasource.
        """
        ds = await storage.get_datasource(name)
        if ds is None:
            return f"Datasource '{name}' not found."

        if description is not None:
            ds.description = description

        await storage.save_datasource(ds)
        return f"Datasource '{name}' updated."

    # -----------------------------------------------------------------------
    # Delete operations
    # -----------------------------------------------------------------------

    @mcp.tool()
    async def delete_model(name: str) -> str:
        """Delete a semantic model.

        Args:
            name: Model name to delete.
        """
        if await storage.delete_model(name):
            return f"Model '{name}' deleted."
        return f"Model '{name}' not found."

    @mcp.tool()
    async def delete_datasource(name: str) -> str:
        """Delete a datasource configuration.

        Args:
            name: Datasource name to delete.
        """
        if await storage.delete_datasource(name):
            return f"Datasource '{name}' deleted."
        return f"Datasource '{name}' not found."

    # -----------------------------------------------------------------------
    # Ingestion
    # -----------------------------------------------------------------------

    @mcp.tool()
    async def ingest_datasource_models(datasource_name: str, include_tables: str = "", schema_name: str = "") -> str:
        """Auto-discover tables in a database and create semantic models from them. Inspects the schema and generates one model per table with dimensions and measures inferred from column types.

        Args:
            datasource_name: Name of an existing datasource (from list_datasources).
            include_tables: Comma-separated list of table names to include. If empty, all tables are ingested.
            schema_name: Database schema to inspect (e.g. "public"). If empty, uses the default schema.
        """
        from slayer.engine.ingestion import ingest_datasource as _ingest

        ds = await storage.get_datasource(datasource_name)
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
            await storage.save_model(model)

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
    """Format data as a pipe-separated table (used for sample data display)."""
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


def _format_json(data: List[Dict[str, Any]], columns: List[str]) -> str:
    """Format data as JSON array."""
    import json

    return json.dumps(data, default=str)


def _format_csv(data: List[Dict[str, Any]], columns: List[str]) -> str:
    """Format data as CSV."""
    if not data:
        return ""
    lines = [",".join(columns)]
    for row in data:
        values = []
        for c in columns:
            v = str(row.get(c, ""))
            if "," in v or '"' in v or "\n" in v:
                v = '"' + v.replace('"', '""') + '"'
            values.append(v)
        lines.append(",".join(values))
    return "\n".join(lines)


def _format_output(result: SlayerResponse, fmt: str) -> str:
    """Format query output in the requested format."""
    if fmt == "csv":
        return _format_csv(data=result.data, columns=result.columns)
    if fmt == "markdown":
        return result.to_markdown()
    return _format_json(data=result.data, columns=result.columns)


def _format_field_meta(entries: Dict[str, Any]) -> List[str]:
    """Format a dict of field metadata entries into lines."""
    lines = []
    for col, fm in entries.items():
        parts = []
        if fm.label:
            parts.append(f"label={fm.label}")
        if fm.format:
            fmt_parts = [f"type={fm.format.type.value}"]
            if fm.format.precision is not None:
                fmt_parts.append(f"precision={fm.format.precision}")
            if fm.format.symbol is not None:
                fmt_parts.append(f"symbol={fm.format.symbol}")
            parts.append(f"format=({', '.join(fmt_parts)})")
        if parts:
            lines.append(f"  {col}: {', '.join(parts)}")
    return lines


def _format_attributes(attributes) -> str:
    """Format response attributes as a compact section."""
    lines = []
    dim_lines = _format_field_meta(attributes.dimensions)
    if dim_lines:
        lines.append("Dimension attributes:")
        lines.extend(dim_lines)
    measure_lines = _format_field_meta(attributes.measures)
    if measure_lines:
        lines.append("Measure attributes:")
        lines.extend(measure_lines)
    return "\n".join(lines) if lines else ""
