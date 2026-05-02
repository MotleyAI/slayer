"""FastAPI server for SLayer."""

import logging
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict

from slayer.mcp.server import create_mcp_server
from slayer.core.format import NumberFormat
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class QueryRequest(BaseModel):
    # Allow extra keys (e.g. forward-compat fields a newer client might send)
    # to pass through to SlayerQuery's pre-validate hook.
    model_config = ConfigDict(extra="allow")

    name: Optional[str] = None  # Run-by-name: backing query for a query-backed model
    source_model: Optional[str] = None
    measures: Optional[List[Dict[str, Any]]] = None
    dimensions: Optional[List[Dict[str, Any]]] = None
    time_dimensions: Optional[List[Dict[str, Any]]] = None
    filters: Optional[List[str]] = None
    order: Optional[List[Dict[str, Any]]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    whole_periods_only: Optional[bool] = None
    dry_run: Optional[bool] = None
    explain: Optional[bool] = None
    variables: Optional[Dict[str, Any]] = None


class FieldMetadataResponse(BaseModel):
    label: Optional[str] = None
    format: Optional[NumberFormat] = None


class AttributesResponse(BaseModel):
    dimensions: Dict[str, FieldMetadataResponse] = {}
    measures: Dict[str, FieldMetadataResponse] = {}


class QueryResponse(BaseModel):
    data: List[Dict[str, Any]]
    row_count: int
    columns: List[str]
    sql: Optional[str] = None
    attributes: Optional[AttributesResponse] = None


class IngestRequest(BaseModel):
    datasource: str
    include_tables: Optional[List[str]] = None
    exclude_tables: Optional[List[str]] = None
    schema_name: Optional[str] = None


def create_app(storage: StorageBackend) -> FastAPI:
    app = FastAPI(title="SLayer", version="0.1.0")
    engine = SlayerQueryEngine(storage=storage)

    # Mount MCP server over SSE at /mcp
    mcp = create_mcp_server(storage=storage)
    mcp_app = mcp.sse_app()
    app.mount("/mcp", mcp_app)

    @app.get("/health")
    async def health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/query",
        responses={400: {"description": "Invalid query payload (e.g. missing source_model/name, mutually exclusive fields, validation error)."}},
    )
    async def query(request: QueryRequest) -> QueryResponse:
        try:
            # Run-by-name: ``{"name": "<model>", "variables": {...}}``
            # routes through ``engine.execute(str)`` so the model's stored
            # backing query runs directly. Cannot be combined with
            # ``source_model`` or other query fields.
            if request.name is not None:
                disallowed = [
                    f for f in (
                        request.source_model, request.measures, request.dimensions,
                        request.time_dimensions, request.filters, request.order,
                        request.limit, request.offset,
                    ) if f is not None
                ]
                if disallowed:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "When 'name' is supplied for run-by-name, no other "
                            "query fields may be set (only 'variables', 'dry_run', "
                            "'explain' are allowed)."
                        ),
                    )
                result = await engine.execute(
                    request.name,
                    variables=request.variables or {},
                    dry_run=bool(request.dry_run),
                    explain=bool(request.explain),
                )
                dry_or_explain = bool(request.dry_run or request.explain)
            else:
                if request.source_model is None:
                    raise HTTPException(
                        status_code=400,
                        detail="Either 'name' (run-by-name) or 'source_model' must be provided.",
                    )
                payload = request.model_dump(exclude_none=True)
                # ``variables`` is consumed at execute() level, not part of
                # SlayerQuery's filter-substitution variables (those merge
                # automatically via the kwarg path).
                runtime_kwarg = payload.pop("variables", None)
                slayer_query = SlayerQuery.model_validate(payload)
                result = await engine.execute(
                    query=slayer_query, variables=runtime_kwarg
                )
                dry_or_explain = bool(slayer_query.dry_run or slayer_query.explain)
            attrs = result.attributes

            def _convert_meta(d: dict) -> Dict[str, FieldMetadataResponse]:
                return {k: FieldMetadataResponse(label=v.label, format=v.format) for k, v in d.items()}

            attributes = None
            if attrs and (attrs.dimensions or attrs.measures):
                attributes = AttributesResponse(
                    dimensions=_convert_meta(attrs.dimensions),
                    measures=_convert_meta(attrs.measures),
                )
            response = QueryResponse(
                data=result.data,
                row_count=result.row_count,
                columns=result.columns,
                attributes=attributes,
            )
            if dry_or_explain:
                response.sql = result.sql
            return response
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/models")
    async def list_models() -> List[Dict[str, Any]]:
        result = []
        for name in await storage.list_models():
            model = await storage.get_model(name)
            if model and model.hidden:
                continue
            entry: Dict[str, Any] = {"name": name}
            if model and model.description:
                entry["description"] = model.description
            result.append(entry)
        return result

    @app.get("/models/{name}")
    async def get_model(name: str) -> Dict[str, Any]:
        model = await storage.get_model(name)
        if model is None:
            raise HTTPException(status_code=404, detail=f"Model '{name}' not found")
        data = model.model_dump(exclude_none=True)
        if "columns" in data:
            data["columns"] = [c for c in data["columns"] if not c.get("hidden")]
        return data

    @app.post(
        "/models",
        responses={400: {"description": "Model failed validation (e.g. user-supplied cache fields on a query-backed model, save-time SQL generation failure)."}},
    )
    async def create_model(model: SlayerModel) -> Dict[str, str]:
        # Route through engine.save_model so query-backed models get cache
        # populated (and user-supplied cache fields are rejected).
        try:
            await engine.save_model(model)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {"status": "created", "name": model.name}

    @app.put(
        "/models/{name}",
        responses={400: {"description": "Body name does not match path name, or model failed validation."}},
    )
    async def update_model(name: str, model: SlayerModel) -> Dict[str, str]:
        if model.name != name:
            raise HTTPException(
                status_code=400,
                detail=f"Path name '{name}' does not match body name '{model.name}'",
            )
        try:
            await engine.save_model(model)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {"status": "updated", "name": name}

    @app.delete("/models/{name}")
    async def delete_model(name: str) -> Dict[str, Any]:
        deleted = await storage.delete_model(name)
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Model '{name}' not found")
        return {"status": "deleted", "name": name}

    @app.get("/datasources")
    async def list_datasources() -> List[Dict[str, Any]]:
        result = []
        for name in await storage.list_datasources():
            ds = await storage.get_datasource(name)
            entry: Dict[str, Any] = {"name": name}
            if ds:
                entry["type"] = ds.type
            result.append(entry)
        return result

    @app.get("/datasources/{name}")
    async def get_datasource(name: str) -> Dict[str, Any]:
        ds = await storage.get_datasource(name)
        if ds is None:
            raise HTTPException(
                status_code=404, detail=f"Datasource '{name}' not found"
            )
        # Mask credentials
        data = ds.model_dump(exclude_none=True)
        for secret_field in ("password", "connection_string"):
            if secret_field in data:
                data[secret_field] = "***"
        return data

    @app.post("/datasources")
    async def create_datasource(datasource: DatasourceConfig) -> Dict[str, str]:
        await storage.save_datasource(datasource)
        return {"status": "created", "name": datasource.name}

    @app.delete("/datasources/{name}")
    async def delete_datasource(name: str) -> Dict[str, Any]:
        deleted = await storage.delete_datasource(name)
        if not deleted:
            raise HTTPException(
                status_code=404, detail=f"Datasource '{name}' not found"
            )
        return {"status": "deleted", "name": name}

    @app.post("/ingest")
    async def ingest(request: IngestRequest) -> Dict[str, Any]:
        ds = await storage.get_datasource(request.datasource)
        if ds is None:
            raise HTTPException(
                status_code=404, detail=f"Datasource '{request.datasource}' not found"
            )
        models = ingest_datasource(
            datasource=ds,
            include_tables=request.include_tables,
            exclude_tables=request.exclude_tables,
            schema=request.schema_name,
        )
        for model in models:
            await storage.save_model(model)
        return {"status": "ingested", "models": [m.name for m in models]}

    return app
