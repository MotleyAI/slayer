"""FastAPI server for SLayer."""

import logging
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict

from slayer.mcp.server import create_mcp_server
from slayer.core.format import NumberFormat
from slayer.core.models import DatasourceConfig, NamedQuery, SlayerModel
from slayer.core.named_query_ops import save_named_query
from slayer.core.query import SlayerQuery
from slayer.engine.ingestion import ingest_datasource
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class QueryRequest(BaseModel):
    # Allow legacy `fields` to flow through to SlayerQuery's v1→v2 migration.
    model_config = ConfigDict(extra="allow")

    source_model: str
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


class RunNamedQueryRequest(BaseModel):
    variables: Optional[Dict[str, Any]] = None


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

    @app.post("/query")
    async def query(request: QueryRequest) -> QueryResponse:
        try:
            slayer_query = SlayerQuery.model_validate(
                request.model_dump(exclude_none=True)
            )
            result = await engine.execute(query=slayer_query)
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
            if slayer_query.dry_run or slayer_query.explain:
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
        if "measures" in data:
            data["measures"] = [m for m in data["measures"] if not m.get("hidden")]
        return data

    @app.post("/models")
    async def create_model(model: SlayerModel) -> Dict[str, str]:
        await storage.save_model(model)
        return {"status": "created", "name": model.name}

    @app.put("/models/{name}")
    async def update_model(name: str, model: SlayerModel) -> Dict[str, str]:
        if model.name != name:
            raise HTTPException(
                status_code=400,
                detail=f"Path name '{name}' does not match body name '{model.name}'",
            )
        await storage.save_model(model)
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

    @app.get("/queries")
    async def list_queries() -> List[Dict[str, Any]]:
        result = []
        for name in await storage.list_queries():
            q = await storage.get_query(name)
            entry: Dict[str, Any] = {"name": name}
            if q and q.description:
                entry["description"] = q.description
            result.append(entry)
        return result

    @app.get("/queries/{name}")
    async def get_query(name: str) -> Dict[str, Any]:
        q = await storage.get_query(name)
        if q is None:
            raise HTTPException(status_code=404, detail=f"NamedQuery '{name}' not found")
        return q.model_dump(mode="json", exclude_none=True)

    @app.post("/queries")
    async def create_query(query: NamedQuery) -> Dict[str, str]:
        try:
            await save_named_query(query, storage=storage, engine=engine)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {"status": "created", "name": query.name}

    @app.put("/queries/{name}")
    async def update_query(name: str, query: NamedQuery) -> Dict[str, str]:
        if query.name != name:
            raise HTTPException(
                status_code=400,
                detail=f"Path name '{name}' does not match body name '{query.name}'",
            )
        try:
            await save_named_query(query, storage=storage, engine=engine)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {"status": "updated", "name": name}

    @app.delete("/queries/{name}")
    async def delete_query(name: str) -> Dict[str, Any]:
        deleted = await storage.delete_query(name)
        if not deleted:
            raise HTTPException(
                status_code=404, detail=f"NamedQuery '{name}' not found"
            )
        return {"status": "deleted", "name": name}

    @app.post("/queries/{name}/run")
    async def run_query(name: str, request: Optional[RunNamedQueryRequest] = None) -> QueryResponse:
        try:
            variables = request.variables if request else None
            result = await engine.execute(query=name, variables=variables)
        except ValueError as e:
            raise HTTPException(status_code=404 if "not found" in str(e) else 400, detail=str(e))

        attrs = result.attributes

        def _convert_meta(d: dict) -> Dict[str, FieldMetadataResponse]:
            return {k: FieldMetadataResponse(label=v.label, format=v.format) for k, v in d.items()}

        attributes = None
        if attrs and (attrs.dimensions or attrs.measures):
            attributes = AttributesResponse(
                dimensions=_convert_meta(attrs.dimensions),
                measures=_convert_meta(attrs.measures),
            )
        return QueryResponse(
            data=result.data,
            row_count=result.row_count,
            columns=result.columns,
            attributes=attributes,
        )

    @app.get("/queries/{name}/inspect")
    async def inspect_query(name: str) -> Dict[str, Any]:
        q = await storage.get_query(name)
        if q is None:
            raise HTTPException(status_code=404, detail=f"NamedQuery '{name}' not found")
        # Fill placeholders for unsupplied variables and run a dry-run probe
        # so we can attach the final-stage result schema.
        runtime = {v: 0 for v in q.unsupplied_variables()}
        stages = list(q.stages)
        stages[-1] = stages[-1].model_copy(update={"dry_run": True})
        probe = q.model_copy(update={"stages": stages})
        try:
            response = await engine.execute(query=probe, variables=runtime)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Inspection failed: {e}")
        attrs = response.attributes
        columns: List[Dict[str, Any]] = []
        for col in response.columns:
            kind = "dimension" if col in attrs.dimensions else (
                "measure" if col in attrs.measures else "unknown"
            )
            columns.append({"name": col, "kind": kind})
        return {
            "name": q.name,
            "description": q.description,
            "stages": [s.model_dump(mode="json", exclude_none=True) for s in q.stages],
            "variables": q.variables,
            "missing_variables": sorted(q.unsupplied_variables()),
            "columns": columns,
            "sql": response.sql,
        }

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
