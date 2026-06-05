"""FastAPI server for SLayer."""

import logging
from importlib.metadata import PackageNotFoundError, version as _pkg_version
from typing import Any, Dict, List, Optional, Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict

from slayer.mcp.server import create_mcp_server
from slayer.core.errors import (
    AmbiguousModelError,
    EntityResolutionError,
    MemoryNotFoundError,
    SchemaDriftError,
    SlayerError,
)
from slayer.core.format import NumberFormat
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.memories.service import MemoryService
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class QueryRequest(BaseModel):
    # Allow extra keys (e.g. forward-compat fields a newer client might send)
    # to pass through to SlayerQuery's pre-validate hook.
    model_config = ConfigDict(extra="allow")

    name: Optional[str] = None  # Run-by-name: backing query for a query-backed model
    # ``source_model`` accepts a string (stored model name) or a dict
    # — the dict form is an inline ``ModelExtension`` (``{"source_name":
    # "<model>", "columns": [...], "joins": [...]}``) or an inline
    # ``SlayerModel`` (``{"name": "...", "sql_table": "...", "data_source":
    # "...", "columns": [...]}``). The full polymorphism is handled by
    # ``SlayerQuery.model_validate`` downstream.
    source_model: Optional[Union[str, Dict[str, Any]]] = None
    # ``measures`` and ``dimensions`` accept bare strings as a shorthand,
    # mirroring the Python API: ``"*:count"`` is lifted to
    # ``{"formula": "*:count"}``, ``"status"`` to ``{"name": "status"}``.
    # ``SlayerQuery``'s before-validators (``_coerce_measures`` /
    # ``_coerce_dimensions``) do the actual lifting downstream.
    measures: Optional[List[Union[str, Dict[str, Any]]]] = None
    dimensions: Optional[List[Union[str, Dict[str, Any]]]] = None
    time_dimensions: Optional[List[Dict[str, Any]]] = None
    filters: Optional[List[str]] = None
    order: Optional[List[Dict[str, Any]]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    whole_periods_only: Optional[bool] = None
    dry_run: Optional[bool] = None
    explain: Optional[bool] = None
    variables: Optional[Dict[str, Any]] = None


class QueryListRequest(BaseModel):
    """Body shape for multi-stage DAG queries at ``POST /query``.

    ``queries`` is a non-empty list of query dicts forming a DAG —
    same shape as ``engine.execute(query=[...])`` and the MCP
    ``query_nested`` tool. Order doesn't matter; the engine auto-sorts.
    Top-level ``variables`` / ``dry_run`` / ``explain`` apply to the
    whole execution.
    """

    model_config = ConfigDict(extra="forbid")

    queries: List[Dict[str, Any]]
    variables: Optional[Dict[str, Any]] = None
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


class ValidateModelsRequest(BaseModel):
    data_source: Optional[str] = None


class DatasourcePriorityRequest(BaseModel):
    """Body for ``PUT /datasources/priority``. A request model — rather
    than a raw ``Dict[str, List[str]]`` — so OpenAPI advertises the exact
    shape and FastAPI rejects mistyped payloads with 422 instead of
    silently coercing them downstream.
    """
    priority: List[str] = []


class SaveMemoryRequest(BaseModel):
    """Body for ``POST /memories``.

    ``linked_entities`` accepts either a list of entity-reference
    strings or an inline ``SlayerQuery`` payload (a JSON object). Both
    forms are dispatched server-side: a list runs strict per-token
    resolution, an object validates as a ``SlayerQuery`` and triggers
    entity extraction (the query is then persisted alongside the
    learning).

    DEV-1428: optional ``id`` lets callers pin the memory's canonical id
    (e.g. for knowledge-base ingestion that wants stable string ids
    like ``kb.policy.42``). Bad charset → 400. Omit → auto-allocated
    int-shaped id.
    """

    learning: str
    linked_entities: Any
    id: Optional[str] = None


class SearchRequest(BaseModel):
    """Body for ``POST /search`` (DEV-1375). Mirrors the MCP / CLI /
    SlayerClient surfaces.

    All retrieval inputs are optional. Empty input falls back to a
    recency listing capped at ``max_results`` hits.
    """

    model_config = ConfigDict(extra="forbid")

    entities: Optional[List[str]] = None
    query: Optional[Any] = None
    question: Optional[str] = None
    datasource: Optional[str] = None
    max_results: int = 10
    cypher_filter: Optional[str] = None


def _slayer_version() -> str:
    try:
        return _pkg_version("motley-slayer")
    except PackageNotFoundError:
        return "0.0.0+unknown"


def create_app(  # NOSONAR(S3776) — FastAPI route-handler factory; complexity is the cumulative inline closure body of every @app.<route> handler. Splitting would require dependency-injecting `engine`/`storage`/`mcp` into a separate module — out of scope for incremental PRs.
    storage: StorageBackend,
    *,
    ingest_on_startup: bool = False,
) -> FastAPI:
    if ingest_on_startup:
        import sys

        from slayer.async_utils import run_sync
        from slayer.engine.ingestion import ingest_all_datasources_idempotent

        run_sync(
            ingest_all_datasources_idempotent(storage=storage, stream=sys.stderr)
        )
    app = FastAPI(title="SLayer", version=_slayer_version())
    engine = SlayerQueryEngine(storage=storage)

    # Mount MCP server over SSE at /mcp. The embedded server intentionally
    # does NOT receive `ingest_on_startup` — orchestration happens once,
    # above, so calling `create_app(ingest_on_startup=True)` doesn't fire
    # the orchestrator twice.
    mcp = create_mcp_server(storage=storage)
    mcp_app = mcp.sse_app()
    app.mount("/mcp", mcp_app)

    @app.get("/health")
    async def health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/query",
        responses={
            400: {"description": "Invalid query payload (e.g. missing source_model/name, mutually exclusive fields, validation error)."},
            422: {
                "description": (
                    "Schema drift detected on the touched models — query "
                    "could not run against the live schema. Body shape: "
                    "``{\"error\": \"schema_drift\", \"models\": [...], "
                    "\"to_delete\": [ToDeleteEntry], \"original\": str|null}``. "
                    "Run validate_models for the same datasource to inspect."
                ),
            },
        },
    )
    async def query(
        request: Union[QueryRequest, QueryListRequest],
    ) -> QueryResponse:
        try:
            # Multi-stage DAG: body is ``{"queries": [...], "variables": ...,
            # "dry_run": ..., "explain": ...}``. Mirrors the MCP
            # ``query_nested`` tool and ``engine.execute(query=[...])``.
            # Engine auto-sorts the list and validates DAG invariants.
            if isinstance(request, QueryListRequest):
                if not request.queries:
                    raise HTTPException(
                        status_code=400,
                        detail="'queries' must be a non-empty list.",
                    )
                dry_run = bool(request.dry_run)
                explain = bool(request.explain)
                result = await engine.execute(
                    query=list(request.queries),
                    variables=request.variables or {},
                    dry_run=dry_run,
                    explain=explain,
                )
            # Run-by-name: ``{"name": "<model>", "variables": {...}}``
            # routes through ``engine.execute(str)`` so the model's stored
            # backing query runs directly. Cannot be combined with
            # ``source_model`` or other query fields.
            elif request.name is not None:
                disallowed = [
                    f for f in (
                        request.source_model, request.measures, request.dimensions,
                        request.time_dimensions, request.filters, request.order,
                        request.limit, request.offset, request.whole_periods_only,
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
                dry_run = bool(request.dry_run)
                explain = bool(request.explain)
                result = await engine.execute(
                    request.name,
                    variables=request.variables or {},
                    dry_run=dry_run,
                    explain=explain,
                )
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
                # ``dry_run``/``explain`` are execution-mode flags only — pop
                # them here and pass as engine kwargs so v3 SlayerQuery
                # (extra="forbid") doesn't reject them.
                dry_run = bool(payload.pop("dry_run", False))
                explain = bool(payload.pop("explain", False))
                slayer_query = SlayerQuery.model_validate(payload)
                result = await engine.execute(
                    query=slayer_query,
                    variables=runtime_kwarg,
                    dry_run=dry_run,
                    explain=explain,
                )
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
            if dry_run or explain:
                response.sql = result.sql
            return response
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except SchemaDriftError as drift:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "schema_drift",
                    "models": drift.models,
                    "to_delete": [
                        e.model_dump(mode="json") for e in drift.to_delete
                    ],
                    "original": str(drift.__cause__) if drift.__cause__ else None,
                },
            )

    @app.get("/models")
    async def list_models(
        data_source: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        identities = await storage._list_all_model_identities()
        result = []
        for ds_name, name in identities:
            if data_source is not None and ds_name != data_source:
                continue
            model = await storage.get_model(name, data_source=ds_name)
            if model is None or model.hidden:
                continue
            entry: Dict[str, Any] = {"name": name, "data_source": ds_name}
            if model.description:
                entry["description"] = model.description
            result.append(entry)
        return result

    @app.get(
        "/models/{name}",
        responses={
            409: {
                "description": (
                    "Model name resolves to multiple datasources. Pass "
                    "``data_source=...`` as a query parameter, or set a "
                    "datasource priority via PUT /datasources/priority."
                )
            }
        },
    )
    async def get_model(
        name: str,
        data_source: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            model = await storage.get_model(name, data_source=data_source)
        except AmbiguousModelError as exc:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{exc} Pass data_source=... as a query parameter, "
                    f"or PUT /datasources/priority to disambiguate."
                ),
            )
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

    @app.delete(
        "/models/{name}",
        responses={
            409: {
                "description": (
                    "Model name resolves to multiple datasources. Pass "
                    "``data_source=...`` as a query parameter, or set a "
                    "datasource priority via PUT /datasources/priority."
                )
            }
        },
    )
    async def delete_model(
        name: str,
        data_source: Optional[str] = None,
    ) -> Dict[str, Any]:
        try:
            deleted = await storage.delete_model(name, data_source=data_source)
        except AmbiguousModelError as exc:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"{exc} Pass data_source=... as a query parameter, "
                    f"or PUT /datasources/priority to disambiguate."
                ),
            )
        if not deleted:
            raise HTTPException(status_code=404, detail=f"Model '{name}' not found")
        return {"status": "deleted", "name": name}

    @app.get("/datasources/priority")
    async def get_datasource_priority() -> Dict[str, List[str]]:
        return {"priority": await storage.get_datasource_priority()}

    @app.put(
        "/datasources/priority",
        responses={
            400: {
                "description": (
                    "Priority list contains a name that is not a "
                    "registered DatasourceConfig."
                )
            }
        },
    )
    async def put_datasource_priority(body: DatasourcePriorityRequest) -> Dict[str, Any]:
        priority = list(body.priority)
        try:
            await storage.set_datasource_priority(priority)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"status": "ok", "priority": priority}

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

    @app.post(
        "/validate-models",
        responses={
            404: {"description": "Datasource not found."},
            422: {
                "description": (
                    "Datasource configuration error during introspection "
                    "(connection refused, authentication failed, etc.). "
                    "Original DB error message in ``detail``."
                ),
            },
        },
    )
    async def validate_models_endpoint(
        request: ValidateModelsRequest,
    ) -> List[Dict[str, Any]]:
        """Diff persisted SlayerModels against live DB schemas. Read-only."""
        if request.data_source is not None:
            ds_check = await storage.get_datasource(request.data_source)
            if ds_check is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Datasource '{request.data_source}' not found",
                )
        from sqlalchemy.exc import SQLAlchemyError

        try:
            entries = await engine.validate_models(data_source=request.data_source)
        except SQLAlchemyError as exc:
            safe_ds = (request.data_source or "").replace("\r", "").replace("\n", "")
            logger.exception("validate_models failed for datasource %r", safe_ds)
            raise HTTPException(
                status_code=422,
                detail=(
                    f"validate_models failed for datasource '{safe_ds}': {exc}"
                ),
            )
        return [e.model_dump(mode="json") for e in entries]

    @app.post(
        "/ingest",
        responses={
            404: {"description": "Datasource not found."},
            422: {
                "description": (
                    "Datasource configuration error — connection refused, "
                    "authentication failed, schema introspection failed, or "
                    "the datasource config itself is invalid (unresolved "
                    "``${ENV_VAR}`` placeholder, malformed connection string, "
                    "etc.). Original error message in ``detail``."
                ),
            },
        },
    )
    async def ingest(request: IngestRequest) -> Dict[str, Any]:
        # Strip newlines from the user-controlled datasource name before it
        # reaches the log or the response detail (S5145 — log-injection
        # surface). The ds value already round-tripped through Pydantic so
        # this is purely defence-in-depth.
        safe_ds_name = request.datasource.replace("\r", "").replace("\n", "")
        try:
            ds = await storage.get_datasource(request.datasource)
        except ValueError as exc:
            # ``storage.get_datasource`` calls ``resolve_env_vars()`` which
            # raises ValueError for unresolved ``${ENV_VAR}`` placeholders.
            # User-correctable, so 422 not 500.
            logger.exception(
                "Ingest config error for datasource %r", safe_ds_name
            )
            raise HTTPException(
                status_code=422,
                detail=f"Ingest failed for datasource '{safe_ds_name}': {exc}",
            )
        if ds is None:
            raise HTTPException(
                status_code=404, detail=f"Datasource '{request.datasource}' not found"
            )
        from sqlalchemy.exc import SQLAlchemyError
        from slayer.engine.ingestion import ingest_datasource_idempotent

        try:
            result = await ingest_datasource_idempotent(
                datasource=ds,
                storage=storage,
                include_tables=request.include_tables,
                exclude_tables=request.exclude_tables,
                schema=request.schema_name,
            )
        except SQLAlchemyError as exc:
            # OperationalError / DatabaseError both derive from SQLAlchemyError
            # (Sonar S5713 — catching them separately is redundant).
            logger.exception("Ingest failed for datasource %r", safe_ds_name)
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Ingest failed for datasource '{safe_ds_name}': {exc}"
                ),
            )
        except ValueError as exc:
            # Non-SQLAlchemy config errors raised inside the introspection
            # path (e.g. malformed connection string, missing required
            # field) — user-correctable.
            logger.exception(
                "Ingest config error for datasource %r", safe_ds_name
            )
            raise HTTPException(
                status_code=422,
                detail=f"Ingest failed for datasource '{safe_ds_name}': {exc}",
            )
        if result.errors:
            # Partial failure — at least one model failed to persist.
            # Mirror the CLI's exit-1 behaviour by surfacing 422 with the
            # full IdempotentIngestResult body (additions/to_delete/errors).
            raise HTTPException(
                status_code=422, detail=result.model_dump(mode="json")
            )
        return result.model_dump(mode="json")

    # ---------- DEV-1357 v2: Memory endpoints ------------------------------

    memory_service = MemoryService(storage=storage)

    @app.post(
        "/memories",
        responses={
            400: {
                "description": (
                    "Invalid input: empty learning, empty entity list, "
                    "ambiguous bare-name reference, or unknown entity."
                )
            }
        },
    )
    async def save_memory(request: SaveMemoryRequest) -> Dict[str, Any]:
        try:
            response = await memory_service.save_memory(
                learning=request.learning,
                linked_entities=request.linked_entities,
                id=request.id,
            )
        except (
            EntityResolutionError,
            AmbiguousModelError,
            ValueError,
        ) as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return response.model_dump(mode="json")

    @app.delete(
        "/memories/{memory_id}",
        responses={
            400: {"description": "Invalid memory id (charset violation)."},
            404: {"description": "Memory not found."},
        },
    )
    async def delete_memory(memory_id: str) -> Dict[str, Any]:
        try:
            response = await memory_service.forget_memory(identifier=memory_id)
        except MemoryNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return response.model_dump(mode="json")

    # ---------- DEV-1375: semantic search -----------------------------

    search_service = SearchService(storage=storage)

    @app.post(
        "/search",
        responses={
            400: {
                "description": (
                    "Invalid input: ambiguous bare-name reference, "
                    "unknown entity, or malformed query payload."
                )
            }
        },
    )
    async def search(request: SearchRequest) -> Dict[str, Any]:
        try:
            response = await search_service.search(
                entities=request.entities,
                query=request.query,
                question=request.question,
                datasource=request.datasource,
                max_results=request.max_results,
                cypher_filter=request.cypher_filter,
            )
        except (SlayerError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return response.model_dump(mode="json")

    return app
