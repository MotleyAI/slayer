"""Python client for SLayer API."""

import logging
from collections.abc import (
    Mapping,
    Mapping as ABCMapping,
    Sequence,
    Sequence as ABCSequence,
)
from typing import (
    TYPE_CHECKING,
    Any,
)
from urllib.parse import quote

from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import FieldMetadata, ResponseAttributes, SlayerResponse
from slayer.memories.models import (
    ForgetMemoryResponse,
    SaveMemoryResponse,
)

if TYPE_CHECKING:
    from slayer.search.service import SearchResponse

logger = logging.getLogger(__name__)


# DEV-1437: the full input union accepted by every public query entry point,
# mirroring ``SlayerQueryEngine.execute`` / ``execute_sync``. The list form is
# the multi-stage DAG that ``query_nested`` (MCP) and ``POST /query`` with
# ``{"queries": [...]}`` (REST) also accept; ``str`` runs a query-backed model
# by name. ``Mapping``/``Sequence`` (not ``Dict``/``List``) so callers passing
# ``list[dict[str, str]]`` aren't rejected by pyright's invariance check.
QueryInput = (
    SlayerQuery
    | Mapping[str, Any]
    | Sequence[SlayerQuery | Mapping[str, Any]]
    | str
)


class SlayerClient:
    """Async-first client for the SLayer REST API, or direct local mode (no server).

    Usage:
        # Remote mode (connects to running server)
        client = SlayerClient(url="http://localhost:5143")

        # Local mode (no server needed)
        from slayer.storage.yaml_storage import YAMLStorage
        client = SlayerClient(storage=YAMLStorage(base_dir="./my_models"))

        # Async usage
        result = await client.query(query)

        # Sync usage (notebooks, scripts)
        result = client.query_sync(query)
    """

    def __init__(
        self,
        url: str = "http://localhost:5143",
        storage: Any | None = None,
    ):
        self.url = url.rstrip("/")
        self._storage = storage
        self._engine = None
        if storage is not None:
            from slayer.engine.query_engine import SlayerQueryEngine

            self._engine = SlayerQueryEngine(storage=storage)

    async def _request(
        self,
        method: str,
        path: str,
        json: dict | None = None,
        params: dict | None = None,
    ) -> Any:
        try:
            import httpx
        except ImportError:
            raise ImportError("Client requires httpx: pip install motley-slayer[client]")
        async with httpx.AsyncClient() as client:
            resp = await client.request(
                method=method, url=f"{self.url}{path}", json=json, params=params
            )
            resp.raise_for_status()
            return resp.json()

    def _request_sync(
        self,
        method: str,
        path: str,
        json: dict | None = None,
        params: dict | None = None,
    ) -> Any:
        try:
            import httpx
        except ImportError:
            raise ImportError("Client requires httpx: pip install motley-slayer[client]")
        with httpx.Client() as client:
            resp = client.request(
                method=method, url=f"{self.url}{path}", json=json, params=params
            )
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    def _validated_dump(payload: Mapping[str, Any]) -> dict[str, Any]:
        """Round-trip a single-query payload through ``SlayerQuery`` so
        the server sees the normalised JSON-mode shape. Necessary because
        the server's ``QueryRequest`` declares ``measures`` /
        ``dimensions`` as ``List[Dict[str, Any]]`` and FastAPI rejects
        string-shorthand entries with HTTP 422 before the server can
        re-coerce them — whereas ``SlayerQuery`` itself accepts shorthand
        and normalises it to dict form.
        """
        return SlayerQuery.model_validate(dict(payload)).model_dump(
            mode="json", exclude_none=True
        )

    @staticmethod
    def _build_query_body(
        query: QueryInput,
        *,
        dry_run: bool = False,
        explain: bool = False,
    ) -> dict[str, Any]:
        """Convert any accepted input shape into the JSON body for
        ``POST /query``. Single source of truth shared by sync + async
        transports. Never mutates caller-owned dicts or lists.

        Shapes (mirroring ``engine.execute``):

        * ``str`` → ``{"name": <str>}`` (run-by-name)
        * ``Sequence`` (list/tuple/...) → ``{"queries": [<item-dict>, ...]}``
          — each item is either a ``SlayerQuery`` or a ``Mapping``; both
          are normalised through ``SlayerQuery`` so string-shorthand
          measures / dimensions round-trip cleanly.
        * ``Mapping`` (dict/MappingProxyType/...) → normalised
          ``SlayerQuery`` JSON dump (same reason as above).
        * ``SlayerQuery`` → ``model_dump(mode="json", exclude_none=True)``

        ``dry_run`` and ``explain`` are appended at the top of the body
        when truthy (the server's ``QueryRequest`` and ``QueryListRequest``
        both accept them).
        """
        if isinstance(query, str):
            body: dict[str, Any] = {"name": query}
        elif isinstance(query, SlayerQuery):
            body = query.model_dump(mode="json", exclude_none=True)
        elif isinstance(query, ABCSequence) and not isinstance(
            query, (bytes, bytearray)
        ):
            # ``str`` is also a Sequence but already handled above; guard the
            # binary string types here too so they raise via the else-branch.
            serialised: list[dict[str, Any]] = []
            for i, item in enumerate(query):
                if isinstance(item, SlayerQuery):
                    serialised.append(
                        item.model_dump(mode="json", exclude_none=True)
                    )
                elif isinstance(item, ABCMapping):
                    serialised.append(SlayerClient._validated_dump(item))
                else:
                    raise TypeError(
                        f"query[{i}] must be SlayerQuery or Mapping; got "
                        f"{type(item).__name__}"
                    )
            body = {"queries": serialised}
        elif isinstance(query, ABCMapping):
            body = SlayerClient._validated_dump(query)
        else:
            raise TypeError(
                "query must be SlayerQuery, Mapping, Sequence, or str; got "
                f"{type(query).__name__}"
            )
        if dry_run:
            body["dry_run"] = True
        if explain:
            body["explain"] = True
        return body

    @staticmethod
    def _normalize_for_engine(query: QueryInput) -> Any:
        """Coerce ``Mapping``/``Sequence`` inputs to concrete ``dict`` /
        ``list`` before forwarding to ``engine.execute`` / ``execute_sync``.
        The engine's dispatch uses ``isinstance(query, dict)`` and
        ``isinstance(query, list)`` directly, so a tuple or
        ``MappingProxyType`` would fall through to the SlayerQuery branch
        and crash. Mirrors the runtime contract advertised by
        ``QueryInput`` on both local and HTTP transports.
        """
        if isinstance(query, str) or isinstance(query, SlayerQuery):
            return query
        if isinstance(query, ABCSequence) and not isinstance(
            query, (bytes, bytearray)
        ):
            return [
                item if isinstance(item, SlayerQuery)
                else dict(item) if isinstance(item, ABCMapping)
                else item  # engine raises with the per-item context.
                for item in query
            ]
        if isinstance(query, ABCMapping):
            return dict(query)
        return query  # engine raises with the per-input context.

    @staticmethod
    def _parse_response(result: dict) -> SlayerResponse:
        """Parse an API JSON response into a SlayerResponse."""
        from slayer.core.format import NumberFormat

        def _parse_meta_dict(d: dict) -> dict[str, FieldMetadata]:
            out = {}
            for k, v in (d or {}).items():
                fmt = None
                if v.get("format"):
                    fmt = NumberFormat.model_validate(v["format"])
                out[k] = FieldMetadata(label=v.get("label"), format=fmt)
            return out

        attrs_raw = result.get("attributes") or {}
        attributes = ResponseAttributes(
            dimensions=_parse_meta_dict(attrs_raw.get("dimensions")),
            measures=_parse_meta_dict(attrs_raw.get("measures")),
        )
        return SlayerResponse(
            data=result["data"],
            columns=result.get("columns") or [],
            sql=result.get("sql"),
            attributes=attributes,
        )

    # ----- Async API -----

    async def query(
        self,
        query: QueryInput,
        *,
        dry_run: bool = False,
        explain: bool = False,
    ) -> SlayerResponse:
        """Execute a query asynchronously. Accepts ``SlayerQuery``, ``dict``,
        ``list[SlayerQuery | dict]`` (multi-stage DAG; last element is the
        root), or ``str`` (run a query-backed model by name).
        """
        if self._engine is not None:
            return await self._engine.execute(
                query=self._normalize_for_engine(query),
                dry_run=dry_run,
                explain=explain,
            )
        body = self._build_query_body(
            query, dry_run=dry_run, explain=explain
        )
        result = await self._request(method="POST", path="/query", json=body)
        return self._parse_response(result)

    async def sql(self, query: QueryInput) -> str:
        """Generate SQL for a query without executing it.

        Accepts the same input union as ``query``.
        """
        return (await self.query(query=query, dry_run=True)).sql

    async def explain(self, query: QueryInput) -> SlayerResponse:
        """Run EXPLAIN ANALYZE on a query.

        Accepts the same input union as ``query``.
        """
        return await self.query(query=query, explain=True)

    async def list_models(self, data_source: str | None = None) -> list[str]:
        if self._storage is not None:
            names = await self._storage.list_models(data_source=data_source)
            return list(names)
        params = {"data_source": data_source} if data_source else None
        return await self._request(method="GET", path="/models", params=params)  # NOSONAR(S1192) — REST path is the API contract; defining a constant adds indirection without value

    async def get_model(
        self,
        name: str,
        data_source: str | None = None,
    ) -> Any | None:
        if self._storage is not None:
            return await self._storage.get_model(name, data_source=data_source)
        params = {"data_source": data_source} if data_source else None
        return await self._request(method="GET", path=f"/models/{name}", params=params)  # NOSONAR(S1192) — REST path is the API contract; defining a constant adds indirection without value

    async def create_model(self, model: dict[str, Any]) -> dict[str, str]:
        return await self._request(method="POST", path="/models", json=model)  # NOSONAR(S1192) — REST path is the API contract; defining a constant adds indirection without value

    async def list_datasources(self) -> list[str]:
        return await self._request(method="GET", path="/datasources")

    async def create_datasource(self, datasource: dict[str, Any]) -> dict[str, str]:
        return await self._request(method="POST", path="/datasources", json=datasource)

    async def get_datasource_priority(self) -> list[str]:
        if self._storage is not None:
            return await self._storage.get_datasource_priority()
        body = await self._request(method="GET", path="/datasources/priority")
        return list(body.get("priority", []))

    async def set_datasource_priority(self, priority: list[str]) -> None:
        if self._storage is not None:
            await self._storage.set_datasource_priority(list(priority))
            return
        await self._request(
            method="PUT",
            path="/datasources/priority",
            json={"priority": list(priority)},
        )

    # ----- Memory API (DEV-1357 v2) -----

    def _memory_service(self):
        # Lazy import to avoid pulling MemoryService into the client's
        # remote-only dependency graph (httpx-only deployments).
        from slayer.memories.service import MemoryService

        if self._storage is None:
            raise RuntimeError(
                "Memory operations need a storage backend; remote-mode "
                "callers go through the HTTP code path."
            )
        return MemoryService(storage=self._storage)

    @staticmethod
    def _coerce_linked_entities(value):
        # SlayerQuery → dict for JSON serialisation; lists / dicts pass
        # through. The service layer revalidates at the boundary.
        if isinstance(value, SlayerQuery):
            return value.model_dump(mode="json", exclude_none=True)
        return value

    async def save_memory(
        self,
        *,
        learning: str,
        linked_entities: list[str] | SlayerQuery | dict[str, Any],
        id: str | None = None,  # noqa: A002 — public kwarg matching MCP / REST
        description: str | None = None,
    ) -> SaveMemoryResponse:
        """Save a memory: a learning text + linked entities (or an
        inline SlayerQuery to extract entities from). DEV-1428:
        optional ``id`` lets callers pin the canonical memory id.

        DEV-1549: optional ``description`` is a ≤ 500-char preview
        surfaced by ``search(compact=True)`` and ``inspect_model``.
        """
        if self._storage is not None:
            response = await self._memory_service().save_memory(
                learning=learning,
                linked_entities=self._coerce_linked_entities(linked_entities),
                id=id,
                description=description,
            )
            return response
        body: dict[str, Any] = {
            "learning": learning,
            "linked_entities": self._coerce_linked_entities(linked_entities),
        }
        if id is not None:
            body["id"] = id
        if description is not None:
            body["description"] = description
        result = await self._request(method="POST", path="/memories", json=body)
        return SaveMemoryResponse.model_validate(result)

    async def forget_memory(
        self, identifier: int | str
    ) -> ForgetMemoryResponse:
        if self._storage is not None:
            return await self._memory_service().forget_memory(
                identifier=identifier
            )
        # DEV-1428: ids are arbitrary strings (subject to the charset
        # validator). Percent-encode the path segment so reserved URL
        # characters in valid ids don't break the request.
        encoded = quote(str(identifier), safe="")
        result = await self._request(
            method="DELETE", path=f"/memories/{encoded}",
        )
        return ForgetMemoryResponse.model_validate(result)

    # ----- Search API (DEV-1375) -----

    async def search(
        self,
        *,
        entities: list[str] | None = None,
        query: SlayerQuery | dict[str, Any] | None = None,
        question: str | None = None,
        datasource: str | None = None,
        max_results: int = 10,
        cypher_filter: str | None = None,
        compact: bool = True,
    ) -> "SearchResponse":
        """Up to three-channel semantic search over memories + canonical
        entities.

        Channels: (1) entity-overlap BM25 over memories; (2) tantivy
        full-text over memories ∪ entities; (3) optional dense embedding
        similarity (gated by the ``advanced_search`` extra and a
        configured provider API key). All hits are fused via Reciprocal
        Rank Fusion (``k=60``) into a single ranked ``results`` list.

        ``datasource`` (DEV-1409, optional): when set, scope memories and
        entities to that one datasource. Entity hits are limited to docs
        rooted at the datasource (exact match or dotted-path descendant).
        Memories surface when any of their tagged entities is rooted at
        the datasource.

        Error contract for an unknown ``datasource``:

        * **Local mode** (constructed with ``storage=...``): the in-process
          ``SearchService.search`` raises ``ValueError`` synchronously.
        * **Remote mode** (constructed with ``base_url=...``): the server
          returns HTTP 400 and ``httpx``'s ``raise_for_status`` raises an
          ``HTTPStatusError`` here — it does NOT propagate as ``ValueError``.
        """
        # Local imports: slayer.search.service transitively imports tantivy,
        # which is not part of the client extras (httpx + pandas). Remote-only
        # client installs that never call .search() should not blow up at
        # module-import time.
        from slayer.search.service import SearchResponse, SearchService

        coerced_query: Any = None
        if query is not None:
            coerced_query = (
                query.model_dump(mode="json", exclude_none=True)
                if isinstance(query, SlayerQuery) else query
            )
        if self._storage is not None:
            return await SearchService(storage=self._storage).search(
                entities=entities,
                query=coerced_query,
                question=question,
                datasource=datasource,
                max_results=max_results,
                cypher_filter=cypher_filter,
                compact=compact,
            )
        body: dict[str, Any] = {
            "max_results": max_results,
            "compact": compact,
        }
        if entities is not None:
            body["entities"] = entities
        if coerced_query is not None:
            body["query"] = coerced_query
        if question is not None:
            body["question"] = question
        if datasource is not None:
            body["datasource"] = datasource
        if cypher_filter is not None:
            body["cypher_filter"] = cypher_filter
        result = await self._request(method="POST", path="/search", json=body)
        return SearchResponse.model_validate(result)

    async def inspect(
        self,
        *,
        reference: str,
        entity_type: str,
        compact: bool = True,
        format: str = "markdown",
        num_rows: int = 3,
        show_sql: bool = False,
        sections: Optional[List[str]] = None,
        descriptions_max_chars: Optional[int] = None,
    ) -> str:
        """Inspect EXACTLY one entity by reference and kind (DEV-1588).

        A single-entity point-lookup (no fusion / ranking / bundled
        memories). ``entity_type`` is required, one of
        ``datasource``/``model``/``column``/``measure``/``aggregation``/
        ``memory``.
        """
        if self._storage is not None:
            # Local import: slayer.inspect.service transitively imports the
            # search render stack (tantivy), which is not part of the client
            # extras. Remote-only installs that never call .inspect() must
            # not blow up at module-import time.
            from slayer.inspect.service import InspectService

            return await InspectService(
                storage=self._storage, engine=self._engine,
            ).inspect(
                reference=reference,
                entity_type=entity_type,
                compact=compact,
                format=format,
                num_rows=num_rows,
                show_sql=show_sql,
                sections=sections,
                descriptions_max_chars=descriptions_max_chars,
            )
        body = self._build_inspect_body(
            reference=reference,
            entity_type=entity_type,
            compact=compact,
            format=format,
            num_rows=num_rows,
            show_sql=show_sql,
            sections=sections,
            descriptions_max_chars=descriptions_max_chars,
        )
        resp = await self._request(method="POST", path="/inspect", json=body)
        return resp["result"]

    @staticmethod
    def _build_inspect_body(
        *,
        reference: str,
        entity_type: str,
        compact: bool,
        format: str,
        num_rows: int,
        show_sql: bool,
        sections: Optional[List[str]],
        descriptions_max_chars: Optional[int],
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "reference": reference,
            "entity_type": entity_type,
            "compact": compact,
            "format": format,
            "num_rows": num_rows,
            "show_sql": show_sql,
        }
        if sections is not None:
            body["sections"] = sections
        if descriptions_max_chars is not None:
            body["descriptions_max_chars"] = descriptions_max_chars
        return body

    # ----- Sync API (for notebooks, scripts, CLI) -----

    def query_sync(
        self,
        query: QueryInput,
        *,
        dry_run: bool = False,
        explain: bool = False,
    ) -> SlayerResponse:
        """Execute a query synchronously. Accepts ``SlayerQuery``, ``dict``,
        ``list[SlayerQuery | dict]`` (multi-stage DAG; last element is the
        root), or ``str`` (run a query-backed model by name).
        """
        if self._engine is not None:
            return self._engine.execute_sync(
                query=self._normalize_for_engine(query),
                dry_run=dry_run,
                explain=explain,
            )
        body = self._build_query_body(
            query, dry_run=dry_run, explain=explain
        )
        result = self._request_sync(method="POST", path="/query", json=body)
        return self._parse_response(result)

    def sql_sync(self, query: QueryInput) -> str:
        """Generate SQL synchronously.

        Accepts the same input union as ``query_sync``.
        """
        return self.query_sync(query=query, dry_run=True).sql

    def explain_sync(self, query: QueryInput) -> SlayerResponse:
        """Run EXPLAIN ANALYZE synchronously.

        Accepts the same input union as ``query_sync``.
        """
        return self.query_sync(query=query, explain=True)

    def inspect_sync(
        self,
        *,
        reference: str,
        entity_type: str,
        compact: bool = True,
        format: str = "markdown",
        num_rows: int = 3,
        show_sql: bool = False,
        sections: Optional[List[str]] = None,
        descriptions_max_chars: Optional[int] = None,
    ) -> str:
        """Synchronous variant of :meth:`inspect` (DEV-1588)."""
        if self._storage is not None:
            from slayer.async_utils import run_sync

            return run_sync(self.inspect(
                reference=reference,
                entity_type=entity_type,
                compact=compact,
                format=format,
                num_rows=num_rows,
                show_sql=show_sql,
                sections=sections,
                descriptions_max_chars=descriptions_max_chars,
            ))
        body = self._build_inspect_body(
            reference=reference,
            entity_type=entity_type,
            compact=compact,
            format=format,
            num_rows=num_rows,
            show_sql=show_sql,
            sections=sections,
            descriptions_max_chars=descriptions_max_chars,
        )
        resp = self._request_sync(method="POST", path="/inspect", json=body)
        return resp["result"]

    def query_df(self, query: QueryInput):
        """Execute a query and return a pandas DataFrame (sync).

        Accepts the same input union as ``query_sync``.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("DataFrame support requires pandas: pip install motley-slayer[client]")
        result = self.query_sync(query=query)
        return pd.DataFrame(result.data)

    def list_models_sync(self) -> list[str]:
        return self._request_sync(method="GET", path="/models")

    def get_model_sync(self, name: str) -> dict[str, Any]:
        return self._request_sync("GET", f"/models/{name}")
