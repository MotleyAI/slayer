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

from slayer.async_utils import run_sync
from slayer.core.format import NumberFormat
from slayer.core.query import SlayerQuery
from slayer.core.recommend import RootModelRecommendation
from slayer.engine.query_engine import (
    FieldMetadata,
    ResponseAttributes,
    SlayerQueryEngine,
    SlayerResponse,
)
from slayer.inspect.service import InspectService
from slayer.memories.models import (
    ForgetMemoryResponse,
    SaveMemoryResponse,
)
from slayer.memories.service import MemoryService
from slayer.search.service import SearchResponse, SearchService

# httpx / pandas are the optional ``client`` extra; keep them import-safe so
# local-engine mode loads without them.
try:
    import httpx
except ImportError:
    httpx = None
try:
    import pandas as pd
except ImportError:
    pd = None

if TYPE_CHECKING:
    from slayer.core.policy import SessionPolicy

logger = logging.getLogger(__name__)


# Input union for every public query entry point (mirrors ``engine.execute``):
# ``str`` = run-by-name, list = multi-stage DAG. ``Mapping``/``Sequence`` keep
# pyright's invariance check from rejecting ``list[dict[str, str]]``.
QueryInput = (
    SlayerQuery
    | Mapping[str, Any]
    | Sequence[SlayerQuery | Mapping[str, Any]]
    | str
)


class SlayerClient:
    """Async-first client for the SLayer REST API, or direct local mode.

    Remote: ``SlayerClient(url=...)``. Local (no server):
    ``SlayerClient(storage=YAMLStorage(...))``. Async ``query`` / sync
    ``query_sync``.
    """

    def __init__(
        self,
        url: str = "http://localhost:5143",
        storage: Any | None = None,
        *,
        policy: "SessionPolicy | None" = None,
    ):
        self.url = url.rstrip("/")
        self._storage = storage
        self._engine = None
        if storage is not None:
            self._engine = SlayerQueryEngine(storage=storage, policy=policy)
        elif policy is not None:
            # Forced-filter policy is local-engine only; fail fast rather than
            # silently disable a security control over HTTP.
            raise ValueError(
                "policy= is only supported in local-engine mode (pass "
                "storage=...); server-side policy over HTTP is not yet "
                "available."
            )

    async def _request(
        self,
        method: str,
        path: str,
        json: dict | None = None,
        params: dict | None = None,
    ) -> Any:
        if httpx is None:
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
        if httpx is None:
            raise ImportError("Client requires httpx: pip install motley-slayer[client]")
        with httpx.Client() as client:
            resp = client.request(
                method=method, url=f"{self.url}{path}", json=json, params=params
            )
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    def _validated_dump(payload: Mapping[str, Any]) -> dict[str, Any]:
        """Normalise a single-query payload through ``SlayerQuery`` so the
        server's ``QueryRequest`` accepts string-shorthand measures/dimensions."""
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
        """Build the ``POST /query`` JSON body from any accepted input shape
        (``str`` run-by-name, list DAG, ``Mapping``, or ``SlayerQuery``); never
        mutates caller-owned data. Shared by sync + async transports."""
        if isinstance(query, str):
            body: dict[str, Any] = {"name": query}
        elif isinstance(query, SlayerQuery):
            body = query.model_dump(mode="json", exclude_none=True)
        elif isinstance(query, ABCSequence) and not isinstance(
            query, (bytes, bytearray)
        ):
            # bytes are Sequences too; route them to the else-branch below.
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
        """Coerce ``Mapping``/``Sequence`` inputs to concrete ``dict``/``list``
        so ``engine.execute``'s ``isinstance`` dispatch resolves them correctly."""
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
        """Execute a query. Accepts ``SlayerQuery`` / ``dict`` / list-DAG /
        ``str`` (run-by-name) — the same union as ``engine.execute``."""
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
        """Generate SQL for a query without executing it (same input union)."""
        return (await self.query(query=query, dry_run=True)).sql

    async def explain(self, query: QueryInput) -> SlayerResponse:
        """Run EXPLAIN ANALYZE on a query (same input union)."""
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
        if self._storage is None:
            raise RuntimeError(
                "Memory operations need a storage backend; remote-mode "
                "callers go through the HTTP code path."
            )
        return MemoryService(storage=self._storage)

    @staticmethod
    def _coerce_linked_entities(value):
        # SlayerQuery → dict; lists/dicts pass through (service revalidates).
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
        """Save a memory (learning + linked entities, or an inline SlayerQuery
        to extract them from). ``id`` pins the canonical id; ``description`` is
        a ≤500-char preview surfaced by search/inspect."""
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
        # Percent-encode: ids are arbitrary strings that may hold reserved
        # URL characters.
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
        """Up-to-three-channel semantic search (BM25 + full-text + optional
        embeddings, RRF-fused) over memories and canonical entities.
        ``datasource`` scopes results to one datasource; an unknown one raises
        ``ValueError`` in local mode, HTTP 400 in remote mode."""
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
        reference: str | list[str] | None = None,
        entity_type: str,
        compact: bool = True,
        format: str = "markdown",
        num_rows: int = 3,
        show_sql: bool = False,
        sections: list[str] | None = None,
        descriptions_max_chars: int | None = None,
    ) -> str:
        """Point-lookup one entity, a same-kind batch (``reference`` a list), or
        a whole collection (``reference`` ``None``/``[]``; ``model``/
        ``datasource`` only). ``entity_type`` is required and applies to every
        id; a list returns one block per id with per-id error isolation."""
        if self._storage is not None:
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
        reference: str | list[str] | None,
        entity_type: str,
        compact: bool,
        format: str,
        num_rows: int,
        show_sql: bool,
        sections: list[str] | None,
        descriptions_max_chars: int | None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
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
        """Execute a query synchronously (same input union as ``query``)."""
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
        """Generate SQL synchronously (same input union)."""
        return self.query_sync(query=query, dry_run=True).sql

    def explain_sync(self, query: QueryInput) -> SlayerResponse:
        """Run EXPLAIN ANALYZE synchronously (same input union)."""
        return self.query_sync(query=query, explain=True)

    def inspect_sync(
        self,
        *,
        reference: str | list[str] | None = None,
        entity_type: str,
        compact: bool = True,
        format: str = "markdown",
        num_rows: int = 3,
        show_sql: bool = False,
        sections: list[str] | None = None,
        descriptions_max_chars: int | None = None,
    ) -> str:
        """Synchronous variant of :meth:`inspect`."""
        if self._storage is not None:
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

    async def recommend_root_model(
        self, items: list[str], *, data_source: str | None = None,
        root_hint: str | None = None,
    ) -> "RootModelRecommendation":
        """Recommend the query root (``source_model``) for a set of
        ``model.column``/``model.metric`` items, plus each item's path from it.
        ``root_hint`` forces the root when it reaches every item."""
        if self._engine is not None:
            return await self._engine.recommend_root_model(
                items, data_source=data_source, root_hint=root_hint
            )
        body: dict[str, Any] = {"items": items}
        if data_source is not None:
            body["data_source"] = data_source
        if root_hint is not None:
            body["root_hint"] = root_hint
        result = await self._request(
            method="POST", path="/recommend-root-model", json=body
        )
        return RootModelRecommendation.model_validate(result)

    def recommend_root_model_sync(
        self, items: list[str], *, data_source: str | None = None,
        root_hint: str | None = None,
    ) -> "RootModelRecommendation":
        """Synchronous variant of :meth:`recommend_root_model`."""
        return run_sync(self.recommend_root_model(
            items, data_source=data_source, root_hint=root_hint
        ))

    def query_df(self, query: QueryInput):
        """Execute a query and return a pandas DataFrame (sync; same input union)."""
        if pd is None:
            raise ImportError("DataFrame support requires pandas: pip install motley-slayer[client]")
        result = self.query_sync(query=query)
        return pd.DataFrame(result.data)

    def list_models_sync(self) -> list[str]:
        return self._request_sync(method="GET", path="/models")

    def get_model_sync(self, name: str) -> dict[str, Any]:
        return self._request_sync("GET", f"/models/{name}")
