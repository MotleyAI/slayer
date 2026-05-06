"""Python client for SLayer API."""

import logging
from typing import Any, Dict, List, Optional, Union

from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import FieldMetadata, ResponseAttributes, SlayerResponse
from slayer.memories.models import (
    ForgetMemoryResponse,
    RecallResponse,
    SaveMemoryResponse,
)

logger = logging.getLogger(__name__)


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
        storage: Optional[Any] = None,
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
        json: Optional[Dict] = None,
        params: Optional[Dict] = None,
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
        json: Optional[Dict] = None,
        params: Optional[Dict] = None,
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
    def _parse_response(result: dict) -> SlayerResponse:
        """Parse an API JSON response into a SlayerResponse."""
        from slayer.core.format import NumberFormat

        def _parse_meta_dict(d: dict) -> Dict[str, FieldMetadata]:
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
        query,
        *,
        dry_run: bool = False,
        explain: bool = False,
    ) -> SlayerResponse:
        """Execute a query asynchronously. Accepts SlayerQuery or dict."""
        if isinstance(query, dict):
            query = SlayerQuery.model_validate(query)
        if self._engine is not None:
            return await self._engine.execute(
                query=query, dry_run=dry_run, explain=explain
            )
        body = query.model_dump(exclude_none=True)
        if dry_run:
            body["dry_run"] = True
        if explain:
            body["explain"] = True
        result = await self._request(method="POST", path="/query", json=body)
        return self._parse_response(result)

    async def sql(self, query) -> str:
        """Generate SQL for a query without executing it."""
        return (await self.query(query=query, dry_run=True)).sql

    async def explain(self, query) -> SlayerResponse:
        """Run EXPLAIN ANALYZE on a query."""
        return await self.query(query=query, explain=True)

    async def list_models(self, data_source: Optional[str] = None) -> List[str]:
        if self._storage is not None:
            names = await self._storage.list_models(data_source=data_source)
            return list(names)
        params = {"data_source": data_source} if data_source else None
        return await self._request(method="GET", path="/models", params=params)  # NOSONAR(S1192) — REST path is the API contract; defining a constant adds indirection without value

    async def get_model(
        self,
        name: str,
        data_source: Optional[str] = None,
    ) -> Optional[Any]:
        if self._storage is not None:
            return await self._storage.get_model(name, data_source=data_source)
        params = {"data_source": data_source} if data_source else None
        return await self._request(method="GET", path=f"/models/{name}", params=params)  # NOSONAR(S1192) — REST path is the API contract; defining a constant adds indirection without value

    async def create_model(self, model: Dict[str, Any]) -> Dict[str, str]:
        return await self._request(method="POST", path="/models", json=model)  # NOSONAR(S1192) — REST path is the API contract; defining a constant adds indirection without value

    async def list_datasources(self) -> List[str]:
        return await self._request(method="GET", path="/datasources")

    async def create_datasource(self, datasource: Dict[str, Any]) -> Dict[str, str]:
        return await self._request(method="POST", path="/datasources", json=datasource)

    async def get_datasource_priority(self) -> List[str]:
        if self._storage is not None:
            return await self._storage.get_datasource_priority()
        body = await self._request(method="GET", path="/datasources/priority")
        return list(body.get("priority", []))

    async def set_datasource_priority(self, priority: List[str]) -> None:
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
        linked_entities: Union[List[str], SlayerQuery, Dict[str, Any]],
    ) -> SaveMemoryResponse:
        """Save a memory: a learning text + linked entities (or an
        inline SlayerQuery to extract entities from)."""
        if self._storage is not None:
            response = await self._memory_service().save_memory(
                learning=learning,
                linked_entities=self._coerce_linked_entities(linked_entities),
            )
            return response
        body = {
            "learning": learning,
            "linked_entities": self._coerce_linked_entities(linked_entities),
        }
        result = await self._request(method="POST", path="/memories", json=body)
        return SaveMemoryResponse.model_validate(result)

    async def forget_memory(
        self, identifier: Union[int, str]
    ) -> ForgetMemoryResponse:
        if self._storage is not None:
            return await self._memory_service().forget_memory(
                identifier=identifier
            )
        result = await self._request(
            method="DELETE", path=f"/memories/{int(identifier)}"
        )
        return ForgetMemoryResponse.model_validate(result)

    async def recall_memories(
        self,
        *,
        about: Union[List[str], SlayerQuery, Dict[str, Any]],
        max_learnings: Optional[int] = None,
        max_queries: Optional[int] = 2,
    ) -> RecallResponse:
        if self._storage is not None:
            return await self._memory_service().recall_memories(
                about=self._coerce_linked_entities(about),
                max_learnings=max_learnings,
                max_queries=max_queries,
            )
        body = {
            "about": self._coerce_linked_entities(about),
            "max_learnings": max_learnings,
            "max_queries": max_queries,
        }
        result = await self._request(
            method="POST", path="/memories/recall", json=body
        )
        return RecallResponse.model_validate(result)

    # ----- Sync API (for notebooks, scripts, CLI) -----

    def query_sync(
        self,
        query,
        *,
        dry_run: bool = False,
        explain: bool = False,
    ) -> SlayerResponse:
        """Execute a query synchronously. Accepts SlayerQuery or dict."""
        if isinstance(query, dict):
            query = SlayerQuery.model_validate(query)
        if self._engine is not None:
            return self._engine.execute_sync(
                query=query, dry_run=dry_run, explain=explain
            )
        body = query.model_dump(exclude_none=True)
        if dry_run:
            body["dry_run"] = True
        if explain:
            body["explain"] = True
        result = self._request_sync(method="POST", path="/query", json=body)
        return self._parse_response(result)

    def sql_sync(self, query) -> str:
        """Generate SQL synchronously."""
        return self.query_sync(query=query, dry_run=True).sql

    def explain_sync(self, query) -> SlayerResponse:
        """Run EXPLAIN ANALYZE synchronously."""
        return self.query_sync(query=query, explain=True)

    def query_df(self, query: SlayerQuery):
        """Execute a query and return a pandas DataFrame (sync)."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("DataFrame support requires pandas: pip install motley-slayer[client]")
        result = self.query_sync(query=query)
        return pd.DataFrame(result.data)

    def list_models_sync(self) -> List[str]:
        return self._request_sync(method="GET", path="/models")

    def get_model_sync(self, name: str) -> Dict[str, Any]:
        return self._request_sync("GET", f"/models/{name}")
