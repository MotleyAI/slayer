"""Python client for SLayer API."""

import logging
from typing import Any, Dict, List, Optional

from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import FieldMetadata, SlayerResponse

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

    async def _request(self, method: str, path: str, json: Optional[Dict] = None) -> Any:
        try:
            import httpx
        except ImportError:
            raise ImportError("Client requires httpx: pip install motley-slayer[client]")
        async with httpx.AsyncClient() as client:
            resp = await client.request(method=method, url=f"{self.url}{path}", json=json)
            resp.raise_for_status()
            return resp.json()

    def _request_sync(self, method: str, path: str, json: Optional[Dict] = None) -> Any:
        try:
            import httpx
        except ImportError:
            raise ImportError("Client requires httpx: pip install motley-slayer[client]")
        with httpx.Client() as client:
            resp = client.request(method=method, url=f"{self.url}{path}", json=json)
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    def _parse_response(result: dict) -> SlayerResponse:
        """Parse an API JSON response into a SlayerResponse."""
        meta = {}
        for k, v in (result.get("meta") or {}).items():
            meta[k] = FieldMetadata(label=v.get("label"))
        return SlayerResponse(
            data=result["data"],
            columns=result.get("columns") or [],
            sql=result.get("sql"),
            meta=meta,
        )

    # ----- Async API -----

    async def query(self, query) -> SlayerResponse:
        """Execute a query asynchronously. Accepts SlayerQuery or dict."""
        if isinstance(query, dict):
            query = SlayerQuery.model_validate(query)
        if self._engine is not None:
            return await self._engine.execute(query=query)
        result = await self._request(method="POST", path="/query", json=query.model_dump(exclude_none=True))
        return self._parse_response(result)

    async def sql(self, query) -> str:
        """Generate SQL for a query without executing it."""
        if isinstance(query, dict):
            query = SlayerQuery.model_validate(query)
        dry_query = query.model_copy(update={"dry_run": True})
        return (await self.query(query=dry_query)).sql

    async def explain(self, query) -> SlayerResponse:
        """Run EXPLAIN ANALYZE on a query."""
        if isinstance(query, dict):
            query = SlayerQuery.model_validate(query)
        explain_query = query.model_copy(update={"explain": True})
        return await self.query(query=explain_query)

    async def list_models(self) -> List[str]:
        return await self._request(method="GET", path="/models")

    async def get_model(self, name: str) -> Dict[str, Any]:
        return await self._request(method="GET", path=f"/models/{name}")

    async def create_model(self, model: Dict[str, Any]) -> Dict[str, str]:
        return await self._request(method="POST", path="/models", json=model)

    async def list_datasources(self) -> List[str]:
        return await self._request(method="GET", path="/datasources")

    async def create_datasource(self, datasource: Dict[str, Any]) -> Dict[str, str]:
        return await self._request(method="POST", path="/datasources", json=datasource)

    # ----- Sync API (for notebooks, scripts, CLI) -----

    def query_sync(self, query) -> SlayerResponse:
        """Execute a query synchronously. Accepts SlayerQuery or dict."""
        if isinstance(query, dict):
            query = SlayerQuery.model_validate(query)
        if self._engine is not None:
            return self._engine.execute_sync(query=query)
        result = self._request_sync(method="POST", path="/query", json=query.model_dump(exclude_none=True))
        return self._parse_response(result)

    def sql_sync(self, query) -> str:
        """Generate SQL synchronously."""
        if isinstance(query, dict):
            query = SlayerQuery.model_validate(query)
        dry_query = query.model_copy(update={"dry_run": True})
        return self.query_sync(query=dry_query).sql

    def explain_sync(self, query) -> SlayerResponse:
        """Run EXPLAIN ANALYZE synchronously."""
        if isinstance(query, dict):
            query = SlayerQuery.model_validate(query)
        explain_query = query.model_copy(update={"explain": True})
        return self.query_sync(query=explain_query)

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
