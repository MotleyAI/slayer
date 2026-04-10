"""Python client for SLayer API."""

import logging
from typing import Any, Dict, List, Optional

from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import FieldMetadata, SlayerResponse

logger = logging.getLogger(__name__)


class SlayerClient:
    """Client for the SLayer REST API, or direct local mode (no server).

    Usage:
        # Remote mode (connects to running server)
        client = SlayerClient(url="http://localhost:5143")

        # Local mode (no server needed)
        from slayer.storage.yaml_storage import YAMLStorage
        client = SlayerClient(storage=YAMLStorage(base_dir="./my_models"))
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

    @staticmethod
    def _coerce_query(query: "SlayerQuery | dict") -> SlayerQuery:
        if isinstance(query, dict):
            return SlayerQuery.model_validate(query)
        return query

    def _request(self, method: str, path: str, json: Optional[Dict] = None) -> Any:
        try:
            import httpx
        except ImportError:
            raise ImportError("Client requires httpx: pip install motley-slayer[client]")
        with httpx.Client() as client:
            resp = client.request(method=method, url=f"{self.url}{path}", json=json)
            resp.raise_for_status()
            return resp.json()

    def query(self, query: "SlayerQuery | dict") -> SlayerResponse:
        """Execute a query and return a SlayerResponse."""
        query = self._coerce_query(query)
        if self._engine is not None:
            return self._engine.execute(query=query)
        result = self._request("POST", "/query", json=query.model_dump(exclude_none=True))
        meta = {}
        for k, v in (result.get("meta") or {}).items():
            meta[k] = FieldMetadata(label=v.get("label"))
        return SlayerResponse(
            data=result["data"],
            columns=result.get("columns") or [],
            sql=result.get("sql"),
            meta=meta,
        )

    def sql(self, query: "SlayerQuery | dict") -> str:
        """Generate SQL for a query without executing it."""
        query = self._coerce_query(query)
        dry_query = query.model_copy(update={"dry_run": True})
        return self.query(query=dry_query).sql

    def explain(self, query: "SlayerQuery | dict") -> SlayerResponse:
        """Run EXPLAIN ANALYZE on a query and return the result."""
        query = self._coerce_query(query)
        explain_query = query.model_copy(update={"explain": True})
        return self.query(query=explain_query)

    def query_df(self, query: "SlayerQuery | dict"):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("DataFrame support requires pandas: pip install motley-slayer[client]")
        result = self.query(query=query)
        return pd.DataFrame(result.data)

    def list_models(self) -> List[str]:
        return self._request("GET", "/models")

    def get_model(self, name: str) -> Dict[str, Any]:
        return self._request("GET", f"/models/{name}")

    def create_model(self, model: Dict[str, Any]) -> Dict[str, str]:
        return self._request("POST", "/models", json=model)

    def list_datasources(self) -> List[str]:
        return self._request("GET", "/datasources")

    def create_datasource(self, datasource: Dict[str, Any]) -> Dict[str, str]:
        return self._request("POST", "/datasources", json=datasource)
