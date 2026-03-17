"""Python client for SLayer API."""

import logging
from typing import Any, Dict, List, Optional

from slayer.core.query import SlayerQuery

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

    def _request(self, method: str, path: str, json: Optional[Dict] = None) -> Any:
        try:
            import httpx
        except ImportError:
            raise ImportError("Client requires httpx: pip install agentic-slayer[client]")
        with httpx.Client() as client:
            resp = client.request(method=method, url=f"{self.url}{path}", json=json)
            resp.raise_for_status()
            return resp.json()

    def query(self, query: SlayerQuery) -> List[Dict[str, Any]]:
        if self._engine is not None:
            result = self._engine.execute(query=query)
            return result.data
        result = self._request("POST", "/query", json=query.model_dump(exclude_none=True))
        return result["data"]

    def query_df(self, query: SlayerQuery):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("DataFrame support requires pandas: pip install agentic-slayer[client]")
        data = self.query(query=query)
        return pd.DataFrame(data)

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
