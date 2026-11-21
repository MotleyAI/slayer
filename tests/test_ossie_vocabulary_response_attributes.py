"""A response lists its metric columns under ``attributes.metrics``; a serialized response carries no ``measures``.

Spec: compatibility/ossie-vocabulary › Response metadata names metrics.

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.core.models import SlayerDataset
from slayer.mcp.server import create_mcp_server
from tests._deprecated_input_helpers import recorded
from tests._ossie_vocabulary_fixtures import BACKENDS, shop_customers, shop_orders, shop_store, vocab_engine

QUERY: dict[str, Any] = {"population": "orders", "dimensions": [{"field": "orders.status", "label": "Status"}],
                         "metrics": [{"metric": "sum(orders.amount)", "alias": "total", "label": "Total"}]}
METRIC_COLUMN = "total"
#: The old Python attribute, read by name: it is deprecated on purpose.
OLD_ATTRIBUTE = "measures"


@pytest.mark.parametrize("backend", BACKENDS)
async def test_the_engine_response_names_metrics(backend: str) -> None:
    async with vocab_engine(backend) as engine:
        response = await engine.execute(QUERY)
    assert METRIC_COLUMN in response.columns
    assert set(response.attributes.metrics) == {METRIC_COLUMN}
    assert response.attributes.metrics[METRIC_COLUMN].label == "Total"
    assert "orders.status" in response.attributes.dimensions
    dumped = response.model_dump(mode="json")
    assert "measures" not in dumped["attributes"]
    assert dumped["columns"] == list(response.columns)


async def test_the_old_python_attribute_reads_metrics_with_one_warning() -> None:
    async with vocab_engine("sqlite") as engine:
        response = await engine.execute(QUERY)
    with recorded() as caught:
        old = getattr(response.attributes, OLD_ATTRIBUTE)
    assert old == response.attributes.metrics
    [w] = [w for w in caught if issubclass(w.category, FutureWarning)]
    assert "metrics" in str(w.message)
    assert "3.0" in str(w.message)
    assert OLD_ATTRIBUTE not in response.attributes.model_dump(mode="json")


async def test_rest_attributes_carry_metrics_only() -> None:
    async with vocab_engine("sqlite") as engine:
        resp = TestClient(create_app(storage=engine.storage)).post("/query", json=QUERY)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body["attributes"]["metrics"]) == {METRIC_COLUMN}
    assert "measures" not in body["attributes"]
    assert body["columns"][-1] == METRIC_COLUMN


async def test_mcp_json_attributes_carry_metrics_only() -> None:
    async with vocab_engine("sqlite") as engine:
        server = create_mcp_server(storage=engine.storage, _seed_help=False)
        blocks, _ = await server.call_tool(name="query", arguments={"query": QUERY, "format": "json"})
    attributes = json.loads(getattr(next(iter(blocks)), "text"))["attributes"]
    assert set(attributes["metrics"]) == {METRIC_COLUMN}
    assert "measures" not in attributes


def routed_client(app: TestClient, *, path_map: dict[str, str] | None = None) -> SlayerClient:
    """An HTTP ``SlayerClient`` whose requests go to ``app``, each path prefix rewritten by ``path_map``."""
    client = SlayerClient(url="http://testserver")

    async def request(*, method: str, path: str, json: dict | None = None, params: dict | None = None) -> Any:  # NOSONAR(S7503) — matches the awaited transport
        for new, old in (path_map or {}).items():
            if path.startswith(new):
                path = old + path[len(new):]
        resp = app.request(method, path, json=json, params=params)
        resp.raise_for_status()
        return resp.json()

    setattr(client, "_request", request)
    return client


async def shop_app(tmp_path: Path) -> TestClient:
    storage = await shop_store(backend="yaml", base=str(tmp_path))
    await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
    return TestClient(create_app(storage=storage))


SHOP_QUERY: dict[str, Any] = {"population": "orders", "dimensions": [{"field": "customers.region", "label": "Region"}],
                              "metrics": [{"metric": "sum(orders.amount)", "alias": "total", "label": "Total"}]}


async def test_the_http_client_reads_metrics(tmp_path: Path) -> None:
    app = await shop_app(tmp_path)
    client = routed_client(app)
    await client.create_dataset(shop_orders())
    response = await client.query(SHOP_QUERY)
    assert set(response.attributes.metrics) == {"total"}
    assert "customers.region" in response.attributes.dimensions


async def test_the_http_client_reads_metrics_through_an_old_route(tmp_path: Path) -> None:
    app = await shop_app(tmp_path)
    client = routed_client(app, path_map={"/datasets": "/models"})
    with recorded():
        await client.create_dataset(shop_orders())
    assert await client.get_dataset("orders", data_source="shop") is not None
    response = await client.query(SHOP_QUERY)
    assert set(response.attributes.metrics) == {"total"}
