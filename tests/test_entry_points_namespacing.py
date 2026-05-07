"""Phase 1b: MCP/REST/Python-client entry points expose the v4 datasource
namespacing contract.

Storage-layer behavior is pinned in ``test_storage_namespacing.py`` and
``test_engine_namespacing.py``; this file pins the user-facing surfaces.

Surfaces covered:

* MCP tools — ``set_datasource_priority`` / ``get_datasource_priority``;
  ``edit_model`` / ``inspect_model`` / ``delete_model`` / ``create_model``
  accept a ``data_source`` argument; ambiguity errors mention both fixes
  (priority list + explicit arg).
* HTTP API — ``GET /models/{name}?data_source=`` / ``DELETE`` / ``409`` on
  ambiguity; ``GET /datasources/priority`` / ``PUT /datasources/priority``.
* Python client — ``client.get_model(name, data_source=...)`` and
  ``client.set_datasource_priority(...)``.

The CLI surface is light glue around storage; covered indirectly by the
storage-namespacing tests + the new API endpoints.
"""

import tempfile
from typing import Any, Optional

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def storage():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


@pytest.fixture
def mcp_server(storage):
    return create_mcp_server(storage=storage)


@pytest.fixture
def http_client(storage):
    return TestClient(create_app(storage=storage))


def _model(name: str, data_source: str) -> SlayerModel:
    return SlayerModel(
        name=name,
        sql_table=name,
        data_source=data_source,
        columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True)],
    )


def _ds(name: str) -> DatasourceConfig:
    return DatasourceConfig(name=name, type="postgres", host="h")


async def _call_mcp(server, *, name: str, arguments: Optional[dict[str, Any]] = None) -> str:
    """Invoke an MCP tool and return its text result."""
    blocks, _ = await server.call_tool(name=name, arguments=arguments or {})
    return blocks[0].text


# ---------------------------------------------------------------------------
# MCP — set_datasource_priority / get_datasource_priority
# ---------------------------------------------------------------------------


class TestMCPDatasourcePriority:
    async def test_set_and_get_roundtrip(self, mcp_server, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))

        result = await _call_mcp(
            mcp_server,
            name="set_datasource_priority",
            arguments={"priority": ["db_b", "db_a"]},
        )
        # Tool reports success in some readable form.
        assert "db_b" in result
        assert "db_a" in result

        # Reflected in storage and via the read tool.
        assert await storage.get_datasource_priority() == ["db_b", "db_a"]
        read = await _call_mcp(mcp_server, name="get_datasource_priority", arguments={})
        assert "db_b" in read

    async def test_set_rejects_unknown_datasource(self, mcp_server, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        result = await _call_mcp(
            mcp_server,
            name="set_datasource_priority",
            arguments={"priority": ["db_a", "nope"]},
        )
        # MCP tools return error text in the response rather than raising;
        # message names the offender + lists known datasources.
        assert "nope" in result
        assert "db_a" in result


# ---------------------------------------------------------------------------
# MCP — edit_model / inspect_model / delete_model accept data_source
# ---------------------------------------------------------------------------


class TestMCPModelToolsDataSourceArg:
    async def test_edit_model_with_data_source_targets_only_that_one(self, mcp_server, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        await _call_mcp(
            mcp_server,
            name="edit_model",
            arguments={
                "model_name": "users",
                "data_source": "db_a",
                "description": "edited a",
            },
        )

        users_a = await storage.get_model("users", data_source="db_a")
        users_b = await storage.get_model("users", data_source="db_b")
        assert users_a is not None and users_a.description == "edited a"
        # db_b's model is untouched.
        assert users_b is not None and users_b.description != "edited a"

    async def test_edit_model_ambiguous_without_data_source_returns_error(self, mcp_server, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        result = await _call_mcp(
            mcp_server,
            name="edit_model",
            arguments={"model_name": "users", "description": "edited"},
        )
        # Error surfaces both remediations + lists the candidates.
        assert "data_source" in result
        assert "set_datasource_priority" in result
        assert "db_a" in result and "db_b" in result

    async def test_inspect_model_with_data_source_filters(self, mcp_server, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        result = await _call_mcp(
            mcp_server,
            name="inspect_model",
            arguments={"model_name": "users", "data_source": "db_a"},
        )
        # Output identifies the datasource so the agent can verify it picked
        # the right one.
        assert "db_a" in result

    async def test_inspect_model_helper_chain_scoped_to_data_source(
        self, mcp_server, storage, monkeypatch
    ) -> None:
        """The helpers backing ``inspect_model`` (``_get_row_count``,
        ``_collect_dim_profile``, ``_collect_measure_profile``,
        ``_collect_reachable_fields``, the sample-data query) all run
        ``engine.execute`` against the model. After v4 each of those calls
        must forward ``data_source=model.data_source`` so the engine's
        bare-name resolution doesn't pick the sibling in another datasource.
        See PR #92 thread #7.
        """
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        # Capture every engine.execute call made by the inspect_model helpers.
        from slayer.engine import query_engine as qe_mod

        captured: list[dict[str, Any]] = []
        original_execute = qe_mod.SlayerQueryEngine.execute

        async def _capturing_execute(self, query, **kwargs):  # type: ignore[no-untyped-def]
            captured.append({
                "source_model": getattr(query, "source_model", None) if not isinstance(query, str) else query,
                "data_source_kwarg": kwargs.get("data_source"),
            })
            # Don't actually run — return an empty response shape so the
            # helpers' ``except Exception: return None`` paths don't muddy
            # the captured list.
            return await original_execute(self, query, **kwargs)

        monkeypatch.setattr(qe_mod.SlayerQueryEngine, "execute", _capturing_execute)

        await _call_mcp(
            mcp_server,
            name="inspect_model",
            arguments={"model_name": "users", "data_source": "db_a"},
        )

        # Helpers should have called engine.execute at least once on the
        # ``users`` model, and *every* such call must carry data_source="db_a".
        users_calls = [c for c in captured if c["source_model"] == "users"]
        assert users_calls, (
            "expected inspect_model helpers to run at least one ``users`` "
            f"query; got: {captured!r}"
        )
        bad = [c for c in users_calls if c["data_source_kwarg"] != "db_a"]
        assert not bad, (
            "inspect_model helper(s) called engine.execute on 'users' "
            f"without data_source='db_a': {bad!r}"
        )

    async def test_delete_model_with_data_source(self, mcp_server, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        await _call_mcp(
            mcp_server,
            name="delete_model",
            arguments={"name": "users", "data_source": "db_a"},
        )
        assert await storage.get_model("users", data_source="db_a") is None
        assert await storage.get_model("users", data_source="db_b") is not None


# ---------------------------------------------------------------------------
# HTTP API — model endpoints + datasource priority
# ---------------------------------------------------------------------------


class TestAPINamespacedModels:
    async def test_get_model_with_data_source_query_param(self, http_client, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        resp = http_client.get("/models/users", params={"data_source": "db_a"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["name"] == "users"
        assert body["data_source"] == "db_a"

    async def test_get_model_ambiguous_returns_409(self, http_client, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        resp = http_client.get("/models/users")
        assert resp.status_code == 409
        detail = resp.json()["detail"]
        # Body cites both candidates.
        assert "db_a" in detail and "db_b" in detail

    async def test_delete_model_with_data_source_query_param(self, http_client, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        resp = http_client.delete("/models/users", params={"data_source": "db_a"})
        assert resp.status_code == 200
        # Only db_a's copy is gone.
        assert await storage.get_model("users", data_source="db_a") is None
        assert await storage.get_model("users", data_source="db_b") is not None

    async def test_list_models_filterable_by_datasource(self, http_client, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("orders", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        resp = http_client.get("/models", params={"data_source": "db_a"})
        assert resp.status_code == 200
        names = sorted(m["name"] for m in resp.json())
        assert names == ["orders", "users"]


class TestAPIDatasourcePriority:
    def test_default_priority_is_empty(self, http_client) -> None:
        resp = http_client.get("/datasources/priority")
        assert resp.status_code == 200
        assert resp.json() == {"priority": []}

    async def test_put_priority_persists(self, http_client, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))

        resp = http_client.put(
            "/datasources/priority", json={"priority": ["db_b", "db_a"]}
        )
        assert resp.status_code == 200

        resp = http_client.get("/datasources/priority")
        assert resp.json() == {"priority": ["db_b", "db_a"]}

    async def test_put_priority_validates(self, http_client, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        resp = http_client.put(
            "/datasources/priority", json={"priority": ["db_a", "nope"]}
        )
        assert resp.status_code == 400
        assert "nope" in resp.json()["detail"]

    def test_put_priority_rejects_string_for_priority(self, http_client) -> None:
        """``{"priority": "db_a"}`` must be a request-validation error
        (HTTP 422), not a misleading 400 from coercing the string into
        the character list ``["d", "b", "_", "a"]``. See PR #92 thread #2.
        """
        resp = http_client.put(
            "/datasources/priority", json={"priority": "db_a"}
        )
        assert resp.status_code == 422

    def test_put_priority_rejects_non_string_items(self, http_client) -> None:
        resp = http_client.put(
            "/datasources/priority", json={"priority": ["db_a", 7]}
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Python client
# ---------------------------------------------------------------------------


class TestClientNamespacing:
    async def test_get_model_with_data_source(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        await storage.save_model(_model("users", data_source="db_a"))
        await storage.save_model(_model("users", data_source="db_b"))

        client = SlayerClient(storage=storage)
        m = await client.get_model("users", data_source="db_a")
        assert m is not None
        assert m.data_source == "db_a"

    async def test_set_datasource_priority(self, storage) -> None:
        await storage.save_datasource(_ds("db_a"))
        await storage.save_datasource(_ds("db_b"))
        client = SlayerClient(storage=storage)

        await client.set_datasource_priority(["db_b", "db_a"])
        assert await storage.get_datasource_priority() == ["db_b", "db_a"]
