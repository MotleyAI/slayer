"""Tests for SlayerClient — local-mode dispatch + HTTP-mode body shapes.

Covers DEV-1437: the client mirrors the engine's full input union
``SlayerQuery | dict | list[SlayerQuery | dict] | str`` on every public
query entry point. Local-mode tests are contract smokes (engine already
accepts every shape, so local-mode dispatch was already working by
accident); HTTP-mode tests pin the bug — ``query.model_dump`` blowing up
on list/str inputs is what the original report describes.
"""

import asyncio
import tempfile
from types import MappingProxyType
from typing import Any
from collections.abc import Mapping

import pytest
from fastapi.testclient import TestClient

from slayer.api.server import create_app
from slayer.client.slayer_client import SlayerClient
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.storage.yaml_storage import YAMLStorage
from tests._saved_query_refinement_fixtures import BY_REGION_MONTH, build_refine_storage, month


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture
def storage() -> YAMLStorage:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


@pytest.fixture
def client(storage: YAMLStorage) -> SlayerClient:
    return SlayerClient(storage=storage)


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        sql_table="orders_t",
        data_source="ds",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="region", sql="region", type=DataType.TEXT),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
        ],
    )


def _ds() -> DatasourceConfig:
    return DatasourceConfig(name="ds", type="sqlite", database=":memory:")


async def _save_orders(storage: YAMLStorage) -> None:
    await storage.save_datasource(_ds())
    await storage.save_model(_orders_model())


# Minimal response payload that ``SlayerClient._parse_response`` consumes
# without complaining. Used by the HTTP-mode mocks.
_CANNED_RESP: dict[str, Any] = {
    "data": [],
    "columns": [],
    "sql": "SELECT 1",
    "attributes": {"dimensions": {}, "measures": {}},
}


class _CapturedRequests:
    """Captures the JSON body of each ``_request_sync`` / ``_request`` call.

    Two stubs (sync + async) replace the underlying httpx-wrapping methods,
    so the HTTP-mode body-shape tests can introspect what would have been
    posted without spinning up an HTTP server.
    """

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def _replace_sync(
        self,
        *,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            {"method": method, "path": path, "json": json, "params": params}
        )
        # ``_parse_response`` reads keys; return a fresh dict so callers
        # cannot mutate the canned response across tests.
        return dict(_CANNED_RESP)

    async def _replace_async(  # NOSONAR(S7503) — must be async def to match the awaited contract of self._request; body has no IO to await
        self,
        *,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._replace_sync(
            method=method, path=path, json=json, params=params
        )

    @property
    def last_body(self) -> dict[str, Any] | None:
        if not self.calls:
            return None
        return self.calls[-1]["json"]


@pytest.fixture
def http_client_with_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[SlayerClient, _CapturedRequests]:
    """A remote-mode ``SlayerClient`` whose request-wrappers are stubbed.

    Returns ``(client, capture)`` where ``capture.last_body`` exposes the
    JSON body of the most recent client→transport call.
    """
    client = SlayerClient(url="http://localhost:5143")
    assert client._engine is None  # remote mode — no storage attached
    capture = _CapturedRequests()
    monkeypatch.setattr(client, "_request_sync", capture._replace_sync)
    monkeypatch.setattr(client, "_request", capture._replace_async)
    return client, capture


# --------------------------------------------------------------------------- #
# Local-mode tests
# --------------------------------------------------------------------------- #


class TestLocalMode:
    def test_init_local(self, storage: YAMLStorage) -> None:
        client = SlayerClient(storage=storage)
        assert client._engine is not None

    def test_init_remote(self) -> None:
        client = SlayerClient(url="http://localhost:5143")
        assert client._engine is None

    async def test_query_dispatches_locally(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        """Local-mode ``query_sync`` reaches the engine (not HTTP)."""
        await _save_orders(storage)
        query = SlayerQuery(
            source_model="orders", measures=[{"formula": "amount:sum"}]
        )
        resp = client.query_sync(query, dry_run=True)
        assert resp.sql is not None
        assert "amount" in resp.sql.lower()

    async def test_query_accepts_dict(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        """``query_sync`` accepts a plain dict (regression)."""
        await _save_orders(storage)
        query_dict = {
            "source_model": "orders",
            "measures": [{"formula": "amount:sum"}],
        }
        resp = client.query_sync(query_dict, dry_run=True)
        assert resp.sql is not None
        assert "amount" in resp.sql.lower()

    async def test_sql_accepts_dict(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        """``sql_sync`` accepts a plain dict (regression)."""
        await _save_orders(storage)
        query_dict = {
            "source_model": "orders",
            "measures": [{"formula": "amount:sum"}],
        }
        sql = client.sql_sync(query_dict)
        assert isinstance(sql, str)
        assert "SELECT" in sql.upper()

    async def test_query_sync_accepts_list_local_smoke(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        """List-of-dicts (multi-stage DAG) reaches the engine in local mode."""
        await _save_orders(storage)
        queries = [
            {
                "name": "by_customer",
                "source_model": "orders",
                "measures": [{"formula": "amount:sum"}],
                "dimensions": [{"name": "customer_id"}],
            },
            {
                "source_model": "by_customer",
                "measures": [{"formula": "amount_sum:avg"}],
            },
        ]
        resp = client.query_sync(queries, dry_run=True)
        assert resp.sql is not None
        assert "avg(" in resp.sql.lower()

    async def test_query_sync_accepts_str_local_smoke(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        """``str`` input runs the backing query of a query-backed model."""
        await _save_orders(storage)
        saved = SlayerModel(
            name="rev_by_region",
            data_source="ds",
            source_queries=[
                SlayerQuery(
                    source_model="orders",
                    measures=[{"formula": "amount:sum"}],
                    dimensions=["region"],
                )
            ],
        )
        await storage.save_model(saved)
        resp = client.query_sync("rev_by_region", dry_run=True)
        assert resp.sql is not None
        assert "amount" in resp.sql.lower()
        assert "region" in resp.sql.lower()

    async def test_query_sync_accepts_tuple_local_mode(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        """``tuple`` input reaches the engine in local mode — the client
        normalises Sequence → list before forwarding so the engine's
        ``isinstance(query, list)`` dispatch matches."""
        await _save_orders(storage)
        queries = (
            {
                "name": "by_customer",
                "source_model": "orders",
                "measures": [{"formula": "amount:sum"}],
                "dimensions": [{"name": "customer_id"}],
            },
            {
                "source_model": "by_customer",
                "measures": [{"formula": "amount_sum:avg"}],
            },
        )
        resp = client.query_sync(queries, dry_run=True)
        assert resp.sql is not None
        assert "avg(" in resp.sql.lower()

    async def test_query_sync_accepts_mappingproxy_local_mode(
        self, client: SlayerClient, storage: YAMLStorage
    ) -> None:
        """``MappingProxyType`` input reaches the engine in local mode
        — the client normalises Mapping → dict before forwarding."""
        await _save_orders(storage)
        payload: Mapping[str, Any] = MappingProxyType(
            {
                "source_model": "orders",
                "measures": [{"formula": "amount:sum"}],
            }
        )
        resp = client.query_sync(payload, dry_run=True)
        assert resp.sql is not None
        assert "amount" in resp.sql.lower()


# --------------------------------------------------------------------------- #
# HTTP-mode body-shape tests (pins the DEV-1437 bug)
# --------------------------------------------------------------------------- #


class TestHttpBodyShape:
    """Verify ``query_sync`` / ``query`` post the right JSON body shape for
    each accepted input form. Uses monkeypatched ``_request_sync`` /
    ``_request`` (no live HTTP server)."""

    # --- list ---------------------------------------------------------- #

    def test_list_body_shape(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        queries = [
            {
                "name": "a",
                "source_model": "orders",
                "measures": [{"formula": "amount:sum"}],
            },
            {"source_model": "a"},
        ]
        resp = client.query_sync(queries)
        # Canned response wires through to a SlayerResponse.
        assert resp.sql == "SELECT 1"
        body = cap.last_body
        assert body is not None
        expected = {
            "queries": [
                SlayerQuery.model_validate(q).model_dump(
                    mode="json", exclude_none=True
                )
                for q in queries
            ]
        }
        assert body == expected

    def test_list_with_slayerquery_items(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        """Mixed list items (``SlayerQuery`` + dict) each serialise to dict."""
        client, cap = http_client_with_capture
        q = SlayerQuery(
            name="a",
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
        )
        items: list[Any] = [q, {"source_model": "a"}]
        client.query_sync(items)
        body = cap.last_body
        assert body is not None
        assert "queries" in body
        # SlayerQuery is serialised in JSON mode.
        assert body["queries"][0] == q.model_dump(
            mode="json", exclude_none=True
        )
        # Dict items are normalised through SlayerQuery (string-shorthand
        # measures / dimensions become dict-form; defaults like ``version``
        # surface).
        assert body["queries"][1] == SlayerQuery.model_validate(
            {"source_model": "a"}
        ).model_dump(mode="json", exclude_none=True)

    # --- str ----------------------------------------------------------- #

    def test_str_body_shape(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        client.query_sync("rev_by_region")
        assert cap.last_body == {"name": "rev_by_region"}

    # --- dict ---------------------------------------------------------- #

    def test_dict_body_shape(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        """Dict input is round-tripped through ``SlayerQuery`` so the
        server sees the JSON-mode dump (string-shorthand normalised,
        defaults included). FastAPI's ``QueryRequest`` declares strict
        list-of-dict types for measures/dimensions; the round-trip is
        the only way string-shorthand input doesn't 422 server-side.
        """
        client, cap = http_client_with_capture
        payload = {
            "source_model": "orders",
            "measures": [{"formula": "amount:sum"}],
        }
        client.query_sync(payload)
        body = cap.last_body
        assert body == SlayerQuery.model_validate(payload).model_dump(
            mode="json", exclude_none=True
        )
        # Helper doesn't mutate the caller. See
        # ``test_does_not_mutate_caller_dict``.
        assert body is not payload

    def test_dict_normalizes_string_shorthand(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        """String-shorthand measures (e.g. ``"amount:sum"``) become
        dict-form (``{"formula": "amount:sum"}``) before reaching the
        server — otherwise FastAPI's ``QueryRequest`` rejects the body
        with HTTP 422.
        """
        client, cap = http_client_with_capture
        payload = {"source_model": "orders", "measures": ["amount:sum"]}
        client.query_sync(payload)
        body = cap.last_body
        assert body is not None
        # Each measure landed as a dict, not a bare string.
        for m in body["measures"]:
            assert isinstance(m, dict)
            assert m.get("formula") == "amount:sum"

    # --- SlayerQuery --------------------------------------------------- #

    def test_slayerquery_body_shape(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        q = SlayerQuery(
            source_model="orders", measures=[{"formula": "amount:sum"}]
        )
        client.query_sync(q)
        # JSON mode — see codex note (4); matches _coerce_linked_entities.
        assert cap.last_body == q.model_dump(mode="json", exclude_none=True)

    # --- dry_run / explain --------------------------------------------- #

    def test_dry_run_explain_appended_list(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        items = [
            {
                "name": "a",
                "source_model": "orders",
                "measures": [{"formula": "amount:sum"}],
            },
            {"source_model": "a"},
        ]
        client.query_sync(items, dry_run=True, explain=True)
        body = cap.last_body
        assert body is not None
        assert body["dry_run"] is True
        assert body["explain"] is True
        assert "queries" in body

    def test_dry_run_explain_appended_str(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        client.query_sync("m", dry_run=True, explain=True)
        assert cap.last_body == {
            "name": "m",
            "dry_run": True,
            "explain": True,
        }

    def test_dry_run_explain_appended_dict(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        client.query_sync(
            {"source_model": "orders"}, dry_run=True, explain=True
        )
        body = cap.last_body
        assert body is not None
        assert body["source_model"] == "orders"
        assert body["dry_run"] is True
        assert body["explain"] is True

    def test_flags_omitted_when_false(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        """``dry_run=False`` / ``explain=False`` (defaults) → keys not present."""
        client, cap = http_client_with_capture
        client.query_sync({"source_model": "orders"})
        body = cap.last_body
        assert body is not None
        assert "dry_run" not in body
        assert "explain" not in body

    # --- mutation safety ----------------------------------------------- #

    def test_does_not_mutate_caller_dict(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, _cap = http_client_with_capture
        payload = {
            "source_model": "orders",
            "measures": [{"formula": "amount:sum"}],
        }
        snapshot = dict(payload)
        client.query_sync(payload, dry_run=True, explain=True)
        assert payload == snapshot
        assert "dry_run" not in payload
        assert "explain" not in payload

    def test_does_not_mutate_caller_list(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        items: list[dict[str, Any]] = [
            {"name": "a", "source_model": "orders"},
            {"source_model": "a"},
        ]
        snapshot = [dict(item) for item in items]
        client.query_sync(items, dry_run=True)
        # Caller's list and items are unchanged.
        assert items == snapshot
        # And body items are independent dicts (helper shallow-copies),
        # so future mutations of caller items don't leak into the body.
        body = cap.last_body
        assert body is not None
        assert body["queries"] is not items
        assert body["queries"][0] is not items[0]
        assert body["queries"][1] is not items[1]

    # --- invalid input ------------------------------------------------- #

    def test_rejects_invalid_input_top_level(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        with pytest.raises(TypeError, match="SlayerQuery"):
            client.query_sync(42)  # type: ignore[arg-type]
        assert cap.last_body is None

    def test_rejects_invalid_list_item(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        with pytest.raises(TypeError, match=r"query\[1\]"):
            client.query_sync(
                [{"source_model": "orders"}, 42]  # type: ignore[list-item]
            )
        assert cap.last_body is None

    # --- non-dict/list runtime shapes (Mapping / Sequence) ------------- #

    def test_mappingproxy_body_shape(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        """``MappingProxyType`` is a ``Mapping`` but not a ``dict`` — the
        helper must honour the declared ``Mapping[str, Any]`` contract
        AND route through the SlayerQuery normalisation pipeline."""
        client, cap = http_client_with_capture
        payload: Mapping[str, Any] = MappingProxyType(
            {"source_model": "orders", "measures": [{"formula": "amount:sum"}]}
        )
        client.query_sync(payload)
        assert cap.last_body == SlayerQuery.model_validate(
            dict(payload)
        ).model_dump(mode="json", exclude_none=True)

    def test_tuple_body_shape(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        """``tuple`` is a ``Sequence`` but not a ``list`` — honoured."""
        client, cap = http_client_with_capture
        items = (
            {"name": "a", "source_model": "orders"},
            {"source_model": "a"},
        )
        client.query_sync(items)
        body = cap.last_body
        assert body is not None
        expected = [
            SlayerQuery.model_validate(it).model_dump(
                mode="json", exclude_none=True
            )
            for it in items
        ]
        assert body["queries"] == expected

    # --- pass-through of variables ------------------------------------ #

    def test_dict_with_variables_preserved(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        """A dict input's own ``variables`` reach the server verbatim."""
        client, cap = http_client_with_capture
        payload = {
            "source_model": "orders",
            "measures": [{"formula": "amount:sum"}],
            "variables": {"region": "US"},
        }
        client.query_sync(payload)
        body = cap.last_body
        assert body is not None
        assert body.get("variables") == {"region": "US"}

    # --- delegating helpers inherit list / str support ----------------- #

    def test_sql_sync_list_input(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        """``sql_sync`` delegates to ``query_sync(dry_run=True)`` — list
        input must reach the transport with ``dry_run`` set."""
        client, cap = http_client_with_capture
        queries = [
            {"name": "a", "source_model": "orders"},
            {"source_model": "a"},
        ]
        sql = client.sql_sync(queries)
        assert sql == "SELECT 1"
        body = cap.last_body
        assert body is not None
        expected = [
            SlayerQuery.model_validate(q).model_dump(
                mode="json", exclude_none=True
            )
            for q in queries
        ]
        assert body.get("queries") == expected
        assert body.get("dry_run") is True

    def test_sql_sync_str_input(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        sql = client.sql_sync("rev_by_region")
        assert sql == "SELECT 1"
        assert cap.last_body == {"name": "rev_by_region", "dry_run": True}

    def test_explain_sync_list_input(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        """``explain_sync`` delegates to ``query_sync(explain=True)`` — list
        input must reach the transport with ``explain`` set."""
        client, cap = http_client_with_capture
        queries = [
            {"name": "a", "source_model": "orders"},
            {"source_model": "a"},
        ]
        resp = client.explain_sync(queries)
        assert resp.sql == "SELECT 1"
        body = cap.last_body
        assert body is not None
        expected = [
            SlayerQuery.model_validate(q).model_dump(
                mode="json", exclude_none=True
            )
            for q in queries
        ]
        assert body.get("queries") == expected
        assert body.get("explain") is True

    def test_explain_sync_str_input(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        client.explain_sync("rev_by_region")
        assert cap.last_body == {"name": "rev_by_region", "explain": True}

    def test_query_df_accepts_list(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``query_df`` delegates to ``query_sync`` — list input must work,
        not be re-tightened by a narrower type hint. Issue notes: 'worth a
        one-line test so nobody re-tightens this later.'"""
        pd = pytest.importorskip("pandas")
        client = SlayerClient(url="http://localhost:5143")
        capture = _CapturedRequests()
        # Wire a richer canned response so DataFrame has at least one row.
        def _replace_sync_with_rows(
            *,
            method: str,
            path: str,
            json: dict[str, Any] | None = None,
            params: dict[str, Any] | None = None,
        ) -> dict[str, Any]:
            capture.calls.append(
                {"method": method, "path": path, "json": json, "params": params}
            )
            return {
                "data": [{"x": 1}, {"x": 2}],
                "columns": ["x"],
                "sql": "SELECT 1",
                "attributes": {"dimensions": {}, "measures": {}},
            }
        monkeypatch.setattr(client, "_request_sync", _replace_sync_with_rows)
        queries = [
            {"name": "a", "source_model": "orders"},
            {"source_model": "a"},
        ]
        df = client.query_df(queries)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        # And the list shape did reach the transport (normalised through
        # SlayerQuery — see ``test_dict_normalizes_string_shorthand``).
        body = capture.last_body
        assert body is not None
        expected = [
            SlayerQuery.model_validate(q).model_dump(
                mode="json", exclude_none=True
            )
            for q in queries
        ]
        assert body.get("queries") == expected

    # --- async mirror -------------------------------------------------- #

    async def test_async_query_list_body_shape(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        """Async ``query`` mirrors sync ``query_sync`` body shape."""
        client, cap = http_client_with_capture
        queries = [
            {"name": "a", "source_model": "orders"},
            {"source_model": "a"},
        ]
        await client.query(queries)
        expected = {
            "queries": [
                SlayerQuery.model_validate(q).model_dump(
                    mode="json", exclude_none=True
                )
                for q in queries
            ]
        }
        assert cap.last_body == expected

    async def test_async_query_str_body_shape(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        await client.query("rev_by_region")
        assert cap.last_body == {"name": "rev_by_region"}


# --------------------------------------------------------------------------- #
# Saved-query refinement
# --------------------------------------------------------------------------- #

REFINE = {"dimensions": ["region"]}
ONLY_BY_NAME = "refine applies only to a saved query run by name"


class TestRefine:
    def test_http_body_shape(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        client.query_sync("monthly_revenue", refine=REFINE)
        assert cap.last_body is not None
        assert set(cap.last_body) == {"name", "refine"}
        assert cap.last_body["name"] == "monthly_revenue"
        assert cap.last_body["refine"]["dimensions"] in (["region"], [{"name": "region"}])

    def test_http_body_keeps_explicit_null(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        client.query_sync("monthly_revenue", refine={"limit": None})
        assert cap.last_body == {"name": "monthly_revenue", "refine": {"limit": None}}

    async def test_http_async_and_helpers_forward_refine(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        await client.query("m", refine={"limit": 1})
        assert cap.last_body == {"name": "m", "refine": {"limit": 1}}
        await client.sql("m", refine={"limit": 1})
        assert cap.last_body == {"name": "m", "refine": {"limit": 1}, "dry_run": True}
        await client.explain("m", refine={"limit": 1})
        assert cap.last_body == {"name": "m", "refine": {"limit": 1}, "explain": True}
        client.sql_sync("m", refine={"limit": 1})
        assert cap.last_body == {"name": "m", "refine": {"limit": 1}, "dry_run": True}
        client.explain_sync("m", refine={"limit": 1})
        assert cap.last_body == {"name": "m", "refine": {"limit": 1}, "explain": True}

    @pytest.mark.parametrize("query", [
        {"source_model": "orders"},
        SlayerQuery(source_model="orders"),
        [{"name": "a", "source_model": "orders"}, {"source_model": "a"}],
    ], ids=["dict", "query", "list"])
    def test_http_rejects_non_name(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
        query: Any,
    ) -> None:
        client, cap = http_client_with_capture
        with pytest.raises(ValueError, match=ONLY_BY_NAME):
            client.query_sync(query, refine=REFINE)
        assert cap.last_body is None

    async def test_local_forwards_to_engine(self, tmp_path) -> None:
        client = SlayerClient(storage=await build_refine_storage(str(tmp_path)))
        resp = await client.query("monthly_revenue", refine=REFINE)
        assert _by_region(resp.data) == BY_REGION_MONTH

    def test_local_sync_and_dataframe(self, tmp_path) -> None:
        pd = pytest.importorskip("pandas")
        client = SlayerClient(storage=asyncio.run(build_refine_storage(str(tmp_path))))
        assert _by_region(client.query_sync("monthly_revenue", refine=REFINE).data) == BY_REGION_MONTH
        df = client.query_df("monthly_revenue", refine=REFINE)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3

    async def test_local_rejects_non_name(self, tmp_path) -> None:
        client = SlayerClient(storage=await build_refine_storage(str(tmp_path)))
        with pytest.raises(ValueError, match=ONLY_BY_NAME):
            await client.query({"source_model": "orders", "measures": ["count(*)"]}, refine=REFINE)

    def test_over_http(self, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
        storage = asyncio.run(build_refine_storage(str(tmp_path)))
        server = TestClient(create_app(storage=storage))

        def via_app(*, method: str, path: str, json: dict | None = None, params: dict | None = None) -> Any:
            resp = server.request(method, path, json=json, params=params)
            resp.raise_for_status()
            return resp.json()

        client = SlayerClient(url="http://testserver")
        monkeypatch.setattr(client, "_request_sync", via_app)
        assert _by_region(client.query_sync("monthly_revenue", refine=REFINE).data) == BY_REGION_MONTH


class TestVariables:
    def test_http_body_by_name(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        client.query_sync("m", refine={"limit": 1}, variables={"status": "paid"})
        assert cap.last_body == {"name": "m", "refine": {"limit": 1}, "variables": {"status": "paid"}}

    def test_http_body_list(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        client.query_sync([{"name": "a", "source_model": "orders"}, {"source_model": "a"}], variables={"x": 1})
        assert cap.last_body is not None
        assert cap.last_body["variables"] == {"x": 1}

    @pytest.mark.parametrize("query", [
        {"source_model": "orders", "measures": ["count(*)"], "variables": {"a": "own", "b": "own"}},
        SlayerQuery.model_validate(
            {"source_model": "orders", "measures": ["count(*)"], "variables": {"a": "own", "b": "own"}}
        ),
    ], ids=["dict", "query"])
    def test_http_single_query_runtime_wins(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
        query: Any,
    ) -> None:
        client, cap = http_client_with_capture
        client.query_sync(query, variables={"b": "runtime"})
        assert cap.last_body is not None
        assert cap.last_body["variables"] == {"a": "own", "b": "runtime"}

    def test_http_does_not_mutate_caller(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, _ = http_client_with_capture
        query = {"source_model": "orders", "measures": ["count(*)"], "variables": {"a": "own"}}
        runtime = {"a": "runtime"}
        client.query_sync(query, variables=runtime)
        assert query["variables"] == {"a": "own"}
        assert runtime == {"a": "runtime"}

    def test_http_empty_variables_omitted(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        client.query_sync("m", variables={})
        assert cap.last_body == {"name": "m"}

    async def test_http_helpers_forward_variables(
        self,
        http_client_with_capture: tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        v = {"status": "paid"}
        await client.query("m", variables=v)
        assert cap.last_body == {"name": "m", "variables": v}
        await client.sql("m", variables=v)
        assert cap.last_body == {"name": "m", "variables": v, "dry_run": True}
        await client.explain("m", variables=v)
        assert cap.last_body == {"name": "m", "variables": v, "explain": True}
        client.sql_sync("m", variables=v)
        assert cap.last_body == {"name": "m", "variables": v, "dry_run": True}
        client.explain_sync("m", variables=v)
        assert cap.last_body == {"name": "m", "variables": v, "explain": True}

    async def test_local_refinement_with_runtime_variables(self, tmp_path) -> None:
        client = SlayerClient(storage=await build_refine_storage(str(tmp_path)))
        resp = await client.query(
            "revenue_by_status", refine={"measures": ["count(*)"]}, variables={"status": "refunded"},
        )
        assert resp.data == [{"orders.region": "US", "orders.revenue": 40.0, "orders._count": 1}]

    async def test_local_refinement_placeholder(self, tmp_path) -> None:
        client = SlayerClient(storage=await build_refine_storage(str(tmp_path)))
        resp = await client.query(
            "monthly_revenue", refine={"filters": ["region = '{region}'"]}, variables={"region": "EU"},
        )
        assert [(month(r["orders.ordered_at"]), r["orders.revenue"]) for r in resp.data] == [("2025-02", 105.0)]

    async def test_local_single_query(self, tmp_path) -> None:
        client = SlayerClient(storage=await build_refine_storage(str(tmp_path)))
        query = {"source_model": "orders", "measures": ["count(*)"], "filters": ["status = '{status}'"]}
        resp = await client.query(query, variables={"status": "refunded"})
        assert resp.data == [{"orders._count": 1}]

    def test_local_sync_and_dataframe(self, tmp_path) -> None:
        pd = pytest.importorskip("pandas")
        client = SlayerClient(storage=asyncio.run(build_refine_storage(str(tmp_path))))
        resp = client.query_sync("revenue_by_status", variables={"status": "refunded"})
        assert resp.data == [{"orders.region": "US", "orders.revenue": 40.0}]
        df = client.query_df("revenue_by_status", variables={"status": "refunded"})
        assert isinstance(df, pd.DataFrame)
        assert df.to_dict("records") == [{"orders.region": "US", "orders.revenue": 40.0}]

    def test_over_http(self, tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
        storage = asyncio.run(build_refine_storage(str(tmp_path)))
        server = TestClient(create_app(storage=storage))

        def via_app(*, method: str, path: str, json: dict | None = None, params: dict | None = None) -> Any:
            resp = server.request(method, path, json=json, params=params)
            resp.raise_for_status()
            return resp.json()

        client = SlayerClient(url="http://testserver")
        monkeypatch.setattr(client, "_request_sync", via_app)
        resp = client.query_sync(
            "revenue_by_status", refine={"measures": ["count(*)"]}, variables={"status": "refunded"},
        )
        assert resp.data == [{"orders.region": "US", "orders.revenue": 40.0, "orders._count": 1}]
        single = {"source_model": "orders", "measures": ["count(*)"], "filters": ["status = '{status}'"],
                  "variables": {"status": "paid"}}
        assert client.query_sync(single, variables={"status": "refunded"}).data == [{"orders._count": 1}]


def _by_region(rows: list[dict[str, Any]]) -> dict[tuple, Any]:
    return {(r["orders.region"], month(r["orders.ordered_at"])): r["orders.revenue"] for r in rows}
