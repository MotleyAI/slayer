"""Tests for SlayerClient — local-mode dispatch + HTTP-mode body shapes.

Covers DEV-1437: the client mirrors the engine's full input union
``SlayerQuery | dict | list[SlayerQuery | dict] | str`` on every public
query entry point. Local-mode tests are contract smokes (engine already
accepts every shape, so local-mode dispatch was already working by
accident); HTTP-mode tests pin the bug — ``query.model_dump`` blowing up
on list/str inputs is what the original report describes.
"""

import tempfile
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple

import pytest

from slayer.client.slayer_client import SlayerClient
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.storage.yaml_storage import YAMLStorage


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
_CANNED_RESP: Dict[str, Any] = {
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
        self.calls: List[Dict[str, Any]] = []

    def _replace_sync(
        self,
        *,
        method: str,
        path: str,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
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
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return self._replace_sync(
            method=method, path=path, json=json, params=params
        )

    @property
    def last_body(self) -> Optional[Dict[str, Any]]:
        if not self.calls:
            return None
        return self.calls[-1]["json"]


@pytest.fixture
def http_client_with_capture(
    monkeypatch: pytest.MonkeyPatch,
) -> Tuple[SlayerClient, _CapturedRequests]:
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        """Mixed list items (``SlayerQuery`` + dict) each serialise to dict."""
        client, cap = http_client_with_capture
        q = SlayerQuery(
            name="a",
            source_model="orders",
            measures=[{"formula": "amount:sum"}],
        )
        items: List[Any] = [q, {"source_model": "a"}]
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        client.query_sync("rev_by_region")
        assert cap.last_body == {"name": "rev_by_region"}

    # --- dict ---------------------------------------------------------- #

    def test_dict_body_shape(
        self,
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        items: List[Dict[str, Any]] = [
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        with pytest.raises(TypeError, match="SlayerQuery"):
            client.query_sync(42)  # type: ignore[arg-type]
        assert cap.last_body is None

    def test_rejects_invalid_list_item(
        self,
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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

    # --- pass-through of variables (DEV-1438 ergonomics live in body) -- #

    def test_dict_with_variables_preserved(
        self,
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        """A dict input carrying top-level ``variables`` reaches the server
        verbatim. DEV-1437 doesn't add a ``variables=`` kwarg (see DEV-1438)
        but the dict-passthrough must not drop or mutate the key.
        """
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        sql = client.sql_sync("rev_by_region")
        assert sql == "SELECT 1"
        assert cap.last_body == {"name": "rev_by_region", "dry_run": True}

    def test_explain_sync_list_input(
        self,
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
            json: Optional[Dict[str, Any]] = None,
            params: Optional[Dict[str, Any]] = None,
        ) -> Dict[str, Any]:
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
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
        http_client_with_capture: Tuple[SlayerClient, _CapturedRequests],
    ) -> None:
        client, cap = http_client_with_capture
        await client.query("rev_by_region")
        assert cap.last_body == {"name": "rev_by_region"}
