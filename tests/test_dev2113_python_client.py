"""Python ``SlayerClient`` behaviour added for TS parity: auth, timeouts, local-mode refusals, new operations."""

import base64
import json
import tempfile
from collections.abc import Iterator

import httpx
import pytest

from slayer.client import sign_gateway_token
from slayer.client.slayer_client import SlayerClient
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.storage.yaml_storage import YAMLStorage
from tests._httpx_mock import HttpxRecorder, install, json_response

URL = "http://slayer.test"
QUERY = {"source_model": "orders", "measures": ["revenue"]}
QUERY_RESPONSE = {
    "data": [], "row_count": 0, "columns": [], "sql": "SELECT 1",
    "attributes": {"dimensions": {}, "measures": {}}, "population": None,
    "population_inferred": False, "warnings": [],
}


@pytest.fixture
def storage() -> Iterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


@pytest.fixture
def ok(monkeypatch: pytest.MonkeyPatch) -> HttpxRecorder:
    return install(monkeypatch=monkeypatch, respond=json_response({"status": "ok"}))


def _timeouts(seconds: float | None) -> dict[str, float | None]:
    return dict.fromkeys(("connect", "read", "write", "pool"), seconds)


class TestDefaults:
    def test_default_url(self) -> None:
        assert SlayerClient().url == "http://localhost:5143"

    async def test_default_timeout_is_60s(self, ok: HttpxRecorder) -> None:
        await SlayerClient(url=URL).health()
        assert ok.last.timeout == _timeouts(60.0)

    def test_default_timeout_sync_transport(self, monkeypatch: pytest.MonkeyPatch) -> None:
        recorder = install(monkeypatch=monkeypatch, respond=json_response(QUERY_RESPONSE))
        SlayerClient(url=URL).query_sync(QUERY)
        assert recorder.last.timeout == _timeouts(60.0)

    async def test_custom_timeout(self, ok: HttpxRecorder) -> None:
        await SlayerClient(url=URL, timeout=5.0).health()
        assert ok.last.timeout == _timeouts(5.0)

    async def test_timeout_none_disables(self, ok: HttpxRecorder) -> None:
        await SlayerClient(url=URL, timeout=None).health()
        assert ok.last.timeout == _timeouts(None)


class TestAuth:
    async def test_sync_token_function(self, ok: HttpxRecorder) -> None:
        await SlayerClient(url=URL, token=lambda: "fn-token").health()
        assert ok.last.headers["authorization"] == "Bearer fn-token"

    async def test_async_token_function_resolved_per_request(self, ok: HttpxRecorder) -> None:
        tokens = iter(["t1", "t2"])

        async def next_token() -> str:
            return next(tokens)

        client = SlayerClient(url=URL, token=next_token)
        await client.health()
        await client.health()
        assert [r.headers["authorization"] for r in ok.requests] == ["Bearer t1", "Bearer t2"]

    def test_async_token_on_sync_transport(self, monkeypatch: pytest.MonkeyPatch) -> None:
        recorder = install(monkeypatch=monkeypatch, respond=json_response(QUERY_RESPONSE))

        async def token() -> str:
            return "async-t"

        SlayerClient(url=URL, token=token).query_sync(QUERY)
        assert recorder.last.headers["authorization"] == "Bearer async-t"

    def test_headers_on_sync_transport(self, monkeypatch: pytest.MonkeyPatch) -> None:
        recorder = install(monkeypatch=monkeypatch, respond=json_response(QUERY_RESPONSE))
        SlayerClient(url=URL, token="t", headers={"X-Tenant": "acme"}).query_sync(QUERY)
        assert (recorder.last.headers["authorization"], recorder.last.headers["x-tenant"]) == ("Bearer t", "acme")

    async def test_with_token_keeps_settings_and_leaves_original(self, ok: HttpxRecorder) -> None:
        original = SlayerClient(url=URL, token="t1", headers={"X-Tenant": "acme"}, timeout=7.0)
        copy = original.with_token("user-jwt")
        await copy.health()
        await original.health()
        first, second = ok.requests
        assert (first.headers["authorization"], first.headers["x-tenant"], first.timeout) == (
            "Bearer user-jwt", "acme", _timeouts(7.0),
        )
        assert second.headers["authorization"] == "Bearer t1"
        assert copy.url == original.url


class TestLocalModeRefusals:
    def test_token_with_storage(self, storage: YAMLStorage) -> None:
        with pytest.raises(ValueError):
            SlayerClient(storage=storage, token="t")

    def test_headers_with_storage(self, storage: YAMLStorage) -> None:
        with pytest.raises(ValueError):
            SlayerClient(storage=storage, headers={"X-Tenant": "acme"})

    def test_with_token_in_local_mode(self, storage: YAMLStorage) -> None:
        with pytest.raises(ValueError):
            SlayerClient(storage=storage).with_token("t")


class TestNewOperations:
    async def test_health(self, ok: HttpxRecorder) -> None:
        assert await SlayerClient(url=URL).health() == {"status": "ok"}

    async def test_delete_model_local_mode(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(DatasourceConfig(name="ds", type="sqlite", database=":memory:"))
        await storage.save_model(SlayerModel(
            name="orders", sql_table="orders_t", data_source="ds",
            columns=[Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True)],
        ))
        await SlayerClient(storage=storage).delete_model("orders", data_source="ds")
        assert await storage.get_model("orders", data_source="ds") is None

    async def test_delete_model_remote_raises_on_404(self, monkeypatch: pytest.MonkeyPatch) -> None:
        install(monkeypatch=monkeypatch, respond=json_response({"detail": "Model 'x' not found"}, status=404))
        with pytest.raises(httpx.HTTPStatusError):
            await SlayerClient(url=URL).delete_model("x")


class TestSignGatewayToken:
    def test_default_lifetime_is_one_hour(self) -> None:
        token = sign_gateway_token({"sub": "u", "tenant": "acme", "roles": []}, "s" * 32, now=1000)
        payload = token.split(".")[1]
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        assert (claims["iat"], claims["exp"]) == (1000, 4600)


class TestResponseWarnings:
    async def test_remote_query_keeps_warnings(self, monkeypatch: pytest.MonkeyPatch) -> None:
        warning = {"kind": "normalization", "rule_id": "r1", "original": "a", "normalized": "b", "location": "measures[0]"}
        install(monkeypatch=monkeypatch, respond=json_response({**QUERY_RESPONSE, "warnings": [warning]}))
        response = await SlayerClient(url=URL).query(QUERY)
        assert [(w.kind, getattr(w, "rule_id", None)) for w in response.warnings] == [("normalization", "r1")]
