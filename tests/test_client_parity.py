"""Python half of the client parity harness: replays ``tests/client_parity.json`` (TS half: ts-sdk/test/parity.test.ts)."""

import inspect
import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from slayer.client import sign_gateway_token
from slayer.client.slayer_client import SlayerClient
from slayer.core.query import SlayerQuery
from tests._httpx_mock import install

MANIFEST: dict[str, Any] = json.loads((Path(__file__).parent / "client_parity.json").read_text())


def _norm_stage(stage: dict[str, Any]) -> dict[str, Any]:
    return SlayerQuery.model_validate(stage).model_dump(mode="json", exclude_none=True)


def _norm_query_body(body: dict[str, Any]) -> dict[str, Any]:
    """Python normalises queries through ``SlayerQuery``; TS sends them verbatim."""
    if "name" in body and "source_model" not in body:
        return body
    if "queries" in body:
        return {**body, "queries": [_norm_stage(s) for s in body["queries"]]}
    top = {k: body[k] for k in ("dry_run", "explain") if k in body}
    stage = _norm_stage({k: v for k, v in body.items() if k not in ("dry_run", "explain")})
    return {**stage, **top}


def _public_methods() -> set[str]:
    return {
        name for name, member in inspect.getmembers(SlayerClient)
        if not name.startswith("_") and (inspect.isfunction(member) or inspect.ismethod(member))
    }


def test_method_set_matches_manifest() -> None:
    expected = {op["python"] for op in MANIFEST["operations"]} | set(MANIFEST["python_only"])
    assert _public_methods() == expected


def test_every_operation_has_a_case() -> None:
    covered = {c["op"] for c in MANIFEST["cases"]} | {"with_token"}
    assert {op["python"] for op in MANIFEST["operations"]} <= covered


@pytest.mark.parametrize("case", MANIFEST["cases"], ids=lambda c: c["name"])
async def test_case(case: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> None:
    response = MANIFEST["responses"][case["op"]]
    recorder = install(monkeypatch=monkeypatch, respond=lambda _r: httpx.Response(200, json=response))
    client = SlayerClient(url=MANIFEST["base_url"], **case.get("client", {}))
    if "with_token" in case:
        client = client.with_token(case["with_token"])

    result = await getattr(client, case["op"])(*case["python"]["args"], **case["python"]["kwargs"])

    assert len(recorder.requests) == 1
    req, expect = recorder.last, case["expect"]
    assert (req.method, req.raw_path, req.query) == (expect["method"], expect["path"], expect["query"])
    if expect["path"] == "/query":
        assert _norm_query_body(req.body) == _norm_query_body(expect["body"])
    else:
        assert req.body == expect["body"]
    for name, value in expect.get("headers", {}).items():
        assert req.headers.get(name) == value
    for name in expect.get("absent_headers", []):
        assert name not in req.headers
    if "returns" in case:
        assert result == case["returns"]


@pytest.mark.parametrize("case", MANIFEST["signing"], ids=lambda c: c["name"])
def test_signing(case: dict[str, Any]) -> None:
    kwargs = case["python"]["kwargs"]
    if case.get("error"):
        with pytest.raises(ValueError):
            sign_gateway_token(case["claims"], case["secret"], **kwargs)
    else:
        assert sign_gateway_token(case["claims"], case["secret"], **kwargs) == case["expected"]
