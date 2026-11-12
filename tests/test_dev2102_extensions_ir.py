"""``SlayerQuery.extensions``: typed at construction, closed, one per model, advertised on every surface."""

from __future__ import annotations

import json
import tempfile
from typing import Any

import pytest
from fastapi.testclient import TestClient
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import ValidationError

from slayer.api.server import QueryRequest, create_app
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ModelExtension, QueryRefinement, SlayerQuery
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage

CUSTOMERS_EXT = {
    "source_name": "customers",
    "columns": [{"name": "tier_label", "sql": "upper(tier)", "type": "TEXT"}],
    "measures": [{"name": "credit_total", "formula": "sum(credit)"}],
    "joins": [{"target_model": "regions", "join_pairs": [["region_id", "id"]]}],
}
ORDERS_EXT = {"source_name": "orders", "columns": [{"name": "double_amount", "sql": "amount * 2", "type": "DOUBLE"}]}
WITH_FILTERS = {"source_name": "customers", "filters": ["credit > 5"]}
MEASURES_ONLY = {"source_name": "orders", "measures": [{"formula": "sum(amount)", "name": "rev"}]}
ONE_MEASURE = [{"formula": "count(*)"}]


def _query(**raw: Any) -> SlayerQuery:
    return SlayerQuery.model_validate(raw)


def _assert_rejected_naming(exc: ValidationError, name: str) -> None:
    """A validation error naming ``name``, never the ``extensions`` key itself being unknown."""
    assert all(e["type"] != "extra_forbidden" for e in exc.errors()), exc
    assert name in str(exc), exc


def _refs(schema: Any) -> set[str]:
    """Every ``$ref`` and ``type`` leaf in a schema fragment."""
    found: set[str] = set()
    if isinstance(schema, dict):
        for key, value in schema.items():
            if key == "$ref" or (key == "type" and isinstance(value, str)):
                found.add(value)
            elif key != "description":
                found |= _refs(value)
    elif isinstance(schema, list):
        for item in schema:
            found |= _refs(item)
    return found


@pytest.fixture(scope="module")
def rest_client():
    with tempfile.TemporaryDirectory() as tmp:
        yield TestClient(create_app(storage=YAMLStorage(base_dir=tmp)))


@pytest.fixture
def mcp_server(tmp_path):
    return create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path)))


class TestTyped:
    def test_extensions_validate_into_typed_objects(self) -> None:
        q = _query(source_model="orders", extensions=[CUSTOMERS_EXT, ORDERS_EXT])
        assert q.source_model == "orders"
        assert [type(e) for e in q.extensions or []] == [ModelExtension, ModelExtension]
        first = (q.extensions or [])[0]
        assert first.source_name == "customers"
        assert isinstance((first.columns or [])[0], Column)
        assert isinstance((first.measures or [])[0], ModelMeasure)
        assert isinstance((first.joins or [])[0], ModelJoin)

    def test_extensions_default_to_none(self) -> None:
        assert SlayerQuery(source_model="orders").extensions is None

    def test_extensions_on_a_rootless_query(self) -> None:
        q = _query(extensions=[CUSTOMERS_EXT], dimensions=["customers.tier_label"])
        assert q.source_model is None
        assert (q.extensions or [])[0].source_name == "customers"

    def test_source_model_name_ignores_extensions(self) -> None:
        assert _query(source_model="orders", extensions=[CUSTOMERS_EXT]).source_model_name == "orders"


class TestRejected:
    def test_two_extensions_of_one_model(self) -> None:
        with pytest.raises(ValidationError) as info:
            _query(source_model="orders", extensions=[CUSTOMERS_EXT, {"source_name": "customers"}])
        _assert_rejected_naming(info.value, "customers")

    def test_unknown_extension_key_is_named(self) -> None:
        with pytest.raises(ValidationError) as info:
            _query(source_model="orders", extensions=[WITH_FILTERS])
        assert {(e["type"], str(e["loc"][-1])) for e in info.value.errors()} == {("extra_forbidden", "filters")}

    def test_an_extension_without_source_name(self) -> None:
        with pytest.raises(ValidationError, match="source_name"):
            _query(source_model="orders", extensions=[{"columns": []}])

    def test_refinements_carry_no_extensions(self) -> None:
        with pytest.raises(ValidationError) as info:
            QueryRefinement.model_validate({"extensions": [CUSTOMERS_EXT]})
        assert {(e["type"], str(e["loc"][-1])) for e in info.value.errors()} == {("extra_forbidden", "extensions")}

    def test_a_model_s_stage_with_a_duplicate_is_rejected_too(self) -> None:
        with pytest.raises(ValidationError) as info:
            SlayerModel(name="qb", source_queries=[{
                "source_model": "orders", "measures": ONE_MEASURE,
                "extensions": [{"source_name": "customers"}, {"source_name": "customers"}]}])
        _assert_rejected_naming(info.value, "customers")


class TestSchema:
    def test_source_model_lists_a_name_or_an_inline_model(self) -> None:
        schema = SlayerQuery.model_json_schema()
        prop = schema["properties"]["source_model"]
        assert _refs(prop) == {"string", "#/$defs/SlayerModel", "null"}
        assert "source_name" not in prop.get("description", "")

    def test_extensions_list_model_extension_references(self) -> None:
        schema = SlayerQuery.model_json_schema()
        prop = schema["properties"]["extensions"]
        assert _refs(prop) == {"array", "#/$defs/ModelExtension", "null"}
        assert prop.get("description")
        ext = schema["$defs"]["ModelExtension"]
        assert ext["additionalProperties"] is False
        assert ext["required"] == ["source_name"]

    def test_refinement_schema_has_no_extensions(self) -> None:
        assert "extensions" not in QueryRefinement.model_json_schema()["properties"]

    async def test_mcp_query_tool_carries_the_definitions(self, mcp_server) -> None:
        tools = {t.name: t for t in await mcp_server.list_tools()}
        schema = tools["query"].inputSchema
        assert {"ModelExtension", "SlayerModel"} <= set(schema.get("$defs", {}))
        assert '"extensions"' in json.dumps(schema)

    def test_openapi_rest_body_carries_extensions(self, rest_client) -> None:
        props = rest_client.get("/openapi.json").json()["components"]["schemas"]["QueryRequest"]["properties"]
        assert "#/components/schemas/ModelExtension" in json.dumps(props["extensions"])
        assert "#/components/schemas/ModelExtension" not in json.dumps(props["source_model"])
        assert "#/components/schemas/SlayerModel" in json.dumps(props["source_model"])


class TestRestAndMcp:
    def test_rest_body_types_extensions_and_feeds_the_query(self) -> None:
        req = QueryRequest.model_validate({"source_model": "orders", "extensions": [ORDERS_EXT], "measures": ["count(*)"]})
        assert isinstance((req.extensions or [])[0], ModelExtension)
        q = SlayerQuery.model_validate(req.model_dump(exclude_none=True))
        assert q.extensions == req.extensions

    @pytest.mark.parametrize("version", [9, 10, 11, 12])
    def test_rest_body_migrates_a_versioned_legacy_root_extension(self, version: int) -> None:
        req = QueryRequest.model_validate({"version": version, "source_model": ORDERS_EXT, "measures": ["count(*)"]})
        assert req.source_model == "orders"
        assert [e.source_name for e in req.extensions or []] == ["orders"]

    @pytest.mark.parametrize("extensions", [
        [WITH_FILTERS], [{"source_name": "orders"}, {"source_name": "orders"}],
    ], ids=["unknown-key", "duplicate"])
    def test_rest_body_rejects_malformed_extensions_with_422(self, rest_client, extensions) -> None:
        resp = rest_client.post("/query", json={
            "source_model": "orders", "extensions": extensions, "measures": ONE_MEASURE, "dry_run": True})
        assert resp.status_code == 422, resp.text

    async def test_mcp_query_tool_rejects_an_unknown_extension_key(self, mcp_server) -> None:
        with pytest.raises(ToolError, match="filters"):
            await mcp_server.call_tool(name="query", arguments={
                "query": {"source_model": "orders", "extensions": [WITH_FILTERS], "measures": ONE_MEASURE},
                "dry_run": True,
            })


class TestRoundTrip:
    @pytest.mark.parametrize("mode", ["python", "json"])
    def test_dump_then_validate_equals_the_original(self, mode) -> None:
        q = _query(source_model="orders", extensions=[CUSTOMERS_EXT, ORDERS_EXT], measures=["count(*)"])
        again = SlayerQuery.model_validate(q.model_dump(mode=mode))
        assert again == q
        assert all(isinstance(e, ModelExtension) for e in again.extensions or [])

    def test_exclude_none_dump_is_what_the_author_wrote(self) -> None:
        q = _query(source_model="orders", extensions=[MEASURES_ONLY])
        dumped = q.model_dump(exclude_none=True)
        assert dumped["source_model"] == "orders"
        assert dumped["extensions"] == [MEASURES_ONLY]

    def test_no_extensions_key_when_unset(self) -> None:
        assert "extensions" not in SlayerQuery(source_model="orders").model_dump(exclude_none=True)
