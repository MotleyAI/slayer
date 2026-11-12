"""``SlayerQuery.source_model`` is a name or None validated at construction, a legacy
extension object converting into ``extensions`` and an inline model refused; ``TimeDimension``
accepts ``column`` as an alias of ``dimension``.

Spec: queries/population — "A population is named, and typed at construction";
queries/time-dimensions — "column is an accepted alias of dimension".
"""

from __future__ import annotations

import tempfile
from types import UnionType
from typing import Annotated, Any, Union, get_args, get_origin

import pytest
from fastapi.testclient import TestClient
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import ValidationError

from slayer.api.server import QueryRequest, create_app
from slayer.core import query as core_query
from slayer.core.enums import TimeGranularity
from slayer.core.models import ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ModelExtension, SlayerQuery, TimeDimension
from slayer.engine.stage_ordering import topologically_order_stages
from slayer.ir import source_bundle
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1866_fixtures import make_chain_exec_engine

EXTENSION = {
    "source_name": "orders",
    "measures": [{"formula": "sum(amount * 2)", "name": "double_amount"}],
}
EXTENSION_WITH_MEASURES = {
    "source_name": "orders",
    "measures": [{"formula": "sum(amount)", "name": "rev"}],
}
EXTENSION_WITH_FILTERS = {"source_name": "orders", "filters": ["subtotal > tax_paid * 5"]}
INLINE_MODEL = {
    "name": "ad_hoc",
    "sql_table": "things",
    "data_source": "ds",
    "columns": [{"name": "x", "sql": "x", "type": "DOUBLE"}],
}
JOIN_TO_CUSTOMERS = {"target_model": "customers", "join_pairs": [["customer_id", "id"]]}
ONE_MEASURE = [{"formula": "sum(amount)"}]


def _leaf_types(tp) -> set:
    """Concrete members of a (possibly Annotated / nested) union."""
    origin = get_origin(tp)
    if origin is Annotated:
        return _leaf_types(get_args(tp)[0])
    if origin in (Union, UnionType):
        return set().union(*(_leaf_types(a) for a in get_args(tp)))
    return {tp}


def _error_keys(exc: ValidationError) -> set[tuple[str, str]]:
    return {(e["type"], str(e["loc"][-1])) for e in exc.errors()}


def _query(**raw: Any) -> SlayerQuery:
    """Construct from JSON-shaped input (the wire form), typed as such."""
    return SlayerQuery.model_validate(raw)


@pytest.fixture(scope="module")
def rest_client():
    with tempfile.TemporaryDirectory() as tmp:
        yield TestClient(create_app(storage=YAMLStorage(base_dir=tmp)))


@pytest.fixture
def mcp_server(tmp_path):
    return create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path)))


@pytest.fixture
async def chain_engine():
    async for engine in make_chain_exec_engine("sqlite"):
        yield engine


# ---------------------------------------------------------------------------
# Schema advertises the three forms
# ---------------------------------------------------------------------------


class TestSchema:
    def test_source_model_is_a_name_or_null(self) -> None:
        schema = SlayerQuery.model_json_schema()
        prop = schema["properties"]["source_model"]
        assert "$ref" not in str(prop)
        assert {a.get("type") for a in prop["anyOf"]} == {"string", "null"}
        assert "ModelExtension" in schema["$defs"]
        assert "SlayerModel" not in schema["$defs"]
        assert "filters" not in prop["description"]

    def test_extension_definition_is_closed_and_typed(self) -> None:
        ext = SlayerQuery.model_json_schema()["$defs"]["ModelExtension"]
        assert ext["additionalProperties"] is False
        assert ext["required"] == ["source_name"]
        assert ext["properties"]["measures"]["anyOf"][0]["items"] == {"$ref": "#/$defs/ModelMeasure"}
        assert ext["properties"]["joins"]["anyOf"][0]["items"] == {"$ref": "#/$defs/ModelJoin"}

    def test_source_spec_is_defined_in_core_and_reused_by_ir(self) -> None:
        spec = core_query.SourceSpec
        assert source_bundle.SourceSpec is spec
        assert "SourceSpec" not in source_bundle.__all__
        assert _leaf_types(spec) == {str}

    async def test_mcp_query_tool_carries_the_definitions(self, mcp_server) -> None:
        tools = {t.name: t for t in await mcp_server.list_tools()}
        defs = tools["query"].inputSchema.get("$defs", {})
        assert "ModelExtension" in defs
        assert "SlayerModel" not in defs

    def test_openapi_embeds_the_forms_for_the_rest_body(self, rest_client) -> None:
        spec = rest_client.get("/openapi.json").json()
        props = spec["components"]["schemas"]["QueryRequest"]["properties"]
        assert "#/components/schemas/ModelExtension" not in str(props["source_model"])
        assert "#/components/schemas/SlayerModel" not in str(props["source_model"])
        assert "#/components/schemas/ModelExtension" in str(props["extensions"])


# ---------------------------------------------------------------------------
# Rejection at construction
# ---------------------------------------------------------------------------


class TestRejection:
    @pytest.mark.parametrize(
        "bad", [42, [1, 2], {"foo": 1}, EXTENSION_WITH_FILTERS], ids=["int", "list", "dict", "filters"],
    )
    def test_non_spec_value_is_rejected(self, bad) -> None:
        with pytest.raises(ValidationError):
            SlayerQuery(source_model=bad)

    @pytest.mark.parametrize("form", ["legacy", "extensions"])
    def test_unknown_extension_key_is_named(self, form: str) -> None:
        raw = ({"source_model": EXTENSION_WITH_FILTERS} if form == "legacy"
               else {"source_model": "orders", "extensions": [EXTENSION_WITH_FILTERS]})
        with pytest.raises(ValidationError) as ei:
            _query(**raw)
        assert _error_keys(ei.value) == {("extra_forbidden", "filters")}

    def test_source_name_plus_model_keys_is_an_extension_never_a_model(self) -> None:
        with pytest.raises(ValidationError) as ei:
            _query(source_model={
                "source_name": "orders", "name": "x", "sql_table": "t", "data_source": "ds",
            })
        assert _error_keys(ei.value) == {
            ("extra_forbidden", "name"),
            ("extra_forbidden", "sql_table"),
            ("extra_forbidden", "data_source"),
        }

    def test_nested_stage_boundary_rejects_too(self) -> None:
        with pytest.raises(ValidationError, match="filters"):
            SlayerModel(
                name="qb",
                source_queries=[{"source_model": "orders", "extensions": [EXTENSION_WITH_FILTERS],
                                 "measures": ONE_MEASURE}],
            )

    def test_rest_body_rejects_unknown_extension_key_with_422(self, rest_client) -> None:
        resp = rest_client.post("/query", json={
            "source_model": "orders", "extensions": [EXTENSION_WITH_FILTERS], "measures": ONE_MEASURE,
            "dry_run": True,
        })
        assert resp.status_code == 422, resp.text
        assert "filters" in resp.text

    def test_rest_body_rejects_non_spec_value_with_422(self, rest_client) -> None:
        resp = rest_client.post("/query", json={
            "source_model": 42, "measures": ONE_MEASURE, "dry_run": True,
        })
        assert resp.status_code == 422, resp.text

    async def test_mcp_query_tool_rejects_unknown_extension_key(self, mcp_server) -> None:
        with pytest.raises(ToolError, match="filters"):
            await mcp_server.call_tool(name="query", arguments={
                "query": {"source_model": "orders", "extensions": [EXTENSION_WITH_FILTERS], "measures": ONE_MEASURE},
                "dry_run": True,
            })


# ---------------------------------------------------------------------------
# Typed at construction
# ---------------------------------------------------------------------------


class TestTypedAtConstruction:
    def test_legacy_extension_object_converts_typed(self) -> None:
        with pytest.warns(FutureWarning, match="extensions"):
            q = _query(source_model={
                **EXTENSION_WITH_MEASURES,
                "joins": [JOIN_TO_CUSTOMERS],
            })
        assert q.source_model == "orders"
        (sm,) = q.extensions or []
        assert isinstance(sm, ModelExtension)
        assert sm.source_name == "orders"
        assert sm.joins
        assert sm.measures
        assert isinstance(sm.joins[0], ModelJoin)
        assert sm.joins[0].target_model == "customers"
        assert isinstance(sm.measures[0], ModelMeasure)

    def test_inline_model_object_is_rejected(self) -> None:
        with pytest.raises(ValidationError, match="(?i)inline"):
            _query(source_model=INLINE_MODEL)

    def test_string_stays_a_string(self) -> None:
        assert SlayerQuery(source_model="orders").source_model == "orders"

    def test_model_instance_is_rejected(self) -> None:
        model = SlayerModel.model_validate(INLINE_MODEL)
        with pytest.raises(ValidationError, match="(?i)inline"):
            SlayerQuery(source_model=model)  # pyright: ignore[reportArgumentType] — the refused form, on purpose
        with pytest.raises(ValidationError, match="(?i)inline"):
            QueryRequest(source_model=model)  # pyright: ignore[reportArgumentType] — the refused form, on purpose

    def test_rest_body_model_types_the_spec_and_feeds_the_query(self) -> None:
        req = QueryRequest.model_validate({"source_model": "orders", "extensions": [EXTENSION], "measures": ["sum(amount)"]})
        assert isinstance((req.extensions or [])[0], ModelExtension)
        q = SlayerQuery.model_validate(req.model_dump(exclude_none=True))
        assert (q.source_model, q.extensions) == (req.source_model, req.extensions)

    def test_model_copy_replacement_keeps_the_name_form(self) -> None:
        """``model_copy`` runs no validation, so the engine only ever swaps in a name."""
        copied = SlayerQuery(source_model="orders").model_copy(update={"source_model": "customers"})
        assert isinstance(copied.source_model, str)
        assert copied.source_model_name == "customers"


# ---------------------------------------------------------------------------
# Serialization round trips
# ---------------------------------------------------------------------------


class TestRoundTrip:
    @pytest.mark.parametrize("raw", [
        {"source_model": "orders", "extensions": [EXTENSION]},
    ], ids=["extension"])
    @pytest.mark.parametrize("mode", ["python", "json"])
    def test_dump_then_validate_equals_the_original(self, raw, mode) -> None:
        q = _query(**raw, measures=["sum(amount)"])
        again = SlayerQuery.model_validate(q.model_dump(mode=mode))
        assert again == q
        assert type(again.source_model) is type(q.source_model)
        assert not isinstance(again.source_model, dict)

    def test_exclude_none_dump_is_the_minimal_object(self) -> None:
        q = _query(source_model="orders", extensions=[EXTENSION_WITH_MEASURES])
        assert q.model_dump(exclude_none=True)["extensions"] == [EXTENSION_WITH_MEASURES]
        assert SlayerQuery(source_model="orders").model_dump(exclude_none=True)["source_model"] == "orders"


# ---------------------------------------------------------------------------
# source_model_name
# ---------------------------------------------------------------------------


class TestSourceModelName:
    @pytest.mark.parametrize(
        "spec, expected",
        [("orders", "orders"), (None, None)],
        ids=["str", "none"],
    )
    def test_property_over_the_two_cases(self, spec, expected) -> None:
        assert SlayerQuery(source_model=spec).source_model_name == expected

    def test_property_over_a_converted_legacy_extension(self) -> None:
        with pytest.warns(FutureWarning):
            assert SlayerQuery(source_model=EXTENSION).source_model_name == "orders"  # pyright: ignore[reportArgumentType] — the deprecated form, on purpose

    async def test_root_qualifier_uses_the_typed_name(self, chain_engine) -> None:
        qualified = await chain_engine.execute(_query(
            source_model="orders", extensions=[EXTENSION], dimensions=["orders.status"]))
        bare = await chain_engine.execute(_query(source_model="orders", extensions=[EXTENSION], dimensions=["status"]))
        assert qualified.sql == bare.sql
        assert qualified.columns == bare.columns == ["orders.status"]


# ---------------------------------------------------------------------------
# Stage ordering over typed extensions
# ---------------------------------------------------------------------------


class TestStageOrdering:
    def test_stage_ordering_walks_typed_specs_from_raw_objects(self) -> None:
        a = SlayerQuery(name="a", source_model="orders")
        c = SlayerQuery(name="c", source_model="orders")
        via_join = SlayerQuery.model_validate({
            "name": "via_join",
            "source_model": "orders",
            "extensions": [{
                "source_name": "orders",
                "joins": [{"target_model": "c", "join_pairs": [["id", "id"]]}],
            }],
        })
        assert isinstance((via_join.extensions or [])[0], ModelExtension)
        root = SlayerQuery(source_model="via_join")
        names = [q.name for q in topologically_order_stages([via_join, a, c, root])]
        assert names.index("c") < names.index("via_join")


# ---------------------------------------------------------------------------
# TimeDimension: column is an alias of dimension
# ---------------------------------------------------------------------------


class TestTimeDimensionColumnAlias:
    def test_column_key_validates_and_dumps_as_dimension(self) -> None:
        q = _query(
            source_model="orders",
            time_dimensions=[{"column": "created_at", "granularity": "month"}],
        )
        assert q.time_dimensions
        assert q.time_dimensions[0].dimension == "created_at"
        dumped = q.model_dump(mode="json", exclude_none=True)["time_dimensions"][0]
        assert dumped == {"dimension": "created_at", "granularity": "month"}

    def test_column_key_accepts_a_column_ref_object(self) -> None:
        td = TimeDimension.model_validate({
            "column": {"name": "created_at", "model": "orders"}, "granularity": "day",
        })
        assert td.dimension == "orders.created_at"

    def test_dimension_key_and_keyword_still_validate(self) -> None:
        via_key = TimeDimension.model_validate({"dimension": "created_at", "granularity": "month"})
        via_kw = TimeDimension(
            dimension="created_at", granularity=TimeGranularity.MONTH,
        )
        assert via_key == via_kw
        assert via_kw.dimension == "created_at"

    def test_dimension_wins_when_both_keys_are_present(self) -> None:
        td = TimeDimension.model_validate({"dimension": "a", "column": "b", "granularity": "month"})
        assert td.dimension == "a"

    def test_schema_advertises_dimension_only(self) -> None:
        props = SlayerQuery.model_json_schema()["$defs"]["TimeDimension"]["properties"]
        assert "dimension" in props
        assert "column" not in props
