"""``SlayerQuery.source_model`` is a typed union (str | ModelExtension | SlayerModel |
None) validated at construction; ``TimeDimension`` accepts ``column`` as an alias of
``dimension``.

Spec: queries/population — "Population specs are typed at construction";
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
from slayer.core.enums import DataType, TimeGranularity
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, ModelExtension, SlayerQuery, TimeDimension
from slayer.engine.stage_ordering import topologically_order_stages
from slayer.ir import source_bundle
from slayer.ir.source_bundle import (
    as_extension_over_nonsibling,
    follow_sibling_chain,
    source_name_if_sibling,
    spec_adds_measures,
)
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev1866_fixtures import DS_CHAIN, make_chain_exec_engine

EXTENSION = {
    "source_name": "orders",
    "columns": [{"name": "double_amount", "sql": "amount * 2", "type": "DOUBLE"}],
}
EXTENSION_WITH_MEASURES = {
    "source_name": "orders",
    "measures": [{"formula": "amount:sum", "name": "rev"}],
}
EXTENSION_WITH_FILTERS = {"source_name": "orders", "filters": ["subtotal > tax_paid * 5"]}
INLINE_MODEL = {
    "name": "ad_hoc",
    "sql_table": "things",
    "data_source": "ds",
    "columns": [{"name": "x", "sql": "x", "type": "DOUBLE"}],
}
JOIN_TO_CUSTOMERS = {"target_model": "customers", "join_pairs": [["customer_id", "id"]]}
ONE_MEASURE = [{"formula": "amount:sum"}]


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


def _has_none(value) -> bool:
    if isinstance(value, dict):
        return any(v is None or _has_none(v) for v in value.values())
    if isinstance(value, list):
        return any(_has_none(v) for v in value)
    return False


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
    def test_source_model_lists_the_three_forms(self) -> None:
        schema = SlayerQuery.model_json_schema()
        prop = schema["properties"]["source_model"]
        assert prop["anyOf"] == [
            {"oneOf": [
                {"type": "string"},
                {"$ref": "#/$defs/ModelExtension"},
                {"$ref": "#/$defs/SlayerModel"},
            ]},
            {"type": "null"},
        ]
        assert {"ModelExtension", "SlayerModel"} <= set(schema["$defs"])
        assert "filters" not in prop["description"]

    def test_extension_definition_is_closed_and_typed(self) -> None:
        ext = SlayerQuery.model_json_schema()["$defs"]["ModelExtension"]
        assert ext["additionalProperties"] is False
        assert ext["required"] == ["source_name"]
        assert ext["properties"]["columns"]["anyOf"][0]["items"] == {"$ref": "#/$defs/Column"}
        assert ext["properties"]["joins"]["anyOf"][0]["items"] == {"$ref": "#/$defs/ModelJoin"}

    def test_source_spec_is_defined_in_core_and_reused_by_ir(self) -> None:
        spec = core_query.SourceSpec
        assert source_bundle.SourceSpec is spec
        assert "SourceSpec" not in source_bundle.__all__
        assert _leaf_types(spec) == {str, ModelExtension, SlayerModel}

    async def test_mcp_query_tool_carries_the_definitions(self, mcp_server) -> None:
        tools = {t.name: t for t in await mcp_server.list_tools()}
        defs = tools["query"].inputSchema.get("$defs", {})
        assert {"ModelExtension", "SlayerModel"} <= set(defs)

    def test_openapi_embeds_the_forms_for_the_rest_body(self, rest_client) -> None:
        spec = rest_client.get("/openapi.json").json()
        prop = spec["components"]["schemas"]["QueryRequest"]["properties"]["source_model"]
        assert "#/components/schemas/ModelExtension" in str(prop)
        assert "#/components/schemas/SlayerModel" in str(prop)


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

    def test_unknown_extension_key_is_named(self) -> None:
        with pytest.raises(ValidationError) as ei:
            _query(source_model=EXTENSION_WITH_FILTERS)
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
                source_queries=[{"source_model": EXTENSION_WITH_FILTERS, "measures": ONE_MEASURE}],
            )

    def test_rest_body_rejects_unknown_extension_key_with_422(self, rest_client) -> None:
        resp = rest_client.post("/query", json={
            "source_model": EXTENSION_WITH_FILTERS, "measures": ONE_MEASURE, "dry_run": True,
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
                "query": {"source_model": EXTENSION_WITH_FILTERS, "measures": ONE_MEASURE},
                "dry_run": True,
            })


# ---------------------------------------------------------------------------
# Typed at construction
# ---------------------------------------------------------------------------


class TestTypedAtConstruction:
    def test_extension_object_validates_typed(self) -> None:
        q = _query(source_model={
            **EXTENSION_WITH_MEASURES,
            "columns": EXTENSION["columns"],
            "joins": [JOIN_TO_CUSTOMERS],
        })
        sm = q.source_model
        assert isinstance(sm, ModelExtension)
        assert sm.source_name == "orders"
        assert sm.columns
        assert sm.joins
        assert sm.measures
        assert isinstance(sm.columns[0], Column)
        assert sm.columns[0].type == DataType.DOUBLE
        assert isinstance(sm.joins[0], ModelJoin)
        assert sm.joins[0].target_model == "customers"
        assert isinstance(sm.measures[0], ModelMeasure)

    def test_inline_model_object_validates_typed(self) -> None:
        q = _query(source_model=INLINE_MODEL)
        assert isinstance(q.source_model, SlayerModel)
        assert q.source_model.name == "ad_hoc"
        assert q.source_model.columns[0].type == DataType.DOUBLE

    def test_string_stays_a_string(self) -> None:
        assert SlayerQuery(source_model="orders").source_model == "orders"

    def test_instances_pass_through_by_identity(self) -> None:
        ext = ModelExtension(source_name="orders")
        model = SlayerModel.model_validate(INLINE_MODEL)
        assert SlayerQuery(source_model=ext).source_model is ext
        assert SlayerQuery(source_model=model).source_model is model
        assert QueryRequest(source_model=ext).source_model is ext
        assert QueryRequest(source_model=model).source_model is model

    def test_rest_body_model_types_the_spec_and_feeds_the_query(self) -> None:
        req = QueryRequest.model_validate({"source_model": EXTENSION, "measures": ["amount:sum"]})
        assert isinstance(req.source_model, ModelExtension)
        q = SlayerQuery.model_validate(req.model_dump(exclude_none=True))
        assert q.source_model == req.source_model

    def test_model_copy_replacement_keeps_a_union_member(self) -> None:
        """``model_copy`` runs no validation, so the engine only ever swaps in typed forms."""
        q = SlayerQuery(source_model="orders")
        for replacement, expected in (
            ("customers", "customers"),
            (ModelExtension(source_name="orders"), "orders"),
            (SlayerModel.model_validate(INLINE_MODEL), "ad_hoc"),
        ):
            copied = q.model_copy(update={"source_model": replacement})
            assert isinstance(copied.source_model, (str, ModelExtension, SlayerModel))
            assert copied.source_model_name == expected


# ---------------------------------------------------------------------------
# Serialization round trips
# ---------------------------------------------------------------------------


class TestRoundTrip:
    @pytest.mark.parametrize("spec", [EXTENSION, INLINE_MODEL], ids=["extension", "model"])
    @pytest.mark.parametrize("mode", ["python", "json"])
    def test_dump_then_validate_equals_the_original(self, spec, mode) -> None:
        q = _query(source_model=spec, measures=["amount:sum"])
        again = SlayerQuery.model_validate(q.model_dump(mode=mode))
        assert again == q
        assert type(again.source_model) is type(q.source_model)
        assert not isinstance(again.source_model, dict)

    def test_exclude_none_dump_is_the_minimal_object(self) -> None:
        q = _query(source_model=EXTENSION_WITH_MEASURES)
        assert q.model_dump(exclude_none=True)["source_model"] == EXTENSION_WITH_MEASURES
        assert SlayerQuery(source_model="orders").model_dump(exclude_none=True)["source_model"] == "orders"

    def test_exclude_none_dump_of_an_inline_model_carries_no_null_keys(self) -> None:
        q = _query(source_model=INLINE_MODEL)
        dumped = q.model_dump(mode="json", exclude_none=True)["source_model"]
        assert not _has_none(dumped)
        assert set(INLINE_MODEL) <= set(dumped)
        assert dumped["columns"][0]["name"] == "x"
        assert SlayerQuery.model_validate({"source_model": dumped}) == q


# ---------------------------------------------------------------------------
# source_model_name
# ---------------------------------------------------------------------------


class TestSourceModelName:
    @pytest.mark.parametrize(
        "spec, expected",
        [("orders", "orders"), (EXTENSION, "orders"), (INLINE_MODEL, "ad_hoc"), (None, None)],
        ids=["str", "extension", "model", "none"],
    )
    def test_property_over_the_four_cases(self, spec, expected) -> None:
        assert SlayerQuery(source_model=spec).source_model_name == expected

    def test_prefix_strip_uses_the_typed_name(self) -> None:
        q = _query(source_model=EXTENSION, dimensions=["orders.status"])
        stripped = q.strip_source_model_prefix()
        assert stripped.dimensions
        dim = stripped.dimensions[0]
        assert isinstance(dim, ColumnRef)
        assert dim.model is None
        assert dim.name == "status"


# ---------------------------------------------------------------------------
# Helpers over typed specs (parity with the retired dict arms)
# ---------------------------------------------------------------------------


class TestHelpersOverTypedSpecs:
    def test_source_name_if_sibling(self) -> None:
        ext = _query(source_model={"source_name": "stage_a"}).source_model
        inline = _query(source_model=INLINE_MODEL).source_model
        assert source_name_if_sibling(spec=ext, sibling_names={"stage_a"}) == "stage_a"
        assert source_name_if_sibling(spec=ext, sibling_names={"other"}) is None
        assert source_name_if_sibling(spec="stage_a", sibling_names={"stage_a"}) == "stage_a"
        assert source_name_if_sibling(spec=inline, sibling_names={"ad_hoc"}) is None

    def test_spec_adds_measures(self) -> None:
        assert spec_adds_measures(_query(source_model=EXTENSION_WITH_MEASURES).source_model)
        assert not spec_adds_measures(_query(source_model=EXTENSION).source_model)
        assert not spec_adds_measures(_query(source_model=INLINE_MODEL).source_model)
        assert not spec_adds_measures("orders")

    def test_as_extension_over_nonsibling_returns_the_held_instance(self) -> None:
        q = _query(source_model=EXTENSION)
        assert as_extension_over_nonsibling(spec=q.source_model, sibling_names=set()) is q.source_model
        assert as_extension_over_nonsibling(spec=q.source_model, sibling_names={"orders"}) is None
        assert as_extension_over_nonsibling(spec="orders", sibling_names=set()) is None
        inline = _query(source_model=INLINE_MODEL).source_model
        assert as_extension_over_nonsibling(spec=inline, sibling_names=set()) is None

    def test_follow_sibling_chain_reaches_the_typed_base(self) -> None:
        a = _query(name="a", source_model=EXTENSION)
        b = _query(name="b", source_model={"source_name": "a"})
        named = {"a": a, "b": b}
        base = follow_sibling_chain(spec=SlayerQuery(source_model="b").source_model, named_queries=named)
        assert base is a.source_model
        assert follow_sibling_chain(spec=None, named_queries=named) is None

    def test_follow_sibling_chain_cycle_raises(self) -> None:
        x = _query(name="x", source_model={"source_name": "y"})
        y = SlayerQuery(name="y", source_model="x")
        with pytest.raises(ValueError, match="Circular"):
            follow_sibling_chain(spec="x", named_queries={"x": x, "y": y})

    def test_stage_ordering_walks_typed_specs_from_raw_objects(self) -> None:
        a = SlayerQuery(name="a", source_model="orders")
        c = SlayerQuery(name="c", source_model="orders")
        via_join = SlayerQuery.model_validate({
            "name": "via_join",
            "source_model": {
                "source_name": "orders",
                "joins": [{"target_model": "c", "join_pairs": [["id", "id"]]}],
            },
        })
        via_nested = SlayerQuery.model_validate({
            "name": "via_nested",
            "source_model": {
                "name": "inline_qb",
                "source_queries": [{"source_model": {"source_name": "a"}}],
            },
        })
        assert isinstance(via_join.source_model, ModelExtension)
        assert isinstance(via_nested.source_model, SlayerModel)
        root = SlayerQuery(source_model="via_nested")
        names = [q.name for q in topologically_order_stages([via_nested, via_join, a, c, root])]
        assert names.index("a") < names.index("via_nested")
        assert names.index("c") < names.index("via_join")


# ---------------------------------------------------------------------------
# Executes exactly as before
# ---------------------------------------------------------------------------


class TestExecutionParity:
    async def test_extension_columns_execute(self, chain_engine) -> None:
        resp = await chain_engine.execute(_query(
            source_model=EXTENSION,
            measures=[{"formula": "double_amount:sum", "name": "dbl"}],
        ))
        assert resp.data[0]["orders.dbl"] == pytest.approx(130.0)
        assert resp.population == "orders"

    async def test_inline_model_executes(self, chain_engine) -> None:
        resp = await chain_engine.execute(_query(
            source_model={
                "name": "inl_orders", "data_source": DS_CHAIN, "sql_table": "orders",
                "columns": [{"name": "amount", "type": "DOUBLE"}],
            },
            measures=[{"formula": "amount:sum", "name": "total"}],
        ))
        assert resp.data[0]["inl_orders.total"] == pytest.approx(65.0)
        assert resp.population == "inl_orders"


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
        assert q.time_dimensions[0].dimension.name == "created_at"
        dumped = q.model_dump(mode="json", exclude_none=True)["time_dimensions"][0]
        assert dumped == {"dimension": {"name": "created_at"}, "granularity": "month"}

    def test_column_key_accepts_a_column_ref_object(self) -> None:
        td = TimeDimension.model_validate({
            "column": {"name": "created_at", "model": "orders"}, "granularity": "day",
        })
        assert (td.dimension.name, td.dimension.model) == ("created_at", "orders")

    def test_dimension_key_and_keyword_still_validate(self) -> None:
        via_key = TimeDimension.model_validate({"dimension": "created_at", "granularity": "month"})
        via_kw = TimeDimension(
            dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
        )
        assert via_key == via_kw
        assert via_kw.dimension.name == "created_at"

    def test_dimension_wins_when_both_keys_are_present(self) -> None:
        td = TimeDimension.model_validate({"dimension": "a", "column": "b", "granularity": "month"})
        assert td.dimension.name == "a"

    def test_schema_advertises_dimension_only(self) -> None:
        props = SlayerQuery.model_json_schema()["$defs"]["TimeDimension"]["properties"]
        assert "dimension" in props
        assert "column" not in props
