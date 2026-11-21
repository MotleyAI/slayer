"""A custom aggregation's result type: declared, else inferred from its formula, else its input's type.

Spec: openspec …/specs/aggregations/formula-templates — "A custom aggregation's result type";
…/specs/aggregations/boolean-inputs — "Boolean aggregation result types and formats".
"""

from __future__ import annotations

import tempfile
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional, cast

import pytest
from pydantic import ValidationError

from slayer.core.enums import DEFAULT_AGGREGATIONS_BY_TYPE, DataType
from slayer.core.models import Aggregation, AggregationParam, DatasetField, SlayerDataset
from slayer.sql.template_type import template_result_type
from slayer.facade.catalog import build_local_view
from slayer.mcp.server import create_mcp_server
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2373_fixtures import (
    DS,
    custom_aggregations,
    emitted_types,
    m,
    orders,
    plan_type,
    read_types,
    shop_datasets,
    stage_columns,
    q,
)
from tests._osi_export_fixtures import customers as osi_customers
from tests._osi_export_fixtures import orders as osi_orders
from tests._osi_export_helpers import dataset, export_datasets, make_env, payload, reimport, stored

INT, DOUBLE, BOOL, DATE = DataType.INTEGER, DataType.FLOAT, DataType.BOOLEAN, DataType.DATE


def _types(expr: str, *, dialect: str = "duckdb", datasets: Optional[list[SlayerDataset]] = None) -> set[Optional[DataType]]:
    """The type the query plan, the emitting stage and a later stage give ``expr`` (one value when they agree)."""
    first = {"dimensions": ["orders.region"], "metrics": [m(expr)]}
    return {
        plan_type({"metrics": [m(expr)]}, dialect=dialect, datasets=datasets),
        emitted_types(first, names=["x"], dialect=dialect, datasets=datasets)["x"],
        read_types(first, names=["x"], dialect=dialect, datasets=datasets)["x"],
    }


def _with(*aggregations: Aggregation) -> list[SlayerDataset]:
    return shop_datasets(orders_dataset=orders(aggregations=list(aggregations)))


# --------------------------------------------------------------------------- #
# aggregations/formula-templates › A custom aggregation's result type
# --------------------------------------------------------------------------- #
class TestInferredFromTheFormula:
    def test_a_count_of_trues_is_int(self) -> None:
        assert _types("trues(orders.flag)") == {INT}

    def test_a_share_over_a_boolean_is_double(self) -> None:
        assert _types("share(orders.status = 'paid')") == {DOUBLE}

    def test_a_boolean_returning_formula_is_boolean(self) -> None:
        assert _types("all_true(orders.flag)", dialect="postgres") == {BOOL}

    def test_an_average_over_an_int_is_double(self) -> None:
        assert _types("my_avg(orders.quantity)") == {DOUBLE}

    @pytest.mark.parametrize(("expr", "expected"), [
        ("wsum(orders.quantity, weight=orders.quantity)", INT),
        ("wsum(orders.quantity, weight=orders.rate)", DOUBLE),
        ("wsum(orders.quantity)", INT),
        ("wdef(orders.quantity)", DOUBLE),
    ])
    def test_parameter_types_enter_the_inference(self, expr: str, expected: DataType) -> None:
        assert _types(expr) == {expected}

    @pytest.mark.parametrize(("expr", "expected"), [
        ("all_true(orders.flag)", BOOL), ("total(orders.price)", DataType.DECIMAL),
    ])
    def test_sqlite_formulas_keep_boolean_and_decimal(self, expr: str, expected: DataType) -> None:
        assert _types(expr, dialect="sqlite") == {expected}

    def test_a_model_formula_overriding_a_built_in_is_typed_from_its_formula(self) -> None:
        override = _with(Aggregation(name="sum", expression="SUM({value}) / 2.0"))
        assert _types("sum(orders.quantity)", datasets=override) == {DOUBLE}


class TestDeclaredDatatype:
    def test_a_declared_datatype_wins(self) -> None:
        declared = shop_datasets(orders_dataset=orders(aggregations=custom_aggregations(trues_datatype="Float")))
        assert _types("trues(orders.flag)", datasets=declared) == {DOUBLE}

    def test_a_declared_array_has_opaque_elements(self) -> None:
        agg = Aggregation.model_validate({"name": "bag", "expression": "ARRAY_AGG({value})", "datatype": "Array"})
        s = q(name="s", population="orders", dimensions=["orders.region"], metrics=[m("bag(orders.status)")])
        column = stage_columns([s, q(population="s", dimensions=["s.region"])], stage="s", datasets=_with(agg))["x"]
        assert (column.datatype, column.element_type) == (DataType.ARRAY, DataType.OPAQUE)

    def test_an_invalid_declared_datatype_is_refused_at_save(self) -> None:
        raw: dict[str, Any] = {
            "name": "orders", "data_source": DS, "sql_table": "orders",
            "fields": [{"name": "flag", "datatype": "Boolean"}],
            "aggregations": [{"name": "trues", "expression": "COUNT(CASE WHEN {value} THEN 1 END)", "datatype": "Bogus"}],
        }
        with pytest.raises(ValidationError) as info:
            SlayerDataset.model_validate(raw)
        assert any(e["loc"] and e["loc"][-1] == "datatype" for e in info.value.errors()), info.value.errors()
        assert "trues" in str(info.value)

    @pytest.mark.parametrize("bogus", [[], {}])
    def test_a_non_string_declared_datatype_is_refused_naming_the_aggregation(self, bogus: Any) -> None:
        with pytest.raises(ValidationError) as info:
            Aggregation.model_validate({"name": "trues", "expression": "SUM({value})", "datatype": bogus})
        assert "trues" in str(info.value)

    async def test_an_invalid_declared_datatype_is_refused_by_the_mcp_save(self) -> None:
        aggregation = {"name": "trues", "expression": "COUNT(CASE WHEN {value} THEN 1 END)", "datatype": "Bogus"}
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            server = create_mcp_server(storage=storage)
            try:
                blocks, _ = cast(Any, await server.call_tool(name="create_dataset", arguments={
                    "name": "orders", "sql_table": "orders", "data_source": DS,
                    "fields": [{"name": "flag", "datatype": "Boolean"}], "aggregations": [aggregation]}))
            finally:
                await getattr(server, "_slayer_engine").aclose()
            assert await storage.get_dataset("orders", data_source=DS) is None
        assert "trues" in blocks[0].text, blocks[0].text
        assert "Bogus" in blocks[0].text, blocks[0].text

    async def test_a_declared_datatype_is_saved_through_mcp(self) -> None:
        aggregation = {"name": "trues", "expression": "COUNT(CASE WHEN {value} THEN 1 END)", "datatype": "Float"}
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            server = create_mcp_server(storage=storage)
            try:
                await server.call_tool(name="create_dataset", arguments={
                    "name": "orders", "sql_table": "orders", "data_source": DS,
                    "fields": [{"name": "flag", "datatype": "Boolean"}], "aggregations": [aggregation]})
            finally:
                await getattr(server, "_slayer_engine").aclose()
            loaded = await storage.get_dataset("orders", data_source=DS)
        assert loaded is not None
        assert loaded.aggregations[0].datatype is DOUBLE

    async def test_round_trips_through_storage(self) -> None:
        declared = orders(aggregations=custom_aggregations(trues_datatype="Float"))
        with tempfile.TemporaryDirectory() as tmp:
            storage = YAMLStorage(base_dir=tmp)
            await storage.save_dataset(declared)
            loaded = await storage.get_dataset("orders", data_source=DS)
        assert loaded is not None
        assert {a.name: a.datatype for a in loaded.aggregations}["trues"] is DOUBLE

    async def test_round_trips_through_the_ossie_export(self, tmp_path: Path) -> None:
        env = await make_env(tmp_path)
        trues = Aggregation.model_validate(
            {"name": "trues", "expression": "COUNT(CASE WHEN {value} THEN 1 END)", "datatype": "Float"})
        result = await export_datasets(env, osi_customers(), osi_orders(aggregations=[trues]))
        exported = payload(dataset(result.document, "orders"))["aggregations"]
        assert {a["name"]: a.get("datatype") for a in exported}["trues"] == "Float"
        target, _ = await reimport(env, result.document)
        loaded = await stored(target, "orders")
        assert {a.name: a.datatype for a in loaded.aggregations}["trues"] is DOUBLE


class TestUnknownFunctionFallsBackToTheInputType:
    @pytest.mark.parametrize(("expr", "expected"), [
        ("custom_pick(orders.order_date)", DATE),
        ("custom_pick(orders.flag)", BOOL),
        ("custom_pick(orders.*)", INT),
    ])
    def test_input_type(self, expr: str, expected: DataType) -> None:
        assert plan_type({"metrics": [m(expr)]}) is expected


# --------------------------------------------------------------------------- #
# Facade agreement (formula-templates "The facade agrees"; boolean-inputs "Facade agrees with the engine").
# --------------------------------------------------------------------------- #
def _facts(*aggregations: Aggregation) -> SlayerDataset:
    return SlayerDataset(
        name="facts", sql_table="facts", data_source=DS, default_time_dimension="at",
        fields=[
            DatasetField(name="id", datatype=INT, primary_key=True),
            DatasetField(name="flag", datatype=BOOL),
            DatasetField(name="qty", datatype=INT),
            DatasetField(name="price", datatype=DOUBLE),
            DatasetField(name="label", datatype=DataType.STRING),
            DatasetField(name="at", datatype=DataType.DATETIME),
        ],
        aggregations=list(aggregations),
    )


def _assert_facade_agrees(model: SlayerDataset, aggs: set[str]) -> int:
    _, metrics = build_local_view(model)
    by_name = {mt.name: mt for mt in metrics}
    checked = 0
    for col in (c for c in model.fields if not c.primary_key):
        for agg in sorted(aggs):
            metric = by_name.get(f"{col.name}_{agg}")
            if metric is None:
                continue
            planned = plan_type({"metrics": [m(f"{agg}(facts.{col.name})")]}, datasets=[model], population="facts")
            assert metric.data_type == planned, (col.name, agg, metric.data_type, planned)
            checked += 1
    return checked


class TestFacadeAgreement:
    def test_custom_aggregations(self) -> None:
        trues, my_avg = (a for a in custom_aggregations() if a.name in ("trues", "my_avg"))
        model = _facts(trues, my_avg)
        assert _assert_facade_agrees(model, {"trues", "my_avg"}) > 0
        _, metrics = build_local_view(model)
        types = {mt.name: mt.data_type for mt in metrics}
        assert (types["flag_trues"], types["qty_my_avg"]) == (INT, DOUBLE)

    def test_built_in_and_custom_aggregations_over_every_column_type(self) -> None:
        model = _facts(Aggregation(name="wsum", expression="SUM({value})"))
        aggs = set().union(*DEFAULT_AGGREGATIONS_BY_TYPE.values()) | {"wsum"}
        assert _assert_facade_agrees(model, aggs) > 0
        _, metrics = build_local_view(model)
        assert {mt.name: mt.data_type for mt in metrics}["flag_wsum"] is INT

    def test_an_overriding_formulas_parameter_default_enters_the_inference(self) -> None:
        weighted = Aggregation(name="sum", expression="SUM({value} * {weight})",
                               params=[AggregationParam(name="weight", sql="price")])
        model = _facts(weighted)
        assert _assert_facade_agrees(model, {"sum"}) > 0
        _, metrics = build_local_view(model)
        assert {mt.name: mt.data_type for mt in metrics}["qty_sum"] is DOUBLE

    def test_a_default_reading_a_related_dataset_leaves_the_type_unknown(self) -> None:
        weighted = Aggregation(name="sum", expression="SUM({value} * {weight})",
                               params=[AggregationParam(name="weight", sql="customers.weight")])
        _, metrics = build_local_view(_facts(weighted))
        assert {mt.name: mt.data_type for mt in metrics}["qty_sum"] is None

    def test_a_built_in_overrides_declared_datatype_agrees_with_the_planner(self) -> None:
        model = _facts(Aggregation(name="sum", datatype=DOUBLE))
        assert _assert_facade_agrees(model, {"sum"}) > 0
        _, metrics = build_local_view(model)
        assert {mt.name: mt.data_type for mt in metrics}["qty_sum"] is INT

    def test_a_declared_datatype_wins_over_an_unbindable_default(self) -> None:
        weighted = Aggregation(name="sum", expression="SUM({value} * {weight})", datatype=DataType.DECIMAL,
                               params=[AggregationParam(name="weight", sql="customers.weight")])
        _, metrics = build_local_view(_facts(weighted))
        assert {mt.name: mt.data_type for mt in metrics}["qty_sum"] is DataType.DECIMAL


# --------------------------------------------------------------------------- #
# aggregations/boolean-inputs › A custom aggregation over a boolean takes its formula's type
# --------------------------------------------------------------------------- #
class TestCustomAggregationOverABoolean:
    def test_takes_its_formulas_type_not_boolean(self) -> None:
        assert _types("trues(orders.flag)") == {INT}


# --------------------------------------------------------------------------- #
# The template typer: COALESCE over a boolean in a numeric position, literal-only bindings, cache keys.
# --------------------------------------------------------------------------- #
class TestTemplateTyper:
    def test_a_boolean_through_coalesce_in_a_sum_is_read_as_its_integer(self) -> None:
        typed = template_result_type(template="SUM(COALESCE({value}, FALSE))", bindings={"value": BOOL}, dialect="postgres")
        assert typed is INT

    def test_a_standalone_coalesce_over_a_boolean_stays_boolean(self) -> None:
        assert template_result_type(template="COALESCE({value}, FALSE)", bindings={"value": BOOL}, dialect="postgres") is BOOL

    def test_literal_only_bindings_are_typed(self) -> None:
        assert template_result_type(template="SUM({value})", bindings={"value": Decimal(1)}, dialect="postgres") is INT

    def test_equal_bindings_of_different_kinds_do_not_share_a_cached_type(self) -> None:
        template = "{value}"
        assert template_result_type(template=template, bindings={"value": True}, dialect="postgres") is BOOL
        assert template_result_type(template=template, bindings={"value": Decimal(1)}, dialect="postgres") is INT

    def test_a_boolean_coalesce_with_a_fractional_fallback_stays_fractional(self) -> None:
        typed = template_result_type(template="SUM(COALESCE({value}, 0.5))", bindings={"value": BOOL}, dialect="postgres")
        assert typed is DOUBLE

    @pytest.mark.parametrize(("template", "value", "expected"), [
        ("SUM(COALESCE({value} = 1, 0.5))", INT, DOUBLE),
        ("SUM({value} = 1)", INT, INT),
        ("SUM(COALESCE(TRUE, 0.5) * {value})", INT, DOUBLE),
        ("BOOL_AND({value} = 1) * 1", INT, INT),
        ("SUM({value}) * 1.0 / COUNT(*)", BOOL, DOUBLE),
    ])
    def test_a_boolean_expression_in_a_numeric_position_is_read_as_its_integer(
        self, template: str, value: DataType, expected: DataType,
    ) -> None:
        assert template_result_type(template=template, bindings={"value": value}, dialect="postgres") is expected
