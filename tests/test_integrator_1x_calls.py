"""A known 1.8.4 integrator's SLayer calls keep working on 2.x, each with its deprecation and its 1.8.4 value.

Spec: compatibility/integrator-1x-calls. Expected values were recorded by running the same calls on
``motley-slayer==1.8.4``.
"""

from __future__ import annotations

import pickle
import warnings
from decimal import Decimal
from typing import Any

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

import slayer.core.syntax as syntax
from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from tests._integrator_1x import (
    DATA_SOURCE, NODE_CLASSES, deprecations, filter_tokens, legacy, node_classes, one_deprecation, quiet,
    v15_models, walk,
)

KEPT_NAMES = {
    "slayer.core.formula": {
        name: "slayer.core.syntax"
        for name in ("parse_formula", "AggregatedMeasureRef", "ArithmeticField", "MixedArithmeticField", "TransformField")
    },
    "slayer.engine.syntax": {
        **{name: f"slayer.core.syntax.{name}" for name in NODE_CLASSES},
        "parse_filter_expr": "slayer.core.syntax.parse_expr",
    },
    "slayer.engine.schema_drift": {
        "EditModelDelete": "slayer.live_schema.schema_drift.EditDatasetDelete",
        "WholeModelDelete": "slayer.live_schema.schema_drift.WholeDatasetDelete",
        "LiveTable": "slayer.live_schema.schema_drift.LiveTable",
        "compute_datasource_drops": "slayer.live_schema.schema_drift.compute_datasource_drops",
        "data_type_bucket": "slayer.live_schema.schema_drift.data_type_bucket",
        "diff_sql_table_model": "slayer.live_schema.schema_drift.diff_sql_table_dataset",
        "retype_findings": "slayer.live_schema.schema_drift.retype_findings",
    },
    "slayer.sql.column_expansion": {"resolve_ref_target": "slayer.sql.field_expansion.resolve_ref_target"},
}


def _formula(name: str) -> Any:
    return legacy("slayer.core.formula", name)


def _aggregated_refs(spec: Any) -> list[tuple[str, str]]:
    """The integrator's walk over a parsed formula's aggregated references."""
    if isinstance(spec, _formula("AggregatedMeasureRef")):
        return [(spec.measure_name, spec.aggregation_name)]
    if isinstance(spec, _formula("TransformField")):
        return _aggregated_refs(spec.inner)
    if isinstance(spec, _formula("ArithmeticField")):
        return [(r.measure_name, r.aggregation_name) for r in spec.agg_refs.values()]
    if isinstance(spec, _formula("MixedArithmeticField")):
        found = [(r.measure_name, r.aggregation_name) for r in spec.agg_refs.values()]
        for _, transform in spec.sub_transforms:
            found.extend(_aggregated_refs(transform))
        return found
    raise AssertionError(f"unknown formula shape {type(spec).__name__}")


def _parse_formula(formula: str, **kwargs: Any) -> Any:
    with quiet():
        parse_formula = _formula("parse_formula")
    return parse_formula(formula, **kwargs)


# --- Each kept form warns with its replacement ---------------------------------------------------------------------


def test_a_kept_form_names_its_replacement() -> None:
    with one_deprecation(naming="is_base_field_sql") as record:
        assert legacy("slayer.core.models", "is_base_column_sql")("amount") is True
    assert "3.0" in str(record[0].message)


# --- Four moved modules keep the integrator's names ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("module", "name", "new_home"),
    [(module, name, home) for module, names in KEPT_NAMES.items() for name, home in names.items()],
)
def test_the_integrators_imports_resolve(module: str, name: str, new_home: str) -> None:
    with one_deprecation(naming=new_home):
        assert legacy(module, name) is not None


@pytest.mark.parametrize("name", NODE_CLASSES)
def test_served_node_classes_are_the_parsers_classes(name: str) -> None:
    with quiet():
        assert legacy("slayer.engine.syntax", name) is getattr(syntax, name)


# --- Formulas parse into the 1.x result shape ----------------------------------------------------------------------


def test_a_sibling_ratio_expands_into_its_constituents() -> None:
    spec = _parse_formula("revenue / order_count", named_measures={"revenue": "sum(amount)", "order_count": "count(*)"})
    assert isinstance(spec, _formula("ArithmeticField"))
    assert _aggregated_refs(spec) == [("amount", "sum"), ("*", "count")]


def test_a_custom_aggregation_is_accepted() -> None:
    spec = _parse_formula("spread(amount)", extra_agg_names=frozenset({"spread"}))
    assert isinstance(spec, _formula("AggregatedMeasureRef"))
    assert (spec.measure_name, spec.aggregation_name) == ("amount", "spread")


def test_an_undeclared_custom_aggregation_is_refused() -> None:
    with pytest.raises(ValueError):
        _parse_formula("spread(amount)")


def test_a_colon_spelling_is_refused_as_a_value_error() -> None:
    with pytest.raises(ValueError, match=r"sum\(amount\)"):
        _parse_formula("amount:sum")


def test_a_cyclic_sibling_chain_raises_value_error() -> None:
    with pytest.raises(ValueError, match="cyclic"):
        _parse_formula("a / 2", named_measures={"a": "b + sum(amount)", "b": "a * 2"})


def test_a_rank_without_direction_is_refused_as_a_value_error() -> None:
    with pytest.raises(ValueError):
        _parse_formula("rank(sum(amount))")


@pytest.mark.parametrize(
    ("formula", "named", "shape", "refs"),
    [
        ("sum(amount) / 2 + cumsum(sum(amount))", {}, "MixedArithmeticField", [("amount", "sum"), ("amount", "sum")]),
        ("cumsum(revenue)", {"revenue": "sum(amount)"}, "TransformField", [("amount", "sum")]),
    ],
)
def test_transform_shapes_match_1_8_4(formula: str, named: dict[str, str], shape: str, refs: list[tuple[str, str]]) -> None:
    spec = _parse_formula(formula, named_measures=named)
    assert isinstance(spec, _formula(shape))
    assert _aggregated_refs(spec) == refs


#: Each fixture measure parsed as the integrator parses it (sibling measures named, custom aggregations extra).
FIXTURE_FORMULAS = {
    "revenue": ("AggregatedMeasureRef", [("amount", "sum")]),
    "order_count": ("AggregatedMeasureRef", [("*", "count")]),
    "aov": ("ArithmeticField", [("amount", "sum"), ("*", "count")]),
    "paid_revenue": ("AggregatedMeasureRef", [("paid_amount", "sum")]),
    "customer_count": ("AggregatedMeasureRef", [("customers.email", "count_distinct")]),
    "amount_spread": ("AggregatedMeasureRef", [("amount", "spread")]),
}


@pytest.mark.parametrize("measure", list(FIXTURE_FORMULAS))
def test_every_fixture_measure_parses_as_in_1_8_4(measure: str) -> None:
    with quiet():
        orders = v15_models()["orders"]
        named = {m.name: m.formula for m in orders.measures if m.name != measure}
        custom = frozenset(a.name for a in orders.aggregations)
        formula = next(m.formula for m in orders.measures if m.name == measure)
    spec = _parse_formula(formula, extra_agg_names=custom, named_measures=named)
    shape, refs = FIXTURE_FORMULAS[measure]
    assert isinstance(spec, _formula(shape))
    assert _aggregated_refs(spec) == refs


# --- Filters parse into the 1.x tree shape -------------------------------------------------------------------------

#: The structural fields 1.8.4 declared per node class (diagnostic metadata such as ``written`` left out).
FIELDS_1_8_4 = {
    "AggCall": ("source", "agg", "args", "kwargs"), "Arith": ("op", "left", "right"), "ArrayLit": ("elements",),
    "BoolOp": ("op", "operands"), "Cmp": ("op", "left", "right"), "DottedRef": ("parts",), "Literal": ("value",),
    "Placeholder": ("name",), "Ref": ("name",), "ScalarCall": ("name", "args"), "StarSource": (),
    "TransformCall": ("op", "input", "args", "kwargs"), "TupleLit": ("elements",), "UnaryOp": ("op", "operand"),
}


def _shape(node: object) -> Any:
    if isinstance(node, BaseModel):
        name = type(node).__name__
        return [name, {f: _shape(getattr(node, f)) for f in FIELDS_1_8_4[name]}]
    if isinstance(node, (tuple, list)):
        return [_shape(x) for x in node]
    if isinstance(node, Decimal):
        return ["Decimal", str(node)]
    return node


def _ref(name: str) -> list[Any]:
    return ["Ref", {"name": name}]


def _dotted(*parts: str) -> list[Any]:
    return ["DottedRef", {"parts": list(parts)}]


def _lit(value: Any) -> list[Any]:
    return ["Literal", {"value": ["Decimal", value] if isinstance(value, int) else value}]


def _agg(source: list[Any], agg: str, args: list[Any] | None = None, kwargs: list[Any] | None = None) -> list[Any]:
    return ["AggCall", {"source": source, "agg": agg, "args": args or [], "kwargs": kwargs or []}]


def _cmp(op: str, left: list[Any], right: list[Any]) -> list[Any]:
    return ["Cmp", {"op": op, "left": left, "right": right}]


def _transform(op: str, input_: list[Any], args: list[Any] | None = None, kwargs: list[Any] | None = None) -> list[Any]:
    return ["TransformCall", {"op": op, "input": input_, "args": args or [], "kwargs": kwargs or []}]


#: Filters the integrator walks, with their 1.8.4 trees and the tokens its walk found.
FILTERS_1_8_4: dict[str, tuple[list[Any], list[str]]] = {
    "status = 'paid'": (_cmp("==", _ref("status"), _lit("paid")), ["orders.status"]),
    "customers.country IN ('DE', 'FR')": (
        _cmp("in", _dotted("customers", "country"), ["TupleLit", {"elements": [_lit("DE"), _lit("FR")]}]),
        ["customers.country"],
    ),
    "sum(amount) > 100": (_cmp(">", _agg(_ref("amount"), "sum"), _lit(100)), ["orders.amount"]),
    "count(customers.id) > 1": (_cmp(">", _agg(_dotted("customers", "id"), "count"), _lit(1)), ["customers.id"]),
    "contains(tags, 'dbt')": (
        ["ScalarCall", {"name": "contains", "args": [_ref("tags"), _lit("dbt")]}], ["orders.tags"],
    ),
    "tags.value = 'dbt'": (_cmp("==", _dotted("tags", "value"), _lit("dbt")), ["orders.tags.value"]),
    "rank(sum(amount), partition_by=customers.email) <= 1": (
        _cmp("<=", _transform("rank", _agg(_ref("amount"), "sum"),
                              kwargs=[["partition_by", _dotted("customers", "email")]]), _lit(1)),
        ["orders.amount", "customers.email"],
    ),
    "aov > 10": (_cmp(">", _ref("aov"), _lit(10)), ["orders.amount"]),
    "NOT (status = 'cancelled')": (
        ["UnaryOp", {"op": "not", "operand": _cmp("==", _ref("status"), _lit("cancelled"))}], ["orders.status"],
    ),
    "amount * 2 >= 10 AND status <> 'x'": (
        ["BoolOp", {"op": "and", "operands": [
            _cmp(">=", ["Arith", {"op": "*", "left": _ref("amount"), "right": _lit(2)}], _lit(10)),
            _cmp("!=", _ref("status"), _lit("x")),
        ]}],
        ["orders.amount", "orders.status"],
    ),
    "status IS NULL": (_cmp("is", _ref("status"), _lit(None)), ["orders.status"]),
    "-amount < 0": (_cmp("<", ["UnaryOp", {"op": "-", "operand": _ref("amount")}], _lit(0)), ["orders.amount"]),
    "count(*) > 0": (_cmp(">", _agg(["StarSource", {}], "count"), _lit(0)), []),
    "{s} = 1": (_cmp("==", ["Placeholder", {"name": "s"}], _lit(1)), []),
    "tags = ['a', 'b']": (_cmp("==", _ref("tags"), ["ArrayLit", {"elements": [_lit("a"), _lit("b")]}]), ["orders.tags"]),
    # Named arguments: 2.0 parses them as their own node class; 1.8.4 kept them as (name, value) pairs.
    "rank(sum(amount), direction='desc') <= 3": (
        _cmp("<=", _transform("rank", _agg(_ref("amount"), "sum"), kwargs=[["direction", _lit("desc")]]), _lit(3)),
        ["orders.amount"],
    ),
    "time_shift(sum(amount), -1, granularity='month') > 0": (
        _cmp(">", _transform("time_shift", _agg(_ref("amount"), "sum"),
                             args=[["UnaryOp", {"op": "-", "operand": _lit(1)}]],
                             kwargs=[["granularity", _lit("month")]]), _lit(0)),
        ["orders.amount"],
    ),
    "last(amount, order_by=created_at) > 1": (
        _cmp(">", _agg(_ref("amount"), "last", kwargs=[["order_by", _ref("created_at")]]), _lit(1)),
        ["orders.amount", "orders.created_at"],
    ),
    "sum(amount, filter=status = 'paid') > 1": (
        _cmp(">", _agg(_ref("amount"), "sum", kwargs=[["filter", _cmp("==", _ref("status"), _lit("paid"))]]), _lit(1)),
        ["orders.amount", "orders.status"],
    ),
    # date_trunc: its own node class in 2.0, an aggregation-call shape in 1.8.4.
    "date_trunc('month', created_at) = '2024-01-01'": (
        _cmp("==", _agg(_lit("month"), "date_trunc", args=[_ref("created_at")]), _lit("2024-01-01")),
        ["orders.created_at"],
    ),
}

#: The integrator's filters named in the spec scenario.
SCENARIO_FILTERS = [
    "status = 'paid'", "customers.country IN ('DE', 'FR')", "sum(amount) > 100", "count(customers.id) > 1",
    "contains(tags, 'dbt')", "tags.value = 'dbt'", "rank(sum(amount), partition_by=customers.email) <= 1",
]


def _parse_filter(text: str) -> Any:
    with quiet():
        parse_filter_expr = legacy("slayer.engine.syntax", "parse_filter_expr")
    return parse_filter_expr(text)


def test_the_integrators_filters_walk_to_their_references() -> None:
    models = v15_models()
    with quiet():
        found = {text: filter_tokens(text, source="orders", models=models) for text in SCENARIO_FILTERS}
    assert found == {text: FILTERS_1_8_4[text][1] for text in SCENARIO_FILTERS}


@pytest.mark.parametrize("text", list(FILTERS_1_8_4))
def test_a_filter_tree_has_its_1_8_4_shape(text: str) -> None:
    tree = _parse_filter(text)
    with quiet():
        known = node_classes()
    assert all(type(node).__name__ in NODE_CLASSES for node in walk(tree, known=known))
    assert _shape(tree) == FILTERS_1_8_4[text][0]


@pytest.mark.parametrize("text", list(FILTERS_1_8_4))
def test_a_filter_reads_the_references_1_8_4_read(text: str) -> None:
    with quiet():
        assert filter_tokens(text, source="orders", models=v15_models()) == FILTERS_1_8_4[text][1]


@pytest.mark.parametrize(
    "text",
    [
        "CAST(amount AS INT) > 1", "TRY_CAST(amount AS INT) > 1", "created_at >= DATE '2024-01-01'",
        "created_at >= TIMESTAMP '2024-01-01 10:00:00'", "status BETWEEN 'a' AND 'b'", "status ~ 'a.*'",
        "status = :s", "x ==",
    ],
)
def test_a_filter_1_8_4_refused_raises_value_error(text: str) -> None:
    with pytest.raises(ValueError):
        _parse_filter(text)


def test_a_filter_tree_is_built_from_the_served_classes() -> None:
    tree = _parse_filter("sum(amount) > 100")
    with quiet():
        assert isinstance(tree, legacy("slayer.engine.syntax", "Cmp"))
        assert isinstance(tree.left, legacy("slayer.engine.syntax", "AggCall"))


# --- Removed helpers and renamed keywords keep working -------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"), [("status = {s}", True), ("status = '{s}'", True), ("status = 'paid'", False)],
)
def test_variable_syntax_is_detected_as_in_1_8_4(text: str, expected: bool) -> None:
    with one_deprecation(naming="slayer"):
        assert legacy("slayer.core.query", "has_variable_syntax")(text) is expected


@pytest.mark.parametrize(
    ("sql", "expected"), [("amount", True), ('"Amount"', True), ("UPPER(status)", False), (None, True)],
)
def test_is_base_column_sql_matches_1_8_4(sql: str | None, expected: bool) -> None:
    with one_deprecation(naming="is_base_field_sql"):
        assert legacy("slayer.core.models", "is_base_column_sql")(sql) is expected


@pytest.mark.parametrize(("sql", "expected"), [("amount", "amount"), ('"Amount"', "Amount"), (None, "n")])
def test_physical_column_sql_matches_1_8_4(sql: str | None, expected: str) -> None:
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        assert legacy("slayer.core.models", "physical_column_sql")(sql=sql, name="n") == expected
    assert deprecations(record) == []


def test_is_identifier_takes_its_old_keywords() -> None:
    with quiet():
        orders = v15_models()["orders"]
        columns = orders.columns
    is_identifier = legacy("slayer.core.models", "is_identifier")
    with one_deprecation(naming="field"):
        found = {c.name: is_identifier(column=c, columns=columns) for c in columns[:1]}
    with quiet():
        found |= {c.name: is_identifier(column=c, columns=columns) for c in columns[1:]}
    assert found == {
        "id": True, "customer_id": False, "status": False, "amount": False, "paid_amount": False,
        "status_upper": False, "tags": False, "created_at": False,
    }


def _resolve(qualifiers: tuple[str, ...]) -> Any:
    models = v15_models()
    with quiet():
        resolve_ref_target = legacy("slayer.sql.column_expansion", "resolve_ref_target")
    return resolve_ref_target(qualifiers=qualifiers, source_model=models["orders"], models_by_name=dict(models))


def test_a_list_element_resolves_through_its_old_keywords() -> None:
    target = _resolve(("orders", "tags"))
    assert tuple(target.list_of) == ("orders", "tags")
    with quiet():
        assert list(target.measures) == []


@pytest.mark.parametrize(
    ("qualifiers", "name", "measures"),
    [
        (("customers",), "customers", ["customer_total"]),
        (("orders", "customers"), "customers", ["customer_total"]),
        ((), "orders", ["revenue", "order_count", "aov", "paid_revenue", "customer_count", "amount_spread"]),
    ],
)
def test_a_reference_target_resolves_as_in_1_8_4(qualifiers: tuple[str, ...], name: str, measures: list[str]) -> None:
    target = _resolve(qualifiers)
    assert target.name == name and target.list_of is None
    with quiet():
        assert [m.name for m in target.measures] == measures


def test_an_unknown_qualifier_resolves_to_nothing() -> None:
    assert _resolve(("nope",)) is None


# --- Drift entry points take their 1.x keywords and results --------------------------------------------------------


def _drift(name: str) -> Any:
    with quiet():
        return legacy("slayer.engine.schema_drift", name)


def _live_orders(*, drop: tuple[str, ...] = (), retype: dict[str, Any] | None = None) -> Any:
    """The live table the integrator builds from a model's base columns."""
    with quiet():
        orders = v15_models()["orders"]
        is_base = legacy("slayer.core.models", "is_base_column_sql")
        physical = legacy("slayer.core.models", "physical_column_sql")
        columns = {
            physical(sql=c.sql, name=c.name): c.type
            for c in orders.columns
            if is_base(c.sql) and physical(sql=c.sql, name=c.name) not in drop
        }
    return _drift("LiveTable")(columns={**columns, **(retype or {})})


def _drops(live_orders: Any) -> list[Any]:
    models = v15_models()
    names = set(models)
    diff = _drift("diff_sql_table_model")
    with quiet():
        customers_live = _drift("LiveTable")(columns={c.name: c.type for c in models["customers"].columns})
    diffs = {
        "orders": diff(model=models["orders"], live_table=live_orders, available_models_in_ds=names),
        "customers": diff(model=models["customers"], live_table=customers_live, available_models_in_ds=names),
    }
    return _drift("compute_datasource_drops")(models=list(models.values()), sql_table_diffs=diffs, sql_diffs={})


def test_a_dropped_columns_removal_reads_in_1x_names() -> None:
    [drop] = _drops(_live_orders(drop=("amount",)))
    assert isinstance(drop, _drift("EditModelDelete"))
    assert not isinstance(drop, _drift("WholeModelDelete"))
    with quiet():
        assert drop.model_name == "orders"
        assert sorted(drop.remove.columns) == ["amount", "paid_amount"]
        assert sorted(drop.remove.measures) == ["amount_spread", "aov", "paid_revenue", "revenue"]
        assert list(drop.remove.aggregations) == []
        assert list(drop.remove.join_edges) == []
        assert list(drop.remove_filters) == []
        assert sorted(r.target.split(":")[1] for r in drop.reasons) == [
            "amount", "amount_spread", "aov", "paid_amount", "paid_revenue", "revenue",
        ]


def test_each_1x_result_attribute_warns() -> None:
    [drop] = _drops(_live_orders(drop=("amount",)))
    with one_deprecation(naming="dataset_name"):
        assert drop.model_name == "orders"
    with one_deprecation(naming="fields"):
        assert "amount" in drop.remove.columns
    with one_deprecation(naming="metrics"):
        assert "revenue" in drop.remove.measures
    with one_deprecation(naming="relationship_edges"):
        assert list(drop.remove.join_edges) == []


def test_a_dropped_join_key_reads_its_1x_edge() -> None:
    [drop] = _drops(_live_orders(drop=("customer_id",)))
    with quiet():
        assert drop.model_name == "orders"
        assert sorted(drop.remove.columns) == ["customer_id"]
        edges = [edge.model_dump(mode="json") for edge in drop.remove.join_edges]
    assert edges == [{"target_model": "customers", "name": None, "join_pairs": [["customer_id", "id"]]}]


def test_a_dropped_filter_column_removes_the_filter() -> None:
    [drop] = _drops(_live_orders(drop=("status",)))
    with quiet():
        assert sorted(drop.remove.columns) == ["status", "status_upper"]
        assert list(drop.remove_filters) == ["status <> 'cancelled'"]


def test_a_vanished_table_is_a_whole_model_drop() -> None:
    models = v15_models()
    diffs = {"orders": _drift("diff_sql_table_model")(model=models["orders"], live_table=None,
                                                      available_models_in_ds=set(models))}
    [drop] = _drift("compute_datasource_drops")(models=list(models.values()), sql_table_diffs=diffs, sql_diffs={})
    assert isinstance(drop, _drift("WholeModelDelete"))
    with quiet():
        assert drop.model_name == "orders"
    assert [(r.target, r.reason) for r in drop.reasons] == [
        ("dataset:orders", "Live table 'orders' not found in datasource 'warehouse'"),
    ]


def test_a_retype_finding_reads_its_column() -> None:
    models = v15_models()
    findings = _drift("retype_findings")(model=models["orders"], live_table=_live_orders(retype={"amount": DataType.STRING}))
    with quiet():
        assert [(f.column, f.remedy_kind) for f in findings] == [("amount", "set_type"), ("paid_amount", "set_type")]
    with one_deprecation(naming="field"):
        assert findings[0].column == "amount"
    assert findings[0].stored_type is DataType.FLOAT and findings[0].live_type is DataType.STRING
    with quiet():
        assert findings[0].model_name == "orders"


def test_data_type_bucket_reads_legacy_members() -> None:
    bucket = _drift("data_type_bucket")
    with quiet():
        assert bucket(getattr(DataType, "INT")) == bucket(DataType.INTEGER)
        assert bucket(getattr(DataType, "TEXT")) == bucket(DataType.STRING)


# --- Legacy DataType names resolve to their 2.0 members ------------------------------------------------------------

LEGACY_DATATYPES = {"INT": DataType.INTEGER, "DOUBLE": DataType.FLOAT, "TEXT": DataType.STRING, "TIMESTAMP": DataType.DATETIME}


@pytest.mark.parametrize(("legacy_name", "member"), list(LEGACY_DATATYPES.items()))
def test_a_legacy_name_is_the_2_0_member(legacy_name: str, member: DataType) -> None:
    with one_deprecation(naming=f"DataType.{member.name}"):
        assert getattr(DataType, legacy_name) is member
    assert legacy_name not in DataType.__members__


def test_each_legacy_lookup_warns_once() -> None:
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        for _ in range(3):
            assert getattr(DataType, "INT") is DataType.INTEGER
    assert len(deprecations(record)) == 3


def test_legacy_names_are_not_members() -> None:
    members = list(DataType)
    assert set(LEGACY_DATATYPES).isdisjoint(m.name for m in members)
    assert set(LEGACY_DATATYPES).isdisjoint(DataType.__members__)
    with pytest.raises(ValidationError):
        TypeAdapter(DataType).validate_python("INT")
    assert set(LEGACY_DATATYPES).isdisjoint(TypeAdapter(DataType).json_schema()["enum"])
    assert pickle.loads(pickle.dumps(DataType.INTEGER)) is DataType.INTEGER
    with quiet():
        assert pickle.loads(pickle.dumps(getattr(DataType, "INT"))) is DataType.INTEGER


@pytest.mark.parametrize("name", ["ARRAY", "BOOLEAN", "DATE"])
def test_unchanged_members_keep_working(name: str) -> None:
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        assert isinstance(getattr(DataType, name), DataType)
    assert deprecations(record) == []


# --- Model objects read through their 1.x names --------------------------------------------------------------------


def test_a_v15_model_reads_through_its_old_names() -> None:
    assert isinstance(v15_models()["orders"], SlayerDataset)
    orders = v15_models()["orders"]
    with one_deprecation(naming="relationships"):
        joins = orders.joins
    with one_deprecation(naming="to"):
        assert joins[0].target_model == "customers"
    with one_deprecation(naming="column_pairs"):
        assert joins[0].join_pairs == [("customer_id", "id")]
    with quiet():
        assert joins[0].cardinality.value == "many_to_one"
        assert joins[0].join_type.value == "left"
    with one_deprecation(naming="fields"):
        columns = orders.columns
    assert [c.name for c in columns] == [
        "id", "customer_id", "status", "amount", "paid_amount", "status_upper", "tags", "created_at",
    ]
    with one_deprecation(naming="metrics"):
        measures = orders.measures
    with one_deprecation(naming="expression"):
        assert measures[0].formula == "sum(amount)"
    with quiet():
        assert [(m.name, m.formula) for m in measures] == [
            ("revenue", "sum(amount)"), ("order_count", "count(*)"), ("aov", "revenue / order_count"),
            ("paid_revenue", "sum(paid_amount)"), ("customer_count", "count_distinct(customers.email)"),
            ("amount_spread", "spread(amount)"),
        ]
    with one_deprecation(naming="datatype"):
        assert columns[3].type is DataType.FLOAT
    with one_deprecation(naming="get_field"):
        assert orders.get_column("amount").name == "amount"
    with one_deprecation(naming="awaits_fields"):
        assert orders.awaits_columns is False
    with quiet():
        assert [(c.name, c.filter, c.primary_key, c.unique, c.allowed_aggregations) for c in columns] == [
            ("id", None, True, False, None), ("customer_id", None, False, False, None),
            ("status", None, False, False, None), ("amount", None, False, False, None),
            ("paid_amount", "status = 'paid'", False, False, None), ("status_upper", None, False, False, None),
            ("tags", None, False, False, None), ("created_at", None, False, False, None),
        ]
        assert columns[6].element_type is DataType.STRING
        assert [(a.name, a.formula) for a in orders.aggregations] == [("spread", "MAX({value}) - MIN({value})")]
    assert orders.default_time_dimension == "created_at"
    assert orders.effective_default_time_dimension == "created_at"


def test_join_key_error_takes_its_old_keywords() -> None:
    models = v15_models()
    with quiet():
        columns = models["customers"].columns
    join_key_error = legacy("slayer.core.models", "join_key_error")
    assert join_key_error(model="orders", target="customers", key="id", side="customers", columns=columns) is None
    assert join_key_error(model="orders", target="customers", key="nope", side="customers", columns=columns) is not None


def test_aggregation_tables_keep_their_1_8_4_values() -> None:
    normalize = legacy("slayer.core.enums", "normalize_aggregation_name")
    assert {n: normalize(n) for n in ("sum", "count_distinct", "avg", "countd", "max")} == {
        "sum": "sum", "count_distinct": "count_distinct", "avg": "avg", "countd": "count_distinct", "max": "max",
    }
    assert sorted(legacy("slayer.core.models", "PRIMARY_KEY_AGGREGATIONS")) == [
        "collect", "count", "count_distinct", "count_distinct_approx", "max", "min",
    ]
    defaults = legacy("slayer.core.models", "DEFAULT_AGGREGATIONS_BY_TYPE")
    with quiet():
        assert sorted(defaults.get(DataType.ARRAY)) == ["count", "count_distinct"]  # 2.0 value; 1.8.4 had count only
        assert "sum" in defaults.get(DataType.INTEGER)


def test_time_granularity_keeps_its_1_8_4_members() -> None:
    granularity = legacy("slayer.core.enums", "TimeGranularity")
    assert {m.name: m.value for m in granularity} == {
        "SECOND": "second", "MINUTE": "minute", "HOUR": "hour", "DAY": "day", "WEEK": "week",
        "WEEK_SUNDAY": "week_sunday", "MONTH": "month", "QUARTER": "quarter", "YEAR": "year",
    }


def test_render_side_imports_keep_working() -> None:
    config = legacy("slayer.core.models", "DatasourceConfig")(name=DATA_SOURCE, type="duckdb")
    assert config.type == "duckdb"
    dialect = legacy("slayer.sql.dialects", "dialect_for_ds_type")("bigquery")
    assert dialect.emit_alias("orders.status") == "orders___status"
    assert legacy("slayer.sql.scope", "__name__") == "slayer.sql.scope"


def test_a_column_is_the_dataset_field() -> None:
    with one_deprecation(naming="DatasetField"):
        assert legacy("slayer.core.models", "Column") is DatasetField
