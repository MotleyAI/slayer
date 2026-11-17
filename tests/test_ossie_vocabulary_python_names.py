"""Public Python names follow the vocabulary; until 3.0 every name on the curated list of pre-2.0 names reaches its
replacement with one ``FutureWarning`` naming it and SLayer 3.0.

The pre-2.0 names below are deliberate: keep this file out of the vocabulary respelling pass.

Spec: compatibility/ossie-vocabulary › Python names follow the vocabulary.
"""

from __future__ import annotations

import importlib
import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict

from slayer.client.slayer_client import SlayerClient
from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasetMetric,
    DatasetRelationship,
    SlayerDataset,
    SourceDatasetOrigin,
)
from slayer.core.policy import FieldFilterRuleset, RelationshipFilterRule, RelationshipFilterRuleset
from slayer.core.query import DatasetExtension, FieldRef, OrderItem, QueryRefinement, SlayerQuery, TimeDimension
from slayer.core.recommend import CandidateCoverage, PopulationRecommendation
from slayer.core.warnings import (
    AssociatedWarningPayload,
    BroadcastGrainWarningPayload,
    BrokenDependentWarning,
    DegenerateReaggregationWarningPayload,
    ExtensionFieldInlinedWarning,
    LibraryNameShadowWarning,
    NameCollisionWarning,
    NarrowedAccessWarning,
    SemiJoinPushedWarningPayload,
    SpineRelationshipDroppedWarning,
    UnrelatedDatasetWarningPayload,
    WholePeriodsNonNestingWarningPayload,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.response_meta import FieldMetadata, ResponseAttributes
from slayer.storage.yaml_storage import YAMLStorage
from tests._deprecated_input_helpers import deprecations, payload_of, recorded
from tests._ossie_vocabulary_fixtures import shop_customers, shop_orders, shop_store, vocab_engine
from tests._stored_upgrade_fixtures import DS

_ERRORS = {
    "AmbiguousJoinPathError": "AmbiguousRelationshipPathError",
    "AmbiguousModelError": "AmbiguousDatasetError",
    "CanonicalAliasShadowsColumnError": "CanonicalAliasShadowsFieldError",
    "CircularJoinPathError": "CircularRelationshipPathError",
    "ColumnCycleError": "DatasetFieldCycleError",
    "ColumnSqlStatementError": "FieldSqlStatementError",
    "DerivedColumnAmbiguousError": "DerivedFieldAmbiguousError",
    "DerivedColumnCircularError": "DerivedFieldCircularError",
    "DerivedColumnFanningError": "DerivedFieldFanningError",
    "DuplicateMeasureNameError": "DuplicateMetricNameError",
    "JoinKeyError": "RelationshipKeyError",
    "JoinTargetCaseError": "RelationshipTargetCaseError",
    "MeasureCycleError": "MetricCycleError",
    "MeasureNameCollidesWithColumnError": "MetricNameCollidesWithFieldError",
    "MeasureRecursionLimitError": "MetricRecursionLimitError",
    "ModelFilterCycleError": "DatasetFilterCycleError",
    "ModelFilterError": "DatasetFilterError",
    "ModelNotFoundError": "DatasetNotFoundError",
    "ModelSqlValidationError": "DatasetSqlValidationError",
    "ReservedModelNameError": "ReservedDatasetNameError",
    "TimeDimensionColumnError": "TimeDimensionFieldError",
    "UnknownJoinTargetError": "UnknownRelationshipTargetError",
    "UnresolvableDimensionJoinError": "UnresolvableDimensionRelationshipError",
    "UnresolvableOrderColumnError": "UnresolvableOrderFieldError",
    "UnsafeJoinInputError": "UnsafeRelationshipInputError",
}

#: (module, pre-2.0 class name) -> replacement name in the same module.
CLASSES: dict[tuple[str, str], str] = {
    ("slayer.core.models", "SlayerModel"): "SlayerDataset",
    ("slayer.core.models", "Column"): "DatasetField",
    ("slayer.core.models", "ModelMeasure"): "DatasetMetric",
    ("slayer.core.models", "ModelJoin"): "DatasetRelationship",
    ("slayer.core.models", "SourceModelOrigin"): "SourceDatasetOrigin",
    ("slayer.core.query", "ColumnRef"): "FieldRef",
    ("slayer.core.query", "ModelExtension"): "DatasetExtension",
    ("slayer.core.query", "ModelVariables"): "DatasetVariables",
    ("slayer.core.query", "ModelMeasure"): "DatasetMetric",
    ("slayer.core.recommend", "RootModelRecommendation"): "PopulationRecommendation",
    ("slayer.core.warnings", "ExtensionColumnInlinedWarning"): "ExtensionFieldInlinedWarning",
    ("slayer.core.warnings", "SlayerExtensionColumnInlinedWarning"): "SlayerExtensionFieldInlinedWarning",
    ("slayer.core.warnings", "SpineJoinDroppedWarning"): "SpineRelationshipDroppedWarning",
    ("slayer.core.warnings", "SlayerSpineJoinDroppedWarning"): "SlayerSpineRelationshipDroppedWarning",
    ("slayer.core.policy", "ColumnFilterRuleset"): "FieldFilterRuleset",
    ("slayer.core.policy", "JoinFilterRule"): "RelationshipFilterRule",
    ("slayer.core.policy", "JoinFilterRuleset"): "RelationshipFilterRuleset",
    ("slayer.core.policy", "JoinHop"): "RelationshipHop",
    ("slayer.live_schema.schema_drift", "EditModelDelete"): "EditDatasetDelete",
    ("slayer.live_schema.schema_drift", "ModelAddition"): "DatasetAddition",
    ("slayer.live_schema.schema_drift", "WholeModelDelete"): "WholeDatasetDelete",
    **{("slayer.core.errors", old): new for old, new in _ERRORS.items()},
}

#: (module, pre-2.0 function name) -> replacement name in the same module.
FUNCTIONS: dict[tuple[str, str], str] = {
    ("slayer.core.models", "join_key_error"): "relationship_key_error",
    ("slayer.live_schema.ingestion", "introspect_table_to_model"): "introspect_table_to_dataset",
    ("slayer.search.render", "collect_model_entity_pairs"): "collect_dataset_entity_pairs",
}

#: Modules defining ``__all__``: it lists each replacement and no pre-2.0 name.
EXPORTING_MODULES = frozenset({"slayer.core.policy"})

STORAGE_METHODS = {
    "get_model": "get_dataset", "save_model": "save_dataset", "delete_model": "delete_dataset",
    "list_models": "list_datasets", "get_model_or_builtin": "get_dataset_or_builtin",
    "resolve_model_identity": "resolve_dataset_identity", "load_models": "load_datasets",
    "builtin_models": "builtin_datasets", "update_column_sampled": "update_field_sampled",
}
ENGINE_METHODS = {
    "save_model": "save_dataset", "create_model_from_query": "create_dataset_from_query",
    "create_model_from_query_sync": "create_dataset_from_query_sync", "delete_model_by_name": "delete_dataset_by_name",
    "detect_join_cardinality": "detect_relationship_cardinality", "edit_model_remove": "edit_dataset_remove",
    "expand_query_backed_model": "expand_query_backed_dataset", "recommend_root_model": "recommend_population",
    "recommend_root_model_sync": "recommend_population_sync", "validate_models": "validate_datasets",
    "validate_sql_model_source": "validate_sql_dataset_source", "virtual_model": "virtual_dataset",
}
CLIENT_METHODS = {
    "create_model": "create_dataset", "get_model": "get_dataset", "get_model_sync": "get_dataset_sync",
    "list_models": "list_datasets", "list_models_sync": "list_datasets_sync",
    "recommend_root_model": "recommend_population", "recommend_root_model_sync": "recommend_population_sync",
}
DATASET_METHODS = {"get_column": "get_field", "get_measure": "get_metric", "list_columns": "list_fields"}

#: Every curated pre-2.0 method name: owner kind -> {old: new}.
METHODS: dict[str, dict[str, str]] = {
    "storage": STORAGE_METHODS, "engine": ENGINE_METHODS, "client": CLIENT_METHODS, "dataset": DATASET_METHODS,
}


def future(caught: list[Any]) -> list[Any]:
    return [w for w in caught if issubclass(w.category, FutureWarning)]


def assert_one_future_naming(caught: list[Any], new: str) -> None:
    [w] = future(caught)
    assert new in str(w.message)
    assert "SLayer 3.0" in str(w.message)


class TestClassNames:
    @pytest.mark.parametrize(("module", "old"), sorted(CLASSES))
    def test_an_old_class_name_resolves_with_one_warning(self, module: str, old: str) -> None:
        mod = importlib.import_module(module)
        new = getattr(mod, CLASSES[(module, old)])
        with recorded() as caught:
            resolved = getattr(mod, old)
        assert resolved is new
        assert_one_future_naming(caught, CLASSES[(module, old)])

    @pytest.mark.parametrize(("module", "old"), sorted(CLASSES))
    def test_the_new_class_name_warns_nothing(self, module: str, old: str) -> None:
        mod = importlib.import_module(module)
        with recorded() as caught:
            new = getattr(mod, CLASSES[(module, old)])
        assert inspect.isclass(new)
        assert future(caught) == []

    def test_from_import_of_slayer_model_receives_slayer_dataset(self) -> None:
        namespace: dict[str, Any] = {}
        with recorded() as caught:
            exec("from slayer.core.models import SlayerModel", namespace)  # noqa: S102 — the old name as a string
        assert namespace["SlayerModel"] is SlayerDataset
        assert_one_future_naming(caught, "SlayerDataset")

    @pytest.mark.parametrize(
        ("module", "old"), sorted(k for k in [*CLASSES, *FUNCTIONS] if k[0] in EXPORTING_MODULES),
    )
    def test_all_lists_the_new_name_only(self, module: str, old: str) -> None:
        exported = importlib.import_module(module).__all__
        new = {**CLASSES, **FUNCTIONS}[(module, old)]
        assert old not in exported
        assert new in exported


class _Spy:
    def __init__(self, result: Any) -> None:
        self.calls: list[tuple[tuple, dict]] = []
        self.result = result

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append((args, kwargs))
        return self.result


class _AsyncSpy(_Spy):
    async def __call__(self, *args: Any, **kwargs: Any) -> Any:  # pyright: ignore[reportIncompatibleMethodOverride]
        return super().__call__(*args, **kwargs)


def sentinel_arguments(fn: Callable[..., Any], *, bound: bool) -> tuple[list[str], dict[str, str]]:
    """A distinct string for every required parameter of ``fn``, positional where it can be."""
    params = list(inspect.signature(fn).parameters.values())[1 if bound else 0:]
    args: list[str] = []
    kwargs: dict[str, str] = {}
    for i, p in enumerate(params):
        if p.default is not p.empty or p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            continue
        if p.kind is p.KEYWORD_ONLY:
            kwargs[p.name] = f"arg{i}"
        else:
            args.append(f"arg{i}")
    return args, kwargs


async def call_through_old(
    *, owner: Any, old: str, new: str, monkeypatch: pytest.MonkeyPatch, bound: bool,
) -> tuple[Any, _Spy, list[Any], set[str]]:
    """Call ``owner.old`` with sentinels while ``owner.new`` is a spy; the result, the spy, the warnings and the
    sentinels sent."""
    target = getattr(type(owner), new) if bound else getattr(owner, new)
    is_async = inspect.iscoroutinefunction(target)
    spy = (_AsyncSpy if is_async else _Spy)(result=object())
    # A pydantic instance refuses unknown attributes: patch its class (a spy object never binds ``self``).
    monkeypatch.setattr(type(owner) if isinstance(owner, BaseModel) else owner, new, spy)
    args, kwargs = sentinel_arguments(getattr(type(owner), old) if bound else getattr(owner, old), bound=bound)
    with recorded() as caught:
        out = getattr(owner, old)(*args, **kwargs)
        if is_async:
            out = await out
    return out, spy, caught, {*args, *kwargs.values()}


def _forwarded(spy: _Spy) -> set[Any]:
    [(args, kwargs)] = spy.calls
    return {*args, *kwargs.values()}


def _make_owner(kind: str, base: Path) -> Any:
    storage = YAMLStorage(base_dir=str(base))
    if kind == "storage":
        return storage
    if kind == "engine":
        return SlayerQueryEngine(storage=storage)
    if kind == "client":
        return SlayerClient(storage=storage)
    return SlayerDataset.model_validate(shop_orders())


async def _release(owner: Any) -> None:
    engine = owner if isinstance(owner, SlayerQueryEngine) else getattr(owner, "_engine", None)
    if isinstance(engine, SlayerQueryEngine):
        await engine.aclose()


class TestMethods:
    @pytest.mark.parametrize(
        ("kind", "old"), [(kind, old) for kind, names in sorted(METHODS.items()) for old in sorted(names)],
    )
    async def test_an_old_method_delegates_with_one_warning(
        self, kind: str, old: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        new = METHODS[kind][old]
        owner = _make_owner(kind, tmp_path)
        try:
            out, spy, caught, sent = await call_through_old(
                owner=owner, old=old, new=new, monkeypatch=monkeypatch, bound=True,
            )
        finally:
            await _release(owner)
        assert out is spy.result
        assert sent <= _forwarded(spy)
        assert_one_future_naming(caught, new)

    async def test_get_model_returns_what_get_dataset_returns(self, tmp_path: Path) -> None:
        storage = await shop_store(backend="yaml", base=str(tmp_path))
        await storage.save_dataset(SlayerDataset.model_validate(shop_customers()))
        with recorded() as caught:
            old = await getattr(storage, "get_model")("customers", data_source=DS)
        new = await storage.get_dataset("customers", data_source=DS)
        assert old is not None
        assert old == new
        assert_one_future_naming(caught, "get_dataset")

    async def test_recommend_root_model_returns_what_recommend_population_returns(self) -> None:
        items = ["customers.name", "sum(orders.amount)"]
        async with vocab_engine("sqlite") as engine:
            with recorded() as caught:
                old = await getattr(engine, "recommend_root_model")(items)
            new = await engine.recommend_population(items)
        assert isinstance(new, PopulationRecommendation)
        assert old == new
        assert_one_future_naming(caught, "recommend_population")


class TestFunctions:
    @pytest.mark.parametrize(("module", "old"), sorted(FUNCTIONS))
    async def test_an_old_function_delegates_with_one_warning(
        self, module: str, old: str, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        mod = importlib.import_module(module)
        out, spy, caught, sent = await call_through_old(
            owner=mod, old=old, new=FUNCTIONS[(module, old)], monkeypatch=monkeypatch, bound=False,
        )
        assert out is spy.result
        assert sent <= _forwarded(spy)
        assert_one_future_naming(caught, FUNCTIONS[(module, old)])


class AttributeCase(BaseModel):
    """A pre-2.0 attribute twin: an instance maker, the names, and a value to assign through it."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    make: Callable[[], Any]
    old: str
    new: str
    assign: Callable[[], Any] | None = None  # None: read-only (a property, or a frozen owner)
    serialized: bool = True


def _dataset() -> SlayerDataset:
    return SlayerDataset.model_validate(shop_orders())


def _query() -> SlayerQuery:
    return SlayerQuery.model_validate({"population": "orders", "metrics": ["count(*)"], "filter": ["amount > 1"]})


_EXT_METRIC = {"name": "aov", "expression": "sum(amount) / count(*)"}
_EXT_REL = {"to": "customers", "join_pairs": [["customer_id", "id"]], "name": "buyer"}


def _extension() -> DatasetExtension:
    return DatasetExtension.model_validate(
        {"source_name": "orders", "metrics": [_EXT_METRIC], "relationships": [_EXT_REL]})


#: Every curated pre-2.0 attribute name, keyed ``<owner>.<old>``.
ATTRIBUTES: dict[str, AttributeCase] = {
    "dataset.columns": AttributeCase(
        make=_dataset, old="columns", new="fields",
        assign=lambda: [DatasetField.model_validate({"name": "only", "datatype": "INT"})]),
    "dataset.measures": AttributeCase(
        make=_dataset, old="measures", new="metrics",
        assign=lambda: [DatasetMetric.model_validate({"name": "n", "expression": "count(*)"})]),
    "dataset.joins": AttributeCase(make=_dataset, old="joins", new="relationships", assign=list),
    "dataset.source_model_origin": AttributeCase(
        make=_dataset, old="source_model_origin", new="source_dataset_origin",
        assign=lambda: SourceDatasetOrigin(name="per_customer"), serialized=False),
    "dataset.awaits_columns": AttributeCase(make=_dataset, old="awaits_columns", new="awaits_fields"),
    "source_origin.agg_column_names": AttributeCase(
        make=lambda: SourceDatasetOrigin(name="s", agg_field_names=frozenset({"a"})),
        old="agg_column_names", new="agg_field_names", assign=lambda: frozenset({"b"})),
    "field.type": AttributeCase(
        make=lambda: DatasetField.model_validate({"name": "x", "datatype": "INT"}),
        old="type", new="datatype", assign=lambda: DataType.TEXT),
    "metric.type": AttributeCase(
        make=lambda: DatasetMetric.model_validate({"name": "m", "expression": "count(*)", "datatype": "INT"}),
        old="type", new="datatype", assign=lambda: DataType.DOUBLE),
    "metric.formula": AttributeCase(
        make=lambda: DatasetMetric.model_validate({"name": "m", "expression": "count(*)"}),
        old="formula", new="expression", assign=lambda: "sum(amount)"),
    "aggregation.formula": AttributeCase(
        make=lambda: Aggregation.model_validate({"name": "doubled", "expression": "SUM({value}) * 2"}),
        old="formula", new="expression", assign=lambda: "SUM({value}) * 3"),
    "relationship.target_model": AttributeCase(
        make=lambda: DatasetRelationship.model_validate({"to": "customers", "join_pairs": [["customer_id", "id"]]}),
        old="target_model", new="to", assign=lambda: "buyers"),
    "query.source_model": AttributeCase(make=_query, old="source_model", new="population", assign=lambda: "customers"),
    "query.measures": AttributeCase(
        make=_query, old="measures", new="metrics",
        assign=lambda: SlayerQuery.model_validate({"population": "orders", "metrics": ["sum(amount)"]}).metrics),
    "query.filters": AttributeCase(make=_query, old="filters", new="filter", assign=lambda: ["amount > 2"]),
    "query.source_model_name": AttributeCase(make=_query, old="source_model_name", new="population_name"),
    "refinement.measures": AttributeCase(
        make=lambda: QueryRefinement.model_validate({"metrics": ["count(*)"]}), old="measures", new="metrics",
        assign=lambda: QueryRefinement.model_validate({"metrics": ["sum(amount)"]}).metrics),
    "refinement.filters": AttributeCase(
        make=lambda: QueryRefinement.model_validate({"filter": ["amount > 1"]}), old="filters", new="filter",
        assign=lambda: ["amount > 2"]),
    "time_dimension.dimension": AttributeCase(
        make=lambda: TimeDimension.model_validate({"field": "created_at", "granularity": "day"}),
        old="dimension", new="field", assign=lambda: "shipped_at"),
    "time_dimension.column_ref": AttributeCase(
        make=lambda: TimeDimension.model_validate({"field": "created_at", "granularity": "day"}),
        old="column_ref", new="field_ref"),
    "order_item.column": AttributeCase(
        make=lambda: OrderItem.model_validate({"field": "status"}), old="column", new="field",
        assign=lambda: FieldRef(name="amount")),
    "order_item.raw_formula": AttributeCase(
        make=lambda: OrderItem.model_validate({"field": "status"}), old="raw_formula", new="raw_expression",
        assign=lambda: "sum(amount)"),
    "field_ref.model": AttributeCase(
        make=lambda: FieldRef(name="name", dataset="customers"), old="model", new="dataset",
        assign=lambda: "buyers"),
    "extension.measures": AttributeCase(
        make=_extension, old="measures", new="metrics",
        assign=lambda: [DatasetMetric.model_validate({**_EXT_METRIC, "name": "aov2"})]),
    "extension.joins": AttributeCase(
        make=_extension, old="joins", new="relationships",
        assign=lambda: [DatasetRelationship.model_validate({**_EXT_REL, "name": "payer"})]),
    "response_attributes.measures": AttributeCase(
        make=lambda: ResponseAttributes(metrics={"orders.n": FieldMetadata(label="N")}),
        old="measures", new="metrics", assign=lambda: {"orders.m": FieldMetadata(label="M")}),
    "recommendation.root_model": AttributeCase(
        make=lambda: PopulationRecommendation(data_source=DS, population="orders", reachable=True),
        old="root_model", new="population", assign=lambda: "customers"),
    "coverage.model_name": AttributeCase(
        make=lambda: CandidateCoverage(dataset_name="orders", reachable_items=[], unreachable_items=[]),
        old="model_name", new="dataset_name", assign=lambda: "customers"),
    "associated.measure": AttributeCase(
        make=lambda: AssociatedWarningPayload(metric="m", location="q", dimensions=[]),
        old="measure", new="metric", assign=lambda: "n"),
    "broadcast.measure": AttributeCase(
        make=lambda: BroadcastGrainWarningPayload(metric="m", location="q", dimensions=[]),
        old="measure", new="metric", assign=lambda: "n"),
    "broken_dependent.model": AttributeCase(
        make=lambda: BrokenDependentWarning(dataset="d", data_source=DS, dependent_kind="filter", name="f",
                                           field="c"),
        old="model", new="dataset", assign=lambda: "e"),
    "broken_dependent.column": AttributeCase(
        make=lambda: BrokenDependentWarning(dataset="d", data_source=DS, dependent_kind="filter", name="f",
                                           field="c"),
        old="column", new="field", assign=lambda: "e"),
    "degenerate.measure": AttributeCase(
        make=lambda: DegenerateReaggregationWarningPayload(metric="m", location="q", operand_grain=[],
                                                           outer_grain=[]),
        old="measure", new="metric", assign=lambda: "n"),
    "extension_inlined.column": AttributeCase(
        make=lambda: ExtensionFieldInlinedWarning(document="q", field="c"), old="column", new="field",
        assign=lambda: "e"),
    "library_shadow.model": AttributeCase(
        make=lambda: LibraryNameShadowWarning(dataset="d", aggregation="a", function="f"), old="model",
        new="dataset", assign=lambda: "e"),
    "name_collision.model": AttributeCase(
        make=lambda: NameCollisionWarning(dataset="d", namespace="n", names=[]), old="model", new="dataset",
        assign=lambda: "e"),
    "narrowed_access.model": AttributeCase(
        make=lambda: NarrowedAccessWarning(data_source=DS, dataset="d", narrowed_by=[]), old="model",
        new="dataset", assign=lambda: "e"),
    "semi_join_pushed.measure": AttributeCase(
        make=lambda: SemiJoinPushedWarningPayload(metric="m", location="q", filter_text="x > 1"),
        old="measure", new="metric", assign=lambda: "n"),
    "spine_dropped.model": AttributeCase(
        make=lambda: SpineRelationshipDroppedWarning(dataset="d", relationship="r"), old="model", new="dataset",
        assign=lambda: "e"),
    "spine_dropped.join": AttributeCase(
        make=lambda: SpineRelationshipDroppedWarning(dataset="d", relationship="r"), old="join",
        new="relationship", assign=lambda: "s"),
    "unrelated.measures": AttributeCase(
        make=lambda: UnrelatedDatasetWarningPayload(dataset="d", population="p", metrics=["m"], location="q"),
        old="measures", new="metrics", assign=lambda: ["n"]),
    "whole_periods.column": AttributeCase(
        make=lambda: WholePeriodsNonNestingWarningPayload(field="c", granularities=[], location="q"),
        old="column", new="field", assign=lambda: "e"),
    "field_filter.column": AttributeCase(
        make=lambda: FieldFilterRuleset(field="region", value="EU"), old="column", new="field"),
    "relationship_filter_rule.join_path": AttributeCase(
        make=lambda: RelationshipFilterRule(target_table="t", relationship_path=("a",)), old="join_path",
        new="relationship_path"),
    "relationship_filter.column": AttributeCase(
        make=lambda: RelationshipFilterRuleset(table="t", field="c", value=1), old="column", new="field"),
    "relationship_filter.joins": AttributeCase(
        make=lambda: RelationshipFilterRuleset(table="t", field="c", value=1), old="joins", new="relationships"),
}
WRITABLE = sorted(k for k, c in ATTRIBUTES.items() if c.assign is not None)
#: Cases whose replacement is a declared field: its old name is also accepted as an input key.
INPUT_KEYS = sorted(k for k, c in ATTRIBUTES.items() if c.serialized and c.new in type(c.make()).model_fields)


class TestAttributes:
    @pytest.mark.parametrize("case", sorted(ATTRIBUTES))
    def test_an_old_attribute_reads_the_new_one(self, case: str) -> None:
        c = ATTRIBUTES[case]
        obj = c.make()
        with recorded() as caught:
            value = getattr(obj, c.old)
        assert value == getattr(obj, c.new)
        assert_one_future_naming(caught, c.new)

    @pytest.mark.parametrize("case", WRITABLE)
    def test_an_old_attribute_writes_the_new_one(self, case: str) -> None:
        c = ATTRIBUTES[case]
        assert c.assign is not None
        obj, value = c.make(), c.assign()
        with recorded() as caught:
            setattr(obj, c.old, value)
        assert getattr(obj, c.new) == value
        assert_one_future_naming(caught, c.new)
        dumped = obj.model_dump(mode="json")
        assert c.old not in dumped
        if c.serialized:
            assert c.new in dumped

    def test_dataset_columns_reads_fields(self) -> None:
        dataset = _dataset()
        with recorded() as caught:
            columns = getattr(dataset, "columns")
        assert columns == dataset.fields
        assert [f.name for f in columns][:2] == ["order_id", "customer_id"]
        assert_one_future_naming(caught, "fields")

    def test_query_source_model_reads_population(self) -> None:
        query = _query()
        with recorded() as caught:
            assert getattr(query, "source_model") == "orders"
        assert_one_future_naming(caught, "population")

    @pytest.mark.parametrize("case", INPUT_KEYS)
    @pytest.mark.parametrize("via", ["keywords", "model_validate"])
    def test_an_old_field_name_is_accepted_as_input(self, case: str, via: str) -> None:
        c = ATTRIBUTES[case]
        obj = c.make()
        data = obj.model_dump(exclude={"version"})  # a versioned document is stored input, converted silently
        data[c.old] = data.pop(c.new)
        with recorded() as caught:
            built = type(obj)(**data) if via == "keywords" else type(obj).model_validate(data)
        assert built.model_dump() == obj.model_dump()
        [w] = deprecations(caught)
        assert c.old in payload_of(w).old
        assert c.new in payload_of(w).new
        assert payload_of(w).removal == "3.0"

    def test_storyline_policy_calls_keep_working(self) -> None:
        mod = importlib.import_module("slayer.core.policy")
        with recorded() as caught:
            rule = getattr(mod, "JoinFilterRule")(**{"target_table": "t", "join_path": ("a",)})
            joined = getattr(mod, "JoinFilterRuleset")(**{"table": "t", "column": "c", "value": 1, "joins": (rule,)})
            single = getattr(mod, "ColumnFilterRuleset")(**{"column": "region", "value": "EU"})
        assert rule == RelationshipFilterRule(target_table="t", relationship_path=("a",))
        assert joined == RelationshipFilterRuleset(table="t", field="c", value=1, relationships=(rule,))
        assert single == FieldFilterRuleset(field="region", value="EU")
        assert sorted(payload_of(w).old for w in deprecations(caught)) == sorted(
            ["column", "join_path", "joins"])

    def test_dataset_columns_write_lands_in_fields(self) -> None:
        dataset = _dataset()
        only = [DatasetField.model_validate({"name": "only", "datatype": "INT"})]
        with recorded() as caught:
            setattr(dataset, "columns", only)
        assert dataset.fields == only
        assert_one_future_naming(caught, "fields")
        dumped = dataset.model_dump(mode="json")
        assert [f["name"] for f in dumped["fields"]] == ["only"]
        assert "columns" not in dumped
