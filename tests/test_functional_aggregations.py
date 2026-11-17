"""Functional aggregation spelling — parser forms, binder validation, slack-rule retirement, and entity-ref
surfaces; unknown names defer to binding; functional input is first-class (no normalization warning, no rewrite)."""

from __future__ import annotations

import tempfile
import warnings as warnings_mod
from decimal import Decimal

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import EntityResolutionError, UnknownFunctionError
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    DatasetMetric,
    SlayerDataset,
)
from slayer.core.query import SlayerQuery
from slayer.core.warnings import NormalizationWarning, SlayerNormalizationWarning
from slayer.core.scope import DatasetScope
from slayer.engine.binding import bind_expr
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.core.syntax import (
    AggCall,
    Arith,
    DottedRef,
    Literal,
    NamedArg,
    Ref,
    ScalarCall,
    StarSource,
    TransformCall,
    parse_expr,
)
from slayer.memories.resolver import extract_entities_from_query, resolve_entity
from slayer.storage.yaml_storage import YAMLStorage

from tests._dev1739_fixtures import make_exec_engine
from tests._engine_helpers import _engine_generate


# ===========================================================================
# Shared model builders.
# ===========================================================================


def _orders(**overrides) -> SlayerDataset:
    fields = dict(
        name="orders",
        data_source="test",
        sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INT),
            DatasetField(name="status", datatype=DataType.TEXT),
            DatasetField(name="revenue", datatype=DataType.DOUBLE),
            DatasetField(name="price", datatype=DataType.DOUBLE),
        ],
        relationships=[
            DatasetRelationship(to="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )
    fields.update(overrides)
    return SlayerDataset(**fields)


def _customers(**overrides) -> SlayerDataset:
    fields = dict(
        name="customers",
        data_source="test",
        sql_table="customers",
        fields=[
            DatasetField(name="id", datatype=DataType.INT, primary_key=True),
            DatasetField(name="score", datatype=DataType.DOUBLE),
        ],
    )
    fields.update(overrides)
    return SlayerDataset(**fields)


# ===========================================================================
# 1.1 — Parser: functional spellings.
# ===========================================================================


class TestStarForms:
    def test_count_star(self) -> None:
        assert parse_expr("count(*)") == AggCall(source=StarSource(), agg="count")

    def test_count_star_with_spaces(self) -> None:
        assert parse_expr("count( * )") == parse_expr("count(*)")

    def test_count_dotted_star(self) -> None:
        node = parse_expr("count(customers.*)")
        assert isinstance(node, AggCall)
        assert node.source == DottedRef(parts=("customers", "*"))

    def test_count_deep_dotted_star(self) -> None:
        assert parse_expr("count(b.c.d.e.*)") == AggCall(source=DottedRef(parts=("b", "c", "d", "e", "*")), agg="count")

    def test_multiplication_untouched(self) -> None:
        assert parse_expr("price * quantity") == Arith(
            op="*", left=Ref(name="price"), right=Ref(name="quantity")
        )

    def test_star_count_in_arithmetic(self) -> None:
        assert parse_expr("count(*) * 2") == Arith(
            op="*", left=AggCall(source=StarSource(), agg="count"), right=Literal(value=Decimal("2")))

    def test_star_and_multiplication_mix(self) -> None:
        assert parse_expr("sum(price) * count(*)") == Arith(
            op="*", left=AggCall(source=Ref(name="price"), agg="sum"), right=AggCall(source=StarSource(), agg="count"))

    def test_star_inside_string_literal_untouched(self) -> None:
        assert parse_expr("replace(status, '*', 'x')") == ScalarCall(
            name="replace",
            args=(Ref(name="status"), Literal(value="*"), Literal(value="x")),
        )


class TestFirstLastArbitration:
    def test_last_over_bare_column_is_aggregation(self) -> None:
        assert parse_expr("last(balance)") == AggCall(
            source=Ref(name="balance"), agg="last"
        )

    def test_first_over_bare_column_is_aggregation(self) -> None:
        assert parse_expr("first(balance)") == AggCall(
            source=Ref(name="balance"), agg="first"
        )

    def test_last_with_time_arg_is_aggregation(self) -> None:
        node = parse_expr("last(balance, updated_at)")
        assert isinstance(node, AggCall)
        assert node.agg == "last"

    def test_last_over_functional_agg_parses_to_one_aggcall(self) -> None:
        assert parse_expr("last(sum(revenue))") == AggCall(
            source=AggCall(source=Ref(name="revenue"), agg="sum"), agg="last"
        )

    def test_first_over_functional_agg_parses_to_one_aggcall(self) -> None:
        assert parse_expr("first(sum(revenue))") == AggCall(
            source=AggCall(source=Ref(name="revenue"), agg="sum"), agg="first"
        )

    def test_last_over_aggregated_arithmetic_parses_to_one_aggcall(self) -> None:
        node = parse_expr("last(sum(revenue) / count(*))")
        assert isinstance(node, AggCall)
        assert node.agg == "last"


_SUM_REVENUE = AggCall(source=Ref(name="revenue"), agg="sum")


class TestDispatchUnchanged:
    def test_transform_over_functional_agg(self) -> None:
        assert parse_expr("cumsum(sum(revenue))") == TransformCall(op="cumsum", input=_SUM_REVENUE)

    def test_rank_over_functional_agg_with_partition(self) -> None:
        assert parse_expr("rank(sum(revenue), partition_by=status, direction='desc')") == TransformCall(
            op="rank", input=_SUM_REVENUE,
            args=(NamedArg(name="partition_by", value=Ref(name="status")),
                  NamedArg(name="direction", value=Literal(value="desc"))),
        )

    def test_scalar_call_stays_scalar(self) -> None:
        assert parse_expr("round(price)") == ScalarCall(
            name="round", args=(Ref(name="price"),)
        )

    def test_upper_stays_scalar(self) -> None:
        assert parse_expr("upper(name)") == ScalarCall(
            name="upper", args=(Ref(name="name"),)
        )

    def test_arithmetic_of_functional_aggs(self) -> None:
        assert parse_expr("sum(revenue) / count(*)") == Arith(
            op="/", left=_SUM_REVENUE, right=AggCall(source=StarSource(), agg="count")
        )


class TestUnknownNameDeferral:
    def test_unknown_over_column_defers(self) -> None:
        assert parse_expr("whatever(price)") == AggCall(source=Ref(name="price"), agg="whatever")

    def test_unknown_over_star_defers(self) -> None:
        assert parse_expr("bogus(*)") == AggCall(source=StarSource(), agg="bogus")

    def test_unknown_over_dotted_defers(self) -> None:
        assert parse_expr("rolling_avg(customers.score)") == AggCall(
            source=DottedRef(parts=("customers", "score")), agg="rolling_avg")

    def test_unknown_with_kwargs_defers(self) -> None:
        assert parse_expr("trimmed(price, low=1, high=9)") == AggCall(
            source=Ref(name="price"), agg="trimmed",
            args=(NamedArg(name="low", value=Literal(value=Decimal("1"))),
                  NamedArg(name="high", value=Literal(value=Decimal("9")))))


class TestFunctionalParseErrors:
    def test_zero_arg_builtin_raises(self) -> None:
        with pytest.raises(ValueError):
            parse_expr("sum()")

    def test_kwargs_only_unknown_call_raises(self) -> None:
        orders = _orders()
        parsed = parse_expr("bogus(p=1)")
        bundle = ResolvedSourceBundle(dialect="postgres", population=orders)
        scope = DatasetScope(population=orders)
        with pytest.raises(UnknownFunctionError):
            bind_expr(parsed, scope=scope, bundle=bundle)

    def test_sql_distinct_keyword_outside_count_rejected(self) -> None:
        with pytest.raises(ValueError, match=r"count\(distinct x\)"):
            parse_expr("sum(distinct user_id)")


# ===========================================================================
# 1.3 — Binder: global name validation, custom aggs, scalar collisions.
# ===========================================================================


async def _binding_error(
    expression: str, *, dataset: SlayerDataset, extra_datasets: list | None = None
) -> ValueError:
    query = SlayerQuery(
        population="orders", metrics=[expression], dimensions=["status"]
    )
    with pytest.raises(ValueError) as ei:
        await _engine_generate(query=query, dataset=dataset, extra_datasets=extra_datasets)
    return ei.value


class TestCustomAggregationBinding:
    async def test_host_model_custom_agg(self) -> None:
        model = _orders(
            aggregations=[Aggregation(name="my_agg", expression="AVG({value})")]
        )
        sql_f = await _engine_generate(
            query=SlayerQuery(population="orders", metrics=["my_agg(price)"]),
            dataset=model,
        )
        assert "AVG(" in sql_f

    async def test_joined_model_custom_agg(self) -> None:
        customers = _customers(
            aggregations=[Aggregation(name="rolling_avg", expression="AVG({value})")]
        )
        sql_f = await _engine_generate(
            query=SlayerQuery(
                population="orders", metrics=["rolling_avg(customers.score)"]
            ),
            dataset=_orders(),
            extra_datasets=[customers],
        )
        assert "AVG(" in sql_f


class TestGlobalNameValidation:
    async def test_unknown_over_column_gets_standard_error(self) -> None:
        err = await _binding_error("bogus(price)", dataset=_orders())
        assert "Unknown function 'bogus'" in str(err)

    async def test_star_bogus_functional_gets_standard_error(self) -> None:
        err = await _binding_error("bogus(*)", dataset=_orders())
        assert "Unknown function 'bogus'" in str(err)

    async def test_dotted_star_bogus_gets_standard_error(self) -> None:
        err = await _binding_error(
            "bogus(customers.*)", dataset=_orders(), extra_datasets=[_customers()]
        )
        assert "Unknown function 'bogus'" in str(err)

    async def test_avg_star_rejected(self) -> None:
        await _binding_error("avg(*)", dataset=_orders())

    async def test_unknown_agg_error_hints_scalar_near_miss(self) -> None:
        err = await _binding_error("rond(price)", dataset=_orders())
        msg = str(err)
        assert "round" in msg
        assert "scalar" in msg.lower()

    async def test_missing_weight_parameter(self) -> None:
        err = await _binding_error("weighted_avg(price)", dataset=_orders())
        assert "weight" in str(err)

    async def test_missing_other_parameter(self) -> None:
        err = await _binding_error("corr(price)", dataset=_orders())
        assert "other" in str(err)


class TestModeAUnchanged:
    async def test_raw_sql_aggregate_column_passthrough(self) -> None:
        # Mode-A column SQL is raw SQL — the functional parser must never
        # touch it.
        model = _orders()
        model = model.model_copy(
            update={
                "fields": [
                    *model.fields,
                    DatasetField(name="grand_total", sql="SUM(revenue)", datatype=DataType.DOUBLE),
                ]
            }
        )
        sql = await _engine_generate(
            query=SlayerQuery.model_validate({"population": "orders", "metrics": ["max(grand_total)"]}),
            dataset=model,
        )
        assert "SUM(orders.revenue)" in sql


class TestScalarCollidingCustomAggNames:
    @pytest.mark.parametrize("name", ["round", "iif", "coalesce"])
    def test_scalar_name_accepted(self, name: str) -> None:
        assert Aggregation(name=name, expression="AVG({value})").name == name

    def test_transform_name_still_rejected(self) -> None:
        with pytest.raises(ValueError, match="cumsum"):
            Aggregation(name="cumsum", expression="AVG({value})")

    def test_regular_custom_name_accepted(self) -> None:
        Aggregation(name="my_agg", expression="AVG({value})")


# ===========================================================================
# 1.5 — Retirement: functional input is first-class, never rewritten/warned.
# ===========================================================================


class TestFunctionalIsFirstClass:
    async def test_no_normalization_warning_on_execute(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="test", type="postgres")
            )
            await storage.save_dataset(_orders())
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(
                population="orders",
                metrics=["sum(revenue)", "count(*)"],
                dimensions=["status"],
                filter=["sum(revenue) > 0"],
            )
            with warnings_mod.catch_warnings():
                warnings_mod.simplefilter("error", SlayerNormalizationWarning)
                resp = await engine.execute(query, dry_run=True)
        func_style = [
            w
            for w in resp.warnings
            if isinstance(w, NormalizationWarning) and w.rule_id == "FUNC_STYLE_AGG"
        ]
        assert func_style == []

    async def test_save_preserves_functional_spelling(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            storage = YAMLStorage(base_dir=d)
            await storage.save_datasource(
                DatasourceConfig(name="test", type="postgres")
            )
            engine = SlayerQueryEngine(storage=storage)
            model = _orders(
                metrics=[DatasetMetric(name="rev", expression="sum(revenue)")]
            )
            with warnings_mod.catch_warnings():
                warnings_mod.simplefilter("error", SlayerNormalizationWarning)
                await engine.save_dataset(model)
            stored = await storage.get_dataset("orders", data_source="test")
        assert stored is not None
        assert stored.metrics[0].expression == "sum(revenue)"


# ===========================================================================
# 1.6 — Entity refs: memories resolution and recommend_root_model.
# ===========================================================================


@pytest.fixture
async def refs_storage(tmp_path):
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(
        DatasourceConfig(name="mydb", type="postgres", host="x")
    )
    await storage.save_dataset(
        SlayerDataset(
            name="orders",
            data_source="mydb",
            sql_table="orders",
            fields=[
                DatasetField(name="id", datatype=DataType.INT, primary_key=True),
                DatasetField(name="status", datatype=DataType.TEXT),
                DatasetField(name="amount", datatype=DataType.DOUBLE),
                DatasetField(name="cost", datatype=DataType.DOUBLE),
            ],
        )
    )
    return storage


class TestEntityRefsFunctional:
    async def test_functional_ref(self, refs_storage) -> None:
        f = await resolve_entity("sum(orders.amount)", storage=refs_storage)
        assert f.canonical_forms == ["mydb.orders.amount"]

    async def test_functional_star_count_resolves_to_model(self, refs_storage) -> None:
        f = await resolve_entity("count(orders.*)", storage=refs_storage)
        assert f.canonical_forms == ["mydb.orders"]

    async def test_expression_text_rejected(self, refs_storage) -> None:
        with pytest.raises(EntityResolutionError):
            await resolve_entity(
                "sum(orders.amount - orders.cost)", storage=refs_storage
            )

    async def test_extract_entities(self, refs_storage) -> None:
        q_func = SlayerQuery(
            population="orders", metrics=["sum(amount)"], dimensions=["status"]
        )
        e_func = await extract_entities_from_query(q_func, storage=refs_storage)
        assert e_func.canonical_forms == ["mydb.orders", "mydb.orders.status", "mydb.orders.amount"]

    async def test_recommend_root_model(self, refs_storage) -> None:
        engine = SlayerQueryEngine(storage=refs_storage)
        try:
            f = await engine.recommend_population(["sum(orders.amount)"])
            assert f.population == "orders"
            assert [ip.path for ip in f.item_paths] == ["sum(amount)"]
        finally:
            await engine.aclose()

    async def test_recommend_expression_rejected(self, refs_storage) -> None:
        engine = SlayerQueryEngine(storage=refs_storage)
        try:
            with pytest.raises((ValueError, EntityResolutionError)):
                await engine.recommend_population(
                    ["sum(orders.amount - orders.cost)"]
                )
        finally:
            await engine.aclose()


# ===========================================================================
# Cross-spelling measure/filter matching.
# ===========================================================================


@pytest.fixture(params=["sqlite", "duckdb"])
async def exec_engine(request):
    async for engine in make_exec_engine(request):
        yield engine


class TestCrossSpellingMatching:
    @pytest.mark.parametrize(
        ("measure", "filter_"),
        [("sum(amount)", "SUM(amount) > 100"), ("SUM(amount)", "sum(amount) > 100")],
    )
    async def test_filter_resolves_to_renamed_measure(self, exec_engine: SlayerQueryEngine, measure, filter_) -> None:
        resp = await exec_engine.execute(SlayerQuery.model_validate({
            "population": "orders", "dimensions": ["channel"],
            "metrics": [{"expression": measure, "name": "rev"}], "filter": [filter_],
        }))
        assert resp.columns == ["orders.channel", "orders.rev"]
        assert [(r["orders.channel"], float(r["orders.rev"])) for r in resp.data] == [("web", 165.0)]
