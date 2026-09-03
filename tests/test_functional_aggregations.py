"""Functional aggregation spelling — parser equivalence, binder validation,
slack-rule retirement, and entity-ref surfaces (OpenSpec change
``dev-1826-make-sure-all-aggregations-support-functional-form``).

Every aggregation writable as ``col:agg(args)`` must be writable as
``agg(col, args)`` producing the identical ``AggCall``; unknown names defer to
binding exactly like colon spellings; functional input is first-class (no
normalization warning, no rewrite on save).
"""

from __future__ import annotations

import tempfile
import warnings as warnings_mod

import pytest

from slayer.core.enums import AGGREGATION_ALIASES, BUILTIN_AGGREGATIONS, DataType
from slayer.core.errors import EntityResolutionError, UnknownFunctionError
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import SlayerQuery
from slayer.core.warnings import NormalizationWarning, SlayerNormalizationWarning
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.syntax import (
    AggCall,
    Arith,
    DottedRef,
    Literal,
    Ref,
    ScalarCall,
    StarSource,
    TransformCall,
    parse_expr,
)
from slayer.memories.resolver import extract_entities_from_query, resolve_entity
from slayer.storage.yaml_storage import YAMLStorage

from tests._engine_helpers import _engine_generate


# ===========================================================================
# Shared model builders.
# ===========================================================================


def _orders(**overrides) -> SlayerModel:
    fields = dict(
        name="orders",
        data_source="test",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="status", type=DataType.TEXT),
            Column(name="revenue", type=DataType.DOUBLE),
            Column(name="price", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )
    fields.update(overrides)
    return SlayerModel(**fields)


def _customers(**overrides) -> SlayerModel:
    fields = dict(
        name="customers",
        data_source="test",
        sql_table="customers",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="score", type=DataType.DOUBLE),
        ],
    )
    fields.update(overrides)
    return SlayerModel(**fields)


# ===========================================================================
# 1.1 — Parser: functional and colon spellings collapse to one AggCall.
# ===========================================================================


class TestFunctionalColonIdentity:
    @pytest.mark.parametrize("agg", sorted(BUILTIN_AGGREGATIONS))
    def test_every_builtin(self, agg: str) -> None:
        assert parse_expr(f"{agg}(revenue)") == parse_expr(f"revenue:{agg}")

    @pytest.mark.parametrize("alias", sorted(AGGREGATION_ALIASES))
    def test_every_alias(self, alias: str) -> None:
        assert parse_expr(f"{alias}(user_id)") == parse_expr(f"user_id:{alias}")

    @pytest.mark.parametrize("spelling", ["SUM", "Sum", "sUm"])
    def test_case_variants(self, spelling: str) -> None:
        assert parse_expr(f"{spelling}(revenue)") == parse_expr(f"revenue:{spelling}")

    def test_camel_case_alias(self) -> None:
        assert parse_expr("countD(user_id)") == parse_expr("user_id:countD")

    def test_dotted_column_source(self) -> None:
        assert parse_expr("count(customers.regions.name)") == parse_expr(
            "customers.regions.name:count"
        )

    def test_dotted_sum(self) -> None:
        assert parse_expr("sum(customers.score)") == parse_expr("customers.score:sum")


class TestStarForms:
    def test_count_star(self) -> None:
        assert parse_expr("count(*)") == AggCall(source=StarSource(), agg="count")
        assert parse_expr("count(*)") == parse_expr("*:count")

    def test_count_star_with_spaces(self) -> None:
        assert parse_expr("count( * )") == parse_expr("*:count")

    def test_count_dotted_star(self) -> None:
        node = parse_expr("count(customers.*)")
        assert node == parse_expr("customers.*:count")
        assert node.source == DottedRef(parts=("customers", "*"))

    def test_count_deep_dotted_star(self) -> None:
        assert parse_expr("count(b.c.d.e.*)") == parse_expr("b.c.d.e.*:count")

    def test_multiplication_untouched(self) -> None:
        assert parse_expr("price * quantity") == Arith(
            op="*", left=Ref(name="price"), right=Ref(name="quantity")
        )

    def test_star_count_in_arithmetic(self) -> None:
        assert parse_expr("count(*) * 2") == parse_expr("*:count * 2")

    def test_star_and_multiplication_mix(self) -> None:
        assert parse_expr("sum(price) * count(*)") == parse_expr("price:sum * *:count")

    def test_star_inside_string_literal_untouched(self) -> None:
        assert parse_expr("replace(status, '*', 'x')") == ScalarCall(
            name="replace",
            args=(Ref(name="status"), Literal(value="*"), Literal(value="x")),
        )


class TestParametricForms:
    def test_percentile_kwarg(self) -> None:
        assert parse_expr("percentile(price, p=0.9)") == parse_expr(
            "price:percentile(p=0.9)"
        )

    def test_window_kwarg(self) -> None:
        assert parse_expr("sum(revenue, window='90d')") == parse_expr(
            "revenue:sum(window='90d')"
        )

    def test_partition_by_kwarg(self) -> None:
        assert parse_expr("sum(revenue, partition_by=region)") == parse_expr(
            "revenue:sum(partition_by=region)"
        )

    def test_partition_by_list_kwarg(self) -> None:
        assert parse_expr("sum(revenue, partition_by=[region, city])") == parse_expr(
            "revenue:sum(partition_by=[region, city])"
        )

    def test_ranked_positional_time_column(self) -> None:
        assert parse_expr("last(balance, updated_at)") == parse_expr(
            "balance:last(updated_at)"
        )

    def test_weighted_avg_kwarg(self) -> None:
        assert parse_expr("weighted_avg(price, weight=quantity)") == parse_expr(
            "price:weighted_avg(weight=quantity)"
        )

    def test_corr_other_kwarg(self) -> None:
        assert parse_expr("corr(x, other=y)") == parse_expr("x:corr(other=y)")


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

    def test_last_over_colon_agg_is_transform(self) -> None:
        assert parse_expr("last(revenue:sum)") == TransformCall(
            op="last", input=AggCall(source=Ref(name="revenue"), agg="sum")
        )

    def test_last_over_functional_agg_is_transform(self) -> None:
        assert parse_expr("last(sum(revenue))") == parse_expr("last(revenue:sum)")

    def test_first_over_functional_agg_is_transform(self) -> None:
        assert parse_expr("first(sum(revenue))") == parse_expr("first(revenue:sum)")

    def test_last_over_aggregated_arithmetic_is_transform(self) -> None:
        node = parse_expr("last(sum(revenue) / count(*))")
        assert isinstance(node, TransformCall)
        assert node.op == "last"


class TestDispatchUnchanged:
    def test_transform_over_functional_agg(self) -> None:
        assert parse_expr("cumsum(sum(revenue))") == parse_expr("cumsum(revenue:sum)")

    def test_rank_over_functional_agg_with_partition(self) -> None:
        assert parse_expr("rank(sum(revenue), partition_by=status)") == parse_expr(
            "rank(revenue:sum, partition_by=status)"
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
        assert parse_expr("sum(revenue) / count(*)") == parse_expr(
            "revenue:sum / *:count"
        )


class TestUnknownNameDeferral:
    def test_unknown_over_column_defers(self) -> None:
        assert parse_expr("whatever(price)") == parse_expr("price:whatever")

    def test_unknown_over_star_defers(self) -> None:
        assert parse_expr("bogus(*)") == parse_expr("*:bogus")

    def test_unknown_over_dotted_defers(self) -> None:
        assert parse_expr("rolling_avg(customers.score)") == parse_expr(
            "customers.score:rolling_avg"
        )

    def test_unknown_with_kwargs_defers(self) -> None:
        assert parse_expr("trimmed(price, low=1, high=9)") == parse_expr(
            "price:trimmed(low=1, high=9)"
        )


class TestFunctionalParseErrors:
    def test_zero_arg_builtin_raises(self) -> None:
        with pytest.raises(ValueError):
            parse_expr("sum()")

    def test_kwargs_only_unknown_call_raises(self) -> None:
        with pytest.raises(UnknownFunctionError):
            parse_expr("bogus(p=1)")

    def test_sql_distinct_keyword_is_syntax_error(self) -> None:
        with pytest.raises(ValueError):
            parse_expr("count(distinct user_id)")


# ===========================================================================
# 1.3 — Binder: global name validation, custom aggs, scalar collisions.
# ===========================================================================


async def _binding_error(
    formula: str, *, model: SlayerModel, extra_models: list | None = None
) -> ValueError:
    query = SlayerQuery(
        source_model="orders", measures=[formula], dimensions=["status"]
    )
    with pytest.raises(ValueError) as ei:
        await _engine_generate(query=query, model=model, extra_models=extra_models)
    return ei.value


class TestCustomAggregationBinding:
    async def test_host_model_custom_agg_sql_parity(self) -> None:
        model = _orders(
            aggregations=[Aggregation(name="my_agg", formula="AVG({value})")]
        )
        sql_f = await _engine_generate(
            query=SlayerQuery(source_model="orders", measures=["my_agg(price)"]),
            model=model,
        )
        sql_c = await _engine_generate(
            query=SlayerQuery(source_model="orders", measures=["price:my_agg"]),
            model=model,
        )
        assert sql_f == sql_c
        assert "AVG(" in sql_f

    async def test_joined_model_custom_agg_sql_parity(self) -> None:
        customers = _customers(
            aggregations=[Aggregation(name="rolling_avg", formula="AVG({value})")]
        )
        sql_f = await _engine_generate(
            query=SlayerQuery(
                source_model="orders", measures=["rolling_avg(customers.score)"]
            ),
            model=_orders(),
            extra_models=[customers],
        )
        sql_c = await _engine_generate(
            query=SlayerQuery(
                source_model="orders", measures=["customers.score:rolling_avg"]
            ),
            model=_orders(),
            extra_models=[customers],
        )
        assert sql_f == sql_c
        assert "AVG(" in sql_f


class TestGlobalNameValidation:
    async def test_unknown_over_column_parity(self) -> None:
        e_func = await _binding_error("bogus(price)", model=_orders())
        e_colon = await _binding_error("price:bogus", model=_orders())
        assert type(e_func) is type(e_colon)
        assert str(e_func) == str(e_colon)
        assert "Unknown aggregation 'bogus'" in str(e_colon)

    async def test_star_bogus_colon_gets_standard_error(self) -> None:
        err = await _binding_error("*:bogus", model=_orders())
        assert "Unknown aggregation 'bogus'" in str(err)

    async def test_star_bogus_functional_gets_standard_error(self) -> None:
        err = await _binding_error("bogus(*)", model=_orders())
        assert "Unknown aggregation 'bogus'" in str(err)

    async def test_dotted_star_bogus_gets_standard_error(self) -> None:
        err = await _binding_error(
            "customers.*:bogus", model=_orders(), extra_models=[_customers()]
        )
        assert "Unknown aggregation 'bogus'" in str(err)

    async def test_avg_star_parity_error(self) -> None:
        e_func = await _binding_error("avg(*)", model=_orders())
        e_colon = await _binding_error("*:avg", model=_orders())
        assert type(e_func) is type(e_colon)
        assert str(e_func) == str(e_colon)

    async def test_unknown_agg_error_hints_scalar_near_miss(self) -> None:
        err = await _binding_error("rond(price)", model=_orders())
        msg = str(err)
        assert "round" in msg
        assert "scalar" in msg.lower()

    async def test_missing_weight_parameter_parity(self) -> None:
        e_func = await _binding_error("weighted_avg(price)", model=_orders())
        e_colon = await _binding_error("price:weighted_avg", model=_orders())
        assert type(e_func) is type(e_colon)
        assert str(e_func) == str(e_colon)
        assert "weight" in str(e_colon)

    async def test_missing_other_parameter_parity(self) -> None:
        e_func = await _binding_error("corr(price)", model=_orders())
        e_colon = await _binding_error("price:corr", model=_orders())
        assert type(e_func) is type(e_colon)
        assert str(e_func) == str(e_colon)
        assert "other" in str(e_colon)


class TestModeAUnchanged:
    async def test_raw_sql_aggregate_column_passthrough(self) -> None:
        # Mode-A column SQL is raw SQL — the functional parser must never
        # touch it.
        model = _orders()
        model = model.model_copy(
            update={
                "columns": [
                    *model.columns,
                    Column(name="grand_total", sql="SUM(revenue)", type=DataType.DOUBLE),
                ]
            }
        )
        sql = await _engine_generate(
            query=SlayerQuery(source_model="orders", measures=["grand_total:max"]),
            model=model,
        )
        assert "SUM(orders.revenue)" in sql


class TestScalarCollidingCustomAggNames:
    @pytest.mark.parametrize("bad", ["round", "iif", "coalesce"])
    def test_scalar_name_rejected(self, bad: str) -> None:
        with pytest.raises(ValueError) as ei:
            Aggregation(name=bad, formula="AVG({value})")
        msg = str(ei.value)
        assert bad in msg
        assert "scalar" in msg.lower()

    def test_transform_name_still_rejected(self) -> None:
        with pytest.raises(ValueError, match="cumsum"):
            Aggregation(name="cumsum", formula="AVG({value})")

    def test_regular_custom_name_accepted(self) -> None:
        Aggregation(name="my_agg", formula="AVG({value})")


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
            await storage.save_model(_orders())
            engine = SlayerQueryEngine(storage=storage)
            query = SlayerQuery(
                source_model="orders",
                measures=["sum(revenue)", "count(*)"],
                dimensions=["status"],
                filters=["sum(revenue) > 0"],
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
                measures=[ModelMeasure(name="rev", formula="sum(revenue)")]
            )
            with warnings_mod.catch_warnings():
                warnings_mod.simplefilter("error", SlayerNormalizationWarning)
                await engine.save_model(model)
            stored = await storage.get_model("orders", data_source="test")
        assert stored is not None
        assert stored.measures[0].formula == "sum(revenue)"


# ===========================================================================
# 1.6 — Entity refs: memories resolution and recommend_root_model.
# ===========================================================================


@pytest.fixture
async def refs_storage(tmp_path):
    storage = YAMLStorage(base_dir=str(tmp_path))
    await storage.save_datasource(
        DatasourceConfig(name="mydb", type="postgres", host="x")
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            data_source="mydb",
            sql_table="orders",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="status", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="cost", type=DataType.DOUBLE),
            ],
        )
    )
    return storage


class TestEntityRefsFunctional:
    async def test_functional_ref_parity(self, refs_storage) -> None:
        f = await resolve_entity("sum(orders.amount)", storage=refs_storage)
        c = await resolve_entity("orders.amount:sum", storage=refs_storage)
        assert f.canonical_forms == c.canonical_forms == ["mydb.orders.amount"]

    async def test_functional_star_count_resolves_to_model(self, refs_storage) -> None:
        f = await resolve_entity("count(orders.*)", storage=refs_storage)
        assert f.canonical_forms == ["mydb.orders"]

    async def test_expression_text_rejected(self, refs_storage) -> None:
        with pytest.raises(EntityResolutionError):
            await resolve_entity(
                "sum(orders.amount - orders.cost)", storage=refs_storage
            )

    async def test_extract_entities_parity(self, refs_storage) -> None:
        q_func = SlayerQuery(
            source_model="orders", measures=["sum(amount)"], dimensions=["status"]
        )
        q_colon = SlayerQuery(
            source_model="orders", measures=["amount:sum"], dimensions=["status"]
        )
        e_func = await extract_entities_from_query(q_func, storage=refs_storage)
        e_colon = await extract_entities_from_query(q_colon, storage=refs_storage)
        assert e_func.canonical_forms == e_colon.canonical_forms

    async def test_recommend_root_model_parity(self, refs_storage) -> None:
        engine = SlayerQueryEngine(storage=refs_storage)
        try:
            f = await engine.recommend_root_model(["sum(orders.amount)"])
            c = await engine.recommend_root_model(["orders.amount:sum"])
            assert f.root_model == c.root_model == "orders"
            assert [ip.path for ip in f.item_paths] == [ip.path for ip in c.item_paths]
        finally:
            await engine.aclose()

    async def test_recommend_expression_rejected(self, refs_storage) -> None:
        engine = SlayerQueryEngine(storage=refs_storage)
        try:
            with pytest.raises((ValueError, EntityResolutionError)):
                await engine.recommend_root_model(
                    ["sum(orders.amount - orders.cost)"]
                )
        finally:
            await engine.aclose()
