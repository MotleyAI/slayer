"""Stage 4 (DEV-1450) — aggregation registry helpers.

Lifts agg-name collection and parameter resolution out of enrichment.py
and generator.py so the new binder modules don't have to reach into
those tangles. The helpers are pure: given a model + a resolve_join_target
callback, they produce structured results without touching storage or
spawning side maps.

Public surface:
- ``collect_reachable_agg_names(...)`` — BFS the join graph for custom
  aggregation names.
- ``resolve_aggregation(name, available_aggs)`` — find the Aggregation
  definition for a name (built-in or model-level custom).
- ``is_known_aggregation_name(name, custom_names)`` — built-in or
  in the custom set.
- ``required_params_for(agg_name)`` — required built-in params.
- ``merge_agg_params(agg_def, query_kwargs)`` — defaults + overrides.
"""

from __future__ import annotations

from slayer.core.enums import BUILTIN_AGGREGATIONS, DataType
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    SlayerModel,
)
from slayer.engine.agg_registry import (
    collect_reachable_agg_names,
    is_known_aggregation_name,
    merge_agg_params,
    required_params_for,
    resolve_aggregation,
)


def _make_model(name: str, *, aggs=None, joins=None) -> SlayerModel:
    return SlayerModel(
        name=name,
        data_source="prod",
        sql_table=name,
        columns=[Column(name="id", type=DataType.INT, primary_key=True)],
        aggregations=aggs or [],
        joins=joins or [],
    )


# ---------------------------------------------------------------------------
# collect_reachable_agg_names — BFS the join graph
# ---------------------------------------------------------------------------


class TestCollectReachableAggNames:
    async def test_empty_when_no_aggs_anywhere(self):
        m = _make_model("orders")

        async def resolve_join_target(*, target_model_name, named_queries):
            return None

        result = await collect_reachable_agg_names(
            m, resolve_join_target, named_queries={},
        )
        assert result is None

    async def test_returns_aggs_from_source_only(self):
        m = _make_model(
            "orders",
            aggs=[
                Aggregation(name="custom_sum", formula="SUM({value} * 2)"),
                Aggregation(name="custom_avg", formula="AVG({value})"),
            ],
        )

        async def resolve_join_target(*, target_model_name, named_queries):
            return None

        result = await collect_reachable_agg_names(
            m, resolve_join_target, named_queries={},
        )
        assert result == frozenset({"custom_sum", "custom_avg"})

    async def test_walks_join_graph(self):
        customers = _make_model(
            "customers",
            aggs=[Aggregation(name="weighted_score", formula="SUM({value}*{w})/SUM({w})", params=[
                AggregationParam(name="w", sql="weight"),
            ])],
        )
        orders = _make_model(
            "orders",
            joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        )

        async def resolve_join_target(*, target_model_name, named_queries):
            return (None, customers) if target_model_name == "customers" else None

        result = await collect_reachable_agg_names(
            orders, resolve_join_target, named_queries={},
        )
        assert result == frozenset({"weighted_score"})

    async def test_unioned_across_models(self):
        b = _make_model(
            "b", aggs=[Aggregation(name="agg_b", formula="SUM({value})")],
        )
        a = _make_model(
            "a",
            aggs=[Aggregation(name="agg_a", formula="SUM({value})")],
            joins=[ModelJoin(target_model="b", join_pairs=[["x", "id"]])],
        )

        async def resolve(*, target_model_name, named_queries):
            return (None, b) if target_model_name == "b" else None

        result = await collect_reachable_agg_names(a, resolve, named_queries={})
        assert result == frozenset({"agg_a", "agg_b"})

    async def test_cycle_safe(self):
        # If two models reference each other, the BFS visited-set must
        # prevent infinite loops.
        b = _make_model("b", aggs=[Aggregation(name="agg_b", formula="SUM({value})")])
        a = _make_model(
            "a",
            aggs=[Aggregation(name="agg_a", formula="SUM({value})")],
            joins=[ModelJoin(target_model="b", join_pairs=[["x", "id"]])],
        )
        # Mutate `b` to point back to `a`.
        b.joins = [ModelJoin(target_model="a", join_pairs=[["x", "id"]])]
        registry = {"a": a, "b": b}

        async def resolve(*, target_model_name, named_queries):
            m = registry.get(target_model_name)
            return (None, m) if m else None

        result = await collect_reachable_agg_names(a, resolve, named_queries={})
        assert result == frozenset({"agg_a", "agg_b"})


# ---------------------------------------------------------------------------
# is_known_aggregation_name
# ---------------------------------------------------------------------------


class TestIsKnownAggregationName:
    def test_builtin_is_known(self):
        assert is_known_aggregation_name("sum", None) is True
        assert is_known_aggregation_name("percentile", None) is True

    def test_custom_in_set_is_known(self):
        assert is_known_aggregation_name(
            "my_agg", frozenset({"my_agg", "other"}),
        ) is True

    def test_custom_not_in_set_is_unknown(self):
        assert is_known_aggregation_name(
            "absent", frozenset({"my_agg"}),
        ) is False

    def test_unknown_with_none_custom_is_unknown(self):
        assert is_known_aggregation_name("xyz", None) is False

    def test_all_builtins_are_recognized(self):
        for b in BUILTIN_AGGREGATIONS:
            assert is_known_aggregation_name(b, None) is True


# ---------------------------------------------------------------------------
# resolve_aggregation
# ---------------------------------------------------------------------------


class TestResolveAggregation:
    def test_custom_match(self):
        custom = Aggregation(name="my_agg", formula="SUM({value})")
        agg = resolve_aggregation("my_agg", [custom])
        assert agg is custom

    def test_overrides_picked_over_other_customs(self):
        a = Aggregation(name="a", formula="SUM({value})")
        b = Aggregation(name="b", formula="AVG({value})")
        assert resolve_aggregation("b", [a, b]) is b

    def test_missing_returns_none(self):
        assert resolve_aggregation("absent", []) is None

    def test_builtin_name_with_no_override(self):
        # "sum" is a built-in; with no model-level override, returns None
        # (caller knows it's a built-in and uses the default formula).
        assert resolve_aggregation("sum", []) is None

    def test_builtin_override_returned(self):
        # Model-level override for a built-in name takes precedence.
        override = Aggregation(
            name="weighted_avg",
            formula="SUM({value}*{weight})/SUM({weight})",
            params=[AggregationParam(name="weight", sql="quantity")],
        )
        assert resolve_aggregation("weighted_avg", [override]) is override


# ---------------------------------------------------------------------------
# required_params_for
# ---------------------------------------------------------------------------


class TestRequiredParamsFor:
    def test_builtin_with_required_params(self):
        # weighted_avg requires "weight"
        assert "weight" in required_params_for("weighted_avg")

    def test_builtin_with_no_required_params(self):
        assert required_params_for("sum") == ()
        assert required_params_for("count") == ()

    def test_unknown_returns_empty(self):
        # Custom aggregations declare their required-ness via Aggregation.params,
        # not via the built-in table.
        assert required_params_for("my_custom_agg") == ()


# ---------------------------------------------------------------------------
# merge_agg_params
# ---------------------------------------------------------------------------


class TestMergeAggParams:
    def test_no_agg_def_returns_query_kwargs(self):
        result = merge_agg_params(None, {"weight": "quantity"})
        assert result == {"weight": "quantity"}

    def test_defaults_from_agg_def(self):
        agg = Aggregation(
            name="my", formula="X",
            params=[AggregationParam(name="weight", sql="default_w")],
        )
        result = merge_agg_params(agg, {})
        assert result == {"weight": "default_w"}

    def test_query_overrides_defaults(self):
        agg = Aggregation(
            name="my", formula="X",
            params=[AggregationParam(name="weight", sql="default_w")],
        )
        result = merge_agg_params(agg, {"weight": "override_w"})
        assert result == {"weight": "override_w"}

    def test_partial_override(self):
        agg = Aggregation(
            name="my", formula="X",
            params=[
                AggregationParam(name="a", sql="default_a"),
                AggregationParam(name="b", sql="default_b"),
            ],
        )
        result = merge_agg_params(agg, {"b": "override_b"})
        assert result == {"a": "default_a", "b": "override_b"}

    def test_query_only_kwargs_pass_through(self):
        # Even params not declared by the agg_def are passed through;
        # validation belongs to the caller (binder).
        agg = Aggregation(name="my", formula="X", params=[])
        result = merge_agg_params(agg, {"window": "30d"})
        assert result == {"window": "30d"}


# ---------------------------------------------------------------------------
# Smoke: AggregationParam shape
# ---------------------------------------------------------------------------


class TestAggregationParamShape:
    def test_has_name_and_sql(self):
        p = AggregationParam(name="weight", sql="quantity")
        assert p.name == "weight"
        assert p.sql == "quantity"

    def test_param_can_be_dotted(self):
        # Custom agg params reference columns by name; multi-hop refs are
        # legal in the param SQL.
        p = AggregationParam(name="w", sql="customers.value")
        assert p.sql == "customers.value"


