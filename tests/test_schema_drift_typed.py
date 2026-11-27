"""Stage 7b.14 — schema-drift metric-ref extraction via the typed
Mode-B parser.

Pins the migration of ``slayer/live_schema/schema_drift.py`` off the legacy
``the legacy formula parser`` / ``legacy mixed-arithmetic node`` field-spec union walk and onto
``parse_expr`` + ``walk_parsed_refs`` (the new scope-free typed parser).

``_metric_expression_refs(expression)`` stays a *best-effort textual* extractor:
it returns the set of field / metric names an expression references — possibly
dotted for cross-dataset refs — so the cascade attribution can check each one
against the dropped-field / dropped-metric sets WITHOUT binding the expression
against a scope (binding is impossible here: the refs being hunted are
exactly the ones about to be dropped, and bare named-metric refs require
planner-side expansion).

The behaviour pinned here matches the legacy walk exactly for every shape the
cascade tests exercise; the only intentional difference is that bare
named-metric refs surface by name (``aov``) instead of being inline-expanded,
which the cascade reaches via the dropped-metric set in a later fixed-point
pass.
"""

from __future__ import annotations

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import DatasetExtension, SlayerQuery
from slayer.core.relationship_search import reachable
from slayer.engine.extensions import QueryScope, query_scopes
from slayer.live_schema.schema_drift import (
    _attribute_ref_to_base,
    _filter_refs_dsl,
    _metric_expression_refs,
    _ref_edges,
)
from slayer.core.syntax import (
    AggCall,
    DottedRef,
    Ref,
    parse_expr,
    walk_parsed_refs,
)


def _ext_datasets() -> dict[str, SlayerDataset]:
    """The stage source and the extension relationships' target, as the runtime loads them."""
    orders = SlayerDataset(name="orders", data_source="ds", sql_table="orders", fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="customer_id", datatype=DataType.INTEGER),
        DatasetField(name="b_id", datatype=DataType.INTEGER), DatasetField(name="s_id", datatype=DataType.INTEGER)])
    customers = SlayerDataset(name="customers", data_source="ds", sql_table="customers", fields=[
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="name", datatype=DataType.STRING)])
    return {"orders": orders, "customers": customers}

def _stage_scope(stage: SlayerQuery, datasets_by_name: dict) -> QueryScope:
    return query_scopes([stage], datasets_by_ds={"ds": datasets_by_name}, data_source="ds")[0][0]



class TestStageGraphBidirectional:
    """The cascade graph and attribution traverse either direction."""

    def test_reverse_edge_reachable_and_attributed(self) -> None:
        orders = SlayerDataset(
            name="orders", data_source="ds", sql_table="orders",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="customer_id", datatype=DataType.INTEGER),
                DatasetField(name="amount", datatype=DataType.FLOAT),
            ],
            relationships=[DatasetRelationship(to="customers",
                             from_columns=["customer_id"], to_columns=["id"])],
        )
        customers = SlayerDataset(
            name="customers", data_source="ds", sql_table="customers",
            fields=[DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True)],
        )
        models = {"orders": orders, "customers": customers}
        graph = _stage_scope(SlayerQuery(population="customers"), models)
        assert graph.host is not None
        assert "orders" in reachable(root=graph.host, datasets_by_name=graph.datasets)
        assert _attribute_ref_to_base(
            ref="orders.amount", base_name="orders", scope=graph,
        ) == "amount"

    def test_named_extension_hop_attributes_by_name(self) -> None:
        stage = SlayerQuery(population="orders", extensions=[DatasetExtension(dataset="orders", relationships=[
            DatasetRelationship(to="customers",
                      from_columns=["customer_id"], to_columns=["id"], name="buyer"),
        ])])
        graph = _stage_scope(stage, _ext_datasets())
        assert _attribute_ref_to_base(
            ref="orders.buyer.name", base_name="customers", scope=graph,
        ) == "name"

    def test_parallel_extension_targets_unaddressable_by_bare_name(self) -> None:
        stage = SlayerQuery(population="orders", extensions=[DatasetExtension(dataset="orders", relationships=[
            DatasetRelationship(to="customers",
                      from_columns=["b_id"], to_columns=["id"], name="buyer"),
            DatasetRelationship(to="customers",
                      from_columns=["s_id"], to_columns=["id"], name="seller"),
        ])])
        graph = _stage_scope(stage, _ext_datasets())
        assert _attribute_ref_to_base(
            ref="orders.seller.name", base_name="customers", scope=graph,
        ) == "name"
        # The bare target token is ambiguous across the pair — no attribution.
        assert _attribute_ref_to_base(
            ref="customers.name", base_name="customers", scope=graph,
        ) is None

    def test_extension_repeating_a_stored_join_is_that_stored_edge(self) -> None:
        join = DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])
        models = _ext_datasets()
        models["orders"] = models["orders"].model_copy(update={"relationships": [join]})
        graph = _stage_scope(SlayerQuery(population="orders", extensions=[DatasetExtension(dataset="orders", relationships=[join])]), models)
        assert _attribute_ref_to_base(
            ref="customers.name", base_name="customers", scope=graph,
        ) == "name"
        assert [e.to for e in _ref_edges(ref="customers.name", scope=graph)] == ["customers"]


class TestWalkParsedRefs:
    """The shared reference-node yielder over a ``ParsedExpr`` tree."""

    def test_bare_ref_yields_self(self) -> None:
        nodes = list(walk_parsed_refs(parse_expr("aov")))
        assert nodes == [Ref(name="aov")]

    def test_agg_yields_aggcall_not_inner_ref(self) -> None:
        nodes = list(walk_parsed_refs(parse_expr("sum(amount)")))
        assert nodes == [AggCall(source=Ref(name="amount"), agg="sum")]

    def test_star_count_yields_aggcall_with_star_source(self) -> None:
        nodes = list(walk_parsed_refs(parse_expr("count(*)")))
        assert len(nodes) == 1
        assert isinstance(nodes[0], AggCall)
        assert nodes[0].agg == "count"

    def test_dotted_agg_yields_dotted_source(self) -> None:
        nodes = list(walk_parsed_refs(parse_expr("sum(customers.revenue)")))
        assert len(nodes) == 1
        assert isinstance(nodes[0], AggCall)
        assert nodes[0].source == DottedRef(parts=("customers", "revenue"))

    def test_arithmetic_descends_both_operands(self) -> None:
        nodes = list(walk_parsed_refs(parse_expr("sum(amount) / sum(count_col)")))
        aggs = {
            n.source.name
            for n in nodes
            if isinstance(n, AggCall) and isinstance(n.source, Ref)
        }
        assert aggs == {"amount", "count_col"}

    def test_transform_descends_input_only(self) -> None:
        # partition_by kwarg fields are NOT yielded (legacy parity).
        nodes = list(
            walk_parsed_refs(parse_expr("cumsum(sum(amount), partition_by=region)"))
        )
        assert nodes == [AggCall(source=Ref(name="amount"), agg="sum")]

    def test_scalar_call_descends_args(self) -> None:
        nodes = list(walk_parsed_refs(parse_expr("coalesce(sum(amount), 0)")))
        assert nodes == [AggCall(source=Ref(name="amount"), agg="sum")]

    def test_transform_list_partition_by_parses_and_skips_kwargs(self) -> None:
        # parse_expr accepts list-valued kwargs; the walker still descends
        # only the transform input, so partition fields never surface.
        nodes = list(
            walk_parsed_refs(
                parse_expr("rank(sum(amount), partition_by=[region, channel], direction='desc')")
            )
        )
        assert nodes == [AggCall(source=Ref(name="amount"), agg="sum")]

    def test_agg_kwargs_not_descended(self) -> None:
        # weighted_avg(weight=quantity): only the aggregated source surfaces,
        # not the weight field (legacy parity — agg args/kwargs are opaque).
        nodes = list(
            walk_parsed_refs(parse_expr("weighted_avg(price, weight=quantity)"))
        )
        assert len(nodes) == 1
        (agg,) = nodes
        assert isinstance(agg, AggCall)
        assert agg.source == Ref(name="price")
        assert agg.agg == "weighted_avg"

    def test_nested_scalar_arith_agg_mixed(self) -> None:
        # The plan's marquee shape: coalesce(<arith over aggs>, 0).
        nodes = list(
            walk_parsed_refs(parse_expr("coalesce(sum(amount) + sum(revenue), 0)"))
        )
        aggs = {
            n.source.name
            for n in nodes
            if isinstance(n, AggCall) and isinstance(n.source, Ref)
        }
        assert aggs == {"amount", "revenue"}


class TestMeasureFormulaRefs:
    """``_metric_expression_refs`` — the cascade's textual name extractor."""

    def test_simple_agg(self) -> None:
        assert _metric_expression_refs("sum(amount)") == {"amount"}

    def test_star_count_excluded(self) -> None:
        # ``*`` is not a real field reference.
        assert _metric_expression_refs("count(*)") == set()

    def test_cross_model_dotted(self) -> None:
        assert _metric_expression_refs("sum(customers.revenue)") == {
            "customers.revenue"
        }

    def test_standalone_dotted_ref(self) -> None:
        # A dotted ref WITHOUT an aggregation surfaces as the dotted name.
        assert _metric_expression_refs("customers.revenue") == {
            "customers.revenue"
        }

    def test_bare_measure_name(self) -> None:
        # Bare named-metric ref surfaces by name (no inline expansion).
        assert _metric_expression_refs("aov") == {"aov"}

    def test_arithmetic_with_bare_and_star(self) -> None:
        assert _metric_expression_refs("total_amount / count(*)") == {
            "total_amount"
        }

    def test_arithmetic_two_aggs(self) -> None:
        assert _metric_expression_refs("sum(amount) / sum(count_col)") == {
            "amount",
            "count_col",
        }

    def test_transform_inner_only(self) -> None:
        assert _metric_expression_refs("cumsum(sum(amount))") == {"amount"}

    def test_transform_list_partition_by_inner_only(self) -> None:
        # List-valued partition_by is documented Mode-B grammar; parse_expr
        # must accept it. Only the inner value's refs surface (legacy never
        # extracted the partition fields either).
        assert _metric_expression_refs(
            "rank(sum(revenue), partition_by=[status, customer_id], direction='desc')"
        ) == {"revenue"}

    def test_func_style_simple_agg_rewritten(self) -> None:
        assert _metric_expression_refs("sum(amount)") == {"amount"}

    def test_func_style_count_star_excluded(self) -> None:
        assert _metric_expression_refs("count(*)") == set()

    def test_func_style_inside_arithmetic(self) -> None:
        assert _metric_expression_refs("sum(amount) / count(*)") == {"amount"}

    def test_mixed_transform_in_arithmetic(self) -> None:
        assert _metric_expression_refs("cumsum(sum(amount)) / count(*)") == {
            "amount"
        }

    def test_scalar_call_inner_agg(self) -> None:
        assert _metric_expression_refs("count(*) / nullif(max(revenue), 0)") == {
            "revenue"
        }

    def test_scalar_call_over_arith_of_aggs(self) -> None:
        assert _metric_expression_refs("coalesce(sum(amount) + sum(revenue), 0)") == {
            "amount",
            "revenue",
        }

    def test_scalar_call_bare_ref(self) -> None:
        # ScalarCall descends bare-ref args, not just agg-bearing ones.
        assert _metric_expression_refs("coalesce(aov, 0)") == {"aov"}

    def test_scalar_call_dotted_ref(self) -> None:
        assert _metric_expression_refs("nullif(customers.revenue, 0)") == {
            "customers.revenue"
        }

    def test_predicate_with_transform(self) -> None:
        # filter-shape expression (comparison wrapping a transform).
        assert _metric_expression_refs("change(sum(amount)) > 0") == {"amount"}

    def test_boolop_descends_all_operands(self) -> None:
        assert _metric_expression_refs(
            "sum(amount) > 0 and sum(revenue) > 0"
        ) == {"amount", "revenue"}

    def test_unary_not_descends_operand(self) -> None:
        assert _metric_expression_refs("not (sum(amount) > 0)") == {"amount"}

    def test_weighted_avg_kwarg_column_not_extracted(self) -> None:
        # Legacy parity: the weight field is not a cascade ref.
        assert _metric_expression_refs("weighted_avg(price, weight=quantity)") == {
            "price"
        }

    def test_malformed_returns_empty(self) -> None:
        assert _metric_expression_refs("this is not ) valid (") == set()

    def test_bare_dunder_identifier_is_extracted(self) -> None:
        # A bare ``robot__details`` ref is extracted like any other (matching the aggregation-source case below).
        assert _metric_expression_refs("robot__details") == {"robot__details"}

    def test_agg_dunder_source_is_extracted(self) -> None:
        # Supports persisted query-backed ``__``-named fields.
        assert _metric_expression_refs("sum(robot__details)") == {
            "robot__details"
        }


class TestFilterRefsDslParity:
    """Parity suite pinned while migrating ``_filter_refs_dsl`` onto
    ``parse_expr`` (verified green on the legacy implementation first).
    Set equality only: reference order is expression order."""

    def test_funcstyle_builtin_agg(self) -> None:
        assert set(_filter_refs_dsl("sum(revenue) > 100")) == {"revenue"}

    def test_agg_alias(self) -> None:
        assert set(_filter_refs_dsl("countd(revenue) > 5")) == {"revenue"}

    def test_custom_agg(self) -> None:
        assert set(_filter_refs_dsl("my_custom_agg(revenue) > 5")) == {"revenue"}

    def test_dotted_path(self) -> None:
        assert set(_filter_refs_dsl("customers.regions.name == 'EU'")) == {
            "customers.regions.name"
        }

    def test_dotted_agg(self) -> None:
        assert set(_filter_refs_dsl("sum(customers.revenue) > 10")) == {
            "customers.revenue"
        }

    def test_star_count_excluded(self) -> None:
        assert set(_filter_refs_dsl("count(*) > 3")) == set()

    def test_like_filter(self) -> None:
        assert set(_filter_refs_dsl("name LIKE 'a%'")) == {"name"}

    def test_plain_columns(self) -> None:
        assert set(_filter_refs_dsl("status == 'x' and amount > 5")) == {
            "status",
            "amount",
        }

    def test_agg_and_column_mixed(self) -> None:
        assert set(_filter_refs_dsl("sum(amount) > 100 and region == 'EU'")) == {
            "amount",
            "region",
        }

    def test_unparseable_returns_empty(self) -> None:
        assert _filter_refs_dsl("this is (( not parseable") == []


class TestFilterRefsDslMigrationGains:
    """Refs only the migrated ``parse_expr`` implementation surfaces."""

    def test_funcstyle_custom_agg(self) -> None:
        assert set(_filter_refs_dsl("my_custom_agg(revenue) > 5")) == {"revenue"}

    def test_expression_agg_source(self) -> None:
        assert set(_filter_refs_dsl("sum(amount - cost) > 0")) == {
            "amount",
            "cost",
        }
