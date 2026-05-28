"""DEV-1450 stage 7b.14 — schema-drift measure-ref extraction via the typed
Mode-B parser.

Pins the migration of ``slayer/engine/schema_drift.py`` off the legacy
``the legacy formula parser`` / ``legacy mixed-arithmetic node`` field-spec union walk and onto
``parse_expr`` + ``walk_parsed_refs`` (the new scope-free typed parser).

``_measure_formula_refs(formula)`` stays a *best-effort textual* extractor:
it returns the set of column / measure names a formula references — possibly
dotted for cross-model refs — so the cascade attribution can check each one
against the dropped-column / dropped-measure sets WITHOUT binding the formula
against a scope (binding is impossible here: the refs being hunted are
exactly the ones about to be dropped, and bare named-measure refs require
planner-side expansion). See the 7b.14 design note on DEV-1450.

The behaviour pinned here matches the legacy walk exactly for every shape the
cascade tests exercise; the only intentional difference is that bare
named-measure refs surface by name (``aov``) instead of being inline-expanded,
which the cascade reaches via the dropped-measure set in a later fixed-point
pass.
"""

from __future__ import annotations

from slayer.engine.schema_drift import _measure_formula_refs
from slayer.engine.syntax import (
    AggCall,
    DottedRef,
    Ref,
    parse_expr,
    walk_parsed_refs,
)


class TestWalkParsedRefs:
    """The shared reference-node yielder over a ``ParsedExpr`` tree."""

    def test_bare_ref_yields_self(self) -> None:
        nodes = list(walk_parsed_refs(parse_expr("aov")))
        assert nodes == [Ref(name="aov")]

    def test_colon_agg_yields_aggcall_not_inner_ref(self) -> None:
        nodes = list(walk_parsed_refs(parse_expr("amount:sum")))
        assert nodes == [AggCall(source=Ref(name="amount"), agg="sum")]

    def test_star_count_yields_aggcall_with_star_source(self) -> None:
        nodes = list(walk_parsed_refs(parse_expr("*:count")))
        assert len(nodes) == 1
        assert isinstance(nodes[0], AggCall)
        assert nodes[0].agg == "count"

    def test_dotted_agg_yields_dotted_source(self) -> None:
        nodes = list(walk_parsed_refs(parse_expr("customers.revenue:sum")))
        assert len(nodes) == 1
        assert isinstance(nodes[0], AggCall)
        assert nodes[0].source == DottedRef(parts=("customers", "revenue"))

    def test_arithmetic_descends_both_operands(self) -> None:
        nodes = list(walk_parsed_refs(parse_expr("amount:sum / count_col:sum")))
        aggs = {
            n.source.name
            for n in nodes
            if isinstance(n, AggCall) and isinstance(n.source, Ref)
        }
        assert aggs == {"amount", "count_col"}

    def test_transform_descends_input_only(self) -> None:
        # partition_by kwarg columns are NOT yielded (legacy parity).
        nodes = list(
            walk_parsed_refs(parse_expr("cumsum(amount:sum, partition_by=region)"))
        )
        assert nodes == [AggCall(source=Ref(name="amount"), agg="sum")]

    def test_scalar_call_descends_args(self) -> None:
        nodes = list(walk_parsed_refs(parse_expr("coalesce(amount:sum, 0)")))
        assert nodes == [AggCall(source=Ref(name="amount"), agg="sum")]

    def test_transform_list_partition_by_parses_and_skips_kwargs(self) -> None:
        # parse_expr accepts list-valued kwargs; the walker still descends
        # only the transform input, so partition columns never surface.
        nodes = list(
            walk_parsed_refs(
                parse_expr("rank(amount:sum, partition_by=[region, channel])")
            )
        )
        assert nodes == [AggCall(source=Ref(name="amount"), agg="sum")]

    def test_agg_kwargs_not_descended(self) -> None:
        # weighted_avg(weight=quantity): only the aggregated source surfaces,
        # not the weight column (legacy parity — agg args/kwargs are opaque).
        nodes = list(
            walk_parsed_refs(parse_expr("price:weighted_avg(weight=quantity)"))
        )
        assert nodes == [
            AggCall(
                source=Ref(name="price"),
                agg="weighted_avg",
                kwargs=(("weight", Ref(name="quantity")),),
            )
        ]

    def test_nested_scalar_arith_agg_mixed(self) -> None:
        # The plan's marquee shape: coalesce(<arith over aggs>, 0).
        nodes = list(
            walk_parsed_refs(parse_expr("coalesce(amount:sum + revenue:sum, 0)"))
        )
        aggs = {
            n.source.name
            for n in nodes
            if isinstance(n, AggCall) and isinstance(n.source, Ref)
        }
        assert aggs == {"amount", "revenue"}


class TestMeasureFormulaRefs:
    """``_measure_formula_refs`` — the cascade's textual name extractor."""

    def test_simple_agg(self) -> None:
        assert _measure_formula_refs("amount:sum") == {"amount"}

    def test_star_count_excluded(self) -> None:
        # ``*`` is not a real column reference.
        assert _measure_formula_refs("*:count") == set()

    def test_cross_model_dotted(self) -> None:
        assert _measure_formula_refs("customers.revenue:sum") == {
            "customers.revenue"
        }

    def test_standalone_dotted_ref(self) -> None:
        # A dotted ref WITHOUT an aggregation surfaces as the dotted name.
        assert _measure_formula_refs("customers.revenue") == {
            "customers.revenue"
        }

    def test_bare_measure_name(self) -> None:
        # Bare named-measure ref surfaces by name (no inline expansion).
        assert _measure_formula_refs("aov") == {"aov"}

    def test_arithmetic_with_bare_and_star(self) -> None:
        assert _measure_formula_refs("total_amount / *:count") == {
            "total_amount"
        }

    def test_arithmetic_two_aggs(self) -> None:
        assert _measure_formula_refs("amount:sum / count_col:sum") == {
            "amount",
            "count_col",
        }

    def test_transform_inner_only(self) -> None:
        assert _measure_formula_refs("cumsum(amount:sum)") == {"amount"}

    def test_transform_list_partition_by_inner_only(self) -> None:
        # List-valued partition_by is documented Mode-B grammar; parse_expr
        # must accept it. Only the inner value's refs surface (legacy never
        # extracted the partition columns either).
        assert _measure_formula_refs(
            "rank(revenue:sum, partition_by=[status, customer_id])"
        ) == {"revenue"}

    def test_func_style_simple_agg_rewritten(self) -> None:
        # Function-style aggs on legacy formulas are rewritten to colon form.
        assert _measure_formula_refs("sum(amount)") == {"amount"}

    def test_func_style_count_star_excluded(self) -> None:
        assert _measure_formula_refs("count(*)") == set()

    def test_func_style_inside_arithmetic(self) -> None:
        assert _measure_formula_refs("sum(amount) / count(*)") == {"amount"}

    def test_mixed_transform_in_arithmetic(self) -> None:
        assert _measure_formula_refs("cumsum(amount:sum) / *:count") == {
            "amount"
        }

    def test_scalar_call_inner_agg(self) -> None:
        assert _measure_formula_refs("*:count / nullif(revenue:max, 0)") == {
            "revenue"
        }

    def test_scalar_call_over_arith_of_aggs(self) -> None:
        assert _measure_formula_refs("coalesce(amount:sum + revenue:sum, 0)") == {
            "amount",
            "revenue",
        }

    def test_scalar_call_bare_ref(self) -> None:
        # ScalarCall descends bare-ref args, not just agg-bearing ones.
        assert _measure_formula_refs("coalesce(aov, 0)") == {"aov"}

    def test_scalar_call_dotted_ref(self) -> None:
        assert _measure_formula_refs("nullif(customers.revenue, 0)") == {
            "customers.revenue"
        }

    def test_predicate_with_transform(self) -> None:
        # filter-shape formula (comparison wrapping a transform).
        assert _measure_formula_refs("change(amount:sum) > 0") == {"amount"}

    def test_boolop_descends_all_operands(self) -> None:
        assert _measure_formula_refs(
            "amount:sum > 0 and revenue:sum > 0"
        ) == {"amount", "revenue"}

    def test_unary_not_descends_operand(self) -> None:
        assert _measure_formula_refs("not (amount:sum > 0)") == {"amount"}

    def test_weighted_avg_kwarg_column_not_extracted(self) -> None:
        # Legacy parity: the weight column is not a cascade ref.
        assert _measure_formula_refs("price:weighted_avg(weight=quantity)") == {
            "price"
        }

    def test_malformed_returns_empty(self) -> None:
        assert _measure_formula_refs("this is not ) valid (") == set()

    def test_bare_dunder_identifier_returns_empty(self) -> None:
        # parse_expr rejects ``__`` in a bare AST identifier; best-effort
        # extraction swallows the error and returns the empty set.
        assert _measure_formula_refs("robot__details") == set()

    def test_colon_agg_dunder_source_is_extracted(self) -> None:
        # ``__`` inside a colon-agg SOURCE is captured pre-AST (not a bare
        # identifier), so parse_expr does NOT reject it — matching the legacy
        # walk and supporting persisted query-backed ``__``-named columns
        # (DEV-1450 C11).
        assert _measure_formula_refs("robot__details:sum") == {
            "robot__details"
        }
