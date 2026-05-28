"""DEV-1450 stage 7b.14 — memory entity-extraction via the typed Mode-B parser.

Pins the migration of ``slayer/memories/resolver.py`` off the legacy
``the legacy formula parser`` / ``legacy mixed-arithmetic node`` field-spec union walk and onto
``parse_expr`` + ``walk_parsed_refs``.

``_formula_entity_tokens(parsed)`` yields the entity *tokens* a measure
formula references — colon-syntax aggregations as ``"<source>:<agg>"`` and
bare / dotted refs as their textual form — which ``extract_entities_from_query``
then feeds one-by-one into ``resolve_entity``. No binding, no scope: the
resolver does its own canonicalisation downstream.

This is a focused unit test of the token yielder; the end-to-end
``extract_entities_from_query`` contract lives in ``test_entity_resolution.py``.
"""

from __future__ import annotations

from slayer.engine.normalization import func_style_agg_to_colon
from slayer.engine.syntax import parse_expr
from slayer.memories.resolver import _formula_entity_tokens


def _tokens(formula: str) -> list[str]:
    return list(_formula_entity_tokens(parse_expr(formula)))


class TestFormulaEntityTokens:
    def test_simple_agg(self) -> None:
        assert _tokens("amount:sum") == ["amount:sum"]

    def test_star_count(self) -> None:
        assert _tokens("*:count") == ["*:count"]

    def test_cross_model_dotted_agg(self) -> None:
        assert _tokens("customers.revenue:sum") == ["customers.revenue:sum"]

    def test_arithmetic_two_aggs(self) -> None:
        assert _tokens("amount:sum / *:count") == ["amount:sum", "*:count"]

    def test_transform_inner_agg(self) -> None:
        assert _tokens("cumsum(amount:sum)") == ["amount:sum"]

    def test_transform_partition_by_opaque(self) -> None:
        # partition_by columns are opaque — only the inner value surfaces.
        assert _tokens("cumsum(amount:sum, partition_by=region)") == [
            "amount:sum"
        ]

    def test_scalar_call_bare_ref(self) -> None:
        assert _tokens("coalesce(aov, 0)") == ["aov"]

    def test_scalar_over_arith_of_aggs(self) -> None:
        assert _tokens("coalesce(amount:sum + revenue:sum, 0)") == [
            "amount:sum",
            "revenue:sum",
        ]

    def test_predicate_with_transform(self) -> None:
        assert _tokens("change(amount:sum) > 0") == ["amount:sum"]

    def test_bare_ref_yields_name(self) -> None:
        # Bare named-measure / column ref surfaces as its textual name so
        # the resolver can canonicalise it (e.g. to ``<ds>.<model>.aov``).
        assert _tokens("aov") == ["aov"]

    def test_weighted_avg_kwarg_column_not_yielded(self) -> None:
        # Only the aggregated source surfaces; the weight column is opaque.
        assert _tokens("price:weighted_avg(weight=quantity)") == [
            "price:weighted_avg",
        ]

    def test_dotted_bare_ref(self) -> None:
        assert _tokens("customers.name") == ["customers.name"]

    def test_list_partition_by_parses(self) -> None:
        # parse_expr accepts list-valued partition_by; only the inner agg
        # token surfaces.
        assert _tokens(
            "rank(amount:sum, partition_by=[region, channel])"
        ) == ["amount:sum"]


class TestFuncStyleNormalization:
    """Function-style aggs are rewritten to colon form before parse_expr in
    ``extract_entities_from_query`` — pinned here at the composition level."""

    def _tokens_normalized(self, formula: str) -> list[str]:
        return list(
            _formula_entity_tokens(parse_expr(func_style_agg_to_colon(formula)))
        )

    def test_func_style_simple_agg(self) -> None:
        assert self._tokens_normalized("sum(amount)") == ["amount:sum"]

    def test_func_style_count_star(self) -> None:
        assert self._tokens_normalized("count(*)") == ["*:count"]

    def test_func_style_in_arithmetic(self) -> None:
        assert self._tokens_normalized("sum(amount) / count(*)") == [
            "amount:sum",
            "*:count",
        ]
