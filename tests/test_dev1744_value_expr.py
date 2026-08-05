"""P-G "same construct, same SQL": the single ValueKey→AST renderer.

``slayer/sql/generator.py`` contains FIVE independent ValueKey renderers (the
issue text says four; the fifth is the outer-wrapper copy):

* R1 ``_render_value_key_for_filter``            — host WHERE / HAVING
* R2 ``_render_filter_value_key_in_target_scope``— cross-model CTE routed filters
* R3 ``_render_value_key_against_aliases``       — POST-phase / alias space
* R4 ``_render_filter_for_outer_wrapper``        — outer combined WHERE
* R5 ``_render_aggregate_composite_expr``        — AGGREGATE-phase composites

…plus three literal renderers and three arithmetic composers riding along. They
have drifted, and B5 is the sharpest instance: R1 and R4 emit scalar calls as
``exp.Anonymous`` passthrough while R2/R3/R5 build a typed node and let the
dialect transpile it. The same ``ScalarCallKey`` therefore reaches Postgres as
``IFNULL(...)`` from a filter (Postgres has no ``IFNULL``) and as
``COALESCE(...)`` from a projection.

This module pins the replacement: ONE ``render_value_key(key, ctx)`` in
``slayer/sql/render/value_expr.py``, parameterised by an explicit
``RenderContext`` and failing closed when the context lacks a facility a key
kind needs. Materialisation stays on ``ScopeFrame`` (P-B) — the renderer
anchors leaves through ``scope.resolve(ref, consumer=...)`` and never by hand.

Also pinned here: B10 (``ScopeFrame._model_for`` raises instead of silently
substituting the root model) and the aggregation registry that replaces
``_build_agg``'s five dispatch mechanisms.

Scope note: this PR defines the COMPLETE context API, including
``consumer=``, but migrates only SAME-SCOPE call sites (R1 filters, R5
composites). R2/R3/R4 migrate in PR 3, which also adds the production-path
proof that ``resolve(consumer=...)`` is exercised — an unused API does not
establish P-B. The ``consumer`` tests here are therefore renderer-level.

B5 nuance discovered while writing these tests: "uppercase + dialect transpile"
alone is NOT the whole policy. Building ``exp.func("LOG10", x)`` yields a
generic ``exp.Log(10, x)`` that re-emits as ``LOG(10, x)`` — which is wrong for
the dialects that have a native single-arg ``LOG10``
(``SqlDialect.should_use_native_log``). The generator already owns that rewrite
(``_rewrite_log_aliases``) but applies it only inside ``_parse`` /
``_parse_predicate``, never to AST-built calls, so R3 gets ``log10`` WRONG today
while R1's Anonymous passthrough gets it right. The unified policy is therefore
transpile-then-log-rewrite, which is the only form that is correct for both
``ifnull`` and ``log10``.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from decimal import Decimal
from typing import AsyncIterator, Optional

import pytest
import sqlglot
from sqlglot import exp

from slayer.core.enums import BUILTIN_AGGREGATIONS, DataType
from slayer.core.errors import (
    RenderContextMissingFacilityError,
    UnknownReferenceError,
)
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    LiteralKey,
    ScalarCallKey,
    SqlExprKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
)
from slayer.core.models import (
    Aggregation,
    Column,
    DatasourceConfig,
    ModelJoin,
    ModelMeasure,
    SlayerModel,
)
from slayer.core.query import ColumnRef, SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import get_dialect
from slayer.sql.generator import SQLGenerator
from slayer.sql.naming import AliasAllocator
from slayer.sql.render.aggregates import resolve_agg_entry, window_agg_class
from slayer.sql.render.value_expr import (
    AliasFacilities,
    contains_aggregate,
    CompositeFacilities,
    FilterFacilities,
    RenderContext,
    _literal,
    render_arithmetic,
    render_value_key,
)
from slayer.sql.scope import ScopeFrame
from slayer.storage.yaml_storage import YAMLStorage


# ===========================================================================
# Models + scope construction (mirrors tests/test_scope.py's idiom).
# ===========================================================================


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="name", sql="name", type=DataType.TEXT),
            Column(name="population", sql="population", type=DataType.DOUBLE),
        ],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", sql="region_id", type=DataType.DOUBLE),
            Column(name="balance", sql="balance", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
            Column(name="label", sql="label", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
            Column(name="net", sql="amount - 1", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
    )


def _scope(
    host: Optional[SlayerModel] = None,
    *others: SlayerModel,
    dialect: str = "postgres",
    allocator: Optional[AliasAllocator] = None,
) -> ScopeFrame:
    host = host or _orders()
    others = others or (_customers(), _regions())
    alloc = allocator or AliasAllocator()
    bundle = ResolvedSourceBundle(
        source_model=host, referenced_models=[host, *others],
    )
    return ScopeFrame(
        scope_id=alloc.next_scope_id(host.name),
        root_model=host, root_relation=host.name,
        bundle=bundle, dialect=get_dialect(dialect), allocator=alloc,
    )


def _filter_ctx(dialect: str = "postgres", **kw):
    """A RenderContext carrying the FILTER facility group (R1's call family)."""

    scope = _scope(dialect=dialect)
    return RenderContext(
        scope=scope,
        dialect=scope.dialect,
        filters=FilterFacilities(**kw),
    )


def _composite_ctx(dialect: str = "postgres", **kw):
    """A RenderContext carrying the COMPOSITE facility group (R5's family)."""

    scope = _scope(dialect=dialect)
    return RenderContext(
        scope=scope,
        dialect=scope.dialect,
        composites=CompositeFacilities(**kw),
    )


def _sql(expr: exp.Expression, dialect: str = "postgres") -> str:
    return expr.sql(dialect=dialect)


# ===========================================================================
# B10 — ScopeFrame._model_for raises on an unknown model.
# ===========================================================================


class TestB10UnknownModelRaises:
    """Today ``_model_for`` ends in ``or self.root_model``: a ``ColumnSqlKey``
    naming a model absent from the bundle silently expands the ROOT model's
    derived SQL instead. That turns a wiring bug into a wrong answer — the
    query runs and returns numbers computed from the wrong model."""

    def test_unknown_model_in_columnsqlkey_raises(self) -> None:

        scope = _scope()
        key = ColumnSqlKey(model="not_in_bundle", column_name="net")
        with pytest.raises(UnknownReferenceError):
            scope.resolve(key)

    def test_error_names_the_missing_model(self) -> None:
        """The message must be actionable: which model was asked for, what the
        scope root is, and what the bundle actually knows."""

        scope = _scope()
        key = ColumnSqlKey(model="not_in_bundle", column_name="net")
        with pytest.raises(UnknownReferenceError) as excinfo:
            scope.resolve(key)
        message = str(excinfo.value)
        assert "not_in_bundle" in message
        assert "orders" in message

    def test_known_models_still_resolve(self) -> None:
        """Parity guard: the root model and every bundle member keep working —
        B10 removes only the silent FALLBACK, not the lookup."""
        scope = _scope()
        expr = scope.resolve(ColumnSqlKey(model="orders", column_name="net"))
        assert "amount" in _sql(expr)

    def test_root_model_lookup_does_not_consult_the_bundle(self) -> None:
        """A scope whose root model is not listed in ``referenced_models`` must
        still resolve its own root — the first branch of ``_model_for``."""
        host = _orders()
        alloc = AliasAllocator()
        bundle = ResolvedSourceBundle(
            source_model=host, referenced_models=[_customers(), _regions()],
        )
        scope = ScopeFrame(
            scope_id=alloc.next_scope_id(host.name),
            root_model=host, root_relation=host.name,
            bundle=bundle, dialect=get_dialect("postgres"), allocator=alloc,
        )
        expr = scope.resolve(ColumnSqlKey(model="orders", column_name="net"))
        assert "amount" in _sql(expr)


# ===========================================================================
# The RenderContext API.
# ===========================================================================


class TestRenderContextApi:
    def test_context_holds_real_production_objects(self) -> None:
        """Pydantic v2 + a ``ScopeFrame`` / dialect strategy / sqlglot nodes
        needs ``arbitrary_types_allowed``; constructing with the real objects
        (not stubs) is what proves the config is right."""

        scope = _scope()
        ctx = RenderContext(scope=scope, dialect=scope.dialect)
        assert ctx.scope is scope
        assert ctx.consumer is None
        assert ctx.filters is None
        assert ctx.composites is None
        assert ctx.aliases is None

    def test_consumer_defaults_to_none_and_is_accepted(self) -> None:
        """The P-B seam exists in PR 1 even though its production callers
        arrive in PR 3."""

        producer, consumer = _scope(), _scope()
        ctx = RenderContext(
            scope=producer, consumer=consumer, dialect=producer.dialect,
        )
        assert ctx.consumer is consumer

    @pytest.mark.parametrize(
        "label,key",
        [
            ("local_column", ColumnKey(leaf="amount")),
            ("joined_column", ColumnKey(path=("customers",), leaf="balance")),
            ("derived_column", ColumnSqlKey(model="orders", column_name="net")),
        ],
    )
    def test_consumer_routes_column_like_leaves_through_materialization(
        self, label, key,
    ) -> None:
        """With a consumer named, EVERY column-like leaf must come back as a
        BARE materialisation alias and be projected in the PRODUCING scope —
        the single Law-2 mechanism, not a second one grown inside the renderer.

        Parametrised over all three column-like kinds because a renderer that
        special-cases one of them would otherwise slip through."""

        producer, consumer = _scope(), _scope()
        ctx = RenderContext(
            scope=producer, consumer=consumer, dialect=producer.dialect,
        )
        out = render_value_key(key, ctx)
        assert isinstance(out, exp.Column), f"{label}: got {type(out).__name__}"
        assert out.table == "", f"{label}: expected a bare alias, got {_sql(out)}"
        assert len(producer.materializations) == 1
        assert producer.materializations[0].alias == _sql(out)

    def test_materializations_apply_to_the_producing_select(self) -> None:
        """The other half of the P-B contract: what the renderer records must
        actually be projectable via ``apply_materializations``, so the consumer's
        bare alias resolves to a real column of the producing SELECT."""

        producer, consumer = _scope(), _scope()
        ctx = RenderContext(
            scope=producer, consumer=consumer, dialect=producer.dialect,
        )
        alias = _sql(render_value_key(ColumnKey(leaf="amount"), ctx))
        select = producer.apply_materializations(
            exp.Select().from_(exp.to_table("orders")),
        )
        assert alias in [p.alias_or_name for p in select.expressions], (
            f"{alias!r} not projected by the producing scope: "
            f"{select.sql(dialect='postgres')}"
        )

    def test_materialization_dedups_within_a_scope(self) -> None:
        """Two renders of the same key across the same boundary share ONE
        ``_val_<n>`` — the dedup key is the producing scope + anchored AST +
        dialect, and the renderer must not defeat it by re-anchoring."""

        producer, consumer = _scope(), _scope()
        ctx = RenderContext(
            scope=producer, consumer=consumer, dialect=producer.dialect,
        )
        a = render_value_key(ColumnKey(leaf="amount"), ctx)
        b = render_value_key(ColumnKey(leaf="amount"), ctx)
        assert _sql(a) == _sql(b)
        assert len(producer.materializations) == 1

    def test_join_paths_register_as_a_side_effect_of_rendering(self) -> None:
        """P-A: join discovery is a side effect of rendering, never a separate
        pass. Rendering a joined leaf must register the crossed path on the
        scope without the caller asking."""

        scope = _scope()
        ctx = RenderContext(scope=scope, dialect=scope.dialect)
        render_value_key(
            ColumnKey(path=("customers", "regions"), leaf="name"), ctx,
        )
        assert scope.join_paths.as_list() == [
            ("customers",), ("customers", "regions"),
        ]

    def test_missing_facility_fails_closed(self) -> None:
        """A key kind that needs a facility the context lacks must RAISE, not
        silently degrade. Silent degradation is how the five copies drifted in
        the first place."""

        scope = _scope()
        bare = RenderContext(scope=scope, dialect=scope.dialect)
        # A POST-phase transform can only be rendered against already-
        # materialised aliases, which live in the ALIAS facility group.
        key = TransformKey(
            op="time_shift",
            input=AggregateKey(source=ColumnKey(leaf="amount"), agg="sum"),
        )
        with pytest.raises(RenderContextMissingFacilityError):
            render_value_key(key, bare)

    def test_missing_facility_error_names_the_key_and_facility(self) -> None:

        scope = _scope()
        bare = RenderContext(scope=scope, dialect=scope.dialect)
        key = TransformKey(
            op="time_shift",
            input=AggregateKey(source=ColumnKey(leaf="amount"), agg="sum"),
        )
        with pytest.raises(RenderContextMissingFacilityError) as excinfo:
            render_value_key(key, bare)
        message = str(excinfo.value)
        assert "TransformKey" in message
        assert "aliases" in message.lower(), (
            f"the error must name the MISSING facility, not just the key: "
            f"{message!r}"
        )

    def test_aggregate_without_composite_facilities_fails_closed(self) -> None:
        """Fail-closed is asserted per facility group, not once.

        A renderer could easily fail closed on the alias group (the branch the
        test above covers) while silently degrading on the composite group —
        which is the drift mode this whole PR exists to prevent. An
        ``AggregateKey`` needs the composite facilities (rn-suffix maps,
        resolved agg kwargs, composite alias map) to render faithfully.
        """

        scope = _scope()
        bare = RenderContext(scope=scope, dialect=scope.dialect)
        key = AggregateKey(source=ColumnKey(leaf="amount"), agg="first")
        with pytest.raises(RenderContextMissingFacilityError) as excinfo:
            render_value_key(key, bare)
        assert "composite" in str(excinfo.value).lower(), str(excinfo.value)

    def test_filtered_aggregate_without_a_builder_fails_closed(self) -> None:
        """A column filter must not vanish.

        The generator wraps a filtered aggregate as
        ``SUM(CASE WHEN <filter> THEN col END)``. Rendering it from ``agg`` and
        ``source`` alone drops the filter and covers rows it must exclude —
        a wrong number rather than an error, so the no-builder path refuses it.
        """

        key = AggregateKey(
            source=ColumnKey(leaf="amount"),
            agg="sum",
            column_filter_key=SqlExprKey(canonical_sql="status = 'new'"),
        )
        ctx = _composite_ctx()
        with pytest.raises(RenderContextMissingFacilityError):
            render_value_key(key, ctx)

    def test_parametric_aggregate_without_a_builder_fails_closed(self) -> None:
        """Same rule for args/kwargs, which need the generator's parameter
        resolution."""

        key = AggregateKey(
            source=ColumnKey(leaf="amount"),
            agg="sum",
            kwargs=(("window", "90d"),),
        )
        ctx = _composite_ctx()
        with pytest.raises(RenderContextMissingFacilityError):
            render_value_key(key, ctx)

    def test_cross_model_star_without_a_builder_fails_closed(self) -> None:
        """``customers.*:count`` counts rows of the JOINED relation.

        ``StarKey.path`` carries that hop, and routing it needs the join graph.
        Emitting a bare ``*`` would count HOST rows instead — a wrong number,
        the same failure class as a dropped column filter.
        """
        key = AggregateKey(source=StarKey(path=("customers",)), agg="count")
        ctx = _composite_ctx()
        with pytest.raises(RenderContextMissingFacilityError):
            render_value_key(key, ctx)

    def test_local_star_still_renders(self) -> None:
        """The guard must not catch the ordinary local ``*:count``."""
        key = AggregateKey(source=StarKey(), agg="count")
        assert _sql(render_value_key(key, _composite_ctx())) == "COUNT(*)"

    def test_transform_key_renders_when_alias_facilities_are_supplied(
        self,
    ) -> None:
        """The positive half of the fail-closed pair.

        PR 1 defines the COMPLETE context API even though the production call
        sites for alias-space rendering migrate in PR 3, so a ``TransformKey``
        WITH its facility must render here — otherwise "fails closed" would be
        indistinguishable from "not implemented".
        """

        scope = _scope()
        agg = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum")
        key = TransformKey(op="time_shift", input=agg)
        ctx = RenderContext(
            scope=scope,
            dialect=scope.dialect,
            aliases=AliasFacilities(
                slot_id_by_key={key: "s1"},
                available_alias_by_slot_id={"s1": "orders.amount_sum_shifted"},
            ),
        )
        out = render_value_key(key, ctx)
        assert "amount_sum_shifted" in _sql(out), _sql(out)


# ===========================================================================
# Coverage of the whole ValueKey union.
# ===========================================================================


class TestRendersEveryKeyKind:
    """The union is closed (11 members). One renderer means every member is
    handled in one place — an unhandled kind must raise, never fall through to
    a bare ``None`` or a stringified repr."""

    def test_local_column_key(self) -> None:

        out = render_value_key(ColumnKey(leaf="amount"), _filter_ctx())
        assert _sql(out) == "orders.amount"

    def test_joined_column_key_anchors_at_the_path_alias(self) -> None:

        out = render_value_key(
            ColumnKey(path=("customers",), leaf="balance"), _filter_ctx(),
        )
        assert _sql(out) == "customers.balance"

    def test_multi_hop_column_key(self) -> None:

        out = render_value_key(
            ColumnKey(path=("customers", "regions"), leaf="name"),
            _filter_ctx(),
        )
        assert _sql(out) == "customers__regions.name"

    def test_column_sql_key_expands_the_derived_expression(self) -> None:
        """Exact SQL, not a substring check: ``net`` is ``amount - 1``, and the
        expansion must be anchored at the scope root."""

        out = render_value_key(
            ColumnSqlKey(model="orders", column_name="net"), _filter_ctx(),
        )
        assert _sql(out) == "orders.amount - 1"

    def test_time_trunc_key(self) -> None:
        """Exact per-dialect SQL — a substring check would accept a truncation
        at the wrong granularity or over the wrong column."""

        key = TimeTruncKey(
            column=ColumnKey(leaf="created_at"), granularity="month",
        )
        out = render_value_key(key, _filter_ctx("postgres"))
        assert _sql(out, "postgres") == "DATE_TRUNC('MONTH', orders.created_at)"

    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "tsql", "bigquery"])
    def test_time_trunc_goes_through_the_dialect_strategy(self, dialect) -> None:
        """Truncation must use the dialect's own wire form, not a literal
        ``DATE_TRUNC``.

        SQLite has no ``DATE_TRUNC`` — it needs ``STRFTIME`` — and T-SQL spells
        it ``DATETRUNC``. Emitting the Postgres form everywhere produces SQL
        the backend rejects, which is the same one-construct-two-renderings
        defect this module exists to remove.
        """

        key = TimeTruncKey(
            column=ColumnKey(leaf="created_at"), granularity="month",
        )
        out = _sql(render_value_key(key, _filter_ctx(dialect)), dialect)
        if dialect == "sqlite":
            assert "DATE_TRUNC" not in out.upper(), out
            assert "STRFTIME" in out.upper(), out
        assert "created_at" in out, out

    def test_week_sunday_granularity_renders(self) -> None:
        """``week_sunday`` is a supported granularity with its own day-shift.

        A hardcoded unit table would have no entry for it and would emit
        ``DATE_TRUNC('WEEK_SUNDAY', col)``, which no dialect accepts.
        """

        key = TimeTruncKey(
            column=ColumnKey(leaf="created_at"), granularity="week_sunday",
        )
        out = _sql(render_value_key(key, _filter_ctx("postgres")), "postgres")
        assert "WEEK_SUNDAY" not in out.upper(), out
        assert "created_at" in out, out

    def test_literal_key_variants(self) -> None:

        ctx = _filter_ctx()
        assert _sql(render_value_key(LiteralKey(value=Decimal(3)), ctx)) == "3"
        assert _sql(render_value_key(LiteralKey(value="x"), ctx)) == "'x'"
        assert _sql(
            render_value_key(LiteralKey(value=None), ctx)
        ).upper() == "NULL"

    def test_star_key(self) -> None:

        out = render_value_key(StarKey(), _filter_ctx())
        assert isinstance(out, exp.Star)

    def test_arithmetic_key(self) -> None:

        out = render_value_key(
            ArithmeticKey(
                op="+",
                operands=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(1))),
            ),
            _filter_ctx(),
        )
        assert _sql(out) == "orders.amount + 1"

    def test_comparison_arithmetic_key(self) -> None:

        out = render_value_key(
            ArithmeticKey(
                op=">",
                operands=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(5))),
            ),
            _filter_ctx(),
        )
        assert _sql(out) == "orders.amount > 5"

    def test_between_key(self) -> None:

        out = render_value_key(
            BetweenKey(
                column=ColumnKey(leaf="amount"),
                low=LiteralKey(value=Decimal(1)),
                high=LiteralKey(value=Decimal(9)),
            ),
            _filter_ctx(),
        )
        assert _sql(out) == "orders.amount BETWEEN 1 AND 9"

    def test_in_key(self) -> None:

        out = render_value_key(
            InKey(
                column=ColumnKey(leaf="label"),
                values=(LiteralKey(value="a"), LiteralKey(value="b")),
            ),
            _filter_ctx(),
        )
        assert _sql(out) == "orders.label IN ('a', 'b')"

    def test_negated_in_key(self) -> None:

        out = render_value_key(
            InKey(
                column=ColumnKey(leaf="label"),
                values=(LiteralKey(value="a"),),
                negated=True,
            ),
            _filter_ctx(),
        )
        # Exact form: a bare "NOT" substring would also match e.g. an
        # IS NOT NULL wrapper that got the predicate wrong.
        assert _sql(out) == "NOT orders.label IN ('a')"

    def test_local_aggregate_key(self) -> None:

        out = render_value_key(
            AggregateKey(source=ColumnKey(leaf="amount"), agg="sum"),
            _composite_ctx(),
        )
        assert _sql(out) == "SUM(orders.amount)"

    def test_star_count_aggregate_key(self) -> None:

        out = render_value_key(
            AggregateKey(source=StarKey(), agg="count"), _composite_ctx(),
        )
        assert _sql(out) == "COUNT(*)"

    def test_unary_minus_keeps_its_sign(self) -> None:
        """The binder represents ``-10`` as a SINGLE-operand ``ArithmeticKey``.

        A fold that starts at ``operands[0]`` and iterates ``operands[1:]``
        never runs its body for one operand and returns it unchanged, so
        ``amount > -10`` would silently become ``amount > 10`` — a wrong
        result, not a failure.
        """

        out = render_value_key(
            ArithmeticKey(op="-", operands=(LiteralKey(value=Decimal(10)),)),
            _filter_ctx(),
        )
        assert _sql(out) == "-10"

    def test_unary_not(self) -> None:

        out = render_value_key(
            ArithmeticKey(
                op="not",
                operands=(
                    ArithmeticKey(
                        op=">",
                        operands=(
                            ColumnKey(leaf="amount"),
                            LiteralKey(value=Decimal(5)),
                        ),
                    ),
                ),
            ),
            _filter_ctx(),
        )
        assert _sql(out) == "NOT orders.amount > 5"

    @pytest.mark.parametrize(
        "key,expected",
        [
            # (a + b) * c — sqlglot does NOT parenthesise by nesting, so
            # without explicit Paren nodes this emits "a + b * c", which
            # evaluates differently.
            (
                ArithmeticKey(
                    op="*",
                    operands=(
                        ArithmeticKey(
                            op="+",
                            operands=(
                                ColumnKey(leaf="amount"),
                                LiteralKey(value=Decimal(1)),
                            ),
                        ),
                        LiteralKey(value=Decimal(2)),
                    ),
                ),
                "(orders.amount + 1) * 2",
            ),
            # a - (b - c): equal precedence on the RIGHT of a non-associative
            # operator still needs parens.
            (
                ArithmeticKey(
                    op="-",
                    operands=(
                        ColumnKey(leaf="amount"),
                        ArithmeticKey(
                            op="-",
                            operands=(
                                LiteralKey(value=Decimal(3)),
                                LiteralKey(value=Decimal(1)),
                            ),
                        ),
                    ),
                ),
                "orders.amount - (3 - 1)",
            ),
            # Higher-precedence child needs NO parens — don't over-wrap.
            (
                ArithmeticKey(
                    op="+",
                    operands=(
                        ColumnKey(leaf="amount"),
                        ArithmeticKey(
                            op="*",
                            operands=(
                                LiteralKey(value=Decimal(2)),
                                LiteralKey(value=Decimal(3)),
                            ),
                        ),
                    ),
                ),
                "orders.amount + 2 * 3",
            ),
        ],
        ids=["lower_prec_child", "right_of_non_associative", "higher_prec_child"],
    )
    def test_arithmetic_precedence_is_parenthesised(self, key, expected) -> None:
        """Operator precedence has to be materialised as ``Paren`` nodes."""

        ctx = _filter_ctx()
        assert _sql(render_value_key(key, ctx)) == expected

    def test_unsupported_literal_type_raises(self) -> None:
        """An unrecognised Python value must not become a quoted string.

        Asserted on the helper directly: the key types' own Pydantic validation
        already rejects such a value, so this is defence in depth rather than a
        reachable path. It matters because the generator's equivalent helper
        raises, and the paths converge in a later PR — a silent
        ``str(value)`` there would turn a loud failure into a wrong value.
        """
        value = datetime(2024, 1, 1)
        with pytest.raises(NotImplementedError):
            _literal(value)

    def test_supported_literal_types_still_render(self) -> None:
        """The fail-closed branch must not swallow the supported cases."""

        assert _literal(None).sql() == "NULL"
        assert _literal(True).sql() == "TRUE"
        assert _literal(Decimal("1.5")).sql() == "1.5"
        assert _literal("x").sql() == "'x'"

    def test_unhandled_kind_raises_notimplementederror(self) -> None:
        """Fail closed on anything outside the union rather than returning a
        stringified repr into the SQL.

        Pinned to ONE exact exception type (matching the existing generator
        renderers' convention) and to a message naming the offending type —
        accepting a tuple of types would let an incidental TypeError from
        somewhere else inside the renderer satisfy this test.
        """

        ctx = _filter_ctx()
        with pytest.raises(NotImplementedError) as excinfo:
            render_value_key(object(), ctx)  # type: ignore[arg-type]
        assert "object" in str(excinfo.value)


# ===========================================================================
# B5 — one ScalarCall render policy everywhere.
# ===========================================================================


# (function name, args, expected postgres SQL, expected tsql SQL)
#
# The expectations encode the UNIFIED policy: uppercase → typed sqlglot node →
# dialect transpile → log-alias rewrite. Where that differs from what R1 emits
# today, the difference IS B5.
#
# Identifiers are unquoted here: bracket / double-quote wrapping is a separate
# emit-time pass over the finished statement, not part of value rendering.
_SCALAR_MATRIX = [
    # R1 today: IFNULL(...) — invalid on Postgres, which has no IFNULL.
    ("ifnull", (ColumnKey(leaf="amount"), LiteralKey(value=Decimal(0))),
     "COALESCE(orders.amount, 0)", "COALESCE(orders.amount, 0)"),
    # R1 today: CONCAT(...) verbatim on every dialect; the operator differs.
    ("concat", (ColumnKey(leaf="label"), LiteralKey(value="x")),
     "orders.label || 'x'", "orders.label + 'x'"),
    # R1 today: LENGTH(...) on T-SQL, which spells it LEN.
    ("length", (ColumnKey(leaf="label"),),
     "LENGTH(orders.label)", "LEN(orders.label)"),
]


class TestB5ScalarCallPolicy:
    @pytest.mark.parametrize(
        "name,args,expected_pg,expected_tsql",
        _SCALAR_MATRIX,
        ids=[m[0] for m in _SCALAR_MATRIX],
    )
    def test_scalar_calls_transpile_per_dialect(
        self, name, args, expected_pg, expected_tsql,
    ) -> None:

        key = ScalarCallKey(name=name, args=args)
        assert _sql(
            render_value_key(key, _filter_ctx("postgres")), "postgres",
        ) == expected_pg
        assert _sql(
            render_value_key(key, _filter_ctx("tsql")), "tsql",
        ) == expected_tsql

    def test_ifnull_never_reaches_postgres_unmapped(self) -> None:
        """The headline B5 bug, stated as the invariant rather than as an exact
        string: Postgres has no ``IFNULL``, so emitting it is broken SQL."""

        key = ScalarCallKey(
            name="ifnull",
            args=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(0))),
        )
        out = _sql(render_value_key(key, _filter_ctx("postgres")), "postgres")
        assert "IFNULL" not in out.upper(), out

    def test_log10_keeps_the_native_single_arg_alias(self) -> None:
        """The other half of the policy, and the reason "transpile" alone is
        the wrong rule.

        ``exp.func("LOG10", x)`` normalises to a generic ``Log(10, x)`` that
        re-emits as ``LOG(10, x)``. Every Tier-1/2 dialect but Oracle has a
        native single-arg ``LOG10``, which is why the generator carries
        ``_rewrite_log_aliases``. Applying transpile WITHOUT that rewrite would
        regress ``log10`` — so the unified renderer must apply both.
        """

        key = ScalarCallKey(name="log10", args=(ColumnKey(leaf="amount"),))
        out = _sql(render_value_key(key, _filter_ctx("postgres")), "postgres")
        assert out.upper().startswith("LOG10("), out

    def test_round_keeps_the_dev1576_postgres_cast(self) -> None:
        """Parity guard: two-arg ROUND on Postgres needs the numeric cast, and
        it is the ONE scalar call R1 already routed through the typed path.
        Unifying must not lose it."""

        key = ScalarCallKey(
            name="round",
            args=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(0))),
        )
        out = _sql(render_value_key(key, _filter_ctx("postgres")), "postgres")
        assert "CAST" in out.upper(), out
        assert "DECIMAL" in out.upper(), out

    def test_like_stays_the_sql_operator(self) -> None:
        """``like(value, pattern)`` is the one allowlist member that is an
        OPERATOR, not a function call. Both legacy paths special-case it; the
        unified renderer keeps that."""

        key = ScalarCallKey(
            name="like",
            args=(ColumnKey(leaf="label"), LiteralKey(value="x%")),
        )
        out = render_value_key(key, _filter_ctx("postgres"))
        assert isinstance(out, exp.Like)
        assert _sql(out) == "orders.label LIKE 'x%'"

    def test_nested_scalar_calls_use_one_policy_throughout(self) -> None:
        """The policy applies at every depth — a nested call must not fall back
        to the passthrough branch."""

        key = ScalarCallKey(
            name="ifnull",
            args=(
                ScalarCallKey(name="length", args=(ColumnKey(leaf="label"),)),
                LiteralKey(value=Decimal(0)),
            ),
        )
        out = _sql(render_value_key(key, _filter_ctx("tsql")), "tsql")
        assert "COALESCE" in out.upper(), out
        assert "LEN(" in out.upper(), out
        assert "IFNULL" not in out.upper(), out
        assert "LENGTH" not in out.upper(), out


class TestLogAliasPolicyIsShared:
    """The log-alias rule lives in ONE place.

    The generator had its own copy of exactly this rule — same ``exp.Log``
    guard, same literal-base checks, same ``Anonymous`` output. Two copies of a
    policy the module docstring calls load-bearing is the drift this PR exists
    to remove, so the generator delegates and these tests pin that it still
    agrees with the shared implementation.
    """

    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "tsql", "bigquery"])
    def test_generator_delegates_to_the_shared_policy(self, dialect) -> None:
        from slayer.sql.generator import SQLGenerator
        from slayer.sql.render.value_expr import rewrite_log_alias

        gen = SQLGenerator(dialect=dialect)
        node = exp.Log(
            this=exp.Literal.number("10"), expression=exp.column("x"),
        )
        via_generator = gen._rewrite_log_aliases(node.copy())
        via_shared = rewrite_log_alias(node.copy(), dialect=gen._dialect)
        assert via_generator.sql(dialect=dialect) == via_shared.sql(dialect=dialect)

    def test_generator_parse_path_still_emits_native_log10(self) -> None:
        """Behavioural companion: the delegation must not lose the rewrite that
        the generator applies over parsed trees."""
        from slayer.sql.generator import SQLGenerator

        gen = SQLGenerator(dialect="postgres")
        out = gen._parse("log10(x)").sql(dialect="postgres")
        assert out.upper().startswith("LOG10("), out

    def test_non_log_nodes_pass_through_untouched(self) -> None:
        from slayer.sql.render.value_expr import rewrite_log_alias

        from slayer.sql.dialects import get_dialect

        node = exp.column("x")
        assert rewrite_log_alias(node, dialect=get_dialect("postgres")) is node


class TestPGSameConstructSameSql:
    """P-G proper: a given ValueKey renders identically wherever it appears.

    The contexts differ (filter facilities vs composite facilities) but the
    rendering POLICY must not branch on them. Any divergence here is the class
    of bug the five copies produced.
    """

    _KEYS = [
        ("column", ColumnKey(leaf="amount")),
        ("joined_column", ColumnKey(path=("customers",), leaf="balance")),
        ("derived_column", ColumnSqlKey(model="orders", column_name="net")),
        ("literal", LiteralKey(value=Decimal(7))),
        ("scalar_call", ScalarCallKey(
            name="ifnull",
            args=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(0))),
        )),
        ("nested_scalar_call", ScalarCallKey(
            name="upper",
            args=(ScalarCallKey(name="trim", args=(ColumnKey(leaf="label"),)),),
        )),
        ("arithmetic", ArithmeticKey(
            op="*",
            operands=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(2))),
        )),
        ("in_predicate", InKey(
            column=ColumnKey(leaf="label"), values=(LiteralKey(value="a"),),
        )),
        ("between_predicate", BetweenKey(
            column=ColumnKey(leaf="amount"),
            low=LiteralKey(value=Decimal(1)),
            high=LiteralKey(value=Decimal(2)),
        )),
    ]

    @pytest.mark.parametrize("label,key", _KEYS, ids=[k[0] for k in _KEYS])
    @pytest.mark.parametrize("dialect", ["postgres", "sqlite", "tsql", "bigquery"])
    def test_same_key_same_sql_across_contexts(
        self, label, key, dialect,
    ) -> None:

        in_filter = _sql(
            render_value_key(key, _filter_ctx(dialect)), dialect,
        )
        in_composite = _sql(
            render_value_key(key, _composite_ctx(dialect)), dialect,
        )
        assert in_filter == in_composite, (
            f"{label} renders differently by context on {dialect}: "
            f"filter={in_filter!r} composite={in_composite!r}"
        )


# ===========================================================================
# The aggregation registry (replaces _build_agg's five dispatch mechanisms).
# ===========================================================================


class TestAggregationRegistry:
    """``_build_agg`` reaches its builders five different ways: a hardcoded
    name pair for first/last, ``_AGG_FUNCTION_MAP`` plus a SECOND inline
    ``agg_class_map``, a frozenset membership test for the stat aggregates,
    name equality for the dialect-hook aggregates, and a formula-template
    fallback. One registry table replaces all five."""

    # Frozen INDEPENDENTLY of both BUILTIN_AGGREGATIONS and the registry, one
    # per former dispatch mechanism, so the coverage assertion cannot become
    # tautological if the registry were ever used to build the enum (or vice
    # versa). Each name must resolve AND be reachable through the old
    # mechanism's builder.
    _REQUIRED_BY_MECHANISM = {
        "first_last_case": ["first", "last"],
        "agg_function_map": ["count", "sum", "avg", "min", "max"],
        "stat_frozenset": [
            "stddev_samp", "stddev_pop", "var_samp", "var_pop",
            "corr", "covar_samp", "covar_pop",
        ],
        "dialect_hook": [
            "percentile", "median", "count_distinct", "count_distinct_approx",
        ],
        "formula_template": ["weighted_avg"],
    }

    def test_every_builtin_resolves(self) -> None:

        for name in sorted(BUILTIN_AGGREGATIONS):
            assert resolve_agg_entry(name).name == name

    @pytest.mark.parametrize(
        "mechanism,names",
        sorted(_REQUIRED_BY_MECHANISM.items()),
        ids=sorted(_REQUIRED_BY_MECHANISM),
    )
    def test_each_former_dispatch_mechanism_is_represented(
        self, mechanism, names,
    ) -> None:
        """One registry table must subsume all five mechanisms — an
        implementation that only ported the easy ``_AGG_FUNCTION_MAP`` entries
        would still pass ``test_every_builtin_resolves`` if the enum happened
        to be small."""

        for name in names:
            assert resolve_agg_entry(name).name == name, mechanism

    def test_required_names_are_really_builtins(self) -> None:
        """Guard on the frozen table's own premise, so a rename in the enum
        surfaces here rather than silently shrinking coverage."""
        for names in self._REQUIRED_BY_MECHANISM.values():
            for name in names:
                assert name in BUILTIN_AGGREGATIONS, name

    def test_unknown_aggregation_raises(self) -> None:

        with pytest.raises(ValueError):
            resolve_agg_entry("definitely_not_an_aggregation")

    def test_windowable_flags_are_exact(self) -> None:
        """Only ``sum`` and ``avg`` are windowable today — that is precisely
        what ``stage_planner`` gates on, and the registry must agree with it
        rather than restating it."""

        assert resolve_agg_entry("sum").windowable is True
        assert resolve_agg_entry("avg").windowable is True
        for name in ("count", "min", "max", "median", "percentile", "first"):
            assert resolve_agg_entry(name).windowable is False, name

    def test_window_agg_class_replaces_the_hardcode(self) -> None:

        assert window_agg_class("sum") is exp.Sum
        assert window_agg_class("avg") is exp.Avg

    def test_registry_and_builtins_agree_both_ways(self) -> None:
        """The import-time invariant, asserted in both directions.

        A missing built-in would fall through to the custom-formula path. A
        registry key that is NOT a built-in — a typo such as ``sumn`` — is the
        subtler half: ``is_builtin_agg`` would accept it and route it AWAY from
        that path, so the typo would render as if it were a real aggregation.
        """
        from slayer.sql.render.aggregates import AGG_REGISTRY

        assert set(AGG_REGISTRY) == set(BUILTIN_AGGREGATIONS)

    def test_non_windowable_aggregation_fails_closed(self) -> None:
        """The generator's windowed path currently reads
        ``exp.Sum if plan.agg == "sum" else exp.Avg`` — a silent catch-all that
        renders ANY other aggregation as AVG. It is unreachable through the
        planner today, which is exactly why it would stay silently wrong.

        Approved divergence: it raises instead.
        """

        for name in ("median", "count", "min", "max", "percentile"):
            with pytest.raises(ValueError):
                window_agg_class(name)


# ===========================================================================
# End-to-end: the migrated call-site families keep working.
# ===========================================================================


async def _e2e_engine(*, base_dir: str, dialect: str = "sqlite") -> SlayerQueryEngine:
    d = base_dir
    db_path = os.path.join(d, "ve.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, "
        "amount REAL, disc REAL, qty REAL, created_at TEXT)"
    )
    cur.executemany(
        "INSERT INTO orders VALUES (?,?,?,?,?,?)",
        [
            (1, "new", 10.0, None, 2.0, "2024-01-01"),
            (2, "new", 20.0, 5.0, 4.0, "2024-02-01"),
            (3, "old", 30.0, None, 1.0, "2024-01-15"),
        ],
    )
    con.commit()
    con.close()

    storage = YAMLStorage(base_dir=os.path.join(d, "store"))
    await storage.save_datasource(
        DatasourceConfig(name="prod", type=dialect, database=db_path)
    )
    await storage.save_model(
        SlayerModel(
            name="orders",
            sql_table="orders",
            data_source="prod",
            # first/last needs a resolvable ranking time column.
            default_time_dimension="created_at",
            columns=[
                Column(name="id", type=DataType.INT, primary_key=True),
                Column(name="status", type=DataType.TEXT),
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="disc", type=DataType.DOUBLE),
                Column(name="qty", type=DataType.DOUBLE),
                Column(name="created_at", type=DataType.TIMESTAMP),
            ],
            # Exercises the formula-template dispatch mechanism through the
            # registry — a CUSTOM aggregation, not a builtin.
            aggregations=[
                Aggregation(name="sum_sq", formula="SUM({value} * {value})"),
            ],
        )
    )
    return SlayerQueryEngine(storage=storage)


@pytest.fixture
async def e2e(tmp_path_factory) -> AsyncIterator[SlayerQueryEngine]:
    yield await _e2e_engine(base_dir=str(tmp_path_factory.mktemp("ve")))


class TestMigratedCallSitesEndToEnd:
    async def test_scalar_call_in_a_row_filter_executes(self, e2e) -> None:
        """R1's family (host WHERE) after migration. ``disc`` is NULL for two
        rows, so ``ifnull(disc, 0) > 1`` must keep exactly the one row where
        ``disc = 5``."""
        resp = await e2e.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="*:count")],
                filters=["ifnull(disc, 0) > 1"],
            )
        )
        assert len(resp.data) == 1
        assert resp.data[0]["orders.status"] == "new"
        assert resp.data[0]["orders._count"] == 1

    async def test_scalar_call_composite_projection_executes(self, e2e) -> None:
        """R5's family (AGGREGATE-phase composite): a scalar call WRAPPING an
        aggregate. Sums: new = 30, old = 30."""
        resp = await e2e.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="ifnull(amount:sum, 0)", name="m")],
            )
        )
        by_status = {r["orders.status"]: r["orders.m"] for r in resp.data}
        assert by_status == {"new": 30.0, "old": 30.0}

    async def test_arithmetic_composite_of_two_aggregates(self, e2e) -> None:
        """The composite family's core shape: arithmetic over two aggregates
        must stay ONE inline expression, not two materialised slots."""
        resp = await e2e.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    ModelMeasure(formula="amount:sum - qty:sum", name="d"),
                ],
            )
        )
        by_status = {r["orders.status"]: r["orders.d"] for r in resp.data}
        assert by_status == {"new": 24.0, "old": 29.0}

    async def test_custom_formula_aggregation_still_dispatches(
        self, e2e,
    ) -> None:
        """The registry must keep the formula-template mechanism reachable for
        aggregations that are NOT builtins — a closed builtin table would drop
        them. new = 10² + 20² = 500; old = 30² = 900."""
        resp = await e2e.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="amount:sum_sq", name="ss")],
            )
        )
        by_status = {r["orders.status"]: r["orders.ss"] for r in resp.data}
        assert by_status == {"new": 500.0, "old": 900.0}

    async def test_having_filter_over_an_aggregate_executes(self, e2e) -> None:
        """AGGREGATE-phase filter (HAVING) — the other half of R1's family."""
        resp = await e2e.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="qty:sum", name="q")],
                filters=["qty:sum > 5"],
            )
        )
        assert [r["orders.status"] for r in resp.data] == ["new"]
        assert resp.data[0]["orders.q"] == 6.0

    async def test_first_last_composite_call_site_executes(self, e2e) -> None:
        """R5's SECOND production call site (``_build_first_last_base_select``,
        the generator) — reached only by a first/last measure, which builds
        its own ranked base SELECT rather than the ordinary composite one.

        Ordered by ``id``: new -> last amount 20, old -> 30."""
        resp = await e2e.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="amount:last", name="la")],
            )
        )
        by_status = {r["orders.status"]: r["orders.la"] for r in resp.data}
        assert by_status == {"new": 20.0, "old": 30.0}

    async def test_scalar_call_inside_a_first_last_composite(self, e2e) -> None:
        """…and the same call site carrying a SCALAR CALL, so a legacy copy
        surviving on that route could not pass unnoticed."""
        resp = await e2e.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[
                    ModelMeasure(formula="ifnull(amount:last, 0)", name="la"),
                ],
            )
        )
        by_status = {r["orders.status"]: r["orders.la"] for r in resp.data}
        assert by_status == {"new": 20.0, "old": 30.0}


class TestOuterWrapperAndShiftedCteFamilies:
    """The two remaining migrated/patched routes, which the ordinary WHERE and
    HAVING tests do not reach.

    * R4 ``_render_filter_for_outer_wrapper`` — the outer combined
      SELECT. R4 is NOT migrated in PR 1 (it is cross-scope, and moves in
      PR 3), but B5 says "everywhere", so its scalar branch IS patched here.
    * R1's shifted-CTE WHERE call site (the generator) — reached only by a
      ``time_shift`` transform, never by a plain host filter.
    """

    async def _engine(self, tmp_path_factory, *, dialect: str = "sqlite") -> SlayerQueryEngine:
        d = str(tmp_path_factory.mktemp("routes"))
        db_path = os.path.join(d, "routes.db")
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute(
            "CREATE TABLE regions (id INTEGER PRIMARY KEY, tier TEXT)"
        )
        cur.executemany(
            "INSERT INTO regions VALUES (?,?)", [(1, "gold"), (2, "silver")],
        )
        cur.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, region_id INTEGER, "
            "status TEXT, amount REAL, disc REAL, created_at TEXT)"
        )
        cur.executemany(
            "INSERT INTO orders VALUES (?,?,?,?,?,?)",
            [
                (1, 1, "new", 100.0, None, "2024-01-15"),
                (2, 1, "new", 200.0, 5.0, "2024-02-15"),
                (3, 2, "old", 300.0, None, "2024-01-20"),
                (4, 2, "old", 400.0, 7.0, "2024-02-20"),
            ],
        )
        con.commit()
        con.close()

        storage = YAMLStorage(base_dir=os.path.join(d, "store"))
        await storage.save_datasource(
            DatasourceConfig(name="prod", type=dialect, database=db_path)
        )
        await storage.save_model(
            SlayerModel(
                name="regions", sql_table="regions", data_source="prod",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="tier", type=DataType.TEXT),
                ],
            )
        )
        await storage.save_model(
            SlayerModel(
                name="orders", sql_table="orders", data_source="prod",
                default_time_dimension="created_at",
                columns=[
                    Column(name="id", type=DataType.INT, primary_key=True),
                    Column(name="region_id", type=DataType.INT),
                    Column(name="status", type=DataType.TEXT),
                    Column(name="amount", type=DataType.DOUBLE),
                    Column(name="disc", type=DataType.DOUBLE),
                    Column(name="created_at", type=DataType.TIMESTAMP),
                    # Join-crossing Column.filter => filtered-local
                    # isolation => the outer combined-SELECT wrapper (R4).
                    Column(
                        name="gold_amount", sql="amount",
                        type=DataType.DOUBLE,
                        filter="regions.tier = 'gold'",
                    ),
                ],
                joins=[
                    ModelJoin(
                        target_model="regions",
                        join_pairs=[["region_id", "id"]],
                    ),
                ],
            )
        )
        return SlayerQueryEngine(storage=storage)

    async def test_outer_wrapper_filter_over_an_isolated_aggregate(
        self, tmp_path_factory,
    ) -> None:
        """R4's route: an AGGREGATE-phase filter on a filtered-local isolated
        aggregate renders as plain WHERE on the joined-back column of the outer
        combined SELECT. Gold totals: new = 300, old = 0/NULL — so a threshold
        of 100 keeps only ``new``."""
        engine = await self._engine(tmp_path_factory)
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="gold_amount:sum", name="g")],
                filters=["gold_amount:sum > 100"],
            )
        )
        assert [r["orders.status"] for r in resp.data] == ["new"]
        assert resp.data[0]["orders.g"] == 300.0

    async def test_outer_wrapper_filter_carrying_a_scalar_call(self, tmp_path_factory) -> None:
        """B5 on R4's route specifically — the patched scalar branch. Wrapping
        the same comparison in ``ifnull`` must not change which rows survive,
        and (on a dialect without IFNULL) must not emit it."""
        engine = await self._engine(tmp_path_factory)
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="gold_amount:sum", name="g")],
                filters=["ifnull(gold_amount:sum, 0) > 100"],
            )
        )
        assert [r["orders.status"] for r in resp.data] == ["new"]
        assert resp.data[0]["orders.g"] == 300.0

    async def test_outer_wrapper_scalar_call_is_transpiled_on_postgres(
        self, tmp_path_factory,
    ) -> None:
        """The emission half of the same case, and the B5 bug in its sharpest
        form: R4 passes scalar calls through as ``exp.Anonymous``, so the
        literal ``IFNULL`` reaches POSTGRES — which has no such function, making
        this generated SQL simply invalid there.

        Postgres-typed datasource + ``dry_run`` (no execution): the point is
        what is EMITTED for that backend. Asserted on Postgres rather than
        SQLite deliberately — SQLite does have ``IFNULL``, so the same
        assertion there would be a policy preference rather than a correctness
        claim.
        """
        engine = await self._engine(tmp_path_factory, dialect="postgres")
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="gold_amount:sum", name="g")],
                filters=["ifnull(gold_amount:sum, 0) > 100"],
            ),
            dry_run=True,
        )
        assert "IFNULL" not in resp.sql.upper(), resp.sql
        assert "COALESCE" in resp.sql.upper(), resp.sql

    async def test_shifted_cte_filter_call_site_executes(self, tmp_path_factory) -> None:
        """R1's SECOND call site (the ``time_shift`` CTE's WHERE, that call site).

        A host filter must apply inside the shifted CTE as well as the host
        base, so the shifted value is computed over the same filtered rows.
        With ``status = 'new'`` only, January's total is 100 and February's
        shifted-by-one-month value must be that same 100.
        """
        from slayer.core.enums import TimeGranularity
        from slayer.core.query import TimeDimension

        engine = await self._engine(tmp_path_factory)
        resp = await engine.execute(
            SlayerQuery(
                source_model="orders",
                time_dimensions=[
                    TimeDimension(
                        dimension=ColumnRef(name="created_at"),
                        granularity=TimeGranularity.MONTH,
                    ),
                ],
                measures=[
                    ModelMeasure(formula="amount:sum", name="amt"),
                    ModelMeasure(
                        formula="time_shift(amount:sum, -1, 'month')",
                        name="prev",
                    ),
                ],
                filters=["status = 'new'"],
            )
        )
        rows = sorted(resp.data, key=lambda r: str(r["orders.created_at"]))
        assert len(rows) == 2, rows
        assert rows[0]["orders.amt"] == 100.0
        assert rows[1]["orders.amt"] == 200.0
        # February's shifted value is January's filtered total.
        assert rows[1]["orders.prev"] == 100.0


# ===========================================================================
# Meta: no key field may be silently ignored.
# ===========================================================================


def _mutation_cases():
    """(label, base_key, mutated_key) triples — each pair differs in ONE field.

    Every renderer bug found while reviewing this PR was the same shape: the
    key carried a field, a render path ignored it, and the output was silently
    WRONG rather than an error. A dropped ``column_filter_key`` made an
    aggregate cover rows the filter excluded; a dropped ``StarKey.path``
    counted host rows instead of the joined relation; a dropped unary operand
    turned ``-10`` into ``10``.

    So the general invariant is: changing a field must change the rendered SQL
    (or make the render refuse). Two keys that differ but render identically
    means that field vanished.
    """
    col = ColumnKey(leaf="amount")
    other = ColumnKey(leaf="label")
    return [
        ("ColumnKey.leaf", col, other),
        ("ColumnKey.path", col, ColumnKey(path=("customers",), leaf="amount")),
        (
            "ColumnSqlKey.column_name",
            ColumnSqlKey(model="orders", column_name="net"),
            ColumnSqlKey(model="orders", column_name="amount"),
        ),
        (
            "TimeTruncKey.granularity",
            TimeTruncKey(column=col, granularity="month"),
            TimeTruncKey(column=col, granularity="year"),
        ),
        (
            "TimeTruncKey.column",
            TimeTruncKey(column=col, granularity="month"),
            TimeTruncKey(column=other, granularity="month"),
        ),
        ("StarKey.path", StarKey(), StarKey(path=("customers",))),
        (
            "LiteralKey.value",
            LiteralKey(value=Decimal(1)), LiteralKey(value=Decimal(2)),
        ),
        (
            "ArithmeticKey.op",
            ArithmeticKey(op="+", operands=(col, LiteralKey(value=Decimal(1)))),
            ArithmeticKey(op="-", operands=(col, LiteralKey(value=Decimal(1)))),
        ),
        (
            "ArithmeticKey.operands",
            ArithmeticKey(op="+", operands=(col, LiteralKey(value=Decimal(1)))),
            ArithmeticKey(op="+", operands=(col, LiteralKey(value=Decimal(9)))),
        ),
        (
            "ArithmeticKey.operand_arity",
            ArithmeticKey(op="-", operands=(LiteralKey(value=Decimal(10)),)),
            ArithmeticKey(
                op="-",
                operands=(LiteralKey(value=Decimal(10)), LiteralKey(value=Decimal(0))),
            ),
        ),
        (
            "ArithmeticKey.nesting",
            ArithmeticKey(
                op="*",
                operands=(
                    ArithmeticKey(op="+", operands=(col, LiteralKey(value=Decimal(1)))),
                    LiteralKey(value=Decimal(2)),
                ),
            ),
            ArithmeticKey(
                op="+",
                operands=(
                    col,
                    ArithmeticKey(
                        op="*",
                        operands=(LiteralKey(value=Decimal(1)), LiteralKey(value=Decimal(2))),
                    ),
                ),
            ),
        ),
        (
            "ScalarCallKey.name",
            ScalarCallKey(name="lower", args=(col,)),
            ScalarCallKey(name="upper", args=(col,)),
        ),
        (
            "ScalarCallKey.args",
            ScalarCallKey(name="lower", args=(col,)),
            ScalarCallKey(name="lower", args=(other,)),
        ),
        (
            "BetweenKey.low",
            BetweenKey(column=col, low=LiteralKey(value=Decimal(1)),
                       high=LiteralKey(value=Decimal(9))),
            BetweenKey(column=col, low=LiteralKey(value=Decimal(2)),
                       high=LiteralKey(value=Decimal(9))),
        ),
        (
            "InKey.values",
            InKey(column=col, values=(LiteralKey(value="a"),)),
            InKey(column=col, values=(LiteralKey(value="b"),)),
        ),
        (
            "InKey.negated",
            InKey(column=col, values=(LiteralKey(value="a"),)),
            InKey(column=col, values=(LiteralKey(value="a"),), negated=True),
        ),
        (
            "AggregateKey.agg",
            AggregateKey(source=col, agg="sum"),
            AggregateKey(source=col, agg="min"),
        ),
        (
            "AggregateKey.source",
            AggregateKey(source=col, agg="sum"),
            AggregateKey(source=other, agg="sum"),
        ),
        (
            "AggregateKey.column_filter_key",
            AggregateKey(source=col, agg="sum"),
            AggregateKey(
                source=col, agg="sum",
                column_filter_key=SqlExprKey(canonical_sql="status = 'new'"),
            ),
        ),
        (
            "AggregateKey.kwargs",
            AggregateKey(source=col, agg="sum"),
            AggregateKey(source=col, agg="sum", kwargs=(("window", "90d"),)),
        ),
        (
            "AggregateKey.star_path",
            AggregateKey(source=StarKey(), agg="count"),
            AggregateKey(source=StarKey(path=("customers",)), agg="count"),
        ),
    ]


_MUTATIONS = _mutation_cases()


class TestNoKeyFieldIsSilentlyIgnored:
    """The generalisation of every renderer bug this PR's review surfaced.

    A field that does not reach the emitted SQL is a wrong-value bug waiting
    to happen: the query runs, returns numbers, and the number is wrong. This
    sweeps the union rather than waiting for each instance to be reported.

    Raising counts as passing — refusing to render a key the context cannot
    honour is the fail-closed contract. What must never happen is two
    materially different keys rendering to the SAME SQL.
    """

    @pytest.mark.parametrize(
        "label,base,mutated", _MUTATIONS, ids=[m[0] for m in _MUTATIONS],
    )
    def test_changing_a_field_changes_the_sql(self, label, base, mutated) -> None:
        def render(key):
            # Try both facility groups; a key needing neither renders in both.
            for ctx in (_composite_ctx(), _filter_ctx()):
                try:
                    return _sql(render_value_key(key, ctx))
                except RenderContextMissingFacilityError:
                    continue
                except NotImplementedError:
                    continue
            return None  # refused everywhere — fail-closed, acceptable

        base_sql = render(base)
        mutated_sql = render(mutated)
        if base_sql is None or mutated_sql is None:
            return  # at least one was refused; nothing was silently dropped
        assert base_sql != mutated_sql, (
            f"{label}: two keys differing in that field render IDENTICALLY as "
            f"{base_sql!r} — the field is silently dropped, which is a "
            f"wrong-value bug rather than an error."
        )


class TestOperatorCompositionEdges:
    """Three edges a Codex pass surfaced, all the same family as the rest:
    output that still parses and still returns rows, but means something else.
    """

    def test_comparison_nested_in_arithmetic_keeps_its_parens(self) -> None:
        """``(a > b) + 1`` must not flatten to ``a > b + 1``.

        sqlglot does not parenthesise by nesting, and the two parse
        differently: the flattened form reads as ``a > (b + 1)``. A precedence
        table covering only arithmetic misses this, because the CHILD is a
        comparison.
        """
        key = ArithmeticKey(
            op="+",
            operands=(
                ArithmeticKey(
                    op=">",
                    operands=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(5))),
                ),
                LiteralKey(value=Decimal(1)),
            ),
        )
        out = _sql(render_value_key(key, _filter_ctx()))
        assert out == "(orders.amount > 5) + 1", out

    def test_boolean_nested_in_arithmetic_keeps_its_parens(self) -> None:
        """Same for a boolean child: ``a AND b + 1`` binds the ``+`` first."""
        key = ArithmeticKey(
            op="+",
            operands=(
                ArithmeticKey(
                    op="and",
                    operands=(
                        ArithmeticKey(
                            op=">",
                            operands=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(1))),
                        ),
                        ArithmeticKey(
                            op="<",
                            operands=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(9))),
                        ),
                    ),
                ),
                LiteralKey(value=Decimal(1)),
            ),
        )
        out = _sql(render_value_key(key, _filter_ctx()))
        assert out.startswith("("), out

    def test_comparison_with_three_operands_is_refused(self) -> None:
        """A chained comparison must RAISE, not left-fold.

        Left-folding ``a < b < c`` compares a BOOLEAN against ``c``; taking
        only the first two operands silently drops the third. The Mode-B parser
        rejects chained comparisons, so this is the structural backstop for
        anything constructing keys directly.
        """
        key = ArithmeticKey(
            op="<",
            operands=(
                ColumnKey(leaf="amount"),
                LiteralKey(value=Decimal(5)),
                LiteralKey(value=Decimal(9)),
            ),
        )
        ctx = _filter_ctx()
        with pytest.raises(NotImplementedError):
            render_value_key(key, ctx)

    def test_is_not_with_extra_operands_is_refused(self) -> None:
        """``is`` / ``is not`` read operands[0] and [1] only — a third would
        vanish without a word."""
        key = ArithmeticKey(
            op="is",
            operands=(
                ColumnKey(leaf="amount"),
                LiteralKey(value=None),
                LiteralKey(value=Decimal(1)),
            ),
        )
        ctx = _filter_ctx()
        with pytest.raises(NotImplementedError):
            render_value_key(key, ctx)


class TestContainsAggregate:
    """``contains_aggregate`` decides GROUP BY / HAVING placement, so it must
    answer "is there an aggregate in this tree", not "when does this evaluate"."""

    def test_bare_aggregate(self) -> None:
        assert contains_aggregate(
            AggregateKey(source=ColumnKey(leaf="amount"), agg="sum"),
        ) is True

    def test_plain_column_is_not_an_aggregate(self) -> None:
        assert contains_aggregate(ColumnKey(leaf="amount")) is False

    def test_aggregate_nested_in_arithmetic_and_scalar_calls(self) -> None:
        agg = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum")
        nested = ScalarCallKey(
            name="ifnull",
            args=(
                ArithmeticKey(op="+", operands=(agg, LiteralKey(value=Decimal(1)))),
                LiteralKey(value=Decimal(0)),
            ),
        )
        assert contains_aggregate(nested) is True

    def test_transform_over_a_raw_column_is_not_an_aggregate(self) -> None:
        """The case a phase test gets wrong.

        Every TransformKey is POST phase, so ``phase >= AGGREGATE`` reports
        True even when the transform wraps a plain column — routing a
        non-aggregate predicate into HAVING.
        """
        key = TransformKey(op="cumsum", input=ColumnKey(leaf="amount"))
        assert contains_aggregate(key) is False

    def test_transform_over_an_aggregate_is_an_aggregate(self) -> None:
        key = TransformKey(
            op="cumsum",
            input=AggregateKey(source=ColumnKey(leaf="amount"), agg="sum"),
        )
        assert contains_aggregate(key) is True


class TestEqualPrecedenceRightChildren:
    """Checking the PARENT operator alone is not enough.

    ``a - (b - c)`` was already handled, but ``a * (b % c)`` was not: the
    parent is ``*``, which looks associative, so the parens were dropped and
    the expression regrouped to ``(a * b) % c``. With a=2 b=3 c=2 that is 0
    instead of 2 — a different number from SQL that parses cleanly.
    """

    def _key(self, outer_op, inner_op):
        return ArithmeticKey(
            op=outer_op,
            operands=(
                ColumnKey(leaf="amount"),
                ArithmeticKey(
                    op=inner_op,
                    operands=(
                        LiteralKey(value=Decimal(3)),
                        LiteralKey(value=Decimal(2)),
                    ),
                ),
            ),
        )

    @pytest.mark.parametrize(
        "outer,inner",
        [("*", "%"), ("*", "/"), ("/", "*"), ("/", "/"), ("-", "+"), ("-", "-")],
    )
    def test_equal_precedence_right_child_keeps_parens(self, outer, inner) -> None:
        out = _sql(render_value_key(self._key(outer, inner), _filter_ctx()))
        assert "(" in out, f"{outer} over {inner} lost its grouping: {out}"

    @pytest.mark.parametrize("op", ["+", "*"])
    def test_even_plus_and_times_keep_their_grouping(self, op) -> None:
        """``+`` and ``*`` are NOT operationally associative either.

        An earlier version of this test asserted the opposite — that these two
        could safely drop their parens because they regroup harmlessly. That is
        true over the reals and false over the machine: with floats, rounding
        makes ``a + (b + c)`` and ``(a + b) + c`` differ, and with
        fixed-precision decimals so does overflow. The binder built a specific
        tree; emitting a different one is a silent accuracy change.

        So the rule is now uniform — every equal-precedence right child keeps
        its parens — which is also one fewer special case to get wrong.
        """
        out = _sql(render_value_key(self._key(op, op), _filter_ctx()))
        assert "(" in out, out


class TestArithmeticArity:
    def test_no_operands_is_refused(self) -> None:
        key = ArithmeticKey(op="+", operands=())
        ctx = _filter_ctx()
        with pytest.raises(NotImplementedError):
            render_value_key(key, ctx)

    @pytest.mark.parametrize("op", ["and", "or"])
    def test_single_operand_boolean_is_that_operand(self, op) -> None:
        """The conjunction of one term is that term — well-defined, and it must
        not fall through to the unary branch and report ``and`` as an
        unsupported unary operator."""
        inner = ArithmeticKey(
            op=">", operands=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(5))),
        )
        key = ArithmeticKey(op=op, operands=(inner,))
        assert _sql(render_value_key(key, _filter_ctx())) == "orders.amount > 5"


class TestContainsAggregateTransformDependencies:
    """``partition_keys`` and ``time_key`` are expression dependencies of a
    transform just as ``input`` is — an aggregate in either one still lands in
    the emitted SQL."""

    def test_aggregate_in_partition_keys(self) -> None:
        agg = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum")
        key = TransformKey(
            op="rank",
            input=ColumnKey(leaf="amount"),
            partition_keys=frozenset({agg}),
        )
        assert contains_aggregate(key) is True

    def test_aggregate_in_time_key(self) -> None:
        agg = AggregateKey(source=ColumnKey(leaf="created_at"), agg="max")
        key = TransformKey(
            op="cumsum", input=ColumnKey(leaf="amount"), time_key=agg,
        )
        assert contains_aggregate(key) is True

    def test_transform_with_no_aggregate_anywhere(self) -> None:
        key = TransformKey(
            op="rank",
            input=ColumnKey(leaf="amount"),
            partition_keys=frozenset({ColumnKey(leaf="label")}),
            time_key=ColumnKey(leaf="created_at"),
        )
        assert contains_aggregate(key) is False


class TestScalarArity:
    """sqlglot's arity handling is inconsistent, and all three modes are bad
    answers for a mistyped filter.

    ``exp.func("ROUND", a, b, c)`` SILENTLY DROPS the third argument.
    ``exp.func("LENGTH", a, b)`` emits ``LENGTH(a, b)`` for the database to
    reject with its own error. ``exp.func("LOWER", a, b)`` raises a raw sqlglot
    ValueError that leaks an internal library name. The allowlist knows the
    right answer, so it is checked before the node is built.
    """

    @pytest.mark.parametrize(
        "name,argc",
        [("round", 3), ("lower", 2), ("length", 2), ("abs", 2),
         ("nullif", 1), ("replace", 2), ("substr", 1), ("like", 3)],
    )
    def test_wrong_arity_is_refused(self, name, argc) -> None:
        from slayer.sql.render.value_expr import render_scalar_call

        args = [exp.column(f"c{i}") for i in range(argc)]
        with pytest.raises(NotImplementedError):
            render_scalar_call(
                name=name, args=args, dialect=get_dialect("postgres"),
            )

    @pytest.mark.parametrize(
        "name,argc",
        [("round", 1), ("round", 2), ("lower", 1), ("substr", 2), ("substr", 3),
         ("replace", 3), ("coalesce", 1), ("coalesce", 4), ("concat", 3)],
    )
    def test_accepted_arities_still_render(self, name, argc) -> None:
        """The variadic and optional-argument forms must keep working."""
        from slayer.sql.render.value_expr import render_scalar_call

        args = [exp.column(f"c{i}") for i in range(argc)]
        out = render_scalar_call(
            name=name, args=args, dialect=get_dialect("postgres"),
        )
        assert out.sql(dialect="postgres")


class TestArityIsRejectedAtBindTime:
    """The renderer check is the backstop; the binder is where a user's typo
    should surface, with a message naming the function and the counts."""

    async def test_round_with_three_args_is_rejected(self, e2e) -> None:
        with pytest.raises(ValueError, match="round"):
            await e2e.execute(
                SlayerQuery(
                    source_model="orders",
                    dimensions=[ColumnRef(name="status")],
                    measures=[ModelMeasure(formula="*:count", name="n")],
                    filters=["round(amount, 2, 99) > 1"],
                ),
                dry_run=True,
            )

    async def test_length_with_two_args_is_rejected(self, e2e) -> None:
        """Previously emitted ``LENGTH(a, b)`` — invalid SQL the backend
        rejected with its own, less useful error."""
        with pytest.raises(ValueError, match="length"):
            await e2e.execute(
                SlayerQuery(
                    source_model="orders",
                    dimensions=[ColumnRef(name="status")],
                    measures=[ModelMeasure(formula="*:count", name="n")],
                    filters=["length(status, status) > 1"],
                ),
                dry_run=True,
            )

    async def test_correct_arity_still_binds(self, e2e) -> None:
        resp = await e2e.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="*:count", name="n")],
                filters=["round(amount, 2) > 1"],
            ),
            dry_run=True,
        )
        assert "ROUND" in resp.sql.upper()


class TestNullInInList:
    """SQL's three-valued logic makes a NULL member a trap, not a member test.

    ``col IN (a, NULL)`` never matches on the NULL. ``col NOT IN (a, NULL)``
    is worse: it evaluates to NULL for EVERY row, so the filter returns ZERO
    rows rather than "everything except a". Neither announces itself — the
    query runs and hands back a plausible-looking empty result.
    """

    def test_renderer_refuses_null_in_the_list(self) -> None:
        key = InKey(
            column=ColumnKey(leaf="label"),
            values=(LiteralKey(value="a"), LiteralKey(value=None)),
        )
        ctx = _filter_ctx()
        with pytest.raises(NotImplementedError):
            render_value_key(key, ctx)

    def test_renderer_refuses_null_in_a_negated_list(self) -> None:
        key = InKey(
            column=ColumnKey(leaf="label"),
            values=(LiteralKey(value="a"), LiteralKey(value=None)),
            negated=True,
        )
        ctx = _filter_ctx()
        with pytest.raises(NotImplementedError):
            render_value_key(key, ctx)

    def test_ordinary_in_list_still_renders(self) -> None:
        key = InKey(
            column=ColumnKey(leaf="label"),
            values=(LiteralKey(value="a"), LiteralKey(value="b")),
        )
        assert _sql(render_value_key(key, _filter_ctx())) == (
            "orders.label IN ('a', 'b')"
        )

    async def test_bind_time_rejects_null_in_list(self, e2e) -> None:
        """The user-facing half: caught at bind, with a message pointing at
        ``is null`` rather than at three-valued logic in the abstract."""
        with pytest.raises(ValueError, match="NULL is not allowed"):
            await e2e.execute(
                SlayerQuery(
                    source_model="orders",
                    dimensions=[ColumnRef(name="status")],
                    measures=[ModelMeasure(formula="*:count", name="n")],
                    filters=["status in ('new', None)"],
                ),
                dry_run=True,
            )

    async def test_bind_time_rejects_null_in_negated_list(self, e2e) -> None:
        """The dangerous one: this previously returned zero rows in silence."""
        with pytest.raises(ValueError, match="NULL is not allowed"):
            await e2e.execute(
                SlayerQuery(
                    source_model="orders",
                    dimensions=[ColumnRef(name="status")],
                    measures=[ModelMeasure(formula="*:count", name="n")],
                    filters=["status not in ('new', None)"],
                ),
                dry_run=True,
            )

    async def test_null_free_in_list_still_executes(self, e2e) -> None:
        resp = await e2e.execute(
            SlayerQuery(
                source_model="orders",
                dimensions=[ColumnRef(name="status")],
                measures=[ModelMeasure(formula="*:count", name="n")],
                filters=["status in ('new', 'missing')"],
            )
        )
        assert [r["orders.status"] for r in resp.data] == ["new"]


class TestUnaryOperandGrouping:
    """The unary branches need the precedence pass too.

    Adding unary support earlier in this PR fixed the dropped sign but routed
    the operand straight into ``exp.Neg`` / ``exp.Not`` without grouping it.
    Both results parse cleanly and mean something else.
    """

    def test_negated_sum_keeps_its_parens(self) -> None:
        """``-(a + b)`` must not flatten to ``-a + b``, which is ``(-a) + b``."""
        key = ArithmeticKey(
            op="-",
            operands=(
                ArithmeticKey(
                    op="+",
                    operands=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(1))),
                ),
            ),
        )
        out = _sql(render_value_key(key, _filter_ctx()))
        assert out == "-(orders.amount + 1)", out

    def test_not_of_a_conjunction_keeps_its_parens(self) -> None:
        """``NOT (a AND b)`` must not flatten to ``NOT a AND b``, which is
        ``(NOT a) AND b`` — De Morgan, and a different row set."""
        key = ArithmeticKey(
            op="not",
            operands=(
                ArithmeticKey(
                    op="and",
                    operands=(
                        ArithmeticKey(
                            op=">",
                            operands=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(1))),
                        ),
                        ArithmeticKey(
                            op="<",
                            operands=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(9))),
                        ),
                    ),
                ),
            ),
        )
        out = _sql(render_value_key(key, _filter_ctx()))
        assert out == "NOT (orders.amount > 1 AND orders.amount < 9)", out

    def test_not_of_a_comparison_needs_no_parens(self) -> None:
        """Don't over-wrap: NOT binds looser than a comparison, so
        ``NOT a > b`` already means ``NOT (a > b)``."""
        key = ArithmeticKey(
            op="not",
            operands=(
                ArithmeticKey(
                    op=">",
                    operands=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(5))),
                ),
            ),
        )
        assert _sql(render_value_key(key, _filter_ctx())) == "NOT orders.amount > 5"

    def test_negated_column_needs_no_parens(self) -> None:
        key = ArithmeticKey(op="-", operands=(ColumnKey(leaf="amount"),))
        assert _sql(render_value_key(key, _filter_ctx())) == "-orders.amount"


class TestComparisonsAreNonAssociative:
    """The equal-precedence rule was right-child-only, and ``is`` skipped it.

    Arithmetic is left-associative, so an equal-precedence LEFT child regroups
    harmlessly. The comparison family is not: SQL binds ``IS`` tighter than
    ``=``, and Postgres rejects a chain of relational operators outright. Both
    shapes are reachable — the Mode-B parser reads ``(a == b) == c`` as a
    NESTED comparison, not a chained one, so the binder does build them.
    """

    def _cmp(self, op, left, right):
        return ArithmeticKey(op=op, operands=(left, right))

    def _inner(self, op="="):
        return self._cmp(
            op, ColumnKey(leaf="amount"), LiteralKey(value=Decimal(5)),
        )

    def test_comparison_over_comparison_keeps_left_parens(self) -> None:
        """``(a = 5) = TRUE`` must not flatten to ``a = 5 = TRUE``."""
        key = self._cmp("=", self._inner(), LiteralKey(value=True))
        out = _sql(render_value_key(key, _filter_ctx()))
        assert out == "(orders.amount = 5) = TRUE", out

    def test_mixed_relational_operators_keep_left_parens(self) -> None:
        """``(a < 5) = TRUE`` emitted bare is ``a < 5 = TRUE`` — a
        non-associative chain Postgres refuses to parse at all."""
        key = self._cmp("=", self._inner("<"), LiteralKey(value=True))
        out = _sql(render_value_key(key, _filter_ctx()))
        assert out == "(orders.amount < 5) = TRUE", out

    def test_is_null_over_a_comparison_keeps_its_parens(self) -> None:
        """The wrong-value case: ``a = 5 IS NULL`` is read as
        ``a = (5 IS NULL)``, i.e. ``a = FALSE`` — a different predicate that
        still returns rows."""
        key = self._cmp("is", self._inner(), LiteralKey(value=None))
        out = _sql(render_value_key(key, _filter_ctx()))
        assert out == "(orders.amount = 5) IS NULL", out

    def test_is_not_null_over_a_comparison_keeps_its_parens(self) -> None:
        key = self._cmp("is not", self._inner(), LiteralKey(value=None))
        out = _sql(render_value_key(key, _filter_ctx()))
        assert out == "NOT (orders.amount = 5) IS NULL", out

    def test_plain_is_null_over_a_column_gains_no_parens(self) -> None:
        """Don't over-wrap: a column is self-delimiting."""
        key = self._cmp("is", ColumnKey(leaf="amount"), LiteralKey(value=None))
        assert _sql(render_value_key(key, _filter_ctx())) == "orders.amount IS NULL"

    def test_arithmetic_left_child_still_flattens(self) -> None:
        """The non-associativity rule is scoped to the comparison level —
        ``(a - 1) - 2`` is the tree a left-fold builds, and re-parenthesising
        every arithmetic left child would churn emission for no gain."""
        inner = self._cmp(
            "-", ColumnKey(leaf="amount"), LiteralKey(value=Decimal(1)),
        )
        key = self._cmp("-", inner, LiteralKey(value=Decimal(2)))
        out = _sql(render_value_key(key, _filter_ctx()))
        assert out == "orders.amount - 1 - 2", out


class TestStarSourceIsCountOnly:
    """``*`` is only defined as ``COUNT``'s argument.

    The builder-free aggregate path gated on the dispatch MECHANISM, which
    passes for every simple aggregation, so a ``StarKey`` source went straight
    into whichever node the registry named — building ``SUM(*)`` and
    ``COUNT(DISTINCT *)``, which no backend accepts. Refused here rather than
    at execution time.
    """

    @pytest.mark.parametrize("agg", ["sum", "avg", "min", "max", "count_distinct"])
    def test_non_count_star_is_refused(self, agg) -> None:
        key = AggregateKey(source=StarKey(), agg=agg)
        with pytest.raises(NotImplementedError, match="bare star"):
            render_value_key(key, _composite_ctx())

    def test_count_star_still_renders(self) -> None:
        key = AggregateKey(source=StarKey(), agg="count")
        assert _sql(render_value_key(key, _composite_ctx())) == "COUNT(*)"


def _grouping_shape(node: exp.Expression):
    """``node`` reduced to WHICH OPERAND BELONGS TO WHICH OPERATOR, and nothing
    else, as nested tuples of ``(operator, operands...)``.

    Comparing whole sqlglot nodes does not work here, because a parsed tree and
    a hand-built one differ in ways that have nothing to do with grouping:
    ``Div`` carries parser-set ``typed`` / ``safe`` flags, nodes pick up
    ``_type`` annotations, and each dialect injects its own numeric cast
    (Postgres renders ``a / b`` as ``CAST(a AS DOUBLE PRECISION) / b``).

    ``Paren`` and ``Cast`` collapse to their operand: the first carries no
    meaning once a tree exists — it is how meaning is PRESERVED across the
    string — and the second is a typing decision. Leaves reduce to their own
    SQL, which is identical on both sides.
    """
    if isinstance(node, (exp.Paren, exp.Cast)):
        return _grouping_shape(node.this)
    operands = [
        node.args.get("this"), node.args.get("expression"),
        *(node.args.get("expressions") or []),
    ]
    operands = [o for o in operands if isinstance(o, exp.Expression)]
    if not operands:
        return node.sql()
    return (type(node).__name__, tuple(_grouping_shape(o) for o in operands))


def _reparses_to_the_same_tree(node: exp.Expression, dialect: str) -> bool:
    """Whether the database will read back the tree we built."""
    reparsed = sqlglot.parse_one(node.sql(dialect=dialect), dialect=dialect)
    return _grouping_shape(reparsed) == _grouping_shape(node)


class TestEveryOperatorPairSurvivesTheRoundTrip:
    """The meta-test for the whole regrouping family.

    Every individual grouping bug in this PR has the same shape: we build one
    tree and the database reads a different one. Asserting emitted STRINGS
    catches them one at a time, and only once someone has thought of the shape.

    This asserts the property directly — render, re-parse, and compare the
    trees modulo parens — over every ordered pair of operators in the
    precedence table, in both operand positions. Note that re-parse STABILITY
    alone would not do: ``a + b * c`` re-parses to a stable string while
    meaning something other than the ``(a + b) * c`` we built.
    """

    # Grouped by result type. Feeding a boolean to an arithmetic operator is
    # not a shape the binder builds, and the dialects wrap such an operand in
    # their own numeric CAST — noise that says nothing about grouping.
    ARITH = ["+", "-", "*", "/", "%"]
    CMP = ["=", "!=", "<", "<=", ">", ">="]
    BOOL = ["and", "or"]
    DIALECTS = ["postgres", "sqlite", "mysql"]

    @staticmethod
    def _leaf(name: str) -> exp.Expression:
        return exp.column(name, table="t")

    def _binary(self, op: str, left, right):
        return render_arithmetic(op, [left, right])

    def _nested(self, inner: str):
        return self._binary(inner, self._leaf("b"), self._leaf("c"))

    def _check(self, built, dialect, label) -> None:
        assert _reparses_to_the_same_tree(built, dialect), (
            f"{label} regrouped: {built.sql(dialect=dialect)}"
        )

    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize("outer", ARITH)
    @pytest.mark.parametrize("inner", ARITH)
    @pytest.mark.parametrize("position", ["left", "right"])
    def test_arithmetic_nested_in_arithmetic(
        self, outer, inner, position, dialect,
    ) -> None:
        nested = self._nested(inner)
        operands = (
            [nested, self._leaf("d")] if position == "left"
            else [self._leaf("a"), nested]
        )
        self._check(
            self._binary(outer, *operands), dialect,
            f"{outer} over {inner} ({position})",
        )

    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize("outer", CMP)
    @pytest.mark.parametrize("inner", ARITH)
    @pytest.mark.parametrize("position", ["left", "right"])
    def test_arithmetic_nested_in_a_comparison(
        self, outer, inner, position, dialect,
    ) -> None:
        nested = self._nested(inner)
        operands = (
            [nested, self._leaf("d")] if position == "left"
            else [self._leaf("a"), nested]
        )
        self._check(
            self._binary(outer, *operands), dialect,
            f"{outer} over {inner} ({position})",
        )

    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize("outer", BOOL)
    @pytest.mark.parametrize("inner", CMP + BOOL)
    @pytest.mark.parametrize("position", ["left", "right"])
    def test_predicate_nested_in_a_connector(
        self, outer, inner, position, dialect,
    ) -> None:
        nested = self._nested(inner)
        other = self._binary("<", self._leaf("d"), self._leaf("e"))
        operands = [nested, other] if position == "left" else [other, nested]
        self._check(
            self._binary(outer, *operands), dialect,
            f"{outer} over {inner} ({position})",
        )

    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize("inner", CMP + BOOL)
    def test_not_over_every_predicate(self, inner, dialect) -> None:
        self._check(
            render_arithmetic("not", [self._nested(inner)]), dialect,
            f"not over {inner}",
        )

    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize("inner", ARITH)
    def test_negation_over_every_arithmetic_operator(self, inner, dialect) -> None:
        self._check(
            render_arithmetic("-", [self._nested(inner)]), dialect,
            f"- over {inner}",
        )

    @pytest.mark.parametrize("dialect", DIALECTS)
    @pytest.mark.parametrize("op", ["is", "is not"])
    @pytest.mark.parametrize("inner", ARITH + CMP)
    def test_is_over_every_value_expression(self, op, inner, dialect) -> None:
        self._check(
            render_arithmetic(op, [self._nested(inner), exp.Null()]), dialect,
            f"{op} over {inner}",
        )


class TestGeneratorComposersShareTheGroupingPolicy:
    """The three LIVE generator composers had the same grouping holes.

    ``value_expr`` is not yet on the generator's arithmetic path (that reroute
    is the scope-assembly PR), so finding these there did not fix them here.
    Each composer built ``exp.Not`` / ``exp.Neg`` / ``exp.Is`` around a bare
    operand and hand-folded ``and``/``or``, producing SQL that parses cleanly
    and returns a different row set:

    * ``not (a AND b)``      -> ``NOT a AND b``   = ``(NOT a) AND b``
    * ``-(a + b)``           -> ``-a + b``        = ``(-a) + b``
    * ``(a = 5) IS NULL``    -> ``a = 5 IS NULL`` = ``a = (5 IS NULL)``
    * ``a AND (b OR c)``     -> ``a AND b OR c``  = ``(a AND b) OR c``

    All four now route through the shared policy in ``value_expr``, so the
    construct groups the same way wherever it is composed (P-G).
    """

    @staticmethod
    def _a():
        return exp.column("a", table="t")

    def _gt(self):
        return exp.GT(this=self._a(), expression=exp.Literal.number("1"))

    def _lt(self):
        return exp.LT(this=self._a(), expression=exp.Literal.number("9"))

    def _eq(self):
        return exp.EQ(this=self._a(), expression=exp.Literal.number("5"))

    def _add(self):
        return exp.Add(this=self._a(), expression=exp.column("b", table="t"))

    def _composers(self):
        """The three live composers, as uniform ``(op, operands) -> node``."""
        gen = SQLGenerator.__new__(SQLGenerator)
        return {
            "build_arithmetic_for_filter": (
                lambda op, ops: SQLGenerator._build_arithmetic_for_filter(
                    op=op, operands=ops,
                )
            ),
            "compose_arithmetic_op": (
                lambda op, ops: SQLGenerator._compose_arithmetic_op(
                    op=op, operands=ops,
                )
            ),
            "build_arith_or_cmp_ast": (
                lambda op, ops: gen._build_arith_or_cmp_ast(op=op, operands=ops)
            ),
        }

    @pytest.mark.parametrize(
        "composer",
        ["build_arithmetic_for_filter", "compose_arithmetic_op",
         "build_arith_or_cmp_ast"],
    )
    def test_not_of_a_conjunction_keeps_its_parens(self, composer) -> None:
        compose = self._composers()[composer]
        conj = exp.And(this=self._gt(), expression=self._lt())
        assert compose("not", [conj]).sql() == "NOT (t.a > 1 AND t.a < 9)"

    @pytest.mark.parametrize(
        "composer",
        ["build_arithmetic_for_filter", "compose_arithmetic_op",
         "build_arith_or_cmp_ast"],
    )
    def test_is_null_over_a_comparison_keeps_its_parens(self, composer) -> None:
        compose = self._composers()[composer]
        out = compose("is", [self._eq(), exp.Null()]).sql()
        assert out == "(t.a = 5) IS NULL", out

    @pytest.mark.parametrize(
        "composer",
        ["build_arithmetic_for_filter", "compose_arithmetic_op",
         "build_arith_or_cmp_ast"],
    )
    def test_is_not_null_over_a_comparison_keeps_its_parens(self, composer) -> None:
        compose = self._composers()[composer]
        out = compose("is not", [self._eq(), exp.Null()]).sql()
        assert out == "NOT (t.a = 5) IS NULL", out

    @pytest.mark.parametrize(
        "composer",
        ["build_arithmetic_for_filter", "compose_arithmetic_op",
         "build_arith_or_cmp_ast"],
    )
    def test_disjunction_inside_a_conjunction_keeps_its_parens(
        self, composer,
    ) -> None:
        compose = self._composers()[composer]
        disj = exp.Or(this=self._gt(), expression=self._lt())
        out = compose("and", [self._lt(), disj]).sql()
        assert out == "t.a < 9 AND (t.a > 1 OR t.a < 9)", out

    @pytest.mark.parametrize(
        "composer",
        ["build_arithmetic_for_filter", "compose_arithmetic_op",
         "build_arith_or_cmp_ast"],
    )
    def test_negated_sum_keeps_its_parens(self, composer) -> None:
        compose = self._composers()[composer]
        assert compose("-", [self._add()]).sql() == "-(t.a + t.b)"

    @pytest.mark.parametrize(
        "composer",
        ["build_arithmetic_for_filter", "compose_arithmetic_op",
         "build_arith_or_cmp_ast"],
    )
    @pytest.mark.parametrize(
        "op,inner_cls,expected",
        [
            # ``a - (b + c)``: dropping the parens regroups to ``(a - b) + c``.
            ("-", exp.Add, "t.a - (t.b + t.c)"),
            # ``a + (b - c)``: the generator treated ``+`` as associative and
            # emitted ``a + b - c``. Over floats and fixed-precision decimals
            # that is a different number, not just a different tree.
            ("+", exp.Sub, "t.a + (t.b - t.c)"),
            ("*", exp.Div, "t.a * (t.b / t.c)"),
            ("/", exp.Mul, "t.a / (t.b * t.c)"),
        ],
    )
    def test_equal_precedence_right_operand_keeps_its_parens(
        self, composer, op, inner_cls, expected,
    ) -> None:
        compose = self._composers()[composer]
        inner = inner_cls(
            this=exp.column("b", table="t"), expression=exp.column("c", table="t"),
        )
        out = compose(op, [self._a(), inner]).sql()
        assert out == expected, out

    def test_lower_precedence_left_operand_keeps_its_parens(self) -> None:
        """``_build_arith_or_cmp_ast`` applied NO precedence pass at all, so
        ``(a + b) * c`` emitted ``a + b * c`` — ``b * c`` binds first."""
        compose = self._composers()["build_arith_or_cmp_ast"]
        out = compose("*", [self._add(), exp.column("c", table="t")]).sql()
        assert out == "(t.a + t.b) * t.c", out

    @pytest.mark.parametrize(
        "composer",
        ["build_arithmetic_for_filter", "compose_arithmetic_op",
         "build_arith_or_cmp_ast"],
    )
    def test_comparison_nested_in_arithmetic_keeps_its_parens(
        self, composer,
    ) -> None:
        """The generator's precedence table knew only ``+ - * /``, so a
        comparison operand fell through ungrouped: ``(a > b) + 1`` emitted
        ``a > b + 1``, read back as ``a > (b + 1)``."""
        compose = self._composers()[composer]
        gt = exp.GT(this=self._a(), expression=exp.column("b", table="t"))
        out = compose("+", [gt, exp.Literal.number("1")]).sql()
        assert out == "(t.a > t.b) + 1", out

    @pytest.mark.parametrize(
        "composer",
        ["build_arithmetic_for_filter", "compose_arithmetic_op",
         "build_arith_or_cmp_ast"],
    )
    def test_plain_shapes_gain_no_parens(self, composer) -> None:
        """Don't over-wrap — the fix must not churn the emission of the
        shapes that were already right."""
        compose = self._composers()[composer]
        assert compose("is", [self._a(), exp.Null()]).sql() == "t.a IS NULL"
        assert compose("not", [self._gt()]).sql() == "NOT t.a > 1"
        assert (
            compose("and", [self._gt(), self._lt()]).sql()
            == "t.a > 1 AND t.a < 9"
        )
