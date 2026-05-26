"""DEV-1452 Stage A — AggRenderSpec + ``_build_agg_render_spec_from_planned``.

Decouples ``slayer/sql/generator.py``'s dialect helpers from the legacy
``EnrichedMeasure``. The new ``AggRenderSpec`` is the frozen Pydantic record
that ``_build_agg`` / ``_build_formula_agg`` / ``_build_percentile`` /
``_build_stat_agg`` / ``_resolve_value_sql`` / ``_resolve_agg_param`` /
``_wrap_cast_for_type`` consume.

These tests fail against current code because ``AggRenderSpec`` and
``_build_agg_render_spec_from_planned`` do not yet exist; the import is the
failure point.

``_build_agg_render_spec_from_planned`` keeps the same five-kwarg signature
as the legacy ``_synthesize_enriched_measure_from_planned``
(``slot``, ``key``, ``source_model``, ``source_relation``, ``full_alias``)
so existing call sites flip with a one-line return-type swap. The logic
mirrors the legacy synthesizer (``slayer/sql/generator.py:7033``) verbatim:

* ``StarKey`` rejects any non-count aggregation, and any args/kwargs on
  ``*:count``.
* ``ColumnKey`` / ``ColumnSqlKey`` resolves the source column on
  ``source_model``; aggregations outside the built-in slice look up the
  custom ``Aggregation`` definition on ``source_model.aggregations`` and
  raise ``AggregationNotAllowedError`` on miss.
* ``first`` / ``last`` derive ``time_column`` from the first ``ColumnKey``
  in ``key.args``.
* ``column_filter_key`` is qualified against ``source_relation`` /
  ``source_model`` and surfaces as ``filter_sql``.
* Cross-model kwargs whose ``ColumnKey.path`` disagrees with
  ``source.path`` raise ``AggregationNotAllowedError``.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import AggregationNotAllowedError
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    Phase,
    SqlExprKey,
    StarKey,
)
from slayer.core.models import Aggregation, AggregationParam, Column, SlayerModel
from slayer.engine.planned import ValueSlot

# These imports drive the failing-test contract — neither name exists on
# the current codebase. Stage A landing flips them green.
# ``_build_agg_render_spec_from_planned`` is a method on ``SQLGenerator``;
# tests instantiate the generator to invoke it (see ``_invoke`` below).
from slayer.sql.generator import (  # type: ignore[attr-defined]
    AggRenderSpec,
    SQLGenerator,
)


def _invoke(slot, key, *, source_model, source_relation, full_alias):
    """Thin shim — instantiate SQLGenerator and invoke the new builder."""
    gen = SQLGenerator(dialect="postgres")
    return gen._build_agg_render_spec_from_planned(  # type: ignore[attr-defined]
        slot=slot,
        key=key,
        source_model=source_model,
        source_relation=source_relation,
        full_alias=full_alias,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _orders_model() -> SlayerModel:
    """Local source model used by the bulk of the tests."""
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.INT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(
                name="net_amount",
                sql="amount - tax",
                type=DataType.DOUBLE,
            ),
            Column(name="tax", type=DataType.DOUBLE),
            Column(name="quantity", type=DataType.INT),
            # Derived time columns covering the two ColumnSqlKey shapes
            # the explicit-time-arg resolver must handle (DEV-1452 Codex
            # fix): bare-identifier rename and a non-trivial expression.
            Column(
                name="created_at_alias",
                sql="created_at",
                type=DataType.TIMESTAMP,
            ),
            Column(
                name="created_at_day",
                sql="DATE_TRUNC('day', created_at)",
                type=DataType.DATE,
            ),
        ],
        aggregations=[
            Aggregation(
                name="rolling_avg",
                formula="AVG({value}) OVER (ORDER BY {time} ROWS BETWEEN {window} PRECEDING AND CURRENT ROW)",
                params=[
                    AggregationParam(name="time", sql="created_at"),
                    AggregationParam(name="window", sql="6"),
                ],
            ),
        ],
    )


def _slot(
    key,
    *,
    slot_id: str = "s1",
    declared_name: str = "x",
    public_name: str = "x",
    phase=Phase.AGGREGATE,
    slot_type=None,
) -> ValueSlot:
    return ValueSlot(
        id=slot_id,
        key=key,
        declared_name=declared_name,
        public_name=public_name,
        phase=phase,
        type=slot_type,
    )


# ---------------------------------------------------------------------------
# AggRenderSpec — construction / field surface
# ---------------------------------------------------------------------------


class TestAggRenderSpecConstruction:
    """The new typed record's field surface and frozen contract."""

    def test_exact_field_set(self):
        # Decision #4: exactly these 11 fields, no more, no less. Extra
        # fields (agg_args / source_measure_name / distinct / window /
        # user_declared / etc.) are deliberately NOT carried.
        assert set(AggRenderSpec.model_fields) == {
            "sql",
            "name",
            "model_name",
            "aggregation",
            "aggregation_def",
            "agg_kwargs",
            "alias",
            "filter_sql",
            "time_column",
            "type",
            "column_type",
        }

    def test_minimal_count_star(self):
        spec = AggRenderSpec(
            sql=None,
            name="",
            model_name="orders",
            aggregation="count",
            alias="orders._count",
        )
        # All 11 fields present; defaults for those omitted.
        assert spec.sql is None
        assert spec.name == ""
        assert spec.model_name == "orders"
        assert spec.aggregation == "count"
        assert spec.alias == "orders._count"
        assert spec.aggregation_def is None
        assert spec.agg_kwargs == {}
        assert spec.filter_sql is None
        assert spec.time_column is None
        assert spec.type is None
        assert spec.column_type is None

    def test_full_field_surface(self):
        agg_def = Aggregation(
            name="custom",
            formula="AVG({value})",
            params=[],
        )
        spec = AggRenderSpec(
            sql="amount",
            name="amount",
            model_name="orders",
            aggregation="custom",
            aggregation_def=agg_def,
            agg_kwargs={"p": "0.5"},
            alias="orders.amount_custom",
            filter_sql="orders.status = 'paid'",
            time_column="orders.created_at",
            type=DataType.DOUBLE,
            column_type=DataType.DOUBLE,
        )
        assert spec.sql == "amount"
        assert spec.aggregation_def is agg_def
        assert spec.agg_kwargs == {"p": "0.5"}
        assert spec.filter_sql == "orders.status = 'paid'"
        assert spec.time_column == "orders.created_at"
        assert spec.type is DataType.DOUBLE
        assert spec.column_type is DataType.DOUBLE

    def test_frozen(self):
        spec = AggRenderSpec(
            sql=None,
            name="",
            model_name="orders",
            aggregation="count",
            alias="orders._count",
        )
        with pytest.raises((TypeError, ValueError)):
            spec.aggregation = "sum"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# _build_agg_render_spec_from_planned — StarKey branch
# ---------------------------------------------------------------------------


class TestBuilderStarKey:
    def test_count_local_returns_count_spec(self):
        key = AggregateKey(source=StarKey(), agg="count")
        slot = _slot(
            key,
            declared_name="_count",
            public_name="_count",
            slot_type=DataType.INT,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders._count",
        )
        assert spec.sql is None
        # ``name`` is empty for star — there's no source column.
        assert spec.name == ""
        assert spec.model_name == "orders"
        assert spec.aggregation == "count"
        assert spec.alias == "orders._count"
        assert spec.type is DataType.INT
        assert spec.column_type is None
        assert spec.filter_sql is None
        assert spec.agg_kwargs == {}

    def test_non_count_star_raises(self):
        key = AggregateKey(source=StarKey(), agg="sum")
        slot = _slot(key, declared_name="_sum", public_name="_sum")
        with pytest.raises(ValueError, match=r"not allowed with measure '\*'"):
            _invoke(
                slot=slot,
                key=key,
                source_model=_orders_model(),
                source_relation="orders",
                full_alias="orders._sum",
            )

    def test_star_with_args_raises(self):
        key = AggregateKey(source=StarKey(), agg="count", args=(Decimal("1"),))
        slot = _slot(key, declared_name="_count", public_name="_count")
        with pytest.raises(ValueError, match=r"\*:count.* no args"):
            _invoke(
                slot=slot,
                key=key,
                source_model=_orders_model(),
                source_relation="orders",
                full_alias="orders._count",
            )

    def test_star_with_kwargs_raises(self):
        key = AggregateKey(
            source=StarKey(),
            agg="count",
            kwargs=(("p", Decimal("0.5")),),
        )
        slot = _slot(key, declared_name="_count", public_name="_count")
        with pytest.raises(ValueError, match=r"\*:count.* no args or kwargs"):
            _invoke(
                slot=slot,
                key=key,
                source_model=_orders_model(),
                source_relation="orders",
                full_alias="orders._count",
            )


# ---------------------------------------------------------------------------
# _build_agg_render_spec_from_planned — ColumnKey / ColumnSqlKey
# ---------------------------------------------------------------------------


class TestBuilderColumnKey:
    def test_bare_sum(self):
        key = AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="sum")
        slot = _slot(
            key,
            declared_name="amount_sum",
            public_name="amount_sum",
            slot_type=DataType.DOUBLE,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.amount_sum",
        )
        # Bare column: ``Column.sql`` is None on the orders.amount fixture,
        # so spec.sql falls back to the bare column name (mirrors legacy
        # ``sql = column.sql or column.name``).
        assert spec.sql == "amount"
        assert spec.name == "amount"
        assert spec.model_name == "orders"
        assert spec.aggregation == "sum"
        assert spec.alias == "orders.amount_sum"
        assert spec.type is DataType.DOUBLE
        assert spec.column_type is DataType.DOUBLE
        assert spec.filter_sql is None
        assert spec.time_column is None
        assert spec.agg_kwargs == {}
        assert spec.aggregation_def is None

    def test_with_column_filter_key_qualifies_filter(self):
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="sum",
            column_filter_key=SqlExprKey(canonical_sql="status = 'paid'"),
        )
        slot = _slot(
            key,
            declared_name="paid_amount_sum",
            public_name="paid_amount_sum",
            slot_type=DataType.DOUBLE,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.paid_amount_sum",
        )
        assert spec.filter_sql is not None
        # Bare-identifier refs in the filter qualify under the host model
        # (matches legacy ``resolve_filter_columns``).
        assert "orders" in spec.filter_sql
        assert "status" in spec.filter_sql

    def test_columnsqlkey_derived_uses_column_sql(self):
        # The derived ``net_amount`` column has ``sql = "amount - tax"`` and
        # type DOUBLE; aggregating it must surface the expression as
        # ``spec.sql``.
        key = AggregateKey(
            source=ColumnSqlKey(path=(), model="orders", column_name="net_amount"),
            agg="sum",
        )
        slot = _slot(
            key,
            declared_name="net_amount_sum",
            public_name="net_amount_sum",
            slot_type=DataType.DOUBLE,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.net_amount_sum",
        )
        assert spec.sql == "amount - tax"
        assert spec.name == "net_amount"
        assert spec.column_type is DataType.DOUBLE

    def test_column_not_found_raises(self):
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="nonexistent"),
            agg="sum",
        )
        slot = _slot(key, declared_name="x_sum", public_name="x_sum")
        # Match the legacy synthesizer's message tightly so a regression
        # that surfaces a different ValueError (e.g. a setup failure) still
        # fails loudly. Legacy emits exactly:
        #     "Aggregate source column 'nonexistent' not found on model 'orders'"
        with pytest.raises(
            ValueError,
            match=r"Aggregate source column 'nonexistent' not found on model 'orders'",
        ):
            _invoke(
                slot=slot,
                key=key,
                source_model=_orders_model(),
                source_relation="orders",
                full_alias="orders.x_sum",
            )


# ---------------------------------------------------------------------------
# _build_agg_render_spec_from_planned — first / last (time_column derivation)
# ---------------------------------------------------------------------------


class TestBuilderFirstLast:
    def test_first_with_explicit_time_column_local(self):
        # ``first(amount, created_at)`` — the positional ColumnKey arg
        # becomes ``spec.time_column``.
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="first",
            args=(ColumnKey(path=(), leaf="created_at"),),
        )
        slot = _slot(
            key,
            declared_name="amount_first",
            public_name="amount_first",
            slot_type=DataType.DOUBLE,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.amount_first",
        )
        assert spec.aggregation == "first"
        assert spec.time_column == "orders.created_at"

    def test_last_with_joined_time_column_uses_path_alias(self):
        # A joined positional time arg uses the ``__``-joined path alias.
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="last",
            args=(ColumnKey(path=("customers",), leaf="signup_at"),),
        )
        slot = _slot(
            key,
            declared_name="amount_last",
            public_name="amount_last",
            slot_type=DataType.DOUBLE,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.amount_last",
        )
        assert spec.aggregation == "last"
        # Mirrors legacy: ``__``-joined path + ``.<leaf>``.
        assert spec.time_column == "customers.signup_at"

    def test_last_with_derived_bare_time_column_local(self):
        # DEV-1452 Codex fix: a ``ColumnSqlKey`` positional arg whose
        # ``Column.sql`` is a bare identifier (renamed column) must
        # resolve to ``<source_relation>.<bare-sql>`` — previously the
        # spec-build loop skipped ``ColumnSqlKey`` entirely and the
        # ranked subquery silently fell back to the query's default
        # ranking column.
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="last",
            args=(
                ColumnSqlKey(
                    path=(), model="orders", column_name="created_at_alias",
                ),
            ),
        )
        slot = _slot(
            key,
            declared_name="amount_last",
            public_name="amount_last",
            slot_type=DataType.DOUBLE,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.amount_last",
        )
        assert spec.aggregation == "last"
        # Bare-identifier derived column expands to its underlying SQL
        # (``created_at``), qualified under the source relation. The
        # derived NAME (``created_at_alias``) isn't projected in the
        # ranked subquery's inner SELECT, so ORDER BY must reference the
        # expanded form that IS visible (``orders.created_at``).
        assert spec.time_column == "orders.created_at"

    def test_first_with_derived_expression_time_column_local(self):
        # A ``ColumnSqlKey`` arg whose ``Column.sql`` is a non-trivial
        # expression (``DATE_TRUNC(...)``) is materialised verbatim
        # (after the sqlglot round-trip); its inner bare refs resolve
        # against the ranked-subquery's FROM (the source relation).
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="first",
            args=(
                ColumnSqlKey(
                    path=(), model="orders", column_name="created_at_day",
                ),
            ),
        )
        slot = _slot(
            key,
            declared_name="amount_first",
            public_name="amount_first",
            slot_type=DataType.DOUBLE,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.amount_first",
        )
        assert spec.aggregation == "first"
        assert spec.time_column is not None
        # Postgres-dialect rendering of DATE_TRUNC('day', created_at).
        # Don't pin the exact whitespace — pin the structural tokens.
        tc = spec.time_column.upper().replace(" ", "")
        assert "DATE_TRUNC" in tc
        assert "'DAY'" in tc
        assert "CREATED_AT" in tc

    def test_cross_model_derived_time_column_raises(self):
        # Cross-model derived time args are not supported by the
        # ranked-subquery builder yet (Stage B follow-up territory). Surface
        # a NotImplementedError rather than silently emitting against the
        # wrong relation alias.
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="last",
            args=(
                ColumnSqlKey(
                    path=("customers",),
                    model="customers",
                    column_name="signup_at_alias",
                ),
            ),
        )
        slot = _slot(
            key,
            declared_name="amount_last",
            public_name="amount_last",
            slot_type=DataType.DOUBLE,
        )
        with pytest.raises(NotImplementedError, match="Cross-model derived time"):
            _invoke(
                slot=slot,
                key=key,
                source_model=_orders_model(),
                source_relation="orders",
                full_alias="orders.amount_last",
            )

    def test_unknown_derived_time_column_raises(self):
        # ``ColumnSqlKey`` whose ``column_name`` is not on ``source_model``
        # raises ValueError, mirroring the source-column lookup-miss path.
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="last",
            args=(
                ColumnSqlKey(
                    path=(), model="orders", column_name="not_a_real_col",
                ),
            ),
        )
        slot = _slot(
            key,
            declared_name="amount_last",
            public_name="amount_last",
            slot_type=DataType.DOUBLE,
        )
        with pytest.raises(ValueError, match="Derived time column 'not_a_real_col'"):
            _invoke(
                slot=slot,
                key=key,
                source_model=_orders_model(),
                source_relation="orders",
                full_alias="orders.amount_last",
            )

    def test_first_with_filter_propagates_both(self):
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="first",
            args=(ColumnKey(path=(), leaf="created_at"),),
            column_filter_key=SqlExprKey(canonical_sql="status = 'paid'"),
        )
        slot = _slot(
            key,
            declared_name="paid_amount_first",
            public_name="paid_amount_first",
            slot_type=DataType.DOUBLE,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.paid_amount_first",
        )
        assert spec.time_column == "orders.created_at"
        assert spec.filter_sql is not None
        assert "status" in spec.filter_sql


# ---------------------------------------------------------------------------
# _build_agg_render_spec_from_planned — custom aggregations
# ---------------------------------------------------------------------------


class TestBuilderCustomAggregation:
    def test_custom_aggregation_def_threaded(self):
        # ``rolling_avg`` is declared on the model's ``aggregations`` list.
        # The builder must look it up and pin ``spec.aggregation_def``.
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="rolling_avg",
            kwargs=(("window", Decimal("6")),),
        )
        slot = _slot(
            key,
            declared_name="amount_rolling_avg",
            public_name="amount_rolling_avg",
            slot_type=DataType.DOUBLE,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.amount_rolling_avg",
        )
        assert spec.aggregation == "rolling_avg"
        assert spec.aggregation_def is not None
        assert spec.aggregation_def.name == "rolling_avg"
        # Kwargs stringified via ``agg_kwarg_canonical_str``.
        assert spec.agg_kwargs == {"window": "6"}

    def test_unknown_aggregation_raises(self):
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="not_a_real_agg",
        )
        slot = _slot(key, declared_name="amount_x", public_name="amount_x")
        with pytest.raises(AggregationNotAllowedError, match=r"unknown aggregation"):
            _invoke(
                slot=slot,
                key=key,
                source_model=_orders_model(),
                source_relation="orders",
                full_alias="orders.amount_x",
            )


# ---------------------------------------------------------------------------
# _build_agg_render_spec_from_planned — parametric / stat aggs
# ---------------------------------------------------------------------------


class TestBuilderParametric:
    def test_percentile_with_p_kwarg(self):
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="percentile",
            kwargs=(("p", Decimal("0.5")),),
        )
        slot = _slot(
            key,
            declared_name="amount_percentile_p_0_5",
            public_name="amount_percentile_p_0_5",
            slot_type=DataType.DOUBLE,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.amount_percentile_p_0_5",
        )
        assert spec.aggregation == "percentile"
        assert spec.agg_kwargs == {"p": "0.5"}
        assert spec.sql == "amount"

    def test_stat_agg_with_other_kwarg(self):
        # ``corr(amount, other=quantity)`` — both legs surface;
        # ``other`` kwarg is canonicalised.
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="corr",
            kwargs=(("other", ColumnKey(path=(), leaf="quantity")),),
        )
        slot = _slot(
            key,
            declared_name="amount_corr",
            public_name="amount_corr",
            slot_type=DataType.DOUBLE,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.amount_corr",
        )
        assert spec.aggregation == "corr"
        # The ``other=`` column kwarg canonicalises to the qualified name
        # (mirrors ``agg_kwarg_canonical_str`` for ColumnKey).
        assert "other" in spec.agg_kwargs
        assert spec.agg_kwargs["other"] == "quantity"


# ---------------------------------------------------------------------------
# _build_agg_render_spec_from_planned — cross-model kwarg path checks
# ---------------------------------------------------------------------------


class TestBuilderCrossModelKwargPath:
    def test_kwarg_columnkey_path_mismatch_raises(self):
        # Aggregate value column is local; kwarg ColumnKey carries a join
        # path — that's a meaningless cross-model kwarg and must reject.
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="weighted_avg",
            kwargs=(
                ("weight", ColumnKey(path=("customers",), leaf="quantity")),
            ),
        )
        slot = _slot(
            key,
            declared_name="amount_weighted_avg",
            public_name="amount_weighted_avg",
            slot_type=DataType.DOUBLE,
        )
        with pytest.raises(AggregationNotAllowedError, match=r"kwarg .* references ColumnKey"):
            _invoke(
                slot=slot,
                key=key,
                source_model=_orders_model(),
                source_relation="orders",
                full_alias="orders.amount_weighted_avg",
            )

    def test_matching_kwarg_path_accepted(self):
        # Mirror image: same path on source AND kwarg → accepted.
        # Cross-model rerooting strips ``("customers",)`` from both source
        # and kwargs before the builder sees them, so the test passes the
        # local-shaped key (path=()) against the customers source model.
        customers = SlayerModel(
            name="customers",
            data_source="prod",
            sql_table="customers",
            columns=[
                Column(name="amount", type=DataType.DOUBLE),
                Column(name="quantity", type=DataType.INT),
            ],
        )
        local_key = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="weighted_avg",
            kwargs=(
                ("weight", ColumnKey(path=(), leaf="quantity")),
            ),
        )
        slot = _slot(
            local_key,
            declared_name="customers_amount_weighted_avg",
            public_name="customers_amount_weighted_avg",
            slot_type=DataType.DOUBLE,
        )
        spec = _invoke(
            slot=slot,
            key=local_key,
            source_model=customers,
            source_relation="customers",
            full_alias="customers.amount_weighted_avg",
        )
        assert spec.agg_kwargs == {"weight": "quantity"}


# ---------------------------------------------------------------------------
# _build_agg_render_spec_from_planned — type propagation
# ---------------------------------------------------------------------------


class TestBuilderTypePropagation:
    def test_slot_type_distinct_from_column_type(self):
        # Codex finding #4: ``spec.type`` (outer agg result type) MUST
        # come from ``slot.type``; ``spec.column_type`` (inner
        # pre-aggregation expression CAST) MUST come from the resolved
        # source column. ``*:count`` on a DOUBLE-typed column is the
        # canonical case where slot.type (INT — count returns integer)
        # differs from column_type (which is moot for star — DOUBLE).
        # Use an explicit ColumnKey case so both fields are populated and
        # distinct: ``count`` of ``amount`` (DOUBLE) → slot.type INT,
        # column_type DOUBLE.
        key = AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="count")
        slot = _slot(
            key,
            declared_name="amount_count",
            public_name="amount_count",
            slot_type=DataType.INT,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.amount_count",
        )
        assert spec.type is DataType.INT
        assert spec.column_type is DataType.DOUBLE

    def test_slot_type_carried_as_outer_cast_type(self):
        # ``slot.type`` (the resolved outer aggregation type) propagates
        # onto ``spec.type`` so callers can ``_wrap_cast_for_type`` the
        # final agg expression.
        key = AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="sum")
        slot = _slot(
            key,
            declared_name="amount_sum",
            public_name="amount_sum",
            slot_type=DataType.DOUBLE,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.amount_sum",
        )
        assert spec.type is DataType.DOUBLE

    def test_slot_none_passes_through(self):
        # HAVING-only synth has ``slot=None`` (no declared projection slot).
        # That must NOT raise; spec.type just becomes ``None``.
        key = AggregateKey(source=ColumnKey(path=(), leaf="amount"), agg="sum")
        spec = _invoke(
            slot=None,
            key=key,
            source_model=_orders_model(),
            source_relation="orders",
            full_alias="orders.amount_sum",
        )
        assert spec.type is None
        assert spec.column_type is DataType.DOUBLE  # column_type still resolved
