"""AggRenderSpec + ``_build_agg_render_spec_from_planned``.

Decouples ``slayer/sql/generator.py``'s dialect helpers from the legacy
``legacy enriched-measure``. The new ``AggRenderSpec`` is the frozen Pydantic record
that ``_build_agg`` / ``_build_formula_agg`` / ``_build_percentile`` /
``_build_stat_agg`` / ``_resolve_value_sql`` / ``_resolve_agg_param`` /
``_wrap_cast_for_type`` consume.

These tests fail against current code because ``AggRenderSpec`` and
``_build_agg_render_spec_from_planned`` do not yet exist; the import is the
failure point.

``_build_agg_render_spec_from_planned`` keeps the same five-kwarg signature
as the legacy ``_synthesize_enriched_measure_from_planned``
(``slot``, ``key``, ``source_dataset``, ``source_relation``, ``full_alias``)
so existing call sites flip with a one-line return-type swap. The logic
mirrors the legacy synthesizer (``slayer/sql/generator.py:7033``) verbatim:

* ``StarKey`` rejects any non-count aggregation, and any args/kwargs on
  ``count(*)``.
* ``FieldKey`` / ``FieldSqlKey`` resolves the source field on
  ``source_dataset``; aggregations outside the built-in slice look up the
  custom ``Aggregation`` definition on ``source_dataset.aggregations`` and
  raise ``AggregationNotAllowedError`` on miss.
* ``first`` / ``last`` derive ``time_field`` from the first ``FieldKey``
  in ``key.args``.
* Cross-dataset kwargs whose ``FieldKey.path`` disagrees with
  ``source.path`` raise ``AggregationNotAllowedError``.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.enums import DataType
from slayer.core.errors import AggregationNotAllowedError
from slayer.core.keys import (
    AggregateKey,
    FieldKey,
    FieldSqlKey,
    Phase,
    StarKey,
)
from slayer.core.models import Aggregation, AggregationParam, DatasetField, SlayerDataset
from slayer.core.scope import DatasetScope
from slayer.engine.binding import bind_expr
from slayer.core.syntax import parse_expr
from slayer.ir.planned import ValueSlot
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.generator import (  # type: ignore[attr-defined]
    AggRenderSpec,
    SQLGenerator,
)
from slayer.sql.render.aggregates import resolve_agg_entry


# These imports drive the failing-test contract — neither name exists on
# the current codebase. Stage A landing flips them green.
# ``_build_agg_render_spec_from_planned`` is a method on ``SQLGenerator``;
# tests instantiate the generator to invoke it (see ``_invoke`` below).


def _invoke(slot, key, *, source_dataset, source_relation, full_alias):
    """Thin shim — instantiate SQLGenerator and invoke the new builder."""
    gen = SQLGenerator(dialect="postgres")
    return gen._build_agg_render_spec_from_planned(  # type: ignore[attr-defined]
        slot=slot,
        key=key,
        source_dataset=source_dataset,
        source_relation=source_relation,
        full_alias=full_alias,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _orders_dataset() -> SlayerDataset:
    """Local source dataset used by the bulk of the tests."""
    return SlayerDataset(
        name="orders",
        data_source="prod",
        sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.INTEGER),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
            DatasetField(
                name="net_amount",
                expression="amount - tax",
                datatype=DataType.FLOAT,
            ),
            DatasetField(name="tax", datatype=DataType.FLOAT),
            DatasetField(name="quantity", datatype=DataType.INTEGER),
            # Derived time fields covering the two FieldSqlKey shapes
            # the explicit-time-arg resolver must handle:
            # bare-identifier rename and a non-trivial expression.
            DatasetField(
                name="created_at_alias",
                sql="created_at",
                datatype=DataType.DATETIME,
            ),
            DatasetField(
                name="created_at_day",
                expression="DATE_TRUNC('day', created_at)",
                datatype=DataType.DATE,
            ),
        ],
        aggregations=[
            Aggregation(
                name="rolling_avg",
                expression=ROLLING_AVG,
                params=[
                    AggregationParam(name="time", sql="created_at"),
                    AggregationParam(name="rows", sql="6"),
                ],
            ),
            # Dataset-level override of a BUILT-IN aggregation, supplying a
            # default ``weight=quantity`` bound onto the key at bind.
            Aggregation(
                name="weighted_avg",
                expression=WEIGHTED_AVG,
                params=[
                    AggregationParam(name="weight", sql="quantity"),
                ],
            ),
        ],
    )


ROLLING_AVG = "AVG({value}) OVER (ORDER BY {time} ROWS BETWEEN {rows} PRECEDING AND CURRENT ROW)"
WEIGHTED_AVG = "SUM({value} * {weight}) / NULLIF(SUM({weight}), 0)"


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
        # Decision #4: exactly these 12 fields, no more, no less. Extra
        # fields (agg_args / source_measure_name / distinct / window /
        # user_declared / etc.) are deliberately NOT carried. A DatasetField.filter
        # rides its source FieldSqlKey now, so there is no ``filter_sql`` field.
        assert set(AggRenderSpec.model_fields) == {
            "sql",
            "name",
            "dataset_name",
            "aggregation",
            "expression",
            "agg_kwargs",
            "alias",
            "time_field",
            "type",
            "field_type",
            "input_type",
            "value",
            "element_type",
        }

    def test_minimal_count_star(self):
        spec = AggRenderSpec(
            sql=None,
            name="",
            dataset_name="orders",
            aggregation="count",
            alias="orders._count",
        )
        # All fields present; defaults for those omitted.
        assert spec.sql is None
        assert spec.name == ""
        assert spec.dataset_name == "orders"
        assert spec.aggregation == "count"
        assert spec.alias == "orders._count"
        assert spec.expression is None
        assert spec.agg_kwargs == {}
        assert spec.time_field is None
        assert spec.type is None
        assert spec.field_type is None

    def test_full_field_surface(self):
        spec = AggRenderSpec(
            sql="amount",
            name="amount",
            dataset_name="orders",
            aggregation="custom",
            expression="AVG({value})",
            alias="orders.amount_custom",
            time_field="orders.created_at",
            type=DataType.FLOAT,
            field_type=DataType.FLOAT,
        )
        assert spec.sql == "amount"
        assert spec.expression == "AVG({value})"
        assert spec.time_field == "orders.created_at"
        assert spec.type is DataType.FLOAT
        assert spec.field_type is DataType.FLOAT

    def test_frozen(self):
        spec = AggRenderSpec(
            sql=None,
            name="",
            dataset_name="orders",
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
            slot_type=DataType.INTEGER,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_dataset=_orders_dataset(),
            source_relation="orders",
            full_alias="orders._count",
        )
        assert spec.sql is None
        # ``name`` is empty for star — there's no source field.
        assert spec.name == ""
        assert spec.dataset_name == "orders"
        assert spec.aggregation == "count"
        assert spec.alias == "orders._count"
        assert spec.type is DataType.INTEGER
        assert spec.field_type is None
        assert spec.agg_kwargs == {}

    def test_non_count_star_raises(self):
        key = AggregateKey(source=StarKey(), agg="sum")
        slot = _slot(key, declared_name="_sum", public_name="_sum")
        source_model = _orders_dataset()
        with pytest.raises(ValueError, match=r"not allowed with metric '\*'"):
            _invoke(
                slot=slot,
                key=key,
                source_dataset=source_model,
                source_relation="orders",
                full_alias="orders._sum",
            )

    def test_star_with_args_raises(self):
        key = AggregateKey(source=StarKey(), agg="count", args=(Decimal("1"),))
        slot = _slot(key, declared_name="_count", public_name="_count")
        source_model = _orders_dataset()
        with pytest.raises(ValueError, match=r"count\(\*\).* no args"):
            _invoke(
                slot=slot,
                key=key,
                source_dataset=source_model,
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
        source_model = _orders_dataset()
        with pytest.raises(ValueError, match=r"count\(\*\).* no args or kwargs"):
            _invoke(
                slot=slot,
                key=key,
                source_dataset=source_model,
                source_relation="orders",
                full_alias="orders._count",
            )


# ---------------------------------------------------------------------------
# _build_agg_render_spec_from_planned — FieldKey / FieldSqlKey
# ---------------------------------------------------------------------------


class TestBuilderColumnKey:
    def test_bare_sum(self):
        key = AggregateKey(source=FieldKey(path=(), leaf="amount"), agg="sum")
        slot = _slot(
            key,
            declared_name="amount_sum",
            public_name="amount_sum",
            slot_type=DataType.FLOAT,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_dataset=_orders_dataset(),
            source_relation="orders",
            full_alias="orders.amount_sum",
        )
        # Bare field: ``DatasetField.sql`` is None on the orders.amount fixture,
        # so spec.sql falls back to the bare field name (mirrors legacy
        # ``sql = column.sql or column.name``).
        assert spec.sql == "amount"
        assert spec.name == "amount"
        assert spec.dataset_name == "orders"
        assert spec.aggregation == "sum"
        assert spec.alias == "orders.amount_sum"
        assert spec.type is DataType.FLOAT
        assert spec.field_type is DataType.FLOAT
        assert spec.time_field is None
        assert spec.agg_kwargs == {}
        assert spec.expression is None

    def test_columnsqlkey_derived_uses_column_sql(self):
        # The derived ``net_amount`` field has ``sql = "amount - tax"`` and
        # type DOUBLE; aggregating it must surface the expression as
        # ``spec.sql``.
        key = AggregateKey(
            source=FieldSqlKey(path=(), dataset="orders", field_name="net_amount"),
            agg="sum",
        )
        slot = _slot(
            key,
            declared_name="net_amount_sum",
            public_name="net_amount_sum",
            slot_type=DataType.FLOAT,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_dataset=_orders_dataset(),
            source_relation="orders",
            full_alias="orders.net_amount_sum",
        )
        assert spec.sql == "amount - tax"
        assert spec.name == "net_amount"
        assert spec.field_type is DataType.FLOAT

    def test_column_not_found_raises(self):
        key = AggregateKey(
            source=FieldKey(path=(), leaf="nonexistent"),
            agg="sum",
        )
        slot = _slot(key, declared_name="x_sum", public_name="x_sum")
        # Match the legacy synthesizer's message tightly so a regression
        # that surfaces a different ValueError (e.g. a setup failure) still
        # fails loudly. Legacy emits exactly:
        #     "Aggregate source field 'nonexistent' not found on dataset 'orders'"
        source_model = _orders_dataset()
        with pytest.raises(
            ValueError,
            match=r"Aggregate source field 'nonexistent' not found on dataset 'orders'",
        ):
            _invoke(
                slot=slot,
                key=key,
                source_dataset=source_model,
                source_relation="orders",
                full_alias="orders.x_sum",
            )


# ---------------------------------------------------------------------------
# _build_agg_render_spec_from_planned — custom aggregations
# ---------------------------------------------------------------------------


class TestBuilderCustomAggregation:
    def test_custom_aggregation_formula_threaded(self):
        # ``rolling_avg`` is declared on the dataset's ``aggregations`` list.
        # The builder must look it up and carry its expression.
        key = AggregateKey(
            source=FieldKey(path=(), leaf="amount"),
            agg="rolling_avg",
            kwargs=(("rows", Decimal("6")),),
        )
        slot = _slot(
            key,
            declared_name="amount_rolling_avg",
            public_name="amount_rolling_avg",
            slot_type=DataType.FLOAT,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_dataset=_orders_dataset(),
            source_relation="orders",
            full_alias="orders.amount_rolling_avg",
        )
        assert spec.aggregation == "rolling_avg"
        assert spec.expression == ROLLING_AVG
        assert set(spec.agg_kwargs) == {"rows"}

    def test_unknown_aggregation_raises(self):
        key = AggregateKey(
            source=FieldKey(path=(), leaf="amount"),
            agg="not_a_real_agg",
        )
        slot = _slot(key, declared_name="amount_x", public_name="amount_x")
        source_model = _orders_dataset()
        with pytest.raises(AggregationNotAllowedError, match=r"unknown aggregation"):
            _invoke(
                slot=slot,
                key=key,
                source_dataset=source_model,
                source_relation="orders",
                full_alias="orders.amount_x",
            )

    def test_builtin_aggregation_model_override_threaded(self):
        # A dataset-level override of a BUILT-IN aggregation (``weighted_avg`` with
        # a default ``weight=quantity``) binds its default onto the key, and the
        # builder carries the override's expression.
        orders = _orders_dataset()
        key = bind_expr(
            parse_expr("weighted_avg(amount)"), scope=DatasetScope(population=orders),
            bundle=ResolvedSourceBundle(dialect="postgres", population=orders),
        ).value_key
        assert isinstance(key, AggregateKey)
        assert dict(key.kwargs) == {"weight": FieldKey(path=(), leaf="quantity")}
        spec = _invoke(
            slot=_slot(key, declared_name="amount_weighted_avg",
                       public_name="amount_weighted_avg", slot_type=DataType.FLOAT),
            key=key,
            source_dataset=orders,
            source_relation="orders",
            full_alias="orders.amount_weighted_avg",
        )
        assert spec.aggregation == "weighted_avg"
        assert spec.expression == WEIGHTED_AVG

    def test_builtin_aggregation_no_override_leaves_formula_none(self):
        # The lookup is unconditional, but a built-in WITHOUT a dataset
        # override must still produce ``expression is None`` so the
        # dispatcher picks the built-in renderer. Pin this so the
        # CodeRabbit fix doesn't accidentally start picking up
        # unrelated dataset-level definitions.
        key = AggregateKey(
            source=FieldKey(path=(), leaf="amount"),
            agg="sum",
        )
        slot = _slot(
            key,
            declared_name="amount_sum",
            public_name="amount_sum",
            slot_type=DataType.FLOAT,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_dataset=_orders_dataset(),
            source_relation="orders",
            full_alias="orders.amount_sum",
        )
        assert spec.aggregation == "sum"
        assert spec.expression is None


# ---------------------------------------------------------------------------
# _build_agg_render_spec_from_planned — parametric / stat aggs
# ---------------------------------------------------------------------------


class TestBuilderParametric:
    def test_percentile_with_p_kwarg(self):
        key = AggregateKey(
            source=FieldKey(path=(), leaf="amount"),
            agg="percentile",
            kwargs=(("p", Decimal("0.5")),),
        )
        slot = _slot(
            key,
            declared_name="amount_percentile_p_0_5",
            public_name="amount_percentile_p_0_5",
            slot_type=DataType.FLOAT,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_dataset=_orders_dataset(),
            source_relation="orders",
            full_alias="orders.amount_percentile_p_0_5",
        )
        assert spec.aggregation == "percentile"
        assert set(spec.agg_kwargs) == {"p"}
        assert spec.sql == "amount"

    def test_stat_agg_with_other_kwarg(self):
        # ``corr(amount, other=quantity)`` — both legs surface;
        # ``other`` kwarg is canonicalised.
        key = AggregateKey(
            source=FieldKey(path=(), leaf="amount"),
            agg="corr",
            kwargs=(("other", FieldKey(path=(), leaf="quantity")),),
        )
        slot = _slot(
            key,
            declared_name="amount_corr",
            public_name="amount_corr",
            slot_type=DataType.FLOAT,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_dataset=_orders_dataset(),
            source_relation="orders",
            full_alias="orders.amount_corr",
        )
        assert spec.aggregation == "corr"
        assert set(spec.agg_kwargs) == {"other"}


# ---------------------------------------------------------------------------
# _build_agg_render_spec_from_planned — cross-dataset kwarg path checks
# ---------------------------------------------------------------------------


class TestBuilderCrossModelKwargPath:
    def test_local_source_pathed_kwarg_accepted(self):
        # A LOCAL aggregate with a structurally-crossing kwarg is
        # a supported crossing INPUT — the widened Law-3 trigger
        # isolates the aggregate host-rooted, and inside that CTE's
        # sub-render the kwarg resolves through the host scope. The spec
        # builder therefore no longer rejects it.
        key = AggregateKey(
            source=FieldKey(path=(), leaf="amount"),
            agg="weighted_avg",
            kwargs=(
                ("weight", FieldKey(path=("customers",), leaf="quantity")),
            ),
        )
        slot = _slot(
            key,
            declared_name="amount_weighted_avg",
            public_name="amount_weighted_avg",
            slot_type=DataType.FLOAT,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_dataset=_orders_dataset(),
            source_relation="orders",
            full_alias="orders.amount_weighted_avg",
        )
        assert spec is not None

    def test_local_source_pathed_columnsqlkey_kwarg_accepted(self):
        # Same relaxation for the FieldSqlKey kwarg kind.
        key = AggregateKey(
            source=FieldKey(path=(), leaf="amount"),
            agg="weighted_avg",
            kwargs=(
                (
                    "weight",
                    FieldSqlKey(
                        path=("customers",),
                        dataset="customers",
                        field_name="net_quantity",
                    ),
                ),
            ),
        )
        slot = _slot(
            key,
            declared_name="amount_weighted_avg",
            public_name="amount_weighted_avg",
            slot_type=DataType.FLOAT,
        )
        orders = _orders_dataset()
        customers = SlayerDataset(
            name="customers", data_source="prod", sql_table="customers",
            fields=[
                DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
                DatasetField(name="quantity", datatype=DataType.INTEGER),
                DatasetField(name="net_quantity", expression="quantity - 1", datatype=DataType.INTEGER),
            ],
        )
        spec = SQLGenerator(dialect="postgres")._build_agg_render_spec_from_planned(
            slot=slot,
            key=key,
            source_dataset=orders,
            source_relation="orders",
            full_alias="orders.amount_weighted_avg",
            bundle=ResolvedSourceBundle(
                dialect="postgres", population=orders, referenced_datasets=[customers]),
        )
        assert spec is not None

    def test_cross_model_source_kwarg_path_mismatch_still_raises(self):
        # The gate survives for CROSS-DATASET aggregates: after reroot
        # prefix-stripping, a kwarg on a DIFFERENT branch than the source
        # would bind against the wrong dataset — still a hard error.
        key = AggregateKey(
            source=FieldKey(path=("customers",), leaf="amount"),
            agg="weighted_avg",
            kwargs=(
                ("weight", FieldKey(path=("warehouses",), leaf="quantity")),
            ),
        )
        slot = _slot(
            key,
            declared_name="amount_weighted_avg",
            public_name="amount_weighted_avg",
            slot_type=DataType.FLOAT,
        )
        orders = _orders_dataset()
        with pytest.raises(
            AggregationNotAllowedError, match=r"kwarg .* references FieldKey",
        ):
            _invoke(
                slot=slot,
                key=key,
                source_dataset=orders,
                source_relation="orders",
                full_alias="orders.amount_weighted_avg",
            )

    def test_matching_kwarg_path_accepted(self):
        # Mirror image: same path on source AND kwarg → accepted.
        # Cross-dataset rerooting strips ``("customers",)`` from both source
        # and kwargs before the builder sees them, so the test passes the
        # local-shaped key (path=()) against the customers source dataset.
        customers = SlayerDataset(
            name="customers",
            data_source="prod",
            sql_table="customers",
            fields=[
                DatasetField(name="amount", datatype=DataType.FLOAT),
                DatasetField(name="quantity", datatype=DataType.INTEGER),
            ],
        )
        local_key = AggregateKey(
            source=FieldKey(path=(), leaf="amount"),
            agg="weighted_avg",
            kwargs=(
                ("weight", FieldKey(path=(), leaf="quantity")),
            ),
        )
        slot = _slot(
            local_key,
            declared_name="customers_amount_weighted_avg",
            public_name="customers_amount_weighted_avg",
            slot_type=DataType.FLOAT,
        )
        spec = _invoke(
            slot=slot,
            key=local_key,
            source_dataset=customers,
            source_relation="customers",
            full_alias="customers.amount_weighted_avg",
        )
        assert set(spec.agg_kwargs) == {"weight"}


# ---------------------------------------------------------------------------
# _build_agg_render_spec_from_planned — type propagation
# ---------------------------------------------------------------------------


class TestBuilderTypePropagation:
    def test_slot_type_distinct_from_column_type(self):
        # Codex finding #4: ``spec.type`` (outer agg result type) MUST
        # come from ``slot.type``; ``spec.field_type`` (inner
        # pre-aggregation expression CAST) MUST come from the resolved
        # source field. ``count(*)`` on a DOUBLE-typed field is the
        # canonical case where slot.type (INT — count returns integer)
        # differs from field_type (which is moot for star — DOUBLE).
        # Use an explicit FieldKey case so both fields are populated and
        # distinct: ``count`` of ``amount`` (DOUBLE) → slot.type INT,
        # field_type DOUBLE.
        key = AggregateKey(source=FieldKey(path=(), leaf="amount"), agg="count")
        slot = _slot(
            key,
            declared_name="amount_count",
            public_name="amount_count",
            slot_type=DataType.INTEGER,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_dataset=_orders_dataset(),
            source_relation="orders",
            full_alias="orders.amount_count",
        )
        assert spec.type is DataType.INTEGER
        assert spec.field_type is DataType.FLOAT

    def test_slot_type_carried_as_outer_cast_type(self):
        # ``slot.type`` (the resolved outer aggregation type) propagates
        # onto ``spec.type`` so callers can ``_wrap_cast_for_type`` the
        # final agg expression.
        key = AggregateKey(source=FieldKey(path=(), leaf="amount"), agg="sum")
        slot = _slot(
            key,
            declared_name="amount_sum",
            public_name="amount_sum",
            slot_type=DataType.FLOAT,
        )
        spec = _invoke(
            slot=slot,
            key=key,
            source_dataset=_orders_dataset(),
            source_relation="orders",
            full_alias="orders.amount_sum",
        )
        assert spec.type is DataType.FLOAT

    def test_slot_none_passes_through(self):
        # HAVING-only synth has ``slot=None`` (no declared projection slot).
        # That must NOT raise; spec.type just becomes ``None``.
        key = AggregateKey(source=FieldKey(path=(), leaf="amount"), agg="sum")
        spec = _invoke(
            slot=None,
            key=key,
            source_dataset=_orders_dataset(),
            source_relation="orders",
            full_alias="orders.amount_sum",
        )
        assert spec.type is None
        assert spec.field_type is DataType.FLOAT  # field_type still resolved


# ---------------------------------------------------------------------------
# ``_build_agg`` routes classification through the single
# ``AGG_REGISTRY`` table (was: ``_AGG_FUNCTION_MAP`` + ``agg_class_map`` + the
# ``count_distinct`` / ``median`` name-equality intercepts). Pin the merged
# dispatch: a SIMPLE class, both former shadow-intercept specials, and the
# unknown-name error path — the emitted SQL must be unchanged by the merge.
# ---------------------------------------------------------------------------
class TestBuildAggRegistryDispatch:
    def _spec(self, aggregation: str, *, sql: str | None = "amount") -> AggRenderSpec:
        return AggRenderSpec(
            sql=sql,
            name="amount" if sql else "",
            dataset_name="orders",
            aggregation=aggregation,
            alias=f"orders.amount_{aggregation}",
        )

    def test_simple_class_comes_from_the_registry_entry(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        expr, is_agg = gen._build_agg(self._spec("sum"))
        assert is_agg is True
        # The emitted node IS the registry entry's node_class — a hardcoded
        # branch would decouple these.
        assert type(expr) is resolve_agg_entry("sum").node_class
        assert expr.sql(dialect="postgres") == "SUM(orders.amount)"

    def test_count_star_renders_count_star(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        expr, is_agg = gen._build_agg(self._spec("count", sql=None))
        assert is_agg is True
        assert expr.sql(dialect="postgres") == "COUNT(*)"

    def test_count_distinct_builds_count_distinct(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        expr, is_agg = gen._build_agg(self._spec("count_distinct"))
        assert is_agg is True
        assert expr.sql(dialect="postgres") == "COUNT(DISTINCT orders.amount)"

    def test_median_routes_to_the_dialect_median_builder(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        expr, is_agg = gen._build_agg(self._spec("median"))
        assert is_agg is True
        # Postgres emits the ordered-set aggregate for a median.
        assert "PERCENTILE_CONT" in expr.sql(dialect="postgres").upper()

    def test_unknown_aggregation_without_a_formula_raises(self) -> None:
        gen = SQLGenerator(dialect="postgres")
        spec = self._spec("definitely_not_an_aggregation")
        with pytest.raises(ValueError, match="no expression"):
            gen._build_agg(spec)
