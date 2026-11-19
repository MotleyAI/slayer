"""The P-G call-site migration: the five render families route
through ``render_value_key`` with byte-identical SQL.

Behavioural unit tests for the renderer surface the migration added: the
``FilterFacilities.agg_builder`` HAVING seam (slot lookup + ``having_full_alias``
recovery), the ``cast_field_sql`` filter-CAST policy, the
``paren_comparison_operands`` grouping policy, the alias-exclusive resolution
mode with table qualification, the ``Optional`` scope fail-closed rule, and
``ScopeFrame.field_type``.

The transitional migration pins (per-family raising sentinels + the static
state-1 inventory) were deleted with the legacy renderers:
once a symbol no longer exists, any reference to it fails the suite outright, so
symbol-absence tests are the parity ballast P-J forbids.
"""

from __future__ import annotations

import tempfile
from decimal import Decimal

import pytest
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import RenderContextMissingFacilityError
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    FieldKey,
    FieldSqlKey,
    LiteralKey,
    TimeTruncKey,
    TransformKey,
)
from slayer.core.models import (
    Aggregation,
    DatasetField,
    DatasourceConfig,
    DatasetRelationship,
    SlayerDataset,
)
from slayer.core.query import QueryDimension, QueryMetric, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import get_dialect
from slayer.sql.naming import AliasAllocator
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.render.value_expr import (
    AliasFacilities,
    CompositeFacilities,
    FilterFacilities,
    RenderContext,
    _filter_cast_type,
    _wrap_cast_for_type,
    contains_aggregate,
    render_value_key,
)
from slayer.sql.scope import ScopeFrame
from slayer.storage.yaml_storage import YAMLStorage
from slayer.core.keys import Phase, ScalarCallKey
from slayer.ir.planned import ValueSlot


# ===========================================================================
# Model graph + generation helper (dry_run, one bundle covers every shape).
# ===========================================================================
def _orders() -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="customer_id", datatype=DataType.FLOAT),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
            DatasetField(name="qty", datatype=DataType.FLOAT),
            DatasetField(name="disc", datatype=DataType.FLOAT),
            DatasetField(name="created_at", datatype=DataType.DATETIME),
            DatasetField(name="net", sql="amount - 1", datatype=DataType.FLOAT),
            # A NON-trivial DERIVED TIMESTAMP: the filter-CAST policy must
            # suppress the CAST for temporal types (Codex F9 / _filter_cast_type).
            DatasetField(name="net_ts", sql="coalesce(created_at, created_at)",
                   datatype=DataType.DATETIME),
            # A MIXED-CASE physical column: quoting must survive on the
            # shifted-CTE WHERE path, which stringifies without a re-parse
            # (byte-parity risk register / Codex F15).
            DatasetField(name="MixedCol", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="customers", from_columns=["customer_id"], to_columns=["id"])],
        aggregations=[Aggregation(name="sum_sq", expression="SUM({value} * {value})")],
    )


def _customers() -> SlayerDataset:
    return SlayerDataset(
        name="customers", sql_table="customers", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="region_id", datatype=DataType.FLOAT),
            DatasetField(name="spend", datatype=DataType.FLOAT),
            DatasetField(name="deep_pop", sql="regions.population", datatype=DataType.FLOAT),
        ],
        relationships=[DatasetRelationship(to="regions", from_columns=["region_id"], to_columns=["id"])],
    )


def _regions() -> SlayerDataset:
    return SlayerDataset(
        name="regions", sql_table="regions", data_source="test",
        fields=[
            DatasetField(name="id", datatype=DataType.FLOAT, primary_key=True),
            DatasetField(name="population", datatype=DataType.FLOAT),
            DatasetField(name="tier", datatype=DataType.STRING),
        ],
    )


async def _generate(query: SlayerQuery, *, dialect: str = "postgres") -> str:
    """Render ``query`` (dry_run) against the orders → customers → regions
    bundle and return the emitted SQL. One bundle reaches every family."""
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(DatasourceConfig(name="test", type=dialect))
        await storage.save_dataset(_orders())
        await storage.save_dataset(_customers())
        await storage.save_dataset(_regions())
        engine = SlayerQueryEngine(storage=storage)
        response = await engine.execute(query, dry_run=True)
        assert response.sql is not None
        return response.sql


_MONTH = TimeDimension(
    field="created_at", granularity=TimeGranularity.MONTH,
)


# --------------------------------------------------------------------------- #
# Per-family production shapes (probe-verified to reach the named renderer).
# --------------------------------------------------------------------------- #
_FILTER_SHAPES = {
    "host_where_scalar": SlayerQuery(
        population="orders",
        dimensions=[QueryDimension(field="status")],
        metrics=[QueryMetric(metric="count(*)")],
        filter=["ifnull(disc, 0) > 1"],
    ),
    "host_where_derived": SlayerQuery(
        population="orders",
        dimensions=[QueryDimension(field="status")],
        metrics=[QueryMetric(metric="count(*)")],
        filter=["net > 5"],
    ),
    "host_having_aggregate": SlayerQuery(
        population="orders",
        dimensions=[QueryDimension(field="status")],
        metrics=[QueryMetric(metric="sum(qty)", alias="q")],
        filter=["sum(qty) > 5"],
    ),
    "shifted_cte_where": SlayerQuery(
        population="orders",
        time_dimensions=[_MONTH],
        metrics=[
            QueryMetric(metric="sum(amount)", alias="amt"),
            QueryMetric(metric="time_shift(sum(amount), -1, 'month')", alias="prev"),
        ],
        filter=["status = 'new'"],
    ),
    "shifted_cte_where_mixed_case": SlayerQuery(
        population="orders",
        time_dimensions=[_MONTH],
        metrics=[
            QueryMetric(metric="sum(amount)", alias="amt"),
            QueryMetric(metric="time_shift(sum(amount), -1, 'month')", alias="prev"),
        ],
        filter=["MixedCol > 5"],
    ),
}

def _scope(dialect: str = "postgres") -> ScopeFrame:
    host, cust, reg = _orders(), _customers(), _regions()
    alloc = AliasAllocator()
    bundle = ResolvedSourceBundle(
        dialect="postgres",
        population=host, referenced_datasets=[host, cust, reg],
    )
    return ScopeFrame(
        scope_id=alloc.next_scope_id(host.name),
        root_dataset=host, root_relation=host.name,
        bundle=bundle, dialect=get_dialect(dialect), allocator=alloc,
    )


def _emit(expr: exp.Expression, dialect: str = "postgres") -> str:
    return expr.sql(dialect=dialect)


class TestOptionalScopeFailsClosed:
    """``RenderContext.scope`` becomes Optional; a scope-needing key kind raises
    ``RenderContextMissingFacilityError`` rather than an ``AttributeError`` when
    the scope is absent."""

    def test_context_accepts_scope_none(self) -> None:
        ctx = RenderContext(scope=None, dialect=get_dialect("postgres"))
        assert ctx.scope is None

    @pytest.mark.parametrize(
        "label,key",
        [
            ("column", FieldKey(leaf="amount")),
            ("derived", FieldSqlKey(dataset="orders", field_name="net")),
            ("time_trunc", TimeTruncKey(
                source=FieldKey(leaf="created_at"), granularity=TimeGranularity.MONTH)),
            ("aggregate", AggregateKey(source=FieldKey(leaf="amount"), agg="sum")),
            ("nested_in_arithmetic", ArithmeticKey(
                op="+",
                operands=(FieldKey(leaf="amount"), LiteralKey(value=Decimal(1))),
            )),
        ],
    )
    def test_scope_needing_key_raises_when_scope_absent(self, label, key) -> None:
        ctx = RenderContext(scope=None, dialect=get_dialect("postgres"))
        with pytest.raises(RenderContextMissingFacilityError):
            render_value_key(key=key, ctx=ctx)


class TestScopeFrameColumnType:
    """``ScopeFrame.field_type(ref)`` gives the renderer the declared type for
    the filter-CAST policy, using the same model lookup as resolution."""

    def test_local_column_type(self) -> None:
        assert _scope().field_type(FieldKey(leaf="amount")) is DataType.FLOAT

    def test_derived_column_type(self) -> None:
        key = FieldSqlKey(dataset="orders", field_name="net")
        assert _scope().field_type(key) is DataType.FLOAT

    def test_temporal_column_type(self) -> None:
        assert _scope().field_type(FieldKey(leaf="created_at")) is DataType.DATETIME

    def test_joined_column_type_uses_the_owning_model(self) -> None:
        key = FieldSqlKey(
            path=("customers",), dataset="customers", field_name="spend",
        )
        assert _scope().field_type(key) is DataType.FLOAT

    def test_unknown_column_type_is_none(self) -> None:
        assert _scope().field_type(FieldKey(leaf="does_not_exist")) is None

    def test_unknown_derived_column_type_is_none(self) -> None:
        key = FieldSqlKey(dataset="orders", field_name="does_not_exist")
        assert _scope().field_type(key) is None


class TestFilterCastPolicyMovedToRenderPackage:
    """``_filter_cast_type`` + ``_wrap_cast_for_type`` move into the render
    package so the CAST policy is renderer-visible (the generator keeps thin
    delegating aliases). ``_filter_cast_type`` suppresses the CAST for temporal
    types only."""

    def test_functions_importable_from_render_package(self) -> None:
        assert callable(_filter_cast_type)
        assert callable(_wrap_cast_for_type)

    def test_temporal_types_suppress_the_cast(self) -> None:
        assert _filter_cast_type(DataType.DATE) is None
        assert _filter_cast_type(DataType.DATETIME) is None

    def test_non_temporal_type_passes_through(self) -> None:
        assert _filter_cast_type(DataType.FLOAT) is DataType.FLOAT


class TestFacilityFieldStructure:
    """The migration's facility-shape contract, asserted structurally so a flag
    that Pydantic silently ignores (extra kwargs) cannot make a behavioural test
    pass for the wrong reason (Codex F5)."""

    def test_filter_facilities_fields(self) -> None:
        fields = FilterFacilities.model_fields
        assert "agg_builder" in fields
        assert "cast_field_sql" in fields
        assert "paren_comparison_operands" in fields
        # The obsolete dead-type field is gone (PR 6 deletes the type).
        assert "first_last_state" not in fields
        # The paren policy defaults to the legacy "more grouping" behaviour.
        assert FilterFacilities().paren_comparison_operands is True

    def test_alias_facilities_has_table_qualifier_field(self) -> None:
        assert "table_by_slot_id" in AliasFacilities.model_fields
        assert AliasFacilities().table_by_slot_id == {}


_NET = FieldSqlKey(dataset="orders", field_name="net")          # DOUBLE, derived
_NET_TS = FieldSqlKey(dataset="orders", field_name="net_ts")    # TIMESTAMP, derived


class TestFilterCastColumnSql:
    """``FilterFacilities.cast_field_sql`` applies the filter-CAST policy to a
    derived ``ColumnSqlKey`` leaf (True for the filter family; False for
    target-scope, which never casts). Asserted on the AST node, not a substring,
    so the wrong expression / a double CAST cannot slip through (Codex F9)."""

    def _out(self, key, **facilities):
        ctx = RenderContext(
            scope=_scope(), dialect=get_dialect("postgres"),
            filters=FilterFacilities(**facilities),
        )
        return render_value_key(key=key, ctx=ctx)

    def test_cast_wraps_a_non_temporal_derived_leaf(self) -> None:
        out = self._out(_NET, cast_field_sql=True)
        assert isinstance(out, exp.Cast), _emit(out)
        # It casts the derived expression itself, not something else.
        assert "amount" in _emit(out).lower(), _emit(out)

    def test_temporal_derived_leaf_is_not_cast(self) -> None:
        out = self._out(_NET_TS, cast_field_sql=True)
        assert not isinstance(out, exp.Cast), _emit(out)

    def test_no_cast_when_flag_false(self) -> None:
        out = self._out(_NET, cast_field_sql=False)
        assert not isinstance(out, exp.Cast), _emit(out)

    def test_no_cast_without_filter_facility(self) -> None:
        ctx = RenderContext(scope=_scope(), dialect=get_dialect("postgres"))
        out = render_value_key(key=_NET, ctx=ctx)
        assert not isinstance(out, exp.Cast), _emit(out)


class TestAggregateBranchPrecedence:
    """The ``AggregateKey`` branch resolves in one fixed order:
    ``ctx.aliases`` (POST-phase, referenced by materialised alias) →
    ``ctx.filters.agg_builder`` (HAVING) → ``ctx.composites.agg_builder``
    (AGGREGATE composite) → the built-in simple/distinct fallback → raise
    (Codex F6)."""

    def _key(self) -> AggregateKey:
        return AggregateKey(source=FieldKey(leaf="amount"), agg="sum")

    def test_aliases_win_over_filter_builder(self) -> None:

        key = self._key()
        calls: list[str] = []

        def filt_builder(*_a, **_kw):
            calls.append("filter")
            return exp.column("FILTER")

        ctx = RenderContext(
            scope=_scope(), dialect=get_dialect("postgres"),
            aliases=AliasFacilities(
                slot_id_by_key={key: "s1"},
                available_alias_by_slot_id={"s1": "orders.rev"},
            ),
            filters=FilterFacilities(agg_builder=filt_builder),
            composites=CompositeFacilities(),
        )
        out = render_value_key(key=key, ctx=ctx)
        assert out.name == "orders.rev", _emit(out)
        assert calls == [], "filter builder must not run when aliases resolve it"

    def test_filter_builder_wins_over_composite_builder(self) -> None:

        key = self._key()

        def filt_builder(*_a, **_kw):
            return exp.column("FILTER")

        def comp_builder(*_a, **_kw):
            return exp.column("COMPOSITE")

        ctx = RenderContext(
            scope=_scope(), dialect=get_dialect("postgres"),
            filters=FilterFacilities(agg_builder=filt_builder),
            composites=CompositeFacilities(agg_builder=comp_builder),
        )
        assert _emit(render_value_key(key=key, ctx=ctx)) == "FILTER"

    def test_composite_builder_used_when_no_filter_builder(self) -> None:

        key = self._key()
        ctx = RenderContext(
            scope=_scope(), dialect=get_dialect("postgres"),
            composites=CompositeFacilities(agg_builder=lambda k: exp.column("COMPOSITE")),
        )
        assert _emit(render_value_key(key=key, ctx=ctx)) == "COMPOSITE"

    def test_builtin_fallback_when_no_builder(self) -> None:

        key = self._key()
        ctx = RenderContext(
            scope=_scope(), dialect=get_dialect("postgres"),
            composites=CompositeFacilities(),
        )
        assert _emit(render_value_key(key=key, ctx=ctx)) == "SUM(orders.amount)"


class TestParenComparisonOperands:
    """``FilterFacilities.paren_comparison_operands`` reproduces the legacy
    ``_paren_if_binary`` policy byte-for-byte: every multi-term comparison
    operand is parenthesised (strictly more grouping than the shared composer
    derives, never less)."""

    def _cmp_key(self) -> ArithmeticKey:
        return ArithmeticKey(
            op=">",
            operands=(
                ArithmeticKey(
                    op="+",
                    operands=(
                        FieldKey(leaf="amount"),
                        LiteralKey(value=Decimal(1)),
                    ),
                ),
                LiteralKey(value=Decimal(5)),
            ),
        )

    def test_flag_on_parenthesises_multiterm_operand(self) -> None:
        ctx = RenderContext(
            scope=_scope(), dialect=get_dialect("postgres"),
            filters=FilterFacilities(paren_comparison_operands=True),
        )
        assert _emit(render_value_key(key=self._cmp_key(), ctx=ctx)) == \
            "(orders.amount + 1) > 5"

    def test_flag_off_uses_minimal_grouping(self) -> None:
        ctx = RenderContext(
            scope=_scope(), dialect=get_dialect("postgres"),
            filters=FilterFacilities(paren_comparison_operands=False),
        )
        assert _emit(render_value_key(key=self._cmp_key(), ctx=ctx)) == \
            "orders.amount + 1 > 5"

    def test_no_filter_facility_uses_minimal_grouping(self) -> None:
        ctx = RenderContext(scope=_scope(), dialect=get_dialect("postgres"))
        assert _emit(render_value_key(key=self._cmp_key(), ctx=ctx)) == \
            "orders.amount + 1 > 5"


class TestFilterAggBuilderSeam:
    """``FilterFacilities.agg_builder`` is the HAVING seam. The renderer does
    the ``slot_by_key`` lookup and the ``having_full_alias`` recovery itself
    (so those fields are genuinely consumed), then hands ``(key, slot,
    having_full_alias)`` to the generator's builder."""

    def _slot(self, key):

        return ValueSlot(
            id="s1", key=key, declared_name="q", phase=Phase.AGGREGATE,
            type=DataType.FLOAT,
        )

    def _key(self) -> AggregateKey:
        return AggregateKey(source=FieldKey(leaf="qty"), agg="sum")

    def _ctx(self, **facilities):
        recorded: dict = {}

        def builder(key, slot, having_full_alias):
            recorded["key"] = key
            recorded["slot"] = slot
            recorded["having_full_alias"] = having_full_alias
            return exp.column("SENTINEL")

        ctx = RenderContext(
            scope=_scope(), dialect=get_dialect("postgres"),
            filters=FilterFacilities(agg_builder=builder, **facilities),
        )
        return ctx, recorded

    def test_slot_alias_recovered_from_aliases_by_slot_id(self) -> None:
        key = self._key()
        slot = self._slot(key)
        ctx, recorded = self._ctx(
            slot_by_key={key: slot},
            aliases_by_slot_id={"s1": ["orders.q"]},
        )
        render_value_key(key=key, ctx=ctx)
        assert recorded["slot"] is slot
        assert recorded["having_full_alias"] == "orders.q"

    def test_placeholder_alias_when_slot_has_no_alias_entry(self) -> None:
        key = self._key()
        slot = self._slot(key)
        ctx, recorded = self._ctx(
            slot_by_key={key: slot},
            aliases_by_slot_id={},
        )
        render_value_key(key=key, ctx=ctx)
        assert recorded["having_full_alias"] == "__having_ref__"

    def test_empty_alias_list_falls_back_to_placeholder(self) -> None:
        """An empty alias list is 'not materialised as a projected column', so
        the recovery must fall back to the placeholder rather than index [0]
        into an empty list (Codex F14)."""
        key = self._key()
        slot = self._slot(key)
        ctx, recorded = self._ctx(
            slot_by_key={key: slot},
            aliases_by_slot_id={"s1": []},
        )
        render_value_key(key=key, ctx=ctx)
        assert recorded["having_full_alias"] == "__having_ref__"

    def test_no_slot_passes_none_and_placeholder(self) -> None:
        key = self._key()
        ctx, recorded = self._ctx(slot_by_key={}, aliases_by_slot_id={})
        render_value_key(key=key, ctx=ctx)
        assert recorded["slot"] is None
        assert recorded["having_full_alias"] == "__having_ref__"

    def test_builder_output_is_returned(self) -> None:
        key = self._key()
        ctx, _ = self._ctx(slot_by_key={}, aliases_by_slot_id={})
        assert _emit(render_value_key(key=key, ctx=ctx)) == "SENTINEL"


_SLOTTED_KINDS = {
    "column": FieldKey(leaf="amount"),
    "derived": FieldSqlKey(dataset="orders", field_name="net"),
    "time_trunc": TimeTruncKey(
        source=FieldKey(leaf="created_at"), granularity=TimeGranularity.MONTH),
    "aggregate": AggregateKey(source=FieldKey(leaf="amount"), agg="sum"),
    "transform": TransformKey(
        op="cumsum",
        input=AggregateKey(source=FieldKey(leaf="amount"), agg="sum"),
    ),
}


class TestAliasExclusiveMode:
    """When ``ctx.aliases`` is set, the five slotted kinds resolve ONLY through
    the alias maps (POST-phase: rebuilding from source would be wrong SQL). The
    context carries ``scope=None`` deliberately — alias resolution must NOT need
    a scope (Codex F1) — and a qualifier from ``table_by_slot_id`` is emitted; a
    miss raises rather than falling back to source."""

    def _ctx(self, key, *, table=None, present=True, scope=None) -> RenderContext:
        slot_id_by_key = {key: "s1"} if present else {}
        return RenderContext(
            scope=scope, dialect=get_dialect("postgres"),
            aliases=AliasFacilities(
                slot_id_by_key=slot_id_by_key,
                available_alias_by_slot_id={"s1": "orders.rev"},
                table_by_slot_id=({"s1": table} if table else {}),
            ),
        )

    @pytest.mark.parametrize("label", sorted(_SLOTTED_KINDS))
    def test_every_slotted_kind_resolves_by_alias_without_scope(self, label) -> None:
        key = _SLOTTED_KINDS[label]
        out = render_value_key(key=key, ctx=self._ctx(key))
        assert isinstance(out, exp.Column), f"{label}: {type(out).__name__}"
        assert out.name == "orders.rev", f"{label}: {_emit(out)}"

    def test_bare_alias_when_no_table_qualifier(self) -> None:
        key = FieldKey(leaf="amount")
        out = render_value_key(key=key, ctx=self._ctx(key))
        assert _emit(out) == '"orders.rev"', _emit(out)

    def test_base_qualified_alias(self) -> None:
        key = FieldKey(leaf="amount")
        out = render_value_key(key=key, ctx=self._ctx(key, table="_base"))
        assert _emit(out) == '_base."orders.rev"', _emit(out)

    def test_cross_model_cte_qualifier(self) -> None:
        key = AggregateKey(source=FieldKey(leaf="amount"), agg="sum")
        out = render_value_key(key=key, ctx=self._ctx(key, table="_cm_x"))
        assert _emit(out) == '_cm_x."orders.rev"', _emit(out)

    @pytest.mark.parametrize("label", sorted(_SLOTTED_KINDS))
    def test_miss_raises(self, label) -> None:
        key = _SLOTTED_KINDS[label]
        ctx = self._ctx(key, present=False)
        with pytest.raises(RenderContextMissingFacilityError):
            render_value_key(key=key, ctx=ctx)

    def test_miss_does_not_fall_back_to_scope(self) -> None:
        """A miss is a promotion bug, not a cue to rebuild from source — even
        when a real scope is present, alias mode raises rather than resolving
        the key against the scope root."""
        key = FieldKey(leaf="amount")
        ctx = self._ctx(key, present=False, scope=_scope())
        with pytest.raises(RenderContextMissingFacilityError):
            render_value_key(key=key, ctx=ctx)


class TestMixedCaseShiftedCteParity:
    """Byte-parity guard for the one path the plan flagged as risky: the
    time_shift shifted-CTE WHERE stringifies without a downstream re-parse, so
    mixed-case identifier quoting must be produced by the render path itself.
    Holds before AND after migration — a regression here means the migrated
    filter path dropped the ``_to_ident`` mixed-case quoting."""

    async def test_mixed_case_column_stays_quoted_in_shifted_cte(self) -> None:
        sql = await _generate(_FILTER_SHAPES["shifted_cte_where_mixed_case"])
        assert '"MixedCol"' in sql, sql
        # And never leaks as a bare (unquoted, case-folding) identifier.
        assert "MixedCol > 5" not in sql.replace('"MixedCol"', ""), sql


class TestContainsAggregateEquivalence:
    """The composite live call site swaps ``_build_agg``'s ``is_agg`` bool for
    ``contains_aggregate(key)``. They agree on the composite shapes: an
    arithmetic-of-aggregates contains an aggregate; a pure column/literal
    composite does not (so it is not routed into GROUP BY / HAVING)."""

    def test_arithmetic_of_aggregates_contains_aggregate(self) -> None:
        key = ArithmeticKey(
            op="-",
            operands=(
                AggregateKey(source=FieldKey(leaf="amount"), agg="sum"),
                AggregateKey(source=FieldKey(leaf="qty"), agg="sum"),
            ),
        )
        assert contains_aggregate(key) is True

    def test_scalar_over_aggregate_contains_aggregate(self) -> None:

        key = ScalarCallKey(
            name="ifnull",
            args=(
                AggregateKey(source=FieldKey(leaf="amount"), agg="sum"),
                LiteralKey(value=Decimal(0)),
            ),
        )
        assert contains_aggregate(key) is True

    def test_pure_arithmetic_without_aggregate_is_not_aggregate(self) -> None:
        key = ArithmeticKey(
            op="+",
            operands=(FieldKey(leaf="amount"), LiteralKey(value=Decimal(1))),
        )
        assert contains_aggregate(key) is False
