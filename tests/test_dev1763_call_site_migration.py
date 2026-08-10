"""DEV-1763 — the P-G call-site migration: route the five live render families
through ``render_value_key`` with byte-identical SQL, leaving the legacy
renderers production-unreferenced (P-J state 1).

This module is the migration's proof, in three layers:

1. **Per-family raising sentinels** (``Test*FamilySentinel``) — one class per
   legacy renderer, patching it to raise and running production query shapes
   the probe confirmed reach it. Each FAILS today (the legacy renderer is still
   on the production path) and PASSES once its family is migrated. A runtime
   proof, not a grep: a source scan cannot tell a live call from one inside a
   docstring or an unreachable branch (the ``tests/test_dev1747_order_resolver``
   pattern, ``TestSingleResolver``).

2. **The static state-1 inventory** (``TestState1Inventory``) — an ``ast`` walk
   asserting that after migration each legacy renderer's *external* call sites
   (callers other than its own body) are exactly the documented set: none for
   filter / aliases / outer-wrapper; the single production-dead
   ``_build_first_last_base_select`` site for composite; the single
   ``first_last_state`` escape hatch in ``_collect_routed_filters`` for
   target-scope. This is the machine-checked inventory PR 6 (DEV-1749) consumes.

3. **Facility unit tests** — the new renderer behaviours the migration adds:
   the ``FilterFacilities.agg_builder`` HAVING seam (slot lookup +
   ``having_full_alias`` recovery), the ``cast_column_sql`` filter-CAST policy,
   the ``paren_comparison_operands`` grouping policy, the alias-exclusive
   resolution mode with table qualification, the ``Optional`` scope fail-closed
   rule, and ``ScopeFrame.column_type``.

Everything new is referenced from inside a test body (never at module import),
so the sentinels and inventory stay collectable and fail for the right reason
while the implementation is still absent.

Refs: DEV-1763, DEV-1742 §5.1 / P-G / P-J, DEV-1749 (deletion consumer).
"""

from __future__ import annotations

import ast
import tempfile
from decimal import Decimal
from pathlib import Path

import pytest
from sqlglot import exp

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import RenderContextMissingFacilityError
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    ColumnSqlKey,
    LiteralKey,
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
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql.dialects import get_dialect
from slayer.sql.generator import SQLGenerator
from slayer.sql.naming import AliasAllocator
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.render.value_expr import (
    AliasFacilities,
    FilterFacilities,
    RenderContext,
    contains_aggregate,
    render_value_key,
)
from slayer.sql.scope import ScopeFrame
from slayer.storage.yaml_storage import YAMLStorage


# ===========================================================================
# Model graph + generation helper (dry_run, one bundle covers every shape).
# ===========================================================================
def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        default_time_dimension="created_at",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="customer_id", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="qty", type=DataType.DOUBLE),
            Column(name="disc", type=DataType.DOUBLE),
            Column(name="created_at", type=DataType.TIMESTAMP),
            Column(name="net", sql="amount - 1", type=DataType.DOUBLE),
            # A NON-trivial DERIVED TIMESTAMP: the filter-CAST policy must
            # suppress the CAST for temporal types (Codex F9 / _filter_cast_type).
            Column(name="net_ts", sql="coalesce(created_at, created_at)",
                   type=DataType.TIMESTAMP),
            # A MIXED-CASE physical column: quoting must survive on the
            # shifted-CTE WHERE path, which stringifies without a re-parse
            # (byte-parity risk register / Codex F15).
            Column(name="MixedCol", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]])],
        aggregations=[Aggregation(name="sum_sq", formula="SUM({value} * {value})")],
    )


def _customers() -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="region_id", type=DataType.DOUBLE),
            Column(name="spend", type=DataType.DOUBLE),
            Column(name="deep_pop", sql="regions.population", type=DataType.DOUBLE),
        ],
        joins=[ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]])],
    )


def _regions() -> SlayerModel:
    return SlayerModel(
        name="regions", sql_table="regions", data_source="test",
        columns=[
            Column(name="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="population", type=DataType.DOUBLE),
            Column(name="tier", type=DataType.TEXT),
        ],
    )


async def _generate(query: SlayerQuery, *, dialect: str = "postgres") -> str:
    """Render ``query`` (dry_run) against the orders → customers → regions
    bundle and return the emitted SQL. One bundle reaches every family."""
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(DatasourceConfig(name="test", type=dialect))
        await storage.save_model(_orders())
        await storage.save_model(_customers())
        await storage.save_model(_regions())
        engine = SlayerQueryEngine(storage=storage)
        response = await engine.execute(query, dry_run=True)
        assert response.sql is not None
        return response.sql


_MONTH = TimeDimension(
    dimension=ColumnRef(name="created_at"), granularity=TimeGranularity.MONTH,
)


# --------------------------------------------------------------------------- #
# Per-family production shapes (probe-verified to reach the named renderer).
# --------------------------------------------------------------------------- #
_COMPOSITE_SHAPES = {
    "arith_of_aggregates": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="amount:sum - qty:sum", name="d")],
    ),
    "scalar_over_aggregate": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="ifnull(amount:sum, 0)", name="m")],
    ),
}

_FILTER_SHAPES = {
    "host_where_scalar": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="*:count")],
        filters=["ifnull(disc, 0) > 1"],
    ),
    "host_where_derived": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="*:count")],
        filters=["net > 5"],
    ),
    "host_having_aggregate": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="qty:sum", name="q")],
        filters=["qty:sum > 5"],
    ),
    "shifted_cte_where": SlayerQuery(
        source_model="orders",
        time_dimensions=[_MONTH],
        measures=[
            ModelMeasure(formula="amount:sum", name="amt"),
            ModelMeasure(formula="time_shift(amount:sum, -1, 'month')", name="prev"),
        ],
        filters=["status = 'new'"],
    ),
    "shifted_cte_where_mixed_case": SlayerQuery(
        source_model="orders",
        time_dimensions=[_MONTH],
        measures=[
            ModelMeasure(formula="amount:sum", name="amt"),
            ModelMeasure(formula="time_shift(amount:sum, -1, 'month')", name="prev"),
        ],
        filters=["MixedCol > 5"],
    ),
}

_ALIAS_SHAPES = {
    "post_phase_filter_on_transform": SlayerQuery(
        source_model="orders",
        time_dimensions=[_MONTH],
        measures=[ModelMeasure(formula="cumsum(amount:sum)", name="cs")],
        filters=["cumsum(amount:sum) > 5"],
    ),
    "post_phase_filter_arith_transform": SlayerQuery(
        source_model="orders",
        time_dimensions=[_MONTH],
        measures=[ModelMeasure(formula="cumsum(amount:sum)", name="cs")],
        filters=["cumsum(amount:sum) + 1 > 5"],
    ),
    "window_transform_composite_input": SlayerQuery(
        source_model="orders",
        time_dimensions=[_MONTH],
        measures=[ModelMeasure(formula="cumsum(amount:sum + qty:sum)", name="cs")],
    ),
}

_OUTER_SHAPES = {
    "outer_composite_projection": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(
            formula="customers.spend:sum + amount:sum", name="mix")],
    ),
}

_TARGET_SHAPES = {
    "cross_model_routed_where": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="customers.spend:sum", name="cs")],
        filters=["customers.deep_pop > 5"],
    ),
    "cross_model_routed_having": SlayerQuery(
        source_model="orders",
        dimensions=[ColumnRef(name="status")],
        measures=[ModelMeasure(formula="customers.spend:sum", name="cs")],
        filters=["customers.spend:sum + 1 > 5"],
    ),
}


# ===========================================================================
# Layer 1 — per-family raising sentinels.
# ===========================================================================
def _boom_factory(method: str):
    def _boom(*_a, **_kw):
        raise AssertionError(
            f"{method} is still on the production render path — DEV-1763 routes "
            f"every family through render_value_key(key, ctx)"
        )
    return _boom


def _assert_present(method: str) -> None:
    assert hasattr(SQLGenerator, method), (
        f"{method} has been deleted; DEV-1763 keeps the legacy renderers "
        f"(P-J state 1) and defers deletion to PR 6 — update this test "
        f"deliberately rather than losing the guard"
    )


class TestCompositeFamilySentinel:
    """``_render_aggregate_composite_expr`` must lose its live production caller
    (``_build_base_select_for_planned``). Its only surviving reference is the
    production-dead ``_build_first_last_base_select`` site (PR 6 deletes it)."""

    _METHOD = "_render_aggregate_composite_expr"

    @pytest.mark.parametrize("shape", sorted(_COMPOSITE_SHAPES))
    async def test_composite_family_never_reached(self, shape, monkeypatch) -> None:
        _assert_present(self._METHOD)
        monkeypatch.setattr(SQLGenerator, self._METHOD, _boom_factory(self._METHOD))
        await _generate(_COMPOSITE_SHAPES[shape])


class TestFilterFamilySentinel:
    """``_render_value_key_for_filter`` must lose both production call sites:
    the host WHERE/HAVING builder and the time_shift shifted-CTE WHERE."""

    _METHOD = "_render_value_key_for_filter"

    @pytest.mark.parametrize("shape", sorted(_FILTER_SHAPES))
    async def test_filter_family_never_reached(self, shape, monkeypatch) -> None:
        _assert_present(self._METHOD)
        monkeypatch.setattr(SQLGenerator, self._METHOD, _boom_factory(self._METHOD))
        await _generate(_FILTER_SHAPES[shape])


class TestAliasFamilySentinel:
    """``_render_value_key_against_aliases`` must lose all six production sites
    (including ``_render_cross_model_transform_chain`` :6166)."""

    _METHOD = "_render_value_key_against_aliases"

    @pytest.mark.parametrize("shape", sorted(_ALIAS_SHAPES))
    async def test_alias_family_never_reached(self, shape, monkeypatch) -> None:
        _assert_present(self._METHOD)
        monkeypatch.setattr(SQLGenerator, self._METHOD, _boom_factory(self._METHOD))
        await _generate(_ALIAS_SHAPES[shape])


class TestOuterWrapperFamilySentinel:
    """``_render_filter_for_outer_wrapper`` must lose its production sites in
    ``_render_with_cross_model_plans`` / ``_render_outer_composite``."""

    _METHOD = "_render_filter_for_outer_wrapper"

    @pytest.mark.parametrize("shape", sorted(_OUTER_SHAPES))
    async def test_outer_family_never_reached(self, shape, monkeypatch) -> None:
        _assert_present(self._METHOD)
        monkeypatch.setattr(SQLGenerator, self._METHOD, _boom_factory(self._METHOD))
        await _generate(_OUTER_SHAPES[shape])


class TestTargetScopeFamilySentinel:
    """``_render_filter_value_key_in_target_scope`` must lose its production
    caller ``_collect_routed_filters`` on the live (``first_last_state is None``)
    path. Patching it to raise also proves production never takes the
    first_last escape hatch, which is the sole intentional surviving reference
    (PR 6 inventory)."""

    _METHOD = "_render_filter_value_key_in_target_scope"

    @pytest.mark.parametrize("shape", sorted(_TARGET_SHAPES))
    async def test_target_scope_family_never_reached(self, shape, monkeypatch) -> None:
        _assert_present(self._METHOD)
        monkeypatch.setattr(SQLGenerator, self._METHOD, _boom_factory(self._METHOD))
        await _generate(_TARGET_SHAPES[shape])


# ===========================================================================
# Layer 2 — the static state-1 inventory (ast, robust to self-recursion).
# ===========================================================================
_GENERATOR_SRC = (
    Path(__file__).resolve().parents[1] / "slayer" / "sql" / "generator.py"
)

# family renderer -> the EXACT external references (``self.<renderer>`` outside
# the renderer's own body) allowed after migration, as
# ``{enclosing class-method: expected count}``. A count catches a second,
# unguarded legacy call inside an allowed method that a set alone would hide
# (Codex F2).
_ALLOWED_EXTERNAL_CALLERS = {
    "_render_value_key_for_filter": {},
    "_render_value_key_against_aliases": {},
    "_render_filter_for_outer_wrapper": {},
    # Production-dead first/last base SELECT — deleted with the type in PR 6.
    "_render_aggregate_composite_expr": {"_build_first_last_base_select": 1},
    # The narrow first_last_state escape hatch (its own helper) — deleted in PR 6.
    "_render_filter_value_key_in_target_scope": {
        "_collect_routed_filters_first_last": 1,
    },
}


def _with_parents(tree: ast.Module) -> ast.Module:
    """Attach ``.parent`` links so a node can walk up to its lexical scope
    (Codex F10/F12 — parentage instead of a brittle line-span heuristic)."""
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            child.parent = node  # type: ignore[attr-defined]
    return tree


def _enclosing_class_method(node: ast.AST) -> "str | None":
    """The name of the nearest ``def`` that is a direct child of a ``ClassDef``
    (i.e. a real method, not a nested closure like ``recurse``). ``None`` if the
    reference is at module level or in a free function. Attributing a closure's
    body to its owning method is what keeps duplicate closure names (many
    ``recurse``s) from colliding (Codex F10/F11)."""
    cur = getattr(node, "parent", None)
    while cur is not None:
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if isinstance(getattr(cur, "parent", None), ast.ClassDef):
                return cur.name
        cur = getattr(cur, "parent", None)
    return None


def _self_attr_refs(tree: ast.Module, name: str) -> list[ast.Attribute]:
    """Every ``self.<name>`` attribute reference — not only ``self.<name>(...)``
    calls, so a stored/passed method reference (``callback=self._render_x``) is
    also inventoried (Codex F11)."""
    refs: list[ast.Attribute] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Attribute)
            and node.attr == name
            and isinstance(node.value, ast.Name)
            and node.value.id == "self"
        ):
            refs.append(node)
    return refs


class TestState1Inventory:
    """The machine-checked inventory PR 6 consumes: every legacy family
    renderer is production-unreferenced except for the documented dead-code /
    escape-hatch sites. FAILS today (the live call sites still reference the
    renderers); PASSES once all five families migrate."""

    @pytest.fixture(scope="class")
    def tree(self) -> ast.Module:
        return _with_parents(ast.parse(_GENERATOR_SRC.read_text()))

    @pytest.mark.parametrize("renderer", sorted(_ALLOWED_EXTERNAL_CALLERS))
    def test_external_references_are_exactly_the_allowed_set(
        self, renderer: str, tree: ast.Module,
    ) -> None:
        own_methods = {
            n.name for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        assert renderer in own_methods, f"{renderer} vanished from generator.py"

        external: dict[str, int] = {}
        for ref in _self_attr_refs(tree, renderer):
            enclosing = _enclosing_class_method(ref)
            if enclosing == renderer:
                continue  # self-recursion inside the renderer's own body
            key = enclosing if enclosing is not None else f"<line {ref.lineno}>"
            external[key] = external.get(key, 0) + 1

        assert external == _ALLOWED_EXTERNAL_CALLERS[renderer], (
            f"{renderer} external references {external} != "
            f"allowed {_ALLOWED_EXTERNAL_CALLERS[renderer]}"
        )


# ===========================================================================
# Layer 3 — facility unit tests (the new renderer behaviours).
# ===========================================================================
def _scope(dialect: str = "postgres") -> ScopeFrame:
    host, cust, reg = _orders(), _customers(), _regions()
    alloc = AliasAllocator()
    bundle = ResolvedSourceBundle(
        source_model=host, referenced_models=[host, cust, reg],
    )
    return ScopeFrame(
        scope_id=alloc.next_scope_id(host.name),
        root_model=host, root_relation=host.name,
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
            ("column", ColumnKey(leaf="amount")),
            ("derived", ColumnSqlKey(model="orders", column_name="net")),
            ("time_trunc", TimeTruncKey(
                column=ColumnKey(leaf="created_at"), granularity="month")),
            ("aggregate", AggregateKey(source=ColumnKey(leaf="amount"), agg="sum")),
            ("nested_in_arithmetic", ArithmeticKey(
                op="+",
                operands=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(1))),
            )),
        ],
    )
    def test_scope_needing_key_raises_when_scope_absent(self, label, key) -> None:
        ctx = RenderContext(scope=None, dialect=get_dialect("postgres"))
        with pytest.raises(RenderContextMissingFacilityError):
            render_value_key(key, ctx)


class TestScopeFrameColumnType:
    """``ScopeFrame.column_type(ref)`` gives the renderer the declared type for
    the filter-CAST policy, using the same model lookup as resolution."""

    def test_local_column_type(self) -> None:
        assert _scope().column_type(ColumnKey(leaf="amount")) is DataType.DOUBLE

    def test_derived_column_type(self) -> None:
        key = ColumnSqlKey(model="orders", column_name="net")
        assert _scope().column_type(key) is DataType.DOUBLE

    def test_temporal_column_type(self) -> None:
        assert _scope().column_type(ColumnKey(leaf="created_at")) is DataType.TIMESTAMP

    def test_joined_column_type_uses_the_owning_model(self) -> None:
        key = ColumnSqlKey(
            path=("customers",), model="customers", column_name="spend",
        )
        assert _scope().column_type(key) is DataType.DOUBLE

    def test_unknown_column_type_is_none(self) -> None:
        assert _scope().column_type(ColumnKey(leaf="does_not_exist")) is None

    def test_unknown_derived_column_type_is_none(self) -> None:
        key = ColumnSqlKey(model="orders", column_name="does_not_exist")
        assert _scope().column_type(key) is None


class TestFilterCastPolicyMovedToRenderPackage:
    """``_filter_cast_type`` + ``_wrap_cast_for_type`` move into the render
    package so the CAST policy is renderer-visible (the generator keeps thin
    delegating aliases). ``_filter_cast_type`` suppresses the CAST for temporal
    types only."""

    def test_functions_importable_from_render_package(self) -> None:
        from slayer.sql.render.value_expr import (  # noqa: F401
            _filter_cast_type,
            _wrap_cast_for_type,
        )

    def test_temporal_types_suppress_the_cast(self) -> None:
        from slayer.sql.render.value_expr import _filter_cast_type

        assert _filter_cast_type(DataType.DATE) is None
        assert _filter_cast_type(DataType.TIMESTAMP) is None

    def test_non_temporal_type_passes_through(self) -> None:
        from slayer.sql.render.value_expr import _filter_cast_type

        assert _filter_cast_type(DataType.DOUBLE) is DataType.DOUBLE


class TestFacilityFieldStructure:
    """The migration's facility-shape contract, asserted structurally so a flag
    that Pydantic silently ignores (extra kwargs) cannot make a behavioural test
    pass for the wrong reason (Codex F5)."""

    def test_filter_facilities_fields(self) -> None:
        fields = FilterFacilities.model_fields
        assert "agg_builder" in fields
        assert "cast_column_sql" in fields
        assert "paren_comparison_operands" in fields
        # The obsolete dead-type field is gone (PR 6 deletes the type).
        assert "first_last_state" not in fields
        # The paren policy defaults to the legacy "more grouping" behaviour.
        assert FilterFacilities().paren_comparison_operands is True

    def test_alias_facilities_has_table_qualifier_field(self) -> None:
        assert "table_by_slot_id" in AliasFacilities.model_fields
        assert AliasFacilities().table_by_slot_id == {}


_NET = ColumnSqlKey(model="orders", column_name="net")          # DOUBLE, derived
_NET_TS = ColumnSqlKey(model="orders", column_name="net_ts")    # TIMESTAMP, derived


class TestFilterCastColumnSql:
    """``FilterFacilities.cast_column_sql`` applies the filter-CAST policy to a
    derived ``ColumnSqlKey`` leaf (True for the filter family; False for
    target-scope, which never casts). Asserted on the AST node, not a substring,
    so the wrong expression / a double CAST cannot slip through (Codex F9)."""

    def _out(self, key, **facilities):
        ctx = RenderContext(
            scope=_scope(), dialect=get_dialect("postgres"),
            filters=FilterFacilities(**facilities),
        )
        return render_value_key(key, ctx)

    def test_cast_wraps_a_non_temporal_derived_leaf(self) -> None:
        out = self._out(_NET, cast_column_sql=True)
        assert isinstance(out, exp.Cast), _emit(out)
        # It casts the derived expression itself, not something else.
        assert "amount" in _emit(out).lower(), _emit(out)

    def test_temporal_derived_leaf_is_not_cast(self) -> None:
        out = self._out(_NET_TS, cast_column_sql=True)
        assert not isinstance(out, exp.Cast), _emit(out)

    def test_no_cast_when_flag_false(self) -> None:
        out = self._out(_NET, cast_column_sql=False)
        assert not isinstance(out, exp.Cast), _emit(out)

    def test_no_cast_without_filter_facility(self) -> None:
        ctx = RenderContext(scope=_scope(), dialect=get_dialect("postgres"))
        out = render_value_key(_NET, ctx)
        assert not isinstance(out, exp.Cast), _emit(out)


class TestAggregateBranchPrecedence:
    """The ``AggregateKey`` branch resolves in one fixed order:
    ``ctx.aliases`` (POST-phase, referenced by materialised alias) →
    ``ctx.filters.agg_builder`` (HAVING) → ``ctx.composites.agg_builder``
    (AGGREGATE composite) → the built-in simple/distinct fallback → raise
    (Codex F6)."""

    def _key(self) -> AggregateKey:
        return AggregateKey(source=ColumnKey(leaf="amount"), agg="sum")

    def test_aliases_win_over_filter_builder(self) -> None:
        from slayer.sql.render.value_expr import CompositeFacilities

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
        out = render_value_key(key, ctx)
        assert out.name == "orders.rev", _emit(out)
        assert calls == [], "filter builder must not run when aliases resolve it"

    def test_filter_builder_wins_over_composite_builder(self) -> None:
        from slayer.sql.render.value_expr import CompositeFacilities

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
        assert _emit(render_value_key(key, ctx)) == "FILTER"

    def test_composite_builder_used_when_no_filter_builder(self) -> None:
        from slayer.sql.render.value_expr import CompositeFacilities

        key = self._key()
        ctx = RenderContext(
            scope=_scope(), dialect=get_dialect("postgres"),
            composites=CompositeFacilities(agg_builder=lambda k: exp.column("COMPOSITE")),
        )
        assert _emit(render_value_key(key, ctx)) == "COMPOSITE"

    def test_builtin_fallback_when_no_builder(self) -> None:
        from slayer.sql.render.value_expr import CompositeFacilities

        key = self._key()
        ctx = RenderContext(
            scope=_scope(), dialect=get_dialect("postgres"),
            composites=CompositeFacilities(),
        )
        assert _emit(render_value_key(key, ctx)) == "SUM(orders.amount)"


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
                        ColumnKey(leaf="amount"),
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
        assert _emit(render_value_key(self._cmp_key(), ctx)) == \
            "(orders.amount + 1) > 5"

    def test_flag_off_uses_minimal_grouping(self) -> None:
        ctx = RenderContext(
            scope=_scope(), dialect=get_dialect("postgres"),
            filters=FilterFacilities(paren_comparison_operands=False),
        )
        assert _emit(render_value_key(self._cmp_key(), ctx)) == \
            "orders.amount + 1 > 5"

    def test_no_filter_facility_uses_minimal_grouping(self) -> None:
        ctx = RenderContext(scope=_scope(), dialect=get_dialect("postgres"))
        assert _emit(render_value_key(self._cmp_key(), ctx)) == \
            "orders.amount + 1 > 5"


class TestFilterAggBuilderSeam:
    """``FilterFacilities.agg_builder`` is the HAVING seam. The renderer does
    the ``slot_by_key`` lookup and the ``having_full_alias`` recovery itself
    (so those fields are genuinely consumed), then hands ``(key, slot,
    having_full_alias)`` to the generator's builder."""

    def _slot(self, key):
        from slayer.core.keys import Phase
        from slayer.engine.planned import ValueSlot

        return ValueSlot(
            id="s1", key=key, declared_name="q", phase=Phase.AGGREGATE,
            type=DataType.DOUBLE,
        )

    def _key(self) -> AggregateKey:
        return AggregateKey(source=ColumnKey(leaf="qty"), agg="sum")

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
        render_value_key(key, ctx)
        assert recorded["slot"] is slot
        assert recorded["having_full_alias"] == "orders.q"

    def test_placeholder_alias_when_slot_has_no_alias_entry(self) -> None:
        key = self._key()
        slot = self._slot(key)
        ctx, recorded = self._ctx(
            slot_by_key={key: slot},
            aliases_by_slot_id={},
        )
        render_value_key(key, ctx)
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
        render_value_key(key, ctx)
        assert recorded["having_full_alias"] == "__having_ref__"

    def test_no_slot_passes_none_and_placeholder(self) -> None:
        key = self._key()
        ctx, recorded = self._ctx(slot_by_key={}, aliases_by_slot_id={})
        render_value_key(key, ctx)
        assert recorded["slot"] is None
        assert recorded["having_full_alias"] == "__having_ref__"

    def test_builder_output_is_returned(self) -> None:
        key = self._key()
        ctx, _ = self._ctx(slot_by_key={}, aliases_by_slot_id={})
        assert _emit(render_value_key(key, ctx)) == "SENTINEL"


_SLOTTED_KINDS = {
    "column": ColumnKey(leaf="amount"),
    "derived": ColumnSqlKey(model="orders", column_name="net"),
    "time_trunc": TimeTruncKey(
        column=ColumnKey(leaf="created_at"), granularity="month"),
    "aggregate": AggregateKey(source=ColumnKey(leaf="amount"), agg="sum"),
    "transform": TransformKey(
        op="cumsum",
        input=AggregateKey(source=ColumnKey(leaf="amount"), agg="sum"),
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
        out = render_value_key(key, self._ctx(key))
        assert isinstance(out, exp.Column), f"{label}: {type(out).__name__}"
        assert out.name == "orders.rev", f"{label}: {_emit(out)}"

    def test_bare_alias_when_no_table_qualifier(self) -> None:
        key = ColumnKey(leaf="amount")
        out = render_value_key(key, self._ctx(key))
        assert _emit(out) == '"orders.rev"', _emit(out)

    def test_base_qualified_alias(self) -> None:
        key = ColumnKey(leaf="amount")
        out = render_value_key(key, self._ctx(key, table="_base"))
        assert _emit(out) == '_base."orders.rev"', _emit(out)

    def test_cross_model_cte_qualifier(self) -> None:
        key = AggregateKey(source=ColumnKey(leaf="amount"), agg="sum")
        out = render_value_key(key, self._ctx(key, table="_cm_x"))
        assert _emit(out) == '_cm_x."orders.rev"', _emit(out)

    @pytest.mark.parametrize("label", sorted(_SLOTTED_KINDS))
    def test_miss_raises(self, label) -> None:
        key = _SLOTTED_KINDS[label]
        ctx = self._ctx(key, present=False)
        with pytest.raises(RenderContextMissingFacilityError):
            render_value_key(key, ctx)

    def test_miss_does_not_fall_back_to_scope(self) -> None:
        """A miss is a promotion bug, not a cue to rebuild from source — even
        when a real scope is present, alias mode raises rather than resolving
        the key against the scope root."""
        key = ColumnKey(leaf="amount")
        ctx = self._ctx(key, present=False, scope=_scope())
        with pytest.raises(RenderContextMissingFacilityError):
            render_value_key(key, ctx)


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
                AggregateKey(source=ColumnKey(leaf="amount"), agg="sum"),
                AggregateKey(source=ColumnKey(leaf="qty"), agg="sum"),
            ),
        )
        assert contains_aggregate(key) is True

    def test_scalar_over_aggregate_contains_aggregate(self) -> None:
        from slayer.core.keys import ScalarCallKey

        key = ScalarCallKey(
            name="ifnull",
            args=(
                AggregateKey(source=ColumnKey(leaf="amount"), agg="sum"),
                LiteralKey(value=Decimal(0)),
            ),
        )
        assert contains_aggregate(key) is True

    def test_pure_arithmetic_without_aggregate_is_not_aggregate(self) -> None:
        key = ArithmeticKey(
            op="+",
            operands=(ColumnKey(leaf="amount"), LiteralKey(value=Decimal(1))),
        )
        assert contains_aggregate(key) is False
