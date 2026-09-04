"""DEV-1827 — the total ValueKey traversal protocol (children / map_children).

Generic walkers/rewriters route through ``children()`` / ``map_children()``,
so a new union member flows through them by construction; a kind without the
protocol fails loudly (``NotImplementedError``), never silently as a leaf.
Kind-dispatch visitors (``_iter_slot_deps``, the renderer) raise on ANY
unknown kind. New names are imported at module scope so the whole module
reports one clear collection error while they do not exist yet.
"""
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import get_args

import pytest

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.keys import (
    KIND_POLICY,
    VALUE_KEY_TYPES,
    AggregateKey,
    ArithmeticKey,
    BetweenKey,
    ColumnKey,
    ColumnSqlKey,
    InKey,
    KindPolicy,
    LiteralKey,
    Phase,
    ScalarCallKey,
    SqlExprKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    _FrozenKey,
    reroot_value_key,
    substitute_value_keys,
)
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, SlayerQuery, TimeDimension
from slayer.engine.aggregate_input_paths import compute_aggregate_input_join_paths
from slayer.engine.binding import walk_value_keys
from slayer.engine.planning import (
    _SLOTTABLE_KIND,
    _iter_slot_deps,
    lower_sugar_transforms,
    rewrite_rank_partition_keys,
)
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import get_dialect
from slayer.sql.generator import SQLGenerator
from slayer.sql.render.value_expr import (
    RenderContext,
    contains_aggregate,
    render_value_key,
)

from tests._engine_helpers import _engine_generate

CITY = ColumnKey(path=(), leaf="city")
REGION = ColumnKey(path=(), leaf="region")
AMOUNT = ColumnKey(path=(), leaf="amount")
TS = ColumnKey(path=(), leaf="created_at")
JOINED = ColumnKey(path=("customers",), leaf="balance")
TT = TimeTruncKey(column=TS, granularity="month")
AGG = AggregateKey(source=AMOUNT, agg="sum")
FILT = SqlExprKey(canonical_sql="status = 'ok'")
CHANGE_TR = TransformKey(op="change", input=AGG, time_key=TT)
RANK_TR = TransformKey(op="rank", input=AGG, partition_keys=frozenset({CITY}))

AGG_FULL = AggregateKey(
    source=AMOUNT,
    agg="last",
    args=(TS, Decimal("2")),
    kwargs=(("p", Decimal("0.5")), ("weight", CITY)),
    column_filter_key=FILT,
    grain="host",
    partition_keys=frozenset({REGION}),
)
TR_FULL = TransformKey(
    op="cumsum",
    input=AGG,
    args=(Decimal("1"),),
    kwargs=(("k", "v"),),
    partition_keys=frozenset({CITY}),
    time_key=TT,
)
AR = ArithmeticKey(op="+", operands=(AMOUNT, LiteralKey(value=Decimal("1"))))
SC = ScalarCallKey(name="coalesce", args=(CITY, "fallback"))
BT = BetweenKey(column=TS, low=LiteralKey(value="a"), high=LiteralKey(value="b"))
IK = InKey(
    column=CITY,
    values=(LiteralKey(value="gold"), LiteralKey(value="silver")),
    negated=True,
)

SAMPLES = {
    ColumnKey: CITY,
    ColumnSqlKey: ColumnSqlKey(path=("a",), model="m", column_name="c"),
    TimeTruncKey: TT,
    StarKey: StarKey(path=("a",)),
    LiteralKey: LiteralKey(value="x"),
    AggregateKey: AGG_FULL,
    TransformKey: TR_FULL,
    ArithmeticKey: AR,
    ScalarCallKey: SC,
    BetweenKey: BT,
    InKey: IK,
}
LEAF_KINDS = (ColumnKey, ColumnSqlKey, StarKey, LiteralKey, SqlExprKey)


class DummyKey(_FrozenKey):
    """A protocol-implementing kind outside the union."""

    child: ValueKey

    @property
    def phase(self) -> Phase:
        return Phase.ROW

    def children(self):
        return (self.child,)

    def map_children(self, fn):
        new = fn(self.child)
        if new is self.child:
            return self
        return self.model_copy(update={"child": new})


class DummyOpaqueKey(_FrozenKey):
    """A kind WITHOUT protocol overrides — every generic visitor must raise."""

    marker: str = "opaque"

    @property
    def phase(self) -> Phase:
        return Phase.ROW


def _record_map(key):
    """Run ``map_children`` with a pass-through fn, returning (out, seen)."""
    seen = []

    def fn(child):
        seen.append(child)
        return child

    return key.map_children(fn), seen


# ---------------------------------------------------------------------------
# Protocol totality (tasks 1.1)
# ---------------------------------------------------------------------------
class TestProtocolTotality:
    def test_every_union_member_has_a_sample(self) -> None:
        missing = set(get_args(ValueKey)) - set(SAMPLES)
        assert not missing, (
            f"ValueKey grew {sorted(m.__name__ for m in missing)}; add a sample "
            f"here and children/map_children overrides (DEV-1827 totality)."
        )

    def test_every_member_overrides_both_methods(self) -> None:
        for member in get_args(ValueKey) + (SqlExprKey,):
            assert member.children is not _FrozenKey.children, member.__name__
            assert member.map_children is not _FrozenKey.map_children, (
                member.__name__
            )

    def test_map_children_identity_returns_self(self) -> None:
        for sample in (*SAMPLES.values(), FILT):
            out, _ = _record_map(sample)
            assert out is sample

    def test_base_default_children_raises_naming_the_protocol(self) -> None:
        key = DummyOpaqueKey()
        with pytest.raises(NotImplementedError) as ei:
            key.children()
        assert "children" in str(ei.value)
        assert "DummyOpaqueKey" in str(ei.value)

    def test_base_default_map_children_raises_naming_the_protocol(self) -> None:
        key = DummyOpaqueKey()
        with pytest.raises(NotImplementedError) as ei:
            key.map_children(lambda c: c)
        assert "map_children" in str(ei.value)


# ---------------------------------------------------------------------------
# children() per kind (design decision 1)
# ---------------------------------------------------------------------------
class TestChildrenPerKind:
    def test_leaves_have_no_children(self) -> None:
        for kind in LEAF_KINDS:
            sample = FILT if kind is SqlExprKey else SAMPLES[kind]
            assert sample.children() == ()

    def test_time_trunc_child_is_the_wrapped_column(self) -> None:
        assert TT.children() == (TS,)
        assert TT.children()[0] is TS

    def test_aggregate_children_field_order(self) -> None:
        # source, key-valued args, key-valued kwarg values, partition members;
        # scalars and column_filter_key are NOT children.
        assert AGG_FULL.children() == (AMOUNT, TS, CITY, REGION)

    def test_aggregate_without_partition_keys(self) -> None:
        assert AGG.children() == (AMOUNT,)

    def test_transform_children_field_order(self) -> None:
        assert TR_FULL.children() == (AGG, CITY, TT)

    def test_transform_without_time_key_or_partitions(self) -> None:
        assert TransformKey(op="cumsum", input=AGG).children() == (AGG,)

    def test_arithmetic_children_are_the_operands(self) -> None:
        assert AR.children() == AR.operands

    def test_scalar_call_children_are_key_valued_args_only(self) -> None:
        assert SC.children() == (CITY,)

    def test_between_children(self) -> None:
        assert BT.children() == (BT.column, BT.low, BT.high)

    def test_in_children_are_column_then_values(self) -> None:
        assert IK.children() == (CITY,) + IK.values


# ---------------------------------------------------------------------------
# map_children contract (design decision 2, task 1.2)
# ---------------------------------------------------------------------------
class TestMapChildrenContract:
    def test_fn_calls_match_children_positionally_by_identity(self) -> None:
        for sample in SAMPLES.values():
            _, seen = _record_map(sample)
            assert [id(s) for s in seen] == [id(c) for c in sample.children()]

    def test_equal_but_distinct_replacements_are_kept(self) -> None:
        # Identity (`is`) is the change detector, not equality: an equal copy
        # must trigger a rebuild and land in the corresponding position.
        for sample in SAMPLES.values():
            if not sample.children():
                continue
            clones = {id(c): c.model_copy() for c in sample.children()}
            out = sample.map_children(lambda c: clones[id(c)])
            assert out is not sample
            assert out == sample
            assert [id(c) for c in out.children()] == [
                id(clones[id(c)]) for c in sample.children()
            ]

    def test_replacements_land_in_their_fields(self) -> None:
        clones = {id(c): c.model_copy() for c in AGG_FULL.children()}
        out = AGG_FULL.map_children(lambda c: clones[id(c)])
        assert out.source is clones[id(AMOUNT)]
        assert out.args[0] is clones[id(TS)]
        assert out.args[1] == Decimal("2")
        assert [k for k, _ in out.kwargs] == ["p", "weight"]
        assert dict(out.kwargs)["weight"] is clones[id(CITY)]
        assert next(iter(out.partition_keys)) is clones[id(REGION)]

    def test_between_replacements_cannot_swap_fields(self) -> None:
        clones = {id(c): c.model_copy() for c in BT.children()}
        out = BT.map_children(lambda c: clones[id(c)])
        assert out.column is clones[id(BT.column)]
        assert out.low is clones[id(BT.low)]
        assert out.high is clones[id(BT.high)]

    def test_multi_member_partition_set_coherence(self) -> None:
        # Two iterations of the SAME frozenset instance agree, so recording
        # order matches children() order even with several members.
        tr = TransformKey(
            op="cumsum", input=AGG, partition_keys=frozenset({CITY, REGION}),
        )
        _, seen = _record_map(tr)
        assert [id(s) for s in seen] == [id(c) for c in tr.children()]

    def test_map_children_is_shallow(self) -> None:
        tree = ArithmeticKey(op="+", operands=(AGG, CITY))
        _, seen = _record_map(tree)
        assert [id(s) for s in seen] == [id(AGG), id(CITY)]
        assert not any(s is AMOUNT for s in seen)

    def test_scalars_and_column_filter_key_never_reach_fn(self) -> None:
        _, seen = _record_map(AGG_FULL)
        assert all(isinstance(s, _FrozenKey) for s in seen)
        assert not any(s is FILT for s in seen)

    def test_column_filter_key_survives_a_rebuild(self) -> None:
        out = AGG_FULL.map_children(lambda c: c.model_copy())
        assert out.column_filter_key is FILT
        assert out.grain == "host"

    def test_aggregate_none_partition_keys_stays_none(self) -> None:
        out = AGG.map_children(lambda c: c.model_copy())
        assert out is not AGG
        assert out.partition_keys is None

    def test_transform_empty_partition_keys_stays_empty(self) -> None:
        tr = TransformKey(op="cumsum", input=AGG)
        out = tr.map_children(lambda c: c.model_copy())
        assert out.partition_keys == frozenset()
        assert out.time_key is None


# ---------------------------------------------------------------------------
# Kind-policy registry (task 1.3)
# ---------------------------------------------------------------------------
class TestKindPolicyRegistry:
    def test_registry_covers_exactly_the_union(self) -> None:
        assert set(KIND_POLICY) == set(get_args(ValueKey))

    def test_slottable_membership(self) -> None:
        assert {k for k, p in KIND_POLICY.items() if p.slottable} == {
            ColumnKey, ColumnSqlKey, AggregateKey, TransformKey, TimeTruncKey,
        }

    def test_slot_composite_membership(self) -> None:
        assert {k for k, p in KIND_POLICY.items() if p.slot_composite} == {
            ArithmeticKey, ScalarCallKey,
        }

    def test_materialised_order_membership(self) -> None:
        assert {k for k, p in KIND_POLICY.items() if p.materialised_order} == {
            AggregateKey, ArithmeticKey, ScalarCallKey, TransformKey,
        }

    def test_kind_policy_is_frozen(self) -> None:
        assert KindPolicy.model_config.get("frozen") is True

    def test_slottable_kind_agrees_with_the_registry(self) -> None:
        assert set(_SLOTTABLE_KIND) == {
            k for k, p in KIND_POLICY.items() if p.slottable
        }

    def test_value_key_types_is_derived_from_the_union(self) -> None:
        assert tuple(VALUE_KEY_TYPES) == get_args(ValueKey)


# ---------------------------------------------------------------------------
# A protocol-implementing kind flows through every generic visitor (task 5.2)
# ---------------------------------------------------------------------------
class TestDummyFlowsThroughGenericVisitors:
    def test_walk_value_keys_descends_via_children(self) -> None:
        dummy = DummyKey(child=CITY)
        assert list(walk_value_keys(dummy)) == [dummy, CITY]

    def test_contains_aggregate_sees_through_the_dummy(self) -> None:
        assert contains_aggregate(DummyKey(child=AGG)) is True
        assert contains_aggregate(DummyKey(child=CITY)) is False

    def test_lower_sugar_reaches_a_nested_change(self) -> None:
        out = lower_sugar_transforms(DummyKey(child=CHANGE_TR))
        assert isinstance(out.child, ArithmeticKey)
        assert out.child.op == "-"

    def test_rank_rewrite_reaches_a_nested_rank(self) -> None:
        out = rewrite_rank_partition_keys(
            key=DummyKey(child=RANK_TR),
            rewrite_fn=lambda k: frozenset({REGION}),
        )
        assert out.child.partition_keys == frozenset({REGION})

    def test_reroot_reaches_the_dummy_child(self) -> None:
        out = reroot_value_key(
            DummyKey(child=JOINED), target_path=("customers",),
        )
        assert out.child == ColumnKey(path=(), leaf="balance")

    def test_substitute_reaches_the_dummy_child(self) -> None:
        out = substitute_value_keys(
            key=DummyKey(child=CITY), mapping={CITY: REGION},
        )
        assert out.child == REGION

    def test_having_walk_finds_the_local_column_inside(self) -> None:
        dummy = DummyKey(child=CITY)
        assert SQLGenerator._direct_local_column_keys(dummy) == [CITY]


class TestOpaqueDummyFailsClosed:
    def test_walk_value_keys_raises(self) -> None:
        walk = walk_value_keys(DummyOpaqueKey())
        with pytest.raises(NotImplementedError):
            list(walk)

    def test_contains_aggregate_raises(self) -> None:
        key = DummyOpaqueKey()
        with pytest.raises(NotImplementedError):
            contains_aggregate(key)

    def test_lower_sugar_raises(self) -> None:
        key = DummyOpaqueKey()
        with pytest.raises(NotImplementedError):
            lower_sugar_transforms(key)

    def test_rank_rewrite_raises(self) -> None:
        key = DummyOpaqueKey()
        with pytest.raises(NotImplementedError):
            rewrite_rank_partition_keys(key=key, rewrite_fn=lambda k: frozenset())

    def test_reroot_raises(self) -> None:
        key = DummyOpaqueKey()
        with pytest.raises(NotImplementedError):
            reroot_value_key(key, target_path=("customers",))

    def test_substitute_raises(self) -> None:
        key = DummyOpaqueKey()
        with pytest.raises(NotImplementedError):
            substitute_value_keys(key=key, mapping={CITY: REGION})

    def test_having_walk_raises(self) -> None:
        key = DummyOpaqueKey()
        with pytest.raises(NotImplementedError):
            SQLGenerator._direct_local_column_keys(key)


# ---------------------------------------------------------------------------
# Kind-dispatch visitors raise on BOTH dummy kinds (design decision 6)
# ---------------------------------------------------------------------------
class TestKindDispatchVisitorsRaise:
    def test_iter_slot_deps_raises_on_both(self) -> None:
        for dummy in (DummyKey(child=CITY), DummyOpaqueKey()):
            with pytest.raises(TypeError):
                list(_iter_slot_deps(dummy))

    def test_iter_slot_deps_still_skips_star_and_literal(self) -> None:
        assert list(_iter_slot_deps(StarKey())) == []
        assert list(_iter_slot_deps(LiteralKey(value="x"))) == []

    def test_render_raises_on_both_at_top_level(self) -> None:
        ctx = RenderContext(dialect=get_dialect("postgres"))
        for dummy in (DummyKey(child=CITY), DummyOpaqueKey()):
            with pytest.raises(NotImplementedError):
                render_value_key(key=dummy, ctx=ctx)

    def test_scalar_call_arg_raises_as_a_key_not_a_literal(self) -> None:
        ctx = RenderContext(dialect=get_dialect("postgres"))
        key = ScalarCallKey.model_construct(
            name="coalesce", args=(DummyOpaqueKey(), "x"),
        )
        with pytest.raises(NotImplementedError) as ei:
            render_value_key(key=key, ctx=ctx)
        assert "literal" not in str(ei.value)

    def test_iif_branch_raises_as_a_key_not_a_literal(self) -> None:
        ctx = RenderContext(dialect=get_dialect("postgres"))
        key = ScalarCallKey.model_construct(
            name="iif",
            args=(Decimal("1"), DummyOpaqueKey(), Decimal("0")),
        )
        with pytest.raises(NotImplementedError) as ei:
            render_value_key(key=key, ctx=ctx)
        assert "literal" not in str(ei.value)

    def test_scalars_still_render_as_literals(self) -> None:
        ctx = RenderContext(dialect=get_dialect("postgres"))
        rendered = render_value_key(
            key=ScalarCallKey(name="coalesce", args=(Decimal("1"), "x")),
            ctx=ctx,
        )
        sql = rendered.sql(dialect="postgres")
        assert "1" in sql
        assert "'x'" in sql


# ---------------------------------------------------------------------------
# Aux-slot collection routes through children() (task 4.1)
# ---------------------------------------------------------------------------
def _aux_slot_ids(tree, *, slot_id_by_key=None):
    fp = SimpleNamespace(
        phase=Phase.AGGREGATE,
        expression=SimpleNamespace(value_key=tree),
        id="f1",
    )
    planned = SimpleNamespace(
        transform_layers=[], filters_by_phase=[fp], order=[],
    )
    return SQLGenerator._collect_base_aux_slot_ids(
        planned_query=planned,
        slot_id_by_key=slot_id_by_key or {AGG: "s1"},
        slots_by_id={},
    )


class TestCollectBaseAuxSlotIds:
    def test_dummy_wrapped_aggregate_is_collected(self) -> None:
        assert _aux_slot_ids(DummyKey(child=AGG)) == ["s1"]

    def test_opaque_dummy_raises(self) -> None:
        key = DummyOpaqueKey()
        with pytest.raises(NotImplementedError):
            _aux_slot_ids(key)

    def test_time_trunc_is_the_slot_not_its_column(self) -> None:
        # B1: the widened traversal must not surface the wrapped raw column
        # as an extra base projection.
        tree = ArithmeticKey(op=">", operands=(TT, LiteralKey(value="a")))
        ids = _aux_slot_ids(tree, slot_id_by_key={TT: "t1", TS: "c1"})
        assert ids == ["t1"]


# ---------------------------------------------------------------------------
# lower_sugar_transforms: the InKey latent-bug fix + widened reach (task 3.2)
# ---------------------------------------------------------------------------
class TestLowerSugarTraversal:
    def test_change_under_in_inside_scalar_call_is_lowered(self) -> None:
        # The pre-DEV-1827 recurse tuple omitted InKey, so this change
        # silently escaped lowering (the latent bug this change fixes).
        key = ScalarCallKey(
            name="coalesce",
            args=(
                InKey(column=CHANGE_TR, values=(LiteralKey(value="gold"),)),
                "x",
            ),
        )
        out = lower_sugar_transforms(key)
        lowered = out.args[0].column
        assert isinstance(lowered, ArithmeticKey)
        assert lowered.op == "-"
        assert lowered.operands[0] is AGG

    def test_change_directly_under_in_still_lowered(self) -> None:
        out = lower_sugar_transforms(
            InKey(column=CHANGE_TR, values=(LiteralKey(value="gold"),)),
        )
        assert isinstance(out.column, ArithmeticKey)

    def test_change_inside_aggregate_source_is_lowered(self) -> None:
        key = AggregateKey(
            source=ArithmeticKey(op="+", operands=(AMOUNT, CHANGE_TR)),
            agg="sum",
        )
        out = lower_sugar_transforms(key)
        assert isinstance(out.source.operands[1], ArithmeticKey)

    def test_change_in_transform_partition_keys_is_lowered(self) -> None:
        key = TransformKey(
            op="cumsum", input=AGG, partition_keys=frozenset({CHANGE_TR}),
        )
        out = lower_sugar_transforms(key)
        assert all(isinstance(p, ArithmeticKey) for p in out.partition_keys)

    def test_change_in_transform_time_key_is_lowered(self) -> None:
        key = TransformKey(op="cumsum", input=AGG, time_key=CHANGE_TR)
        out = lower_sugar_transforms(key)
        assert isinstance(out.time_key, ArithmeticKey)

    def test_identity_preserved_when_nothing_lowers(self) -> None:
        tree = ArithmeticKey(op="+", operands=(AGG, CITY))
        assert lower_sugar_transforms(tree) is tree

    def test_sugar_free_nested_trees_are_identity(self) -> None:
        # The widened reach is a no-op absent sugar: trees with a NON-sugar
        # transform in the newly-reached positions come back by identity.
        for tree in (
            AggregateKey(
                source=ArithmeticKey(op="+", operands=(AMOUNT, RANK_TR)),
                agg="sum",
            ),
            TransformKey(
                op="cumsum", input=AGG, partition_keys=frozenset({RANK_TR}),
            ),
            TransformKey(op="cumsum", input=AGG, time_key=RANK_TR),
        ):
            assert lower_sugar_transforms(tree) is tree


# ---------------------------------------------------------------------------
# rewrite_rank_partition_keys contract (task 3.3)
# ---------------------------------------------------------------------------
class TestRankRewriteContract:
    def test_rewrite_fn_receives_the_pre_rebuild_node(self) -> None:
        inner = TransformKey(
            op="rank", input=AGG, partition_keys=frozenset({CITY}),
        )
        outer = TransformKey(
            op="rank", input=inner, partition_keys=frozenset({CITY}),
        )
        seen = []

        def fn(k):
            seen.append(k)
            return frozenset({REGION})

        out = rewrite_rank_partition_keys(key=outer, rewrite_fn=fn)
        assert seen[0] is inner
        assert seen[1] is outer
        assert seen[1].input is inner
        assert out.partition_keys == frozenset({REGION})
        assert out.input.partition_keys == frozenset({REGION})

    def test_aggregate_partition_keys_still_rewritten(self) -> None:
        agg = AggregateKey(
            source=AMOUNT, agg="sum", partition_keys=frozenset({CITY}),
        )
        out = rewrite_rank_partition_keys(
            key=agg, rewrite_fn=lambda k: frozenset({REGION}),
        )
        assert out.partition_keys == frozenset({REGION})

    def test_identity_preserved_without_rank_keys(self) -> None:
        tree = ArithmeticKey(op="+", operands=(AGG, CITY))
        out = rewrite_rank_partition_keys(
            key=tree, rewrite_fn=lambda k: frozenset({REGION}),
        )
        assert out is tree


# ---------------------------------------------------------------------------
# substitute_value_keys atomicity under the rebased traversal (task 2.2)
# ---------------------------------------------------------------------------
class TestSubstituteAtomicity:
    def test_replacement_value_is_not_re_substituted(self) -> None:
        replacement = ArithmeticKey(
            op="+", operands=(CITY, LiteralKey(value=Decimal("1"))),
        )
        out = substitute_value_keys(
            key=ArithmeticKey(op="+", operands=(CITY, AMOUNT)),
            mapping={CITY: replacement},
        )
        assert out.operands[0] is replacement
        assert out.operands[0].operands[0] == CITY


# ---------------------------------------------------------------------------
# B1 — walk_value_keys reaches TimeTruncKey.column without side effects
# ---------------------------------------------------------------------------
class TestTimeTruncWidening:
    def test_walk_yields_the_wrapped_column(self) -> None:
        assert list(walk_value_keys(TT)) == [TT, TS]

    def test_filter_phase_stays_row(self) -> None:
        tree = ArithmeticKey(op=">", operands=(TT, LiteralKey(value="2024-01")))
        assert max(k.phase for k in walk_value_keys(tree)) == Phase.ROW

    def test_having_walk_does_not_flag_the_wrapped_column(self) -> None:
        # The TimeTruncKey IS the grouped slot; its raw column must not be
        # reported as a direct ungrouped reference.
        assert SQLGenerator._direct_local_column_keys(TT) == []
        tree = ArithmeticKey(op=">", operands=(TT, LiteralKey(value="a")))
        assert SQLGenerator._direct_local_column_keys(tree) == []


# ---------------------------------------------------------------------------
# Join-path discovery descends expression sources via children() (task 3.5)
# ---------------------------------------------------------------------------
def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="customer_id", sql="customer_id", type=DataType.DOUBLE),
            Column(name="amount", sql="amount", type=DataType.DOUBLE),
        ],
        joins=[
            ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]]),
        ],
    )


def _customers_model() -> SlayerModel:
    return SlayerModel(
        name="customers", sql_table="customers", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="balance", sql="balance", type=DataType.DOUBLE),
        ],
    )


def _paths_for(key: AggregateKey):
    orders = _orders_model()
    bundle = ResolvedSourceBundle(
        source_model=orders, referenced_models=[orders, _customers_model()],
    )
    return compute_aggregate_input_join_paths(
        key=key, anchor_model=orders, anchor_relation="orders", bundle=bundle,
    )


class TestJoinDiscoveryExpressionSources:
    def test_arithmetic_source_operand_crossing_is_discovered(self) -> None:
        key = AggregateKey(
            source=ArithmeticKey(op="-", operands=(JOINED, AMOUNT)), agg="sum",
        )
        assert ("customers",) in _paths_for(key)

    def test_iif_branch_crossing_is_discovered(self) -> None:
        key = AggregateKey(
            source=ScalarCallKey(
                name="iif",
                args=(
                    ArithmeticKey(
                        op=">", operands=(JOINED, LiteralKey(value=Decimal("0"))),
                    ),
                    AMOUNT,
                    Decimal("0"),
                ),
            ),
            agg="sum",
        )
        assert ("customers",) in _paths_for(key)

    def test_time_trunc_inside_a_source_crosses_via_its_column(self) -> None:
        # B1: the trunc's crossing lives on its wrapped column.
        joined_tt = TimeTruncKey(
            column=ColumnKey(path=("customers",), leaf="balance"),
            granularity="month",
        )
        key = AggregateKey(
            source=ArithmeticKey(op="+", operands=(joined_tt, AMOUNT)),
            agg="sum",
        )
        assert ("customers",) in _paths_for(key)

    # model_construct bypasses the source-union validation on purpose:
    # totality over out-of-union kinds is a runtime property.
    def test_dummy_source_flows_via_children(self) -> None:
        key = AggregateKey.model_construct(
            source=DummyKey(child=JOINED), agg="sum",
        )
        assert ("customers",) in _paths_for(key)

    def test_opaque_source_fails_closed(self) -> None:
        key = AggregateKey.model_construct(source=DummyOpaqueKey(), agg="sum")
        with pytest.raises(NotImplementedError):
            _paths_for(key)


# ---------------------------------------------------------------------------
# HAVING validation wiring survives the migration (task 3.5)
# ---------------------------------------------------------------------------
def _status_orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source="test",
        columns=[
            Column(name="id", sql="id", type=DataType.DOUBLE, primary_key=True),
            Column(name="status", sql="status", type=DataType.TEXT),
            Column(name="created_at", sql="created_at", type=DataType.TIMESTAMP),
        ],
    )


class TestHavingValidationWiring:
    async def test_ungrouped_bare_column_in_having_rejected(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            time_dimensions=[TimeDimension(
                dimension=ColumnRef(name="created_at"),
                granularity=TimeGranularity.MONTH,
            )],
            measures=[ModelMeasure(formula="*:count")],
            filters=["_count > 1 and status == 'completed'"],
        )
        model = _status_orders_model()
        with pytest.raises(ValueError, match="dimensions / GROUP BY"):
            await _engine_generate(query=query, model=model)

    async def test_grouped_column_in_having_still_accepted(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=[ColumnRef(name="status")],
            measures=[ModelMeasure(formula="*:count")],
            filters=["_count > 1 and status == 'completed'"],
        )
        sql = await _engine_generate(query=query, model=_status_orders_model())
        assert "HAVING" in sql
