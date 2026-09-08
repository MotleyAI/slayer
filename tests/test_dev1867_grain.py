"""DEV-1867 — Grain as a first-class type with lattice operations.

Covers the new ``slayer/core/grain.py`` value type (construction, set protocol,
Grain-only equality, lattice predicates, operand contract) and the planner sites
retyped to ``Grain``: ``regroup_root_grain``, ``_effective_root_grain``,
``_prune_functionally_determined_grain``, and the ``_validate_nested_producer_plan``
broadcast-direction admission. Fails on import until ``grain.py`` exists; the
grain-direction planner tests fail until the planners return/accept ``Grain``. The
row-phase-skip and deeper-CTE guards are behavior-preserving — they pass once the
module exists and must keep passing after the refactor.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from slayer.core.grain import Grain
from slayer.core.keys import (
    AggregateKey,
    ArithmeticKey,
    ColumnKey,
    TransformKey,
)
from slayer.engine.planned import (
    PlannedQuery,
    RegroupAttachPlan,
    RegroupSubstitution,
)
from slayer.engine.regroup_planner import regroup_root_grain
from slayer.engine.stage_planner import (
    _effective_root_grain,
    _prune_functionally_determined_grain,
    _validate_nested_producer_plan,
)

REGION = ColumnKey(path=(), leaf="region")
CITY = ColumnKey(path=(), leaf="city")
CHANNEL = ColumnKey(path=(), leaf="channel")
MONTH = ColumnKey(path=(), leaf="month")
AMOUNT = ColumnKey(path=(), leaf="amount")

SUM_BY_REGION = AggregateKey(source=AMOUNT, agg="sum", partition_keys=frozenset({REGION}))
SUM_BY_CITY = AggregateKey(source=AMOUNT, agg="sum", partition_keys=frozenset({CITY}))
BARE_SUM = AggregateKey(source=AMOUNT, agg="sum")
WINDOW_SUM = AggregateKey(source=AMOUNT, agg="sum", kwargs=(("window", "90d"),))
# rank(sum(partition_by=region) + sum(partition_by=city)) — union grain {region, city}.
RANK_UNION = TransformKey(
    op="rank", input=ArithmeticKey(op="+", operands=(SUM_BY_REGION, SUM_BY_CITY)),
)
# rank(sum(partition_by=region)) — fully determined by region.
RANK_DETERMINED = TransformKey(op="rank", input=SUM_BY_REGION)


class TestGrainConstruction:
    def test_of_coerces_list_set_generator(self) -> None:
        keys = {REGION, CITY}
        assert Grain.of([REGION, CITY]) == Grain.of(keys)
        assert Grain.of(k for k in (REGION, CITY)) == Grain.of(keys)

    def test_of_dedups(self) -> None:
        assert len(Grain.of([REGION, REGION, CITY])) == 2

    def test_of_rejects_non_valuekey(self) -> None:
        with pytest.raises((ValidationError, TypeError)):
            Grain.of([REGION, "not-a-key"])

    def test_frozen_mutation_fails(self) -> None:
        with pytest.raises((ValidationError, TypeError)):
            Grain.of([REGION]).keys = frozenset()  # type: ignore[misc]

    def test_empty_constant(self) -> None:
        assert Grain.of([]) == Grain.EMPTY
        assert Grain.EMPTY.is_empty
        assert not Grain.EMPTY


class TestGrainSetProtocol:
    def test_membership(self) -> None:
        g = Grain.of([REGION, CITY])
        assert REGION in g
        assert CHANNEL not in g

    def test_iteration_and_len(self) -> None:
        g = Grain.of([REGION, CITY])
        assert set(g) == {REGION, CITY}
        assert len(g) == 2

    def test_bool_and_is_empty(self) -> None:
        assert bool(Grain.of([REGION]))
        assert not Grain.of([REGION]).is_empty
        assert not bool(Grain.of([]))
        assert Grain.of([]).is_empty


class TestGrainEquality:
    def test_hash_equality(self) -> None:
        a = Grain.of([REGION, CITY])
        b = Grain.of([CITY, REGION])
        assert a == b
        assert hash(a) == hash(b)

    def test_dict_key_use(self) -> None:
        a = Grain.of([REGION, CITY])
        b = Grain.of([CITY, REGION])
        assert {a: "v"}[b] == "v"

    def test_not_equal_to_raw_frozenset(self) -> None:
        g = Grain.of([REGION])
        assert g != frozenset({REGION})
        assert frozenset({REGION}) != g
        assert not (g == frozenset({REGION}))


class TestGrainLattice:
    def test_union_and_empty_identity(self) -> None:
        assert Grain.of([REGION]).union(Grain.of([CITY])) == Grain.of([REGION, CITY])
        assert Grain.of([REGION]).union(Grain.EMPTY) == Grain.of([REGION])
        assert Grain.EMPTY.union(Grain.of([REGION])) == Grain.of([REGION])

    def test_subgrain_reflexive_strict_irreflexive(self) -> None:
        g = Grain.of([REGION])
        assert g.is_subgrain_of(g)
        assert not g.is_strict_subgrain_of(g)

    def test_subgrain_direction(self) -> None:
        coarse = Grain.of([REGION])
        fine = Grain.of([REGION, CITY])
        assert coarse.is_subgrain_of(fine)
        assert coarse.is_strict_subgrain_of(fine)
        assert not fine.is_subgrain_of(coarse)

    def test_broadcasts_into_both_directions_and_equal(self) -> None:
        coarse = Grain.of([REGION])
        fine = Grain.of([REGION, CITY])
        assert coarse.broadcasts_into(fine)
        assert not fine.broadcasts_into(coarse)
        assert coarse.broadcasts_into(coarse)

    def test_comparison_operators(self) -> None:
        coarse = Grain.of([REGION])
        fine = Grain.of([REGION, CITY])
        assert coarse <= fine
        assert coarse < fine
        assert fine >= coarse
        assert fine > coarse
        assert not (fine <= coarse)

    def test_operator_named_method_agreement(self) -> None:
        a = Grain.of([REGION])
        b = Grain.of([REGION, CITY])
        assert (a | b) == a.union(b)
        assert (a <= b) == a.is_subgrain_of(b)
        assert (a < b) == a.is_strict_subgrain_of(b)
        assert a.broadcasts_into(b) == a.is_subgrain_of(b)


class TestGrainOperandContract:
    def test_comparison_with_frozenset_raises(self) -> None:
        g = Grain.of([REGION])
        with pytest.raises(TypeError):
            _ = g <= frozenset({REGION})
        with pytest.raises(TypeError):
            _ = frozenset({REGION}) <= g

    def test_or_with_grain_and_raw_set_returns_grain(self) -> None:
        base = Grain.of([REGION])
        # Grain, a mutable set, and a frozenset (a distinct AbstractSet, not a
        # ``set`` subclass) all coerce; result is always a Grain.
        for other in (Grain.of([CITY]), {CITY}, frozenset({CITY})):
            out = base | other
            assert type(out) is Grain
            assert out == Grain.of([REGION, CITY])

    def test_sub_with_grain_and_raw_set_returns_grain(self) -> None:
        base = Grain.of([REGION, CITY])
        for other in (Grain.of([CITY]), {CITY}, frozenset({CITY})):
            out = base - other
            assert type(out) is Grain
            assert out == Grain.of([REGION])

    def test_or_with_invalid_operand_raises(self) -> None:
        with pytest.raises(TypeError):
            _ = Grain.of([REGION]) | 5  # type: ignore[operator]


class TestRegroupRootGrain:
    def test_bare_partitioned_root_own_grain(self) -> None:
        out = regroup_root_grain(SUM_BY_REGION)
        assert type(out) is Grain
        assert out == Grain.of([REGION])

    def test_transform_root_union_of_inner_grains(self) -> None:
        out = regroup_root_grain(RANK_UNION)
        assert type(out) is Grain
        assert out == Grain.of([REGION, CITY])

    def test_unpartitioned_root_empty(self) -> None:
        out = regroup_root_grain(BARE_SUM)
        assert type(out) is Grain
        assert out == Grain.EMPTY


class TestEffectiveRootGrain:
    def test_bare_non_windowed_full_projected_grain(self) -> None:
        grain, windowed = _effective_root_grain(
            BARE_SUM, projected_dim_keys=[REGION, CITY],
            projected_td_keys=[MONTH], active_bucket=None,
        )
        assert type(grain) is Grain
        assert grain == Grain.of([REGION, CITY, MONTH])
        assert windowed is False

    def test_bare_windowed_excludes_active_bucket(self) -> None:
        grain, windowed = _effective_root_grain(
            WINDOW_SUM, projected_dim_keys=[REGION],
            projected_td_keys=[MONTH], active_bucket=MONTH,
        )
        assert type(grain) is Grain
        assert grain == Grain.of([REGION])
        assert windowed is True


class TestPruneFunctionallyDeterminedGrain:
    def test_type_preserved_and_input_unchanged(self) -> None:
        g = Grain.of([REGION, RANK_DETERMINED])
        out = _prune_functionally_determined_grain(g)
        assert type(out) is Grain
        assert g == Grain.of([REGION, RANK_DETERMINED])  # input untouched
        assert RANK_DETERMINED not in out  # determined by region → dropped
        assert REGION in out


def _sub(*, windowed: bool) -> RegroupSubstitution:
    orig = WINDOW_SUM if windowed else BARE_SUM
    return RegroupSubstitution(placeholder=orig, producer_slot_id="s", original_key=orig)


def _attach(*, host_keys, windowed, phase="combined", nested=()):
    return RegroupAttachPlan(
        producer_plan=PlannedQuery(
            source_relation="_nested", regroup_attach_plans=list(nested),
        ),
        alias_hint="a",
        attach_phase=phase,
        join_pairs=[(k, f"slot{i}") for i, k in enumerate(host_keys)],
        substitutions=[_sub(windowed=windowed)],
    )


def _producer(attach: RegroupAttachPlan) -> PlannedQuery:
    return PlannedQuery(source_relation="_regroup", regroup_attach_plans=[attach])


_SUBSET_MSG = (
    "A union-grain producer's nested attach grain is not a subset "
    "of the producer grain; only subset inner grains broadcast (DEV-1839)."
)


class TestValidateNestedProducerPlan:
    UNION = Grain.of([REGION, CITY])

    def test_equal_grain_windowed_admitted(self) -> None:
        pp = _producer(_attach(host_keys=[REGION, CITY], windowed=True))
        _validate_nested_producer_plan(producer_plan=pp, producer_grain=self.UNION)

    def test_equal_grain_non_windowed_rejected(self) -> None:
        pp = _producer(_attach(host_keys=[REGION, CITY], windowed=False))
        with pytest.raises(NotImplementedError) as ei:
            _validate_nested_producer_plan(producer_plan=pp, producer_grain=self.UNION)
        assert str(ei.value) == _SUBSET_MSG

    def test_strict_subgrain_admitted(self) -> None:
        pp = _producer(_attach(host_keys=[REGION], windowed=False))
        _validate_nested_producer_plan(producer_plan=pp, producer_grain=self.UNION)

    def test_supergrain_rejected(self) -> None:
        pp = _producer(_attach(host_keys=[REGION, CITY, CHANNEL], windowed=False))
        with pytest.raises(NotImplementedError) as ei:
            _validate_nested_producer_plan(producer_plan=pp, producer_grain=self.UNION)
        assert str(ei.value) == _SUBSET_MSG

    def test_row_phase_attach_skipped(self) -> None:
        # A supergrain ROW attach would reject if checked; the phase gate skips it.
        pp = _producer(
            _attach(host_keys=[REGION, CITY, CHANNEL], windowed=False, phase="row"),
        )
        _validate_nested_producer_plan(producer_plan=pp, producer_grain=self.UNION)

    def test_deeper_cte_rejected(self) -> None:
        inner = _attach(host_keys=[REGION], windowed=False)
        pp = _producer(_attach(host_keys=[REGION], windowed=False, nested=[inner]))
        with pytest.raises(NotImplementedError) as ei:
            _validate_nested_producer_plan(producer_plan=pp, producer_grain=self.UNION)
        assert "needs a further regroup producer CTE" in str(ei.value)
