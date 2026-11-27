"""The attached-value key kind: identity, traversal, classification, home and the plan-time invariant."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from decimal import Decimal
from typing import Any, Optional

import pytest

from slayer.core.keys import (
    KIND_POLICY,
    VALUE_KEY_TYPES,
    AggregateKey,
    ArithmeticKey,
    AttachedKey,
    FieldKey,
    Grain,
    LiteralKey,
    Phase,
    anchor_token,
    is_attached_source,
    is_reaggregation_key,
    operand_constituents,
    prepend_value_key,
    reroot_value_key,
    source_anchor_path,
    source_row_leaves,
    substitute_consumer_keys,
    substitute_value_keys,
    walk_value_keys,
)
from slayer.engine.cardinality.relationship_safety import grain_member_attributable
from slayer.engine.compile.stages import ProducerContext, _assert_attached_reads_planned
from slayer.engine.population.home import resolve_aggregate_homes
from slayer.engine.reference_closure import key_closure
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._dev2137_fixtures import all_datasets

TEAM, NAME = FieldKey(leaf="team"), FieldKey(leaf="name")
JOINED_TEAM = FieldKey(path=("contractors",), leaf="team")
ACROSS = FieldKey(path=(anchor_token("assignments"),), leaf="contractor_id")


def attached(*pairs: tuple[str, Any], attach: str = "a0") -> AttachedKey:
    return AttachedKey(attach=attach, on=tuple(pairs))


class TestIdentity:
    def test_pairs_are_canonical_by_slot(self) -> None:
        a, b = attached(("team", TEAM), ("name", NAME)), attached(("name", NAME), ("team", TEAM))
        assert a == b
        assert hash(a) == hash(b)
        assert [slot for slot, _ in a.on] == ["name", "team"]

    def test_a_slot_pairs_with_its_own_member(self) -> None:
        assert attached(("team", TEAM), ("name", NAME)) != attached(("team", NAME), ("name", TEAM))

    def test_the_attach_id_is_identity(self) -> None:
        assert attached(("team", TEAM)) != attached(("team", TEAM), attach="a1")

    def test_the_join_is_identity(self) -> None:
        assert attached(("team", TEAM)) != attached(("team", JOINED_TEAM))

    def test_one_value_at_every_phase(self) -> None:
        # The reading position sets the phase; the key carries none.
        assert "phase" not in AttachedKey.model_fields


class TestTraversal:
    def test_children_are_the_members_in_slot_order(self) -> None:
        assert attached(("team", TEAM), ("name", NAME)).children() == (NAME, TEAM)

    def test_map_children_identity_returns_self(self) -> None:
        key = attached(("team", TEAM))
        assert key.map_children(lambda c: c) is key

    def test_map_children_rewrites_members_only(self) -> None:
        key = attached(("team", TEAM), ("name", NAME))
        out = key.map_children(lambda c: JOINED_TEAM if c == TEAM else c)
        assert out == attached(("team", JOINED_TEAM), ("name", NAME))

    def test_walk_reaches_the_members(self) -> None:
        key = attached(("team", TEAM))
        assert list(walk_value_keys(key)) == [key, TEAM]

    def test_reroot_rewrites_the_join_never_the_identity(self) -> None:
        key = attached(("team", JOINED_TEAM), attach="a7")
        assert reroot_value_key(key, target_path=("contractors",)) == attached(("team", TEAM), attach="a7")

    def test_prepend_rewrites_the_join_never_the_identity(self) -> None:
        key = attached(("team", TEAM), attach="a7")
        assert prepend_value_key(key, host_path=("contractors",)) == attached(("team", JOINED_TEAM), attach="a7")

    def test_substituted_whole(self) -> None:
        key = attached(("team", TEAM))
        expr = ArithmeticKey(op=">", operands=(key, LiteralKey(value=Decimal(50))))
        out = substitute_value_keys(expr, {key: FieldKey(leaf="band")})
        assert out == ArithmeticKey(op=">", operands=(FieldKey(leaf="band"), LiteralKey(value=Decimal(50))))

    def test_member_substitution_keeps_the_identity(self) -> None:
        assert substitute_value_keys(attached(("team", TEAM)), {TEAM: JOINED_TEAM}) == attached(("team", JOINED_TEAM))


class TestKindRegistration:
    def test_a_value_key_kind(self) -> None:
        assert AttachedKey in VALUE_KEY_TYPES

    def test_policy_of_the_column_it_renders_as(self) -> None:
        assert KIND_POLICY[AttachedKey] == KIND_POLICY[FieldKey]


class TestClassification:
    def test_an_attached_constituent(self) -> None:
        key = attached(("team", TEAM))
        source = ArithmeticKey(op="*", operands=(key, LiteralKey(value=Decimal(2))))
        assert operand_constituents(source) == [key]
        assert source_row_leaves(source) == []
        assert is_attached_source(source)

    def test_an_aggregation_over_attached_values_is_second_order(self) -> None:
        agg = AggregateKey(source=attached(("team", TEAM)), agg="sum")
        assert is_reaggregation_key(agg)

    def test_a_mixed_source_is_row_grain(self) -> None:
        source = ArithmeticKey(op="*", operands=(attached(("team", TEAM)), FieldKey(leaf="rate")))
        assert not is_reaggregation_key(AggregateKey(source=source, agg="sum"))

    def test_the_source_anchor_reads_the_members(self) -> None:
        assert source_anchor_path(attached(("team", JOINED_TEAM))) == ("contractors",)


def _bundle() -> tuple[ResolvedSourceBundle, dict[str, Any]]:
    datasets = all_datasets()
    by_name = {d.name: d for d in datasets}
    return ResolvedSourceBundle(dialect="duckdb", population=by_name["contractors"], referenced_datasets=datasets), by_name


class TestStandsForItsMembers:
    @pytest.mark.parametrize("member", [TEAM, ACROSS], ids=["attributable", "across-a-fanning-hop"])
    def test_attributability(self, member: FieldKey) -> None:
        bundle, by_name = _bundle()
        root = by_name["contractors"]

        def verdict(key: Any) -> bool:
            return grain_member_attributable(key=key, target_path=(), root_dataset=root, datasets_by_name=by_name,
                                             bundle=bundle, host_dataset=root)

        assert verdict(attached(("k", member))) == verdict(member)

    @pytest.mark.parametrize("member", [TEAM, ACROSS], ids=["local", "across"])
    def test_dependency_closure(self, member: FieldKey) -> None:
        bundle, by_name = _bundle()
        root = by_name["contractors"]

        def closure(key: Any) -> Any:
            return key_closure(key=key, anchor_dataset=root, anchor_relation=root.name, bundle=bundle)

        assert closure(attached(("k", member))) == closure(member)

    def test_an_aggregation_over_it_homes_on_its_operand(self) -> None:
        bundle, by_name = _bundle()
        agg = AggregateKey(source=attached(("team", TEAM)), agg="sum", partition_keys=Grain.of([TEAM]))
        homes = resolve_aggregate_homes(roots=[agg], host_dataset=by_name["contractors"], datasets_by_name=by_name,
                                        bundle=bundle, dim_keys=[TEAM], td_keys=[], active_bucket=None)
        assert homes.of(agg).kind == "operand_dataset"


class TestSubstitutionRecordsRebuiltAggregates:
    @pytest.mark.parametrize("substitute", [substitute_value_keys, substitute_consumer_keys])
    def test_two_changed_partition_members_both_pair(self, substitute: Any) -> None:
        a, b, keep = (AggregateKey(source=FieldKey(leaf=x), agg="max") for x in ("x1", "x2", "x3"))
        before = AggregateKey(source=FieldKey(leaf="rate"), agg="sum", partition_keys=Grain.of([a, b, keep]))
        mapping = {FieldKey(leaf="x1"): attached(("t", TEAM)), FieldKey(leaf="x2"): attached(("t", TEAM), attach="a1")}
        rebuilt: dict[Any, Any] = {}
        after = substitute(before, mapping, rebuilt=rebuilt)
        a2, b2 = (AggregateKey(source=mapping[FieldKey(leaf=x)], agg="max") for x in ("x1", "x2"))
        assert after == before.model_copy(update={"partition_keys": Grain.of([a2, b2, keep])})
        assert rebuilt == {before: after, a: a2, b: b2}

    def test_nothing_rebuilt_records_nothing(self) -> None:
        rebuilt: dict[Any, Any] = {}
        key = AggregateKey(source=FieldKey(leaf="rate"), agg="sum")
        assert substitute_value_keys(key, {FieldKey(leaf="other"): TEAM}, rebuilt=rebuilt) is key
        assert rebuilt == {}


def check_planned(reads: list[Any], edges: list[Any], *, grains: dict[str, frozenset[str]],
                  answers: Optional[dict[str, str]] = None, slots: Optional[dict[str, frozenset[str]]] = None,
                  resolves: Callable[[Any], bool] = lambda _member: True) -> None:
    """The invariant over producers whose answer slot is ``v`` and whose slots are their grain plus ``v``."""
    answers = {a: "v" for a in grains} if answers is None else answers
    slots = {a: g | {"v"} for a, g in grains.items()} if slots is None else slots
    _assert_attached_reads_planned(reads=reads, edges=edges, producer_grains=grains, producer_slots=slots,
                                   answer_slots=answers, resolves=resolves)


class TestPlanTimeInvariant:
    def test_a_planned_read(self) -> None:
        key = attached(("team", TEAM), ("name", NAME))
        check_planned([(key, Phase.ROW)], [(key, Phase.ROW)], grains={"a0": frozenset({"team", "name"})})

    def test_one_value_read_at_two_phases_needs_an_edge_at_each(self) -> None:
        key = attached(("team", TEAM))
        reads = [(key, Phase.ROW), (key, Phase.AGGREGATE)]
        check_planned(reads, reads, grains={"a0": frozenset({"team"})})
        with pytest.raises(RuntimeError, match="a0"):
            check_planned(reads, reads[:1], grains={"a0": frozenset({"team"})})

    def test_an_unattached_read_is_refused(self) -> None:
        key = attached(("team", TEAM))
        with pytest.raises(RuntimeError, match="a0"):
            check_planned([(key, Phase.ROW)], [], grains={"a0": frozenset({"team"})})

    def test_a_duplicate_edge_is_refused(self) -> None:
        key = attached(("team", TEAM))
        with pytest.raises(RuntimeError, match="a0"):
            check_planned([(key, Phase.ROW)], [(key, Phase.ROW)] * 2, grains={"a0": frozenset({"team"})})

    @pytest.mark.parametrize("grain", [frozenset({"team", "name"}), frozenset()], ids=["uncovered", "extra"])
    def test_the_pairs_cover_the_producer_grain_exactly(self, grain: frozenset[str]) -> None:
        key = attached(("team", TEAM))
        with pytest.raises(RuntimeError, match="a0"):
            check_planned([(key, Phase.ROW)], [(key, Phase.ROW)], grains={"a0": grain})

    def test_an_unknown_producer_is_refused(self) -> None:
        key = attached(("team", TEAM), attach="a9")
        with pytest.raises(RuntimeError, match="a9"):
            check_planned([(key, Phase.ROW)], [(key, Phase.ROW)], grains={})

    def test_a_member_unresolved_in_the_reader_is_refused(self) -> None:
        key = attached(("team", TEAM), ("name", NAME))
        with pytest.raises(RuntimeError, match="a0"):
            check_planned([(key, Phase.ROW)], [(key, Phase.ROW)], grains={"a0": frozenset({"team", "name"})},
                          resolves=lambda member: member != NAME)

    def test_a_missing_answer_slot_is_refused(self) -> None:
        key = attached(("team", TEAM))
        with pytest.raises(RuntimeError, match="a0"):
            check_planned([(key, Phase.ROW)], [(key, Phase.ROW)], grains={"a0": frozenset({"team"})},
                          slots={"a0": frozenset({"team"})})


class TestRetired:
    @pytest.mark.parametrize(("module", "name"), [
        ("slayer.engine.elaborate_env", "check_reserved_regroup_prefix"),
        ("slayer.engine.compile.regroup", "reserved_prefix_fields"),
        ("slayer.engine.key_metadata", "minted_placeholders"),
        ("slayer.engine.compile.stages", "_rebuilt_aggregates"),
        ("slayer.engine.compile.stages", "_ordered_parts"),
        ("slayer.engine.compile.stages", "_attaches_carrying"),
        ("slayer.engine.compile.stages", "_placeholders_read"),
    ])
    def test_module_attribute(self, module: str, name: str) -> None:
        assert not hasattr(importlib.import_module(module), name)

    @pytest.mark.parametrize("field", ["carried_attaches", "reserved_placeholders", "placeholder_originals"])
    def test_producer_context_field(self, field: str) -> None:
        assert field not in ProducerContext.model_fields
