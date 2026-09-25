"""DEV-1901 — ``SqlFragmentKey``: a bound parameter expression (template + typed refs)."""

from __future__ import annotations

from decimal import Decimal
from typing import get_args

import pytest
from pydantic import ValidationError

from slayer.core.keys import (
    KIND_POLICY,
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    KindPolicy,
    Phase,
    SqlFragmentKey,
    ValueKey,
    reroot_value_key,
    substitute_value_keys,
    walk_value_keys,
)
from slayer.core.refs import legacy_key_repr
from slayer.sql.naming import canonical_aggregate_alias

AMOUNT = ColumnKey(path=(), leaf="amount")
SPEND = ColumnKey(path=("customers",), leaf="spend")
POP = ColumnKey(path=("customers", "regions"), leaf="pop")
CUST_SPEND = ColumnSqlKey(path=(), model="orders", column_name="cust_spend")
FRAG = SqlFragmentKey(template="{r0} * {r1}", refs=(SPEND, POP))


def _agg(weight) -> AggregateKey:
    return AggregateKey(source=AMOUNT, agg="wsum", kwargs=(("weight", weight),))


class TestShape:
    def test_fields(self) -> None:
        assert FRAG.template == "{r0} * {r1}"
        assert FRAG.refs == (SPEND, POP)

    def test_phase_is_row(self) -> None:
        assert FRAG.phase == Phase.ROW

    def test_frozen(self) -> None:
        key = SqlFragmentKey(template="{r0} * {r1}", refs=(SPEND, POP))
        with pytest.raises((TypeError, ValueError)):
            key.template = "{r0}"  # type: ignore[misc]

    def test_refs_accept_derived_columns(self) -> None:
        assert SqlFragmentKey(template="{r0} + 1", refs=(CUST_SPEND,)).refs == (CUST_SPEND,)

    def test_zero_refs(self) -> None:
        key = SqlFragmentKey(template="1 + 1", refs=())
        assert key.children() == ()

    def test_refs_reject_non_column_keys(self) -> None:
        agg = AggregateKey(source=AMOUNT, agg="sum")
        with pytest.raises(ValidationError):
            SqlFragmentKey(template="{r0}", refs=(agg,))  # pyright: ignore[reportArgumentType] — deliberately invalid


class TestIdentity:
    def test_equal_and_same_hash(self) -> None:
        twin = SqlFragmentKey(template="{r0} * {r1}", refs=(SPEND, POP))
        assert twin == FRAG
        assert hash(twin) == hash(FRAG)

    @pytest.mark.parametrize("other", [
        SqlFragmentKey(template="{r0} + {r1}", refs=(SPEND, POP)),
        SqlFragmentKey(template="{r0} * {r1}", refs=(POP, SPEND)),
        SqlFragmentKey(template="{r0} * {r1}", refs=(SPEND, AMOUNT)),
    ])
    def test_template_and_refs_both_identify(self, other) -> None:
        assert other != FRAG

    def test_aggregates_over_equal_fragments_intern(self) -> None:
        twin = SqlFragmentKey(template="{r0} * {r1}", refs=(SPEND, POP))
        assert _agg(FRAG) == _agg(twin)
        assert len({_agg(FRAG), _agg(twin)}) == 1

    def test_legacy_spelling_names_the_kind_and_template(self) -> None:
        spelled = legacy_key_repr(FRAG)
        assert spelled.startswith("SqlFragmentKey(")
        assert spelled != legacy_key_repr(
            SqlFragmentKey(template="{r0} + {r1}", refs=(SPEND, POP)))


class TestTraversal:
    def test_children_are_the_refs(self) -> None:
        assert FRAG.children() == (SPEND, POP)

    def test_map_children_identity_returns_self(self) -> None:
        assert FRAG.map_children(lambda c: c) is FRAG

    def test_map_children_replaces_refs_in_place(self) -> None:
        out = FRAG.map_children(lambda c: AMOUNT if c == POP else c)
        assert isinstance(out, SqlFragmentKey)
        assert out.template == FRAG.template
        assert out.refs == (SPEND, AMOUNT)

    def test_reroot_strips_the_ref_paths(self) -> None:
        out = reroot_value_key(FRAG, target_path=("customers",))
        assert out == SqlFragmentKey(
            template="{r0} * {r1}",
            refs=(ColumnKey(path=(), leaf="spend"), ColumnKey(path=("regions",), leaf="pop")))

    def test_substitute_reaches_the_refs(self) -> None:
        out = substitute_value_keys(key=FRAG, mapping={POP: AMOUNT})
        assert out.refs == (SPEND, AMOUNT)

    def test_walk_reaches_refs_through_an_aggregate_kwarg(self) -> None:
        assert {SPEND, POP} <= set(walk_value_keys(_agg(FRAG)))


class TestUnionMembership:
    def test_is_a_value_key_kind(self) -> None:
        assert SqlFragmentKey in get_args(ValueKey)

    def test_kind_policy_is_plain(self) -> None:
        assert KIND_POLICY[SqlFragmentKey] == KindPolicy()

    def test_aggregate_kwarg_admits_it(self) -> None:
        key = _agg(FRAG)
        assert dict(key.kwargs)["weight"] is FRAG
        assert FRAG in key.children()

    def test_aggregate_arg_admits_it(self) -> None:
        key = AggregateKey(source=AMOUNT, agg="wsum", args=(FRAG,))
        assert key.args == (FRAG,)


class TestAliasEncoding:
    """Internal aliases derive from the full key: distinct templates never alias."""

    @staticmethod
    def _alias(weight) -> str:
        alias = canonical_aggregate_alias(
            _agg(weight), profile="cross_model_cte", source_relation="orders")
        assert alias is not None
        return alias

    def test_equal_fragments_share_an_alias(self) -> None:
        assert self._alias(FRAG) == self._alias(
            SqlFragmentKey(template="{r0} * {r1}", refs=(SPEND, POP)))

    def test_distinct_templates_never_alias(self) -> None:
        assert self._alias(FRAG) != self._alias(
            SqlFragmentKey(template="{r0} + {r1}", refs=(SPEND, POP)))

    def test_distinct_refs_never_alias(self) -> None:
        assert self._alias(FRAG) != self._alias(
            SqlFragmentKey(template="{r0} * {r1}", refs=(SPEND, AMOUNT)))

    def test_fragment_never_aliases_its_column(self) -> None:
        assert self._alias(SqlFragmentKey(template="{r0} * 1", refs=(SPEND,))) \
            != self._alias(SPEND)

    def test_boolean_is_encoded_distinct_from_one(self) -> None:
        assert self._alias(True) != self._alias(Decimal("1"))
