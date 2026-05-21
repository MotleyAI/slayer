"""Stage 7a.6 (DEV-1450) — ValueRegistry tests.

The ValueRegistry interns ValueKeys by structural identity, producing
stable SlotIds and ValueSlots. Two structurally-equal keys share one
slot; the same key declared with multiple aliases accumulates multiple
``public_aliases`` on a single slot (P4 / C13).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from slayer.core.errors import (
    CanonicalAliasShadowsColumnError,
    DuplicateMeasureNameError,
    MeasureNameCollidesWithColumnError,
)
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    LiteralKey,
    Phase,
    StarKey,
)
from slayer.engine.planning import ValueRegistry


# ---------------------------------------------------------------------------
# Basic interning
# ---------------------------------------------------------------------------


class TestInterning:
    def test_intern_returns_stable_slot_id(self):
        r = ValueRegistry()
        sid = r.intern(
            key=ColumnKey(path=(), leaf="status"),
            declared_name="status",
            phase=Phase.ROW,
        )
        assert isinstance(sid, str)
        assert sid
        # Re-intern with same key returns same id.
        sid2 = r.intern(
            key=ColumnKey(path=(), leaf="status"),
            declared_name="status",
            phase=Phase.ROW,
        )
        assert sid == sid2

    def test_different_keys_different_slots(self):
        r = ValueRegistry()
        a = r.intern(
            key=ColumnKey(path=(), leaf="status"),
            declared_name="status",
            phase=Phase.ROW,
        )
        b = r.intern(
            key=ColumnKey(path=(), leaf="amount"),
            declared_name="amount",
            phase=Phase.ROW,
        )
        assert a != b

    def test_structurally_equal_aggregates_intern(self):
        # The same AggregateKey via two different bind paths shares a slot.
        r = ValueRegistry()
        key1 = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"), agg="sum",
        )
        key2 = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"), agg="sum",
        )
        a = r.intern(key=key1, declared_name="amount_sum", phase=Phase.AGGREGATE)
        b = r.intern(key=key2, declared_name="amount_sum", phase=Phase.AGGREGATE)
        assert a == b
        assert len(r.slots) == 1

    def test_kwargs_normalisation_collapses_to_one_slot(self):
        # percentile(p=0.5) and percentile(p=0.50) intern.
        r = ValueRegistry()
        key1 = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="percentile",
            kwargs=(("p", Decimal("0.5")),),
        )
        key2 = AggregateKey(
            source=ColumnKey(path=(), leaf="amount"),
            agg="percentile",
            kwargs=(("p", Decimal("0.50")),),
        )
        a = r.intern(key=key1, declared_name="amount_p50", phase=Phase.AGGREGATE)
        b = r.intern(key=key2, declared_name="amount_p50", phase=Phase.AGGREGATE)
        assert a == b
        assert len(r.slots) == 1


# ---------------------------------------------------------------------------
# Multi-alias (P4 / C13)
# ---------------------------------------------------------------------------


class TestMultiAlias:
    def test_two_names_same_key_share_slot_multi_aliases(self):
        # P4 / C13: same structural key declared with two different names.
        r = ValueRegistry()
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="revenue"), agg="sum",
        )
        a = r.intern(
            key=key, declared_name="rev1", public_name="rev1",
            phase=Phase.AGGREGATE,
        )
        b = r.intern(
            key=key, declared_name="rev2", public_name="rev2",
            phase=Phase.AGGREGATE,
        )
        assert a == b
        slot = r.get(a)
        assert set(slot.public_aliases) == {"rev1", "rev2"}

    def test_hidden_then_declared_promotes_to_public(self):
        # Intern as hidden (order-only / filter-only) first, then later
        # declare a public alias for the same key — the slot becomes
        # public.
        r = ValueRegistry()
        key = AggregateKey(
            source=ColumnKey(path=(), leaf="revenue"), agg="sum",
        )
        hid = r.intern(
            key=key, declared_name="revenue_sum",
            phase=Phase.AGGREGATE, hidden=True,
        )
        pub = r.intern(
            key=key, declared_name="rev", public_name="rev",
            phase=Phase.AGGREGATE,
        )
        assert hid == pub
        slot = r.get(pub)
        assert slot.hidden is False
        assert "rev" in slot.public_aliases


# ---------------------------------------------------------------------------
# Alias-collision validations (preserved from DEV-1443 via P4)
# ---------------------------------------------------------------------------


class TestAliasCollisions:
    def test_duplicate_declared_name_raises(self):
        # Two measures with the same explicit name pointing at DIFFERENT
        # keys → DuplicateMeasureNameError.
        r = ValueRegistry()
        r.intern(
            key=AggregateKey(
                source=ColumnKey(path=(), leaf="amount"), agg="sum",
            ),
            declared_name="rev",
            public_name="rev",
            phase=Phase.AGGREGATE,
        )
        with pytest.raises(DuplicateMeasureNameError):
            r.intern(
                key=AggregateKey(
                    source=ColumnKey(path=(), leaf="revenue"), agg="sum",
                ),
                declared_name="rev",
                public_name="rev",
                phase=Phase.AGGREGATE,
            )

    def test_name_collides_with_source_column_raises(self):
        # A declared name matching a column on the host model is rejected.
        r = ValueRegistry(source_column_names=frozenset({"amount"}))
        with pytest.raises(MeasureNameCollidesWithColumnError):
            r.intern(
                key=AggregateKey(
                    source=ColumnKey(path=(), leaf="revenue"), agg="sum",
                ),
                declared_name="amount",
                public_name="amount",
                phase=Phase.AGGREGATE,
            )

    def test_canonical_alias_shadowing_raises(self):
        # `revenue:sum` canonicalises to `revenue_sum`. If the model has
        # a source column literally named `revenue_sum`, the canonical
        # alias collides — reject.
        r = ValueRegistry(source_column_names=frozenset({"revenue_sum"}))
        with pytest.raises(CanonicalAliasShadowsColumnError):
            r.intern(
                key=AggregateKey(
                    source=ColumnKey(path=(), leaf="revenue"), agg="sum",
                ),
                declared_name="revenue_sum",
                public_name=None,
                canonical_alias="revenue_sum",
                phase=Phase.AGGREGATE,
            )


# ---------------------------------------------------------------------------
# Lookup
# ---------------------------------------------------------------------------


class TestLookup:
    def test_get_by_slot_id(self):
        r = ValueRegistry()
        sid = r.intern(
            key=ColumnKey(path=(), leaf="status"),
            declared_name="status",
            phase=Phase.ROW,
        )
        slot = r.get(sid)
        assert slot.id == sid
        assert slot.declared_name == "status"

    def test_get_unknown_raises(self):
        r = ValueRegistry()
        with pytest.raises(KeyError):
            r.get("does_not_exist")

    def test_find_by_key_returns_slot_id(self):
        r = ValueRegistry()
        key = AggregateKey(source=StarKey(), agg="count")
        sid = r.intern(key=key, declared_name="_count", phase=Phase.AGGREGATE)
        assert r.find_by_key(key) == sid

    def test_find_by_key_missing_returns_none(self):
        r = ValueRegistry()
        key = AggregateKey(source=StarKey(), agg="count")
        assert r.find_by_key(key) is None


# ---------------------------------------------------------------------------
# Literal interning
# ---------------------------------------------------------------------------


class TestLiterals:
    def test_literal_key_interns(self):
        r = ValueRegistry()
        a = r.intern(
            key=LiteralKey(value=Decimal(1)),
            declared_name="lit_1",
            phase=Phase.ROW,
        )
        b = r.intern(
            key=LiteralKey(value=Decimal(1)),
            declared_name="lit_1",
            phase=Phase.ROW,
        )
        assert a == b
