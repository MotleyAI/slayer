"""The one dependency closure (python.engine.arc42.md principle 10).

Unit tests for ``slayer.engine.reference_closure``: a fragment's / key's
dependency closure is its own relationship path plus every path the definition of any
derived field it names crosses, recursively — root-relative prefixes, tri-state
``None`` when a definition cannot be analysed, a raised cycle guard on a
self-referential definition. Plus ``ResolvedSourceBundle.datasets_by_name`` — the
host-inclusive, source-first dataset map every walker consumes.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.keys import FieldKey, FieldSqlKey, StarKey, TimeTruncKey
from slayer.core.models import SlayerDataset
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.field_expansion import DatasetFieldCycleError

from slayer.engine.reference_closure import fragment_closure, key_closure
from slayer.engine.compile.stages import _PushBlocked, _ref_sql_dependency_paths

from tests._dev1900_fixtures import dev1900_datasets, unparseable_derived_datasets

#: The one fanning crossing, root-relative from the query host (orders).
CROSS = ("regions", "region_events")


def _bundle(datasets):
    by = {m.name: m for m in datasets}
    referenced = [by[n] for n in ("customers", "regions", "region_events", "stores", "plans")]
    return by, ResolvedSourceBundle(dialect="postgres", population=by["orders"], referenced_datasets=referenced)


def _regions_sqlkey(column_name: str) -> FieldSqlKey:
    return FieldSqlKey(path=("regions",), dataset="regions", field_name=column_name)


class TestFragmentClosure:
    """Anchored owner-locally at regions (owner_path=())."""

    def setup_method(self):
        self.by, self.bundle = _bundle(dev1900_datasets())
        self.regions = self.by["regions"]

    def _frag(self, sql, *, owner_path=(), anchor_relation="regions"):
        return fragment_closure(sql=sql, dataset=self.regions, owner_path=owner_path,
                                anchor_relation=anchor_relation, bundle=self.bundle)

    def test_structural_crossing(self):
        assert self._frag("region_events.value") == (("region_events",),)

    def test_derived_crossing_is_expanded(self):
        assert self._frag("bad_pop") == (("region_events",),)

    def test_chain_crossing_is_expanded(self):
        assert self._frag("bad_pop * 2") == (("region_events",),)

    def test_local_column_contributes_nothing(self):
        assert self._frag("pop") == ()

    def test_local_derived_contributes_nothing(self):
        assert self._frag("derived_pop") == ()

    def test_opaque_qualifier_tolerated(self):
        assert self._frag("nosuch.col + 1") == ()

    def test_unparseable_is_tristate_none(self):
        assert self._frag(")((( bad") is None

    def test_owner_path_prefixes_the_crossing(self):
        got = self._frag("region_events.value", owner_path=("customers", "regions"),
                         anchor_relation="customers__regions")
        assert got is not None
        assert ("customers", "regions", "region_events") in got


class TestKeyClosure:
    """Anchored at customers — a to-one hop from the orders host."""

    def setup_method(self):
        self.by, self.bundle = _bundle(dev1900_datasets())
        self.customers = self.by["customers"]

    def _key(self, key):
        return key_closure(key=key, anchor_dataset=self.customers,
                           anchor_relation="customers", bundle=self.bundle)

    def test_derived_key_descends_into_crossing(self):
        got = self._key(_regions_sqlkey("bad_pop"))
        assert got is not None
        assert ("regions",) in got
        assert CROSS in got

    def test_chain_derived_key_descends(self):
        got = self._key(_regions_sqlkey("bad_pop2"))
        assert got is not None
        assert CROSS in got

    def test_local_derived_key_no_crossing(self):
        assert self._key(_regions_sqlkey("derived_pop")) == (("regions",),)

    def test_plain_column_key_is_its_path(self):
        assert self._key(FieldKey(path=("regions",), leaf="pop")) == (("regions",),)

    def test_star_key_does_not_descend(self):
        got = self._key(StarKey(path=("regions",)))
        assert got is not None
        assert CROSS not in got
        assert all(len(p) <= 1 for p in got), f"StarKey must not descend: {got}"

    def test_a_string_is_never_sql(self):
        by, bundle = _bundle(dev1900_datasets())
        got = key_closure(key="region_events.value", anchor_dataset=by["regions"],
                          anchor_relation="regions", bundle=bundle)
        assert got == ()

    def test_time_trunc_delegates_to_its_column(self):
        key = TimeTruncKey(source=_regions_sqlkey("bad_pop"), granularity=TimeGranularity.MONTH)
        got = self._key(key)
        assert got is not None
        assert CROSS in got


class TestKeyClosurePathologies:
    def setup_method(self):
        self.by, self.bundle = _bundle(unparseable_derived_datasets())
        self.customers = self.by["customers"]

    def _key(self, key):
        return key_closure(key=key, anchor_dataset=self.customers,
                           anchor_relation="customers", bundle=self.bundle)

    def test_unparseable_definition_is_tristate_none(self):
        assert self._key(_regions_sqlkey("unparseable")) is None

    def test_cyclic_definition_raises(self):
        key = _regions_sqlkey("cyc")
        with pytest.raises(DatasetFieldCycleError):
            self._key(key)


class TestPushPlanHonoursUnanalyzable:
    """The semi-join push consumer must honour fragment_closure's tri-state:
    None (unanalyzable) ≠ () (local). An unparseable definition fails the push
    CLOSED — dropping the hop would omit a required join / mis-scope a predicate."""

    def setup_method(self):
        self.by, self.bundle = _bundle(unparseable_derived_datasets())

    def _deps(self, key):
        return _ref_sql_dependency_paths(
            key, host_dataset=self.by["orders"],
            datasets_by_name=self.by, bundle=self.bundle,
        )

    def test_unanalyzable_fragment_blocks_the_push(self):
        key = _regions_sqlkey("unparseable")
        with pytest.raises(_PushBlocked):
            self._deps(key)

    def test_local_derived_contributes_no_hop(self):
        assert self._deps(_regions_sqlkey("derived_pop")) == ()

    def test_crossing_derived_registers_its_hop(self):
        assert self._deps(_regions_sqlkey("bad_pop")) == (("region_events",),)


class TestModelsByName:
    def test_host_inclusive_and_source_first(self):
        by, bundle = _bundle(dev1900_datasets())
        mbn = bundle.datasets_by_name
        assert set(mbn) == {"orders", "customers", "regions", "region_events",
                            "stores", "plans"}
        assert next(iter(mbn)) == "orders"
        assert mbn["orders"] is by["orders"]

    def test_source_wins_dedup_over_a_referenced_namesake(self):
        """A distinct dataset sharing the source's name in referenced_models must
        NOT replace the source — the source is inserted first and wins."""
        by = {m.name: m for m in dev1900_datasets()}
        source_orders = by["orders"]
        shadow_orders = SlayerDataset(name="orders", data_source="test", sql_table="orders")
        assert shadow_orders is not source_orders
        bundle = ResolvedSourceBundle(
            dialect="postgres",
            population=source_orders,
            referenced_datasets=[shadow_orders, by["customers"]])
        mbn = bundle.datasets_by_name
        assert list(mbn) == ["orders", "customers"]
        assert mbn["orders"] is source_orders

    def test_hand_built_bundle_includes_the_host(self):
        """A hand-built bundle whose source is not in referenced_models still
        exposes the host, so a source→host reverse hop resolves deterministically
        rather than incidentally (gap 5)."""
        by = {m.name: m for m in dev1900_datasets()}
        bundle = ResolvedSourceBundle(
            dialect="postgres",
            population=by["regions"], referenced_datasets=[by["region_events"]])
        assert "regions" in bundle.datasets_by_name

    def test_rerooted_keeps_the_former_source_in_the_universe(self):
        """Re-rooting at a producer root must not shrink the map: the former
        source stays visible so a root→ex-host reverse hop remains provable."""
        by = {m.name: m for m in dev1900_datasets()}
        bundle = ResolvedSourceBundle(
            dialect="postgres",
            population=by["orders"], referenced_datasets=[by["customers"]])
        rerooted = bundle.rerooted(by["customers"])
        assert rerooted.population is by["customers"]
        assert set(rerooted.datasets_by_name) == {"orders", "customers"}
        # Already-listed source: no duplicate entry.
        listed = ResolvedSourceBundle(
            dialect="postgres",
            population=by["orders"],
            referenced_datasets=[by["orders"], by["customers"]])
        assert [m.name for m in listed.rerooted(by["customers"]).referenced_datasets] == [
            "orders", "customers"]
