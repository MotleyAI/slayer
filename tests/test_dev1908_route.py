"""DEV-1908 — the determination route steps back only to the common prefix (D9).

Between a model on one join path and a column on a prefix of that same path,
``attributable_from_root`` / ``reroot_from_root`` / ``broadcast_reason`` walk back
only to the two paths' longest common prefix (the reverse suffix), never a round
trip through the query root. A to-one reverse suffix is attributable and reroots
to the suffix path; a fanning one is reported as the fanning hop, never
"unreachable". Non-prefix-sharing pairs keep the round-trip route byte-identical.
Sharing cases fail on the current tree (refused as a revisit); non-sharing cases
are green pins.
"""

from __future__ import annotations

from slayer.core.enums import JoinCardinality
from slayer.core.keys import ColumnKey
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.join_safety import (
    attributable_from_root,
    broadcast_reason,
    reroot_from_root,
)


def _m(name, cols, joins=None):
    return SlayerModel(name=name, data_source="ds", sql_table=name,
                       columns=[Column(name=c) for c in cols], joins=joins or [])


def _graph(*, named_event: bool = False) -> dict[str, SlayerModel]:
    """orders → customers → regions; regions → region_events (1:N, reverse to-one)
    and regions → countries (m:1, reverse fans); orders → stores (sibling)."""
    orders = _m("orders", ["id", "customer_id", "store_id"], [
        ModelJoin(target_model="customers", join_pairs=[["customer_id", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE),
        ModelJoin(target_model="stores", join_pairs=[["store_id", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE)])
    customers = _m("customers", ["id", "region_id"], [
        ModelJoin(target_model="regions", join_pairs=[["region_id", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE)])
    regions = _m("regions", ["id", "pop", "country_id"], [
        ModelJoin(target_model="region_events", join_pairs=[["id", "region_id"]],
                  cardinality=JoinCardinality.ONE_TO_MANY,
                  name="evt" if named_event else None),
        ModelJoin(target_model="countries", join_pairs=[["country_id", "id"]],
                  cardinality=JoinCardinality.MANY_TO_ONE)])
    return {m.name: m for m in (orders, customers, regions,
                                _m("region_events", ["id", "region_id", "value"]),
                                _m("countries", ["id", "gdp"]),
                                _m("stores", ["id", "rent"]))}


class TestToOneReverseSuffix:
    def test_attributable_over_the_reverse_hop(self):
        M = _graph()
        assert attributable_from_root(
            host_path=("customers", "regions"),
            target_path=("customers", "regions", "region_events"),
            root_model=M["region_events"], models_by_name=M, host_name="orders") is True

    def test_key_reroots_to_the_reverse_suffix(self):
        M = _graph()
        rerooted = reroot_from_root(
            ColumnKey(path=("customers", "regions"), leaf="pop"),
            target_path=("customers", "regions", "region_events"),
            root_model=M["region_events"], models_by_name=M, host_name="orders")
        assert rerooted == ColumnKey(path=("regions",), leaf="pop")

    def test_attributable_through_a_named_edge_in_the_suffix(self):
        M = _graph(named_event=True)
        assert attributable_from_root(
            host_path=("customers", "regions"),
            target_path=("customers", "regions", "evt"),
            root_model=M["region_events"], models_by_name=M, host_name="orders") is True

    def test_named_edge_reroots_to_the_edge_token_not_the_model_name(self):
        M = _graph(named_event=True)
        rerooted = reroot_from_root(
            ColumnKey(path=("customers", "regions"), leaf="pop"),
            target_path=("customers", "regions", "evt"),
            root_model=M["region_events"], models_by_name=M, host_name="orders")
        assert rerooted == ColumnKey(path=("evt",), leaf="pop")


class TestFanningReverseSuffix:
    def test_not_attributable(self):
        M = _graph()
        assert attributable_from_root(
            host_path=("customers", "regions"),
            target_path=("customers", "regions", "countries"),
            root_model=M["countries"], models_by_name=M, host_name="orders") is False

    def test_reason_names_the_fanning_hop_not_unreachable(self):
        M = _graph()
        reason = broadcast_reason(
            host_path=("customers", "regions"),
            target_path=("customers", "regions", "countries"),
            root_model=M["countries"], models_by_name=M, host_name="orders")
        assert "regions" in reason
        assert "fanning" in reason or "unproven" in reason
        assert "unreachable" not in reason

    def test_two_hop_prefix_side_dimension_not_attributable(self):
        # `customers.tier` from a countries-rooted aggregate: reverse suffix is
        # countries → regions (fans) → customers, a TWO-hop reverse suffix.
        M = _graph()
        assert attributable_from_root(
            host_path=("customers",),
            target_path=("customers", "regions", "countries"),
            root_model=M["countries"], models_by_name=M, host_name="orders") is False

    def test_two_hop_prefix_side_reason_names_the_fanning_hop(self):
        M = _graph()
        reason = broadcast_reason(
            host_path=("customers",),
            target_path=("customers", "regions", "countries"),
            root_model=M["countries"], models_by_name=M, host_name="orders")
        assert "regions" in reason
        assert "fanning" in reason or "unproven" in reason
        assert "unreachable" not in reason


class TestNonSharingPairsUnchanged:
    """Sibling branches sharing only the root keep the round-trip route (green pin)."""

    def test_sibling_stays_unattributable(self):
        M = _graph()
        assert attributable_from_root(
            host_path=("stores",), target_path=("customers",),
            root_model=M["customers"], models_by_name=M, host_name="orders") is False

    def test_sibling_reason_is_unchanged(self):
        M = _graph()
        reason = broadcast_reason(
            host_path=("stores",), target_path=("customers",),
            root_model=M["customers"], models_by_name=M, host_name="orders")
        # The reverse customers → orders hop fans: the classification is the
        # fanning hop (naming orders), never "unreachable".
        assert "fanning" in reason or "unproven" in reason
        assert "orders" in reason
        assert "unreachable" not in reason
