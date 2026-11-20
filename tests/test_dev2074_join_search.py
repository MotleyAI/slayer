"""The shared relationship-graph search layer ``slayer.core.relationship_search`` (design D1–D4)."""

from __future__ import annotations

import pytest

from slayer.core import relationship_search
from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.relationship_search import (
    HopFilter,
    Route,
    UnaddressablePair,
    addressable_hops,
    blocking_pairs,
    count_routes,
    distances,
    paths_from,
    reachable,
    routes,
    shortest_route,
    shortest_routes,
    unaddressable_pairs,
)
from slayer.core.relationship_walker import neighbors, observe_traversals, resolve_hop
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.time_spine import TIME_SPINE_DATASET, spine_dataset

M2O = RelationshipCardinality.MANY_TO_ONE


def _m(
    name: str, *, joins: list[DatasetRelationship] | None = None, extra: list[DatasetField] | None = None,
    default_time_dimension: str | None = None,
) -> SlayerDataset:
    cols = [
        DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
        DatasetField(name="fk", datatype=DataType.INTEGER),
        DatasetField(name="fk2", datatype=DataType.INTEGER),
        DatasetField(name="grp", datatype=DataType.STRING),
        *(extra or []),
    ]
    return SlayerDataset(
        name=name, data_source="ds", sql_table=name, fields=cols, relationships=joins or [],
        default_time_dimension=default_time_dimension,
    )


def _to(target: str, *, name: str | None = None, src: str = "fk", tgt: str = "id",
        cardinality: RelationshipCardinality | None = None) -> DatasetRelationship:
    return DatasetRelationship(to=target, from_columns=[src], to_columns=[tgt], name=name, cardinality=cardinality)


def _by(*models: SlayerDataset) -> dict[str, SlayerDataset]:
    return {m.name: m for m in models}


def _named_two_cycle() -> dict[str, SlayerDataset]:
    """``a.to_b`` (a→b m:1) and ``b.to_a`` (b→a m:1): two named edges between one pair."""
    return _by(
        _m("a", joins=[_to("b", name="to_b", cardinality=M2O)]),
        _m("b", joins=[_to("a", name="to_a", cardinality=M2O)]),
    )


def _unnamed_parallel() -> dict[str, SlayerDataset]:
    """``orders`` declares two unnamed relationships onto ``addresses``."""
    return _by(
        _m("orders", joins=[_to("addresses"), _to("addresses", src="fk2")]),
        _m("addresses"),
    )


def _tok(route: Route | None) -> tuple[str, ...] | None:
    return None if route is None else route.tokens


class TestAddressability:
    def test_single_named_edge_emits_its_name(self) -> None:
        models = _by(_m("customers", joins=[_to("regions", name="hr")]), _m("regions"))
        route = shortest_route(root=models["customers"], target="regions", datasets_by_name=models)
        assert _tok(route) == ("hr",)
        hops = addressable_hops(dataset=models["customers"], datasets_by_name=models)
        assert [(e.name, e.to) for e in hops] == [("hr", "regions")]

    def test_reverse_traversal_of_a_named_edge_emits_its_name(self) -> None:
        models = _by(_m("customers", joins=[_to("regions", name="hr")]), _m("regions"))
        assert _tok(shortest_route(root=models["regions"], target="customers",
                                   datasets_by_name=models)) == ("hr",)

    def test_unnamed_edge_emits_target_model(self) -> None:
        models = _by(_m("orders", joins=[_to("customers")]), _m("customers"))
        assert _tok(shortest_route(root=models["orders"], target="customers",
                                   datasets_by_name=models)) == ("customers",)

    def test_every_emitted_token_resolves_to_the_edge_traversed(self) -> None:
        models = _by(
            _m("orders", joins=[_to("customers", name="buyer"), _to("customers", src="fk2", name="seller")]),
            _m("customers", joins=[_to("regions")]),
            _m("regions"),
        )
        for hop in addressable_hops(dataset=models["orders"], datasets_by_name=models):
            token = hop.name or hop.to
            assert resolve_hop(current=models["orders"], token=token, datasets_by_name=models) == hop

    def test_unnamed_parallel_pair_is_unaddressable_and_reported(self) -> None:
        models = _unnamed_parallel()
        assert addressable_hops(dataset=models["orders"], datasets_by_name=models) == []
        assert shortest_route(root=models["orders"], target="addresses", datasets_by_name=models) is None
        pairs = unaddressable_pairs(datasets_by_name=models)
        assert len(pairs) == 1
        pair = pairs[0]
        assert isinstance(pair, UnaddressablePair)
        assert {pair.source_dataset, pair.to} == {"orders", "addresses"}
        assert len(pair.edges) == 2

    def test_named_sibling_does_not_rescue_an_unnamed_parallel_edge(self) -> None:
        models = _by(
            _m("orders", joins=[_to("addresses", name="billing"), _to("addresses", src="fk2")]),
            _m("addresses"),
        )
        hops = addressable_hops(dataset=models["orders"], datasets_by_name=models)
        assert [h.name for h in hops] == ["billing"]
        pairs = unaddressable_pairs(datasets_by_name=models)
        assert len(pairs) == 1
        assert [e.name for e in pairs[0].edges] == [None]

    def test_named_parallel_pair_is_addressable(self) -> None:
        models = _by(
            _m("orders", joins=[_to("addresses", name="billing"), _to("addresses", src="fk2", name="shipping")]),
            _m("addresses"),
        )
        hops = addressable_hops(dataset=models["orders"], datasets_by_name=models)
        assert sorted(h.name for h in hops if h.name) == ["billing", "shipping"]
        assert unaddressable_pairs(datasets_by_name=models) == []

    def test_edge_name_shadowing_a_model_makes_the_bare_edge_unaddressable(self) -> None:
        models = _by(
            _m("mid", joins=[_to("leaf"), _to("other", src="fk2", name="leaf")]),
            _m("leaf"),
            _m("other"),
        )
        route = shortest_route(root=models["mid"], target="leaf", datasets_by_name=models)
        assert route is None
        assert _tok(shortest_route(root=models["mid"], target="other", datasets_by_name=models)) == ("leaf",)
        pairs = unaddressable_pairs(datasets_by_name=models)
        assert [{p.source_dataset, p.to} for p in pairs] == [{"mid", "leaf"}]

    def test_stage_spelling_shadowing_a_model(self) -> None:
        stage = _m("__stage_1", joins=[_to("orders", cardinality=M2O)]).with_spelling("customers")
        models = _by(_m("orders", joins=[_to("customers", src="fk2")]), _m("customers"), stage)
        assert _tok(shortest_route(root=models["orders"], target="__stage_1",
                                   datasets_by_name=models)) == ("customers",)
        assert shortest_route(root=models["orders"], target="customers", datasets_by_name=models) is None
        pairs = unaddressable_pairs(datasets_by_name=models)
        assert [{p.source_dataset, p.to} for p in pairs] == [{"orders", "customers"}]


class TestSpineRule:
    def _two_facts(self) -> dict[str, SlayerDataset]:
        """Axes on ``orders`` and ``returns``; ``lines`` reaches ``orders`` to-one; ``customers`` is detached."""
        ts = [DatasetField(name="at", datatype=DataType.DATETIME)]
        orders = _m("orders", extra=ts, default_time_dimension="at", joins=[_to("customers", cardinality=M2O)])
        returns = _m("returns", extra=ts, default_time_dimension="at")
        lines = _m("lines", joins=[_to("orders", cardinality=M2O)])
        return _by(orders, returns, lines, _m("customers"), spine_dataset(data_source="ds"))

    def test_routes_may_end_at_the_spine(self) -> None:
        models = self._two_facts()
        route = shortest_route(root=models["orders"], target=TIME_SPINE_DATASET, datasets_by_name=models)
        assert route is not None
        assert len(route.edges) == 1
        assert route.fanning == 0

    def test_routes_never_cross_the_spine(self) -> None:
        models = self._two_facts()
        assert "returns" not in reachable(root=models["orders"], datasets_by_name=models)
        assert "returns" not in distances(root=models["orders"], datasets_by_name=models)
        assert shortest_route(root=models["orders"], target="returns", datasets_by_name=models) is None
        assert routes(root=models["orders"], target="returns", datasets_by_name=models) == []
        assert count_routes(root=models["orders"], target="returns", datasets_by_name=models) == 0
        assert all(r.edges[-1].to != "returns"
                   for r in paths_from(root=models["orders"], datasets_by_name=models, depth=4))

    def test_routes_may_begin_at_the_spine(self) -> None:
        models = self._two_facts()
        spine = models[TIME_SPINE_DATASET]
        assert distances(root=spine, datasets_by_name=models) == {
            TIME_SPINE_DATASET: 0, "orders": 1, "returns": 1, "customers": 1, "lines": 2,
        }
        assert _tok(shortest_route(root=spine, target="lines", datasets_by_name=models)) == ("orders", "lines")

    def test_detached_dataset_meets_the_spine_as_a_product(self) -> None:
        models = self._two_facts()
        route = shortest_route(root=models[TIME_SPINE_DATASET], target="customers", datasets_by_name=models)
        assert _tok(route) == ("customers",)
        assert route is not None
        assert route.fanning == 1
        back = shortest_route(root=models["customers"], target=TIME_SPINE_DATASET, datasets_by_name=models)
        assert _tok(back) == (TIME_SPINE_DATASET,)

    def test_population_factor_reaches_the_spine_by_the_product_edge(self) -> None:
        models = self._two_facts()
        models["orders"] = models["orders"].as_population_factor()
        route = shortest_route(root=models["orders"], target=TIME_SPINE_DATASET, datasets_by_name=models)
        assert _tok(route) == (TIME_SPINE_DATASET,)
        assert route is not None
        assert route.fanning == 1


class TestTieBreak:
    def test_named_two_cycle_picks_the_to_one_edge(self) -> None:
        models = _named_two_cycle()
        route = shortest_route(root=models["a"], target="b", datasets_by_name=models)
        assert _tok(route) == ("to_b",)
        assert route is not None
        assert route.fanning == 0
        back = shortest_route(root=models["b"], target="a", datasets_by_name=models)
        assert _tok(back) == ("to_a",)

    def test_fewer_hops_beat_fewer_fanning_hops(self) -> None:
        # root→t directly fans out; root→mid→t is fan-out-free but longer.
        models = _by(
            _m("root", joins=[_to("t", src="grp", tgt="grp"), _to("mid", cardinality=M2O)]),
            _m("mid", joins=[_to("t", cardinality=M2O)]),
            _m("t"),
        )
        assert _tok(shortest_route(root=models["root"], target="t", datasets_by_name=models)) == ("t",)

    def test_equal_length_prefers_fewer_fanning_hops_over_tokens(self) -> None:
        # Two 2-hop routes: via "aaa" fans out at the second hop, via "zzz" never does.
        models = _by(
            _m("root", joins=[_to("aaa", cardinality=M2O), _to("zzz", src="fk2", cardinality=M2O)]),
            _m("aaa", joins=[_to("t", src="grp", tgt="grp")]),
            _m("zzz", joins=[_to("t", cardinality=M2O)]),
            _m("t"),
        )
        route = shortest_route(root=models["root"], target="t", datasets_by_name=models)
        assert _tok(route) == ("zzz", "t")
        assert route is not None
        assert route.fanning == 0

    def test_equal_length_equal_fanning_picks_smallest_tokens(self) -> None:
        models = _by(
            _m("root", joins=[_to("zzz", cardinality=M2O), _to("aaa", src="fk2", cardinality=M2O)]),
            _m("aaa", joins=[_to("t", cardinality=M2O)]),
            _m("zzz", joins=[_to("t", cardinality=M2O)]),
            _m("t"),
        )
        assert _tok(shortest_route(root=models["root"], target="t", datasets_by_name=models)) == ("aaa", "t")

    def test_to_one_filter_skips_fanning_hops(self) -> None:
        models = _by(
            _m("root", joins=[_to("t", src="grp", tgt="grp"), _to("mid", cardinality=M2O)]),
            _m("mid", joins=[_to("t", cardinality=M2O)]),
            _m("t"),
        )
        route = shortest_route(root=models["root"], target="t", datasets_by_name=models,
                               hop_filter=HopFilter.TO_ONE)
        assert _tok(route) == ("mid", "t")
        assert distances(root=models["root"], datasets_by_name=models, hop_filter=HopFilter.TO_ONE) == {
            "root": 0, "mid": 1, "t": 2,
        }
        assert reachable(root=models["t"], datasets_by_name=models, hop_filter=HopFilter.TO_ONE) == {"t"}

    def test_root_is_the_empty_route(self) -> None:
        models = _named_two_cycle()
        route = shortest_route(root=models["a"], target="a", datasets_by_name=models)
        assert route is not None
        assert route.tokens == ()
        assert route.edges == ()
        assert route.fanning == 0

    def test_shortest_routes_covers_every_reachable_model(self) -> None:
        models = _by(
            _m("orders", joins=[_to("customers", cardinality=M2O)]),
            _m("customers", joins=[_to("regions", name="hr", cardinality=M2O)]),
            _m("regions"),
            _m("island"),
        )
        got = {name: r.tokens for name, r in
               shortest_routes(root=models["orders"], datasets_by_name=models).items()}
        assert got == {"orders": (), "customers": ("customers",), "regions": ("customers", "hr")}


class TestRoutes:
    def _diamond(self) -> dict[str, SlayerDataset]:
        """root reaches t directly and via ``mid``; a dead-end chain hangs off root."""
        return _by(
            _m("root", joins=[_to("t", cardinality=M2O), _to("mid", src="fk2", cardinality=M2O),
                              _to("dead1", src="grp", tgt="grp")]),
            _m("mid", joins=[_to("t", cardinality=M2O)]),
            _m("t"),
            _m("dead1", joins=[_to("dead2", cardinality=M2O)]),
            _m("dead2", joins=[_to("dead3", cardinality=M2O)]),
            _m("dead3"),
        )

    def test_routes_are_ordered_by_the_tie_break(self) -> None:
        models = self._diamond()
        got = routes(root=models["root"], target="t", datasets_by_name=models, cap=5)
        assert [r.tokens for r in got] == [("t",), ("mid", "t")]

    def test_cap_bounds_the_enumeration(self) -> None:
        models = self._diamond()
        assert len(routes(root=models["root"], target="t", datasets_by_name=models, cap=1)) == 1

    def test_shortest_only_keeps_minimal_routes(self) -> None:
        models = self._diamond()
        got = routes(root=models["root"], target="t", datasets_by_name=models, cap=5, shortest_only=True)
        assert [r.tokens for r in got] == [("t",)]

    def test_shortest_only_keeps_every_equal_minimal_route(self) -> None:
        models = _by(
            _m("root", joins=[_to("aaa", cardinality=M2O), _to("zzz", src="fk2", cardinality=M2O)]),
            _m("aaa", joins=[_to("t", cardinality=M2O)]),
            _m("zzz", joins=[_to("t", cardinality=M2O)]),
            _m("t"),
        )
        got = routes(root=models["root"], target="t", datasets_by_name=models, cap=5, shortest_only=True)
        assert [r.tokens for r in got] == [("aaa", "t"), ("zzz", "t")]

    def test_shortest_only_with_no_route_is_empty(self) -> None:
        models = _unnamed_parallel()
        got = routes(root=models["orders"], target="addresses", datasets_by_name=models, cap=5, shortest_only=True)
        assert got == []

    def test_to_one_filter_applies_to_route_enumeration(self) -> None:
        models = self._diamond()
        models["root"] = _m("root", joins=[_to("t", src="grp", tgt="grp"),
                                           _to("mid", src="fk2", cardinality=M2O)])
        got = routes(root=models["root"], target="t", datasets_by_name=models, cap=5,
                     hop_filter=HopFilter.TO_ONE)
        assert [r.tokens for r in got] == [("mid", "t")]

    def test_relevance_pruning_never_expands_a_dead_end(self) -> None:
        models = self._diamond()
        with observe_traversals() as seen:
            routes(root=models["root"], target="t", datasets_by_name=models, cap=5)
        assert "dead2" not in seen
        assert "dead3" not in seen

    def test_unaddressable_edges_are_not_routed(self) -> None:
        models = _unnamed_parallel()
        assert routes(root=models["orders"], target="addresses", datasets_by_name=models, cap=5) == []

    def test_cycles_terminate(self) -> None:
        models = _by(
            _m("x", joins=[_to("y", cardinality=M2O)]),
            _m("y", joins=[_to("z", cardinality=M2O)]),
            _m("z", joins=[_to("x", cardinality=M2O)]),
        )
        got = routes(root=models["x"], target="z", datasets_by_name=models, cap=5)
        assert [r.tokens for r in got] == [("z",), ("y", "z")]


class TestBlockingPairs:
    def _shop(self) -> dict[str, SlayerDataset]:
        """``shop → orders ⇉ addresses → cities``."""
        return _by(
            _m("shop", joins=[_to("orders")]),
            _m("orders", joins=[_to("addresses"), _to("addresses", src="fk2")]),
            _m("addresses", joins=[_to("cities")]),
            _m("cities"),
        )

    def test_pair_on_the_way_to_the_target_is_reported(self) -> None:
        models = self._shop()
        for target in ("addresses", "cities"):
            pairs = blocking_pairs(root=models["shop"], target=target, datasets_by_name=models)
            assert [{p.source_dataset, p.to} for p in pairs] == [{"orders", "addresses"}]

    def test_reached_target_reports_nothing(self) -> None:
        models = self._shop()
        assert blocking_pairs(root=models["shop"], target="orders", datasets_by_name=models) == []

    def test_pair_reaching_the_target_only_through_the_spine_is_not_reported(self) -> None:
        ts = [DatasetField(name="at", datatype=DataType.DATETIME)]
        models = _by(
            _m("a", joins=[_to("b"), _to("b", src="fk2")]),
            _m("b", extra=ts, default_time_dimension="at"),
            _m("c", extra=ts, default_time_dimension="at"),
            spine_dataset(data_source="ds"),
        )
        assert blocking_pairs(root=models["a"], target="c", datasets_by_name=models) == []

    def test_neighbor_reads_do_not_grow_with_the_pair_count(self, monkeypatch: pytest.MonkeyPatch) -> None:
        hub = _m("hub", joins=[_to(f"m{i}", src="grp", tgt="grp") for i in range(3)])
        spokes = [_m(f"m{i}", joins=[_to(f"leaf{i}"), _to(f"leaf{i}", src="fk2")]) for i in range(3)]
        models = _by(hub, *spokes, _m("leaf0", joins=[_to("far")]), _m("leaf1"), _m("leaf2"), _m("far"))
        calls: dict[str, int] = {}

        def counting(*, dataset: SlayerDataset, datasets_by_name: dict[str, SlayerDataset]):
            calls[dataset.name] = calls.get(dataset.name, 0) + 1
            return neighbors(dataset=dataset, datasets_by_name=datasets_by_name)

        monkeypatch.setattr(relationship_search, "neighbors", counting)
        pairs = blocking_pairs(root=models["hub"], target="far", datasets_by_name=models)
        assert len(pairs) == 3
        assert max(calls.values()) <= 3


class TestCountRoutes:
    def test_parallel_edges_count_as_distinct_routes(self) -> None:
        for models in (_unnamed_parallel(), _by(
            _m("orders", joins=[_to("addresses", name="billing"), _to("addresses", src="fk2", name="shipping")]),
            _m("addresses"),
        )):
            assert count_routes(root=models["orders"], target="addresses", datasets_by_name=models, cap=5) == 2
            assert count_routes(root=models["addresses"], target="orders", datasets_by_name=models, cap=5) == 2

    def test_unaddressable_pair_on_the_only_route_is_ambiguous(self) -> None:
        models = _by(
            _m("root", joins=[_to("orders", cardinality=M2O)]),
            _m("orders", joins=[_to("addresses"), _to("addresses", src="fk2")]),
            _m("addresses"),
        )
        assert count_routes(root=models["root"], target="addresses", datasets_by_name=models) == 2

    def test_unique_unreachable_and_trivial(self) -> None:
        models = _by(_m("orders", joins=[_to("customers")]), _m("customers"), _m("island"))
        assert count_routes(root=models["orders"], target="customers", datasets_by_name=models) == 1
        assert count_routes(root=models["customers"], target="orders", datasets_by_name=models) == 1
        assert count_routes(root=models["orders"], target="island", datasets_by_name=models) == 0
        assert count_routes(root=models["orders"], target="orders", datasets_by_name=models) == 1

    def test_count_is_capped(self) -> None:
        models = _by(
            _m("root", joins=[_to("t"), _to("t", src="fk2"), _to("t", src="grp", tgt="grp")]),
            _m("t"),
        )
        assert count_routes(root=models["root"], target="t", datasets_by_name=models, cap=2) == 2
        assert count_routes(root=models["root"], target="t", datasets_by_name=models, cap=5) == 3

    def test_all_simple_routes_count_not_only_shortest(self) -> None:
        models = TestRoutes()._diamond()
        assert count_routes(root=models["root"], target="t", datasets_by_name=models, cap=5) == 2


class TestPathsFrom:
    def _chain(self) -> dict[str, SlayerDataset]:
        return _by(
            _m("a", joins=[_to("b", cardinality=M2O)]),
            _m("b", joins=[_to("c", name="to_c", cardinality=M2O)]),
            _m("c", joins=[_to("d", cardinality=M2O)]),
            _m("d"),
        )

    def test_depth_bounds_the_listing(self) -> None:
        models = self._chain()
        got = [r.tokens for r in paths_from(root=models["a"], datasets_by_name=models, depth=2)]
        assert got == [("b",), ("b", "to_c")]
        assert paths_from(root=models["a"], datasets_by_name=models, depth=0) == []

    def test_diamond_yields_one_entry_per_simple_path(self) -> None:
        models = TestRoutes()._diamond()
        to_t = [r.tokens for r in paths_from(root=models["root"], datasets_by_name=models, depth=3)
                if r.edges[-1].to == "t"]
        assert sorted(to_t) == [("mid", "t"), ("t",)]

    def test_paths_never_revisit_a_model(self) -> None:
        models = _named_two_cycle()
        got = [r.tokens for r in paths_from(root=models["a"], datasets_by_name=models, depth=3)]
        assert sorted(got) == [("to_a",), ("to_b",)]

    def test_passable_excludes_a_model_and_everything_behind_it(self) -> None:
        models = self._chain()
        got = [r.tokens for r in paths_from(root=models["a"], datasets_by_name=models, depth=3,
                                             passable=lambda m: m.name != "c")]
        assert got == [("b",)]

    def test_unaddressable_edges_are_not_listed(self) -> None:
        models = _unnamed_parallel()
        assert paths_from(root=models["orders"], datasets_by_name=models, depth=3) == []


class TestDeterminism:
    @pytest.mark.parametrize("reverse", [False, True])
    def test_answers_do_not_depend_on_model_order(self, reverse: bool) -> None:
        base = _by(
            _m("root", joins=[_to("aaa", cardinality=M2O), _to("zzz", src="fk2", cardinality=M2O),
                              _to("t", src="grp", tgt="grp")]),
            _m("aaa", joins=[_to("t", cardinality=M2O)]),
            _m("zzz", joins=[_to("t", cardinality=M2O)]),
            _m("t", joins=[_to("leaf", name="lf", cardinality=M2O)]),
            _m("leaf"),
        )
        names = sorted(base, reverse=reverse)
        models = {n: base[n] for n in names}
        root = models["root"]
        assert _tok(shortest_route(root=root, target="leaf", datasets_by_name=models)) == ("t", "lf")
        assert [r.tokens for r in routes(root=root, target="t", datasets_by_name=models, cap=5)] == [
            ("t",), ("aaa", "t"), ("zzz", "t"),
        ]
        assert [r.tokens for r in paths_from(root=root, datasets_by_name=models, depth=1)] == [
            ("aaa",), ("t",), ("zzz",),
        ]
