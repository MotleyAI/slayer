"""Unit tests for the pure ``JoinGraph`` routing primitive (DEV-1626/DEV-1853).

``JoinGraph`` builds an in-memory undirected multigraph from a set of models'
declared joins: every declared edge is traversable in both directions, and
parallel edges between the same pair are distinct routes. The primitive is
join-type-agnostic: it just reads declared joins.

``shortest_path`` returns the executable hop-token sequence (excluding the
root), picking the lexicographically-smallest sequence among all
minimal-distance paths so diamond graphs resolve deterministically; a hop
across unnamed parallel edges is not executable.
"""

from __future__ import annotations

from slayer.core.enums import DataType, JoinType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.engine.join_graph import JoinGraph


def _m(name: str, joins: list[ModelJoin] | None = None) -> SlayerModel:
    return SlayerModel(
        name=name,
        data_source="mydb",
        sql_table=name,
        columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
        joins=joins or [],
    )


def _left(target: str) -> ModelJoin:
    return ModelJoin(target_model=target, join_pairs=[["x_id", "id"]], join_type=JoinType.LEFT)


def _inner(target: str) -> ModelJoin:
    return ModelJoin(target_model=target, join_pairs=[["x_id", "id"]], join_type=JoinType.INNER)


class TestReachability:
    def test_left_join_reaches_both_ways(self) -> None:
        # DEV-1853 divergences.md class (d): a LEFT edge, declared once, is
        # reachable from either endpoint (was forward-only).
        models = [_m("orders", [_left("customers")]), _m("customers")]
        g = JoinGraph.build_from_models(models)
        assert g.reachable_from("orders") == {"orders", "customers"}
        assert g.reachable_from("customers") == {"customers", "orders"}

    def test_transitive_reachability(self) -> None:
        models = [
            _m("orders", [_left("customers")]),
            _m("customers", [_left("regions")]),
            _m("regions"),
        ]
        g = JoinGraph.build_from_models(models)
        assert g.reachable_from("orders") == {"orders", "customers", "regions"}

    def test_single_inner_edge_reachable_both_ways(self) -> None:
        # One declared INNER edge routes both directions (no mirror needed).
        models = [
            _m("orders", [_inner("order_items")]),
            _m("order_items"),
        ]
        g = JoinGraph.build_from_models(models)
        assert g.reachable_from("orders") == {"orders", "order_items"}
        assert g.reachable_from("order_items") == {"order_items", "orders"}

    def test_edge_to_unknown_target_is_skipped(self) -> None:
        # Join target not in the loaded model set → edge dropped, no crash.
        models = [_m("orders", [_left("missing")])]
        g = JoinGraph.build_from_models(models)
        assert g.reachable_from("orders") == {"orders"}

    def test_cycle_is_visited_guarded(self) -> None:
        models = [_m("a", [_left("b")]), _m("b", [_left("a")])]
        g = JoinGraph.build_from_models(models)
        assert g.reachable_from("a") == {"a", "b"}


class TestShortestPath:
    def test_root_equals_target(self) -> None:
        g = JoinGraph.build_from_models([_m("orders")])
        assert g.shortest_path("orders", "orders") == []

    def test_unreachable_returns_none(self) -> None:
        # A disconnected component stays unreachable (reverse-only no longer
        # counts as disconnected — DEV-1853).
        g = JoinGraph.build_from_models(
            [_m("orders", [_left("customers")]), _m("customers"), _m("logs")]
        )
        assert g.shortest_path("logs", "orders") is None

    def test_unnamed_parallel_edges_are_not_executable(self) -> None:
        # Two unnamed edges between a pair: routes exist but no executable
        # token disambiguates them → shortest_path reports unreachable.
        models = [_m("orders", [_left("customers"), _left("customers")]), _m("customers")]
        g = JoinGraph.build_from_models(models)
        assert g.shortest_path("orders", "customers") is None

    def test_single_hop(self) -> None:
        g = JoinGraph.build_from_models([_m("orders", [_left("customers")]), _m("customers")])
        assert g.shortest_path("orders", "customers") == ["customers"]

    def test_multi_hop(self) -> None:
        models = [
            _m("orders", [_left("customers")]),
            _m("customers", [_left("regions")]),
            _m("regions"),
        ]
        g = JoinGraph.build_from_models(models)
        assert g.shortest_path("orders", "regions") == ["customers", "regions"]

    def test_diamond_picks_shortest_then_lexicographic(self) -> None:
        # orders -> customers -> regions AND orders -> warehouses -> regions.
        # Both paths length 2; lexicographically smallest hop sequence wins:
        # ["customers", "regions"] < ["warehouses", "regions"].
        models = [
            _m("orders", [_left("customers"), _left("warehouses")]),
            _m("customers", [_left("regions")]),
            _m("warehouses", [_left("regions")]),
            _m("regions"),
        ]
        g = JoinGraph.build_from_models(models)
        assert g.shortest_path("orders", "regions") == ["customers", "regions"]

    def test_shorter_path_beats_lexicographic(self) -> None:
        # Direct 1-hop "zzz" beats a 2-hop "aaa"->target on distance.
        models = [
            _m("root", [_left("zzz"), _left("aaa")]),
            _m("aaa", [_left("zzz")]),
            _m("zzz"),
        ]
        g = JoinGraph.build_from_models(models)
        assert g.shortest_path("root", "zzz") == ["zzz"]

    def test_inner_reverse_path_over_single_edge(self) -> None:
        # DEV-1853: the reverse direction routes over the one declared edge.
        models = [
            _m("orders", [_inner("order_items")]),
            _m("order_items"),
        ]
        g = JoinGraph.build_from_models(models)
        assert g.shortest_path("order_items", "orders") == ["orders"]
