"""Reachability and shortest routes over the bidirectional join multigraph (``slayer.core.join_search``).

Every declared edge is traversable in both directions and parallel edges between
one pair are distinct routes. ``shortest_route`` returns the executable hop-token
sequence (excluding the root); among minimal-distance routes the tie-break is
fanning hops, then the lexicographically-smallest token sequence; a hop across
unnamed parallel edges is not executable.
"""

from __future__ import annotations

from slayer.core.enums import DataType, JoinType
from slayer.core.join_search import reachable, shortest_route
from slayer.core.models import Column, ModelJoin, SlayerModel


def _m(name: str, joins: list[ModelJoin] | None = None) -> SlayerModel:
    return SlayerModel(
        name=name,
        data_source="mydb",
        sql_table=name,
        columns=[
            Column(name="id", sql="id", type=DataType.INT, primary_key=True),
            Column(name="x_id", type=DataType.INT),
        ],
        joins=joins or [],
    )


def _left(target: str) -> ModelJoin:
    return ModelJoin(target_model=target, join_pairs=[["x_id", "id"]], join_type=JoinType.LEFT)


def _inner(target: str) -> ModelJoin:
    return ModelJoin(target_model=target, join_pairs=[["x_id", "id"]], join_type=JoinType.INNER)


def _by(models: list[SlayerModel]) -> dict[str, SlayerModel]:
    return {m.name: m for m in models}


def _reach(models: list[SlayerModel], root: str) -> set[str]:
    by = _by(models)
    return set(reachable(root=by[root], models_by_name=by))


def _path(models: list[SlayerModel], root: str, target: str) -> list[str] | None:
    by = _by(models)
    route = shortest_route(root=by[root], target=target, models_by_name=by)
    return None if route is None else list(route.tokens)


class TestReachability:
    def test_left_join_reaches_both_ways(self) -> None:
        # A LEFT edge, declared once, is reachable from either endpoint.
        models = [_m("orders", [_left("customers")]), _m("customers")]
        assert _reach(models, "orders") == {"orders", "customers"}
        assert _reach(models, "customers") == {"customers", "orders"}

    def test_transitive_reachability(self) -> None:
        models = [
            _m("orders", [_left("customers")]),
            _m("customers", [_left("regions")]),
            _m("regions"),
        ]
        assert _reach(models, "orders") == {"orders", "customers", "regions"}

    def test_single_inner_edge_reachable_both_ways(self) -> None:
        # One declared INNER edge routes both directions (no mirror needed).
        models = [
            _m("orders", [_inner("order_items")]),
            _m("order_items"),
        ]
        assert _reach(models, "orders") == {"orders", "order_items"}
        assert _reach(models, "order_items") == {"order_items", "orders"}

    def test_edge_to_unknown_target_is_skipped(self) -> None:
        # Join target not in the loaded model set → edge dropped, no crash.
        models = [_m("orders", [_left("missing")])]
        assert _reach(models, "orders") == {"orders"}

    def test_cycle_is_visited_guarded(self) -> None:
        models = [_m("a", [_left("b")]), _m("b", [_left("a")])]
        assert _reach(models, "a") == {"a", "b"}


class TestShortestPath:
    def test_root_equals_target(self) -> None:
        assert _path([_m("orders")], "orders", "orders") == []

    def test_unreachable_returns_none(self) -> None:
        # A disconnected component stays unreachable.
        models = [_m("orders", [_left("customers")]), _m("customers"), _m("logs")]
        assert _path(models, "logs", "orders") is None

    def test_unnamed_parallel_edges_are_not_executable(self) -> None:
        # Two unnamed edges between a pair: routes exist but no executable
        # token disambiguates them → no route.
        models = [_m("orders", [_left("customers"), _left("customers")]), _m("customers")]
        assert _path(models, "orders", "customers") is None

    def test_single_hop(self) -> None:
        models = [_m("orders", [_left("customers")]), _m("customers")]
        assert _path(models, "orders", "customers") == ["customers"]

    def test_multi_hop(self) -> None:
        models = [
            _m("orders", [_left("customers")]),
            _m("customers", [_left("regions")]),
            _m("regions"),
        ]
        assert _path(models, "orders", "regions") == ["customers", "regions"]

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
        assert _path(models, "orders", "regions") == ["customers", "regions"]

    def test_shorter_path_beats_lexicographic(self) -> None:
        # Direct 1-hop "zzz" beats a 2-hop "aaa"->target on distance.
        models = [
            _m("root", [_left("zzz"), _left("aaa")]),
            _m("aaa", [_left("zzz")]),
            _m("zzz"),
        ]
        assert _path(models, "root", "zzz") == ["zzz"]

    def test_inner_reverse_path_over_single_edge(self) -> None:
        # The reverse direction routes over the one declared edge.
        models = [
            _m("orders", [_inner("order_items")]),
            _m("order_items"),
        ]
        assert _path(models, "order_items", "orders") == ["orders"]
