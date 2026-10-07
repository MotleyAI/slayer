"""FK cycles must not disable rollup for the whole schema.

A cycle (e.g. transactions -> coupon_usages -> transactions) previously made
ingestion skip join generation for every table. Now only the back-edges that
close a cycle are dropped; every other FK edge keeps producing joins.
"""

from slayer.engine.ingestion import _break_cycles, _compute_transitive_closure


def test_acyclic_graph_untouched() -> None:
    graph = {"a": {"b"}, "b": {"c"}, "c": set()}
    assert _break_cycles(graph) == []
    assert graph == {"a": {"b"}, "b": {"c"}, "c": set()}


def test_two_node_cycle_drops_one_edge_keeps_the_other() -> None:
    graph = {"coupon_usages": {"transactions"}, "transactions": {"coupon_usages"}}
    dropped = _break_cycles(graph)
    assert dropped == [("transactions", "coupon_usages")]
    # The spanning edge survives: coupon_usages still reaches transactions.
    assert _compute_transitive_closure(graph, "coupon_usages") == {"transactions"}
    assert _compute_transitive_closure(graph, "transactions") == set()


def test_self_loop_dropped() -> None:
    graph = {"a": {"a", "b"}, "b": set()}
    assert _break_cycles(graph) == [("a", "a")]
    assert _compute_transitive_closure(graph, "a") == {"b"}


def test_unrelated_edges_survive_a_cycle_elsewhere() -> None:
    graph = {
        "a": {"b"},
        "b": {"a"},
        "orders": {"users"},
        "users": set(),
    }
    dropped = _break_cycles(graph)
    assert dropped == [("b", "a")]
    assert _compute_transitive_closure(graph, "orders") == {"users"}


def test_deterministic_for_the_same_schema() -> None:
    def fresh() -> dict[str, set[str]]:
        return {"x": {"y"}, "y": {"z"}, "z": {"x"}}

    first = _break_cycles(fresh())
    for _ in range(5):
        assert _break_cycles(fresh()) == first
    # The two tree edges stay; only the closing edge goes.
    graph = fresh()
    _break_cycles(graph)
    assert _compute_transitive_closure(graph, "x") == {"y", "z"}
