"""DEV-1853 — which side declares an edge is storage trivia.

The same semantic edge declared on either model answers every traversal,
reachability, SQL, and value question identically (byte-identical SQL on
unnamed paths). This is the parity that lets stored mirror pairs collapse to
one edge.
"""

from __future__ import annotations

import tempfile

import pytest

from slayer.core.enums import JoinType
from slayer.core.query import SlayerQuery
from slayer.engine.join_graph import JoinGraph
from slayer.engine.join_safety import safe_reachable

from tests._dev1853_fixtures import chain_engine, chain_models

QUERIES = {
    "reverse_dims": SlayerQuery(
        source_model="customers", dimensions=["name", "orders.status"]),
    "forward_dims": SlayerQuery(
        source_model="orders", dimensions=["status", "customers.name"]),
    "reverse_aggregate": SlayerQuery(
        source_model="customers", dimensions=["name"],
        measures=[{"formula": "orders.amount:sum", "name": "t"}]),
    "multi_hop": SlayerQuery(
        source_model="regions", dimensions=["name", "customers.orders.status"]),
}


def _canon_rows(resp) -> list[tuple]:
    # repr-keyed sort: row values may mix None with strings.
    return sorted((tuple(sorted(r.items())) for r in resp.data), key=repr)


@pytest.mark.parametrize("join_type", [JoinType.LEFT, JoinType.INNER])
@pytest.mark.parametrize("query_id", sorted(QUERIES))
async def test_declaring_side_gives_identical_sql_and_values(
    join_type, query_id,
) -> None:
    query = QUERIES[query_id]
    with tempfile.TemporaryDirectory() as d1, \
            tempfile.TemporaryDirectory() as d2:
        fwd = await chain_engine(d1, join_type=join_type, declare="forward")
        rev = await chain_engine(d2, join_type=join_type, declare="reverse")
        sql_f = (await fwd.execute(query, dry_run=True)).sql
        sql_r = (await rev.execute(query, dry_run=True)).sql
        assert sql_f == sql_r
        resp_f = await fwd.execute(query)
        resp_r = await rev.execute(query)
        assert _canon_rows(resp_f) == _canon_rows(resp_r)


def test_reachability_parity() -> None:
    graph_f = JoinGraph.build_from_models(chain_models(declare="forward"))
    graph_r = JoinGraph.build_from_models(chain_models(declare="reverse"))
    for node in ("orders", "customers", "regions"):
        assert graph_f.reachable_from(node) == graph_r.reachable_from(node)
    for src, dst in (("customers", "orders"), ("orders", "regions")):
        assert graph_f.count_simple_paths(src, dst) == \
            graph_r.count_simple_paths(src, dst)


def test_safety_parity() -> None:
    fwd = {m.name: m for m in chain_models(declare="forward")}
    rev = {m.name: m for m in chain_models(declare="reverse")}
    probes = [
        ("orders", ("customers",)),
        ("orders", ("customers", "regions")),
        ("customers", ("orders",)),
    ]
    for root, path in probes:
        assert safe_reachable(root=fwd[root], path=path, models_by_name=fwd) == \
            safe_reachable(root=rev[root], path=path, models_by_name=rev)
