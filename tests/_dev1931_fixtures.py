"""Shared fixtures for DEV-1931 — definition-default home resolution
(bare-vs-dotted split + host-local retention).

Extends the DEV-1832 Graph-A ``orders → customers → regions (→ region_events,
1:N fanning) / stores`` graph with custom aggregations whose non-overridden
weight defaults exercise each resolution frame:

- ``customers.wsum_host``      bare-qualified ROOT column ``orders.amount`` — a
                               genuine host-local default (self-reference to the
                               query root); homes at ``()``.
- ``customers.wsum_host_expr`` the same inside an expression (``orders.amount * 1``).
- ``customers.wsum_mixed``     owner-local ``spend`` + root-local ``orders.amount``:
                               each reference resolves in its own frame.
- ``customers.wsum_regions_pop`` owner-reachable dotted ``regions.pop`` → the
                               owner-relative ``customers.regions.pop``.
- ``customers.wsum_bare``      bare owner-local ``spend`` — never resurrected as ``()``.
- ``customers.wsum_multi``     multi-leaf expression source with a bare default.
- ``regions.wfan_widen``       one home-widening default (``customers.spend``) plus
                               one fanning default (``bad_pop``) — the F1 safety case.

Reuses the DEV-1832 seeded dual-engine harness, ``gen`` and query shorthands.
The ``ambiguous_owner_models`` graph (two unnamed parallel edges) covers the
fail-closed ambiguous-hop case.
"""

from __future__ import annotations

from typing import List

from slayer.core.enums import DataType
from slayer.core.models import (
    Aggregation,
    AggregationParam,
    Column,
    ModelJoin,
    SlayerModel,
)

from tests._dev1832_fixtures import dev1832_models


def _model(models: List[SlayerModel], name: str) -> SlayerModel:
    return next(m for m in models if m.name == name)


def dev1931_models() -> List[SlayerModel]:
    """DEV-1832 Graph A + the DEV-1931 custom aggregations. Host first (orders)."""
    models = dev1832_models()
    cust = _model(models, "customers")
    cust.aggregations.extend([
        Aggregation(
            name="wsum_host", formula="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="orders.amount")]),
        Aggregation(
            name="wsum_host_expr", formula="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="orders.amount * 1")]),
        Aggregation(
            name="wsum_mixed", formula="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="spend + orders.amount")]),
        Aggregation(
            name="wsum_regions_pop", formula="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="regions.pop")]),
        # Bare owner-local default; also reused over a multi-leaf expression source.
        Aggregation(
            name="wsum_bare", formula="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="spend")]),
        # An unreachable-from-both qualifier: fails closed under owner-first + root
        # fallback (no join `nowhere` on the owner OR the root).
        Aggregation(
            name="wsum_nowhere", formula="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="nowhere.col")]),
        # A partially-resolvable owner reference: the first hop `regions` resolves
        # from the owner customers, but the second hop `plans` is not a join of
        # regions — fails closed, never re-anchored at the root.
        Aggregation(
            name="wsum_partial", formula="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="regions.plans.fee")]),
    ])
    regions = _model(models, "regions")
    # F1: w1 widens the home to customers (root fallback), w2 crosses the fanning
    # regions → region_events hop (owner-local derived `bad_pop`).
    regions.aggregations.append(Aggregation(
        name="wfan_widen", formula="SUM({value} * {w1} * {w2})",
        params=[AggregationParam(name="w1", sql="customers.spend"),
                AggregationParam(name="w2", sql="bad_pop")]))
    return models


def ambiguous_owner_models() -> List[SlayerModel]:
    """Root ``r → o`` (to-one) and ``r → ag`` (one clean edge); ``o → ag`` via TWO
    unnamed parallel edges (ambiguous). The ``o``-declared ``wscore`` defaults its
    weight to ``ag.score``: ambiguous from the owner ``o``, it must fail closed and
    NOT silently re-anchor at the root ``r`` — where ``ag`` IS cleanly reachable, so
    a root retry would wrongly succeed."""
    r = SlayerModel(
        name="r", data_source="test", sql_table="r",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="o_id", type=DataType.INT),
                 Column(name="ag_id", type=DataType.INT)],
        joins=[ModelJoin(target_model="o", join_pairs=[["o_id", "id"]]),
               ModelJoin(target_model="ag", join_pairs=[["ag_id", "id"]])])
    o = SlayerModel(
        name="o", data_source="test", sql_table="o",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="a1", type=DataType.INT),
                 Column(name="a2", type=DataType.INT),
                 Column(name="val", type=DataType.DOUBLE)],
        joins=[ModelJoin(target_model="ag", join_pairs=[["a1", "id"]]),
               ModelJoin(target_model="ag", join_pairs=[["a2", "id"]])],
        aggregations=[Aggregation(
            name="wscore", formula="SUM({value} * {weight})",
            params=[AggregationParam(name="weight", sql="ag.score")])])
    ag = SlayerModel(
        name="ag", data_source="test", sql_table="ag",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="score", type=DataType.DOUBLE)])
    return [r, o, ag]


# --------------------------------------------------------------------------- #
# Oracles — SUM over the DEV-1832 orders rows (each order once); the orphan order
# o8 has no customer, so its `customers.spend` is NULL and it drops from the SUM.
# --------------------------------------------------------------------------- #
#: SUM(customers.spend * orders.amount), home = orders: 1000+2000+4500+3750+300
#: +1600+1200+360+300.
WSUM_HOST_VALUE = 15010.0
#: SUM(customers.spend * (customers.spend + orders.amount)), home = orders.
WSUM_MIXED_VALUE = 102510.0
