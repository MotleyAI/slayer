"""DEV-1832 task 1.3 — the home dataset resolved on the term (D2).

Every ``#### Scenario`` of ``queries/semantics`` › *Home dataset of a row-level
aggregation source*: the elaborator resolves each aggregate's home once and the
term carries it as ``Aggregate.home_path`` (relative to the environment's host).
These fail on the current tree — the term has no ``home_path`` and every
aggregate's home is the query root.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.elaborate import elaborate_query
from slayer.ir.source_bundle import ResolvedSourceBundle

from tests._dev1832_fixtures import (
    ModelMeasure,
    bundle,
    dev1832_models,
    orders_q,
)


def _home_path(formula: str) -> tuple:
    """The ``home_path`` of the single aggregate in a measure over orders."""
    elab = elaborate_query(
        query=orders_q(measures=[ModelMeasure(formula=formula, name="m")]),
        bundle=bundle(dev1832_models()))
    agg_keys = [k for k in elab.terms if isinstance(k, AggregateKey)]
    assert len(agg_keys) == 1, agg_keys
    return elab.terms[agg_keys[0]].home_path


def _parallel_edge_models() -> list[SlayerModel]:
    """A model with two NAMED joins to the same target (parallel edges)."""
    tickets = SlayerModel(
        name="tk", data_source="test", sql_table="tk",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="opened_by", type=DataType.INT),
                 Column(name="closed_by", type=DataType.INT)],
        joins=[ModelJoin(target_model="ag", join_pairs=[["opened_by", "id"]], name="opener"),
               ModelJoin(target_model="ag", join_pairs=[["closed_by", "id"]], name="closer")])
    ag = SlayerModel(
        name="ag", data_source="test", sql_table="ag",
        columns=[Column(name="id", type=DataType.INT, primary_key=True),
                 Column(name="score", type=DataType.DOUBLE)])
    return [tickets, ag]


class TestHomeDatasetPerScenario:
    def test_deepest_determining_dataset_wins(self):
        # sum(customers.spend - customers.regions.pop) → home is customers.
        assert _home_path("sum(customers.spend - customers.regions.pop)") == ("customers",)

    def test_host_side_leaf_pulls_home_to_root(self):
        # amount is host-local → home is orders (the root).
        assert _home_path("sum(amount - customers.discount)") == ()

    def test_branches_meet_at_common_ancestor(self):
        # customers.spend and stores.rent diverge at the root → home is orders.
        assert _home_path("sum(customers.spend - stores.rent)") == ()

    def test_a_parameter_widens_the_home(self):
        # weight=amount (orders-local) pulls the home to the root.
        assert _home_path(
            "wsum(customers.spend + customers.regions.pop, weight=amount)") == ()

    def test_a_definition_default_widens_the_home_the_same_way(self):
        # wsum's default weight (spend) is customers-local → home stays customers,
        # not the root — the default joins the home candidates.
        assert _home_path("wsum(customers.spend - customers.regions.pop)") == ("customers",)

    def test_spelling_never_moves_the_home(self):
        assert _home_path("sum(customers.spend)") == _home_path("sum(customers.spend + 0)")
        assert _home_path("sum(customers.spend + 0)") == ("customers",)

    def test_no_home_falls_back_to_the_anchor(self):
        # bad_pop crosses the fanning regions→region_events hop, so no dataset
        # determines it; the home resolves to the anchor and the input-safety
        # checker raises downstream (exercised in test_dev1832_cross_model_exec).
        assert _home_path("sum(amount - customers.regions.bad_pop)") == ()


class TestHomeIsSpellingInvariant:
    def test_single_column_source_home_matches_its_expression_twin(self):
        # sum(customers.spend) and its degenerate expression twin resolve the
        # identical home (the term depends on the leaves, not the spelling).
        assert _home_path("sum(customers.spend)") == _home_path("sum(customers.spend * 1)")

    def test_single_deep_leaf_homes_at_its_own_dataset(self):
        # A lone customers.regions.pop homes at customers.regions; adding a
        # customers-local leaf pulls the shared home shallower to customers.
        assert _home_path("sum(customers.regions.pop)") == ("customers", "regions")
        assert _home_path(
            "sum(customers.spend - customers.regions.pop)") == ("customers",)

    @pytest.mark.parametrize("formula,expected", [
        ("sum(amount - cost)", ()),                       # both host-local
        ("sum(customers.spend - customers.discount)", ("customers",)),
    ])
    def test_home_path_matrix(self, formula, expected):
        assert _home_path(formula) == expected


class TestParallelNamedEdges:
    """Two named joins to the same target carry distinct path segments (the join
    name), so their leaves resolve to distinct paths (see test_dev1832_anchor's
    ``test_parallel_named_edges_yield_distinct_paths``); the home is their common
    ancestor. (The synthesized-sub-plan root-relativity and the nested cross-model
    producer's consumption of ``home_path`` are observable in
    test_dev1832_cross_model_exec's target-homed execution and the golden SQL.)"""

    def test_home_at_root_over_parallel_edges(self):
        models = _parallel_edge_models()
        elab = elaborate_query(
            query=SlayerQuery(source_model="tk", measures=[
                ModelMeasure(formula="sum(opener.score - closer.score)", name="m")]),
            bundle=ResolvedSourceBundle(source_model=models[0], referenced_models=models[1:]))
        aggs = [k for k in elab.terms if isinstance(k, AggregateKey)]
        assert len(aggs) == 1
        # opener/closer are both provably to-one from tk and diverge → home is tk.
        assert elab.terms[aggs[0]].home_path == ()
