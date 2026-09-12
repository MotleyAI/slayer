"""DEV-1871 — the one elaboration pass and its typing environment (design D2).

``elaborate_query`` gives every top-level expression a position verdict, home
dataset, resolved grain, and Broadcast insertions at grain-union points, and
memoizes one term per aggregate key. ``compile_query`` compiles the elaborated
environment to the same plan ``plan_query`` produces.
"""

from __future__ import annotations


from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, ColumnKey
from slayer.core.models import Column, SlayerModel
from slayer.core.query import ComputedDimension, SlayerQuery
from slayer.engine.compile import compile_query
from slayer.engine.compile.stages import bind_query_inputs, plan_query
from slayer.core.keys import Grain
from slayer.ir.planned import PlannedQuery
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.elaborate import elaborate_query
from slayer.ir.elaborated import ElaboratedQuery
from slayer.ir.terms import Aggregate, ModelDataset


_DS = "elab_ds"
_STATUS = ColumnKey(leaf="status")
_TIER = ColumnKey(leaf="tier")


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders", data_source=_DS, sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT),
            Column(name="tier", type=DataType.TEXT),
            Column(name="amount", type=DataType.DOUBLE),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(source_model=_orders_model())


def _partitioned_key(partition: ColumnKey) -> AggregateKey:
    return AggregateKey(
        source=ColumnKey(leaf="amount"), agg="sum",
        partition_keys=Grain.of({partition}),
    )


class TestTypingEnvironment:
    def test_every_position_gets_a_verdict(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "rev"}],
            filters=["status = 'ok'", "amount:sum > 5"],
            order=[{"column": "status", "direction": "asc"}],
        )
        elab = elaborate_query(query=query, bundle=_bundle())
        assert isinstance(elab, ElaboratedQuery)
        assert [e.verdict for e in elab.dimensions] == ["field"]
        assert [e.verdict for e in elab.measures] == ["measure"]
        assert [e.verdict for e in elab.filters] == ["field", "measure"]
        assert [e.verdict for e in elab.order] == ["field"]

    def test_measure_entry_home_and_resolved_grain(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "rev"}],
        )
        elab = elaborate_query(query=query, bundle=_bundle())
        entry = elab.measures[0]
        assert entry.home == ModelDataset(data_source=_DS, model_name="orders")
        assert entry.grain == Grain.of({_STATUS})

    def test_aggregate_terms_memoized_by_key_identity(self) -> None:
        """The same aggregate in measure and filter position is one term."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "rev"}],
            filters=["amount:sum > 5"],
        )
        elab = elaborate_query(query=query, bundle=_bundle())
        agg_keys = [k for k in elab.terms if isinstance(k, AggregateKey)]
        assert len(agg_keys) == 1
        term = elab.terms[agg_keys[0]]
        assert isinstance(term, Aggregate)
        assert term.recipe == agg_keys[0]
        assert term.grain == Grain.of({_STATUS})

    def test_broadcasts_inserted_at_the_grain_union(self) -> None:
        """Mixed-partition arithmetic combines at the union grain, each side
        lifted by an explicit Broadcast."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=[
                "status",
                ComputedDimension(
                    expression=(
                        "amount:sum(partition_by=status)"
                        " + amount:sum(partition_by=tier)"
                    ),
                    name="mix",
                ),
            ],
        )
        elab = elaborate_query(query=query, bundle=_bundle())
        status_grain = Grain.of({_STATUS})
        tier_grain = Grain.of({_TIER})
        assert elab.terms[_partitioned_key(_STATUS)].grain == status_grain
        assert elab.terms[_partitioned_key(_TIER)].grain == tier_grain

        entry = elab.dimensions[1]
        union = status_grain.union(tier_grain)
        assert len(entry.broadcasts) == 2
        assert all(b.into == union for b in entry.broadcasts)
        assert {b.source.grain for b in entry.broadcasts} == {
            status_grain, tier_grain,
        }


class TestEntryEquivalence:
    def test_raw_and_prebound_entries_build_the_same_environment(self) -> None:
        """D9 stage 2: binding is a sub-phase, so both doors agree."""
        query = SlayerQuery(
            source_model="orders",
            dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "rev"}],
            filters=["amount:sum > 5"],
            order=[{"column": "status", "direction": "asc"}],
        )
        bundle = _bundle()
        raw = elaborate_query(query=query, bundle=bundle)
        prebound = bind_query_inputs(query=query, bundle=bundle)
        via_prebound = elaborate_query(
            query=query, bundle=bundle, prebound=prebound,
        )
        assert raw == via_prebound


class TestCompileSeam:
    def test_compile_of_elaborated_matches_plan_query(self) -> None:
        query = SlayerQuery(
            source_model="orders",
            dimensions=["status"],
            measures=[{"formula": "amount:sum", "name": "rev"}],
        )
        planned = plan_query(query=query, bundle=_bundle())
        compiled = compile_query(
            elaborated=elaborate_query(query=query, bundle=_bundle()),
        )
        assert isinstance(compiled, PlannedQuery)
        assert compiled == planned
