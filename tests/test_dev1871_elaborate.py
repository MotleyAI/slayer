"""The one elaboration pass and its typing environment (design D2).

``elaborate_query`` gives every top-level expression a position verdict, home
dataset, resolved grain, and Broadcast insertions at grain-union points, and
memoizes one term per aggregate key. ``compile_query`` compiles the elaborated
environment to the same plan ``plan_query`` produces.
"""

from __future__ import annotations


from slayer.core.enums import DataType
from slayer.core.keys import AggregateKey, FieldKey
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.query import QueryDimension, SlayerQuery
from slayer.engine.compile import compile_query
from slayer.engine.bind_inputs import bind_query_inputs
from slayer.engine.plan import plan_query
from slayer.core.keys import Grain
from slayer.ir.planned import PlannedQuery
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.engine.elaborate import elaborate_query
from slayer.ir.elaborated import ElaboratedQuery
from slayer.ir.terms import Aggregate, StoredDataset


_DS = "elab_ds"
_STATUS = FieldKey(leaf="status")
_TIER = FieldKey(leaf="tier")


def _orders_dataset() -> SlayerDataset:
    return SlayerDataset(
        name="orders", data_source=_DS, sql_table="orders",
        fields=[
            DatasetField(name="id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="tier", datatype=DataType.STRING),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
    )


def _bundle() -> ResolvedSourceBundle:
    return ResolvedSourceBundle(dialect="postgres", population=_orders_dataset())


def _partitioned_key(partition: FieldKey) -> AggregateKey:
    return AggregateKey(
        source=FieldKey(leaf="amount"), agg="sum",
        partition_keys=Grain.of({partition}),
    )


class TestTypingEnvironment:
    def test_every_position_gets_a_verdict(self) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["orders.status"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
            "filter": ["orders.status = 'ok'", "sum(orders.amount) > 5"],
            "order": [{"field": "orders.status", "direction": "asc"}],
        })
        elab = elaborate_query(query=query, bundle=_bundle())
        assert isinstance(elab, ElaboratedQuery)
        assert [e.verdict for e in elab.dimensions] == ["field"]
        assert [e.verdict for e in elab.metrics] == ["metric"]
        assert [e.verdict for e in elab.filters] == ["field", "metric"]
        assert [e.verdict for e in elab.order] == ["field"]

    def test_measure_entry_home_and_resolved_grain(self) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["orders.status"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
        })
        elab = elaborate_query(query=query, bundle=_bundle())
        entry = elab.metrics[0]
        assert entry.home == StoredDataset(data_source=_DS, dataset_name="orders")
        assert entry.grain == Grain.of({_STATUS})

    def test_aggregate_terms_memoized_by_key_identity(self) -> None:
        """The same aggregate in metric and filter position is one term."""
        query = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["orders.status"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
            "filter": ["sum(orders.amount) > 5"],
        })
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
            population="orders",
            dimensions=[
                "orders.status",
                QueryDimension(
                    field=(
                        "sum(orders.amount, partition_by=orders.status)"
                        " + sum(orders.amount, partition_by=orders.tier)"
                    ),
                    alias="mix",
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
        query = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["orders.status"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
            "filter": ["sum(orders.amount) > 5"],
            "order": [{"field": "orders.status", "direction": "asc"}],
        })
        bundle = _bundle()
        raw = elaborate_query(query=query, bundle=bundle)
        prebound = bind_query_inputs(query=query, bundle=bundle)
        via_prebound = elaborate_query(
            query=query, bundle=bundle, prebound=prebound,
        )
        assert raw == via_prebound


class TestCompileSeam:
    def test_compile_of_elaborated_matches_plan_query(self) -> None:
        query = SlayerQuery.model_validate({
            "population": "orders",
            "dimensions": ["orders.status"],
            "metrics": [{"metric": "sum(orders.amount)", "alias": "rev"}],
        })
        planned = plan_query(query=query, bundle=_bundle())
        compiled = compile_query(
            elaborated=elaborate_query(query=query, bundle=_bundle()),
        )
        assert isinstance(compiled, PlannedQuery)
        assert compiled == planned
