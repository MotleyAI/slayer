"""DEV-1866 — population inference selection + determination-item extraction.

Metadata-only: exercises the selection core (``infer_population``) and the
determination-item extractor (``determination_items``) against small
topologies, plus the ``source_model``-optional relaxation on ``SlayerQuery``.
No SQL is executed here — executed-value parity lives in
``test_dev1866_execution.py``.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import TimeGranularity
from slayer.core.query import ComputedDimension, SlayerQuery, TimeDimension
from slayer.engine.population import (
    PopulationChoice,
    determination_items,
    infer_population,
)

from tests._dev1866_fixtures import (
    DS_CHAIN,
    chain_models_by_name,
    make_inference_storage,
)


@pytest.fixture
async def storage():
    return await make_inference_storage()


# --------------------------------------------------------------------------- #
# source_model becomes optional (core relaxation).
# --------------------------------------------------------------------------- #
class TestSourceModelOptional:
    def test_constructs_without_source_model(self) -> None:
        q = SlayerQuery(dimensions=["customers.region"])
        assert q.source_model is None

    def test_validates_from_dict_without_source_model(self) -> None:
        q = SlayerQuery.model_validate({"dimensions": ["customers.region"]})
        assert q.source_model is None
        # No schema bump for this change (design §7): same version as an explicit query.
        assert q.version == SlayerQuery(source_model="orders").version

    def test_roundtrips_without_source_model(self) -> None:
        q = SlayerQuery(dimensions=["customers.region"])
        dumped = q.model_dump(mode="json", exclude_none=True)
        assert "source_model" not in dumped
        again = SlayerQuery.model_validate(dumped)
        assert again.source_model is None

    def test_explicit_source_model_still_accepted(self) -> None:
        assert SlayerQuery(source_model="orders").source_model == "orders"

    def test_strip_prefix_is_noop_when_rootless(self) -> None:
        q = SlayerQuery(dimensions=["customers.region"])
        stripped = q.strip_source_model_prefix()
        assert stripped.source_model is None
        assert stripped.dimensions == q.dimensions


# --------------------------------------------------------------------------- #
# Selection: infer_population chooses the fewest-hops viable determiner.
# --------------------------------------------------------------------------- #
class TestSelection:
    async def test_canonical_picks_coarse_side(self, storage) -> None:
        """dims on customers + a measure on orders ⇒ population = customers."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                measures=[{"formula": "orders.amount:sum", "name": "rev"}],
            ),
            storage=storage,
        )
        assert isinstance(choice, PopulationChoice)
        assert choice.model_name == "customers"
        assert choice.data_source == DS_CHAIN

    async def test_measure_does_not_shift_population(self, storage) -> None:
        with_measure = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                measures=[{"formula": "orders.amount:sum", "name": "rev"}],
            ),
            storage=storage,
        )
        without_measure = await infer_population(
            query=SlayerQuery(dimensions=["customers.region"]),
            storage=storage,
        )
        assert with_measure.model_name == without_measure.model_name == "customers"

    async def test_field_filter_pulls_population_to_its_owner(self, storage) -> None:
        """A field-typed filter on orders makes orders.status a determination item."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                filters=["orders.status = 'ok'"],
            ),
            storage=storage,
        )
        assert choice.model_name == "orders"

    async def test_measure_typed_filter_does_not_participate(self, storage) -> None:
        """An aggregation-bearing filter is dropped ⇒ population unchanged."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                filters=["orders.amount:sum > 100"],
            ),
            storage=storage,
        )
        assert choice.model_name == "customers"

    async def test_saved_measure_filter_does_not_participate(self, storage) -> None:
        """A filter on a saved measure (orders.revenue) is dropped ⇒ customers."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                filters=["orders.revenue > 100"],
            ),
            storage=storage,
        )
        assert choice.model_name == "customers"

    async def test_raw_row_mode_uses_same_rule(self, storage) -> None:
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                distinct_dimension_values=False,
            ),
            storage=storage,
        )
        assert choice.model_name == "customers"

    async def test_to_many_hop_disqualifies_a_candidate(self, storage) -> None:
        """basket cannot to-one-determine basket_items.sku ⇒ the child wins."""
        choice = await infer_population(
            query=SlayerQuery(dimensions=["basket.id", "basket_items.sku"]),
            storage=storage,
        )
        assert choice.model_name == "basket_items"

    async def test_undeclared_cardinality_but_unique_key_covered(self, storage) -> None:
        """events → days is provably to-one via the days.id primary key."""
        choice = await infer_population(
            query=SlayerQuery(dimensions=["events.kind", "days.label"]),
            storage=storage,
        )
        assert choice.model_name == "events"

    async def test_hop_sum_selection_over_multiple_items(self, storage) -> None:
        """customers pays 0+1 hops, orders pays 1+2 — the lower total wins."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region", "customers.regions.name"],
            ),
            storage=storage,
        )
        assert choice.model_name == "customers"

    async def test_sql_functional_aggregate_filter_excluded(self, storage) -> None:
        """A SQL-style ``sum(orders.amount) = 100`` filter is aggregate-typed and
        must not pull the population to orders (parity with the colon form)."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                filters=["sum(orders.amount) = 100"],
            ),
            storage=storage,
        )
        assert choice.model_name == "customers"

    async def test_measure_anchor_never_shifts_datasource(self, storage) -> None:
        """A measure referencing a model in another datasource must not affect
        scoping — measures never participate in inference."""
        choice = await infer_population(
            query=SlayerQuery(
                dimensions=["customers.region"],
                measures=[{"formula": "widgets.x:count", "name": "wc"}],
            ),
            storage=storage,
        )
        assert choice.model_name == "customers"
        assert choice.data_source == DS_CHAIN


# --------------------------------------------------------------------------- #
# Determination-item extraction rules.
# --------------------------------------------------------------------------- #
class TestDeterminationItems:
    def _items(self, query: SlayerQuery, **kw) -> set[str]:
        return set(determination_items(
            query, models_by_name=chain_models_by_name(), **kw
        ))

    def test_dimensions_and_field_filters_participate(self) -> None:
        items = self._items(SlayerQuery(
            dimensions=["customers.region"],
            filters=["orders.status = 'ok'"],
        ))
        assert items == {"customers.region", "orders.status"}

    def test_time_dimensions_participate(self) -> None:
        items = self._items(SlayerQuery(
            dimensions=["customers.region"],
            time_dimensions=[TimeDimension(
                dimension="orders.ordered_at", granularity=TimeGranularity.MONTH,
            )],
        ))
        assert items == {"customers.region", "orders.ordered_at"}

    def test_order_entries_do_not_participate(self) -> None:
        items = self._items(SlayerQuery(
            dimensions=["customers.region"],
            order=[{"column": "orders.amount", "direction": "desc"}],
        ))
        assert items == {"customers.region"}

    def test_measures_never_participate(self) -> None:
        items = self._items(SlayerQuery(
            dimensions=["customers.region"],
            measures=[{"formula": "orders.amount:sum", "name": "rev"}],
        ))
        assert items == {"customers.region"}

    def test_saved_measure_filter_ref_dropped(self) -> None:
        items = self._items(SlayerQuery(
            dimensions=["customers.region"],
            filters=["orders.revenue > 100"],
        ))
        assert items == {"customers.region"}

    def test_aggregation_filter_dropped(self) -> None:
        items = self._items(SlayerQuery(
            dimensions=["customers.region"],
            filters=["orders.amount:sum > 100"],
        ))
        assert items == {"customers.region"}

    def test_computed_dim_row_valued_ref_participates(self) -> None:
        items = self._items(SlayerQuery(dimensions=[
            ComputedDimension(expression="customers.tier", name="t"),
        ]))
        assert items == {"customers.tier"}

    def test_computed_dim_excludes_refs_inside_aggregation(self) -> None:
        items = self._items(SlayerQuery(dimensions=[
            "customers.region",
            ComputedDimension(expression="orders.amount:sum", name="tot"),
        ]))
        assert items == {"customers.region"}

    def test_computed_dim_partition_by_participates(self) -> None:
        items = self._items(SlayerQuery(dimensions=[
            ComputedDimension(
                expression="orders.amount:sum(partition_by=customers.tier)",
                name="share",
            ),
        ]))
        assert items == {"customers.tier"}

    def test_model_level_filters_never_participate(self) -> None:
        models = chain_models_by_name()
        models["customers"] = models["customers"].model_copy(
            update={"filters": ["regions.name <> 'Nowhere'"]}
        )
        items = set(determination_items(
            SlayerQuery(dimensions=["customers.region"]), models_by_name=models
        ))
        assert items == {"customers.region"}

    def test_items_are_deduped_across_dims_and_filters(self) -> None:
        items = determination_items(
            SlayerQuery(
                dimensions=["customers.region"],
                filters=["customers.region = 'North'"],
            ),
            models_by_name=chain_models_by_name(),
        )
        assert sorted(items) == ["customers.region"]

    def test_unresolved_variable_is_masked_not_a_ref(self) -> None:
        items = self._items(SlayerQuery(
            dimensions=["customers.region"],
            filters=["customers.region = {reg}"],
        ))
        assert items == {"customers.region"}

    def test_ref_introduced_only_by_variable_value_does_not_participate(self) -> None:
        items = self._items(SlayerQuery(
            dimensions=["customers.region"],
            filters=["customers.region = {reg}"],
            variables={"reg": "customers.tier"},
        ))
        assert items == {"customers.region"}

    def test_runtime_variable_value_does_not_participate(self) -> None:
        items = self._items(
            SlayerQuery(
                dimensions=["customers.region"],
                filters=["customers.region = {reg}"],
            ),
            runtime_variables={"reg": "customers.tier"},
        )
        assert items == {"customers.region"}
