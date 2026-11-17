"""A stage's grain is typed ``StageSchema`` data, recorded from its dimension positions."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from slayer.core.enums import DataType
from slayer.core.query import SlayerQuery
from slayer.core.scope import StageField, StageSchema
from slayer.engine.plan import plan_query, plan_stages
from slayer.ir.source_bundle import ResolvedSourceBundle
from tests._dev1836_fixtures import dev1836_datasets

MONTH = [{"field": "ordered_at", "granularity": "month"}]
AMOUNT = {"expression": "sum(amount)", "name": "t"}
CHANNEL_TOTAL = {"expression": "sum(amount, partition_by=channel)", "name": "ct"}


def _bundle() -> ResolvedSourceBundle:
    models = dev1836_datasets()
    return ResolvedSourceBundle(population=models[0], referenced_datasets=models, dialect="postgres")


def _grain(**body):
    query = SlayerQuery.model_validate({"population": "orders", **body})
    schema = plan_query(query=query, bundle=_bundle()).stage_schema
    assert schema is not None
    return schema.grain


class TestGrainEmission:
    def test_aggregating_stage_grain_is_dimensions_and_time_dimensions(self) -> None:
        grain = _grain(dimensions=["status"], time_dimensions=MONTH, metrics=[AMOUNT])
        assert grain == ["status", "ordered_at"]

    def test_dimension_only_stage_grain_is_its_dimensions(self) -> None:
        assert _grain(dimensions=["status", "channel"]) == ["status", "channel"]

    def test_raw_rows_stage_has_no_grain(self) -> None:
        assert _grain(dimensions=["status"], distinct_dimension_values=False) is None

    def test_measures_only_stage_has_empty_grain(self) -> None:
        assert _grain(metrics=[AMOUNT]) == []

    def test_dotted_dimension_member_is_its_flat_name(self) -> None:
        assert _grain(dimensions=["customers.tier"], metrics=[AMOUNT]) == ["customers__tier"]

    def test_aggregate_valued_computed_dimension_is_a_member(self) -> None:
        grain = _grain(
            dimensions=["status", CHANNEL_TOTAL],
            metrics=[{"expression": "count(*)", "name": "n"}],
        )
        assert grain == ["status", "ct"]

    def test_aggregate_valued_computed_dimension_in_dimension_only_stage(self) -> None:
        assert _grain(dimensions=["status", CHANNEL_TOTAL]) == ["status", "ct"]

    def test_partitioned_measure_is_not_a_member(self) -> None:
        grain = _grain(
            dimensions=["status", "channel"],
            metrics=[{"expression": "sum(amount, partition_by=status)", "name": "st"}],
        )
        assert grain == ["status", "channel"]

    def test_transform_valued_computed_dimension_is_a_member(self) -> None:
        grain = _grain(
            dimensions=["status", {"expression": "rank(sum(amount, partition_by=channel), direction='desc')", "name": "rk"}],
            metrics=[{"expression": "count(*)", "name": "n"}],
        )
        assert grain == ["status", "rk"]

    def test_cumsum_stage_grain_includes_its_time_bucket(self) -> None:
        grain = _grain(
            time_dimensions=MONTH,
            metrics=[{"expression": "cumsum(sum(amount))", "name": "cs"}],
        )
        assert grain == ["ordered_at"]

    def test_slot_shared_by_dimension_and_measure_contributes_only_the_dimension(self) -> None:
        expr = "sum(amount, partition_by=status)"
        grain = _grain(
            dimensions=["status", {"expression": expr, "name": "ct"}],
            metrics=[{"expression": expr, "name": "ct2"}],
        )
        assert grain == ["status", "ct"]

    def test_slot_under_two_dimension_aliases_contributes_the_first(self) -> None:
        grain = _grain(
            dimensions=["status", {"expression": "status", "name": "s2"}],
            metrics=[AMOUNT],
        )
        assert grain == ["status"]


class TestSiblingStageGrain:
    def test_named_sibling_stage_schema_carries_its_grain(self) -> None:
        sibling = SlayerQuery.model_validate({
            "name": "c", "population": "customers", "dimensions": ["id"],
            "metrics": [{"expression": "max(tier)", "name": "tr"}],
        })
        root = SlayerQuery.model_validate({
            "population": "c", "dimensions": ["tr"],
            "metrics": [{"expression": "count(*)", "name": "n"}],
        })
        customers = next(m for m in dev1836_datasets() if m.name == "customers")
        bundle = _bundle().model_copy(update={"stage_source_models": {"c": customers}})
        planned = plan_stages(queries=[sibling, root], bundle=bundle)
        assert planned[0].stage_schema is not None
        assert planned[0].stage_schema.grain == ["id"]


class TestGrainValidator:
    def test_grain_member_must_name_a_column(self) -> None:
        columns = [StageField(name="a", sql_alias="a", datatype=DataType.TEXT)]
        with pytest.raises(ValidationError):
            StageSchema(relation_name="s", fields=columns, grain=["b"])

    def test_grain_members_naming_columns_validate(self) -> None:
        schema = StageSchema(
            relation_name="s",
            fields=[StageField(name="a", sql_alias="a", datatype=DataType.TEXT)],
            grain=["a"],
        )
        assert schema.grain == ["a"]
