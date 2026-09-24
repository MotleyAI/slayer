"""``model_from_stage_schema``: the one stage→model builder and its grain stamping."""

from __future__ import annotations

from typing import List, Optional

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.scope import StageColumn, StageSchema
from slayer.engine.join_safety import provably_to_one
from slayer.ir.source_bundle import model_from_stage_schema

MONEY = NumberFormat(type=NumberFormatType.CURRENCY, precision=2)


def _schema(grain: Optional[List[str]]) -> StageSchema:
    return StageSchema(
        relation_name="s1",
        columns=[
            StageColumn(name="id", sql_alias="id", public_alias="id", type=DataType.INT),
            StageColumn(name="tier", sql_alias="tier", public_alias="tier", type=DataType.TEXT),
            StageColumn(name="rev", sql_alias="rev", public_alias="rev"),
        ],
        grain=grain,
    )


def _build(grain: Optional[List[str]], **kw) -> SlayerModel:
    return model_from_stage_schema(name="s1", schema=_schema(grain), data_source="ds", **kw)


def _col(model: SlayerModel, name: str) -> Column:
    return next(c for c in model.columns if c.name == name)


def _flags(model: SlayerModel) -> dict:
    return {c.name: (c.primary_key, c.unique) for c in model.columns}


class TestGrainStamping:
    def test_single_member_grain_is_unique_not_primary_key(self) -> None:
        model = _build(["id"])
        assert _flags(model) == {"id": (False, True), "tier": (False, False), "rev": (False, False)}

    def test_composite_grain_is_primary_key_on_each_member(self) -> None:
        model = _build(["id", "tier"])
        assert _flags(model) == {"id": (True, False), "tier": (True, False), "rev": (False, False)}

    def test_empty_grain_stamps_nothing(self) -> None:
        assert not any(pk or uq for pk, uq in _flags(_build([])).values())

    def test_absent_grain_stamps_nothing(self) -> None:
        assert not any(pk or uq for pk, uq in _flags(_build(None)).values())

    def test_single_member_grain_proves_a_join_onto_it(self) -> None:
        edge = ModelJoin(target_model="s1", join_pairs=[["customer_id", "id"]])
        assert provably_to_one(edge=edge, target_model=_build(["id"]))

    def test_partial_composite_grain_join_stays_unproven(self) -> None:
        model = _build(["id", "tier"])
        partial = ModelJoin(target_model="s1", join_pairs=[["customer_id", "id"]])
        full = ModelJoin(target_model="s1", join_pairs=[["customer_id", "id"], ["tier", "tier"]])
        assert not provably_to_one(edge=partial, target_model=model)
        assert provably_to_one(edge=full, target_model=model)


class TestInputs:
    def test_sibling_form_reads_the_stage_relation(self) -> None:
        model = _build(["id"])
        assert model.name == "s1"
        assert model.sql_table == "s1"
        assert model.sql is None
        assert model.data_source == "ds"

    def test_query_backed_form_carries_wrapped_sql(self) -> None:
        model = _build(["id"], sql="SELECT 1 AS id")
        assert model.sql == "SELECT 1 AS id"
        assert model.sql_table is None

    def test_column_sql_applied(self) -> None:
        model = _build(["id"], sql="SELECT 1 AS id", column_sql={"tier": "tier_fit"})
        assert _col(model, "tier").sql == "tier_fit"

    def test_default_time_dimension_passed_through(self) -> None:
        schema = StageSchema(
            relation_name="s1",
            columns=[StageColumn(name="ordered_at", sql_alias="ordered_at", type=DataType.TIMESTAMP)],
            grain=["ordered_at"],
        )
        model = model_from_stage_schema(
            name="s1", schema=schema, data_source="ds",
            sql="SELECT 1", default_time_dimension="ordered_at",
        )
        assert model.default_time_dimension == "ordered_at"


class TestColumnFacts:
    def test_column_metadata_carried(self) -> None:
        schema = StageSchema(
            relation_name="s1",
            columns=[
                StageColumn(
                    name="created_at", sql_alias="created_at", type=DataType.TIMESTAMP,
                    granularity=TimeGranularity.MONTH, label="Created",
                    description="Order creation month",
                ),
                StageColumn(
                    name="rev", sql_alias="rev", type=DataType.DOUBLE, label="Revenue",
                    format=MONEY, description="Total revenue",
                ),
            ],
            grain=["created_at"],
        )
        model = model_from_stage_schema(name="s1", schema=schema, data_source="ds")
        created, rev = _col(model, "created_at"), _col(model, "rev")
        assert (created.type, created.granularity, created.label, created.description) == (
            DataType.TIMESTAMP, TimeGranularity.MONTH, "Created", "Order creation month",
        )
        assert (rev.type, rev.granularity, rev.label, rev.format, rev.description) == (
            DataType.DOUBLE, None, "Revenue", MONEY, "Total revenue",
        )

    def test_untyped_column_defaults_to_double(self) -> None:
        assert _col(_build(None), "rev").type == DataType.DOUBLE
