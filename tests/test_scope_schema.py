"""Stage 2 (DEV-1450) — typed scope and stage-schema types (P5, P6).

``ModelScope`` is the scope kind used while binding refs against a model
with joins; dotted refs walk the join graph. ``StageSchema`` is the
typed projection of a query stage — downstream stages bind against it
as a *flat* namespace (P5: no join syntax through StageSchema).

Per I2, ``ModelScope.source_model`` is ``Optional`` from day one so a
future anchor-less mode is a type-additive change. DEV-1450's binder
will assert ``source_model is not None`` at use sites.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.scope import ModelScope, StageColumn, StageSchema


def _orders_model() -> SlayerModel:
    return SlayerModel(
        name="orders",
        data_source="prod",
        sql_table="orders",
        columns=[
            Column(name="id", type=DataType.INT, primary_key=True),
            Column(name="amount", type=DataType.DOUBLE),
            Column(name="status", type=DataType.TEXT),
        ],
    )


# ---------------------------------------------------------------------------
# StageColumn
# ---------------------------------------------------------------------------


class TestStageColumn:
    def test_minimal(self):
        c = StageColumn(name="rev", sql_alias="rev")
        assert c.name == "rev"
        assert c.sql_alias == "rev"
        assert c.public_alias is None
        assert c.hidden is False
        assert c.type is None

    def test_full_fields(self):
        c = StageColumn(
            name="customers__regions__name",
            sql_alias="customers__regions__name",
            public_alias="customers.regions.name",
            type=DataType.TEXT,
            label="Region",
            format=None,
            hidden=False,
            description="region of the customer",
            meta={"source": "fk"},
            sampled="North,South,East",
            provenance="join_walk(orders→customers→regions)",
        )
        assert c.public_alias == "customers.regions.name"
        assert c.label == "Region"
        assert c.meta == {"source": "fk"}

    def test_hidden_slot(self):
        c = StageColumn(name="_h_revenue_sum", sql_alias="_h_revenue_sum", hidden=True)
        assert c.hidden is True
        assert c.public_alias is None

    def test_frozen(self):
        c = StageColumn(name="x", sql_alias="x")
        with pytest.raises((TypeError, ValueError)):
            c.name = "y"  # type: ignore[misc]

    def test_value_equality(self):
        a = StageColumn(name="x", sql_alias="x", public_alias="x")
        b = StageColumn(name="x", sql_alias="x", public_alias="x")
        assert a == b


# ---------------------------------------------------------------------------
# StageSchema
# ---------------------------------------------------------------------------


class TestStageSchema:
    def test_lookup_by_name(self):
        s = StageSchema(
            relation_name="stage_1",
            columns=[
                StageColumn(name="rev", sql_alias="rev"),
                StageColumn(name="region", sql_alias="region"),
            ],
        )
        assert s["rev"].sql_alias == "rev"
        assert s["region"].name == "region"

    def test_missing_raises(self):
        s = StageSchema(relation_name="s", columns=[])
        with pytest.raises(KeyError):
            _ = s["absent"]

    def test_contains(self):
        s = StageSchema(
            relation_name="s",
            columns=[StageColumn(name="rev", sql_alias="rev")],
        )
        assert "rev" in s
        assert "absent" not in s

    def test_get_returns_none(self):
        s = StageSchema(
            relation_name="s",
            columns=[StageColumn(name="rev", sql_alias="rev")],
        )
        assert s.get("rev") is not None
        assert s.get("absent") is None

    def test_hidden_column_is_present_but_no_public_alias(self):
        s = StageSchema(
            relation_name="s",
            columns=[
                StageColumn(name="rev", sql_alias="rev", public_alias="rev"),
                StageColumn(name="_h_extra", sql_alias="_h_extra", hidden=True),
            ],
        )
        # Hidden slot is bindable (still in the schema), but has no public_alias.
        assert "_h_extra" in s
        assert s["_h_extra"].public_alias is None
        assert s["_h_extra"].hidden is True

    def test_flat_namespace_dunder_names_ok(self):
        # P5: __-bearing identifiers are FLAT names in StageSchema scope —
        # they're not interpreted as join-path aliases here.
        s = StageSchema(
            relation_name="s",
            columns=[StageColumn(
                name="robot_details__modelseriesval",
                sql_alias="robot_details__modelseriesval",
                public_alias="robot_details__modelseriesval",
            )],
        )
        assert "robot_details__modelseriesval" in s
        assert s["robot_details__modelseriesval"].name == "robot_details__modelseriesval"


# ---------------------------------------------------------------------------
# ModelScope
# ---------------------------------------------------------------------------


class TestModelScope:
    def test_with_source_model(self):
        m = _orders_model()
        scope = ModelScope(source_model=m)
        assert scope.source_model is m

    def test_source_model_none_is_constructible_i2(self):
        # I2: future anchor-less mode reserves source_model=None. Today's
        # binder must reject None at the use site, but the TYPE must allow
        # it so the new branch is purely additive later.
        scope = ModelScope(source_model=None)
        assert scope.source_model is None

    def test_default_source_model_none(self):
        # No mandatory field — defaulting to None keeps both modes type-compatible.
        scope = ModelScope()
        assert scope.source_model is None
