"""Mode-A variable discoverability in inspect renders (DEV-1730).

An sql-mode model parameterised with ``{var}`` / ``{? ... ?}`` surfaces its
required/optional variables through the model skeleton (additive, no schema
bump), so an agent can learn the query contract without reading the SQL.
"""

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.inspect.model_render import (
    model_skeleton_fields,
    render_model_skeleton,
)


def _model() -> SlayerModel:
    return SlayerModel(
        name="rr",
        sql="SELECT '{d_from}' AS d FROM t WHERE 1=1 "
            "AND {? brand IN ({brand}) ?} AND {? mkt IN ({market}) ?}",
        data_source="ds",
        columns=[Column(name="d", sql="d", type=DataType.TIMESTAMP)],
    )


def test_skeleton_fields_carry_variables():
    fields = model_skeleton_fields(model=_model())
    assert fields["variables"]["required"] == ["d_from"]
    assert fields["variables"]["optional"] == ["brand", "market"]


def test_skeleton_render_lists_variables_line():
    out = render_model_skeleton(model=_model())
    assert "Variables:" in out
    assert "d_from" in out and "brand" in out and "market" in out


def test_skeleton_render_omits_variables_line_when_none():
    plain = SlayerModel(
        name="m", sql_table="t", data_source="ds",
        columns=[Column(name="a", sql="a", type=DataType.TEXT)],
    )
    out = render_model_skeleton(model=plain)
    assert "Variables:" not in out


def test_variables_are_derived_not_persisted_no_schema_bump():
    # Discoverability is structural: no new persisted SlayerModel field, so the
    # serialized schema version is unchanged and no `variables` key is stored.
    model = _model()
    dumped = model.model_dump()
    assert "variables" not in dumped
    assert dumped["version"] == SlayerModel.model_fields["version"].default
