"""Mode-A variable discoverability in inspect renders (DEV-1730).

An sql-mode model parameterised with ``{var}`` / ``{? ... ?}`` surfaces its
required/optional variables through the model skeleton (additive, no schema
bump), so an agent can learn the query contract without reading the SQL.
"""

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasetMetric, SlayerDataset
from slayer.core.query import extract_dataset_variables
from slayer.inspect.dataset_render import (
    dataset_skeleton_fields,
    render_dataset_skeleton,
)


def _model() -> SlayerDataset:
    return SlayerDataset(
        name="rr",
        sql="SELECT '{d_from}' AS d FROM t WHERE 1=1 "
            "AND {? brand IN ({brand}) ?} AND {? mkt IN ({market}) ?}",
        data_source="ds",
        fields=[DatasetField(name="d", sql="d", datatype=DataType.TIMESTAMP)],
    )


def test_skeleton_fields_carry_variables():
    fields = dataset_skeleton_fields(dataset=_model())
    assert fields["variables"]["required"] == ["d_from"]
    assert fields["variables"]["optional"] == ["brand", "market"]


def test_skeleton_render_lists_variables_line():
    out = render_dataset_skeleton(dataset=_model())
    assert "Variables:" in out
    assert "d_from" in out
    assert "brand" in out
    assert "market" in out


def test_skeleton_render_omits_variables_line_when_none():
    plain = SlayerDataset(
        name="m", sql_table="t", data_source="ds",
        fields=[DatasetField(name="a", sql="a", datatype=DataType.TEXT)],
    )
    out = render_dataset_skeleton(dataset=plain)
    assert "Variables:" not in out


def test_variables_are_derived_not_persisted_no_schema_bump():
    # Discoverability is structural: no new persisted SlayerModel field, so the
    # serialized schema version is unchanged and no `variables` key is stored.
    model = _model()
    dumped = model.model_dump()
    assert "variables" not in dumped
    assert dumped["version"] == SlayerDataset.model_fields["version"].default


def _measure_model(**kw) -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source="ds",
        fields=[DatasetField(name="amount", sql="amount", datatype=DataType.DOUBLE)],
        metrics=[DatasetMetric(name="amt_scaled", expression="sum(amount) * {k}")],
        **kw,
    )


def test_saved_measure_placeholder_required():
    assert extract_dataset_variables(_measure_model()).required == ["k"]
    assert dataset_skeleton_fields(dataset=_measure_model())["variables"]["required"] == ["k"]


def test_defaulted_saved_measure_placeholder_optional():
    model = _measure_model(query_variables={"k": 2})
    variables = extract_dataset_variables(model)
    assert variables.required == []
    assert variables.optional == ["k"]
