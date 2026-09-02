"""DEV-1836 task 1.3 — Cube importer maps ``relationship`` → ``Join.cardinality``
(design D8, F10).

Spec: openspec …/specs/models/join-cardinality — "Cube import maps relationship
to cardinality". The mapping is a closed table; anything else stays ``None``
and lands in the conversion report — never coerced into safety evidence.
"""

from __future__ import annotations

import pytest

from slayer.core.enums import JoinCardinality
from slayer.core.models import SlayerModel
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import CubeCube, CubeDimension, CubeJoin, CubeProject

DS = "test_ds"


def _project(join_kwargs: dict) -> CubeProject:
    return CubeProject(cubes=[
        CubeCube(
            name="orders", sql_table="public.orders",
            joins=[CubeJoin(name="customers",
                            sql="{CUBE}.customer_id = {customers.id}",
                            **join_kwargs)],
            dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number")],
        ),
        CubeCube(
            name="customers", sql_table="public.customers",
            dimensions=[CubeDimension(name="id", sql="{CUBE}.id", type="number",
                                      primary_key=True)],
        ),
    ])


def _convert(project: CubeProject) -> tuple[dict[str, SlayerModel], object]:
    result = CubeToSlayerConverter(project=project, data_source=DS).convert()
    return {m.name: m for m in result.models}, result.report


@pytest.mark.parametrize(("relationship", "expected"), [
    ("many_to_one", JoinCardinality.MANY_TO_ONE),
    ("belongs_to", JoinCardinality.MANY_TO_ONE),
    ("one_to_many", JoinCardinality.ONE_TO_MANY),
    ("has_many", JoinCardinality.ONE_TO_MANY),
    ("one_to_one", JoinCardinality.ONE_TO_ONE),
    ("has_one", JoinCardinality.ONE_TO_ONE),
])
def test_relationship_maps_to_cardinality(relationship, expected) -> None:
    models, report = _convert(_project({"relationship": relationship}))
    (join,) = models["orders"].joins
    assert join.cardinality == expected
    assert not any(relationship in issue.message for issue in report.issues)


def test_parsed_default_maps_like_an_explicit_value() -> None:
    # CubeJoin defaults relationship to Cube's own default, many_to_one.
    models, _ = _convert(_project({}))
    (join,) = models["orders"].joins
    assert join.cardinality == JoinCardinality.MANY_TO_ONE


def test_unknown_relationship_is_not_trusted_and_reported() -> None:
    """F10 — an unrecognized string leaves cardinality unset and warns."""
    models, report = _convert(_project({"relationship": "zero_or_more"}))
    (join,) = models["orders"].joins
    assert join.cardinality is None
    hits = [i for i in report.issues if "zero_or_more" in i.message]
    assert hits, [i.message for i in report.issues]
    assert all(i.severity in ("warning", "error") for i in hits)
