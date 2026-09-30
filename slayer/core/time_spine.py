"""The built-in time spine: a virtual per-datasource model of all instants, and its edges."""

from __future__ import annotations

import re
from typing import Mapping, Optional

from slayer.core.enums import DataType, JoinCardinality, JoinType
from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.core.query import SlayerQuery

TIME_SPINE_MODEL = "time_spine"
TIME_SPINE_COLUMN = "timestamp"


def spine_model(*, data_source: str) -> SlayerModel:
    """The virtual ``time_spine`` model; never stored, its rows are rendered as a bucket series."""
    return SlayerModel(
        name=TIME_SPINE_MODEL, data_source=data_source, sql_table=TIME_SPINE_MODEL,
        description=(
            "Built-in time spine: every instant. Group by time_spine.timestamp at a granularity "
            "(with a lower bound) to get every bucket in range, each fact attributed through its axis."
        ),
        columns=[Column(name=TIME_SPINE_COLUMN, type=DataType.TIMESTAMP, primary_key=True)],
    )


def is_spine(model: SlayerModel) -> bool:
    return model.name == TIME_SPINE_MODEL


def axis_join(model: SlayerModel) -> Optional[ModelJoin]:
    """``model``'s virtual many-to-one edge from its axis to the spine, when it has an axis."""
    if is_spine(model) or model.population_spine:
        return None
    axis = model.effective_default_time_dimension
    if axis is None or model.get_column(axis) is None:
        return None
    return ModelJoin(
        target_model=TIME_SPINE_MODEL, join_pairs=[[axis, TIME_SPINE_COLUMN]],
        cardinality=JoinCardinality.MANY_TO_ONE,
    )


def spine_joins(model: SlayerModel, *, models_by_name: Mapping[str, SlayerModel]) -> list[ModelJoin]:
    """``model``'s declared joins plus its axis edge when the spine is in the universe."""
    if TIME_SPINE_MODEL not in models_by_name:
        return list(model.joins)
    axis = axis_join(model)
    return [*model.joins, axis] if axis is not None else list(model.joins)


PRODUCT_JOIN_TYPE = JoinType.INNER


def names_spine(ref: str) -> bool:
    """A dotted reference into the spine model."""
    return ref.split(".")[0] == TIME_SPINE_MODEL


_SPINE_REF = re.compile(rf"(?<![\w.]){TIME_SPINE_MODEL}\.")


def mentions_spine(text: str) -> bool:
    """A formula / filter text reading the spine."""
    return bool(_SPINE_REF.search(text))


def is_spine_query(query: SlayerQuery) -> bool:
    """A query rooted at the spine, or reading it in a time dimension, dimension or filter."""
    dims = [getattr(d, "expression", None) or getattr(d, "full_name", "") for d in query.dimensions or []]
    return query.source_model_name == TIME_SPINE_MODEL or any(
        names_spine(td.dimension.full_name) for td in query.time_dimensions or []
    ) or any(mentions_spine(t) for t in [*dims, *(query.filters or [])])
