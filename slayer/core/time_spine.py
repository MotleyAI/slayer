"""The built-in time spine: a virtual per-datasource model of all instants, and its edges."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from slayer.core.enums import JoinCardinality, JoinType
from slayer.core.models import Column, ModelJoin, ModelMeasure, SlayerModel
from slayer.core.query import ColumnRef, ComputedDimension, ModelExtension, OrderItem, SlayerQuery
from slayer.core.spine_names import TIME_SPINE_COLUMN, TIME_SPINE_COLUMN_TYPE, TIME_SPINE_MODEL
from slayer.core.syntax import respell_path_heads, text_paths
from slayer.core.warnings import NameCollisionWarning


def spine_model(*, data_source: str, wired: Optional[Sequence[SlayerModel]] = None) -> SlayerModel:
    """The virtual ``time_spine`` model; never stored, its rows are rendered as a bucket series.
    With ``wired`` (the datasource's models) its description is the full one naming each axis;
    without, a listing summary pointing at it."""
    status = (
        f"Built-in virtual model (not stored; every datasource has one): every instant. Group by "
        f"{TIME_SPINE_MODEL}.{TIME_SPINE_COLUMN} at a granularity to get every bucket in range, empty ones included."
    )
    if wired is None:
        details = (
            f" For the models wired to it and its rules: "
            f"inspect(reference='{data_source}.{TIME_SPINE_MODEL}', entity_type='model')."
        )
    else:
        axes = [f"{m.name} via {j.join_pairs[0][0]}" for m in wired if (j := axis_join(m)) is not None]
        details = (
            " Needs a lower bound (a date_range or a >= filter on the timestamp); the upper bound defaults to "
            "the current bucket. Each model is attributed through its axis — its default_time_dimension, "
            "else its only date/time column; it has no countable rows, and coalesce(<measure>, 0) fills empty "
            "buckets." + (f" Wired: {', '.join(axes)}." if axes else " No model is wired to it yet.")
        )
    return SlayerModel(
        name=TIME_SPINE_MODEL, data_source=data_source, sql_table=TIME_SPINE_MODEL,
        description=status + details,
        columns=[Column(name=TIME_SPINE_COLUMN, type=TIME_SPINE_COLUMN_TYPE, primary_key=True)],
    )


def is_spine(model: SlayerModel) -> bool:
    return model.name == TIME_SPINE_MODEL


def is_spine_spelling(name: str) -> bool:
    """``name`` equals the spine's name in any case."""
    return name.casefold() == TIME_SPINE_MODEL


def spine_variant_warnings(model: SlayerModel) -> List[NameCollisionWarning]:
    """The name-collision warning of a stored model named like the spine in another case."""
    if model.name == TIME_SPINE_MODEL or not is_spine_spelling(model.name):
        return []
    return [NameCollisionWarning(
        model=model.name, namespace="datasource's model names", names=sorted([model.name, TIME_SPINE_MODEL]),
    )]


class _SpineRespeller:
    """Respells the unquoted spine spellings a query writes, bar those ``keep`` holds, recording each one."""

    def __init__(self, keep: Callable[[str], bool]) -> None:
        self.keep = keep
        self.found: Set[str] = set()

    def name(self, spelling: str) -> Optional[str]:
        if spelling == TIME_SPINE_MODEL or not is_spine_spelling(spelling) or self.keep(spelling):
            return None
        self.found.add(spelling)
        return TIME_SPINE_MODEL

    def text(self, text: str) -> str:
        return respell_path_heads(text, respell=self.name)

    def ref(self, ref: ColumnRef) -> ColumnRef:
        new = self.name(ref.parts[0]) if len(ref.parts) > 1 and not ref.quoted_parts[0] else None
        return ref if new is None else ref.model_copy(update={"model": ".".join([new, *ref.parts[1:-1]])})

    def joins(self, joins: List[ModelJoin]) -> List[ModelJoin]:
        return [j.model_copy(update={"target_model": self.name(j.target_model) or j.target_model}) for j in joins]

    def extension(self, ext: ModelExtension) -> ModelExtension:
        return ext.model_copy(update={
            "source_name": self.name(ext.source_name) or ext.source_name,
            "joins": self.joins(ext.joins) if ext.joins is not None else None,
        })

    def dimension(self, dim: ColumnRef | ComputedDimension) -> ColumnRef | ComputedDimension:
        if isinstance(dim, ColumnRef):
            return self.ref(dim)
        return dim.model_copy(update={"expression": self.text(dim.expression)})

    def measure(self, measure: ModelMeasure) -> ModelMeasure:
        return measure.model_copy(update={"formula": self.text(measure.formula)})

    def order(self, item: OrderItem) -> OrderItem:
        raw = self.text(item.raw_formula) if item.raw_formula is not None else None
        return item.model_copy(update={"column": self.ref(item.column), "raw_formula": raw})


def respell_spine(query: SlayerQuery, *, keep: Callable[[str], bool]) -> Tuple[SlayerQuery, frozenset[str]]:
    """``query`` with each unquoted spelling of the spine in a model-name position, bar those ``keep`` holds, spelled
    ``time_spine``; and the spellings respelled."""
    r = _SpineRespeller(keep)
    update: Dict[str, Any] = {}
    if query.source_model is not None:
        update["source_model"] = r.name(query.source_model) or query.source_model
    if query.extensions is not None:
        update["extensions"] = [r.extension(e) for e in query.extensions]
    if query.dimensions is not None:
        update["dimensions"] = [r.dimension(d) for d in query.dimensions]
    if query.time_dimensions is not None:
        update["time_dimensions"] = [
            td.model_copy(update={"dimension": r.text(td.dimension)}) for td in query.time_dimensions]
    if query.measures is not None:
        update["measures"] = [r.measure(m) for m in query.measures]
    if query.filters is not None:
        update["filters"] = [r.text(f) for f in query.filters]
    if query.order is not None:
        update["order"] = [r.order(o) for o in query.order]
    if query.main_time_dimension is not None:
        update["main_time_dimension"] = r.text(query.main_time_dimension)
    return (query.model_copy(update=update) if r.found else query), frozenset(r.found)


def axis_column(model: SlayerModel) -> Optional[str]:
    """``model``'s axis, a P factor's included: its default time column, else its sole one."""
    axis = None if is_spine(model) else model.effective_default_time_dimension
    return axis if axis is not None and model.get_column(axis) is not None else None


def axis_join(model: SlayerModel) -> Optional[ModelJoin]:
    """``model``'s virtual many-to-one edge from its axis to the spine, when it has an axis."""
    axis = None if model.population_spine else axis_column(model)
    if axis is None:
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


def reads_spine(text: str, *, root: Optional[str]) -> bool:
    """A Mode-B text reading the spine by a dotted path from ``root``."""
    return any(len(path) > 1 and path[0] == TIME_SPINE_MODEL for path in text_paths(text, root=root))


def is_spine_query(query: SlayerQuery) -> bool:
    """A query rooted at the spine, or reading it in a time dimension, dimension or filter."""
    dims = [getattr(d, "expression", None) or getattr(d, "full_name", "") for d in query.dimensions or []]
    tds = [td.dimension for td in query.time_dimensions or []]
    root = query.source_model_name
    return root == TIME_SPINE_MODEL or any(
        reads_spine(t, root=root) for t in [*dims, *tds, *(query.filters or [])]
    )


def query_host(model: SlayerModel, *, query: SlayerQuery) -> SlayerModel:
    """``model`` as ``query``'s host: a spine query's P is the factor of ``time_spine × P``."""
    return model.as_population_factor() if is_spine_query(query) and not is_spine(model) else model
