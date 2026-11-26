"""The built-in time spine: a virtual per-datasource dataset of all instants, and its edges."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from slayer.core.enums import TEMPORAL_DATATYPES, RelationshipCardinality, JoinType
from slayer.core.errors import DefaultTimeDimensionTypeError
from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.core.query import DatasetExtension, OrderItem, QueryDimension, QueryMetric, SlayerQuery, map_filter
from slayer.core.spine_names import TIME_SPINE_FIELD, TIME_SPINE_FIELD_TYPE, TIME_SPINE_DATASET
from slayer.core.syntax import respell_path_heads, text_paths
from slayer.core.warnings import NameCollisionWarning


def spine_dataset(*, data_source: str, wired: Optional[Sequence[SlayerDataset]] = None) -> SlayerDataset:
    """The virtual ``time_spine`` dataset; never stored, its rows are rendered as a bucket series.
    With ``wired`` (the datasource's datasets) its description is the full one naming each axis;
    without, a listing summary pointing at it."""
    status = (
        f"Built-in virtual dataset (not stored; every datasource has one): every instant. Group by "
        f"{TIME_SPINE_DATASET}.{TIME_SPINE_FIELD} at a granularity to get every bucket in range, empty ones included."
    )
    if wired is None:
        details = (
            f" For the datasets wired to it and its rules: "
            f"inspect(reference='{data_source}.{TIME_SPINE_DATASET}', entity_type='dataset')."
        )
    else:
        axes = [f"{m.name} via {j.column_pairs[0][0]}" for m in wired if (j := axis_relationship(m)) is not None]
        details = (
            " Needs a lower bound (a date_range or a >= filter on the timestamp); the upper bound defaults to "
            "the current bucket. Each dataset is attributed through its axis — its default_time_dimension, "
            "else its only date/time field; it has no countable rows, and coalesce(<metric>, 0) fills empty "
            "buckets." + (f" Wired: {', '.join(axes)}." if axes else " No dataset is wired to it yet.")
        )
    return SlayerDataset(
        name=TIME_SPINE_DATASET, data_source=data_source, sql_table=TIME_SPINE_DATASET,
        description=status + details,
        fields=[DatasetField(name=TIME_SPINE_FIELD, datatype=TIME_SPINE_FIELD_TYPE, primary_key=True)],
    )


def is_spine(dataset: SlayerDataset) -> bool:
    return dataset.name == TIME_SPINE_DATASET


def is_spine_spelling(name: str) -> bool:
    """``name`` equals the spine's name in any case."""
    return name.casefold() == TIME_SPINE_DATASET


def spine_variant_warnings(dataset: SlayerDataset) -> List[NameCollisionWarning]:
    """The name-collision warning of a stored dataset named like the spine in another case."""
    if dataset.name == TIME_SPINE_DATASET or not is_spine_spelling(dataset.name):
        return []
    return [NameCollisionWarning(
        dataset=dataset.name, namespace="datasource's dataset names", names=sorted([dataset.name, TIME_SPINE_DATASET]),
    )]


class _SpineRespeller:
    """Respells the unquoted spine spellings a query writes, bar those ``keep`` holds, recording each one."""

    def __init__(self, keep: Callable[[str], bool]) -> None:
        self.keep = keep
        self.found: Set[str] = set()

    def name(self, spelling: str) -> Optional[str]:
        if spelling == TIME_SPINE_DATASET or not is_spine_spelling(spelling) or self.keep(spelling):
            return None
        self.found.add(spelling)
        return TIME_SPINE_DATASET

    def text(self, text: str) -> str:
        return respell_path_heads(text, respell=self.name)

    def relationships(self, relationships: List[DatasetRelationship]) -> List[DatasetRelationship]:
        return [j.model_copy(update={"to": self.name(j.to) or j.to}) for j in relationships]

    def extension(self, ext: DatasetExtension) -> DatasetExtension:
        return ext.model_copy(update={
            "dataset": self.name(ext.dataset) or ext.dataset,
            "relationships": self.relationships(ext.relationships) if ext.relationships is not None else None,
        })

    def dimension(self, dim: QueryDimension) -> QueryDimension:
        return dim.model_copy(update={"field": self.text(dim.field)})

    def metric(self, metric: QueryMetric) -> QueryMetric:
        return metric.model_copy(update={"metric": self.text(metric.metric)})

    def order(self, item: OrderItem) -> OrderItem:
        raw = self.text(item.raw_expression) if item.raw_expression is not None else None
        return item.model_copy(update={"field": self.text(item.field), "raw_expression": raw})


def respell_spine(query: SlayerQuery, *, keep: Callable[[str], bool]) -> Tuple[SlayerQuery, frozenset[str]]:
    """``query`` with each unquoted spelling of the spine in a dataset-name position, bar those ``keep`` holds, spelled
    ``time_spine``; and the spellings respelled."""
    r = _SpineRespeller(keep)
    update: Dict[str, Any] = {}
    if query.population is not None:
        update["population"] = r.name(query.population) or query.population
    if query.extensions is not None:
        update["extensions"] = [r.extension(e) for e in query.extensions]
    if query.dimensions is not None:
        update["dimensions"] = [r.dimension(d) for d in query.dimensions]
    if query.time_dimensions is not None:
        update["time_dimensions"] = [
            td.model_copy(update={"field": r.text(td.field)}) for td in query.time_dimensions]
    if query.metrics is not None:
        update["metrics"] = [r.metric(m) for m in query.metrics]
    if query.filter is not None:
        update["filter"] = map_filter(query.filter, r.text)
    if query.order is not None:
        update["order"] = [r.order(o) for o in query.order]
    if query.main_time_dimension is not None:
        update["main_time_dimension"] = r.text(query.main_time_dimension)
    return (query.model_copy(update=update) if r.found else query), frozenset(r.found)


def axis_field(dataset: SlayerDataset) -> Optional[str]:
    """``dataset``'s axis, a P factor's included: its default time field, else its sole one."""
    axis = None if is_spine(dataset) else dataset.effective_default_time_dimension
    return axis if axis is not None and dataset.get_field(axis) is not None else None


def check_default_time_dimension(dataset: SlayerDataset) -> None:
    """A declared ``default_time_dimension`` must name a date or timestamp field: it is the dataset's time axis."""
    field = dataset.get_field(dataset.default_time_dimension) if dataset.default_time_dimension else None
    if field is not None and field.datatype is not None and field.datatype not in TEMPORAL_DATATYPES:
        raise DefaultTimeDimensionTypeError(dataset=dataset.name, field=field.name, field_type=field.datatype)


def axis_relationship(dataset: SlayerDataset) -> Optional[DatasetRelationship]:
    """``dataset``'s virtual many-to-one edge from its axis to the spine, when it has an axis."""
    axis = None if dataset.population_spine else axis_field(dataset)
    if axis is None:
        return None
    return DatasetRelationship(
        to=TIME_SPINE_DATASET, from_columns=[axis], to_columns=[TIME_SPINE_FIELD],
        cardinality=RelationshipCardinality.MANY_TO_ONE,
    )


def spine_relationships(dataset: SlayerDataset, *, datasets_by_name: Mapping[str, SlayerDataset]) -> list[DatasetRelationship]:
    """``dataset``'s declared relationships plus its axis edge when the spine is in the universe."""
    if TIME_SPINE_DATASET not in datasets_by_name:
        return list(dataset.relationships)
    axis = axis_relationship(dataset)
    return [*dataset.relationships, axis] if axis is not None else list(dataset.relationships)


PRODUCT_JOIN_TYPE = JoinType.INNER


def names_spine(ref: str) -> bool:
    """A dotted reference into the spine dataset."""
    return ref.split(".")[0] == TIME_SPINE_DATASET


def reads_spine(text: str) -> bool:
    """A query text reading the spine: a dotted reference anchored at it."""
    return any(len(path) > 1 and path[0] == TIME_SPINE_DATASET for path in text_paths(text, root=None))


def is_spine_query(query: SlayerQuery) -> bool:
    """A query rooted at the spine, or reading it in a time dimension, dimension or filter."""
    dims = [d.field for d in query.dimensions or []]
    tds = [td.field for td in query.time_dimensions or []]
    return query.population_name == TIME_SPINE_DATASET or any(
        reads_spine(t) for t in [*dims, *tds, *(query.filter_conditions)]
    )


def query_host(dataset: SlayerDataset, *, query: SlayerQuery) -> SlayerDataset:
    """``dataset`` as ``query``'s host: a spine query's P is the factor of ``time_spine × P``."""
    return dataset.as_population_factor() if is_spine_query(query) and not is_spine(dataset) else dataset
