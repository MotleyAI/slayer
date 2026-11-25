"""The zone state of a stage's timestamp output (queries/time-zones): what its timestamp reads make it, so a reader
in another zone converts it — undeclared when any read is, else the stage's zone when any read converts, else civil."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import Literal, Optional

from slayer.core.enums import DataType
from slayer.core.errors import AmbiguousRelationshipPathError
from slayer.core.keys import AggregateKey, FieldKey, FieldSqlKey, ValueKey, field_leaf
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.relationship_walker import follow
from slayer.core.time_spine import is_spine
from slayer.core.time_zones import CIVIL
from slayer.engine.field_dependency import value_reads

ReadKind = Literal["converted", "civil", "undeclared"]


class _Reads:
    """The timestamp reads of one stage's outputs."""

    def __init__(
        self, *, datasets_by_name: Mapping[str, SlayerDataset], datasource_zone: Optional[str], stages: frozenset[str],
    ) -> None:
        self.datasets_by_name = dict(datasets_by_name)
        self.datasource_zone = datasource_zone
        self.stages = stages

    def column(self, dataset: SlayerDataset, field: Optional[DatasetField], seen: frozenset) -> Iterator[ReadKind]:
        """How a read of ``field`` on ``dataset`` reads (``field`` ``None``: an unmapped source column)."""
        if field is None:
            yield "civil" if dataset.name in self.stages else "undeclared"
            return
        if field.needs_expansion:
            yield from self.derived(dataset, field, seen)
            return
        if field.datatype is DataType.DATETIME_TZ:
            yield "converted"
        elif field.datatype is DataType.DATETIME and not is_spine(dataset):
            zone = field.time_zone if field.time_zone is not None or dataset.name in self.stages \
                else self.datasource_zone
            yield "civil" if zone == CIVIL else ("undeclared" if zone is None else "converted")

    def derived(self, dataset: SlayerDataset, field: DatasetField, seen: frozenset) -> Iterator[ReadKind]:
        key = (dataset.name, field.name)
        if key in seen:
            return
        for target, name, column in value_reads(field, host=dataset, reachable=self.datasets_by_name):
            read = _column_reader(target, name) if column else target.get_field(name)
            if column and read is None:
                yield "undeclared"
                continue
            yield from self.column(target, read, seen | {key})


def _column_reader(dataset: SlayerDataset, column: str) -> Optional[DatasetField]:
    readers = [f for f in dataset.fields if f.is_base and f.physical_name == column]
    return next((f for f in readers if f.name == column), readers[0] if readers else None)


def _value_leaves(key: ValueKey) -> Iterator[FieldKey | FieldSqlKey]:
    """The field reads ``key``'s value comes from: an aggregate's value only, never its ranking key or parameters."""
    if isinstance(key, (FieldKey, FieldSqlKey)):
        yield key
        return
    if isinstance(key, AggregateKey):
        if isinstance(key.source, (FieldKey, FieldSqlKey)) or hasattr(key.source, "children"):
            yield from _value_leaves(key.source)
        return
    for child in key.children():
        yield from _value_leaves(child)


def output_time_zone(
    key: ValueKey, *, root: Optional[SlayerDataset], datasets_by_name: Mapping[str, SlayerDataset],
    zone: Optional[str], datasource_zone: Optional[str], stages: frozenset[str],
) -> Optional[str]:
    """The zone state of a stage output ``key`` in ``zone``: that zone, ``civil``, or ``None`` (undeclared, or no
    zone)."""
    if zone is None or root is None:
        return None
    reads = _Reads(datasets_by_name=datasets_by_name, datasource_zone=datasource_zone, stages=stages)
    kinds: set[ReadKind] = set()
    for leaf in _value_leaves(key):
        try:
            chain = follow(root=root, path=leaf.path, datasets_by_name=dict(datasets_by_name))
        except AmbiguousRelationshipPathError:
            chain = None
        owner = (datasets_by_name.get(chain[-1].to) if chain else root) if chain is not None else None
        if owner is None:
            kinds.add("undeclared")
            continue
        kinds.update(reads.column(owner, owner.get_field(field_leaf(leaf)), frozenset()))
    if "undeclared" in kinds:
        return None
    return zone if "converted" in kinds else CIVIL
