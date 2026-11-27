"""The read check (system principle 12): every temporal column a statement reads from a dataset carries the read
path's tag. ``SLAYER_VALIDATE_ZONED_READS=1`` checks every statement the generator builds."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Optional

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.errors import SlayerError
from slayer.core.models import SlayerDataset
from slayer.sql.zoned_reads import ReadTag
from slayer.sql.zoned_reads import read_tag as _read_tag

_ENV_FLAG = "SLAYER_VALIDATE_ZONED_READS"
_TRUTHY = frozenset({"1", "true", "yes", "on"})
_TEMPORAL = (DataType.DATETIME, DataType.DATETIME_TZ)


class ZonedReadLeakError(SlayerError):
    """A temporal dataset column reached the SQL without passing the read path."""


def read_tag(node: Expression) -> Optional[ReadTag]:
    return _read_tag(node)


def _source(column: exp.Column) -> Optional[Expression]:
    """The FROM / JOIN relation ``column``'s qualifier names, in the nearest enclosing SELECT binding it."""
    select = column.find_ancestor(exp.Select)
    while select is not None:
        relations = [select.args.get("from_"), *(select.args.get("joins") or [])]
        for holder in relations:
            relation = holder.this if holder is not None else None
            if relation is not None and relation.alias_or_name == column.table:
                return relation
        select = select.find_ancestor(exp.Select)
    return None


def _temporal_columns(dataset: SlayerDataset) -> frozenset[str]:
    return frozenset(f.physical_name for f in dataset.fields if f.is_base and f.datatype in _TEMPORAL)


def check_zoned_reads(statement: Expression, *, datasets: Mapping[str, SlayerDataset], stamp: str) -> None:
    """Raise ``ZonedReadLeakError`` naming the first temporal read of a ``stamp``-ed dataset relation without a tag."""
    temporal = {name: _temporal_columns(d) for name, d in datasets.items()}
    for column in statement.find_all(exp.Column):
        if not column.table or read_tag(column) is not None:
            continue
        relation = _source(column)
        name = relation.meta.get(stamp) if relation is not None else None
        if name is not None and column.name in temporal.get(name, ()):
            raise ZonedReadLeakError(
                f"The temporal column {column.table}.{column.name} of dataset {name!r} is read without the read path "
                f"(system principle 12)."
            )


def maybe_check_zoned_reads(statement: Expression, *, datasets: Mapping[str, SlayerDataset], stamp: str) -> None:
    """``check_zoned_reads`` iff ``SLAYER_VALIDATE_ZONED_READS`` is truthy (read per call)."""
    value = os.environ.get(_ENV_FLAG)
    if value is not None and value.strip().lower() in _TRUTHY:
        check_zoned_reads(statement, datasets=datasets, stamp=stamp)
