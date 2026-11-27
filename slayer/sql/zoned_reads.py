"""The one read path of a dataset column (system principle 12): under a stage's zone, a timestamp read is converted
once, here, to the zone's wall clock; the predicates defining a dataset read stored values."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Literal, Optional
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.time_spine import is_spine
from slayer.core.time_zones import CIVIL
from slayer.core.warnings import TimeZoneUndeclaredWarning
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects.base import SqlDialect

Context = Literal["value", "stored"]
VALUE: Context = "value"
STORED: Context = "stored"
Outcome = Literal["converted", "civil", "undeclared", "stored", "unzoned"]

#: Meta key of a read's tag on its ``exp.Column``.
READ_TAG = "slayer_zoned_read"
#: Meta key of a read awaiting the read path: its ``ReadSite``.
SITE_MARK = "slayer_read_site"


class ReadTag(BaseModel):
    """How the read path read a column: its context and its zone outcome."""

    model_config = ConfigDict(frozen=True)

    context: Context
    outcome: Outcome


class ReadSite(BaseModel):
    """One column read: the dataset it reads, the field mapping that column (``None``: no field maps it), its
    root-relative join path and its context."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    dataset: SlayerDataset
    field: Optional[DatasetField] = None
    column: str
    path: tuple[str, ...] = ()
    context: Context = VALUE
    #: Without a zone, an instant read here reads as the dialect's naive type (not inside a definition or a key).
    reads_naive: bool = True


class ZoneSetting(BaseModel):
    """What a stage's reads convert by: its zone, its datasource's default source zone and its stage datasets."""

    model_config = ConfigDict(frozen=True)

    datasource_zone: Optional[str] = None
    stages: frozenset[str] = frozenset()


_warnings: ContextVar[Optional[dict[tuple, TimeZoneUndeclaredWarning]]] = ContextVar(
    "slayer_zone_warnings", default=None,
)


@contextmanager
def collect_zone_warnings() -> Generator[list[TimeZoneUndeclaredWarning], None, None]:
    """Collect every undeclared read rendered inside the block, once per dataset, field and join path."""
    sink: dict[tuple, TimeZoneUndeclaredWarning] = {}
    token = _warnings.set(sink)
    out: list[TimeZoneUndeclaredWarning] = []
    try:
        yield out
    finally:
        _warnings.reset(token)
        out.extend(sink.values())


def _warn_undeclared(site: ReadSite, *, zone: str) -> None:
    sink = _warnings.get()
    name = site.field.name if site.field is not None else site.column
    key = (site.dataset.name, name, site.path)
    if sink is not None and key not in sink:
        sink[key] = TimeZoneUndeclaredWarning(dataset=site.dataset.name, field=name, path=list(site.path),
                                              time_zone=zone)


def read_tag(node: Expression) -> Optional[ReadTag]:
    tag = node.meta.get(READ_TAG)
    return tag if isinstance(tag, ReadTag) else None


def _tagged(column: Expression, *, context: Context, outcome: Outcome) -> None:
    target = column if isinstance(column, exp.Column) else column.find(exp.Column)
    if target is not None:
        target.meta[READ_TAG] = ReadTag(context=context, outcome=outcome)


def source_zone(field: DatasetField, *, dataset: SlayerDataset, setting: ZoneSetting) -> Optional[str]:
    """A ``DateTime`` field's source zone (an IANA zone or ``civil``); ``None``: undeclared."""
    if field.time_zone is not None:
        return field.time_zone
    return None if dataset.name in setting.stages else setting.datasource_zone


def field_for_column(dataset: SlayerDataset, column: str) -> Optional[DatasetField]:
    """The field reading source column ``column`` (the one named so first); ``None`` when none does."""
    readers = [f for f in dataset.fields if f.is_base and f.physical_name == column]
    return next((f for f in readers if f.name == column), readers[0] if readers else None)


def read_column(column: exp.Column, *, site: ReadSite, dialect: SqlDialect, setting: ZoneSetting) -> Expression:
    """``column`` as SLayer reads it at ``site``: under the dialect's zone, a timestamp value converted to its wall
    clock (``civil`` and undeclared ones as stored, the latter with a warning); a stored predicate's as stored."""
    field = site.field
    if (field is not None and field.datatype is DataType.TIME
            and site.dataset.name not in setting.stages):
        return dialect.read_time_of_day(column)  # no zone: a time of day is never converted
    instant = field is not None and dialect.holds_instant(datatype=field.datatype, db_type=field.db_type)
    zone = dialect.time_zone
    if zone is None or site.context == STORED:
        return _read_as_stored(column, site=site, dialect=dialect, setting=setting, instant=instant)
    if field is None:
        return _read_undeclared(column, site=site, zone=zone, setting=setting)
    if instant:
        _tagged(column, context=VALUE, outcome="converted")
        return dialect.convert_zone(column, source=None, target=ZoneInfo(zone))
    if field.datatype is not DataType.DATETIME or is_spine(site.dataset):
        # The spine's buckets are generated in the stage's own wall clock.
        _tagged(column, context=VALUE, outcome="civil")
        return dialect.read_column(column, zoned=False)
    source = source_zone(field, dataset=site.dataset, setting=setting)
    if source == CIVIL:
        _tagged(column, context=VALUE, outcome="civil")
        return dialect.civil_read(column)
    if source is None:
        return dialect.civil_read(_read_undeclared(column, site=site, zone=zone, setting=setting))
    _tagged(column, context=VALUE, outcome="converted")
    if source == zone and site.dataset.name in setting.stages:
        return column  # a stage output already in this zone
    return dialect.convert_zone(column, source=ZoneInfo(source), target=ZoneInfo(zone))


def _read_as_stored(
    column: exp.Column, *, site: ReadSite, dialect: SqlDialect, setting: ZoneSetting, instant: bool,
) -> Expression:
    _tagged(column, context=site.context, outcome="unzoned" if dialect.time_zone is None else "stored")
    naive = instant and site.reads_naive and site.dataset.name not in setting.stages
    return dialect.read_column(column, zoned=naive)


def _read_undeclared(column: exp.Column, *, site: ReadSite, zone: str, setting: ZoneSetting) -> Expression:
    if site.dataset.name not in setting.stages:
        _warn_undeclared(site, zone=zone)
    _tagged(column, context=VALUE, outcome="undeclared")
    return column


def mark_read(column: exp.Column, site: ReadSite) -> exp.Column:
    """``column`` marked as a read at ``site``, for ``apply_reads``."""
    column.meta[SITE_MARK] = site
    return column


def apply_reads(tree: Expression, *, dialect: SqlDialect, setting: ZoneSetting) -> Expression:
    """``tree`` with every marked column read through the read path."""
    marked = [c for c in tree.find_all(exp.Column) if isinstance(c.meta.get(SITE_MARK), ReadSite)]
    for column in marked:
        site = column.meta.pop(SITE_MARK)
        stub = exp.Placeholder()
        is_root = column is tree
        column.replace(stub)
        read = read_column(column, site=site, dialect=dialect, setting=setting)
        if is_root:
            return read
        stub.replace(read)
    return tree


def zone_setting(bundle: ResolvedSourceBundle) -> ZoneSetting:
    """What the reads of the stage ``bundle`` resolves convert by."""
    return ZoneSetting(datasource_zone=bundle.datasource_time_zone, stages=frozenset(bundle.stage_displays))
