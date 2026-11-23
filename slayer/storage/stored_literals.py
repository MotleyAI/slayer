"""A stored query's literals typed by the column the binder reads them against (``models/stored-upgrade``): legacy time
literals repaired, and date-only comparisons given their whole-day meaning. A comparison it cannot type is left."""

from __future__ import annotations

from typing import Any, Callable, Optional, Sequence

from pydantic import BaseModel, ConfigDict

from slayer.core.date_only import scan_date_only
from slayer.core.enums import DataType, TypeKind
from slayer.core.errors import SlayerError
from slayer.core.legacy_spellings import rewrite_spans
from slayer.core.models import SlayerDataset
from slayer.core.names import match_name
from slayer.core.time_spine import TIME_SPINE_DATASET, is_spine_spelling
from slayer.engine.extensions import resolve_path
from slayer.storage.date_only_migration import StoredDatasets, upgraded_extension_texts, warn_untyped
from slayer.storage.document_keys import CURRENT_KEYS
from slayer.storage.extension_fields import extension_field_type
from slayer.storage.legacy_time_literals import legacy_literal_comparisons, repaired_literals
from slayer.storage.qualified_references import StageReading
from slayer.storage.rank_direction_migration import rewrite_query_fields

__all__ = ["LiteralGates", "typed_stage"]

_TEMPORAL = frozenset({TypeKind.DATE, TypeKind.TIMESTAMP})


class LiteralGates(BaseModel):
    """The literal rewrites a stored stage's own version calls for."""

    model_config = ConfigDict(frozen=True)

    legacy: bool = False
    whole_day: bool = False

    @property
    def any(self) -> bool:
        return self.legacy or self.whole_day


def typed_stage(
    stage: dict, *, gates: LiteralGates, reading: StageReading, extension_types: Optional[StoredDatasets],
    label: Optional[str],
) -> dict:
    """A migrated raw stored ``stage`` with its gated rewrites applied, each compared column typed from ``reading``; its
    extensions' own texts typed with ``extension_types`` (``None``: none hold a date-only literal)."""
    columns = _Columns(stage=stage, reading=reading)
    out = dict(stage)
    if gates.legacy:
        out = _with_filters(out, columns.repaired)
    if gates.whole_day:
        out = rewrite_query_fields(out, lambda text: columns.whole_day(text, label=label), CURRENT_KEYS)
        if extension_types is not None:
            out = upgraded_extension_texts(out, datasets=extension_types, label=label)
    return out


def _with_filters(stage: dict, fn: Callable[[str], str]) -> dict:
    filters: Any = stage.get(CURRENT_KEYS.query_filter)
    if isinstance(filters, str):
        return {**stage, CURRENT_KEYS.query_filter: fn(filters)}
    if isinstance(filters, list):
        return {**stage, CURRENT_KEYS.query_filter: [fn(f) if isinstance(f, str) else f for f in filters]}
    return stage


class _Columns:
    """A stage's compared columns, typed as the binder reads them from its population."""

    def __init__(self, *, stage: dict, reading: StageReading) -> None:
        self.stage = stage
        self.reading = reading

    def type_of(self, parts: Sequence[str], quoted: Sequence[bool]) -> Optional[DataType]:
        """The type of the column ``parts`` names; ``None`` when unresolvable or untyped."""
        host = self.reading.host
        if host is None or not parts:
            return None
        flags = tuple(quoted) + (False,) * (len(parts) - len(quoted))
        try:
            target = self._target(tuple(parts[:-1]), quoted=flags[:-1], host=host)
            if target is None:
                return None
            declared = extension_field_type(self.stage, dataset=target, datasets=self.reading.datasets,
                                            leaf=parts[-1], quoted=flags[-1])
            if declared is not None:
                return declared
            found = match_name(name=parts[-1], declared=[f.name for f in target.fields], quoted=flags[-1])
        except SlayerError:
            return None
        return next((f.datatype for f in target.fields if f.name == found), None)

    def _target(self, hops: tuple[str, ...], *, quoted: tuple[bool, ...], host: SlayerDataset) -> Optional[SlayerDataset]:
        if not hops:
            return host
        datasets = self.reading.datasets
        if not quoted[0] and hops[0] != TIME_SPINE_DATASET and hops[0] not in datasets and is_spine_spelling(hops[0]):
            # The spine in another case, untypable beside a stored dataset spelt like it.
            if any(n != TIME_SPINE_DATASET and is_spine_spelling(n) for n in datasets):
                return None
            hops = (TIME_SPINE_DATASET, *hops[1:])
        resolved = resolve_path(hops, root=host, datasets_by_name=datasets, quoted=quoted)
        if resolved is None:
            return None
        chain = resolved[1]
        return self.reading.datasets.get(chain[-1].to) if chain else host

    def _kind(self, parts: Sequence[str], quoted: Sequence[bool]) -> Optional[TypeKind]:
        found = self.type_of(parts, quoted)
        return found.kind if found is not None else None

    def repaired(self, text: str) -> str:
        """``text`` with each legacy literal compared with a DATE or TIMESTAMP column repaired."""
        found = [c for c in legacy_literal_comparisons(text) if self._kind(c.parts, c.quoted) in _TEMPORAL]
        return repaired_literals(text, found) if found else text

    def whole_day(self, text: str, *, label: Optional[str]) -> str:
        """``text`` with each date-only comparison of a TIMESTAMP column given its whole-day meaning; an untypable one
        logged."""
        scan = scan_date_only(text)
        if not scan.sites and not scan.unlocated:
            return text
        rewrite, untyped = [], scan.unlocated > 0
        for site in scan.sites:
            found = self.type_of(site.parts, site.quoted)
            if found is not None and found.kind is TypeKind.TIMESTAMP:
                rewrite.append(site)
            untyped = untyped or found is None
        if untyped:
            warn_untyped(label, text)
        return rewrite_spans(text, rewrite)
