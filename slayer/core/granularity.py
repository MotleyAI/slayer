"""Custom granularities: datasource definitions, the resolved granularity type, and nesting."""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated, Any, Iterable, Mapping, Optional, Union

from pydantic import BaseModel, BeforeValidator, model_validator

from slayer.core.enums import (
    GRANULARITY_NAMES,
    SUB_DAY_GRANULARITIES,
    UNIT_MONTHS,
    UNIT_SECONDS,
    GranularityParts,
    TimeGranularity,
    boundaries_nest,
    natural_origin,
)

class CustomGranularity(BaseModel, frozen=True):
    """A named granularity: boundaries at ``origin + k × multiple × base``. Lenient at
    construction; ``definition_error`` states the save-time rules."""

    name: str
    base: str
    multiple: int = 1
    origin: Optional[datetime] = None

    @model_validator(mode="before")
    @classmethod
    def _default_origin(cls, data: Any) -> Any:
        if isinstance(data, dict) and data.get("origin") is None and data.get("base") in GRANULARITY_NAMES:
            return {**data, "origin": natural_origin(TimeGranularity(data["base"]))}
        return data

    def __str__(self) -> str:
        return self.name

    @property
    def unit(self) -> TimeGranularity:
        return TimeGranularity(self.base)

    @property
    def anchor(self) -> datetime:
        return self.origin if self.origin is not None else natural_origin(self.unit)


# A resolved granularity: built-in, or a datasource definition.
Granularity = Union[TimeGranularity, CustomGranularity]


def _coerce_spec(value: Any) -> Any:
    if isinstance(value, CustomGranularity):
        return value.name
    if isinstance(value, str) and value.lower() in GRANULARITY_NAMES:
        return TimeGranularity(value.lower())
    return value


# A granularity as written: a built-in member, else a datasource granularity name.
GranularitySpec = Annotated[Union[TimeGranularity, str], BeforeValidator(_coerce_spec)]


def granularity_key(name: str) -> str:
    return name.casefold()


def resolve_granularity(
    spec: "GranularitySpec | Granularity", *, defined: Mapping[str, CustomGranularity],
) -> Optional[Granularity]:
    """``spec`` resolved against a datasource's definitions (keyed by ``granularity_key``)."""
    if isinstance(spec, (TimeGranularity, CustomGranularity)):
        return spec
    lowered = spec.lower()
    if lowered in GRANULARITY_NAMES:
        return TimeGranularity(lowered)
    return defined.get(granularity_key(spec))


def granularity_parts(g: Granularity) -> GranularityParts:
    """``(base, multiple, origin)``; a built-in is its own base once, naturally aligned."""
    if isinstance(g, CustomGranularity):
        return g.unit, g.multiple, g.anchor
    return g, 1, natural_origin(g)


def is_sub_day(g: Granularity) -> bool:
    return granularity_parts(g)[0] in SUB_DAY_GRANULARITIES


def nests_into(fine: Granularity, coarse: Granularity) -> bool:
    """Every ``coarse`` boundary is a ``fine`` boundary (boundary containment)."""
    return boundaries_nest(fine=granularity_parts(fine), coarse=granularity_parts(coarse))


def definition_error(g: CustomGranularity, *, reserved: Mapping[str, str]) -> Optional[str]:
    """The save-time rule ``g`` violates, else ``None``; ``reserved`` maps a taken name to its kind."""
    if not g.name.isidentifier():
        return "the name must be an identifier"
    taken = reserved.get(g.name.lower())
    if taken is not None:
        return f"the name is taken by a built-in {taken}"
    if g.base not in GRANULARITY_NAMES:
        return f"base {g.base!r} is not a built-in granularity ({', '.join(t.value for t in TimeGranularity)})"
    if g.multiple < 1:
        return f"multiple must be an integer >= 1, got {g.multiple}"
    return _origin_error(g.anchor, unit=g.unit)


def _origin_error(origin: datetime, *, unit: TimeGranularity) -> Optional[str]:
    if origin.tzinfo is not None:
        return "origin must not carry a time zone"
    if origin.microsecond:
        return "origin must have second precision (no sub-second part)"
    if unit not in SUB_DAY_GRANULARITIES and origin.time() != datetime.min.time():
        return f"a {unit.value} base must have an origin without a time of day"
    if unit in UNIT_MONTHS and origin.day > 28:
        return f"a {unit.value} base must have an origin day of month of at most 28"
    if unit in SUB_DAY_GRANULARITIES and int((origin - natural_origin(unit)).total_seconds()) % UNIT_SECONDS[unit]:
        return f"origin must align to a {unit.value} boundary"
    return None


def unknown_granularity_message(*, name: str, defined: Iterable[CustomGranularity], where: str) -> str:
    """The unknown-granularity summary: the name, where it was used, and every usable name."""
    custom = sorted(g.name for g in defined)
    listed = ", ".join(t.value for t in TimeGranularity)
    extra = f"; defined on this datasource: {', '.join(custom)}" if custom else "; this datasource defines none"
    return f"Unknown granularity {name!r} in {where}. Built-in granularities: {listed}{extra}."


def origin_as_date(g: CustomGranularity) -> date | datetime:
    """The origin as a date when it is midnight, else as a timestamp."""
    anchor = g.anchor
    return anchor.date() if anchor.time() == datetime.min.time() else anchor
