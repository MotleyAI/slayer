"""Time-zone names: IANA zones, the ``civil`` marker, and their validation."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from functools import lru_cache
from typing import Optional
from zoneinfo import ZoneInfo, available_timezones

from slayer.core.errors import TimeDimensionFieldError, TimeZoneError

#: A field's marker for zone-free civil time: never converted.
CIVIL = "civil"


@lru_cache(maxsize=1)
def _iana_zones() -> frozenset[str]:
    return frozenset(available_timezones() | {"UTC"})


def is_iana_zone(value: str) -> bool:
    return value in _iana_zones()


def check_zone(value: str | None, *, owner: str, civil: bool = False) -> str | None:
    """``value`` when it names an IANA zone (or ``civil`` where allowed), else a ``TimeZoneError``."""
    if value is None or is_iana_zone(value) or (civil and value == CIVIL):
        return value
    takes = "an IANA zone name or 'civil'" if civil else "an IANA zone name such as 'UTC' or 'America/New_York'"
    raise TimeZoneError(f"Unknown time zone {value!r} on {owner}; it takes {takes}.", zone=value)


def from_zone(value: datetime, zone: str) -> datetime:
    """Naive ``value`` read in ``zone`` as an aware instant: a skipped time takes the pre-transition offset, a
    repeated time its later occurrence (the Postgres rule)."""
    tz = ZoneInfo(zone)
    early, late = value.replace(tzinfo=tz, fold=0), value.replace(tzinfo=tz, fold=1)
    if early.utcoffset() == late.utcoffset():
        return early
    if early.astimezone(timezone.utc).astimezone(tz).replace(tzinfo=None) != value:
        return early
    return late


def wall_clock(instant: datetime, zone: str | None) -> datetime:
    """A clock reading as naive wall clock: ``zone``'s, else the host's (a naive reading is host-local)."""
    if zone is None:
        return instant if instant.tzinfo is None else instant.astimezone().replace(tzinfo=None)
    aware = instant if instant.tzinfo is not None else instant.astimezone()
    return aware.astimezone(ZoneInfo(zone)).replace(tzinfo=None)


def check_bucketed_zones(*, name: str, zone: Optional[str], bucketed_zones: Sequence[tuple[str, str]]) -> None:
    """A leaf bucketed in another zone re-buckets only in that zone."""
    if zone is None:
        return
    for leaf, bucketed in bucketed_zones:
        if bucketed == zone:
            continue
        field = "field" if leaf == name else f"field {leaf!r}"
        raise TimeDimensionFieldError(
            summary=f"Cannot re-bucket in {zone}: the {field} is already bucketed in {bucketed}, and a "
            f"{bucketed} bucket start is not a {zone} bucket boundary.",
            location=f"time dimension {name!r}",
            suggestion=f"Bucket the raw field instead, or run the stage that buckets it in {zone}.",
        )
