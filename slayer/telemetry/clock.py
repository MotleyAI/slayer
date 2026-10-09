"""Telemetry's clock (a seam: tests replace ``now``)."""

import datetime


def now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)
