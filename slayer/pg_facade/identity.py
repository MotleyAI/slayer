"""Server-identity constants for the Postgres facade (DEV-1486).

What the facade reports as its Postgres version and connection parameters.
BI tools branch on ``server_version`` / ``version()``; we present as
PostgreSQL 14.0 while identifying SLayer in a trailing comment so logs and
screenshots make the facade obvious.
"""

from __future__ import annotations

from typing import List, Tuple

import slayer

PG_SERVER_VERSION = "14.0"


def version_string() -> str:
    """The ``version()`` / ``SELECT version()`` string."""
    return (
        f"PostgreSQL {PG_SERVER_VERSION} "
        f"(SLayer Postgres facade {slayer.__version__}) on slayer-semantic-layer"
    )


def parameter_status_defaults() -> List[Tuple[str, str]]:
    """The ParameterStatus burst sent after auth, before the first
    ReadyForQuery. UTC + UTF8 keep timestamp / encoding handling unambiguous."""
    return [
        ("server_version", PG_SERVER_VERSION),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("IntervalStyle", "postgres"),
        ("TimeZone", "UTC"),
        ("standard_conforming_strings", "on"),
        ("integer_datetimes", "on"),
    ]
