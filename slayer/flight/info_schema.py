"""Backward-compat shim — INFORMATION_SCHEMA moved to ``slayer.facade.info_schema``.

The shared builder is pyarrow-free and returns a ``RowBatch``; this shim wraps
the result back into a ``pyarrow.Table`` so the Flight facade's existing
callers (and tests) keep their Arrow-shaped contract.
"""

from __future__ import annotations

from typing import Optional

import pyarrow as pa
import sqlglot.expressions as exp

from slayer.facade.info_schema import (
    SUPPORTED_INFO_SCHEMA_TABLES,
    CATALOG_NAME,
    match_info_schema as _shared_match_info_schema,
)
from slayer.flight.catalog import FlightCatalog
from slayer.flight.types import row_batch_to_arrow


def match_info_schema(
    *, parsed: exp.Expression, catalog: FlightCatalog,
) -> Optional[pa.Table]:
    """Return the canned ``INFORMATION_SCHEMA.<table>`` answer as a
    ``pyarrow.Table`` or ``None``."""
    batch = _shared_match_info_schema(parsed=parsed, catalog=catalog)
    if batch is None:
        return None
    return row_batch_to_arrow(batch)


__all__ = [
    "CATALOG_NAME",
    "SUPPORTED_INFO_SCHEMA_TABLES",
    "match_info_schema",
]
