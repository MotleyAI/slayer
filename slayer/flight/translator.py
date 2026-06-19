"""Backward-compat shim — the translator moved to ``slayer.facade.translator``.

The SQL → SlayerQuery pipeline is now facade-agnostic and shared (DEV-1486).
The Flight facade speaks Arrow, so its probe / INFORMATION_SCHEMA results carry
a ``pyarrow.Table`` rather than the shared pyarrow-free ``RowBatch``. This shim
calls the shared ``translate`` with ``dialect=None`` and re-wraps the two
``RowBatch``-carrying results into Arrow-shaped ones, preserving the Flight
facade's historical contract (``result.table``).
"""

from __future__ import annotations

import pyarrow as pa
from pydantic import ConfigDict

from slayer.facade.catalog import FacadeCatalog
from slayer.facade.translator import (
    NoOpResult,
    QueryResult,
    READ_ONLY_MESSAGE,
    SELECT_STAR_MESSAGE,
    TranslationError,
    TranslatorResult,
)
from slayer.facade.translator import (
    InfoSchemaResult as _SharedInfoSchemaResult,
)
from slayer.facade.translator import (
    ProbeResult as _SharedProbeResult,
)
from slayer.facade.translator import translate as _shared_translate
from slayer.flight.types import row_batch_to_arrow


class ProbeResult(TranslatorResult):
    """Flight-shaped probe result carrying an Arrow ``pyarrow.Table``."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    table: pa.Table


class InfoSchemaResult(TranslatorResult):
    """Flight-shaped INFORMATION_SCHEMA result carrying a ``pyarrow.Table``."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    table: pa.Table


def translate(sql: str, catalog: FacadeCatalog) -> TranslatorResult:
    """Translate ``sql`` for the Flight facade (``dialect=None``), converting
    the shared ``RowBatch`` results into Arrow-shaped ones.

    ``allow_column_cast=False`` — DEV-1566 ``CAST(<col> AS <type>)`` projections
    are gated to the pg-facade. The Flight handler materialises rows via
    ``pa.Table.from_pylist`` against a catalog-typed schema, which rejects
    values whose Python type doesn't match the declared Arrow type
    (e.g. ``datetime.date`` against ``pa.timestamp``); Flight has no
    value-coercion pass to bridge the gap. Rejecting at translate time
    surfaces a clean ``TranslationError`` instead of an opaque
    ``ArrowTypeError`` at materialisation.
    """
    result = _shared_translate(sql, catalog, dialect=None, allow_column_cast=False)
    if isinstance(result, _SharedProbeResult):
        return ProbeResult(table=row_batch_to_arrow(result.batch))
    if isinstance(result, _SharedInfoSchemaResult):
        return InfoSchemaResult(table=row_batch_to_arrow(result.batch))
    return result


__all__ = [
    "InfoSchemaResult",
    "NoOpResult",
    "ProbeResult",
    "QueryResult",
    "READ_ONLY_MESSAGE",
    "SELECT_STAR_MESSAGE",
    "TranslationError",
    "TranslatorResult",
    "translate",
]
