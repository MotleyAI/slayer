"""Backward-compat shim — probe queries moved to ``slayer.facade.probe_queries``.

The shared matcher is pyarrow-free and returns a ``RowBatch``; this shim wraps
the result back into a ``pyarrow.Table`` for the Flight facade's callers/tests.
"""

from __future__ import annotations

from typing import Optional

import pyarrow as pa
import sqlglot.expressions as exp

from slayer.facade.probe_queries import match_probe as _shared_match_probe
from slayer.flight.types import row_batch_to_arrow


def match_probe(parsed: exp.Expression) -> Optional[pa.Table]:
    """Return the canned ``pyarrow.Table`` for a matching probe, else ``None``."""
    batch = _shared_match_probe(parsed)
    if batch is None:
        return None
    return row_batch_to_arrow(batch)


__all__ = ["match_probe"]
