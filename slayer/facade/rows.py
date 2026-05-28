"""``RowBatch`` — the facade-neutral, pyarrow-free result shape.

Probe queries, INFORMATION_SCHEMA, and pg_catalog all produce a small canned
result set. Instead of building a ``pyarrow.Table`` (which would force a
pyarrow dependency on the Postgres facade), they return a ``RowBatch``:
a typed column list plus a list of row dicts. Each facade renders it into its
own wire format — the Flight facade wraps it into a ``pyarrow.Table`` at the
edge; the Postgres facade walks it into ``RowDescription`` + ``DataRow``
messages.
"""

from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType


class FacadeColumn(BaseModel):
    name: str
    type: DataType


class RowBatch(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    columns: List[FacadeColumn]
    rows: List[Dict[str, Any]]
