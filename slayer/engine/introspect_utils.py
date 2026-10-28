"""The engine's one door to a live table's structure: columns and keys via the dialect's reflection hooks.

Free of ``ingestion`` so ``query_engine`` (which ``ingestion`` imports) can use it too.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa

from slayer.engine.schema_scope import (
    SchemaRef,
    default_schema_ref_for_engine,
    engine_qualifies_tokens,
)
from slayer.sql.dialects import ReflectedColumn, SqlDialect, TableKeys, dialect_for_sa_backend


def dialect_for_engine(sa_engine: Any) -> SqlDialect:
    """The dialect reflecting ``sa_engine``'s structure."""
    return dialect_for_sa_backend(getattr(getattr(sa_engine, "dialect", None), "name", None))


def _resolved_ref(sa_engine: sa.Engine | None, ref: SchemaRef | None) -> SchemaRef | None:
    """``ref``; ``None`` becomes the connection default where a bare read would sweep attached catalogs."""
    if ref is not None or sa_engine is None or not engine_qualifies_tokens(sa_engine):
        return ref
    try:
        return default_schema_ref_for_engine(sa_engine)
    except Exception:  # noqa: BLE001 — unresolvable (e.g. a mock): the hook scopes to the current one
        return None


def reflect_columns(
    *, sa_engine: sa.Engine | None, inspector: Any, table_name: str, ref: SchemaRef | None,
) -> list[ReflectedColumn]:
    ref = _resolved_ref(sa_engine, ref)
    return dialect_for_engine(sa_engine).reflect_columns(
        sa_engine=sa_engine, inspector=inspector, table=table_name,
        schema=ref.name if ref else None, catalog=ref.catalog if ref else None,
    )


def reflect_table_keys(
    *, sa_engine: sa.Engine | None, inspector: Any, table_name: str, ref: SchemaRef | None,
) -> TableKeys:
    ref = _resolved_ref(sa_engine, ref)
    return dialect_for_engine(sa_engine).reflect_table_keys(
        sa_engine=sa_engine, inspector=inspector, table=table_name,
        schema=ref.name if ref else None, catalog=ref.catalog if ref else None,
    )
