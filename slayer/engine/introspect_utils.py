"""The engine's one door to a live table's structure: columns and keys through the datasource's client.

Free of ``ingestion`` so ``query_engine`` (which ``ingestion`` imports) can use it too.
"""

from __future__ import annotations

from slayer.engine.schema_scope import SchemaRef
from slayer.sql.client import SqlClient
from slayer.sql.dialects import ReflectedColumn, SqlDialect, TableKeys, dialect_for_ds_type
from slayer.sql.dialects.reflection import TableLocation


def dialect_of(client: SqlClient) -> SqlDialect:
    """The dialect of the client's datasource."""
    return dialect_for_ds_type(client.datasource.type)


def table_location(table_name: str, ref: SchemaRef | None) -> TableLocation:
    """``table_name`` (in SQLAlchemy's spelling) in ``ref``'s schema; an unset part is the connection's current one."""
    return TableLocation(
        name=table_name, schema_name=ref.live_name if ref else None, catalog=ref.catalog if ref else None,
    )


async def reflect_columns(*, client: SqlClient, table_name: str, ref: SchemaRef | None) -> list[ReflectedColumn]:
    return await client.reflect_columns(table_location(table_name, ref))


async def reflect_table_keys(*, client: SqlClient, table_name: str, ref: SchemaRef | None) -> TableKeys:
    return await client.reflect_table_keys(table_location(table_name, ref))
