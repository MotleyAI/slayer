"""Storage-driven type refinement of a legacy dataset dict against its own live table.

Non-SQLite: a DOUBLE base field whose live type is integer narrows to INT.
SQLite (declared types are only affinity hints): the integer-affinity probe
widens a persisted INT that stores non-integers to DOUBLE / TEXT, and narrows
a persisted DOUBLE to INT only when the probe certifies integer storage; the
auto-default integer ``format`` follows the new type, a custom one is kept.

Every read goes through the loading engine's client; an unreachable datasource
raises, so storage callers see what a query against a down datasource would.
"""

from __future__ import annotations

import logging


from slayer.core.enums import DataType, TypeKind, kind_of, stored_datatype
from slayer.core.models import DatasourceConfig, is_base_field_sql, physical_column_sql
from slayer.live_schema.introspect_utils import table_location
from slayer.live_schema.schema_scope import UNREACHABLE, schema_ref_from_token, sql_table_location, table_schema_ref
from slayer.database import sqlite_introspect
from slayer.database.client import ObjectNotFound, SqlClient
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.reflection import TableLocation


logger = logging.getLogger(__name__)


def _field_is_base(sql: object) -> bool:
    """A field dict's ``sql`` claims a live column (see ``is_base_field_sql``)."""
    return (sql is None or isinstance(sql, str)) and is_base_field_sql(sql)


def _physical(col: dict) -> str | None:
    """The live column a base field dict names, else ``None``."""
    name, sql = col.get("name"), col.get("sql")
    if not isinstance(name, str) or not _field_is_base(sql):
        return None
    return physical_column_sql(sql=sql, name=name)


def _is_sqlite_datasource(datasource: DatasourceConfig) -> bool:
    return (datasource.type or "").lower() == "sqlite"


def _is_auto_default_integer_format_dict(fmt: dict | None) -> bool:
    """Dict-shape mirror of :func:`slayer.live_schema.ingestion._is_auto_default_integer_format`."""
    if not isinstance(fmt, dict):
        return False
    if fmt.get("type") != "integer":
        return False
    return (
        fmt.get("precision") in (None, "")
        and fmt.get("symbol") in (None, "")
    )


def _format_for_widened_type_dict(verdict: DataType) -> dict | None:
    if verdict is DataType.FLOAT:
        return {"type": "float"}
    return None  # TEXT clears format


def _is_double(value: object) -> bool:
    """A stored datatype of the double kind."""
    return kind_of(stored_datatype(value)) is TypeKind.DOUBLE


def has_refineable_fields(d: dict) -> bool:
    """Whether ``d`` is a table-backed dataset dict with a DOUBLE base field (refinement needs the datasource)."""
    if not isinstance(d, dict):
        return False
    sql_table = d.get("sql_table")
    if not isinstance(sql_table, str) or not sql_table:
        return False
    fields = d.get("fields")
    if not isinstance(fields, list) or not fields:
        return False
    return any(
        isinstance(c, dict)
        and _is_double(c.get("datatype"))
        and _field_is_base(c.get("sql"))
        for c in fields
    )


def has_sqlite_widenable_fields(d: dict) -> bool:
    """Whether ``d`` is a table-backed dataset dict with an INT base field (SQLite widening; best-effort)."""
    if not isinstance(d, dict):
        return False
    sql_table = d.get("sql_table")
    if not isinstance(sql_table, str) or not sql_table:
        return False
    columns = d.get("fields")
    if not isinstance(columns, list) or not columns:
        return False
    return any(
        isinstance(c, dict)
        and stored_datatype(c.get("datatype")) is DataType.INTEGER
        and _field_is_base(c.get("sql"))
        for c in columns
    )


def _parse_sql_table_with_default_schema(
    sql_table: str, datasource: DatasourceConfig,
) -> tuple[str | None, str]:
    """``sql_table``'s ``(schema, table)`` live names; unqualified → ``datasource.schema_name``."""
    at = sql_table_location(sql_table, dialect=dialect_for_ds_type(datasource.type))
    return at.schema_name or datasource.schema_name or None, at.name


async def _live_table_location(sql_table: str, datasource: DatasourceConfig,
                               client: SqlClient) -> TableLocation | None:
    """Where ``sql_table``'s live table is read; ``None`` for a catalog this connection cannot reach."""
    at = sql_table_location(sql_table, dialect=dialect_for_ds_type(datasource.type))
    if at.schema_name is None:
        default = datasource.schema_name or None
        return table_location(at.name, schema_ref_from_token(default, dialect_name=datasource.type) if default else None)
    try:
        ref = await table_schema_ref(catalog=at.catalog, schema=at.schema_name, client=client)
    except ValueError:
        return None
    return table_location(at.name, ref)


async def _safe_probe(
    *, client: SqlClient, table: str, field: str, schema: str | None,
    sql_table: str, persisted_type: str,
) -> DataType | None:
    """Run the probe; an unexpected failure keeps the persisted type, an unreachable database raises."""
    try:
        return await sqlite_introspect.probe_sqlite_integer_column(
            client=client, table=table, column=field, schema=schema,
        )
    except UNREACHABLE:
        raise
    except Exception as exc:
        logger.warning(
            "probe call raised for %s.%s; keeping persisted type %s: %s",
            sql_table, field, persisted_type, exc,
        )
        return None


def _verdict_changes_persisted_type(
    *, persisted_type: str, verdict: DataType,
) -> bool:
    """Returns True when the probe verdict warrants flipping the persisted
    type. INT persisted: any non-INT verdict widens. DOUBLE persisted:
    only an INT verdict narrows (probe-verified)."""
    if stored_datatype(persisted_type) is DataType.INTEGER:
        return verdict is not DataType.INTEGER
    if _is_double(persisted_type):
        return verdict is DataType.INTEGER
    return False


def _apply_format_flip(
    *, col: dict, d: dict, col_name: str, persisted_type: str, verdict: DataType,
) -> None:
    """Update ``col["format"]`` in place to match the new type. Only the
    auto-default ``NumberFormat(INTEGER)`` is overwritten; custom formats
    are preserved verbatim (with an INFO log hint for the widening direction)."""
    if _is_auto_default_integer_format_dict(col.get("format")):
        new_format = _format_for_widened_type_dict(verdict)
        if new_format is None:
            col.pop("format", None)
        else:
            col["format"] = new_format
        return
    if stored_datatype(persisted_type) is DataType.INTEGER:
        # Only log the "custom format preserved" hint for the widening
        # direction; on the DOUBLE → INT narrowing the format is typically
        # already a numeric default.
        logger.info(
            "Custom format on %s.%s preserved on SQLite probe widening "
            "(persisted INT -> %s). Review whether the format still applies.",
            d.get("name", "<unknown>"),
            col.get("name", col_name),
            verdict.value,
        )


async def _refine_one_field(
    *, client: SqlClient, col: dict, d: dict, table_name: str, schema_name: str | None,
    sql_table: str,
) -> bool:
    """Run the probe for one field and apply the verdict. Returns True if
    the field dict was mutated."""
    col_name = _physical(col)
    if col_name is None:
        return False
    persisted_type = col.get("datatype")
    verdict = await _safe_probe(
        client=client, table=table_name, field=col_name, schema=schema_name,
        sql_table=sql_table, persisted_type=str(persisted_type),
    )
    if verdict is None:
        return False
    if not _verdict_changes_persisted_type(
        persisted_type=str(persisted_type), verdict=verdict,
    ):
        return False
    col["datatype"] = verdict.value
    _apply_format_flip(
        col=col, d=d, col_name=col_name,
        persisted_type=str(persisted_type), verdict=verdict,
    )
    return True


async def _refine_dict_sqlite_probe(d: dict, datasource: DatasourceConfig, client: SqlClient) -> bool:
    """Probe-verified SQLite refinement of ``d`` in place: widen INT on non-integer storage, narrow DOUBLE
    only on certified integer storage; ``True`` when a field changed."""
    sql_table = d["sql_table"]
    columns = d["fields"]
    refineable: list[dict] = [
        c for c in columns
        if isinstance(c, dict)
        and (_is_double(c.get("datatype")) or stored_datatype(c.get("datatype")) is DataType.INTEGER)
        and _field_is_base(c.get("sql"))
    ]
    if not refineable:
        return False

    schema_name, table_name = _parse_sql_table_with_default_schema(sql_table, datasource)
    changed = False
    for col in refineable:
        if await _refine_one_field(
            client=client, col=col, d=d, table_name=table_name,
            schema_name=schema_name, sql_table=sql_table,
        ):
            changed = True
    return changed


async def refine_dict_with_live_schema(d: dict, datasource: DatasourceConfig, client: SqlClient) -> bool:
    """Refine ``d``'s field types in place against its own live table through ``client``.

    Returns whether a field changed; idempotent. An unreachable datasource raises.
    """
    if _is_sqlite_datasource(datasource):
        if not (has_refineable_fields(d) or has_sqlite_widenable_fields(d)):
            return False
        return await _refine_dict_sqlite_probe(d, datasource, client)

    if not has_refineable_fields(d):
        return False

    sql_table = d["sql_table"]
    columns = d["fields"]
    refinable: list[dict] = [
        c for c in columns
        if isinstance(c, dict)
        and _is_double(c.get("datatype"))
        and _field_is_base(c.get("sql"))
    ]
    if not refinable:
        return False

    location = await _live_table_location(sql_table, datasource, client)
    if location is None:
        return False  # a catalog this connection cannot reach keeps its persisted types
    try:
        live = await client.reflect_columns(location)
    except ObjectNotFound:
        return False  # persisted types are the safe fallback for a table the database lacks
    live_columns = {c.name: c.narrowed_type for c in live}

    changed = False
    for col in refinable:
        bare = _physical(col)
        if bare is None:
            continue
        live_type = live_columns.get(bare)
        if live_type is DataType.INTEGER:
            col["datatype"] = DataType.INTEGER.value
            changed = True
    return changed
