"""DEV-1361 / DEV-1538: storage-driven type refinement on load.

DEV-1361 narrowed legacy ``DOUBLE`` to ``INT`` for base columns whose live
SQLAlchemy type is integer. That rule is correct for strict-typed databases
(Postgres, DuckDB, ClickHouse, ...) but actively wrong on SQLite, where the
declared column type is only an affinity hint — a column declared
``INTEGER`` can store mixed REAL / TEXT / BLOB storage classes per row.

DEV-1538 splits this module's behavior by datasource type:

* **SQLite**: skip the declared-type-driven narrowing entirely. Instead,
  for every persisted INT base column, run
  :func:`slayer.sql.sqlite_introspect.probe_sqlite_integer_column` against
  the live storage classes and widen the persisted type to DOUBLE / TEXT
  when the probe disagrees. The auto-default integer ``format`` is also
  flipped (FLOAT for DOUBLE, cleared for TEXT); user-set custom formats
  are preserved verbatim and an INFO log line is emitted as a hint.
* **Non-SQLite**: existing DEV-1361 narrowing unchanged.

Hard-fails when the datasource is unreachable: the SQLAlchemy connect
error propagates so storage callers (and thus query callers) see the same
behaviour as a normal query against a down DS would.
"""

from __future__ import annotations

import logging
from typing import Optional

import sqlalchemy as sa

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig


logger = logging.getLogger(__name__)


def _column_is_base(sql: Optional[str]) -> bool:
    """A column whose ``sql`` is ``None`` or a single bare identifier is a
    "base" column — it claims a live database column. Derived expressions
    (``amount * 2``, ``length(name)``, ``customers.region``, etc.) are not.
    Mirrors the predicate used by ``slayer.engine.schema_drift``.
    """
    if sql is None:
        return True
    s = sql.strip()
    if not s or s[0].isdigit():
        return False
    return all(c.isalnum() or c == "_" for c in s)


def _is_sqlite_datasource(datasource: DatasourceConfig) -> bool:
    return (datasource.type or "").lower() == "sqlite"


def _is_auto_default_integer_format_dict(fmt: Optional[dict]) -> bool:
    """Dict-shape mirror of
    :func:`slayer.engine.ingestion._is_auto_default_integer_format` —
    detects the auto-ingested ``NumberFormat(type=INTEGER)`` default so
    DEV-1538 widening can flip the format without trampling custom
    user-set formats.
    """
    if not isinstance(fmt, dict):
        return False
    if fmt.get("type") != "integer":
        return False
    return (
        fmt.get("precision") in (None, "")
        and fmt.get("symbol") in (None, "")
    )


def _format_for_widened_type_dict(verdict: DataType) -> Optional[dict]:
    if verdict is DataType.DOUBLE:
        return {"type": "float"}
    return None  # TEXT clears format


def has_refineable_columns(d: dict) -> bool:
    """Return True iff ``d`` is a table-backed model dict with at least one
    column that requires live-schema introspection to refine:

    * Any DOUBLE-typed base column (the DEV-1361 narrowing target), OR
    * Any INT-typed base column (the DEV-1538 SQLite-affinity widening
      target — the predicate is datasource-type-agnostic here; the
      SQLite branch is the only consumer that acts on the INT case, but
      that decision lives in :func:`refine_dict_with_live_schema`).

    Used by storage callers to decide whether the live datasource is
    actually needed before raising on a missing ``DatasourceConfig``.
    """
    if not isinstance(d, dict):
        return False
    sql_table = d.get("sql_table")
    if not isinstance(sql_table, str) or not sql_table:
        return False
    columns = d.get("columns")
    if not isinstance(columns, list) or not columns:
        return False
    return any(
        isinstance(c, dict)
        and c.get("type") in (DataType.DOUBLE.value, DataType.INT.value)
        and _column_is_base(c.get("sql"))
        for c in columns
    )


def _refine_dict_sqlite_probe(d: dict, datasource: DatasourceConfig) -> bool:
    """DEV-1538: SQLite-aware refinement.

    Replaces the DEV-1361 declared-type narrowing on SQLite with
    probe-verified type refinement that works in both directions:

    * **Widen** (DEV-1538): persisted ``INT`` → ``DOUBLE`` / ``TEXT`` when
      the probe shows mixed REAL / non-coercible-TEXT / BLOB storage.
    * **Narrow** (DEV-1361 spirit, probe-verified): persisted ``DOUBLE`` →
      ``INT`` when the probe shows every sampled value is integer-shaped.
      The narrowing only fires when the probe positively certifies INT —
      saturated samples (``None`` verdict) or any evidence of REAL / TEXT
      / BLOB leaves the persisted ``DOUBLE`` alone.

    Returns ``True`` if any column changed. Mutates ``d`` in place. Hard-
    fails with the SQLAlchemy connect error when the datasource is
    unreachable.
    """
    sql_table = d["sql_table"]
    columns = d["columns"]
    refineable: list[dict] = [
        c for c in columns
        if isinstance(c, dict)
        and c.get("type") in (DataType.INT.value, DataType.DOUBLE.value)
        and _column_is_base(c.get("sql"))
    ]
    if not refineable:
        return False

    # Local import to avoid loading the helper on cold-start of non-SQLite
    # backends.
    from slayer.sql.sqlite_introspect import probe_sqlite_integer_column

    # Parse schema-qualified sql_table the same way the ingest helper does.
    if "." in sql_table:
        schema_name, _, table_name = sql_table.partition(".")
        schema_name = schema_name or None
    else:
        schema_name, table_name = None, sql_table

    sa_engine = sa.create_engine(datasource.resolve_env_vars().get_connection_string())
    changed = False
    try:
        with sa_engine.connect() as conn:
            for col in refineable:
                col_name = col.get("sql") or col.get("name")
                if not isinstance(col_name, str):
                    continue
                persisted_type = col.get("type")
                try:
                    verdict = probe_sqlite_integer_column(
                        conn=conn,
                        table=table_name,
                        column=col_name,
                        schema=schema_name,
                    )
                except Exception as exc:
                    logger.warning(
                        "probe call raised for %s.%s; keeping persisted type %s: %s",
                        sql_table,
                        col_name,
                        persisted_type,
                        exc,
                    )
                    verdict = None
                if verdict is None:
                    continue
                # Decide whether this verdict changes the persisted type.
                if persisted_type == DataType.INT.value:
                    # DEV-1538 widening: only flip on disagreement.
                    if verdict is DataType.INT:
                        continue
                elif persisted_type == DataType.DOUBLE.value:
                    # DEV-1361 narrowing (probe-verified): only narrow to
                    # INT. Probe-says-DOUBLE or probe-says-TEXT leaves
                    # the persisted DOUBLE alone.
                    if verdict is not DataType.INT:
                        continue
                col["type"] = verdict.value
                # Format flip rule: only overwrite the auto-default
                # NumberFormat(type=INTEGER); custom formats preserved.
                if _is_auto_default_integer_format_dict(col.get("format")):
                    new_format = _format_for_widened_type_dict(verdict)
                    if new_format is None:
                        col.pop("format", None)
                    else:
                        col["format"] = new_format
                elif persisted_type == DataType.INT.value:
                    # Only log the "custom format preserved" hint for the
                    # widening direction; on the DOUBLE → INT narrowing
                    # the format is typically already a numeric default.
                    logger.info(
                        "Custom format on %s.%s preserved on SQLite probe widening "
                        "(persisted INT -> %s). Review whether the format still "
                        "applies.",
                        d.get("name", "<unknown>"),
                        col.get("name", col_name),
                        verdict.value,
                    )
                changed = True
    finally:
        sa_engine.dispose()
    return changed


def refine_dict_with_live_schema(d: dict, datasource: DatasourceConfig) -> bool:
    """Mutate ``d`` in place: refine column types against the live schema.

    SQLite (DEV-1538):
        Run the per-column affinity probe for every persisted INT base
        column and widen INT -> DOUBLE / TEXT when the probe disagrees.
        The DEV-1361 DOUBLE -> INT narrowing is **skipped** on SQLite —
        the declared-type signal it relies on is broken on this backend.

    Non-SQLite (DEV-1361):
        Walk the live schema and narrow DOUBLE-typed base columns to INT
        whenever the live SQL type is integer.

    Returns ``True`` if any refinement was applied, ``False`` otherwise.

    Hard-fails with the SQLAlchemy connect error when the datasource is
    unreachable. Idempotent: a second call on a refined dict is a no-op.
    """
    if not has_refineable_columns(d):
        return False

    if _is_sqlite_datasource(datasource):
        return _refine_dict_sqlite_probe(d, datasource)

    # DEV-1361 narrowing path — unchanged from pre-1538 behaviour.
    sql_table = d["sql_table"]
    columns = d["columns"]
    refinable: list[dict] = [
        c for c in columns
        if isinstance(c, dict)
        and c.get("type") == DataType.DOUBLE.value
        and _column_is_base(c.get("sql"))
    ]
    if not refinable:
        return False

    # Local import to avoid circular import at module load time.
    from slayer.engine.schema_drift import _live_schema_for_datasource

    live = _live_schema_for_datasource(datasource=datasource)
    table = live.get(sql_table)
    if table is None:
        return False
    live_columns = table.columns

    changed = False
    for col in refinable:
        bare = col.get("sql") or col.get("name")
        if not isinstance(bare, str):
            continue
        live_type = live_columns.get(bare)
        if live_type is DataType.INT:
            col["type"] = DataType.INT.value
            changed = True
    return changed
