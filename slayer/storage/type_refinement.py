"""DEV-1361 / DEV-1538: storage-driven type refinement on load.

DEV-1361 narrowed legacy ``DOUBLE`` to ``INT`` for base columns whose live
SQLAlchemy type is integer. That rule is correct for strict-typed databases
(Postgres, DuckDB, ClickHouse, ...) but actively wrong on SQLite, where the
declared column type is only an affinity hint — a column declared
``INTEGER`` can store mixed REAL / TEXT / BLOB storage classes per row.

DEV-1538 splits this module's behavior by datasource type:

* **SQLite**: skip the declared-type-driven narrowing entirely. Instead,
  run :func:`slayer.sql.sqlite_introspect.probe_sqlite_integer_column`
  for persisted INT / DOUBLE base columns. Widen persisted INT to
  DOUBLE / TEXT when the probe disagrees, and narrow persisted DOUBLE to
  INT only when the probe positively certifies integer storage. The
  auto-default integer ``format`` is flipped on widening (FLOAT for
  DOUBLE, cleared for TEXT); user-set custom formats are preserved
  verbatim and an INFO log line is emitted as a hint.
* **Non-SQLite**: existing DEV-1361 narrowing unchanged.

Hard-fails when the datasource is unreachable: the SQLAlchemy connect
error propagates so storage callers (and thus query callers) see the same
behaviour as a normal query against a down DS would.
"""

from __future__ import annotations

import logging


from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig


logger = logging.getLogger(__name__)


def _column_is_base(sql: str | None) -> bool:
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


def _is_auto_default_integer_format_dict(fmt: dict | None) -> bool:
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


def _format_for_widened_type_dict(verdict: DataType) -> dict | None:
    if verdict is DataType.DOUBLE:
        return {"type": "float"}
    return None  # TEXT clears format


def has_refineable_columns(d: dict) -> bool:
    """Return True iff ``d`` is a table-backed model dict with at least one
    DOUBLE-typed base column — the DEV-1361 narrowing target. These
    columns cannot be refined without live introspection, so callers
    treat a missing datasource as a hard failure.

    Used by storage callers to decide whether the live datasource is
    *required* before raising on a missing ``DatasourceConfig``. The
    DEV-1538 SQLite-INT widening case is handled by
    :func:`has_sqlite_widenable_columns` and is best-effort (missing DS
    → log warning + skip refinement, since the persisted INT is a safe
    default).
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
        and c.get("type") == DataType.DOUBLE.value
        and _column_is_base(c.get("sql"))
        for c in columns
    )


def has_sqlite_widenable_columns(d: dict) -> bool:
    """Return True iff ``d`` is a table-backed model dict with at least one
    INT-typed base column — the DEV-1538 SQLite-affinity widening target.

    Unlike :func:`has_refineable_columns`, this predicate is *advisory* —
    callers run the probe to attempt widening when possible, but a missing
    datasource is NOT a hard fail because the persisted INT is a safe
    default (re-ingest will heal it once the DS is back). Only matters for
    SQLite datasources; non-SQLite consumers no-op silently inside
    :func:`refine_dict_with_live_schema`.
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
        and c.get("type") == DataType.INT.value
        and _column_is_base(c.get("sql"))
        for c in columns
    )


def _parse_sql_table_with_default_schema(
    sql_table: str, datasource: DatasourceConfig,
) -> tuple[str | None, str]:
    """Split ``sql_table`` into ``(schema, table)``, falling back to
    ``datasource.schema_name`` when the name is unqualified. This honours
    attached SQLite schemas instead of silently using ``main``.
    """
    default_schema = getattr(datasource, "schema_name", None) or None
    if "." in sql_table:
        schema_name, _, table_name = sql_table.partition(".")
        return (schema_name or None), table_name
    return default_schema, sql_table


def _safe_probe(
    *, conn, table: str, column: str, schema: str | None,
    sql_table: str, persisted_type: str,
) -> DataType | None:
    """Run the probe with a defence-in-depth try/except so the refinement
    loop never aborts on an unexpected exception."""
    from slayer.sql.sqlite_introspect import probe_sqlite_integer_column
    try:
        return probe_sqlite_integer_column(
            conn=conn, table=table, column=column, schema=schema,
        )
    except Exception as exc:
        logger.warning(
            "probe call raised for %s.%s; keeping persisted type %s: %s",
            sql_table, column, persisted_type, exc,
        )
        return None


def _verdict_changes_persisted_type(
    *, persisted_type: str, verdict: DataType,
) -> bool:
    """Returns True when the probe verdict warrants flipping the persisted
    type. INT persisted: any non-INT verdict widens. DOUBLE persisted:
    only an INT verdict narrows (probe-verified)."""
    if persisted_type == DataType.INT.value:
        return verdict is not DataType.INT
    if persisted_type == DataType.DOUBLE.value:
        return verdict is DataType.INT
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
    if persisted_type == DataType.INT.value:
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


def _refine_one_column(
    *, conn, col: dict, d: dict, table_name: str, schema_name: str | None,
    sql_table: str,
) -> bool:
    """Run the probe for one column and apply the verdict. Returns True if
    the column dict was mutated."""
    col_name = col.get("sql") or col.get("name")
    if not isinstance(col_name, str):
        return False
    persisted_type = col.get("type")
    verdict = _safe_probe(
        conn=conn, table=table_name, column=col_name, schema=schema_name,
        sql_table=sql_table, persisted_type=str(persisted_type),
    )
    if verdict is None:
        return False
    if not _verdict_changes_persisted_type(
        persisted_type=str(persisted_type), verdict=verdict,
    ):
        return False
    col["type"] = verdict.value
    _apply_format_flip(
        col=col, d=d, col_name=col_name,
        persisted_type=str(persisted_type), verdict=verdict,
    )
    return True


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

    schema_name, table_name = _parse_sql_table_with_default_schema(sql_table, datasource)
    from slayer.sql import engine_factory
    sa_engine = engine_factory.get_engine(datasource.resolve_env_vars())
    changed = False
    with sa_engine.connect() as conn:
        for col in refineable:
            if _refine_one_column(
                conn=conn, col=col, d=d, table_name=table_name,
                schema_name=schema_name, sql_table=sql_table,
            ):
                changed = True
    # Cached engine — don't dispose; engine_factory owns lifecycle.
    return changed


def refine_dict_with_live_schema(d: dict, datasource: DatasourceConfig) -> bool:
    """Mutate ``d`` in place: refine column types against the live schema.

    SQLite (DEV-1538):
        Run the per-column affinity probe for every persisted INT and
        DOUBLE base column. Widen persisted INT to DOUBLE / TEXT when the
        probe disagrees, and narrow persisted DOUBLE to INT only when the
        probe positively certifies integer storage. The declared-type-
        driven narrowing path is bypassed entirely on SQLite — the
        affinity signal it relies on is broken on this backend.

    Non-SQLite (DEV-1361):
        Walk the live schema and narrow DOUBLE-typed base columns to INT
        whenever the live SQL type is integer.

    Returns ``True`` if any refinement was applied, ``False`` otherwise.

    Hard-fails with the SQLAlchemy connect error when the datasource is
    unreachable. Idempotent: a second call on a refined dict is a no-op.
    """
    if _is_sqlite_datasource(datasource):
        # SQLite admits both INT and DOUBLE base columns to the probe-driven
        # refinement (widen INT or probe-verified narrow DOUBLE).
        if not (has_refineable_columns(d) or has_sqlite_widenable_columns(d)):
            return False
        return _refine_dict_sqlite_probe(d, datasource)

    if not has_refineable_columns(d):
        return False

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
