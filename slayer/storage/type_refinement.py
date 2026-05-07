"""DEV-1361: storage-driven type refinement.

After the v4→v5 dict migrator coarsens legacy ``number`` to ``DOUBLE``,
this helper introspects the model's datasource and refines ``DOUBLE`` to
``INT`` for base columns whose live SQL type is integer.

Hard-fails when the datasource is unreachable: the SQLAlchemy connect error
propagates so storage callers (and thus query callers) see the same
behaviour as a normal query against a down DS would.

Reuses ``slayer.engine.schema_drift._live_schema_for_datasource`` for the
introspection itself; this module only contains the refinement rule.
"""

from __future__ import annotations

from typing import Optional

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig


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


def has_refineable_columns(d: dict) -> bool:
    """Return True iff ``d`` is a table-backed model dict with at least one
    DOUBLE-typed base column — i.e. ``refine_dict_with_live_schema`` would
    have something to ask the live schema about. Used by storage callers to
    decide whether the live datasource is actually needed before raising on
    a missing ``DatasourceConfig``.
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


def refine_dict_with_live_schema(d: dict, datasource: DatasourceConfig) -> bool:
    """Mutate ``d`` in place: refine ``DOUBLE`` → ``INT`` for base columns
    whose live SQL type is integer. Returns ``True`` if any refinement was
    applied, ``False`` otherwise.

    Skipped entirely when the model is query-backed (no ``sql_table``) or in
    SQL source-mode (``sql`` set without ``sql_table``). Only the table-mode
    columns can be looked up against a live schema.

    Hard-fails with the SQLAlchemy connect error when the datasource is
    unreachable. Idempotent: a second call on a refined dict is a no-op
    because no column carries ``DOUBLE`` whose live type is integer.
    """
    if not has_refineable_columns(d):
        return False
    sql_table = d["sql_table"]
    columns = d["columns"]
    refinable: list[dict] = [
        c for c in columns
        if isinstance(c, dict)
        and c.get("type") == DataType.DOUBLE.value
        and _column_is_base(c.get("sql"))
    ]

    # Local import to avoid circular import at module load time
    # (schema_drift -> storage -> ... can chain).
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
