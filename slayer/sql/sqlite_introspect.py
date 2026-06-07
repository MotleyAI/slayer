"""DEV-1538: SQLite affinity-probe helper.

SQLite's declared column types are affinity hints, not constraints. A column
declared ``INTEGER`` can store mixed storage classes (INTEGER, REAL, TEXT,
BLOB) at the row level, and the affinity rule only converts REAL→INTEGER
when it would be lossless and reversible. Trusting the declared type at
ingest time silently truncates the actual REAL data once it flows through
downstream ``CAST(... AS INTEGER)`` sites — see DEV-1538 for the autopsy.

This module exposes a single probe that inspects the actual storage classes
of stored values and returns the appropriate :class:`DataType`:

* :class:`DataType.INT` when every sampled value is integer-shaped (and the
  sample is not saturated).
* :class:`DataType.DOUBLE` when REAL values are present, or non-integral
  integers are present (caught via ``ROUND(col) <> col``), or every TEXT
  value coerces to a finite ``float``.
* :class:`DataType.TEXT` when BLOB values are present, any TEXT value is
  non-coercible (or non-finite), or distinct-text saturates the coerce cap.
* ``None`` when the probe itself errors, OR when the row sample saturates
  AND the verdict would otherwise be INT (we can't certify INT past the cap).
  Callers treat ``None`` as "keep the SA-derived type".
"""

from __future__ import annotations

import logging
import math
from typing import Optional

import sqlalchemy as sa
from sqlglot import exp

from slayer.core.enums import DataType


logger = logging.getLogger(__name__)


# Row-count cap for the main probe SQL. We always run with LIMIT (CAP + 1)
# so we can detect saturation: getting back exactly CAP + 1 rows means
# there's at least one row past the cap.
PROBE_SCAN_CAP: int = 100_000

# Distinct-text-value cap for the secondary coerce probe.
COERCE_DISTINCT_LIMIT: int = 1_000


def _quote_sqlite_ident(name: str) -> str:
    """Quote a single identifier for SQLite. Goes through ``sqlglot.exp``
    so embedded double-quotes are escaped correctly."""
    return exp.to_identifier(name, quoted=True).sql(dialect="sqlite")


def _qualified_table(table: str, schema: Optional[str]) -> str:
    """Build a quoted, optionally schema-qualified SQLite table reference."""
    tname = _quote_sqlite_ident(table)
    if schema:
        return f"{_quote_sqlite_ident(schema)}.{tname}"
    return tname


def probe_sqlite_integer_column(
    *,
    conn: sa.engine.Connection,
    table: str,
    column: str,
    schema: Optional[str] = None,
) -> Optional[DataType]:
    """Probe one SQLite column's actual storage classes and return the
    appropriate :class:`DataType`.

    The decision order is:

    1. Main probe SQL fails → log WARNING, return ``None``.
    2. ``n_blob > 0`` → :class:`DataType.TEXT` (BLOBs can't be safely cast
       numerically).
    3. ``n_text > 0`` (checked BEFORE the REAL branch):
       a. Coerce probe SQL fails → log WARNING, return ``None``.
       b. Distinct-text result is saturated → :class:`DataType.TEXT`
          (conservatively widened — we can't prove all distinct values
          coerce).
       c. Any text value's ``float()`` raises or result is not finite →
          :class:`DataType.TEXT`.
       d. Every distinct text value coerces to a finite float →
          :class:`DataType.DOUBLE`.
    4. ``n_real > 0`` OR ``n_non_integral > 0`` → :class:`DataType.DOUBLE`.
    5. All-zero counts:
       a. Row sample saturated → log WARNING, return ``None``.
       b. Otherwise (empty / all-NULL / all-INTEGER) → :class:`DataType.INT`.

    The caller treats ``None`` as "keep the SA-derived type as-is" — saturated
    samples and probe failures are indistinguishable from the caller's
    perspective, which is what we want: both mean "the probe couldn't
    certify a widening".
    """
    qtable = _qualified_table(table, schema)
    qcol = _quote_sqlite_ident(column)
    scan_cap_plus_one = PROBE_SCAN_CAP + 1

    main_sql = sa.text(
        f"SELECT "
        f"  COUNT(*) AS n_rows, "
        f"  SUM(CASE WHEN typeof({qcol}) <> 'null' THEN 1 ELSE 0 END) AS n_non_null, "
        f"  SUM(CASE WHEN typeof({qcol}) = 'real' THEN 1 ELSE 0 END) AS n_real, "
        f"  SUM(CASE WHEN typeof({qcol}) = 'text' THEN 1 ELSE 0 END) AS n_text, "
        f"  SUM(CASE WHEN typeof({qcol}) = 'blob' THEN 1 ELSE 0 END) AS n_blob, "
        f"  SUM(CASE WHEN {qcol} IS NOT NULL "
        f"            AND typeof({qcol}) IN ('integer','real') "
        f"            AND ROUND({qcol}) <> {qcol} THEN 1 ELSE 0 END) AS n_non_integral "
        f"FROM (SELECT {qcol} FROM {qtable} LIMIT {scan_cap_plus_one})"
    )

    try:
        row = conn.execute(main_sql).fetchone()
    except Exception as exc:
        logger.warning(
            "probe failed for %s.%s: %s",
            table,
            column,
            exc,
        )
        return None

    if row is None:
        # No rows returned at all — defensive; SELECT-with-aggregates always
        # returns one row, but if a driver disagrees, treat as empty.
        return DataType.INT

    n_rows = int(row[0] or 0)
    n_non_null = int(row[1] or 0)
    n_real = int(row[2] or 0)
    n_text = int(row[3] or 0)
    n_blob = int(row[4] or 0)
    n_non_integral = int(row[5] or 0)

    saturated = n_rows > PROBE_SCAN_CAP

    # 1. BLOB always forces TEXT (no safe numeric cast).
    if n_blob > 0:
        return DataType.TEXT

    # 2. TEXT presence forces the coerce branch BEFORE the REAL branch so
    #    a column with REAL + non-coercible TEXT widens to TEXT, not DOUBLE.
    if n_text > 0:
        distinct_limit_plus_one = COERCE_DISTINCT_LIMIT + 1
        coerce_sql = sa.text(
            f"SELECT DISTINCT {qcol} FROM {qtable} "
            f"WHERE typeof({qcol}) = 'text' "
            f"LIMIT {distinct_limit_plus_one}"
        )
        try:
            distinct_rows = conn.execute(coerce_sql).fetchall()
        except Exception as exc:
            logger.warning(
                "coerce probe failed for %s.%s: %s",
                table,
                column,
                exc,
            )
            return None

        # Saturated distinct-text sample → conservative TEXT.
        if len(distinct_rows) > COERCE_DISTINCT_LIMIT:
            return DataType.TEXT

        for r in distinct_rows:
            raw = r[0]
            if raw is None:
                continue
            try:
                f = float(str(raw).strip())
            except (TypeError, ValueError):
                return DataType.TEXT
            if not math.isfinite(f):
                return DataType.TEXT
        # Every distinct text value is a finite float — fall through to DOUBLE.
        return DataType.DOUBLE

    # 3. Pure numeric paths.
    if n_real > 0 or n_non_integral > 0:
        return DataType.DOUBLE

    # 4. All-zero counts.
    if saturated:
        logger.warning(
            "probe sample saturated for %s.%s (>%d rows); cannot certify INT",
            table,
            column,
            PROBE_SCAN_CAP,
        )
        return None
    if n_non_null == 0:
        # Empty table OR all-NULL — no evidence either way. Return None so
        # callers treat this as "no information" rather than "certified
        # INT". Ingest callers will fall back to the SA-derived type (INT
        # for declared INTEGER affinity, same final outcome). Refinement
        # callers won't narrow a persisted DOUBLE based on a non-observation.
        return None
    return DataType.INT
