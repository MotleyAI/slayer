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
from pydantic import BaseModel
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


class _ProbeCounts(BaseModel):
    """Result of the main probe SQL — one row of per-storage-class counts."""

    n_rows: int
    n_non_null: int
    n_real: int
    n_text: int
    n_blob: int
    n_non_integral: int


def _run_main_probe(
    *, conn: sa.engine.Connection, qtable: str, qcol: str, table: str, column: str,
) -> Optional[_ProbeCounts]:
    """Run the main probe aggregate. Returns ``None`` on driver failure
    (after logging a WARNING)."""
    scan_cap_plus_one = PROBE_SCAN_CAP + 1
    sql = sa.text(
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
        row = conn.execute(sql).fetchone()
    except Exception as exc:
        logger.warning("probe failed for %s.%s: %s", table, column, exc)
        return None
    if row is None:
        # Defensive: SELECT-with-aggregates always returns one row.
        return _ProbeCounts(n_rows=0, n_non_null=0, n_real=0, n_text=0, n_blob=0, n_non_integral=0)
    return _ProbeCounts(
        n_rows=int(row[0] or 0),
        n_non_null=int(row[1] or 0),
        n_real=int(row[2] or 0),
        n_text=int(row[3] or 0),
        n_blob=int(row[4] or 0),
        n_non_integral=int(row[5] or 0),
    )


def _classify_text_branch(
    *, conn: sa.engine.Connection, qtable: str, qcol: str, table: str, column: str,
) -> Optional[DataType]:
    """The ``n_text > 0`` decision branch — runs the bounded distinct-text
    coerce probe and classifies. Returns the verdict (DOUBLE / TEXT) or
    ``None`` on coerce-probe driver failure (after logging a WARNING).
    """
    scan_cap_plus_one = PROBE_SCAN_CAP + 1
    distinct_limit_plus_one = COERCE_DISTINCT_LIMIT + 1
    # Bound the DISTINCT scan to the same row sample as the main probe —
    # otherwise a column with mixed-text storage in a billion-row table
    # would silently turn the LIMIT-100k probe into a full-table scan.
    coerce_sql = sa.text(
        f"SELECT DISTINCT {qcol} FROM ("
        f"  SELECT {qcol} FROM {qtable} LIMIT {scan_cap_plus_one}"
        f") AS sample "
        f"WHERE typeof({qcol}) = 'text' "
        f"LIMIT {distinct_limit_plus_one}"
    )
    try:
        distinct_rows = conn.execute(coerce_sql).fetchall()
    except Exception as exc:
        logger.warning("coerce probe failed for %s.%s: %s", table, column, exc)
        return None

    # Saturated distinct-text sample → conservative TEXT (can't prove all
    # distinct values coerce).
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
    # Every distinct text value is a finite float.
    return DataType.DOUBLE


def _classify_no_evidence(
    *, counts: _ProbeCounts, table: str, column: str,
) -> Optional[DataType]:
    """The all-zero-evidence branch — saturated, empty, or all-NULL all
    decline to certify INT. Returns ``None`` after logging a WARNING for
    saturated samples; returns ``None`` silently for empty / all-NULL;
    returns ``DataType.INT`` only when there's positive integer evidence."""
    if counts.n_rows > PROBE_SCAN_CAP:
        logger.warning(
            "probe sample saturated for %s.%s (>%d rows); cannot certify INT",
            table, column, PROBE_SCAN_CAP,
        )
        return None
    if counts.n_non_null == 0:
        # Empty table OR all-NULL — no evidence either way. Return None so
        # callers treat this as "no information" rather than "certified
        # INT". Ingest callers will fall back to the SA-derived type;
        # refinement callers won't narrow a persisted DOUBLE.
        return None
    return DataType.INT


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
       b. No non-NULL evidence (empty / all-NULL) → ``None``.
       c. Otherwise (all sampled non-NULL values are INTEGER) →
          :class:`DataType.INT`.

    The caller treats ``None`` as "keep the SA-derived type as-is" — saturated
    samples and probe failures are indistinguishable from the caller's
    perspective, which is what we want: both mean "the probe couldn't
    certify a widening".
    """
    qtable = _qualified_table(table, schema)
    qcol = _quote_sqlite_ident(column)
    counts = _run_main_probe(
        conn=conn, qtable=qtable, qcol=qcol, table=table, column=column,
    )
    if counts is None:
        return None
    if counts.n_blob > 0:
        return DataType.TEXT
    if counts.n_text > 0:
        return _classify_text_branch(
            conn=conn, qtable=qtable, qcol=qcol, table=table, column=column,
        )
    if counts.n_real > 0 or counts.n_non_integral > 0:
        return DataType.DOUBLE
    return _classify_no_evidence(counts=counts, table=table, column=column)
