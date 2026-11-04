"""SQLite integer-affinity probe, run through a client's ``execute``.

SQLite's declared column types are affinity hints: a column declared ``INTEGER``
can store REAL, TEXT or BLOB values row by row. The probe samples the stored
storage classes and returns:

* ``DataType.INT`` when every sampled value is integer-shaped (sample not saturated);
* ``DataType.DOUBLE`` when REAL or non-integral values appear, or every TEXT value
  coerces to a finite float;
* ``DataType.TEXT`` when BLOBs appear, a TEXT value does not coerce (or is not
  finite), or the distinct-text sample saturates;
* ``None`` when a probe statement fails, or the row sample saturates on an INT
  verdict; callers keep the type they had. An unreachable database raises.
"""

from __future__ import annotations

import logging
import math

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.sql.client import AuthenticationRefused, DatabaseUnavailable, SqlClient
from slayer.sql.dialects import sqlite as _sqlite
from slayer.sql.dialects.sqlite import StorageCensus, census_counts


logger = logging.getLogger(__name__)


# Row-count cap for the main probe; it runs with LIMIT (CAP + 1) so saturation shows.
PROBE_SCAN_CAP: int = _sqlite.PROBE_SCAN_CAP

# Distinct-text-value cap for the secondary coerce probe.
COERCE_DISTINCT_LIMIT: int = 1_000


def _identifier(name: str) -> exp.Identifier:
    """A quoted identifier; an already-quoted name is quoted once."""
    if len(name) >= 2 and name[0] == '"' == name[-1]:
        name = name[1:-1].replace('""', '"')
    return exp.to_identifier(name, quoted=True)


def _sample(*, table: str, schema: str | None, column: str) -> exp.Subquery:
    """``(SELECT column FROM [schema.]table LIMIT CAP + 1) AS sample``."""
    relation = exp.Table(this=_identifier(table), db=_identifier(schema) if schema else None)
    select = exp.select(exp.column(_identifier(column))).from_(relation).limit(PROBE_SCAN_CAP + 1)
    return select.subquery("sample")


def _sql(statement: Expression) -> str:
    return statement.sql(dialect="sqlite")


async def _run_main_probe(
    *, client: SqlClient, table: str, schema: str | None, column: str,
) -> StorageCensus | None:
    """The column's storage census; ``None`` on driver failure (after logging a WARNING)."""
    value = exp.column(_identifier(column))
    sql = _sql(exp.select(exp.Count(this=exp.Star()), *census_counts(value)).from_(
        _sample(table=table, schema=schema, column=column),
    ))
    try:
        rows = (await client.execute(sql)).rows
    except (DatabaseUnavailable, AuthenticationRefused):
        raise
    except Exception as exc:
        logger.warning("probe failed for %s.%s: %s", table, column, exc)
        return None
    row = list(rows[0].values()) if rows else [0] * 6
    return StorageCensus.of(n_rows=row[0], counts=row[1:])


async def _classify_text_branch(
    *, client: SqlClient, table: str, schema: str | None, column: str,
) -> DataType | None:
    """The ``n_text > 0`` decision branch — runs the bounded distinct-text
    coerce probe and classifies. Returns the verdict (DOUBLE / TEXT) or
    ``None`` on coerce-probe driver failure (after logging a WARNING).
    """
    value = exp.column(_identifier(column))
    # Bounded to the main probe's row sample, so mixed-text storage never scans the whole table.
    coerce_sql = _sql(
        exp.select(value.as_("v")).distinct()
        .from_(_sample(table=table, schema=schema, column=column))
        .where(exp.EQ(
            this=exp.Anonymous(this="typeof", expressions=[value.copy()]), expression=exp.Literal.string("text"),
        ))
        .limit(COERCE_DISTINCT_LIMIT + 1)
    )
    try:
        distinct_rows = [row["v"] for row in (await client.execute(coerce_sql)).rows]
    except (DatabaseUnavailable, AuthenticationRefused):
        raise
    except Exception as exc:
        logger.warning("coerce probe failed for %s.%s: %s", table, column, exc)
        return None

    # Saturated distinct-text sample → conservative TEXT (can't prove all
    # distinct values coerce).
    if len(distinct_rows) > COERCE_DISTINCT_LIMIT:
        return DataType.TEXT

    for raw in distinct_rows:
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


async def probe_sqlite_integer_column(
    *,
    client: SqlClient,
    table: str,
    column: str,
    schema: str | None = None,
) -> DataType | None:
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
    counts = await _run_main_probe(client=client, table=table, schema=schema, column=column)
    if counts is None:
        return None
    if counts.n_blob > 0:
        return DataType.TEXT
    if counts.n_text > 0:
        return await _classify_text_branch(client=client, table=table, schema=schema, column=column)
    verdict = counts.numeric_verdict(cap=PROBE_SCAN_CAP)
    if verdict is None and counts.n_rows > PROBE_SCAN_CAP:
        logger.warning(
            "probe sample saturated for %s.%s (>%d rows); cannot certify INT", table, column, PROBE_SCAN_CAP,
        )
    return verdict
