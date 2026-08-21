"""Shared test helpers for engine-driven SQL-shape assertions.

Used by tests migrated off the legacy ``slayer.engine.enrichment`` pipeline
(DEV-1484 Stage C). Naming intentionally underscored so pytest skips it
during test discovery while still allowing ``from tests._engine_helpers
import ...`` from individual test modules.

Helpers:

* :func:`_assert_valid_sql` — verifies generated SQL parses with sqlglot
  and contains no nested ``WITH`` clause. Lifted from the legacy
  ``test_sql_generator.py`` ``_validating_generate`` wrapper.
* :func:`_engine_generate` — builds a fresh ephemeral ``YAMLStorage`` +
  ``SlayerQueryEngine`` for a single ``SlayerModel``, runs the supplied
  ``SlayerQuery`` with ``dry_run=True``, and returns the emitted SQL.
  Mirrors the legacy ``_generate(query, model)`` semantics on the
  typed pipeline.
"""

from __future__ import annotations

import re
import tempfile
from typing import Optional

import sqlglot
from sqlglot import exp

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage


async def make_seeded_sqlite_engine(
    *, base_dir: str, db_path: str, models: list[SlayerModel], datasource: str = "test"
) -> SlayerQueryEngine:
    """Storage + engine bound to a seeded SQLite file (DEV-1815).

    Consolidates the byte-identical ``make_sqlite_engine`` helpers previously
    duplicated across the per-DEV fixture modules; they now delegate here.
    """
    storage = YAMLStorage(base_dir=base_dir)
    await storage.save_datasource(
        DatasourceConfig(name=datasource, type="sqlite", database=db_path),
    )
    for model in models:
        await storage.save_model(model)
    return SlayerQueryEngine(storage=storage)


def _assert_valid_sql(sql: str, dialect: str = "postgres") -> None:
    """Assert generated SQL is structurally valid (parses, no nested WITH).

    DEV-1713 removed the BigQuery ``TypeError`` carve-out: finalised BigQuery
    naming/mangling no longer emits the dotted-alias shapes sqlglot choked on,
    so a ``TypeError`` here is now a real failure for every dialect.
    """
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
        assert statements, f"SQL failed to parse:\n{sql}"
        assert len(statements) == 1, (
            f"Expected 1 SQL statement, got {len(statements)}:\n{sql}"
        )
    except TypeError as exc:
        raise AssertionError(
            f"sqlglot TypeError while validating {dialect} SQL:\n{sql}"
        ) from exc
    with_lines = [
        line for line in sql.split("\n")
        if line.strip().upper().startswith("WITH ")
    ]
    assert len(with_lines) <= 1, f"Nested WITH clauses detected:\n{sql}"


async def _engine_generate(
    *,
    query: SlayerQuery,
    model: SlayerModel,
    dialect: str = "postgres",
    extra_models: Optional[list] = None,
    validate: bool = True,
) -> str:
    """Build a fresh ``YAMLStorage`` + ``SlayerQueryEngine`` for ``model``,
    run ``query`` with ``dry_run=True``, and return the emitted SQL.

    Each call creates and tears down its own temporary directory so callers
    can freely compare two model variants (with vs without a saved measure,
    etc.) without storage cross-talk. ``extra_models`` is an optional list
    of additional ``SlayerModel`` instances to register in the same store
    (e.g. join targets sharing ``model.data_source``).

    ``validate=False`` skips save-time DEV-1410 derived-column cycle
    detection for the few migrated tests that feed intentionally-shaped
    models the cycle validator would otherwise reject.
    """
    with tempfile.TemporaryDirectory() as d:
        storage = YAMLStorage(base_dir=d)
        await storage.save_datasource(
            DatasourceConfig(name=model.data_source, type=dialect)
        )
        await storage.save_model(model, _validate=validate)
        for extra in extra_models or []:
            await storage.save_model(extra, _validate=validate)
        engine = SlayerQueryEngine(storage=storage)
        response = await engine.execute(query, dry_run=True)
        sql = response.sql
        assert sql is not None, "engine.execute(dry_run=True) returned no SQL"
        _assert_valid_sql(sql, dialect=dialect)
        return sql


def _outer_select(sql: str, *, dialect: str = "postgres") -> exp.Select:
    """Parse ``sql`` and return its outermost ``SELECT`` expression.

    Skips a leading ``WITH`` by descending into the CTE chain's final
    ``SELECT`` — the typed pipeline emits a single top-level statement, so
    this is the projection-and-filter-bearing query body.
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    select = parsed.find(exp.Select)
    assert select is not None, f"no SELECT found in SQL:\n{sql}"
    return select


def _where_text(sql: str, *, dialect: str = "postgres") -> str:
    """Rendered text of the outermost SELECT's WHERE predicate ('' if none)."""
    where = _outer_select(sql, dialect=dialect).args.get("where")
    return where.this.sql(dialect=dialect) if where is not None else ""


def _having_text(sql: str, *, dialect: str = "postgres") -> str:
    """Rendered text of the outermost SELECT's HAVING predicate ('' if none)."""
    having = _outer_select(sql, dialect=dialect).args.get("having")
    return having.this.sql(dialect=dialect) if having is not None else ""


def _norm(s: str) -> str:
    """Collapse all runs of whitespace to single spaces."""
    return " ".join(s.split())


def _join_aliases(sql: str, *, dialect: str = "postgres") -> set[str]:
    """The set of joined-table aliases in ``sql``.

    Walks every ``JOIN`` node and collects the joined table's alias (or
    bare name when unaliased) — e.g. ``LEFT JOIN customers AS customers``
    yields ``customers``; ``LEFT JOIN regions AS customers__regions``
    yields ``customers__regions``.

    DEV-1732: shared out of ``tests/test_sql_generator.py`` so the
    frame-bound tests assert against real JOIN nodes rather than alias
    substrings in predicate text.
    """
    tree = sqlglot.parse_one(sql, dialect=dialect)
    aliases: set[str] = set()
    for join in tree.find_all(exp.Join):
        target = join.this
        if isinstance(target, exp.Table):
            aliases.add(target.alias_or_name)
    return aliases


def _extract_src_body(sql: str) -> str:
    """Pull out the ``_src`` subquery body from a generated window-measure SQL.

    Resilient when the outer query also contains other LEFT JOIN (...) blocks
    (e.g. cross-model measure subqueries): anchors on the ``\\n) AS _src`` suffix
    and reverse-searches for the matching ``LEFT JOIN (\\n`` before it. Multiple
    windowed measures emit SIBLING ``) AS _src`` closes (never nested); anchor on
    the LAST so the reverse search pairs it with the last measure's opening.

    The missing-anchor assertion is not reachable with today's generator output
    (CodeRabbit): without it the helper silently returns a slice from an
    arbitrary offset, so a future change to the join keyword or its formatting
    would surface as a confusing assertion against the wrong text rather than a
    clear failure here.
    """
    closes = list(re.finditer(r"\n[ \t]*\) AS _src", sql))
    assert closes, f"No `) AS _src` closing the _src subquery in:\n{sql}"
    end = closes[-1].start()
    opens = list(re.finditer(r"LEFT JOIN \(\n", sql[:end]))
    assert opens, f"No `LEFT JOIN (` opening the _src subquery in:\n{sql}"
    return sql[opens[-1].end():end]


def _extract_cte_body(sql: str, cte_name_pattern: str) -> str:
    """Extract one CTE body by matching ``<cte_name> AS (`` and walking balanced
    parentheses to its closing ``)``.

    Robust against nested subqueries inside the CTE body (e.g. the ranked
    ``FROM (SELECT ... ROW_NUMBER() …) AS …`` that first/last isolated CTEs
    contain). ``cte_name_pattern`` is a regex matched against the CTE name —
    typical use: ``r"_cm_\\w*loss_payment_amt\\w*"``. Raises ``AssertionError``
    if no matching CTE is found.
    """
    name_match = re.search(rf"({cte_name_pattern})\s+AS\s*\(", sql)
    assert name_match, f"No CTE matching {cte_name_pattern!r} in:\n{sql}"
    # Position just after the opening paren of ``<name> AS (``.
    body_start = sql.index("(", name_match.start()) + 1
    depth = 1
    i = body_start
    while i < len(sql) and depth > 0:
        ch = sql[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return sql[body_start:i]
        i += 1
    raise AssertionError(
        f"Unbalanced parens — no closing ) for CTE {name_match.group(1)!r}:\n{sql}"
    )
