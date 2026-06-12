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

import tempfile
from typing import Optional

import sqlglot
from sqlglot import exp

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.yaml_storage import YAMLStorage

_SQLGLOT_TYPEERROR_DIALECTS = {"bigquery"}


def _assert_valid_sql(sql: str, dialect: str = "postgres") -> None:
    """Assert generated SQL is structurally valid (parses, no nested WITH)."""
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
        assert statements, f"SQL failed to parse:\n{sql}"
        assert len(statements) == 1, (
            f"Expected 1 SQL statement, got {len(statements)}:\n{sql}"
        )
    except TypeError as exc:
        if dialect not in _SQLGLOT_TYPEERROR_DIALECTS:
            raise AssertionError(
                f"sqlglot TypeError while validating {dialect} SQL:\n{sql}"
            ) from exc
        return
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
