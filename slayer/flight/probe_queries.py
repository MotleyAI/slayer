"""Probe-query whitelist (DEV-1390 §6.5).

A small list of connection-probe SQL patterns that BI tools and JDBC
drivers issue during connect / re-connect / dialect-sniffing. We answer
them with canned responses so the connection feels healthy without
routing them into the SLayer engine.

The list is **provisional** — Phase 1.0 capture did not observe any
*driver-spontaneous* probes from the upstream Apache JDBC driver during
DatabaseMetaData introspection; every probe in the capture came from
the test harness calling ``executeQuery`` explicitly. So the whitelist
is sized for *user-typed* probes from interactive clients (DBeaver,
Hex SQL cell, etc.). Phase 2 hand-tests against PBI/Sigma/Looker/etc.
may add more.

The matcher takes a parsed sqlglot expression (the translator parses
once and dispatches across multiple checks). On match, returns a
``pyarrow.Table`` with the canned schema + data. On no match, returns
``None`` so the caller falls through to the next pipeline step.
"""

from __future__ import annotations

from typing import Optional

import pyarrow as pa
import sqlglot.expressions as exp

import slayer


def _table_select_one() -> pa.Table:
    schema = pa.schema([pa.field("1", pa.int64())])
    return pa.Table.from_pylist([{"1": 1}], schema=schema)


def _table_select_null_empty() -> pa.Table:
    schema = pa.schema([pa.field("NULL", pa.int64())])
    return pa.Table.from_pylist([], schema=schema)


def _table_select_version() -> pa.Table:
    schema = pa.schema([pa.field("version", pa.utf8())])
    value = f"SLayer Flight SQL {slayer.__version__}"
    return pa.Table.from_pylist([{"version": value}], schema=schema)


def _table_select_current_database() -> pa.Table:
    schema = pa.schema([pa.field("current_database", pa.utf8())])
    return pa.Table.from_pylist([{"current_database": "slayer"}], schema=schema)


def _is_one_expr_select(node: exp.Expression) -> bool:
    """A SELECT with exactly one projection and no FROM / GROUP BY / ORDER /
    LIMIT / etc."""
    if not isinstance(node, exp.Select):
        return False
    expressions = node.args.get("expressions") or []
    if len(expressions) != 1:
        return False
    # Reject any structural clause the bare probes don't carry. WHERE is
    # allowed (the "SELECT NULL WHERE 1=0" probe needs it). sqlglot v30+
    # uses "from_" (trailing underscore) for the FROM clause, not "from".
    for clause in ("from_", "joins", "group", "order", "limit", "offset",
                   "having", "qualify", "distinct"):
        if node.args.get(clause):
            return False
    return True


def _matches_select_one(node: exp.Expression) -> bool:
    if not _is_one_expr_select(node):
        return False
    if node.args.get("where") is not None:
        return False
    proj = node.args["expressions"][0]
    return isinstance(proj, exp.Literal) and not proj.is_string and proj.this == "1"


def _matches_select_null_where_false(node: exp.Expression) -> bool:
    if not _is_one_expr_select(node):
        return False
    where = node.args.get("where")
    if where is None:
        return False
    proj = node.args["expressions"][0]
    if not isinstance(proj, exp.Null):
        return False
    # WHERE expression must be 1=0 (or 0=1; we keep it permissive enough that
    # sqlglot canonicalisation doesn't trip us, but strict enough that
    # WHERE 1=1 doesn't match — that'd be a different probe).
    pred = where.this
    if not isinstance(pred, exp.EQ):
        return False
    lhs, rhs = pred.this, pred.expression
    if not isinstance(lhs, exp.Literal) or not isinstance(rhs, exp.Literal):
        return False
    if lhs.is_string or rhs.is_string:
        return False
    return {str(lhs.this), str(rhs.this)} == {"1", "0"}


def _matches_select_version(node: exp.Expression) -> bool:
    if not _is_one_expr_select(node):
        return False
    if node.args.get("where") is not None:
        return False
    proj = node.args["expressions"][0]
    # `version()` parses as an Anonymous function call.
    if isinstance(proj, exp.Anonymous):
        return str(proj.this).lower() == "version"
    # `@@version` parses as nested Parameter -> Parameter -> Var.
    if isinstance(proj, exp.Parameter):
        inner = proj.this
        if isinstance(inner, exp.Parameter):
            var = inner.this
            if isinstance(var, exp.Var):
                return str(var.this).lower() == "version"
    return False


def _matches_select_current_database(node: exp.Expression) -> bool:
    if not _is_one_expr_select(node):
        return False
    if node.args.get("where") is not None:
        return False
    proj = node.args["expressions"][0]
    if isinstance(proj, exp.CurrentDatabase):
        return True
    # Some sqlglot versions / dialects parse current_database() as an
    # Anonymous call; cover that path too.
    if isinstance(proj, exp.Anonymous):
        return str(proj.this).lower() == "current_database"
    return False


def match_probe(parsed: exp.Expression) -> Optional[pa.Table]:
    """Return the canned ``pa.Table`` for a matching probe, else ``None``."""
    if _matches_select_one(parsed):
        return _table_select_one()
    if _matches_select_null_where_false(parsed):
        return _table_select_null_empty()
    if _matches_select_version(parsed):
        return _table_select_version()
    if _matches_select_current_database(parsed):
        return _table_select_current_database()
    return None
