"""Probe-query whitelist (DEV-1390 §6.5; shared by both facades).

A small list of connection-probe SQL patterns that BI tools and drivers
issue during connect / re-connect / dialect-sniffing. We answer them with
canned responses so the connection feels healthy without routing them into
the SLayer engine.

The matcher takes a parsed sqlglot expression (the translator parses once
and dispatches across multiple checks). On match, returns a ``RowBatch``
with the canned schema + data. On no match, returns ``None`` so the caller
falls through to the next pipeline step.
"""

from __future__ import annotations


import sqlglot.expressions as exp

import slayer
from slayer.core.enums import DataType
from slayer.facade.rows import FacadeColumn, RowBatch


def _batch_select_one() -> RowBatch:
    return RowBatch(
        columns=[FacadeColumn(name="1", type=DataType.INT)],
        rows=[{"1": 1}],
    )


def _batch_select_null_empty() -> RowBatch:
    return RowBatch(
        columns=[FacadeColumn(name="NULL", type=DataType.INT)],
        rows=[],
    )


def _batch_select_version() -> RowBatch:
    value = f"SLayer Flight SQL {slayer.__version__}"
    return RowBatch(
        columns=[FacadeColumn(name="version", type=DataType.TEXT)],
        rows=[{"version": value}],
    )


def _batch_select_current_database() -> RowBatch:
    return RowBatch(
        columns=[FacadeColumn(name="current_database", type=DataType.TEXT)],
        rows=[{"current_database": "slayer"}],
    )


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


def match_probe(parsed: exp.Expression) -> RowBatch | None:
    """Return the canned ``RowBatch`` for a matching probe, else ``None``."""
    if _matches_select_one(parsed):
        return _batch_select_one()
    if _matches_select_null_where_false(parsed):
        return _batch_select_null_empty()
    if _matches_select_version(parsed):
        return _batch_select_version()
    if _matches_select_current_database(parsed):
        return _batch_select_current_database()
    return None
