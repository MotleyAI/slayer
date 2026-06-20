"""DEV-1542: PostgresDialect.

Postgres is the Postgres-shaped default made explicit. Native DATE_TRUNC,
PERCENTILE_CONT, CORR, COVAR_SAMP, COVAR_POP, native log10/log2.
"""

from __future__ import annotations

from typing import Optional

from sqlglot import exp

from slayer.sql.dialects.base import SqlDialect


def _cast_round_arg_to_numeric(node: exp.Expression) -> exp.Expression:
    """Wrap the first arg of a 2-arg ``ROUND`` in ``CAST(... AS DECIMAL)``.

    Postgres has no ``round(double precision, integer)`` overload — only
    ``round(numeric, integer)`` — so a 2-arg round over a DOUBLE expression
    fails without an explicit numeric cast. 1-arg round (``round(double)``)
    is fine and left alone. Idempotent: skips when the arg is already cast to
    a numeric/decimal type.
    """
    if not isinstance(node, exp.Round):
        return node
    decimals = node.args.get("decimals")
    if decimals is None:  # 1-arg round — no overload problem.
        return node
    inner = node.this
    if isinstance(inner, exp.Cast):
        cast_to = inner.to
        if isinstance(cast_to, exp.DataType) and cast_to.this in (
            exp.DataType.Type.DECIMAL,
            exp.DataType.Type.BIGDECIMAL,
        ):
            return node  # already numeric-cast — idempotent.
    node.set("this", exp.cast(inner.copy(), "DECIMAL"))
    return node


class PostgresDialect(SqlDialect):
    sqlglot_name: str = "postgres"
    ds_type_aliases: frozenset[str] = frozenset({"postgres", "postgresql"})
    explain_prefix: Optional[str] = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    def rewrite_target_ast(self, tree: exp.Expression) -> exp.Expression:
        """DEV-1576: numeric-cast the first arg of every 2-arg ROUND so
        ``round(double precision, int)`` becomes ``round(numeric, int)``."""
        return tree.transform(_cast_round_arg_to_numeric)
