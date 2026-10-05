"""PostgresDialect: the Postgres-shaped base default made explicit."""

from __future__ import annotations


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
    explain_prefix: str | None = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    # NAMEDATALEN: 63 usable bytes; over-length names are SILENTLY truncated.
    max_identifier_bytes: int | None = 63
    statement_timeout_best_effort: bool = True
    url_scheme: str | None = "postgresql"
    sync_driver: str | None = "psycopg2"
    async_driver: str | None = "asyncpg"
    install_extra: str | None = "postgres"

    def build_integer_sequence(self, *, size: int) -> exp.Select:
        return exp.select(exp.column("i")).from_(exp.Table(
            this=exp.Anonymous(this="GENERATE_SERIES", expressions=[
                exp.Literal.number(0), exp.Literal.number(size - 1),
            ]),
            alias=exp.TableAlias(this=exp.to_identifier("_seq"), columns=[exp.to_identifier("i")]),
        ))

    def statement_timeout_sql(self, timeout_seconds: int) -> str | None:
        """Transaction-local, so it never outlives the call on a pooled connection."""
        return f"SET LOCAL statement_timeout = {timeout_seconds * 1000}"

    def rewrite_target_ast(self, tree: exp.Expression) -> exp.Expression:
        """Numeric-cast every 2-arg ROUND's first arg: Postgres has no ``round(double, int)``."""
        return tree.transform(_cast_round_arg_to_numeric)
