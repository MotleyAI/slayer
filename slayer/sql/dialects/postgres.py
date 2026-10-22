"""PostgresDialect: the Postgres-shaped base default made explicit."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar

from sqlglot import exp
from sqlglot.expressions.datatypes import DType
from sqlglot.expressions.core import Expression

from slayer.sql.dialects.base import ServerProfile, SqlDialect, as_text

#: The first Postgres major version with ``pg_input_is_valid``.
_PG_INPUT_IS_VALID_SINCE = (16, 0)


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
    vendor_cast_types: ClassVar[frozenset[DType]] = frozenset({exp.DataType.Type.MONEY})
    ilike_native: ClassVar[bool] = True
    explain_prefix: str | None = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    # NAMEDATALEN: 63 usable bytes; over-length names are SILENTLY truncated.
    max_identifier_bytes: int | None = 63
    statement_timeout_best_effort: bool = True
    try_cast_reads_profile: bool = True
    url_scheme: str | None = "postgresql+psycopg2"
    async_driver: str | None = "asyncpg"
    install_extra: str | None = "postgres"

    def server_profile_sql(self) -> str | None:
        return "SELECT current_setting('server_version_num')"

    def parse_server_profile(
        self, *, base_row: Sequence[Any] | None, correlated_row: Sequence[Any] | None = None,
    ) -> ServerProfile:
        number = base_row[0] if base_row else None
        try:
            version_num = int(number) if number is not None else None
        except (TypeError, ValueError):
            version_num = None
        return ServerProfile(version=None if version_num is None else (version_num // 10000, version_num % 10000))

    def build_try_cast(self, value: Expression, to: exp.DataType) -> Expression:
        """Postgres 16+ checks the input with ``pg_input_is_valid``; an older or unknown server gets a plain cast."""
        version = self.server_profile.version
        if version is None or version < _PG_INPUT_IS_VALID_SINCE:
            return exp.Cast(this=value, to=to)
        check = exp.Anonymous(this="pg_input_is_valid", expressions=[
            as_text(value), exp.Literal.string(to.sql(dialect=self.sqlglot_name)),
        ])
        return exp.Case(ifs=[exp.If(this=check, true=exp.Cast(this=value.copy(), to=to))])

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
