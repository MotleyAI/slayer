"""PostgresDialect: the Postgres-shaped base default made explicit."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, ClassVar

from sqlglot import exp
from sqlglot.expressions.datatypes import DType
from sqlglot.expressions.core import Expression

from slayer.core.keys import CastKey, ScalarCallKey
from slayer.sql.dialects.base import ServerProfile, SqlDialect, TextFn, anon, as_text, pad_fill
from slayer.sql.dialects.reflection import TableLocation, all_of, call, col, eq_literal, relation

#: The first Postgres major version with ``pg_input_is_valid``.
_PG_INPUT_IS_VALID_SINCE = (16, 0)


def _cast_log_arg_to_numeric(node: Expression) -> Expression:
    """A based ``LOG(b, x)`` has only the ``numeric`` overload: cast ``x`` (Postgres has no ``log2``)."""
    if not isinstance(node, exp.Log) or node.args.get("expression") is None:
        return node
    value = node.expression
    if not (isinstance(value, exp.Cast) and value.to.is_type(exp.DataType.Type.DECIMAL)):
        node.set("expression", exp.cast(value.copy(), "DECIMAL"))
    return node


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
    sa_backend_names: frozenset[str] = frozenset({"postgresql"})
    explain_prefix: str | None = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = False
    # NAMEDATALEN: 63 usable bytes; over-length names are SILENTLY truncated.
    max_identifier_bytes: int | None = 63
    statement_timeout_best_effort: bool = True
    json_db_types: ClassVar[frozenset[str]] = frozenset({"JSON", "JSONB"})
    url_scheme: str | None = "postgresql+psycopg2"
    async_driver: str | None = "asyncpg"
    install_extra: str | None = "postgres"

    def profile_needed(self, keys: Iterable[object]) -> bool:
        """``TRY_CAST`` and ``json_value`` check their input with ``pg_input_is_valid`` on 16+."""
        return any(
            (isinstance(k, CastKey) and k.try_) or (isinstance(k, ScalarCallKey) and k.name == "json_value")
            for k in keys
        )

    def server_profile_sql(self) -> str | None:
        return "SELECT current_setting('server_version_num')"

    def build_text_call(self, fn: TextFn, args: tuple[Expression, ...]) -> Expression:
        """``left`` / ``right`` / ``lpad`` / ``rpad`` / ``repeat`` are native with the reference meaning."""
        if fn in ("left", "right", "lpad", "rpad", "repeat"):
            fill = [pad_fill(args)] if fn in ("lpad", "rpad") and len(args) == 2 else []
            return anon(fn.upper(), *args, *fill)
        return super().build_text_call(fn, args)

    def build_modulo(self, left: Expression, right: Expression, integer: bool) -> Expression:
        """``%`` has no DOUBLE overload: non-integers go through NUMERIC."""
        if integer:
            return super().build_modulo(left, right, integer)
        numeric = exp.DataType.build("NUMERIC")
        return anon("MOD", exp.Cast(this=left.copy(), to=numeric), exp.Cast(this=right.copy(), to=numeric))

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

    def column_comments_query(self, location: TableLocation) -> exp.Select:
        scope = (
            eq_literal(col("nspname", "n"), location.schema_name) if location.schema_name
            else call("pg_table_is_visible", col("oid", "c"))
        )
        return (
            exp.select(col("attname", "a"), call("col_description", col("oid", "c"), col("attnum", "a")))
            .from_(relation("pg_class", db="pg_catalog", alias="c"))
            .join(
                relation("pg_namespace", db="pg_catalog", alias="n"),
                on=exp.EQ(this=col("oid", "n"), expression=col("relnamespace", "c")),
            )
            .join(
                relation("pg_attribute", db="pg_catalog", alias="a"),
                on=exp.EQ(this=col("attrelid", "a"), expression=col("oid", "c")),
            )
            .where(all_of([
                eq_literal(col("relname", "c"), location.name),
                exp.GT(this=col("attnum", "a"), expression=exp.Literal.number(0)),
                exp.Not(this=col("attisdropped", "a")),
                scope,
            ]))
        )

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
        """Numeric-cast every 2-arg ROUND's and based LOG's value: their only overloads take ``numeric``."""
        return tree.transform(_cast_round_arg_to_numeric).transform(_cast_log_arg_to_numeric)
