"""PostgresDialect: the Postgres-shaped base default made explicit."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pydantic import Field
from sqlglot import exp

from slayer.core.enums import DataType
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.dialects.reflection import (
    ResultColumnType,
    TableLocation,
    all_of,
    call,
    col,
    eq_literal,
    relation,
    result_column_type_of_name,
)

_NUMERIC_OID = 1700

# ``pg_type`` OIDs → canonical type names.
_PG_TYPE_NAMES: dict[int, str] = {
    16: "boolean", 17: "bytea", 18: '"char"', 20: "bigint", 21: "smallint", 23: "integer", 25: "text",
    26: "oid", 114: "json", 142: "xml", 650: "cidr", 700: "real", 701: "double precision", 790: "money",
    869: "inet", 1000: "boolean[]", 1005: "smallint[]", 1007: "integer[]", 1009: "text[]",
    1015: "character varying[]", 1016: "bigint[]", 1021: "real[]", 1022: "double precision[]",
    1042: "character", 1043: "character varying", 1082: "date", 1083: "time",
    1114: "timestamp without time zone", 1184: "timestamp with time zone", 1186: "interval",
    1231: "numeric[]", 1266: "time with time zone", 1560: "bit", 1562: "bit varying", 1700: "numeric",
    2950: "uuid", 3802: "jsonb",
}


def describe_pg_cursor_column(description_row: Sequence[Any]) -> ResultColumnType | None:
    """A psycopg description row by OID; ``numeric`` carries its typmod; an unknown OID is unnamed TEXT."""
    code = description_row[1]
    if not isinstance(code, int):
        return None
    name = _PG_TYPE_NAMES.get(code)
    if name is None:
        return ResultColumnType(type=DataType.TEXT)
    precision, scale = description_row[4], description_row[5]
    if code == _NUMERIC_OID and precision is not None and scale is not None:
        name = f"numeric({precision},{scale})"
    return result_column_type_of_name(name)


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
    sa_backend_names: frozenset[str] = frozenset({"postgresql"})
    explain_prefix: str | None = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    # NAMEDATALEN: 63 usable bytes; over-length names are SILENTLY truncated.
    max_identifier_bytes: int | None = 63
    statement_timeout_best_effort: bool = True
    url_scheme: str | None = "postgresql+psycopg"
    install_extra: str | None = "postgres"
    extra_drivers: dict[str, str] = Field(
        default_factory=lambda: {"psycopg": "psycopg", "psycopg2": "psycopg2-binary", "asyncpg": "asyncpg"},
    )
    refused_loops: dict[str, frozenset[str]] = Field(
        default_factory=lambda: {"psycopg": frozenset({"ProactorEventLoop"})},
    )

    def describe_cursor_column(self, description_row: Sequence[Any]) -> ResultColumnType | None:
        return describe_pg_cursor_column(description_row)

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
        """Numeric-cast every 2-arg ROUND's first arg: Postgres has no ``round(double, int)``."""
        return tree.transform(_cast_round_arg_to_numeric)
