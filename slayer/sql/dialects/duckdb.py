"""DuckdbDialect.

DuckDB shape matches Postgres: native DATE_TRUNC, native PERCENTILE_CONT
(emitted via sqlglot's QUANTILE_CONT translation), native CORR / COVAR /
log10 / log2. Structure is read from DuckDB's own catalog functions, never
the Postgres catalog queries duckdb-engine inherits.
"""

from __future__ import annotations

from typing import Any, ClassVar

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.expressions.core import Expression

from slayer.sql.dialects.base import SqlDialect
from slayer.sql.dialects.reflection import (
    CatalogEngine,
    ReflectedColumn,
    ReflectedForeignKey,
    TableKeys,
    all_of,
    best_effort,
    call,
    col,
    column_from_type_string,
    eq_literal,
    fetch,
    table_function,
    without_primary_key,
)


def _scope(*, table: str, schema: str | None, catalog: str | None) -> list[Expression]:
    """One table of one schema of one catalog; unset parts are the connection's current ones."""
    return [
        eq_literal(col("table_name"), table),
        eq_literal(col("schema_name"), schema) if schema
        else exp.EQ(this=col("schema_name"), expression=call("current_schema")),
        eq_literal(col("database_name"), catalog) if catalog
        else exp.EQ(this=col("database_name"), expression=call("current_database")),
    ]


def _plain_index_columns(sql: str | None) -> list[str] | None:
    """The columns of a ``CREATE UNIQUE INDEX`` over plain column references, else ``None``."""
    if not sql:
        return None
    try:
        statement = sqlglot.parse_one(sql, read="duckdb")
    except SqlglotError:
        return None
    index = statement.this if isinstance(statement, exp.Create) else None
    params = index.args.get("params") if isinstance(index, exp.Index) else None
    if params is None or params.args.get("where") is not None:
        return None
    members = [m.this if isinstance(m, exp.Ordered) else m for m in params.args.get("columns") or []]
    if not members or not all(isinstance(m, exp.Column) and not m.table for m in members):
        return None
    return [m.name for m in members]


class DuckdbDialect(SqlDialect):
    sqlglot_name: str = "duckdb"
    ilike_native: ClassVar[bool] = True
    ds_type_aliases: frozenset[str] = frozenset({"duckdb"})
    sa_backend_names: frozenset[str] = frozenset({"duckdb"})
    explain_prefix: str | None = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    max_identifier_bytes: int | None = 256  # safe documented ceiling
    approx_count_distinct_native: bool = True
    url_scheme: str | None = "duckdb"

    def build_integer_sequence(self, *, size: int) -> exp.Select:
        return exp.select(exp.column("i")).from_(exp.Table(
            this=exp.Anonymous(this="RANGE", expressions=[exp.Literal.number(0), exp.Literal.number(size)]),
            alias=exp.TableAlias(this=exp.to_identifier("_seq"), columns=[exp.to_identifier("i")]),
        ))

    def reflect_columns(
        self, *, sa_engine: CatalogEngine | None, inspector: Any,  # NOSONAR(S1172) — hook signature
        table: str, schema: str | None, catalog: str | None,
    ) -> list[ReflectedColumn]:
        query = (
            exp.select(col("column_name"), col("data_type"), col("comment"))
            .from_(table_function("duckdb_columns"))
            .where(all_of(_scope(table=table, schema=schema, catalog=catalog)))
            .order_by(col("column_index"))
        )
        return [
            column_from_type_string(name=name, data_type=dtype, comment=comment)
            for name, dtype, comment in fetch(sa_engine, query, dialect=self.sqlglot_name)
        ]

    def reflect_table_keys(
        self, *, sa_engine: CatalogEngine | None, inspector: Any,  # NOSONAR(S1172) — hook signature
        table: str, schema: str | None, catalog: str | None,
    ) -> TableKeys:
        scope = _scope(table=table, schema=schema, catalog=catalog)

        def constraints(kind: str, *columns: str) -> list[tuple[Any, ...]]:
            query = (
                exp.select(*(col(c) for c in columns))
                .from_(table_function("duckdb_constraints"))
                .where(all_of([*(c.copy() for c in scope), eq_literal(col("constraint_type"), kind)]))
                .order_by(col("constraint_index"))
            )
            return fetch(sa_engine, query, dialect=self.sqlglot_name)

        def unique_indexes() -> list[list[str]]:
            query = (
                exp.select(col("sql"))
                .from_(table_function("duckdb_indexes"))
                .where(all_of([*(c.copy() for c in scope), col("is_unique")]))
            )
            sets = (_plain_index_columns(sql) for (sql,) in fetch(sa_engine, query, dialect=self.sqlglot_name))
            return [s for s in sets if s]

        pk_rows = best_effort(
            lambda: constraints("PRIMARY KEY", "constraint_column_names"),
            default=[], table=table, what="primary key",
        )
        pk = list(pk_rows[0][0]) if pk_rows else []
        fks = best_effort(
            lambda: [
                ReflectedForeignKey(
                    columns=list(cols), referred_schema=schema,
                    referred_table=referred, referred_columns=list(referred_cols),
                )
                for cols, referred, referred_cols in constraints(
                    "FOREIGN KEY", "constraint_column_names", "referenced_table", "referenced_column_names",
                )
            ],
            default=[], table=table, what="foreign keys",
        )
        uniques = best_effort(
            lambda: [list(cols) for (cols,) in constraints("UNIQUE", "constraint_column_names")],
            default=[], table=table, what="unique constraints",
        )
        indexes = best_effort(unique_indexes, default=[], table=table, what="unique indexes")
        return TableKeys(
            primary_key=pk, foreign_keys=fks, unique_key_sets=without_primary_key(pk, uniques + indexes),
        )
