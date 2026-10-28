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

from slayer.core.json_path import JsonPath
from slayer.core.regex_pattern import RegexPattern, RegexReplacement
from slayer.sql.dialects.base import SqlDialect, TextFn, anon, case, lit, non_null_elements, pad_fill
from slayer.sql.dialects.patterns import RE2, RegexFlavour, emit_replacement
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

#: ``json_type`` values that are not a scalar.
_NON_SCALAR_JSON = ("OBJECT", "ARRAY", "NULL")


def _compact(values: Expression) -> Expression:
    return non_null_elements(values, filter_name="LIST_FILTER")


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
    regex_flavour: ClassVar[RegexFlavour] = RE2
    database_name: ClassVar[str] = "DuckDB"

    def build_text_call(self, fn: TextFn, args: tuple[Expression, ...]) -> Expression:
        """``left`` / ``right`` / ``lpad`` / ``rpad`` / ``repeat`` are native with the reference meaning."""
        if fn in ("left", "right", "lpad", "rpad", "repeat"):
            fill = [pad_fill(args)] if fn in ("lpad", "rpad") and len(args) == 2 else []
            return anon(fn.upper(), *args, *fill)
        return super().build_text_call(fn, args)

    def build_regexp_like(self, value: Expression, pattern: RegexPattern, case_insensitive: bool) -> Expression:
        return anon("REGEXP_MATCHES", value, self.regex_text(pattern, case_insensitive=case_insensitive))

    def build_regexp_extract(
        self, value: Expression, pattern: RegexPattern, group: int, case_insensitive: bool,
    ) -> Expression:
        """The first of ``regexp_extract_all`` (NULL on no match, unlike ``regexp_extract``'s ``''``)."""
        matches = anon("REGEXP_EXTRACT_ALL", value, self.regex_text(pattern, case_insensitive=case_insensitive),
                       lit(group))
        return self.skipped_group_empty(value, pattern, group, anon("LIST_EXTRACT", matches, lit(1)))

    def build_regexp_replace(
        self, value: Expression, pattern: RegexPattern, replacement: RegexReplacement, case_insensitive: bool,
    ) -> Expression:
        return anon("REGEXP_REPLACE", value, self.regex_text(pattern, case_insensitive=case_insensitive),
                    lit(emit_replacement(replacement, self.regex_flavour)), lit("g"))

    def build_json_value(self, value: Expression, path: JsonPath, native: bool) -> Expression:
        text = lit(path.text)
        scalar = exp.Not(this=exp.In(this=anon("JSON_TYPE", value, text),
                                     expressions=[lit(t) for t in _NON_SCALAR_JSON]))
        read = case((scalar, anon("JSON_EXTRACT_STRING", value, text)))
        return case((anon("JSON_VALID", value), read))

    def list_length(self, values: Expression) -> Expression:
        return anon("LEN", _compact(values))

    def list_contains(self, values: Expression, value: Expression) -> Expression:
        return anon("LIST_CONTAINS", _compact(values), value)

    def list_overlaps(self, values: Expression, other: Expression) -> Expression:
        return anon("LIST_HAS_ANY", _compact(values), _compact(other))

    def list_contains_all(self, values: Expression, other: Expression) -> Expression:
        return anon("LIST_HAS_ALL", _compact(values), _compact(other))

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
            column_from_type_string(
                name=name, data_type=dtype, comment=comment,
                zoned_types=self.zoned_timestamp_db_types, json_types=self.json_db_types,
            )
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
