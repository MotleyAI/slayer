"""DuckdbDialect.

DuckDB shape matches Postgres: native DATE_TRUNC, native PERCENTILE_CONT
(emitted via sqlglot's QUANTILE_CONT translation), native CORR / COVAR /
log10 / log2.
"""

from __future__ import annotations

from sqlglot import exp

from slayer.sql.dialects.base import SqlDialect


class DuckdbDialect(SqlDialect):
    sqlglot_name: str = "duckdb"
    ds_type_aliases: frozenset[str] = frozenset({"duckdb"})
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
