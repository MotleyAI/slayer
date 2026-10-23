"""DuckdbDialect.

DuckDB shape matches Postgres: native DATE_TRUNC, native PERCENTILE_CONT
(emitted via sqlglot's QUANTILE_CONT translation), native CORR / COVAR /
log10 / log2.
"""

from __future__ import annotations

from typing import ClassVar

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.json_path import JsonPath
from slayer.core.regex_pattern import RegexPattern, RegexReplacement
from slayer.sql.dialects.base import SqlDialect, TextFn, anon, case, lit, pad_fill
from slayer.sql.dialects.patterns import RE2, RegexFlavour, emit_replacement

#: ``json_type`` values that are not a scalar.
_NON_SCALAR_JSON = ("OBJECT", "ARRAY", "NULL")


class DuckdbDialect(SqlDialect):
    sqlglot_name: str = "duckdb"
    ilike_native: ClassVar[bool] = True
    ds_type_aliases: frozenset[str] = frozenset({"duckdb"})
    explain_prefix: str | None = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True
    max_identifier_bytes: int | None = 256  # safe documented ceiling
    approx_count_distinct_native: bool = True
    url_scheme: str | None = "duckdb"
    regex_flavour: ClassVar[RegexFlavour] = RE2

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

    def build_integer_sequence(self, *, size: int) -> exp.Select:
        return exp.select(exp.column("i")).from_(exp.Table(
            this=exp.Anonymous(this="RANGE", expressions=[exp.Literal.number(0), exp.Literal.number(size)]),
            alias=exp.TableAlias(this=exp.to_identifier("_seq"), columns=[exp.to_identifier("i")]),
        ))
