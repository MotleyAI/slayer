"""PostgresDialect: the reference meaning of the portable library."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, ClassVar, Optional

from pydantic import Field
from sqlglot import exp
from sqlglot.expressions.datatypes import DType
from sqlglot.expressions.core import Expression

from slayer.core.enums import DataType
from slayer.core.json_path import JsonPath
from slayer.core.keys import CastKey, ScalarCallKey
from slayer.core.regex_pattern import RegexPattern, RegexReplacement
from slayer.sql.dialects.base import (
    LIST_ELEMENT,
    LegacyTypeCategories,
    CONSTANT_INPUT,
    PORTABLE_LIBRARY,
    IdentifierFolding,
    set_statement,
    NativeFn,
    ListFn,
    ServerProfile,
    SqlDialect,
    TextFn,
    anon,
    as_text,
    case,
    is_empty_array,
    is_not_null,
    is_null,
    lit,
    operand_copy,
    pad_fill,
)
from slayer.sql.dialects.patterns import POSTGRES, RegexFlavour
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
    2950: "uuid", 3802: "jsonb", 199: "json[]", 1001: "bytea[]", 1014: "character[]",
    1115: "timestamp without time zone[]", 1182: "date[]", 1185: "timestamp with time zone[]", 2951: "uuid[]",
    3807: "jsonb[]",
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


#: The first Postgres major version with ``pg_input_is_valid``.
_PG_INPUT_IS_VALID_SINCE = (16, 0, 0)
#: The array type both lists of ``contains_any`` / ``contains_all`` are read as, by element type.
#: The element-set subquery's one relation: a fixed name, so every copy of the expression reads alike.
_SET_RELATION = "__slayer_s"
_CANONICAL_ARRAYS = {
    DataType.TEXT: "TEXT[]", DataType.INT: "BIGINT[]", DataType.DOUBLE: "DOUBLE PRECISION[]",
    DataType.BOOLEAN: "BOOLEAN[]", DataType.DATE: "DATE[]", DataType.TIMESTAMP: "TIMESTAMP[]",
}


def _server_version(number: int) -> tuple[int, int, int]:
    """``server_version_num`` as ``(major, minor, patch)``: ``MMmmpp`` before 10, ``MM00mm`` after."""
    if number < 100000:
        return number // 10000, number // 100 % 100, number % 100
    return number // 10000, number % 10000, 0


def _numeric(value: Expression) -> Expression:
    """``value`` as ``numeric``: a number literal already converts, a ``DECIMAL`` cast already is."""
    if (isinstance(value, exp.Literal) and value.is_number) or (
            isinstance(value, exp.Cast) and value.to.is_type(exp.DataType.Type.DECIMAL)):
        return value
    return exp.cast(value.copy(), "DECIMAL")


def _cast_log_arg_to_numeric(node: Expression) -> Expression:
    """A based ``LOG(b, x)`` has only the ``numeric`` overload: cast both (Postgres has no ``log2``)."""
    if not isinstance(node, exp.Log) or node.args.get("expression") is None:
        return node
    node.set("this", _numeric(node.this))
    node.set("expression", _numeric(node.expression))
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


def _constant(value: Expression) -> bool:
    """The outermost ``TRY_CAST`` input's own tag (``walk`` is breadth-first), so a nested cast's never counts."""
    return next((bool(node.meta[CONSTANT_INPUT]) for node in value.walk() if CONSTANT_INPUT in node.meta), False)


class PostgresDialect(SqlDialect):
    sqlglot_name: str = "postgres"
    array_cast_types: ClassVar[bool] = True
    ds_type_aliases: frozenset[str] = frozenset({"postgres", "postgresql"})
    vendor_cast_types: ClassVar[frozenset[DType]] = frozenset({exp.DataType.Type.MONEY})
    ilike_native: ClassVar[bool] = True
    identifier_folding: ClassVar[IdentifierFolding] = "lower"
    sa_backend_names: frozenset[str] = frozenset({"postgresql"})
    explain_prefix: str | None = "EXPLAIN ANALYZE"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = False
    # NAMEDATALEN: 63 usable bytes; over-length names are SILENTLY truncated.
    max_identifier_bytes: int | None = 63
    statement_timeout_best_effort: bool = True
    json_db_types: ClassVar[frozenset[str]] = frozenset({"JSON", "JSONB"})
    url_scheme: str | None = "postgresql+psycopg"
    install_extra: str | None = "postgres"
    extra_drivers: dict[str, str] = Field(
        default_factory=lambda: {"psycopg": "psycopg", "psycopg2": "psycopg2-binary", "asyncpg": "asyncpg"},
    )
    refused_loops: dict[str, frozenset[str]] = Field(
        default_factory=lambda: {"psycopg": frozenset({"ProactorEventLoop"})},
    )
    legacy_type_categories: LegacyTypeCategories = LegacyTypeCategories(
        names={
            "boolean": "boolean", "bigint": "number", "smallint": "number", "integer": "number", "oid": "number",
            "real": "number", "double precision": "number", "numeric": "number", "money": "number",
            "date": "time", "time": "time", "timestamp without time zone": "time",
            "timestamp with time zone": "time", "interval": "time",
        },
        other="string",
    )

    database_name: ClassVar[str] = "Postgres"
    native_lists: ClassVar[bool] = True
    library_functions: ClassVar[frozenset[str]] = PORTABLE_LIBRARY
    native_functions: ClassVar[frozenset[NativeFn]] = frozenset({
        "REVERSE", "CONCAT_WS", "CHR", "ASCII", "SPLIT_PART", "PI", "CBRT", "GREATEST", "LEAST",
    })
    regex_flavour: ClassVar[Optional[RegexFlavour]] = POSTGRES

    def describe_cursor_column(self, description_row: Sequence[Any]) -> ResultColumnType | None:
        return describe_pg_cursor_column(description_row)

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
        return ServerProfile(version=None if version_num is None else _server_version(version_num))

    def validates_input(self) -> bool:
        """Whether ``pg_input_is_valid`` can check a conversion's input (Postgres 16+)."""
        version = self.server_profile.version
        return version is not None and version >= _PG_INPUT_IS_VALID_SINCE

    def build_try_cast(self, value: Expression, to: exp.DataType) -> Expression:
        """Postgres 16+ checks the input with ``pg_input_is_valid``; an older or unknown server gets a plain cast."""
        if not self.validates_input():
            return exp.Cast(this=value, to=to)
        check = exp.Anonymous(this="pg_input_is_valid", expressions=[
            as_text(value), exp.Literal.string(to.sql(dialect=self.sqlglot_name)),
        ])
        # A constant input is read through a subquery: Postgres folds a constant cast at plan time, invalid or not.
        read = exp.Subquery(this=exp.select(value.copy())) if _constant(value) else value.copy()
        return exp.Case(ifs=[exp.If(this=check, true=exp.Cast(this=read, to=to))])

    def build_integer_text_try_cast(self, text: Expression, to: exp.DataType) -> Expression:
        """``pg_input_is_valid`` already accepts only integer text in range."""
        return self.build_try_cast(text, to)

    def build_regexp_like(self, value: Expression, pattern: RegexPattern, case_insensitive: bool) -> Expression:
        node = exp.RegexpILike if case_insensitive else exp.RegexpLike
        return node(this=value.copy(), expression=self.regex_text(pattern))

    def build_regexp_extract(
        self, value: Expression, pattern: RegexPattern, group: int, case_insensitive: bool,
    ) -> Expression:
        match = anon("REGEXP_MATCH", value, self.regex_text(pattern, wrap=True),
                     *([lit("i")] if case_insensitive else []))
        picked = exp.Bracket(this=exp.Paren(this=match), expressions=[lit(group)])
        return self.skipped_group_empty(value, pattern, group, picked)

    def build_regexp_replace(
        self, value: Expression, pattern: RegexPattern, replacement: RegexReplacement, case_insensitive: bool,
    ) -> Expression:
        return anon("REGEXP_REPLACE", value, self.regex_text(pattern), self.replacement_text(replacement),
                    lit("gi" if case_insensitive else "g"))

    def build_json_value(self, value: Expression, path: JsonPath, native: bool) -> Expression:
        jsonb = exp.DataType.build("JSONB")
        already = isinstance(value, exp.Cast) and value.to.is_type(exp.DataType.Type.JSONB)
        doc = value.copy() if already else exp.Cast(this=value.copy(), to=jsonb)
        keys = [lit(str(step)) for step in path.steps]
        found = anon("JSONB_EXTRACT_PATH", doc, *keys)
        scalar = exp.In(this=anon("JSONB_TYPEOF", found), expressions=[lit("string"), lit("number"), lit("boolean")])
        read = case((scalar, anon("JSONB_EXTRACT_PATH_TEXT", doc, *keys)))
        if native or not self.validates_input():
            return read
        valid = anon("PG_INPUT_IS_VALID", as_text(value), lit("jsonb"))
        return case((valid, read))

    def build_list_call(
        self, fn: ListFn, args: tuple[Expression, ...], *, each: Optional[DataType] = None,
        element: Optional[DataType] = None,
    ) -> Expression:
        """``&&`` / ``@>`` need one array type on both sides: each list is read as its element's canonical one."""
        array = _CANONICAL_ARRAYS.get(element) if element is not None else None
        if fn in ("contains_any", "contains_all") and each is None and array and not is_empty_array(args[1]):
            to = exp.DataType.build(array, dialect=self.sqlglot_name)
            args = tuple(exp.Cast(this=a.copy(), to=to.copy()) for a in args)
        return super().build_list_call(fn, args, each=each, element=element)

    def list_length(self, values: Expression) -> Expression:
        return anon("CARDINALITY", values)

    def build_list_set(self, values: Expression, element: Optional[DataType]) -> Expression:
        """``ARRAY(SELECT DISTINCT e FROM UNNEST(x) e … ORDER BY e)``, ``x`` read as its element's canonical array
        type so lists of one family compare; both CASE branches carry that type."""
        array = _CANONICAL_ARRAYS.get(element) if element is not None else None
        to = exp.DataType.build(array, dialect=self.sqlglot_name) if array else None
        typed = exp.Cast(this=values.copy(), to=to.copy()) if to is not None else values.copy()
        e = exp.column(LIST_ELEMENT)
        rows = (exp.select(e).distinct().from_(exp.Unnest(
            expressions=[typed],
            alias=exp.TableAlias(this=exp.to_identifier(_SET_RELATION), columns=[exp.to_identifier(LIST_ELEMENT)])))
            .where(is_not_null(e.copy())).order_by(e.copy()))
        absent: Expression = exp.Cast(this=exp.Null(), to=to.copy()) if to is not None else exp.Null()
        return case((is_null(values), absent), default=exp.Array(expressions=[rows]))

    def build_list_key(self, values: Expression) -> Expression:
        return values

    def list_contains(self, values: Expression, value: Expression) -> Expression:
        found = exp.EQ(this=operand_copy(value), expression=exp.Any(this=exp.Paren(this=values.copy())))
        return exp.Coalesce(this=found, expressions=[exp.false()])

    def list_overlaps(self, values: Expression, other: Expression) -> Expression:
        return exp.ArrayOverlaps(this=operand_copy(values), expression=operand_copy(other))

    def list_contains_all(self, values: Expression, other: Expression) -> Expression:
        return exp.ArrayContainsAll(this=operand_copy(values), expression=anon("ARRAY_REMOVE", other, exp.Null()))

    def empty_list(self) -> Expression:
        """``'{}'``: an empty ``ARRAY[]`` has no type to take."""
        return lit("{}")

    def build_collect(self, value: Expression, element: Optional[DataType]) -> Expression:
        return self.filtered_collect(value)

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

    def statement_timeout(self, timeout_seconds: int) -> Expression | None:
        """Transaction-local, so it never outlives the call on a pooled connection."""
        return set_statement(name="statement_timeout", value=timeout_seconds * 1000, kind="LOCAL")

    def rewrite_target_ast(self, tree: exp.Expression) -> exp.Expression:
        """Numeric-cast every 2-arg ROUND's and based LOG's value: their only overloads take ``numeric``."""
        return tree.transform(_cast_round_arg_to_numeric).transform(_cast_log_arg_to_numeric)
