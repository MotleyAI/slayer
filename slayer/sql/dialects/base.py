"""SqlDialect strategy base class.

Every dialect-specific SQL-generation quirk lives on a subclass of
``SqlDialect``. The base class itself is a fully concrete Postgres-shaped
default — concrete dialects (``SqliteDialect``, ``TsqlDialect``, ...)
override only the methods whose behaviour differs.

The class is a Pydantic ``BaseModel`` with ``frozen=True`` so registry
singletons can't drift. Method overrides happen via regular subclassing —
fields use class-level defaults (``sqlglot_name: str = "postgres"``).
"""

from __future__ import annotations

import hashlib
import re
from functools import lru_cache
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Optional, TypeGuard, get_args
from collections.abc import Callable, Sequence

from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.expressions.core import Expression
from sqlglot.dialects.dialect import Dialect as _SqlglotDialect

from slayer.core.enums import DataType, TimeGranularity
from slayer.core.errors import IdentifierCollisionError, IdentifierLengthError
from slayer.sql._identifier_fit import (
    SqlLexis,
    fit_identifier,
    overlimit_tokens,
    quoted_identifiers,
    substitute_quoted,
)
from slayer.sql.naming_bijection import decode_alias, encode_alias

if TYPE_CHECKING:
    import sqlalchemy as sa

    from slayer.core.models import DatasourceConfig


# ---------------------------------------------------------------------------
# Granularity & duration mapping (used by default impls of date_trunc /
# time-offset / interval helpers)
# ---------------------------------------------------------------------------

_GRANULARITY_TO_DATE_TRUNC = {
    TimeGranularity.SECOND: "second",
    TimeGranularity.MINUTE: "minute",
    TimeGranularity.HOUR: "hour",
    TimeGranularity.DAY: "day",
    TimeGranularity.WEEK: "week",
    TimeGranularity.MONTH: "month",
    TimeGranularity.QUARTER: "quarter",
    TimeGranularity.YEAR: "year",
}

_WINDOW_UNIT_SQL = {
    "y": "year",
    "m": "month",
    "w": "week",
    "d": "day",
    "h": "hour",
    "min": "minute",
    "s": "second",
}


def _granularity_to_unit(granularity: str) -> str:
    """Map a granularity string to a SQL INTERVAL unit name.

    Quarter has no INTERVAL unit on most dialects — callers normalise to
    ``MONTH`` with the value multiplied by 3 before invoking the default.
    Week stays ``WEEK`` (Postgres / MySQL / ClickHouse / BigQuery all
    accept it). SQLite + T-SQL override the whole method.
    """
    return {
        "year": "YEAR",
        "month": "MONTH",
        "day": "DAY",
        "quarter": "MONTH",  # caller multiplies by 3
        "week": "WEEK",
        # A one-period shift of a Sunday-week is just one week.
        "week_sunday": "WEEK",
        "hour": "HOUR",
        "minute": "MINUTE",
        "second": "SECOND",
    }.get(granularity, granularity.upper())


# ---------------------------------------------------------------------------
# Shared variance-decomposition formula (used by MySQL + T-SQL overrides
# of build_covar_2arg).
# ---------------------------------------------------------------------------


TimeUnit = Literal[
    "second", "minute", "hour", "day", "week", "week_sunday", "month", "quarter", "year",
]
StatAgg1Name = Literal["stddev_samp", "stddev_pop", "var_samp", "var_pop"]
StatAgg2Name = Literal["corr", "covar_samp", "covar_pop"]

def is_stat_agg1(name: str) -> TypeGuard[StatAgg1Name]:
    return name in get_args(StatAgg1Name)


def is_stat_agg2(name: str) -> TypeGuard[StatAgg2Name]:
    return name in get_args(StatAgg2Name)


_OPERATOR_SHAPED = (exp.Binary, exp.Unary, exp.Between, exp.In)


def is_operator(node: object) -> bool:
    """Whether ``node`` is an operator expression (needs parens as an operand)."""
    return isinstance(node, _OPERATOR_SHAPED) and not isinstance(node, exp.Paren)


def operand_copy(value: Expression) -> Expression:
    """A copy of ``value`` safe to place as an operator's operand."""
    return exp.Paren(this=value.copy()) if is_operator(value) else value.copy()


def _build_covar_decomposition(
    *,
    col_expr: Expression,
    other_expr: Expression,
    agg: StatAgg2Name,
    var_fn_samp: str,
    var_fn_pop: str,
    stddev_fn: str,
) -> Expression:
    """corr / covar via ``cov(x, y) = (Var(x+y) - Var(x) - Var(y)) / 2`` for dialects without them.

    Each leg is NULL-guarded by the other; ``exp.Anonymous`` calls dodge sqlglot's
    MySQL VAR_SAMP → VARIANCE (= VAR_POP) rewrite.
    """
    var_fn = var_fn_samp if agg in ("covar_samp", "corr") else var_fn_pop

    def _guarded(value: Expression, guard: Expression) -> Expression:
        return exp.Case(ifs=[exp.If(
            this=exp.Not(this=exp.Is(this=operand_copy(guard), expression=exp.Null())),
            true=value.copy(),
        )])

    x_guarded = _guarded(col_expr, other_expr)
    y_guarded = _guarded(other_expr, col_expr)

    def _call(fn: str, *args: Expression) -> exp.Anonymous:
        return exp.Anonymous(this=fn, expressions=[a.copy() for a in args])

    xy_sum = exp.Add(this=x_guarded.copy(), expression=y_guarded.copy())
    covar = exp.Div(
        this=exp.Paren(this=exp.Sub(
            this=exp.Sub(this=_call(var_fn, xy_sum), expression=_call(var_fn, x_guarded)),
            expression=_call(var_fn, y_guarded),
        )),
        expression=exp.Literal.number(2),
    )

    if agg != "corr":
        return covar

    raw_denom = exp.Paren(this=exp.Mul(
        this=_call(stddev_fn, x_guarded), expression=_call(stddev_fn, y_guarded),
    ))
    denom = exp.Anonymous(
        this="NULLIF", expressions=[raw_denom, exp.Literal.number(0)]
    )
    return exp.Div(this=covar, expression=denom)


# ---------------------------------------------------------------------------
# SqlDialect — base class with Postgres-shaped defaults
# ---------------------------------------------------------------------------


@lru_cache(maxsize=None)
def _sqlglot_backslash_escapes(sqlglot_name: str) -> bool:
    """Whether sqlglot's tokenizer treats backslash as a string escape — read from the
    parser itself so Mode-A escaping can't drift; a reshaped internal API fails loudly."""
    tokenizer = _SqlglotDialect.get_or_raise(sqlglot_name).tokenizer_class
    escapes = getattr(tokenizer, "STRING_ESCAPES", None)
    if not isinstance(escapes, (list, tuple, set, frozenset)):
        raise RuntimeError(
            f"Cannot derive the backslash-escaping regime for sqlglot dialect "
            f"{sqlglot_name!r}: its tokenizer's STRING_ESCAPES is "
            f"{type(escapes).__name__}, expected a collection of strings. A "
            f"sqlglot upgrade may have changed this internal API."
        )
    return "\\" in escapes


@lru_cache(maxsize=None)
def _sqlglot_masking_lexis(sqlglot_name: str) -> SqlLexis:
    """Lexical rules the identifier masker needs, read from sqlglot's tokenizer
    (same source as the parser, so masking can't drift from it): ordinary-string
    backslash escapes, ``/* */`` nesting, and ``$$``/``$tag$`` dollar-quoting. Guards
    the semi-internal attributes so a sqlglot reshape fails loudly here."""
    tokenizer = _SqlglotDialect.get_or_raise(sqlglot_name).tokenizer_class
    nested = getattr(tokenizer, "NESTED_COMMENTS", None)
    heredoc = getattr(tokenizer, "HEREDOC_STRINGS", None)
    raw = getattr(tokenizer, "RAW_STRINGS", None)
    if not isinstance(nested, bool) or not isinstance(
        heredoc, (list, tuple, set, frozenset)
    ) or not isinstance(raw, (list, tuple, set, frozenset)):
        raise RuntimeError(
            f"Cannot derive the masking lexis for sqlglot dialect {sqlglot_name!r}: "
            f"tokenizer NESTED_COMMENTS/HEREDOC_STRINGS/RAW_STRINGS shape changed. A "
            f"sqlglot upgrade may have changed these internal APIs."
        )
    return SqlLexis(
        backslash_escapes=_sqlglot_backslash_escapes(sqlglot_name),
        nested_comments=nested,
        dollar_quotes="$" in heredoc or any("$" in str(r) for r in raw),
    )


def _digest(secret: str | None) -> str:
    """Non-reversible id for secret material in cache keys. 16 hex chars keeps
    it log-readable; collisions are negligible at this scale."""
    if not secret:
        return ""
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()[:16]


class SqlDialect(BaseModel):
    """One database's quirks; the base IS the Postgres-shaped default, subclasses override what differs."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    sqlglot_name: str = "postgres"
    ds_type_aliases: frozenset[str] = frozenset()
    explain_prefix: str | None = "EXPLAIN"
    explain_postfix: str = ""
    log10_native: bool = True
    log2_native: bool = True

    # Whether NUMERIC/DECIMAL columns are stored and aggregated exactly.
    # False (SQLite's numeric affinity) keeps the inferred cast instead of
    # native-type preservation.
    exact_decimal_native: bool = True

    # Conservative universal identifier budget in BYTES; ``None`` = unbounded
    # (fitting hooks become no-ops). Default is the tightest Tier-1 value
    # (Postgres), so a new dialect over-shortens rather than silently truncating.
    max_identifier_bytes: int | None = 63

    # Approximate distinct: exact COUNT(DISTINCT) unless native (sqlglot's
    # ApproxDistinct); Oracle/T-SQL name the call, as sqlglot mis-spells theirs.
    approx_count_distinct_native: bool = False
    approx_count_distinct_anonymous_name: str | None = None

    # A rejected ``statement_timeout_sql`` is rolled back and reported instead of raised.
    statement_timeout_best_effort: bool = False

    @property
    def backslash_escapes_strings(self) -> bool:
        """Whether string literals treat backslash as an escape; feeds Mode-A ``{var}`` escaping."""
        return _sqlglot_backslash_escapes(self.sqlglot_name)

    @property
    def identifier_masking_lexis(self) -> SqlLexis:
        """Lexical rules for the identifier masker (ordinary-string backslash
        escapes, ``/* */`` nesting, dollar-quoting, and this dialect's identifier
        quote pair), so masking of user literals/comments matches this dialect's
        grammar rather than over/under-masking a Postgres-shaped default. Derived
        from sqlglot's tokenizer plus the emitter's own identifier quote."""
        return _sqlglot_masking_lexis(self.sqlglot_name).model_copy(
            update={"identifier_quote": self._identifier_quote_anchors()},
        )

    # ------------------------------------------------------------------
    # Null-safe equality
    # ------------------------------------------------------------------

    def declared_cast_type(self, dt: Optional[DataType]) -> Optional[DataType]:
        """The declared/inferred CAST target for a value of type ``dt`` on this dialect (``None`` skips the cast). Default: unchanged; a dialect without native temporal storage overrides to drop DATE / TIMESTAMP casts (P2)."""
        return dt

    def build_null_safe_eq(
        self, left: Expression, right: Expression,
    ) -> Expression:
        """Null-safe equality for grain join-backs (``IS NOT DISTINCT FROM``; sqlglot transpiles
        it, incl. MySQL ``<=>``); dialects without a native form override."""
        return exp.NullSafeEQ(this=left, expression=right)

    # ------------------------------------------------------------------
    # ORDER BY term construction
    # ------------------------------------------------------------------

    def build_ordered(
        self,
        order_col: Expression,
        *,
        descending: bool,
        nulls: Literal["default", "first", "last"] = "default",
    ) -> exp.Ordered:
        """The one builder of ``ORDER BY`` terms. ``"default"`` renders nulls last on every
        dialect (emulated where there's no syntax); ``"first"``/``"last"`` are honoured as asked.
        T-SQL overrides: its emulation re-resolves the alias against FROM and fails."""
        kwargs: dict = {"this": order_col, "desc": descending}
        if nulls == "first":
            kwargs["nulls_first"] = True
        elif nulls == "last":
            kwargs["nulls_first"] = False
        return exp.Ordered(**kwargs)

    def native_nulls_first(self, *, descending: bool) -> bool:
        """Where NULLs natively sort for ``descending`` — setting it yields a bare ``ORDER BY``
        (wanted inside window frames); read from sqlglot's emitter so it can't disagree."""
        ordering = getattr(
            _SqlglotDialect.get_or_raise(self.sqlglot_name),
            "NULL_ORDERING", None,
        )
        if ordering == "nulls_are_last":
            return False
        if ordering == "nulls_are_small":
            return not descending
        if ordering == "nulls_are_large":
            return descending
        raise RuntimeError(
            f"Cannot derive the native null ordering for sqlglot dialect "
            f"{self.sqlglot_name!r}: NULL_ORDERING is {ordering!r}. A sqlglot "
            f"upgrade may have changed this API.",
        )

    @staticmethod
    def _expanded_null_safe_eq(
        left: Expression, right: Expression,
    ) -> Expression:
        """``left = right OR (left IS NULL AND right IS NULL)`` — the portable
        expansion for dialects without a native null-safe equality operator."""
        eq = exp.EQ(this=left.copy(), expression=right.copy())
        both_null = exp.And(
            this=exp.Is(this=left.copy(), expression=exp.Null()),
            expression=exp.Is(this=right.copy(), expression=exp.Null()),
        )
        return exp.paren(exp.Or(this=eq, expression=exp.paren(both_null)))

    # ------------------------------------------------------------------
    # Date-trunc / time arithmetic
    # ------------------------------------------------------------------

    def build_date_trunc(
        self,
        col_expr: Expression,
        granularity: TimeGranularity,
    ) -> Expression:
        """Default: ``DATE_TRUNC('unit', col)`` via sqlglot's ``exp.DateTrunc``.

        Non-bare-column / non-cast operands are wrapped in
        ``CAST(... AS TIMESTAMP)`` so Postgres can pick the right
        ``date_trunc`` overload — preserving today's
        ``generator.py:_build_date_trunc`` behaviour.
        """
        if granularity == TimeGranularity.WEEK_SUNDAY:
            # Sunday-anchored week = Monday-week of (col + 1 day),
            # shifted back 1 day. This is Metabase's own reference formula and
            # reuses each dialect's existing (Monday-based) WEEK truncation, so
            # WEEK_SUNDAY's correctness tracks WEEK's per dialect. BigQuery —
            # whose native WEEK is Sunday — overrides this to emit
            # ``DATE_TRUNC(col, WEEK(SUNDAY))`` directly.
            shifted = self.build_time_offset_expr(
                col_expr=col_expr, offset=1, granularity=TimeGranularity.DAY,
            )
            monday = self.build_date_trunc(
                col_expr=shifted, granularity=TimeGranularity.WEEK,
            )
            return self.build_time_offset_expr(
                col_expr=monday, offset=-1, granularity=TimeGranularity.DAY,
            )
        gran_str = _GRANULARITY_TO_DATE_TRUNC.get(granularity, granularity.value)
        if not isinstance(col_expr, (exp.Column, exp.Cast)):
            col_expr = exp.Cast(this=col_expr, to=exp.DataType.build("TIMESTAMP"))
        return exp.DateTrunc(this=col_expr, unit=exp.Literal.string(gran_str))

    def build_time_offset_expr(
        self,
        col_expr: Expression,
        offset: int,
        granularity: TimeGranularity | TimeUnit,
    ) -> Expression:
        """Default: ``col ± INTERVAL N UNIT`` via ``exp.Add`` / ``exp.Sub``.

        Granularity normalization (preserved across every dialect):
        ``quarter`` becomes ``val * 3`` of ``MONTH``. SQLite additionally
        normalises ``week`` to ``val * 7`` of ``days`` — that branch lives
        on ``SqliteDialect`` since other dialects accept ``WEEK`` natively.
        """
        granularity = TimeGranularity(granularity)
        unit = _granularity_to_unit(granularity.value)
        val = offset * 3 if granularity == TimeGranularity.QUARTER else offset
        if val >= 0:
            return exp.Add(
                this=col_expr,
                expression=exp.Interval(
                    this=exp.Literal.number(val),
                    unit=exp.Var(this=unit),
                ),
            )
        return exp.Sub(
            this=col_expr,
            expression=exp.Interval(
                this=exp.Literal.number(-val),
                unit=exp.Var(this=unit),
            ),
        )

    def duration_interval_exprs(
        self,
        parts: list[tuple[int, str]],
        sign: int = 1,
    ) -> list[Expression]:
        """Default: one ``exp.Interval`` per (amount, unit) pair.

        The Add-vs-Sub direction is decided by ``add_intervals_expr`` from
        its own ``sign`` arg, so the Interval values themselves stay
        positive at this layer. sqlglot transpiles each single-unit
        interval per dialect (MySQL/ClickHouse/BigQuery all accept
        ``INTERVAL N UNIT``).
        """
        return [
            exp.Interval(
                this=exp.Literal.number(amount),
                unit=exp.Var(this=_WINDOW_UNIT_SQL[unit].upper()),
            )
            for amount, unit in parts
        ]

    def add_intervals_expr(
        self,
        expr: Expression,
        intervals: list[Expression],
        sign: int = 1,
    ) -> Expression:
        """Default: fold ``exp.Add`` (sign>=0) or ``exp.Sub`` (sign<0) over
        the interval list."""
        op_cls = exp.Add if sign >= 0 else exp.Sub
        result = expr
        for iv in intervals:
            result = op_cls(this=result, expression=iv)
        return result

    def frame_time_operand(self, expr: Expression) -> Expression:
        """The source time column as it must appear in a trailing-window frame
        comparison. Default: unchanged — the frame bounds (``add_intervals_expr``)
        carry the same time type, so ``expr < bucket_end`` is already exact."""
        return expr

    # ------------------------------------------------------------------
    # Median / percentile / stat aggregates
    # ------------------------------------------------------------------

    def build_median(self, inner: Expression) -> Expression:
        """Default: ``PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY inner)``."""
        return self.build_percentile(p=exp.Literal.number("0.5"), col_expr=inner)

    def build_percentile(
        self, p: Expression, col_expr: Expression,
    ) -> Expression:
        """Default: ``PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY col)``; ``p`` is a validated literal."""
        return exp.WithinGroup(
            this=exp.PercentileCont(this=p.copy()),
            expression=exp.Order(expressions=[exp.Ordered(
                this=col_expr.copy(),
                nulls_first=self.native_nulls_first(descending=False),
            )]),
        )

    def build_approx_count_distinct(self, col_expr: Expression) -> Expression:
        """Approximate distinct; the exact ``COUNT(DISTINCT col)`` unless the dialect has a native one."""
        if self.approx_count_distinct_anonymous_name is not None:
            return exp.Anonymous(
                this=self.approx_count_distinct_anonymous_name,
                expressions=[col_expr.copy()],
            )
        if self.approx_count_distinct_native:
            return exp.ApproxDistinct(this=col_expr.copy())
        return exp.Count(this=exp.Distinct(expressions=[col_expr.copy()]))

    def build_stat_agg_1arg(
        self, agg_name: StatAgg1Name, col_expr: Expression,
    ) -> Expression:
        """Default: emit the canonical name; sqlglot transpiles per dialect."""
        return self._named_call(agg_name, col_expr)

    def build_covar_2arg(
        self,
        agg_name: StatAgg2Name,
        col_expr: Expression,
        other_expr: Expression,
    ) -> Expression:
        """Default: native ``CORR(x, y)`` / ``COVAR_SAMP(x, y)`` / ``COVAR_POP(x, y)``."""
        return self._named_call(agg_name, col_expr, other_expr)

    def _named_call(
        self, agg_name: StatAgg1Name | StatAgg2Name, *args: Expression,
    ) -> Expression:
        """``AGG_NAME(args)`` as this dialect's parser would build it, spelling kept."""
        name = agg_name.upper()
        node = exp.func(name, *(a.copy() for a in args), dialect=self.sqlglot_name)
        assert isinstance(node, Expression)  # concrete Func classes and Anonymous all are
        node.meta["name"] = name
        return node

    # ------------------------------------------------------------------
    # Log-alias rewrite
    # ------------------------------------------------------------------

    def should_use_native_log(self, base: int) -> bool:
        """Whether ``log{N}(x)`` should be emitted as the dialect's native
        single-arg function (vs the canonical 2-arg ``LOG(N, x)``).

        Defaults: log10 native = True (every Tier-1+2 dialect except
        Oracle), log2 native = True (Postgres-shaped baseline). Concrete
        dialects override via the ``log10_native`` / ``log2_native``
        fields.
        """
        if base == 10:
            return self.log10_native
        if base == 2:
            return self.log2_native
        return False

    # ------------------------------------------------------------------
    # AST rewrite hook + per-connection UDF registration
    # ------------------------------------------------------------------

    def rewrite_parsed_ast(self, tree: Expression) -> Expression:
        """Default: identity. SQLite overrides to rewrite JSONExtract to
        the function-call form."""
        return tree

    def rewrite_target_ast(self, tree: Expression) -> Expression:
        """Default: identity. Target-keyed AST rewrite.

        Applied in ``SQLGenerator._parse`` using the generator's **target**
        dialect (``self._dialect``), independent of the parse dialect. This is
        the place for output-shaping a dialect needs that the input-side
        ``rewrite_parsed_ast`` cannot do: formula/measure expressions are
        canonically parsed as Postgres regardless of target, so a
        ``rewrite_parsed_ast`` override would fire for every backend.

        ``PostgresDialect`` overrides this to wrap the first argument of a
        2-arg ``ROUND`` in a numeric ``CAST`` (Postgres has no
        ``round(double precision, integer)`` — only ``round(numeric, int)``).
        SQLite / DuckDB round ``DOUBLE`` natively, so they keep the identity.
        """
        return tree

    def apply_pagination(
        self,
        select: exp.Select,
        *,
        limit: Optional[int],
        offset: Optional[int],
    ) -> exp.Select:
        """Apply LIMIT/OFFSET to a completed ``SELECT`` (P-H).

        The single place pagination is expressed. Every render path routes
        here, so a dialect that spells pagination differently is handled once
        rather than per path — the cross-model combined statement used to append
        raw ``LIMIT``/``OFFSET`` text and emitted literal ``LIMIT`` on SQL
        Server, while the same query carrying a transform layer went through the
        outer wrap and came out correct.

        Setting the bounds on the ``Select`` is what makes transposition work:
        sqlglot rewrites them per dialect only when generating the wrapping
        SELECT, never from a free-standing ``Limit`` node.
        """
        out = select
        if limit is not None:
            out = out.limit(limit)
        if offset is not None:
            out = out.offset(offset)
        return out

    # Identifier-length fitting. Aliases stay canonical inside SLayer,
    # fitted only on emission and restored on the result keys.

    def quote_identifier(self, name: str) -> str:
        """``name`` wrapped in this dialect's identifier quotes."""
        return exp.Identifier(this=name, quoted=True).sql(dialect=self.sqlglot_name)

    def fit_alias(self, name: str) -> str:
        """Length-only fitting; identity when ``name`` already fits.

        Drives the write pass (not ``emit_alias``), so an under-limit alias
        produces byte-identical SQL even on dialects that mangle dots.
        """
        return fit_identifier(name=name, limit=self.max_identifier_bytes)

    def emit_alias(self, alias: str) -> str:
        """The final identifier a canonical alias reaches the SQL as.

        Equals ``fit_alias`` here; BigQuery/T-SQL compose dot-mangling on top.
        Used to build the read-side map, so must match the emitted token exactly.
        """
        return self.fit_alias(alias)

    def alias_rewrite_map(self, aliases: Sequence[str]) -> dict[str, str]:
        """``{canonical: fitted}`` for the write pass, only where they differ.

        The collision check covers every alias including identities: a short
        alias equal to another's fitted form is just as much a duplicate.
        """
        if self.max_identifier_bytes is None:
            return {}
        allocation: dict[str, str] = {}
        owner: dict[str, str] = {}
        for alias in aliases:
            if alias in allocation:
                continue
            fitted = self.fit_alias(alias)
            prior = owner.get(fitted)
            if prior is not None and prior != alias:
                raise IdentifierCollisionError(
                    first=prior, second=alias, emitted=fitted,
                    dialect=self.sqlglot_name, limit=self.max_identifier_bytes,
                    namespace="projection alias",
                )
            owner[fitted] = alias
            allocation[alias] = fitted
        return {k: v for k, v in allocation.items() if k != v}

    def decode_alias_map(self, aliases: Sequence[str]) -> dict[str, str]:
        """``{emitted: canonical}`` — read-side inverse, rebuilt by re-running
        the pure fitting rather than threading a map through generation.

        Raises on two canonical aliases fitting to one emitted form, for
        symmetry with ``alias_rewrite_map`` — the read side can be handed a
        different alias set than the write side, so its guard is independent.
        Ownership is recorded for identity aliases too (only the output map
        drops them), so a fitted alias colliding with an unchanged one is
        caught just as ``alias_rewrite_map`` catches it.
        """
        out: dict[str, str] = {}
        owner: dict[str, str] = {}
        for alias in aliases:
            emitted = self.emit_alias(alias)
            prior = owner.get(emitted)
            if prior is not None and prior != alias:
                raise IdentifierCollisionError(
                    first=prior, second=alias, emitted=emitted,
                    dialect=self.sqlglot_name, limit=self.max_identifier_bytes,
                    namespace="result key",
                )
            owner[emitted] = alias
            if emitted != alias:
                out[emitted] = alias
        return out

    def _rekey_row(
        self,
        row: dict[str, Any],
        mapping: dict[str, str],
        *,
        fallback: Callable[[str], str] | None = None,
    ) -> dict[str, Any]:
        """Apply ``mapping`` to a row's keys, erroring if two keys collapse onto
        one (silent column loss).

        ``fallback`` decodes keys absent from ``mapping`` (BigQuery/T-SQL
        ``___`` -> ``.``). Applied here, not upstream, so the collapse check
        sees every key.
        """
        out: dict[str, Any] = {}
        for key, value in row.items():
            if key in mapping:
                decoded = mapping[key]
            elif fallback is not None:
                decoded = fallback(key)
            else:
                decoded = key
            if decoded in out:
                raise IdentifierCollisionError(
                    first=key, second=decoded, emitted=decoded,
                    dialect=self.sqlglot_name, limit=self.max_identifier_bytes,
                    namespace="result key",
                )
            out[decoded] = value
        return out

    def _identifier_quote_anchors(self) -> tuple[str, str]:
        """This dialect's identifier open/close quote chars (``"``/``"``,
        `` ` ``/`` ` ``, ``[``/``]``), read off ``quote_identifier`` so the scan
        can't drift from the emitter."""
        probe = self.quote_identifier("x")
        return probe[0], probe[-1]

    def _emission_fit_map(
        self, *, sql: str, aliases: Sequence[str], exempt: frozenset[str],
    ) -> dict[str, str]:
        """``{canonical: fitted}`` for every over-limit SLayer-minted identifier —
        the plan-derived projection ``aliases`` plus internal CTE columns scanned
        off the assembled SQL. Exempt names pass through unfitted (they win on a
        spelling tie). Fails closed on a fitted-form collision."""
        limit = self.max_identifier_bytes
        if limit is None:
            return {}
        quote_open, quote_close = self._identifier_quote_anchors()
        present = quoted_identifiers(
            sql, quote_open=quote_open, quote_close=quote_close,
            lexis=self.identifier_masking_lexis,
        )
        # ``fit_alias`` sizes against the post-mangle form (BigQuery/T-SQL dot
        # expansion), so this also catches a dotted alias under the raw limit that
        # mangling would push over.
        scanned = sorted(n for n in present if self.fit_alias(n) != n)
        allocation: dict[str, str] = {}
        owner: dict[str, str] = {}
        for name in (*aliases, *scanned):
            if name in exempt or name in allocation:
                continue
            fitted = self.fit_alias(name)
            prior = owner.get(fitted)
            if prior is not None and prior != name:
                raise IdentifierCollisionError(
                    first=prior, second=name, emitted=fitted,
                    dialect=self.sqlglot_name, limit=limit,
                )
            owner[fitted] = name
            allocation[name] = fitted
        for name, fitted in allocation.items():
            if fitted != name and fitted in present and fitted not in allocation:
                raise IdentifierCollisionError(
                    first=name, second=fitted, emitted=fitted,
                    dialect=self.sqlglot_name, limit=limit,
                )
        return {k: v for k, v in allocation.items() if k != v}

    def rewrite_emitted_sql(
        self, sql: str, *, aliases: Sequence[str] = (), exempt: frozenset[str] = frozenset(),
    ) -> str:
        """Post-pass string rewrite of the final SQL; write-side companion to
        ``rewrite_parsed_ast``, applied at the end of ``generate()``.

        Fits every over-limit SLayer-minted identifier — the plan-derived
        projection ``aliases`` and internal CTE columns scanned off the assembled
        SQL — replacing each canonical token everywhere it occurs (literals/comments
        excepted). ``exempt`` names user-authored
        identifiers that pass through byte-identical. Under-limit SQL is
        byte-identical; unbounded dialects are a no-op. BigQuery/T-SQL compose
        dot-mangling after this pass.
        """
        mapping = self._emission_fit_map(sql=sql, aliases=aliases, exempt=exempt)
        if not mapping:
            return sql
        return substitute_quoted(
            sql=sql, mapping=mapping, quote=self.quote_identifier,
            lexis=self.identifier_masking_lexis,
        )

    def assert_no_overlimit_identifiers(
        self, sql: str, *, exempt: frozenset[str] = frozenset(),
    ) -> None:
        """Always-on emission backstop: raise if the final SQL still carries a
        non-exempt over-limit identifier — an unaccounted name the database would
        truncate silently (sql principle 9)."""
        limit = self.max_identifier_bytes
        if limit is None:
            return
        survivors = [
            t for t in overlimit_tokens(
                sql, limit=limit, quote_styles=[self._identifier_quote_anchors()],
                lexis=self.identifier_masking_lexis,
            )
            if t not in exempt
        ]
        if survivors:
            raise IdentifierLengthError(
                tokens=survivors, dialect=self.sqlglot_name, limit=limit,
            )

    def decode_result_keys(
        self,
        rows: list[dict[str, Any]],
        *,
        aliases: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        """Reverse the write-side rewrite on result-row keys, so consumers always
        see SLayer's canonical alias shape regardless of dialect or shortening.

        BigQuery/T-SQL additionally decode the ``___`` mangling back to dots.
        """
        mapping = self.decode_alias_map(aliases)
        if not mapping:
            return rows
        return [self._rekey_row(row=row, mapping=mapping) for row in rows]

    def register_udfs(self, dbapi_connection) -> None:
        """Default: no-op. SQLite overrides to register Python aggregate
        / scalar UDFs on every fresh connection."""
        return None

    # ------------------------------------------------------------------
    # EXPLAIN
    # ------------------------------------------------------------------

    def build_explain_sql(self, sql: str) -> str:
        """Wrap ``sql`` in the dialect's EXPLAIN prefix/postfix pair.

        Raises ``ValueError`` when ``explain_prefix`` is ``None``
        (BigQuery — EXPLAIN unsupported). Preserves today's
        ``query_engine.py:_build_explain_sql`` semantics.
        """
        if self.explain_prefix is None:
            raise ValueError(
                f"EXPLAIN is not supported for dialect '{self.sqlglot_name}'. "
                "Use dry_run=True to inspect the generated SQL instead."
            )
        return f"{self.explain_prefix} {sql}{self.explain_postfix}"

    # ------------------------------------------------------------------
    # Runtime hooks: keep dialect conditionals out of engine_factory / client (no-op defaults).
    # ------------------------------------------------------------------

    def build_connection_url(
        self,
        datasource: "DatasourceConfig",
    ) -> str | None:
        """Hook: connection string, or ``None`` to use ``DatasourceConfig.get_connection_string()``."""
        return None

    def build_engine(
        self,
        datasource: "DatasourceConfig",
        *,
        connection_string: str,
    ) -> "sa.Engine | None":
        """Hook: a dialect-built engine, or ``None`` for ``engine_factory``'s default ``create_engine``."""
        return None

    # ------------------------------------------------------------------
    # Credential identity (engine-cache safety)
    # ------------------------------------------------------------------

    def credential_fingerprint(self, datasource: "DatasourceConfig") -> str:
        """Opaque identity of the credentials this datasource authenticates with.

        Part of the engine cache key: a dialect whose secret is *not* in the
        connection string MUST override this, or callers with different
        credentials share one engine. Return a digest, never raw secret —
        keys reach logs.
        """
        return _digest(datasource.credentials_json)

    def apply_session_overrides(
        self,
        dbapi_connection: Any,
        datasource: "DatasourceConfig",
    ) -> None:
        """Hook: session setup on every new pooled connection (e.g. ``USE WAREHOUSE``)."""
        return None

    def statement_timeout_sql(self, timeout_seconds: int) -> str | None:
        """Hook: statement that sets the timeout before the query, or ``None``."""
        return None

    def set_connection_timeout(self, dbapi_connection: Any, timeout_seconds: int) -> object:
        """Hook: put the timeout on the DBAPI connection; returns the prior state for restore."""
        return None

    def restore_connection_timeout(self, dbapi_connection: Any, prior: object) -> None:
        """Hook: undo ``set_connection_timeout`` with the state it returned."""
        return None

    def timeout_permission_sql(self) -> str | None:
        """Hook: query whose scalar says whether this user may set the timeout, or ``None``."""
        return None

    def timeout_permitted(self, value: Any) -> bool:
        """Hook: interpret the ``timeout_permission_sql`` scalar."""
        return True

    def map_cursor_type_code(self, type_code: int) -> str | None:
        """Hook: cursor type code → SLayer category, or ``None`` for the Postgres OID map."""
        return None


class DottedAliasManglingMixin:
    """Shared ``.``-to-``___`` alias mangling for BigQuery / T-SQL.

    ``fit_alias`` / ``emit_alias`` / ``decode_result_keys`` are identical on both;
    only ``rewrite_emitted_sql``'s identifier-quote anchor differs, supplied via
    the three class attributes. Mixed in before ``SqlDialect`` so the base LENGTH
    pass runs first (``super().rewrite_emitted_sql``) and the dot-mangle composes
    on its still-dotted output.
    """

    dotted_alias_re: ClassVar[re.Pattern[str]]
    alias_quote_open: ClassVar[str]
    alias_quote_close: ClassVar[str]

    def fit_alias(self, name: str) -> str:
        """Size the budget against the post-mangle form (``.`` -> ``___`` adds 2
        bytes per dot); the return value stays dotted for the regex."""
        return fit_identifier(
            name=name, limit=self.max_identifier_bytes, expand=encode_alias,
        )

    def emit_alias(self, alias: str) -> str:
        """The final identifier: length-fitted, then dot-mangled."""
        return encode_alias(self.fit_alias(alias))

    def rewrite_emitted_sql(
        self, sql: str, *, aliases: Sequence[str] = (), exempt: frozenset[str] = frozenset(),
    ) -> str:
        """Base LENGTH pass, then ``.`` -> ``___`` inside quoted identifiers."""
        sql = super().rewrite_emitted_sql(sql=sql, aliases=aliases, exempt=exempt)
        return self.dotted_alias_re.sub(
            lambda m: (
                f"{self.alias_quote_open}{encode_alias(m.group(1))}"
                f"{self.alias_quote_close}"
            ),
            sql,
        )

    def decode_result_keys(
        self,
        rows: list[dict[str, Any]],
        *,
        aliases: Sequence[str] = (),
    ) -> list[dict[str, Any]]:
        """Reverse the mangling on result-row keys: consult the emitted->canonical
        map, falling back to the ``___`` -> ``.`` bijection for fitted keys."""
        mapping = self.decode_alias_map(aliases)
        return [
            self._rekey_row(row=row, mapping=mapping, fallback=decode_alias)
            for row in rows
        ]
