"""Mode-B Python-AST parser (DEV-1450).

``parse_expr(text) -> ParsedExpr`` lowers a Mode-B DSL string
(``ModelMeasure.formula``, ``SlayerQuery.measures`` / ``.filters``) to a typed
tree — pure syntax, no scope resolution or named-measure expansion (binder's
job). Grammar: bare/dotted refs; colon or functional aggregations, which
collapse to one ``AggCall`` (DEV-1826); transform calls; a closed scalar
allowlist; arithmetic / comparison / boolean / unary; grouping. Rejects
non-allowlisted calls, raw ``OVER(...)``, and chained comparisons.
"""

from __future__ import annotations

import ast
import re
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import BUILTIN_AGGREGATIONS, normalize_aggregation_name
from slayer.core.errors import IllegalWindowInFilterError, UnknownFunctionError
from slayer.core.formula import ALL_TRANSFORMS
from slayer.core.keys import SCALAR_FUNCTIONS
from slayer.core.refs import split_agg_suffix


# ---------------------------------------------------------------------------
# ParsedExpr family
# ---------------------------------------------------------------------------


class _BaseNode(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class Ref(_BaseNode):
    name: str


class DottedRef(_BaseNode):
    parts: Tuple[str, ...]


class StarSource(_BaseNode):
    pass


class Literal(_BaseNode):
    value: Union[Decimal, str, bool, None] = None


class TupleLit(_BaseNode):
    """Literal-only tuple/list RHS for ``IN`` / ``NOT IN`` (DEV-1475); non-literal
    elements and empty tuples are rejected at parse time."""

    elements: Tuple[Literal, ...]


class AggCall(_BaseNode):
    # source may also be an aggregation-free scalar expression (``sum(a - b)``).
    source: Union[
        Ref, DottedRef, StarSource, Literal, "ScalarCall", "Arith", "UnaryOp",
    ]
    agg: str
    args: Tuple[Any, ...] = ()
    kwargs: Tuple[Tuple[str, Any], ...] = ()


class TransformCall(_BaseNode):
    op: str
    input: Any
    args: Tuple[Any, ...] = ()
    kwargs: Tuple[Tuple[str, Any], ...] = ()


class ScalarCall(_BaseNode):
    name: str
    args: Tuple[Any, ...] = ()


class Arith(_BaseNode):
    op: str
    left: Any
    right: Any


class UnaryOp(_BaseNode):
    op: str
    operand: Any


class Cmp(_BaseNode):
    op: str
    left: Any
    right: Any


class BoolOp(_BaseNode):
    op: str
    operands: Tuple[Any, ...]


ParsedExpr = Union[
    Ref, DottedRef, StarSource, Literal, TupleLit,
    AggCall, TransformCall, ScalarCall,
    Arith, UnaryOp, Cmp, BoolOp,
]

# ``AggCall.source`` forward-references ScalarCall / Arith / UnaryOp (defined
# below it) for expression aggregation sources.
AggCall.model_rebuild()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


# The ``__slayer_`` prefix is reserved from user input (P3). Matched only at an
# identifier boundary so a legal embedded name (``foo__slayer_bar``) stays
# referenceable; ``\w`` is Unicode-aware, covering ``é__slayer_bar`` too.
_RESERVED_EXPR_PREFIX = "__slayer_"
_RESERVED_EXPR_PREFIX_RE = re.compile(r"(?<!\w)__slayer_")
_PLACEHOLDER_PREFIX = "__slayer_agg_"
_PLACEHOLDER_RE = re.compile(rf"^{_PLACEHOLDER_PREFIX}(\d+)__$")
# ``_preprocess_star_args`` mints this for a call's first-arg ``*`` (``count(*)``);
# Python's AST cannot parse a bare ``*``.
_STAR_ARG_TOKEN = "__slayer_star__"
_OVER_RE = re.compile(r"\b[oO][vV][eE][rR]\s*\(")
# Escape-aware Python-string matcher — blanks literals before keyword scans so
# an ``OVER(`` / ``:sum`` inside a quoted value isn't mistaken for syntax.
_PY_STRING_LITERAL_RE = re.compile(r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"")
# SQL ``[NOT] LIKE`` → the ``like(col, pattern)`` scalar (DEV-1704). LHS bare/
# dotted ident or one scalar call; RHS a single-quoted (escape-aware) pattern.
# Keyword as ASCII classes, not ``re.IGNORECASE`` — IGNORECASE folds spoofs like
# ``lıke`` (dotless ı) into ``like``.
_SQL_LIKE_RE = re.compile(
    r"\b(\w+\([^()]*\)|(?:\w+\.)*\w+)\s+"
    r"([nN][oO][tT]\s+)?[lL][iI][kK][eE]\s+"
    r"('(?:\\.|[^'\\])*')"
)


def _rewrite_sql_like(text: str) -> str:
    """``col LIKE 'p%'`` → ``like(col, 'p%')``; ``col NOT LIKE 'p%'`` →
    ``not like(col, 'p%')``. Matches starting inside string literals are
    left untouched."""
    spans = [(m.start(), m.end()) for m in _PY_STRING_LITERAL_RE.finditer(text)]

    def _sub(m: "re.Match[str]") -> str:
        # Skip a match inside a string literal, or one whose LHS is fused into a
        # Unicode identifier the ``\b``/``\w`` LHS token splits (``℘name``,
        # decomposed ``éname``) — that name is a reference, not a LIKE operand.
        if any(s <= m.start() < e for s, e in spans) or _is_ident_adjacent(
            text, m.start() - 1
        ):
            return m.group(0)
        lhs, neg, pat = m.group(1), m.group(2), m.group(3)
        call = f"like({lhs}, {pat})"
        return f"not {call}" if neg else call

    return _SQL_LIKE_RE.sub(_sub, text)


# SQL ``CASE WHEN … END`` → nested ``iif(cond, then, otherwise)``. Lowered to the
# host language (a plain call) so binding + per-dialect emission reuse the whole
# scalar-call machinery. Only the WHEN-condition segments are operator-normalised
# (so ``CASE WHEN a = 5 …`` works in measures too); THEN/ELSE values are sliced
# verbatim and only recursed for nested CASE, so a value like ``'a AND b'`` is
# never rewritten. String literals are single tokens, so keywords inside them are
# invisible to the keyword scan. Identifiers lex complete (Unicode-aware start,
# dotted paths as ONE token), so a name containing or qualified by a keyword
# (``customers.case``, ``écase``) can never equal one.
_CASE_TOKEN_RE = re.compile(
    r"(?P<str>'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\")"
    r"|(?P<id>[^\W\d]\w*(?:\s*\.\s*[^\W\d]\w*)*)"
    r"|(?P<lp>[(\[])"
    r"|(?P<rp>[)\]])",
    re.DOTALL,
)
_CASE_STOPS_OPERAND = frozenset({"WHEN", "THEN", "ELSE", "END"})
_CASE_KEYWORDS = _CASE_STOPS_OPERAND | {"CASE"}


def _is_ident_adjacent(text: str, pos: int) -> bool:
    """Whether ``text[pos]`` is identifier material the ``\\w`` token class
    misses (combining marks, ``Other_ID_Start`` symbols like ``℘``)."""
    return 0 <= pos < len(text) and ("a" + text[pos]).isidentifier()


def _sub_keyword_isolated(pattern: "re.Pattern[str]", repl: str, text: str) -> str:
    """``pattern.sub(repl, text)`` but leaving a keyword match fused into a
    Unicode identifier untouched — combining marks or ``Other_ID_Start`` symbols
    (``℘``) on either side that ``\\b``/``\\w`` cannot see would otherwise
    rewrite ``℘NULL`` → ``℘None`` or ``℘AND`` → ``℘and``, silently rebinding the
    reference (mirrors ``_case_keyword``'s adjacency guard)."""
    def _guard(m: "re.Match[str]") -> str:
        if _is_ident_adjacent(text, m.start() - 1) or _is_ident_adjacent(text, m.end()):
            return m.group(0)
        return repl
    return pattern.sub(_guard, text)


def _case_keyword(text: str, tok: Tuple[Optional[str], str, int, int]) -> Optional[str]:
    """The CASE-grammar keyword a token spells, or None for an identifier.

    ASCII-exact (``caſe`` uppercases to ``CASE`` only via Unicode folding) and
    rejected when adjacent raw text continues an identifier around the token.
    """
    kind, val, start, end = tok
    if kind != "id" or not val.isascii():
        return None
    up = val.upper()
    if up not in _CASE_KEYWORDS:
        return None
    if _is_ident_adjacent(text, start - 1) or _is_ident_adjacent(text, end):
        return None
    return up


def _case_has_when(
    text: str, toks: List[Tuple[Optional[str], str, int, int]], i: int,
) -> bool:
    """Whether the CASE at ``toks[i]`` opens a conditional: a depth-0 ``WHEN``
    follows before any other structural keyword, an unmatched ``)``, or end.

    Accepted mislex edge: a BARE keyword-named simple-CASE operand
    (``CASE case WHEN 1 …``) is genuinely ambiguous and errors loudly —
    parenthesize the operand (``CASE (case) WHEN 1 …``) to disambiguate.
    """
    depth = 0
    for j in range(i + 1, len(toks)):
        kind = toks[j][0]
        if kind == "lp":
            depth += 1
        elif kind == "rp":
            if depth == 0:
                return False
            depth -= 1
        elif kind == "id" and depth == 0:
            kw = _case_keyword(text, toks[j])
            if kw is not None:
                return kw == "WHEN"
    return False


def _rewrite_case_when(text: str) -> str:
    if "case" not in text.lower():
        return text
    toks = [
        (m.lastgroup, m.group(), m.start(), m.end())
        for m in _CASE_TOKEN_RE.finditer(text)
    ]
    if not any(_case_keyword(text, t) == "CASE" for t in toks):
        return text
    result, _ = _rw_value(text, toks, 0, frozenset(), 0)
    return result


def _rw_value(text, toks, i, stop_kws, start_char):  # NOSONAR(S3776) — one cohesive token-scan over a value/condition span: paren-depth tracking, the depth-0 stop-keyword break, and nested-CASE recursion are each one decision, and splitting them scatters the slice-and-recurse contract that preserves the original text.
    """Rewrite one value/condition span → ``(rewritten, next_index)``.

    ``start_char`` is the span start (end of the keyword just consumed): bare
    values (``1``) have no token, so non-CASE content is recovered by slicing the
    original text (preserving colons/dots/spacing). Stops at a depth-0 stop
    keyword or an unmatched ``)``.
    """
    parts: List[str] = []
    depth = 0
    last = start_char
    while i < len(toks):
        kind, _, start, _ = toks[i]
        if kind == "lp":
            depth += 1
        elif kind == "rp":
            if depth == 0:
                break
            depth -= 1
        elif kind == "id":
            kw = _case_keyword(text, toks[i])
            if depth == 0 and kw in stop_kws:
                break
            if kw == "CASE" and _case_has_when(text, toks, i):
                parts.append(text[last:start])
                nested, i = _rw_case(text, toks, i)
                parts.append(nested)
                last = toks[i - 1][3]
                continue
        i += 1
    stop_char = toks[i][2] if i < len(toks) else len(text)
    parts.append(text[last:stop_char])
    return "".join(parts).strip(), i


def _rw_case(text, toks, i):
    """Rewrite a ``CASE … END`` starting at ``toks[i]`` (the CASE keyword)."""
    i += 1  # consume CASE
    operand, i = _rw_value(text, toks, i, _CASE_STOPS_OPERAND, toks[i - 1][3])
    is_simple = bool(operand)
    branches: List[Tuple[str, str]] = []
    while i < len(toks) and _case_keyword(text, toks[i]) == "WHEN":
        i += 1
        cond, i = _rw_value(text, toks, i, frozenset({"THEN"}), toks[i - 1][3])
        if not (i < len(toks) and _case_keyword(text, toks[i]) == "THEN"):
            raise ValueError(
                f"Malformed CASE expression in {text!r}: a WHEN branch is "
                f"missing its THEN."
            )
        i += 1
        then_val, i = _rw_value(
            text, toks, i, frozenset({"WHEN", "ELSE", "END"}), toks[i - 1][3],
        )
        branches.append((cond, then_val))
    else_val = "None"
    if i < len(toks) and _case_keyword(text, toks[i]) == "ELSE":
        i += 1
        else_val, i = _rw_value(text, toks, i, frozenset({"END"}), toks[i - 1][3])
        else_val = else_val or "None"
    if not (i < len(toks) and _case_keyword(text, toks[i]) == "END"):
        raise ValueError(
            f"Malformed CASE expression in {text!r}: missing END."
        )
    i += 1  # consume END
    if not branches:
        raise ValueError(
            f"Malformed CASE expression in {text!r}: needs at least one "
            f"WHEN … THEN branch."
        )
    result = else_val
    for cond, then_val in reversed(branches):
        if is_simple:
            cond_expr = f"{operand} == {cond}"
        else:
            cond_expr = _normalize_sql_filter_operators(cond)
        result = f"iif({cond_expr}, {then_val}, {result})"
    return result, i


_COLON_AGG_RE = re.compile(
    r"(\*|[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*(?:\.\*)?)"  # source: * / ident / dotted
    r":"
    r"([a-zA-Z_]\w*)"
    # No (args) consumption — Python's AST handles that.
)

_FILTER_KEYWORDS = frozenset({"and", "or", "not", "in", "is"})
_SCAN_TOKEN_RE = re.compile(
    r"(?P<ws>\s+)"
    r"|(?P<string>'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\")"
    r"|(?P<ident>[A-Za-z_]\w*)"
    r"|(?P<number>\d+(?:\.\d*)?|\.\d+)"
    r"|(?P<op_eq2>==|<=|>=|!=)"
    r"|(?P<op_eq>=)"
    r"|(?P<lparen>\()"
    r"|(?P<rparen>\))"
    r"|(?P<lbrack>\[)"
    r"|(?P<rbrack>\])"
    r"|(?P<comma>,)"
    r"|(?P<other>.)",
    re.DOTALL,
)


_BIN_OP_MAP: Dict[type, str] = {
    ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/",
    ast.Mod: "%", ast.Pow: "**", ast.FloorDiv: "//",
}
_CMP_OP_MAP: Dict[type, str] = {
    ast.Eq: "==", ast.NotEq: "!=",
    ast.Lt: "<", ast.LtE: "<=",
    ast.Gt: ">", ast.GtE: ">=",
    # SQL ``IS [NOT] NULL`` lowers to ``is [not] None``; rendered back as
    # ``IS [NOT] NULL`` by the SQL generator.
    ast.Is: "is", ast.IsNot: "is not",
    # DEV-1475: ``IN`` / ``NOT IN`` with a literal-tuple RHS (shape enforced in
    # the ``ast.Compare`` branch).
    ast.In: "in", ast.NotIn: "not in",
}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def parse_expr(text: str) -> ParsedExpr:
    """Parse a Mode-B expression string into a ``ParsedExpr``.

    ``__`` in identifiers is legal (DEV-1743); only the ``__slayer_`` prefix is
    reserved. Raises ``ValueError`` (empty/syntax/unsupported node/chained
    comparison/reserved prefix), ``UnknownFunctionError``, or
    ``IllegalWindowInFilterError`` (raw ``OVER(...)``).
    """
    if not text or not text.strip():
        raise ValueError("Empty Mode-B expression.")

    _reject_reserved_expr_token(text)

    # Scan for raw ``OVER(`` after blanking string literals so a quoted value
    # (``status == 'OVER('``) isn't mistaken for window usage. Skip an ``OVER``
    # fused into a Unicode identifier (``℘OVER(x)``) the leading ``\b`` splits.
    blanked = _PY_STRING_LITERAL_RE.sub("", text)
    if any(
        not _is_ident_adjacent(blanked, m.start() - 1)
        for m in _OVER_RE.finditer(blanked)
    ):
        raise IllegalWindowInFilterError(
            filter_expr=text,
            source="raw OVER(...) is not allowed in Mode-B DSL",
            suggestion=(
                "use a transform instead (rank, percent_rank, dense_rank, "
                "ntile, cumsum, lag, lead, time_shift, …)."
            ),
        )

    preprocessed, agg_map = _preprocess_colons(_rewrite_case_when(text))
    # AFTER the colon pass — ``customers.*:count`` must become a colon
    # placeholder first, so the only ``*`` left in call-first-arg position is
    # the functional spelling (``count(*)`` / ``count(customers.*)``).
    preprocessed = _preprocess_star_args(preprocessed)

    try:
        py_ast = ast.parse(preprocessed, mode="eval").body
    except SyntaxError as e:
        raise ValueError(
            f"Invalid Mode-B expression {text!r}: {e}"
        )

    return _convert(py_ast, agg_map=agg_map, original=text)


def parse_filter_expr(text: str) -> ParsedExpr:
    """Parse a Mode-B *filter* string, accepting SQL operator spellings.

    Filters historically accepted SQL-style operators (``=``, ``<>``, ``NULL``,
    and the keyword forms ``AND`` / ``OR`` / ``NOT`` / ``IS`` / ``IN``)
    alongside the Python spellings. This wrapper normalizes those to their
    Python equivalents (string-literal-aware, so quoted contents are
    untouched) and then delegates to :func:`parse_expr`. Measures / order use
    ``parse_expr`` directly — only filters get the SQL-operator leniency.
    """
    return parse_expr(_normalize_sql_filter_operators(text))


# SQL keyword rewrites as explicit ASCII character classes — ``re.IGNORECASE``
# folds Unicode spoofs (``ıs`` → ``is``) into keywords.
_SQL_NULL_RE = re.compile(r"\b[nN][uU][lL][lL]\b")
_SQL_KEYWORD_RES: Tuple[Tuple[re.Pattern, str], ...] = tuple(
    (re.compile(r"\b" + "".join(f"[{c}{c.upper()}]" for c in kw) + r"\b"), kw)
    for kw in ("is", "not", "and", "or", "in")
)


def _normalize_sql_filter_operators(text: str) -> str:
    """Rewrite SQL operator spellings to Python ones outside string literals.

    ``NULL`` → ``None``; ``IS`` / ``NOT`` / ``AND`` / ``OR`` / ``IN`` →
    lowercase; standalone ``=`` → ``==``; ``<>`` → ``!=``; ``col [NOT] LIKE
    'p%'`` → ``[not ]like(col, 'p%')``.
    """
    # LIKE first, on the whole string (its pattern is a quoted literal). The
    # rest run per non-literal part (escape-aware split) so a keyword inside a
    # quoted value isn't rewritten.
    text = _rewrite_sql_like(text)
    parts = _PY_STRING_LITERAL_RE.split(text)
    literals = _PY_STRING_LITERAL_RE.findall(text)
    result: List[str] = []
    for i, part in enumerate(parts):
        part = _sub_keyword_isolated(_SQL_NULL_RE, "None", part)
        for kw_re, kw in _SQL_KEYWORD_RES:
            part = _sub_keyword_isolated(kw_re, kw, part)
        part = part.replace("<>", "!=")
        # SQL ``||`` → Python ``|`` (BitOr), desugared to ``concat`` in
        # ``_convert``; same precedence relative to comparisons.
        part = part.replace("||", "|")
        result.append(part)
        if i < len(literals):
            result.append(literals[i])
    # ``=`` → ``==`` runs last, on the rejoined string, via a paren-aware scanner
    # that leaves kwarg ``=`` inside non-scalar calls alone (DEV-1492).
    return _rewrite_comparison_equals("".join(result))


def _classify_paren(
    hist: List[Tuple[str, str]],
) -> Tuple[bool, Optional[str]]:
    """Classify an open ``(`` as a CALL or GROUPING paren.

    A call paren follows a bare identifier or a ``)`` / ``]``; lowercase keywords
    (``and`` / ``not`` / ``in`` …) do not open one. After a ``:``
    (``revenue:first(...)``) the callee is dropped to ``None`` so
    :func:`_is_kwarg_equals` treats it as an aggregation, not a transform.
    """
    prev_kind = hist[-1][0] if hist else None
    prev_text = hist[-1][1] if hist else ""
    is_call = prev_kind == "NAME" or prev_text in (")", "]")
    callee = hist[-1][1] if (is_call and prev_kind == "NAME") else None
    if callee is not None and len(hist) >= 2 and hist[-2] == ("OTHER", ":"):
        callee = None
    return is_call, callee


def _is_kwarg_equals(
    stack: List[Tuple[bool, Optional[str]]],
    hist: List[Tuple[str, str]],
) -> bool:
    """Whether a lone ``=`` is a Python keyword-argument separator (DEV-1492).

    Scalars never take kwargs. Transforms take the value first, so a kwarg only
    follows a ``,`` (keeping ``consecutive_periods(status = 'paid')`` a
    predicate). Aggregations/unknowns may take a kwarg first, so ``=`` follows
    ``(`` or ``,``.
    """
    top = stack[-1] if stack else None
    if top is None or not top[0]:
        return False
    callee = top[1]
    if callee is not None and callee.lower() in SCALAR_FUNCTIONS:
        return False
    prev_kind = hist[-1][0] if hist else None
    if prev_kind != "NAME":
        return False
    prev_prev_kind = hist[-2][0] if len(hist) >= 2 else None
    if callee is not None and callee in ALL_TRANSFORMS:
        return prev_prev_kind == "COMMA"
    return prev_prev_kind in ("LPAREN", "COMMA")


def _push_hist(hist: List[Tuple[str, str]], kind: str, text: str) -> None:
    """Append ``(kind, text)`` and trim ``hist`` to the last 2 entries."""
    hist.append((kind, text))
    if len(hist) > 2:
        del hist[0]


def _handle_pass_through(
    m: "re.Match[str]",
    out: List[str],
    stack: List[Tuple[bool, Optional[str]]],
    hist: List[Tuple[str, str]],
) -> None:
    """Whitespace and string literals: emit verbatim, don't touch hist."""
    out.append(m.group(0))


def _handle_ident(
    m: "re.Match[str]",
    out: List[str],
    stack: List[Tuple[bool, Optional[str]]],
    hist: List[Tuple[str, str]],
) -> None:
    ident = m.group(0)
    out.append(ident)
    _push_hist(hist, "KW" if ident in _FILTER_KEYWORDS else "NAME", ident)


def _handle_other(
    m: "re.Match[str]",
    out: List[str],
    stack: List[Tuple[bool, Optional[str]]],
    hist: List[Tuple[str, str]],
) -> None:
    """Numbers, compound ops (``==``/``<=``/``>=``/``!=``), brackets, and
    any single char not otherwise classified (``:``, ``.``, ``+``, ``-``,
    ``*``, ``/``, ``%``, ``<``, ``>``, ``!``, ``|``, ``{``, ``}``, ...)."""
    text = m.group(0)
    out.append(text)
    _push_hist(hist, "OTHER", text)


def _handle_comma(
    m: "re.Match[str]",
    out: List[str],
    stack: List[Tuple[bool, Optional[str]]],
    hist: List[Tuple[str, str]],
) -> None:
    out.append(",")
    _push_hist(hist, "COMMA", ",")


def _handle_lparen(
    m: "re.Match[str]",
    out: List[str],
    stack: List[Tuple[bool, Optional[str]]],
    hist: List[Tuple[str, str]],
) -> None:
    stack.append(_classify_paren(hist))
    out.append("(")
    _push_hist(hist, "LPAREN", "(")


def _handle_rparen(
    m: "re.Match[str]",
    out: List[str],
    stack: List[Tuple[bool, Optional[str]]],
    hist: List[Tuple[str, str]],
) -> None:
    if stack:
        stack.pop()
    out.append(")")
    _push_hist(hist, "OTHER", ")")


def _handle_op_eq(
    m: "re.Match[str]",
    out: List[str],
    stack: List[Tuple[bool, Optional[str]]],
    hist: List[Tuple[str, str]],
) -> None:
    out.append("=" if _is_kwarg_equals(stack, hist) else "==")
    _push_hist(hist, "OTHER", "=")


_HANDLERS: Dict[str, Any] = {
    "ws": _handle_pass_through,
    "string": _handle_pass_through,
    "ident": _handle_ident,
    "number": _handle_other,
    "op_eq2": _handle_other,
    "op_eq": _handle_op_eq,
    "lparen": _handle_lparen,
    "rparen": _handle_rparen,
    "lbrack": _handle_other,
    "rbrack": _handle_other,
    "comma": _handle_comma,
    "other": _handle_other,
}


def _rewrite_comparison_equals(text: str) -> str:
    """Rewrite SQL ``=`` to Python ``==`` except where it is a kwarg separator.

    A lone ``=`` is kept (kwarg) inside a non-scalar CALL paren when it follows
    ``ident`` after ``(`` or ``,`` — Python's kwarg grammar (see
    :func:`_is_kwarg_equals`); inside scalars a ``=`` is always the user's
    comparison. Compound ops and string literals pass through untouched.
    Token dispatch is keyed by the scan regex's ``lastgroup``.
    """
    out: List[str] = []
    # Each frame: (is_call, callee) — callee is the ident before ``(``, else None.
    stack: List[Tuple[bool, Optional[str]]] = []
    # Last 2 significant tokens (NAME / KW / LPAREN / COMMA / OTHER; no strings).
    hist: List[Tuple[str, str]] = []
    for m in _SCAN_TOKEN_RE.finditer(text):
        _HANDLERS[m.lastgroup](m, out, stack, hist)
    return "".join(out)


# ---------------------------------------------------------------------------
# Reference walk (best-effort textual extraction)
# ---------------------------------------------------------------------------


def walk_parsed_refs(
    parsed: ParsedExpr,
) -> Iterator[Union[Ref, DottedRef, AggCall]]:
    """Yield the reference-bearing leaves (``Ref`` / ``DottedRef`` / ``AggCall``)
    of a tree — scope-free name extraction for schema-drift / memory tagging.

    Descent (matches the binder's walk): ``AggCall`` yielded whole (args/kwargs
    opaque, so ``weighted_avg(weight=quantity)`` surfaces ``price`` not
    ``quantity``); ``TransformCall`` descends ``input`` only; ``ScalarCall`` /
    arithmetic / comparison / boolean descend operands; literals yield nothing.
    """
    if isinstance(parsed, (Ref, DottedRef, AggCall)):
        yield parsed
        return
    if isinstance(parsed, TransformCall):
        yield from walk_parsed_refs(parsed.input)
        return
    if isinstance(parsed, ScalarCall):
        for a in parsed.args:
            yield from walk_parsed_refs(a)
        return
    if isinstance(parsed, Arith):
        yield from walk_parsed_refs(parsed.left)
        yield from walk_parsed_refs(parsed.right)
        return
    if isinstance(parsed, Cmp):
        yield from walk_parsed_refs(parsed.left)
        yield from walk_parsed_refs(parsed.right)
        return
    if isinstance(parsed, UnaryOp):
        yield from walk_parsed_refs(parsed.operand)
        return
    if isinstance(parsed, BoolOp):
        for op in parsed.operands:
            yield from walk_parsed_refs(op)
        return
    # Literal / StarSource / TupleLit → no references (TupleLit holds only
    # Literals by construction).


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


def _reject_reserved_expr_token(text: str) -> None:
    """Reject the reserved ``__slayer_`` token in RAW Mode-B input (P3).

    Runs BEFORE ``_preprocess_colons`` mints its own ``__slayer_agg_N__``
    placeholders, so a colon-agg like ``revenue:sum`` is unaffected while a
    literal ``__slayer_agg_0__`` spoof is rejected [C3]. String literals are
    blanked first (Python syntax, so escapes count) so quoted data is never
    mistaken for an identifier. Matched at an identifier boundary so a legal
    embedded name like ``foo__slayer_bar`` stays referenceable.
    """
    if _RESERVED_EXPR_PREFIX_RE.search(_PY_STRING_LITERAL_RE.sub("", text)):
        raise ValueError(
            f"Mode-B expression {text!r} uses the reserved "
            f"{_RESERVED_EXPR_PREFIX!r} identifier prefix, which is reserved "
            f"for SLayer-internal placeholders."
        )


def _preprocess_colons(
    text: str,
) -> Tuple[str, Dict[int, Tuple[Union[Ref, DottedRef, StarSource], str]]]:
    """Replace ``<source>:<agg>`` with placeholder identifiers.

    Captures source kind + agg name. Any trailing ``(args)`` is left in
    place so Python's AST parses it naturally as a Call. String literal
    spans are skipped — the literal text is user data, not DSL syntax.
    """
    agg_map: Dict[int, Tuple[Union[Ref, DottedRef, StarSource], str]] = {}
    counter = [0]
    literal_spans = [
        # CR review: use the escape-aware matcher so backslash-escaped
        # quotes don't leak ``:sum`` colon rewrites into the string body.
        (m.start(), m.end()) for m in _PY_STRING_LITERAL_RE.finditer(text)
    ]

    def _in_literal(pos: int) -> bool:
        return any(s <= pos < e for s, e in literal_spans)

    def _replace(match: re.Match) -> str:
        if _in_literal(match.start()):
            return match.group(0)
        source_str = match.group(1)
        agg_name = match.group(2)
        source: Union[Ref, DottedRef, StarSource]
        if source_str == "*":
            source = StarSource()
        elif "." in source_str:
            source = DottedRef(parts=tuple(source_str.split(".")))
        else:
            source = Ref(name=source_str)
        idx = counter[0]
        counter[0] += 1
        agg_map[idx] = (source, agg_name)
        return f"{_PLACEHOLDER_PREFIX}{idx}__"

    return _COLON_AGG_RE.sub(_replace, text), agg_map


def _preprocess_star_args(text: str) -> str:  # NOSONAR(S3776) — one token-scan with two star shapes (bare ``(*``, dotted ``path.*``); the prev/next-significant-token checks ARE the grammar being recognised, and splitting them hides the call-first-argument contract.
    """Replace a call's first-argument ``*`` / ``path.*`` with the reserved
    star token (``count(*)`` → ``count(__slayer_star__)``) so Python's AST can
    parse it. Token-aware, string-literal-safe; multiplication is untouched
    (a multiplying ``*`` never directly follows a call's ``(`` or a ``.``).
    """
    if "*" not in text:
        return text
    toks = [(m.lastgroup, m.group(0)) for m in _SCAN_TOKEN_RE.finditer(text)]
    n = len(toks)

    def _prev(i: int) -> int:
        j = i - 1
        while j >= 0 and toks[j][0] == "ws":
            j -= 1
        return j

    def _next(i: int) -> int:
        j = i + 1
        while j < n and toks[j][0] == "ws":
            j += 1
        return j

    def _is_call_lparen(i: int) -> bool:
        p = _prev(i)
        return p >= 0 and toks[p][0] == "ident"

    def _dotted_star_opens_call(dot_idx: int) -> bool:
        # Walk ``ident(.ident)*`` back from the ``.`` before the star; the
        # path must open a call's first argument (directly after ``ident(``).
        j = dot_idx
        while True:
            q = _prev(j)
            if q < 0 or toks[q][0] != "ident":
                return False
            r = _prev(q)
            if r >= 0 and toks[r] == ("other", "."):
                j = r
                continue
            return r >= 0 and toks[r][0] == "lparen" and _is_call_lparen(r)

    out: List[str] = []
    for i, (kind, val) in enumerate(toks):
        if kind == "other" and val == "*":
            nx = _next(i)
            closes_arg = nx < n and toks[nx][1] in (",", ")")
            p = _prev(i)
            if closes_arg and p >= 0:
                if toks[p][0] == "lparen" and _is_call_lparen(p):
                    out.append(_STAR_ARG_TOKEN)
                    continue
                if toks[p] == ("other", ".") and _dotted_star_opens_call(p):
                    out.append(_STAR_ARG_TOKEN)
                    continue
        out.append(val)
    return "".join(out)


def _convert(node: ast.AST, *, agg_map: Dict, original: str) -> ParsedExpr:  # NOSONAR(S3776) — one-pass dispatch over ast node kinds (Constant/Name/Compare/BinOp/UnaryOp/BoolOp/Call/Attribute…) producing typed ParsedExpr; the branches are flat and short, and splitting hides the exhaustive ast-kind coverage one read scans for. Surfaces ParsedExpr.kind contract directly.
    if isinstance(node, ast.Constant):
        return _convert_constant(node, original=original)

    if isinstance(node, ast.Name):
        m = _PLACEHOLDER_RE.match(node.id)
        if m:
            idx = int(m.group(1))
            source, agg = agg_map[idx]
            return AggCall(source=source, agg=agg)
        if node.id == _STAR_ARG_TOKEN:
            return StarSource()
        # SQL-cased boolean literals (PR #316): Python's ast only treats
        # True/False as constants, so `true`/`FALSE`/... arrive as names.
        # Both are reserved words in every target dialect, never columns.
        if node.id.lower() in ("true", "false"):
            return Literal(value=node.id.lower() == "true")
        return Ref(name=node.id)

    if isinstance(node, ast.Attribute):
        parts = _flatten_attribute(node, original=original)
        # ``count(customers.*)`` — the star pre-pass turned the trailing ``*``
        # into the star token; restore it so the DottedRef matches colon form.
        parts = ["*" if p == _STAR_ARG_TOKEN else p for p in parts]
        return DottedRef(parts=tuple(parts))

    if isinstance(node, ast.Call):
        return _convert_call(node, agg_map=agg_map, original=original)

    if isinstance(node, ast.BinOp):
        op_type = type(node.op)
        if op_type is ast.BitOr:
            # SQL ``||`` (normalized to ``|``) desugars to the ``concat`` scalar.
            return ScalarCall(
                name="concat",
                args=(
                    _convert(node.left, agg_map=agg_map, original=original),
                    _convert(node.right, agg_map=agg_map, original=original),
                ),
            )
        if op_type not in _BIN_OP_MAP:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: unsupported "
                f"binary operator {op_type.__name__}."
            )
        return Arith(
            op=_BIN_OP_MAP[op_type],
            left=_convert(node.left, agg_map=agg_map, original=original),
            right=_convert(node.right, agg_map=agg_map, original=original),
        )

    if isinstance(node, ast.UnaryOp):
        op_type = type(node.op)
        if op_type is ast.USub:
            return UnaryOp(
                op="-",
                operand=_convert(node.operand, agg_map=agg_map, original=original),
            )
        if op_type is ast.UAdd:
            # `+x` is a no-op; collapse to the operand directly.
            return _convert(node.operand, agg_map=agg_map, original=original)
        if op_type is ast.Not:
            return UnaryOp(
                op="not",
                operand=_convert(node.operand, agg_map=agg_map, original=original),
            )
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: unsupported unary "
            f"operator {op_type.__name__}."
        )

    if isinstance(node, ast.Compare):
        if len(node.ops) != 1 or len(node.comparators) != 1:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: chained "
                f"comparisons are not supported. Each Cmp must be a "
                f"single comparison; split (e.g.) `1 < x < 10` into "
                f"`1 < x and x < 10`."
            )
        op_type = type(node.ops[0])
        if op_type not in _CMP_OP_MAP:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: unsupported "
                f"comparison operator {op_type.__name__}."
            )
        # DEV-1475: ``IN`` / ``NOT IN`` carry a literal-only tuple RHS; scalar,
        # empty, and non-literal RHS are rejected (signed numerics admitted).
        if op_type in (ast.In, ast.NotIn):
            rhs_node = node.comparators[0]
            if not isinstance(rhs_node, (ast.Tuple, ast.List)):
                raise ValueError(
                    f"Invalid Mode-B expression {original!r}: the right-"
                    f"hand side of ``in`` / ``not in`` must be a tuple/"
                    f"list literal (e.g. ``status in ('a', 'b')``); got "
                    f"{type(rhs_node).__name__}."
                )
            if not rhs_node.elts:
                raise ValueError(
                    f"Invalid Mode-B expression {original!r}: empty "
                    f"tuple is not allowed on the right-hand side of "
                    f"``in`` / ``not in`` (dialect-dependent SQL); use "
                    f"a non-empty literal tuple."
                )
            elements: List[Literal] = []
            for elt in rhs_node.elts:
                converted = _convert_in_rhs_element(
                    elt, agg_map=agg_map, original=original,
                )
                elements.append(converted)
            return Cmp(
                op=_CMP_OP_MAP[op_type],
                left=_convert(node.left, agg_map=agg_map, original=original),
                right=TupleLit(elements=tuple(elements)),
            )
        return Cmp(
            op=_CMP_OP_MAP[op_type],
            left=_convert(node.left, agg_map=agg_map, original=original),
            right=_convert(node.comparators[0], agg_map=agg_map, original=original),
        )

    if isinstance(node, ast.BoolOp):
        op_str = "and" if isinstance(node.op, ast.And) else "or"
        operands = tuple(
            _convert(v, agg_map=agg_map, original=original) for v in node.values
        )
        return BoolOp(op=op_str, operands=operands)

    if isinstance(node, ast.IfExp):
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: the Python conditional "
            f"'x if cond else y' is not supported. Use SQL "
            f"'CASE WHEN cond THEN x ELSE y END' or iif(cond, x, y)."
        )

    raise ValueError(
        f"Invalid Mode-B expression {original!r}: unsupported AST node "
        f"{type(node).__name__}."
    )


def _convert_in_rhs_element(
    node: ast.AST, *, agg_map: Dict, original: str,
) -> Literal:
    """Convert one element of an ``IN`` / ``NOT IN`` literal-tuple RHS.

    The Python parser emits a negative numeric literal as
    ``UnaryOp(USub, Constant(int|float))`` rather than a bare
    ``Constant`` with a negative value, so a strict ``isinstance(_,
    Literal)`` check against ``_convert``'s output would reject
    ``amount in (-1, -2)``. This helper collapses the sign onto the
    inner numeric before the literal check (Codex review). Boolean and
    string literals are unaffected.
    """
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        inner = node.operand
        if (
            isinstance(inner, ast.Constant)
            and isinstance(inner.value, (int, float))
            and not isinstance(inner.value, bool)
        ):
            signed = -inner.value if isinstance(node.op, ast.USub) else inner.value
            if isinstance(signed, int):
                return Literal(value=Decimal(signed))
            return Literal(value=Decimal(str(signed)))
    converted = _convert(node, agg_map=agg_map, original=original)
    if not isinstance(converted, Literal):
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: every element on "
            f"the right-hand side of ``in`` / ``not in`` must be a "
            f"literal (string, number, or boolean); got "
            f"{type(converted).__name__}."
        )
    return converted


def _convert_constant(node: ast.Constant, *, original: str) -> Literal:
    val = node.value
    if isinstance(val, bool):
        return Literal(value=val)
    if val is None:
        return Literal(value=None)
    if isinstance(val, int):
        return Literal(value=Decimal(val))
    if isinstance(val, float):
        return Literal(value=Decimal(str(val)))
    if isinstance(val, str):
        return Literal(value=val)
    raise ValueError(
        f"Invalid Mode-B expression {original!r}: unsupported literal "
        f"type {type(val).__name__}."
    )


def _flatten_attribute(
    node: ast.Attribute, *, original: str,
) -> List[str]:
    parts: List[str] = [node.attr]
    cur: ast.AST = node.value
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        parts.append(cur.id)
    else:
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: unsupported "
            f"attribute base {type(cur).__name__}."
        )
    return list(reversed(parts))


def _convert_kwarg_value(node: ast.AST, *, agg_map: Dict, original: str):
    """Convert a call keyword-argument value.

    List / tuple values (e.g. ``partition_by=[region, channel]`` for the
    rank family) convert to a tuple of converted elements so the parser
    accepts the documented multi-column transform-kwarg grammar instead of
    raising on the bare ``ast.List`` node; scalar values convert normally.
    """
    if isinstance(node, (ast.List, ast.Tuple)):
        return tuple(
            _convert(e, agg_map=agg_map, original=original) for e in node.elts
        )
    return _convert(node, agg_map=agg_map, original=original)


# The node kinds an ``AggCall.source`` may take (column, star, or an
# aggregation-free scalar expression). Cmp / BoolOp / TupleLit are excluded —
# a predicate is not an aggregatable value.
_AGG_SOURCE_KINDS = (Ref, DottedRef, StarSource, Literal, ScalarCall, Arith, UnaryOp)
# Names that are BOTH a builtin aggregation and a transform; dispatched by
# first-arg shape (aggregated input → transform, else aggregation).
_FIRST_LAST = frozenset({"first", "last"})


def _contains_agg_or_transform(node: Any) -> bool:
    """Whether a parsed subtree contains any AggCall / TransformCall."""
    if isinstance(node, (AggCall, TransformCall)):
        return True
    if isinstance(node, ScalarCall):
        return any(_contains_agg_or_transform(a) for a in node.args)
    if isinstance(node, (Arith, Cmp)):
        return _contains_agg_or_transform(node.left) or _contains_agg_or_transform(
            node.right
        )
    if isinstance(node, UnaryOp):
        return _contains_agg_or_transform(node.operand)
    if isinstance(node, BoolOp):
        return any(_contains_agg_or_transform(o) for o in node.operands)
    return False


def _validated_agg_source(source: Any, *, func_name: str, original: str) -> Any:
    """Validate a functional aggregation's first argument as its source."""
    if _contains_agg_or_transform(source):
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: aggregations and "
            f"transforms cannot be nested inside the expression aggregated "
            f"by {func_name!r}."
        )
    if not isinstance(source, _AGG_SOURCE_KINDS):
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: {func_name!r} cannot "
            f"aggregate a {type(source).__name__}; the aggregated expression "
            f"must be built from columns, literals, arithmetic, and scalar "
            f"functions."
        )
    return source


def _reject_bare_star_args(
    args: Tuple[Any, ...],
    kwargs: Tuple[Tuple[str, Any], ...],
    *,
    func_name: str,
    original: str,
) -> None:
    """``*`` is only an aggregation source — reject it as a plain call arg."""
    values = list(args) + [v for _, v in kwargs]
    if any(isinstance(v, StarSource) for v in values):
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: '*' is only valid as "
            f"an aggregation source (e.g. count(*)), not as an argument to "
            f"{func_name!r}."
        )


def _convert_call(  # NOSONAR(S3776) — the one call-dispatch ladder (colon placeholder → builtin functional aggregation → transform → scalar → unknown-name AggCall deferral); each rung IS the documented dispatch order and splitting them hides it.
    node: ast.Call, *, agg_map: Dict, original: str,
) -> ParsedExpr:
    if not isinstance(node.func, ast.Name):
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: function calls "
            f"with non-name callee are not supported."
        )
    func_name = node.func.id

    args = tuple(
        _convert(a, agg_map=agg_map, original=original) for a in node.args
    )
    # Reject ``**kwargs`` dictionary unpacking (``kw.arg is None``) rather
    # than silently dropping it (CR) — a dropped ``**`` would change call
    # semantics without warning.
    if any(kw.arg is None for kw in node.keywords):
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: dictionary unpacking "
            f"(**kwargs) is not supported in calls."
        )
    kwargs = tuple(
        (kw.arg, _convert_kwarg_value(kw.value, agg_map=agg_map, original=original))
        for kw in node.keywords
        if kw.arg is not None  # guarded above; narrows kw.arg to str
    )

    # Colon-aggregation placeholder?
    m = _PLACEHOLDER_RE.match(func_name)
    if m:
        idx = int(m.group(1))
        source, agg = agg_map[idx]
        return AggCall(source=source, agg=agg, args=args, kwargs=kwargs)

    # Functional builtin aggregation? Matched via alias/case healing, exactly
    # like colon names heal at binding; ``agg`` stores the RAW token so both
    # spellings collapse to the identical AggCall. ``first``/``last`` over an
    # aggregated input fall through to the transform branch.
    healed = normalize_aggregation_name(func_name)
    if healed in BUILTIN_AGGREGATIONS and not (
        healed in _FIRST_LAST and args and _contains_agg_or_transform(args[0])
    ):
        if not args:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: aggregation "
                f"{func_name!r} requires a value argument, e.g. "
                f"{func_name}(column)."
            )
        source = _validated_agg_source(
            args[0], func_name=func_name, original=original,
        )
        return AggCall(source=source, agg=func_name, args=args[1:], kwargs=kwargs)

    # Transform?
    if func_name in ALL_TRANSFORMS:
        if not args:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: transform "
                f"{func_name!r} requires at least one positional argument "
                f"(the value to transform)."
            )
        _reject_bare_star_args(args, kwargs, func_name=func_name, original=original)
        return TransformCall(
            op=func_name,
            input=args[0],
            args=args[1:],
            kwargs=kwargs,
        )

    # Scalar function? Case-insensitive match; normalised to lower case into
    # ``ScalarCall`` so ``COALESCE`` / ``coalesce`` intern to one key.
    if func_name.lower() in SCALAR_FUNCTIONS:
        if kwargs:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: scalar function "
                f"{func_name!r} does not accept keyword arguments. Pass "
                f"values positionally."
            )
        _reject_bare_star_args(args, kwargs, func_name=func_name, original=original)
        return ScalarCall(name=func_name.lower(), args=args)

    # Unknown name with an aggregatable first arg → AggCall candidate (parity
    # with ``x:whatever``), validated at binding.
    if args and isinstance(args[0], _AGG_SOURCE_KINDS) and not _contains_agg_or_transform(args[0]):
        return AggCall(source=args[0], agg=func_name, args=args[1:], kwargs=kwargs)

    raise UnknownFunctionError(
        name=func_name,
        location=original,
        suggestion=(
            f"Mode-B accepts the closed scalar allowlist "
            f"({sorted(SCALAR_FUNCTIONS)}), transforms "
            f"({sorted(ALL_TRANSFORMS)}), and aggregations in colon or "
            f"functional form (`revenue:sum` / `sum(revenue)`); custom "
            f"aggregation names must be defined on the model."
        ),
    )


# ---------------------------------------------------------------------------
# Canonical text rendering + entity-ref splitting (DEV-1826)
# ---------------------------------------------------------------------------


def _canonical_call_params(
    args: Tuple[Any, ...], kwargs: Tuple[Tuple[str, Any], ...],
) -> str:
    parts = [canonical_measure_text(a) for a in args]
    parts += [f"{k}={_canonical_kwarg_text(v)}" for k, v in kwargs]
    return f"({', '.join(parts)})" if parts else ""


def _canonical_kwarg_text(value: Any) -> str:
    if isinstance(value, tuple):
        return f"[{', '.join(canonical_measure_text(v) for v in value)}]"
    return canonical_measure_text(value)


def canonical_measure_text(parsed: Any) -> str:  # NOSONAR(S3776) — flat per-node-kind rendering table; each branch is one spelling rule.
    """Render a ``ParsedExpr`` back to canonical colon-spelling text.

    Used for alias derivation so the functional and colon spellings of one
    formula sanitize to the SAME public name (DEV-1826). Deterministic, not a
    verbatim round-trip: grouping parens are dropped and spacing normalised.
    """
    if isinstance(parsed, Ref):
        return parsed.name
    if isinstance(parsed, DottedRef):
        return ".".join(parsed.parts)
    if isinstance(parsed, StarSource):
        return "*"
    if isinstance(parsed, Literal):
        if isinstance(parsed.value, str):
            return f"'{parsed.value}'"
        return str(parsed.value)
    if isinstance(parsed, TupleLit):
        return f"({', '.join(canonical_measure_text(e) for e in parsed.elements)})"
    if isinstance(parsed, AggCall):
        source = canonical_measure_text(parsed.source)
        if isinstance(parsed.source, (Ref, DottedRef, StarSource)):
            return f"{source}:{parsed.agg}{_canonical_call_params(parsed.args, parsed.kwargs)}"
        # Expression source (``sum(amount - cost)``): render functionally so the
        # text can't collide with a distinct parse tree like ``amount - cost:sum``.
        parts = [source]
        parts += [canonical_measure_text(a) for a in parsed.args]
        parts += [f"{k}={_canonical_kwarg_text(v)}" for k, v in parsed.kwargs]
        return f"{parsed.agg}({', '.join(parts)})"
    if isinstance(parsed, TransformCall):
        inner = canonical_measure_text(parsed.input)
        params = _canonical_call_params(parsed.args, parsed.kwargs)
        return f"{parsed.op}({inner}{', ' + params[1:-1] if params else ''})"
    if isinstance(parsed, ScalarCall):
        return f"{parsed.name}({', '.join(canonical_measure_text(a) for a in parsed.args)})"
    if isinstance(parsed, (Arith, Cmp)):
        left = canonical_measure_text(parsed.left)
        right = canonical_measure_text(parsed.right)
        return f"{left} {parsed.op} {right}"
    if isinstance(parsed, UnaryOp):
        operand = canonical_measure_text(parsed.operand)
        return f"not {operand}" if parsed.op == "not" else f"{parsed.op}{operand}"
    if isinstance(parsed, BoolOp):
        return f" {parsed.op} ".join(
            canonical_measure_text(o) for o in parsed.operands
        )
    return str(parsed)


def _functional_suffix_text(raw: str, *, agg: str) -> str:
    """Rebuild the ``agg`` / ``agg(rest-args)`` suffix of a functional entity
    ref, preserving any extra-arg text verbatim (``last(balance, updated_at)``
    → ``last(updated_at)``)."""
    open_idx = raw.index("(")
    depth = 0
    split_idx = None
    for i in range(open_idx, len(raw)):
        ch = raw[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                split_idx = i
                break
        elif ch == "," and depth == 1:
            split_idx = i
            break
    if split_idx is None or raw[split_idx] == ")":
        return agg
    rest = raw[split_idx + 1:].strip()
    if rest.endswith(")"):
        rest = rest[:-1].strip()
    return f"{agg}({rest})"


def split_entity_agg_ref(raw: str) -> Tuple[str, Optional[str]]:
    """``(prefix, agg_suffix)`` of a single aggregated-column entity
    reference, accepting BOTH spellings: ``orders.amount:sum`` and
    ``sum(orders.amount)`` split identically (DEV-1826).

    Colon and call-free text splits exactly like
    :func:`slayer.core.refs.split_agg_suffix`. Functional text must parse to
    an ``AggCall`` over a pure column / star source; multi-column expression
    text (``sum(a - b)``) raises ``ValueError`` — an expression is not an
    entity.
    """
    prefix, suffix = split_agg_suffix(raw)
    if suffix is not None or "(" not in raw:
        return prefix, suffix
    parsed = parse_expr(raw)
    if not isinstance(parsed, AggCall) or not isinstance(
        parsed.source, (Ref, DottedRef, StarSource)
    ):
        raise ValueError(
            f"{raw!r} is not a single aggregated column reference; only "
            f"`agg(column)` / `column:agg` forms name an entity."
        )
    source = parsed.source
    if isinstance(source, StarSource):
        prefix = "*"
    elif isinstance(source, Ref):
        prefix = source.name
    else:
        prefix = ".".join(source.parts)
    return prefix, _functional_suffix_text(raw, agg=parsed.agg)
