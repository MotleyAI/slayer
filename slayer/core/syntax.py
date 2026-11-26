"""The Mode-B grammar.

``parse_expr(text) -> ParsedExpr`` parses a Mode-B expression (``DatasetMetric.expression``,
query metrics / dimensions / filters / order) with one SQL-compatible pyparsing grammar —
pure syntax, no scope resolution or named-metric expansion (binder's job). Every spelling
of a construct collapses to one node: CASE → ``iif``, ``EXTRACT`` → ``date_part``,
``count(distinct x)`` → ``count_distinct``, ``BETWEEN`` → its conjunction, ``x::t`` →
``Cast``, interval literals → ``date_add``, regex operators → ``regexp_like``, JSON arrows and
aliases → ``json_value``, ``date_trunc(unit, x)`` → ``unit(x)``.
"""

from __future__ import annotations

import difflib
import math
import re
import threading
import unicodedata
from decimal import Decimal
from enum import Enum
from functools import lru_cache, partial
from itertools import count
from types import MappingProxyType
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union, cast

import pyparsing as pp
from pydantic import BaseModel, ConfigDict

from slayer.core.direction import normalize_direction
from slayer.core.enums import (
    ALL_TRANSFORMS,
    AMBIGUOUS_WEEKDAY_NAMES,
    BUILTIN_AGGREGATIONS,
    DATE_PART_ALIASES,
    GRANULARITY_NAMES,
    PART_ONLY_WORDS,
    RESERVED_NAMES,
    WEEKDAY_CALLS,
    TimeGranularity,
    normalize_aggregation_name,
    part_call_hint,
)
from slayer.core.errors import (
    AggArgumentError,
    AggFilterError,
    CallArgumentError,
    ColonSpellingError,
    GranularityCallError,
    IllegalWindowInFilterError,
    ReservedFunctionNameError,
    UnknownFunctionError,
    VariableError,
)
from slayer.core.json_path import JsonPathError, check_key, json_path_text, parse_json_path
from slayer.core.legacy_spellings import LegacySpan, note_legacy_spellings
from slayer.core.keys import (
    DATE_UNIT_ARGS, RESERVED_SCALAR_NAMES, SCALAR_FUNCTIONS, SCALAR_SPELLING_REGISTRY, SPELLING_REWRITE, check_scalar_arity,
)
from slayer.core.names import match_name
from slayer.core.regex_pattern import RegexPattern
from slayer.core.scalar_args import STRING_SLOTS, check_path, check_pattern, check_replacement, slot_name


# ---------------------------------------------------------------------------
# ParsedExpr family
# ---------------------------------------------------------------------------


class _BaseNode(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)


class Ref(_BaseNode):
    name: str
    # Written double-quoted; not part of identity.
    quoted: bool = False
    # Written as ``AGG(…)``: must name a dataset metric; not part of identity.
    evaluated: bool = False

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Ref) and self.name == other.name

    def __hash__(self) -> int:
        return hash(("Ref", self.name))


class DottedRef(_BaseNode):
    parts: Tuple[str, ...]
    # Some segment was written double-quoted; not part of identity.
    quoted: bool = False
    # Per segment, whether it was double-quoted (empty: none); not part of identity.
    quoted_parts: Tuple[bool, ...] = ()
    # Written as ``AGG(…)``: must name a dataset metric; not part of identity.
    evaluated: bool = False

    def quoted_at(self, i: int) -> bool:
        return 0 <= i < len(self.quoted_parts) and self.quoted_parts[i]

    def __eq__(self, other: object) -> bool:
        return isinstance(other, DottedRef) and self.parts == other.parts

    def __hash__(self) -> int:
        return hash(("DottedRef", self.parts))


class StarSource(_BaseNode):
    pass


class Literal(_BaseNode):
    value: Decimal | str | bool | None = None


class TemporalLiteral(_BaseNode):
    """``DATE '…'`` / ``TIMESTAMP '…'``; the text is validated at binding."""

    kind: str
    text: str


class Placeholder(_BaseNode):
    """An unsubstituted ``{name}`` variable in a literal slot."""

    name: str
    # Offset in the parsed text; not part of identity.
    start: int = -1

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Placeholder) and self.name == other.name

    def __hash__(self) -> int:
        return hash(("Placeholder", self.name))


class TupleLit(_BaseNode):
    """Literal-only list RHS for ``IN`` / ``NOT IN``."""

    elements: Tuple[Literal, ...]


class ArrayLit(_BaseNode):
    """A bracketed literal list ``['a', 'b']`` (``[]`` empty)."""

    elements: Tuple[Any, ...] = ()


class BoundVariable(_BaseNode):
    """Variable ``name`` filling a whole ``IN`` right-hand side: its ``values``, one list (``listed``) or one value;
    the left side's type decides which it is."""

    name: str
    values: Tuple[Any, ...]
    listed: bool


class AggCall(_BaseNode):
    source: Any
    agg: str
    args: Tuple[Any, ...] = ()
    kwargs: Tuple[Tuple[str, Any], ...] = ()
    # ``FILTER (WHERE …)`` predicate.
    filter: Any = None
    # A library-named call ill-formed as the library function: its error, raised unless the owner defines ``agg``.
    library_error: Optional[str] = None


class TransformCall(_BaseNode):
    op: str
    input: Any
    args: Tuple[Any, ...] = ()
    kwargs: Tuple[Tuple[str, Any], ...] = ()


class ScalarCall(_BaseNode):
    name: str
    args: Tuple[Any, ...] = ()
    # The callee and arguments as written (a function or spelling an owning dataset's aggregation may shadow; a
    # spelling rewrites its arguments); not part of identity.
    written: Optional[str] = None
    written_args: Optional[Tuple[Any, ...]] = None

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ScalarCall) and (self.name, self.args) == (other.name, other.args)

    def __hash__(self) -> int:
        return hash(("ScalarCall", self.name, self.args))


class DateTrunc(_BaseNode):
    """``date_trunc('<unit>', value)``, the time bucket; the lower-cased unit is resolved at binding."""

    unit: str
    value: Any


class NamedArg(_BaseNode):
    """An unparenthesised ``name = value`` argument of a non-scalar call: a keyword argument when ``name`` is a
    parameter of the callee, else the comparison (classified at binding)."""

    name: str
    value: Any


class Cast(_BaseNode):
    """``CAST(x AS t)`` / ``x::t`` / ``TRY_CAST(x AS t)``; ``type_text`` is the normalised author type."""

    source: Any
    type_text: str
    try_: bool = False
    # A spelling's callee and arguments as written (``TO_DATE(s)``), as on ``ScalarCall``; not part of identity.
    written: Optional[str] = None
    written_args: Optional[Tuple[Any, ...]] = None

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Cast) and (self.source, self.type_text, self.try_) == (
            other.source, other.type_text, other.try_)

    def __hash__(self) -> int:
        return hash(("Cast", self.source, self.type_text, self.try_))


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
    # Written as a single `=`; not part of identity.
    single_eq: bool = False

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Cmp) and (self.op, self.left, self.right) == (other.op, other.left, other.right)

    def __hash__(self) -> int:
        return hash(("Cmp", self.op, self.left, self.right))


class BoolOp(_BaseNode):
    op: str
    operands: Tuple[Any, ...]


class OrderItem(_BaseNode):
    """One ``ORDER BY`` item of a window: ``value [ASC|DESC] [NULLS FIRST|LAST]``."""

    value: Any
    descending: bool = False
    nulls: Optional[str] = None


class WindowFrame(_BaseNode):
    """A ``ROWS`` / ``RANGE`` frame; each bound is ``(kind, offset)``, ``end`` ``None`` for a single start bound."""

    unit: str
    start: Tuple[str, Optional[Decimal]]
    end: Optional[Tuple[str, Optional[Decimal]]] = None


class WindowCall(_BaseNode):
    """``function OVER (PARTITION BY … ORDER BY … frame)``, read only in window-reading mode."""

    function: Any
    partition: Tuple[Any, ...] = ()
    order: Tuple[OrderItem, ...] = ()
    frame: Optional[WindowFrame] = None


ParsedExpr = Union[
    Ref, DottedRef, StarSource, Literal, TemporalLiteral, Placeholder, TupleLit, ArrayLit, BoundVariable,
    AggCall, TransformCall, ScalarCall, DateTrunc, NamedArg, Cast,
    Arith, UnaryOp, Cmp, BoolOp, WindowCall,
]

#: Ossie's ranking and offset window functions with their argument counts; under ``OVER`` they stay undispatched.
WINDOW_FUNCTION_ARITIES: Mapping[str, Tuple[int, int]] = MappingProxyType({
    "row_number": (0, 0), "rank": (0, 0), "dense_rank": (0, 0), "percent_rank": (0, 0), "cume_dist": (0, 0),
    "ntile": (1, 1), "lag": (1, 3), "lead": (1, 3), "first_value": (1, 1), "last_value": (1, 1),
    "nth_value": (2, 2),
})


# ---------------------------------------------------------------------------
# Lexical helpers
# ---------------------------------------------------------------------------


# The ``__slayer_`` prefix is reserved from user input (P3).
_RESERVED_EXPR_PREFIX = "__slayer_"
# Masked ``{name}`` placeholders: the braces become private-use sentinels, so offsets hold.
_PH_OPEN, _PH_CLOSE = "", ""
_VAR_NAME = r"(?a:[A-Za-z_]\w*)"
_CODE_PLACEHOLDER_RE = re.compile(r"\{\{|\}\}|\{(" + _VAR_NAME + r")\}")
_LITERAL_VALUES = "variables supply literal values"
_OPTIONAL_BLOCKS = (
    "Optional blocks '{? ... ?}' are supported only on raw-SQL dataset surfaces, not in Mode-B expressions."
)
_DISTINCT_ONLY = ("DISTINCT is supported only over one value other than *, as count(distinct x), sum(distinct x), "
                  "avg(distinct x), min(distinct x), max(distinct x) and array_agg(distinct x), i.e. count_distinct(x), "
                  "sum_distinct(x), avg_distinct(x), min(x), max(x) and collect(x)")
#: Calls taking a leading DISTINCT, and the aggregation each spells.
_DISTINCT_CALLS = {"count": "count_distinct", "sum": "sum_distinct", "avg": "avg_distinct", "min": "min", "max": "max",
                   "array_agg": "collect"}
#: The DISTINCT calls Ossie Layer 1 defines.
_OSSIE_DISTINCT_CALLS = frozenset({"count", "sum"})
#: Ordered-set aggregates (``F(p) WITHIN GROUP (ORDER BY x)``), and the aggregation each spells.
_ORDERED_SET_CALLS = {"percentile_cont": "percentile", "percentile_disc": "percentile_disc"}
_ORDERED_SET_ONLY = ("WITHIN GROUP is supported only as PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x) and "
                     "PERCENTILE_DISC(p) WITHIN GROUP (ORDER BY x), one fraction p, ascending order")
_NOT_IN = "not in"
_IN_OPS = frozenset({"in", _NOT_IN})
_IS_NOT = "is not"


class _Problem(ValueError):
    """A grammar-level rejection; ``parse_expr`` prefixes the expression text."""

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class _Region(BaseModel):
    kind: str  # "code" | "string" | "ident" | "comment"
    start: int
    end: int


def _quoted_end(text: str, i: int) -> int:
    """End offset of the quoted span opening at ``i`` (a doubled quote escapes; unterminated runs to the end)."""
    quote, j, n = text[i], i + 1, len(text)
    while j < n:
        if text[j] != quote:
            j += 1
        elif j + 1 < n and text[j + 1] == quote:
            j += 2
        else:
            return j + 1
    return n


def _block_comment_end(text: str, i: int) -> int:
    """End offset of the nesting ``/* … */`` comment opening at ``i``."""
    depth, j, n = 0, i, len(text)
    while j < n:
        pair = text[j:j + 2]
        if pair == "/*":
            depth, j = depth + 1, j + 2
        elif pair == "*/":
            depth, j = depth - 1, j + 2
            if not depth:
                return j
        else:
            j += 1
    raise _Problem(f"unterminated /* comment starting at column {i + 1}.")


def _span_end(text: str, i: int) -> Optional[Tuple[str, int]]:
    """The kind and end of a quoted span or comment opening at ``i``, else None."""
    ch, pair = text[i], text[i:i + 2]
    if ch in ("'", '"'):
        return ("string" if ch == "'" else "ident"), _quoted_end(text, i)
    if pair == "--":
        end = text.find("\n", i)
        return "comment", len(text) if end < 0 else end
    if pair == "/*":
        return "comment", _block_comment_end(text, i)
    return None


def _regions(text: str) -> List[_Region]:
    """Split ``text`` into code, ``'…'`` string, ``"…"`` identifier and SQL comment spans."""
    out: List[_Region] = []
    i = start = 0
    n = len(text)
    while i < n:
        span = _span_end(text, i)
        if span is None:
            i += 1
            continue
        if i > start:
            out.append(_Region(kind="code", start=start, end=i))
        kind, end = span
        out.append(_Region(kind=kind, start=i, end=end))
        i = start = end
    if start < n:
        out.append(_Region(kind="code", start=start, end=n))
    return out


class StringLiteralSpan(BaseModel):
    """A ``'…'`` literal at ``text[start:end]`` (quotes included) and its ``value``."""

    model_config = ConfigDict(frozen=True)

    start: int
    end: int
    value: str


def string_literal_spans(text: str) -> List[StringLiteralSpan]:
    """Each string literal of ``text`` in order, between code regions; none when ``text`` has an unterminated comment."""
    try:
        regions = _regions(text)
    except _Problem:
        return []
    return [
        StringLiteralSpan(start=r.start, end=r.end, value=text[r.start + 1:r.end - 1].replace("''", "'"))
        for r in regions if r.kind == "string" and r.end - r.start >= 2 and text[r.end - 1] == "'"
    ]


def _mask_code(chunk: str) -> str:
    if "{?" in chunk or "?}" in chunk:
        raise _Problem(_OPTIONAL_BLOCKS)
    chunk = _CODE_PLACEHOLDER_RE.sub(
        lambda m: f"{_PH_OPEN}{m.group(1)}{_PH_CLOSE}" if m.group(1) else m.group(0), chunk,
    )
    if "{" in chunk or "}" in chunk:
        raise _Problem("a brace expression is not a variable placeholder; write a variable as {name}.")
    return chunk


def _mask_placeholders(text: str) -> str:
    """``{name}`` outside string literals → sentinel-bracketed name of the same length; comments → spaces."""
    if _PH_OPEN in text or _PH_CLOSE in text:
        raise _Problem("unsupported character in expression.")
    parts: List[str] = []
    for region in _regions(text):
        chunk = text[region.start:region.end]
        if region.kind == "comment":
            chunk = re.sub(r"[^\n]", " ", chunk)
        elif region.kind == "code":
            chunk = _mask_code(chunk)
        elif region.kind == "ident" and re.search(r"\{" + _VAR_NAME + r"\}", chunk):
            raise _Problem(f"{_LITERAL_VALUES}, never an identifier.")
        parts.append(chunk)
    return "".join(parts)


class CallSpan(BaseModel):
    """A single-name call ``callee(…)`` at ``text[start:end]``: its callee as written, its name, whether that is
    double-quoted, the offset of its argument list's ``(`` and its argument count."""

    model_config = ConfigDict(frozen=True)

    callee: str
    name: str
    quoted: bool
    start: int
    paren: int
    end: int
    arguments: int


def _argument_paren(text: str, i: int) -> int:
    """The offset of the first ``(`` at or after ``i`` outside comments."""
    while text[i] != "(":
        span = _span_end(text, i)
        i = span[1] if span is not None and span[0] == "comment" else i + 1
    return i


class _State:
    """Per-parse scratch: the expression text, the farthest failure offset, the calls and method calls read, the
    non-Ossie spellings rewritten and, while rewriting, the colon spans."""

    def __init__(self) -> None:
        self.text = ""
        self.farthest = 0
        self.calls: Dict[Tuple[int, int], CallSpan] = {}
        self.methods: Dict[Tuple[int, int], LegacySpan] = {}
        self.spellings: Dict[str, None] = {}
        self.spans: Optional[List["_ColonSpan"]] = None
        #: While ``reference_spans`` reads: each reference node by id, with its offsets.
        self.refs: Optional[Dict[int, Tuple[Any, int, int]]] = None
        #: While ``literal_comparisons`` reads: each string literal by id with its offsets, and each written comparison.
        self.literals: Optional[Dict[int, Tuple[Any, int, int]]] = None
        self.compared: Optional[Dict[int, Any]] = None
        #: Window-reading mode: ``OVER (…)`` reads as a ``WindowCall`` instead of refusing.
        self.windows = False

    def reset(self, text: str) -> None:
        self.text, self.farthest, self.calls, self.methods, self.spellings = text, 0, {}, {}, {}
        self.windows = False


_STATE = _State()
# Re-entrant: rendering inside a parse action probes the grammar (``_probe_tree``).
_PARSE_LOCK = threading.RLock()


def _spelled(kind: str) -> None:
    """Note a non-Ossie spelling the parser rewrites (``nonossie_spellings``)."""
    _STATE.spellings.setdefault(kind, None)


def _spelled_call(kind: str, node: Any) -> Any:
    _spelled(kind)
    return node


def _record_failure(_s: str, loc: int, _expr: Any, _err: Any) -> None:
    if loc > _STATE.farthest:
        _STATE.farthest = loc


def _on(element: pp.ParserElement, fn: Callable[[List[Any]], Any]) -> pp.ParserElement:
    """``element`` producing ``fn(tokens)``, groups as plain lists."""
    def action(toks: pp.ParseResults) -> Any:
        return fn(toks.as_list())
    return element.set_parse_action(action)


def _first(toks: pp.ParseResults) -> str:
    """The first token of a terminal match, as text."""
    return str(toks[0])


def _t(element: pp.ParserElement) -> pp.ParserElement:
    """A terminal whose failures feed the farthest-failure error position."""
    element.set_fail_action(_record_failure)
    return element


def _fail(detail: str) -> pp.ParserElement:
    """Matches nothing and raises ``detail`` — the committed branch of a construct."""
    def _raise(_s: str, _loc: int, _toks: pp.ParseResults) -> None:
        raise _Problem(detail)
    return pp.Empty().set_parse_action(_raise)


def _need(element: pp.ParserElement, detail: str) -> pp.ParserElement:
    return element | _fail(detail)


_RUN_CHAR = r"[^\x00-\x08\x0e-\x1f\x7f!-/:-@\[-^`{-~\s]"
_RUN = r"(?!\d)" + _RUN_CHAR + r"+"


class _Word:
    """A lexed identifier: the raw spelling, its NFKC name, and whether it was double-quoted."""

    __slots__ = ("raw", "name", "quoted")

    def __init__(self, raw: str, name: str, quoted: bool) -> None:
        self.raw = raw
        self.name = name
        self.quoted = quoted


def _check_reserved(name: str) -> None:
    if name.startswith(_RESERVED_EXPR_PREFIX):
        raise _Problem(
            f"the identifier {name!r} uses the reserved {_RESERVED_EXPR_PREFIX!r} prefix, which is "
            f"reserved for SLayer-internal names."
        )


def _bare_word(s: str, loc: int, toks: pp.ParseResults) -> _Word:
    raw = _first(toks)
    name = unicodedata.normalize("NFKC", raw)
    if not name.isidentifier():
        raise pp.ParseException(s, loc, f"{raw!r} is not an identifier")
    _check_reserved(name)
    return _Word(raw=raw, name=name, quoted=False)


def _quoted_word(toks: List[Any]) -> _Word:
    raw = toks[0]
    name = raw[1:-1].replace('""', '"')
    if not name:
        raise _Problem('an empty quoted identifier "" names nothing.')
    _check_reserved(name)
    return _Word(raw=raw, name=name, quoted=True)


def _kw(*words: str) -> pp.ParserElement:
    """An ASCII-exact, case-insensitive keyword (a Unicode case-fold spoof is never one)."""
    wanted = frozenset(words)
    def is_keyword(toks: pp.ParseResults) -> bool:
        raw = _first(toks)
        return raw.isascii() and raw.lower() in wanted
    return _t(pp.Regex(_RUN).add_condition(is_keyword).set_name("/".join(w.upper() for w in words)))


def _sym(pattern: str, name: str) -> pp.ParserElement:
    return _t(pp.Regex(pattern).set_name(name))


LPAR, RPAR = pp.Suppress(_sym(r"\(", "'('")), pp.Suppress(_sym(r"\)", "')'"))
LBRACK, RBRACK = pp.Suppress(_sym(r"\[", "'['")), pp.Suppress(_sym(r"\]", "']'"))
COMMA = pp.Suppress(_sym(",", "','"))
DOT = pp.Suppress(_sym(r"\.", "'.'"))
STAR = _sym(r"\*(?!\*)", "'*'")
COLON = pp.Suppress(_sym(r":(?!:)", "':'"))
DCOLON = pp.Suppress(_sym("::", "'::'"))
EQ = pp.Suppress(_sym(r"=(?!=)", "'='"))

_BARE = _t(pp.Regex(_RUN).set_name("identifier")).set_parse_action(_bare_word)
_QUOTED = _on(_t(pp.Regex(r'"(?:[^"]|"")*"').set_name("quoted identifier")), _quoted_word)
_PLACEHOLDER = _t(pp.Regex(_PH_OPEN + "(" + _VAR_NAME + ")" + _PH_CLOSE).set_name("variable"))


def _number(toks: List[Any]) -> Literal:
    text = toks[0]
    return Literal(value=Decimal(text))


def _string(_s: str, loc: int, toks: pp.ParseResults) -> Literal:
    raw = _first(toks)
    node = Literal(value=raw[1:-1].replace("''", "'"))
    if _STATE.literals is not None:
        _STATE.literals[id(node)] = (node, loc, loc + len(raw))
    return node


def _written_comparison(node: Any) -> Any:
    """``node``, recorded for ``literal_comparisons`` as a comparison written in the text."""
    if _STATE.compared is not None:
        _STATE.compared[id(node)] = node
    return node


_NUMBER = _on(_t(pp.Regex(r"(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?").set_name("number")), _number)
_STRING = _t(pp.Regex(r"'(?:[^']|'')*'").set_name("string")).set_parse_action(_string)
_CONSTANT_WORDS = {"True": True, "False": False, "None": None}
_SQL_CONSTANT_WORDS = {"true": True, "false": False, "null": None}


def _constant_value(raw: str) -> Tuple[bool, Any]:
    if raw in _CONSTANT_WORDS:
        return True, _CONSTANT_WORDS[raw]
    if raw.isascii() and raw.lower() in _SQL_CONSTANT_WORDS:
        return True, _SQL_CONSTANT_WORDS[raw.lower()]
    return False, None


def _constant(s: str, loc: int, toks: pp.ParseResults) -> Literal:
    known, value = _constant_value(_first(toks))
    if not known:
        raise pp.ParseException(s, loc, "not a constant")
    return Literal(value=value)


_CONSTANT = _t(pp.Regex(_RUN).set_name("constant")).set_parse_action(_constant)


def _placeholder_node(s: str, loc: int, toks: pp.ParseResults) -> Placeholder:
    raw = _first(toks)
    end = loc + len(raw)
    rest = s[end:].lstrip()
    if rest.startswith(("(", ".")) or (rest.startswith(":") and not rest.startswith("::")):
        raise _Problem(f"{_LITERAL_VALUES}; a variable cannot name a function, field or path.")
    return Placeholder(name=raw[1:-1], start=loc)


def _reject_placeholder(_s: str, _loc: int, _toks: pp.ParseResults) -> None:
    raise _Problem(f"{_LITERAL_VALUES}; a variable cannot name a field or path segment.")


def _reject_placeholder_kwarg(_s: str, _loc: int, _toks: pp.ParseResults) -> None:
    raise _Problem(f"{_LITERAL_VALUES}; a variable cannot name a keyword argument.")


# ---------------------------------------------------------------------------
# Grammar
# ---------------------------------------------------------------------------


expr = pp.Forward()
expr.set_name("expression")
_concat_level = pp.Forward()

_SEGMENT = _BARE | _QUOTED
_PATH = pp.Group(_SEGMENT + pp.ZeroOrMore(DOT + (_SEGMENT | _PLACEHOLDER.copy().set_parse_action(_reject_placeholder))))


def _path_node(words: List[_Word]) -> Ref | DottedRef:
    quoted = any(w.quoted for w in words)
    if len(words) == 1:
        return Ref(name=words[0].name, quoted=quoted)
    return DottedRef(parts=tuple(w.name for w in words), quoted=quoted, quoted_parts=tuple(w.quoted for w in words))


def _star_path_node(words: List[_Word]) -> DottedRef:
    return DottedRef(parts=(*(w.name for w in words), "*"), quoted=any(w.quoted for w in words),
                     quoted_parts=(*(w.quoted for w in words), False))


_PATH_STAR = pp.Group(_PATH + DOT + pp.Suppress(STAR))


class _Distinct:
    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value


def _spanned(build: Callable[[Any], Any]) -> Callable[[List[Any]], Any]:
    """A ``pp.Located`` reference's action: ``build`` over its value, its offsets recorded for ``reference_spans``."""
    def action(toks: List[Any]) -> Any:
        node = build(toks[1])
        if _STATE.refs is not None:
            _STATE.refs[id(node)] = (node, toks[0], toks[2])
        return node
    return action


_ARG_END = pp.FollowedBy(_sym(r"[,)]", "',' or ')'"))
_STAR_ARG = _on((pp.Located(pp.Suppress(STAR)) + _ARG_END), _spanned(lambda _value: StarSource()))
_PATH_STAR_ARG = _on((pp.Located(_PATH_STAR) + _ARG_END), _spanned(lambda value: _star_path_node(value[0][0])))
_SEQ_ITEMS = pp.Group(pp.Optional(pp.DelimitedList(expr, allow_trailing_delim=True)))
_LIST_VALUE = _on((LBRACK + _SEQ_ITEMS + RBRACK), lambda toks: tuple(toks[0]))
_TUPLE_VALUE = _on((LPAR + pp.Group(expr + pp.OneOrMore(COMMA + expr) + pp.Optional(COMMA)) + RPAR), lambda toks: tuple(toks[0]))
_DISTINCT_VALUE = _kw("distinct") + _fail(f"{_DISTINCT_ONLY}.")
_KW_VALUE = _DISTINCT_VALUE | _LIST_VALUE | _TUPLE_VALUE | _STAR_ARG | _PATH_STAR_ARG | expr
def _named_arg(toks: List[Any]) -> NamedArg:
    """A ``name = value`` argument, the name's offsets recorded for ``reference_spans`` (a comparison reads it)."""
    node = NamedArg(name=toks[1][0].name, value=toks[3])
    if _STATE.refs is not None:
        _STATE.refs[id(node)] = (node, toks[0], toks[2])
    return node


_KWARG = _on((pp.Located(_BARE) + EQ + _KW_VALUE), _named_arg)
_PH_KWARG = (_PLACEHOLDER + EQ).set_parse_action(_reject_placeholder_kwarg)
_VALUE_ARG = _STAR_ARG | _PATH_STAR_ARG | expr
_DISTINCT_ARG = _on((pp.Suppress(_kw("distinct")) + _need(_VALUE_ARG, f"{_DISTINCT_ONLY}.")), lambda toks: _Distinct(value=toks[0]))
_UNPACKING = _sym(r"\*\*", "'**'") + _fail("dictionary unpacking (**kwargs) is not supported in calls.")
_ANY_ARG = _UNPACKING | _PH_KWARG | _KWARG | _DISTINCT_ARG | _VALUE_ARG
_POSITIONAL_ARG = _UNPACKING | _PH_KWARG | _DISTINCT_ARG | _VALUE_ARG


def _arg_list(first: pp.ParserElement, rest: pp.ParserElement) -> pp.ParserElement:
    return pp.Group(pp.Optional(first + pp.ZeroOrMore(COMMA + rest) + pp.Optional(COMMA)))


def _callee_is_scalar(toks: pp.ParseResults) -> bool:
    name = toks.as_list()[0][-1].name.lower()
    return name in SCALAR_FUNCTIONS or name in DATE_PART_ALIASES


_SCALAR_CALL = _PATH.copy().add_condition(_callee_is_scalar) + LPAR + _arg_list(_POSITIONAL_ARG, _POSITIONAL_ARG) + RPAR
_OTHER_CALL = _PATH + LPAR + _arg_list(_ANY_ARG, _ANY_ARG) + RPAR
_CALL = _SCALAR_CALL | _OTHER_CALL


def _split_args(items: List[Any], *, call: str) -> Tuple[Tuple[Any, ...], Tuple[Tuple[str, Any], ...], bool]:
    """The arguments in written order (``name = value`` ones as ``NamedArg``), no keywords, and a leading DISTINCT."""
    positional: List[Any] = []
    distinct = False
    named: set = set()
    for i, item in enumerate(items):
        if isinstance(item, NamedArg):
            if item.name in named:
                raise _Problem(f"{call}() got keyword argument {item.name!r} more than once.")
            named.add(item.name)
        if isinstance(item, _Distinct):
            if i != 0:
                raise _Problem(f"{_DISTINCT_ONLY}.")
            distinct = True
            item = item.value
        if isinstance(item, tuple):
            raise _Problem(f"{call}(): a list is only valid as a keyword-argument value.")
        positional.append(item)
    return tuple(positional), (), distinct


class _PendingCall:
    """A parsed call at ``text[start:end]``, dispatched once its postfix is known (a trailing ``OVER (`` rejects
    first, or picks a window function's reading)."""

    __slots__ = ("words", "items", "start", "end")

    def __init__(self, words: List[_Word], items: List[Any], start: int, end: int) -> None:
        self.words = words
        self.items = items
        self.start = start
        self.end = end


def _resolve(node: Any) -> Any:
    return _dispatch_pending(node) if isinstance(node, _PendingCall) else node


def _record_call(call: _PendingCall) -> None:
    """Record the single-name ``call`` as written for ``call_spans``."""
    word = call.words[0]
    _STATE.calls[(call.start, call.end)] = CallSpan(
        callee=word.raw, name=word.name, quoted=word.quoted, start=call.start, end=call.end,
        paren=_argument_paren(_STATE.text, call.start + len(word.raw)), arguments=len(call.items),
    )


def _dispatch_pending(call: _PendingCall) -> Any:
    words = call.words
    name = words[-1].name
    args, kwargs, distinct = _split_args(call.items, call=name)
    if distinct:
        values = [a for a in args if not isinstance(a, NamedArg)]
        if (name.lower() not in _DISTINCT_CALLS or len(words) > 1 or len(values) != 1
                or isinstance(args[0], (StarSource, NamedArg))):
            raise _Problem(f"{_DISTINCT_ONLY}.")
        if name.lower() not in _OSSIE_DISTINCT_CALLS:
            _spelled(f"{name.upper()}(DISTINCT …)")
        name = _DISTINCT_CALLS[name.lower()]
    if len(words) > 1:
        return _method_call(
            path=tuple(w.name for w in words[:-1]), name=name, args=args, kwargs=kwargs, span=(call.start, call.end),
        )
    _record_call(call)
    return _dispatch_call(name, args=args, kwargs=kwargs, original=_STATE.text)


_CALL_ATOM = _on(pp.Located(_CALL), lambda toks: _PendingCall(
    words=list(toks[1][0]), items=list(toks[1][1]), start=toks[0], end=toks[2],
))

_COLON_SOURCE = (_on(pp.Suppress(STAR), lambda _toks: StarSource())
                 | _on(_PATH_STAR.copy(), lambda toks: _star_path_node(toks[0][0]))
                 | _on(_PATH.copy(), lambda toks: _path_node(list(toks[0]))))
# The removed colon spelling ``<source>:<name>[(<args>)]``: refused, or recorded by ``rewrite_colon_spans``.
_COLON_AGG = (pp.Located(_COLON_SOURCE) + COLON + pp.Located(_BARE)
              + pp.Optional(pp.Located(LPAR + pp.Located(_arg_list(_ANY_ARG, _ANY_ARG)) + RPAR)))


class _ColonSpan(BaseModel):
    """One colon span's offsets: the whole span, its source, its name (as written) and its argument list."""

    start: int
    end: int
    source: Tuple[int, int]
    name: str
    args: Optional[Tuple[int, int]] = None


def colon_twin(*, source: str, name: str, args: str = "", units: frozenset[str] = frozenset()) -> str:
    """The functional twin of the colon span ``<source>:<name>[(<args>)]``; ``units`` are further granularities."""
    if (name.lower() in GRANULARITY_NAMES or name.casefold() in units) and not args:
        return f"date_trunc('{name}', {source})"
    return f"{name}({source}, {args})" if args else f"{name}({source})"


def _colon_node(toks: List[Any]) -> AggCall:
    start, (source,), src_end, _name_start, (word,), end, *call = toks
    # ``call``: the parenthesised argument list's offsets and items.
    items: List[Any] = []
    args_at: Optional[Tuple[int, int]] = None
    if call:
        _open, (args_start, (items,), args_end), end = call
        args_at = (args_start, args_end)
    span = _ColonSpan(start=start, end=end, source=(start, src_end), name=word.raw, args=args_at)
    if _STATE.spans is None:
        text = _STATE.text
        raise ColonSpellingError(span=text[start:end], twin=colon_twin(
            source=text[start:src_end], name=span.name, args=text[args_at[0]:args_at[1]] if args_at else ""))
    _STATE.spans.append(span)
    args, kwargs, _distinct = _split_args(list(items), call=word.name)
    return AggCall(source=source, agg=word.name, args=args, kwargs=kwargs)


_on(_COLON_AGG, _colon_node)
_REF = _on(pp.Located(_PATH.copy()), _spanned(lambda value: _path_node(list(value[0]))))

# -- CASE ----------------------------------------------------------------------

_CASE_WORDS = ("case", "when", "then", "else", "end")
WHEN, THEN, ELSE, END = (pp.Suppress(_kw(w)) for w in _CASE_WORDS[1:])
_MISSING_THEN = "Malformed CASE expression: a WHEN branch is missing its THEN."
_WHEN_CLAUSE = pp.Group(WHEN + _need(expr, "Malformed CASE expression: a WHEN needs a condition.")
                        + _need(THEN, _MISSING_THEN)
                        + _need(expr, "Malformed CASE expression: a THEN needs a value."))
_CASE_TAIL = (pp.Group(pp.OneOrMore(_WHEN_CLAUSE))
              + pp.Group(pp.Optional(ELSE + _need(expr, "Malformed CASE expression: an ELSE needs a value.")))
              + _need(END, "Malformed CASE expression: missing END."))
_SEARCHED_CASE = pp.Suppress(_kw("case")) + pp.FollowedBy(WHEN) + pp.Group(pp.Empty()) + _CASE_TAIL
# A bare keyword-named simple-CASE operand (``CASE case WHEN …``) is ambiguous: no CASE.
_SIMPLE_CASE = (pp.Suppress(_kw("case")) + ~(_kw(*_CASE_WORDS) + WHEN)
                + pp.Group(expr) + pp.FollowedBy(WHEN) + _CASE_TAIL)


def _case_node(toks: List[Any]) -> Any:
    operand = toks[0][0] if len(toks[0]) else None
    branches = [(b[0], b[1]) for b in toks[1]]
    result = toks[2][0] if len(toks[2]) else Literal(value=None)
    for cond, value in reversed(branches):
        test = Cmp(op="==", left=operand, right=cond) if operand is not None else cond
        result = ScalarCall(name="iif", args=(test, value, result))
    return result


_CASE = _on(_SEARCHED_CASE | _SIMPLE_CASE, _case_node)

# -- CAST / EXTRACT / POSITION / SUBSTRING / TRIM ------------------------------

_TYPE_EXTRA_WORDS = ("precision", "varying", "with", "without", "time", "zone", "local", "unsigned", "signed")
_TYPE_WORD = _t(pp.Regex(r"[A-Za-z_][A-Za-z0-9_]*").set_name("type name"))
_TYPE_PARAMS = LPAR + _need(
    pp.DelimitedList(_sym(r"\d+", "type parameter")) + RPAR,
    "a type's parameters are whole numbers in parentheses, e.g. DECIMAL(10,2).",
)
_ARRAY_SUFFIX = _on(LBRACK + _need(RBRACK, "an array type is written <type>[], e.g. INT[]."), lambda _toks: "[]")
_TYPE_SPEC = (pp.Group(_TYPE_WORD + pp.ZeroOrMore(_kw(*_TYPE_EXTRA_WORDS)))
              + pp.Group(pp.Optional(_TYPE_PARAMS))
              + pp.Group(pp.ZeroOrMore(_kw(*_TYPE_EXTRA_WORDS)))
              + pp.Group(pp.ZeroOrMore(_ARRAY_SUFFIX)))


def _type_text(toks: List[Any]) -> str:
    head, params, tail, arrays = toks
    text = " ".join(w.upper() for w in head)
    if params:
        text += f"({','.join(params)})"
    if tail:
        text += " " + " ".join(w.upper() for w in tail)
    return text + "".join(arrays)


_on(_TYPE_SPEC, _type_text)
_CAST = ((pp.Suppress(_kw("cast")) | _kw("try_cast")) + LPAR
         + _need(expr, "CAST needs a value: CAST(x AS <type>).")
         + _need(pp.Suppress(_kw("as")), "CAST needs AS <type>: CAST(x AS <type>).")
         + _need(_TYPE_SPEC, "CAST needs a type after AS.")
         + _need(RPAR, "CAST(x AS <type>) is missing its ')'."))


def _cast_node(toks: List[Any]) -> Cast:
    try_ = len(toks) == 3
    return Cast(source=toks[-2], type_text=toks[-1], try_=try_)


_on(_CAST, _cast_node)

_EXTRACT = (pp.Suppress(_kw("extract")) + LPAR + (_STRING | _BARE) + pp.Suppress(_kw("from"))
            + expr + RPAR)


def _extract_node(toks: List[Any]) -> ScalarCall:
    part, value = toks
    word = part.value if isinstance(part, Literal) else part.name
    return ScalarCall(name="date_part", args=_date_args(
        name="date_part", args=(Literal(value=word), value), original=_STATE.text,
    ))


_on(_EXTRACT, _extract_node)

_RETURNING_TYPES = frozenset({"INT", "DOUBLE", "BOOLEAN", "DATE", "TIMESTAMP", "TEXT"})
_JSON_VALUE_RETURNING = (pp.Suppress(_kw("json_value")) + LPAR + expr + COMMA + expr + pp.Suppress(_kw("returning"))
                         + _need(_TYPE_SPEC, "RETURNING needs a type: INT, DOUBLE, BOOLEAN, DATE, TIMESTAMP or TEXT.")
                         + _need(RPAR, "json_value(x, path RETURNING <type>) is missing its ')'."))


def _json_value_returning(toks: List[Any]) -> Cast:
    _spelled("json_value(… RETURNING …)")
    value, path, type_text = toks
    if type_text not in _RETURNING_TYPES:
        raise _Problem(f"json_value() RETURNING {type_text} is not supported; use one of {', '.join(sorted(_RETURNING_TYPES))}.")
    return Cast(source=ScalarCall(name="json_value", args=(value, path)), type_text=type_text, try_=True)


_on(_JSON_VALUE_RETURNING, _json_value_returning)
_POSITION = _on((pp.Suppress(_kw("position")) + LPAR + _concat_level + pp.Suppress(_kw("in")) + expr + RPAR), lambda toks: ScalarCall(name="instr", args=(toks[1], toks[0])))
_SUBSTRING = _on((pp.Suppress(_kw("substring")) + LPAR + expr + pp.Suppress(_kw("from")) + expr
              + pp.Optional(pp.Suppress(_kw("for")) + expr) + RPAR), lambda toks: _spelled_call("SUBSTRING(… FROM …)", ScalarCall(name="substr", args=tuple(toks))))
_TRIM_FORM = (_kw("trim") + LPAR + _kw("both", "leading", "trailing")) + _fail(
    "TRIM(BOTH | LEADING | TRAILING …) is not supported; use trim(x), ltrim(x) or rtrim(x).",
)

# -- typed literals, intervals, clock keywords ----------------------------------

#: Typed-literal keywords → their literal kind (Ossie's ``TIMESTAMP_NTZ`` is SLayer's naive ``TIMESTAMP``).
_TEMPORAL_KINDS = {"date": "date", "timestamp": "timestamp", "timestamp_ntz": "timestamp"}
_TEMPORAL = _on((_kw(*_TEMPORAL_KINDS) + _STRING),
                lambda toks: TemporalLiteral(kind=_TEMPORAL_KINDS[toks[0].lower()], text=toks[1].value))
_TIME_LITERAL = (_kw("time") + _STRING) + _fail(
    "TIME '…' literals are not supported (there is no time-of-day type); compare with a TIMESTAMP '…' literal.",
)
_UNIT_WORDS = frozenset(g.value for g in TimeGranularity)


def _unit_of(word: str) -> Optional[str]:
    low = word.lower()
    if low in _UNIT_WORDS:
        return low
    if low.endswith("s") and low[:-1] in _UNIT_WORDS:
        return low[:-1]
    return None


def _is_unit(toks: pp.ParseResults) -> bool:
    raw = _first(toks)
    return raw.isascii() and _unit_of(raw) is not None


_UNIT = _t(pp.Regex(_RUN).add_condition(_is_unit))
_INTERVAL = (pp.Suppress(_kw("interval")) + (_STRING | _NUMBER) + pp.Optional(_UNIT))


def _interval_count(text: str) -> Literal:
    if not re.fullmatch(r"[+-]?\d+", text.strip()):
        raise _Problem(f"an interval count must be a whole number; got {text.strip()!r}.")
    return Literal(value=Decimal(int(text)))


def _interval_node(toks: List[Any]) -> ScalarCall:
    _spelled("INTERVAL literal")
    quantity = toks[0]
    unit_word = toks[1] if len(toks) > 1 else None
    if isinstance(quantity.value, str):
        pieces = quantity.value.split()
    else:
        pieces = [str(quantity.value)]
    if unit_word is not None:
        pieces.append(unit_word)
    if len(pieces) > 2 and len(pieces) % 2 == 0 and unit_word is None:
        chain = " ".join(f"+ INTERVAL '{n} {u}'" for n, u in zip(pieces[::2], pieces[1::2]))
        raise _Problem(f"an interval literal names one unit; write a chain: x {chain}.")
    if len(pieces) != 2:
        raise _Problem(
            "write an interval literal as INTERVAL '<n> <unit>', INTERVAL '<n>' <unit> or INTERVAL <n> <unit>."
        )
    count, word = pieces
    unit = _unit_of(word)
    if unit is None:
        accepted = ", ".join(g.value for g in TimeGranularity)
        raise _Problem(f"unknown interval unit {word!r}; accepted: {accepted}.")
    return ScalarCall(name="interval", args=(_interval_count(count), Literal(value=unit)))


_on(_INTERVAL, _interval_node)
_CLOCK = _on((_kw("current_date", "current_timestamp") + ~pp.FollowedBy(LPAR)), lambda toks: ScalarCall(name="current_date" if toks[0].lower() == "current_date" else "now"))

# -- primary, postfix ----------------------------------------------------------

_PAREN = LPAR + expr + RPAR
_PLACEHOLDER_ATOM = _PLACEHOLDER.copy().set_parse_action(_placeholder_node)
_KEYWORD_LED = pp.FollowedBy(pp.Regex(
    r"(?i:case|cast|try_cast|extract|position|substring|trim|date|timestamp|timestamp_ntz|time|interval|current_date"
    r"|current_timestamp"
    r"|json_value)(?![A-Za-z0-9_])"
)) + (_CASE | _CAST | _EXTRACT | _POSITION | _SUBSTRING | _TRIM_FORM | _TEMPORAL | _TIME_LITERAL | _INTERVAL | _CLOCK
      | _JSON_VALUE_RETURNING)
_ARRAY = _on((LBRACK + _SEQ_ITEMS + RBRACK), lambda toks: ArrayLit(elements=tuple(toks[0])))
_ATOM = (_KEYWORD_LED | _CONSTANT | _PLACEHOLDER_ATOM | _NUMBER | _STRING | _PAREN | _ARRAY
         | _CALL_ATOM | _COLON_AGG | _REF)

_FILTER_CLAUSE = (pp.Suppress(_kw("filter")) + LPAR
                  + _need(pp.Suppress(_kw("where")), "FILTER needs (WHERE <condition>).")
                  + _need(expr, "FILTER (WHERE …) needs a condition.")
                  + _need(RPAR, "FILTER (WHERE <condition>) is missing its ')'."))


def _reject_over(_s: str, _loc: int, _toks: pp.ParseResults) -> None:
    if _STATE.windows:
        return
    raise IllegalWindowInFilterError(
        filter_expr=_STATE.text,
        source="raw OVER(...) is not allowed in Mode-B DSL",
        suggestion="use a transform instead (rank, percent_rank, dense_rank, ntile, cumsum, lag, lead, time_shift, …).",
    )


_OVER = pp.Suppress((_kw("over") + pp.FollowedBy(LPAR)).set_parse_action(_reject_over))
_WINDOW_FORM = "a window is OVER ([PARTITION BY …] [ORDER BY …] [ROWS|RANGE frame])."
_PARTITION = _on(
    pp.Suppress(_kw("partition")) + _need(pp.Suppress(_kw("by")), "PARTITION needs BY.")
    + _need(pp.Group(pp.DelimitedList(expr)), "PARTITION BY needs a value."),
    lambda toks: ("partition", tuple(toks[0])),
)


def _order_item(toks: List[Any]) -> OrderItem:
    words = [t.lower() for t in toks[1:]]
    return OrderItem(value=toks[0], descending="desc" in words,
                     nulls=next((w for w in words if w in ("first", "last")), None))


_ORDER_ITEM = _on(
    expr + pp.Optional(_kw("asc", "desc"))
    + pp.Optional(pp.Suppress(_kw("nulls")) + _need(_kw("first", "last"), "NULLS needs FIRST or LAST.")),
    _order_item,
)
_ORDER = _on(
    pp.Suppress(_kw("order")) + _need(pp.Suppress(_kw("by")), "ORDER needs BY.")
    + _need(pp.Group(pp.DelimitedList(_ORDER_ITEM)), "ORDER BY needs a value."),
    lambda toks: ("order", tuple(toks[0])),
)


def _frame_bound(toks: List[Any]) -> Tuple[str, Optional[Decimal]]:
    head, side = toks[0], toks[-1].lower()
    if not isinstance(head, Literal):
        return (f"{head.lower()} {side}", None)
    value = head.value
    if not isinstance(value, Decimal) or value != value.to_integral_value():
        raise _Problem("a frame offset is a whole number, e.g. ROWS BETWEEN 6 PRECEDING AND CURRENT ROW.")
    # Kept a Decimal: ``int()`` of ``1e1000000000`` would allocate its billion digits.
    return (side, value)


_SIDE = _need(_kw("preceding", "following"), "a frame bound ends in PRECEDING or FOLLOWING.")
_BOUND = _on(
    (_kw("unbounded") + _SIDE) | (_kw("current") + _need(_kw("row"), "CURRENT needs ROW.")) | (_NUMBER + _SIDE),
    _frame_bound,
)
_NEED_BOUND = _need(_BOUND, "a frame bound is UNBOUNDED PRECEDING, n PRECEDING, CURRENT ROW, n FOLLOWING or UNBOUNDED "
                            "FOLLOWING.")
_FRAME = _on(
    _kw("rows", "range") + (
        (pp.Suppress(_kw("between")) + _NEED_BOUND + _need(pp.Suppress(_kw("and")), "BETWEEN needs AND.")
         + _NEED_BOUND)
        | _NEED_BOUND
    ),
    lambda toks: WindowFrame(unit=toks[0].lower(), start=toks[1], end=toks[2] if len(toks) > 2 else None),
)


class _Window:
    """A parsed ``OVER (…)`` clause, applied to its call in ``_primary_node``."""

    __slots__ = ("partition", "order", "frame")

    def __init__(self, items: List[Any]) -> None:
        parts = {i[0]: i[1] for i in items if isinstance(i, tuple)}
        self.partition: Tuple[Any, ...] = parts.get("partition", ())
        self.order: Tuple[OrderItem, ...] = parts.get("order", ())
        self.frame = next((i for i in items if isinstance(i, WindowFrame)), None)


_WINDOW = _on(
    _OVER + LPAR + pp.Optional(_PARTITION) + pp.Optional(_ORDER) + pp.Optional(_FRAME) + _need(RPAR, _WINDOW_FORM),
    _Window,
)


class _WithinGroup:
    """A parsed ``WITHIN GROUP (ORDER BY x)``: the ordered value of an ordered-set aggregate."""

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value


_WITHIN_GROUP = _on(
    pp.Suppress(_kw("within")) + pp.Suppress(_kw("group")) + LPAR + pp.Suppress(_kw("order")) + pp.Suppress(_kw("by"))
    + _need(expr, f"{_ORDERED_SET_ONLY}.") + pp.Optional(pp.Suppress(_kw("asc")))
    + _need(RPAR, f"{_ORDERED_SET_ONLY}."),
    lambda toks: _WithinGroup(value=toks[0]),
)
_PRIMARY = _ATOM + pp.Optional(_WITHIN_GROUP) + pp.Group(pp.ZeroOrMore(pp.Group(_FILTER_CLAUSE))) + pp.Optional(_WINDOW)


def _ordered_set(call: Any, within: _WithinGroup) -> AggCall:
    """SQL ``PERCENTILE_CONT|DISC(p) WITHIN GROUP (ORDER BY x)`` as ``percentile|percentile_disc(x, p=p)``."""
    name = call.words[0].name.lower() if isinstance(call, _PendingCall) and len(call.words) == 1 else None
    if name not in _ORDERED_SET_CALLS:
        raise _Problem(f"{_ORDERED_SET_ONLY}.")
    try:
        args, _, distinct = _split_args(call.items, call=name)
    except _Problem:
        raise _Problem(f"{_ORDERED_SET_ONLY}.") from None
    if distinct or len(args) != 1 or isinstance(args[0], NamedArg):
        raise _Problem(f"{_ORDERED_SET_ONLY}.")
    return AggCall(source=within.value, agg=_ORDERED_SET_CALLS[name], args=(NamedArg(name="p", value=args[0]),))


def _is_window_function(node: Any) -> bool:
    return (isinstance(node, _PendingCall) and len(node.words) == 1 and not node.words[0].quoted
            and node.words[0].name.lower() in WINDOW_FUNCTION_ARITIES)


def _window_function(call: _PendingCall) -> ScalarCall:
    """An Ossie ranking or offset call under ``OVER``, undispatched."""
    word = call.words[0]
    args, _, distinct = _split_args(call.items, call=word.name)
    if distinct:
        raise _Problem(f"{_DISTINCT_ONLY}.")
    _reject_bare_star_args(args, (), func_name=word.name, original=_STATE.text)
    _record_call(call)
    return ScalarCall(name=word.name.lower(), args=args, written=word.name)


def _primary_node(toks: List[Any]) -> Any:
    window = toks[-1] if isinstance(toks[-1], _Window) else None
    items = toks[:-1] if window is not None else toks
    if window is not None and not isinstance(items[0], _PendingCall):
        raise _Problem("OVER (…) applies only to a function call, e.g. ROW_NUMBER() OVER (ORDER BY x).")
    if window is not None and len(items) == 2 and _is_window_function(items[0]):
        node = _window_function(items[0])
    else:
        node = _ordered_set(items[0], items[1]) if len(items) == 3 else _resolve(items[0])
    node = _with_filters(node, items[-1])
    if window is None:
        return node
    return WindowCall(function=node, partition=window.partition, order=window.order, frame=window.frame)


def _with_filters(node: Any, clauses: List[Any]) -> Any:
    for clause in clauses:
        if isinstance(node, (Ref, DottedRef)) and node.evaluated:
            raise AggFilterError(expression=_STATE.text)
        if not isinstance(node, AggCall):
            raise _Problem("FILTER (WHERE …) applies only to an aggregation, e.g. sum(x) FILTER (WHERE c).")
        if node.filter is not None:
            raise _Problem("an aggregation takes one FILTER (WHERE …) clause.")
        node = node.model_copy(update={"filter": clause[0]})
    return node


_on(_PRIMARY, _primary_node)


def _fold_casts(toks: pp.ParseResults) -> Any:
    items = list(toks[0])
    _spelled("::")
    node = items[0]
    for type_text in items[1:]:
        node = Cast(source=node, type_text=type_text)
    return node


def _signed(toks: pp.ParseResults) -> Any:
    sign, node = toks[0]
    return UnaryOp(op="-", operand=node) if sign == "-" else node


def _pow(toks: pp.ParseResults) -> Any:
    _spelled("**")
    left, right = toks[0]
    return ScalarCall(name="pow", args=(left, right))


def _fold_arith(toks: pp.ParseResults) -> Any:
    items = list(toks[0])
    node = items[0]
    for op, right in zip(items[1::2], items[2::2]):
        node = _interval_as_date_add(op=op, left=node, right=right) or Arith(op=op, left=node, right=right)
    return node


_BARE_ARROW = (
    "'->' yields a JSON value, which Mode B does not read; end the chain with ->> to read the value as text, "
    "e.g. payload -> 'a' ->> 'b'."
)


def _arrow_steps(operand: Any) -> Tuple[Any, ...]:
    """The path steps one ``->`` / ``->>`` operand adds: a ``'$…'`` path, a key, or an array index."""
    value = operand.value if isinstance(operand, Literal) else None
    try:
        if isinstance(value, str):
            return parse_json_path(value).steps if value.startswith("$") else (check_key(value),)
    except JsonPathError as e:
        raise _Problem(f"{e}.") from None
    if isinstance(value, Decimal) and value == value.to_integral_value() and value >= 0:
        return (int(value),)
    raise _Problem(
        "the right operand of -> / ->> must be a literal key ('a'), a non-negative array index (0) or a "
        "'$…' path."
    )


def _fold_concat(toks: pp.ParseResults) -> Any:
    """``||`` concatenation and ``->`` / ``->>`` JSON reads share one left-associative level, as in Postgres."""
    items = list(toks[0])
    node = items[0]
    chain: Optional[Tuple[Any, Tuple[Any, ...]]] = None
    for op, right in zip(items[1::2], items[2::2]):
        if op == "||":
            if chain is not None:
                raise _Problem(_BARE_ARROW)
            node = ScalarCall(name="concat", args=(node, right))
            continue
        _spelled(op)
        base, steps = chain if chain is not None else (node, ())
        steps = (*steps, *_arrow_steps(right))
        if op == "->":
            chain = (base, steps)
        else:
            node, chain = ScalarCall(name="json_value", args=(base, Literal(value=json_path_text(steps)))), None
    if chain is not None:
        raise _Problem(_BARE_ARROW)
    return node


_FLOORDIV = _sym("//", "'//'") + _fail("'//' is not an operator; use div(a, b) or floor(a / b).")
_PIPE = _sym(r"\|(?!\|)", "'|'") + _fail("'|' is not an operator; use || to concatenate strings.")
# Parentheses are atoms (_PAREN), so infix_notation's own grouping is disabled.
_concat_level <<= pp.infix_notation(_PRIMARY, [
    (DCOLON + _need(_TYPE_SPEC, "'::' needs a type."), 1, pp.OpAssoc.LEFT, _fold_casts),
    (pp.Suppress(_sym(r"\*\*", "'**'")), 2, pp.OpAssoc.RIGHT, _pow),
    (_sym(r"\+|-(?!>)", "'-' or '+'"), 1, pp.OpAssoc.RIGHT, _signed),
    (_FLOORDIV | _sym(r"\*(?!\*)|/(?!/)|%", "'*', '/' or '%'"), 2, pp.OpAssoc.LEFT, _fold_arith),
    (_sym(r"\+|-(?!>)", "'+' or '-'"), 2, pp.OpAssoc.LEFT, _fold_arith),
    (_PIPE | _sym(r"\|\||->>|->", "'||', '->' or '->>'"), 2, pp.OpAssoc.LEFT, _fold_concat),
], lpar=pp.NoMatch(), rpar=pp.NoMatch())  # pyright: ignore[reportArgumentType] — unannotated; documented to accept a ParserElement

# -- comparison level ----------------------------------------------------------

_CMP_OPS = {"=": "==", "==": "==", "<>": "!=", "!=": "!=", "<": "<", "<=": "<=", ">": ">", ">=": ">="}
_IS = pp.Suppress(_kw("is"))
_NOT_FLAG = _on(pp.Optional(_kw("not")), lambda toks: bool(len(toks)))


class _Suffix:
    """A comparison-level suffix: builds the predicate node from its left operand."""

    __slots__ = ("build",)

    def __init__(self, build) -> None:
        self.build = build


def _suffix(element: pp.ParserElement, build) -> pp.ParserElement:
    return _on(element, lambda toks: _Suffix(lambda left: build(left, toks)))


def _is_op(negate: bool) -> str:
    return _IS_NOT if negate else "is"


def _negated(node: Any, negate: bool) -> Any:
    return UnaryOp(op="not", operand=node) if negate else node


def _truth_test(left: Any, toks: List[Any]) -> Any:
    _spelled("IS TRUE / IS FALSE")
    negate, word = toks[0], toks[1].lower()
    known = Cmp(op=_is_op(True), left=left, right=Literal(value=None))
    test = BoolOp(op="and", operands=(known, left if word == "true" else UnaryOp(op="not", operand=left)))
    return _negated(test, negate)


def _regex_operator(left: Any, toks: List[Any]) -> Any:
    """``~`` / ``~*`` / ``!~`` / ``!~*`` as ``[NOT] regexp_like(left, p[, 'i'])``."""
    op, pattern = toks
    _spelled(op)
    flags = (Literal(value="i"),) if op.endswith("*") else ()
    return _negated(ScalarCall(name="regexp_like", args=(left, pattern, *flags)), op.startswith("!"))


class _ListItems:
    """An ``IN`` right-hand side of lists: each item is one list the left side may equal."""

    __slots__ = ("items",)

    def __init__(self, items: List[Any]) -> None:
        self.items = items


def _in_rhs_elements(items: List[Any]) -> TupleLit | _ListItems:
    if not items:
        raise _Problem(
            "an empty list is not allowed on the right-hand side of in / not in (dialect-dependent SQL); "
            "use a non-empty literal list."
        )
    if any(isinstance(item, ArrayLit) for item in items):
        if not all(isinstance(item, (ArrayLit, Placeholder)) for item in items):
            raise _Problem(
                "the right-hand side of in / not in mixes lists with scalars; write every item as a list, "
                "e.g. tags in (['a'], ['b', 'c'])."
            )
        return _ListItems(items)
    elements = [_in_scalar_item(item) for item in items]
    if any(isinstance(item, Placeholder) for item in elements):
        # Each variable's value decides once bound whether its item is a list or a scalar.
        return _ListItems(elements)
    return TupleLit(elements=tuple(cast(Literal, item) for item in elements))


def _in_scalar_item(item: Any) -> Literal | Placeholder:
    if (isinstance(item, UnaryOp) and item.op == "-" and isinstance(item.operand, Literal)
            and isinstance(item.operand.value, Decimal)):
        item = Literal(value=-item.operand.value)
    if not isinstance(item, (Literal, Placeholder)):
        raise _Problem(
            "every element on the right-hand side of in / not in must be a literal (string, number, or "
            f"boolean); got {type(item).__name__}."
        )
    return item


def _in_comparison(left: Any, negate: bool, right: Any) -> Any:
    """``L IN (M1, M2)`` over lists is ``L = M1 OR L = M2``."""
    if not isinstance(right, _ListItems):
        node = Cmp(op=_NOT_IN if negate else "in", left=left, right=right)
        return _written_comparison(node) if isinstance(right, TupleLit) else node
    equalities = tuple(Cmp(op="==", left=left, right=item) for item in right.items)
    return _negated(equalities[0] if len(equalities) == 1 else BoolOp(op="or", operands=equalities), negate)


_IN_LIST = ((LPAR + _PLACEHOLDER_ATOM + RPAR)
            | _PLACEHOLDER_ATOM
            | _on((LPAR + _SEQ_ITEMS + RPAR), lambda toks: _in_rhs_elements(list(toks[0])))
            | _on((LBRACK + _SEQ_ITEMS + RBRACK), lambda toks: _spelled_call("IN […]", _in_rhs_elements(list(toks[0]))))
            | _STRING.copy().add_parse_action(lambda: _spelled("IN '…'"))
            | _fail("the right-hand side of in / not in must be a literal list, e.g. status in ('a', 'b')."))

_CMP_SUFFIX = (
    # SQL ``v = ANY(list)`` is list membership.
    _suffix(_sym(r"==|=", "comparison") + pp.Suppress(_kw("any")) + LPAR + _concat_level + RPAR,
            lambda left, t: _spelled_call("= ANY(…)", ScalarCall(name="contains", args=(t[1], left))))
    | _suffix(_sym(r"==|!=|<>|<=|>=|=|<|>", "comparison") + _concat_level,
            lambda left, t: _written_comparison(Cmp(op=_CMP_OPS[t[0]], left=left, right=t[1], single_eq=t[0] == "=")))
    | _suffix(_IS + _NOT_FLAG + pp.Suppress(_kw("distinct")) + pp.Suppress(_kw("from")) + _concat_level,
              lambda left, t: Cmp(op="is not distinct from" if t[0] else "is distinct from", left=left, right=t[1]))
    | _suffix(_IS + _NOT_FLAG + pp.Suppress(_kw("null") | pp.Regex(r"None\b")),
              lambda left, t: Cmp(op=_is_op(t[0]), left=left, right=Literal(value=None)))
    | _suffix(_IS + _NOT_FLAG + (_kw("true", "false")), _truth_test)
    | _suffix(_IS + _NOT_FLAG + _concat_level,
              lambda left, t: _spelled_call("IS <value>", Cmp(op=_is_op(t[0]), left=left, right=t[1])))
    | _suffix(_NOT_FLAG + pp.Suppress(_kw("in")) + _IN_LIST, lambda left, t: _in_comparison(left, t[0], t[1]))
    | _suffix(_NOT_FLAG + pp.Suppress(_kw("between")) + _concat_level + pp.Suppress(_kw("and")) + _concat_level,
              lambda left, t: _negated(BoolOp(op="and", operands=(
                  Cmp(op=">=", left=left, right=t[1]), Cmp(op="<=", left=left, right=t[2]),
              )), t[0]))
    | _suffix(_NOT_FLAG + _kw("like", "ilike") + _concat_level,
              lambda left, t: _negated(ScalarCall(name=t[1].lower(), args=(left, t[2])), t[0]))
    | _suffix(_sym(r"!~\*|!~|~\*|~", "regex operator") + _concat_level, _regex_operator)
    | (_NOT_FLAG + _kw("similar") + _kw("to")) + _fail(
        "SIMILAR TO is not supported; use LIKE, ILIKE or a regex (x ~ 'p', regexp_like(x, 'p')).")
    | (_kw("regexp", "rlike") + _fail(
        "the REGEXP / RLIKE operators are not supported; use x ~ 'p' or regexp_like(x, 'p')."))
)


def _comparison_node(toks: pp.ParseResults) -> Any:
    left, *suffixes = toks[0]
    if len(suffixes) > 1:
        raise _Problem(
            "chained comparisons are not supported. Each comparison must be a single comparison; split "
            "(e.g.) `1 < x < 10` into `1 < x and x < 10`."
        )
    return suffixes[0].build(left) if suffixes else left




def _not(toks: pp.ParseResults) -> Any:
    return UnaryOp(op="not", operand=toks[0][1])


def _bool(word: str):
    return lambda toks: BoolOp(op=word, operands=tuple(toks[0]))


_PREDICATE = pp.infix_notation(_concat_level, [
    (_CMP_SUFFIX, 1, pp.OpAssoc.LEFT, _comparison_node),
    (_kw("not"), 1, pp.OpAssoc.RIGHT, _not),
    (pp.Suppress(_kw("and")), 2, pp.OpAssoc.LEFT, _bool("and")),
    (pp.Suppress(_kw("or")), 2, pp.OpAssoc.LEFT, _bool("or")),
], lpar=pp.NoMatch(), rpar=pp.NoMatch())  # pyright: ignore[reportArgumentType] — unannotated; documented to accept a ParserElement
_TERNARY = _kw("if") + _fail(
    "the Python conditional 'x if cond else y' is not supported; use CASE WHEN cond THEN x ELSE y END or iif(cond, x, y).",
)
expr <<= _PREDICATE + pp.Optional(_TERNARY)
_EXPRESSION = (expr + pp.StringEnd().set_fail_action(_record_failure)).parse_with_tabs()
# A whole-text call with each argument's source offsets.
_LOCATED_ARG = pp.Group(pp.Located(_ANY_ARG))
_LOCATED_CALL = (
    pp.Group(pp.Located(_PATH | _PLACEHOLDER)) + LPAR
    + pp.Group(pp.Optional(_LOCATED_ARG + pp.ZeroOrMore(COMMA + _LOCATED_ARG) + pp.Optional(COMMA))) + RPAR
    + pp.StringEnd()
).parse_with_tabs()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


class _Reading(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    parsed: Any
    legacy: Tuple[LegacySpan, ...]
    calls: Tuple[CallSpan, ...]
    spellings: Tuple[str, ...] = ()


def parse_expr(text: str, *, windows: bool = False) -> ParsedExpr:
    """Parse a Mode-B expression string into a ``ParsedExpr``; a deprecated spelling read is noted for its warning.
    ``windows`` reads ``OVER (…)`` as a ``WindowCall`` (the strict reader's mode).

    Raises ``ValueError`` (empty / syntax / unsupported form / reserved prefix),
    ``UnknownFunctionError``, or ``IllegalWindowInFilterError`` (raw ``OVER(...)`` outside ``windows``).
    """
    reading = _read(text, windows)
    if reading.legacy:
        note_legacy_spellings(reading.legacy)
    return reading.parsed


@lru_cache(maxsize=8192)
def reference_spans(text: str) -> Tuple[ParsedExpr, Mapping[int, Tuple[int, int]]]:
    """``text`` parsed, with the offsets of its reference nodes (``Ref``, ``DottedRef``, ``*``) keyed by node id.

    Raises ``ValueError`` when ``text`` does not parse.
    """
    with _PARSE_LOCK:
        _STATE.reset(text)
        _STATE.refs = {}
        try:
            parsed = cast(ParsedExpr, _parse_tree(text))
            found = _STATE.refs
        finally:
            _STATE.refs = None
    return parsed, MappingProxyType(
        {key: (start, start + len(text[start:end].rstrip())) for key, (_node, start, end) in found.items()})


class LiteralComparison(BaseModel):
    """A string literal ``value`` at ``text[start:end]`` compared directly with the reference ``parts`` (per-segment
    ``quoted``)."""

    model_config = ConfigDict(frozen=True)

    value: str
    start: int
    end: int
    parts: Tuple[str, ...]
    quoted: Tuple[bool, ...]


def _reference_reading(node: Any) -> Optional[Tuple[Tuple[str, ...], Tuple[bool, ...]]]:
    if isinstance(node, Ref) and not node.evaluated:
        return (node.name,), (node.quoted,)
    if isinstance(node, DottedRef) and not node.evaluated:
        return node.parts, tuple(node.quoted_at(i) for i in range(len(node.parts)))
    return None


def _compared_literals(node: Cmp) -> Iterator[Tuple[Any, Any]]:
    """``(literal, reference)`` per string literal ``node`` compares directly with a reference."""
    if node.op in _IN_OPS:
        if isinstance(node.right, TupleLit):
            yield from ((e, node.left) for e in node.right.elements)
        return
    yield from ((node.right, node.left), (node.left, node.right))


@lru_cache(maxsize=8192)
def literal_comparisons(text: str) -> Tuple[LiteralComparison, ...]:
    """Each string literal ``text`` compares directly with a column reference — ``=``, ``==``, ``!=``, ``<>``, ``<``,
    ``<=``, ``>``, ``>=`` either way round, or an element of an all-literal ``[NOT] IN`` list — in source order; none
    when ``text`` does not parse."""
    with _PARSE_LOCK:
        _STATE.reset(text)
        _STATE.literals, _STATE.compared = {}, {}
        try:
            parsed = _EXPRESSION.parse_string(_mask_placeholders(text), parse_all=True)[0]
            literals, compared = _STATE.literals, _STATE.compared
        except (ValueError, pp.ParseBaseException):
            return ()
        finally:
            _STATE.literals = _STATE.compared = None
    out: List[LiteralComparison] = []
    written = (n for n in walk_parsed(parsed) if isinstance(n, Cmp) and compared.get(id(n)) is n)
    for cmp in written:
        for literal, other in _compared_literals(cmp):
            reading = _reference_reading(other)
            located = literals.get(id(literal))
            if reading is not None and located is not None and located[0] is literal and isinstance(literal.value, str):
                out.append(LiteralComparison(
                    value=literal.value, start=located[1], end=located[2], parts=reading[0], quoted=reading[1]))
    return tuple(sorted(out, key=lambda c: c.start))


def legacy_spans(text: str, *, windows: bool = False) -> Tuple[LegacySpan, ...]:
    """The deprecated spellings in template ``text``, each with its canonical twin; none when it does not parse."""
    try:
        return _read(_bound_strings(text), windows).legacy
    except Exception:  # noqa: BLE001 — unparseable text has no spans
        return ()


def nonossie_spellings(text: str, *, windows: bool = False) -> Tuple[str, ...]:
    """The spellings Ossie Layer 1 does not define that template ``text`` uses and the parser rewrites (``~``, ``->>``,
    ``::``, …); none when it does not parse."""
    try:
        return _read(_bound_strings(text), windows).spellings
    except Exception:  # noqa: BLE001 — unparseable text has no spellings
        return ()


def call_spans(text: str, *, windows: bool = False) -> Tuple[CallSpan, ...]:
    """The single-name calls in template ``text``; none when it does not parse."""
    try:
        return _read(_bound_strings(text), windows).calls
    except Exception:  # noqa: BLE001 — unparseable text has no spans
        return ()


def unwrap_agg_calls(text: str) -> str:
    """``text`` with each ``AGG(x)`` call (any case) replaced by ``x``, every other byte kept."""
    out: List[str] = []
    last = 0
    for call in call_spans(text):
        if call.name.casefold() != "agg" or call.arguments != 1 or call.start < last:
            continue
        out += [text[last:call.start], text[call.paren + 1:call.end - 1].strip()]
        last = call.end
    return "".join([*out, text[last:]])


#: Parenthesis depth past which an expression is refused: deeper ones exhaust the parser's recursion.
MAX_NESTING = 32


def _nesting(text: str) -> int:
    """The deepest parenthesis nesting in ``text``'s code, literals and comments aside."""
    depth = deepest = 0
    for region in (r for r in _regions(text) if r.kind == "code"):
        for ch in text[region.start:region.end]:
            depth += (ch == "(") - (ch == ")")
            deepest = max(deepest, depth)
    return deepest


def _parse_tree(text: str) -> Any:
    """``text`` parsed by the grammar (the caller holds ``_PARSE_LOCK``); raises ``ValueError`` when it does not parse."""
    if _nesting(text) > MAX_NESTING:
        raise ValueError(f"Invalid Mode-B expression {text!r}: parentheses nest more than {MAX_NESTING} deep.")
    try:
        masked = _mask_placeholders(text)
        if not masked.strip():
            raise ValueError("Empty Mode-B expression.")
        return _EXPRESSION.parse_string(masked, parse_all=True)[0]
    except _Problem as problem:
        raise ValueError(f"Invalid Mode-B expression {text!r}: {problem.detail}") from None
    except pp.ParseBaseException as e:
        raise ValueError(f"Invalid Mode-B expression {text!r}: {_syntax_message(text, e)}") from None
    except RecursionError:
        raise ValueError(f"Invalid Mode-B expression {text!r}: it nests too deeply to read.") from None


@lru_cache(maxsize=8192)
def _read(text: str, windows: bool) -> _Reading:
    try:
        return _reading(text, windows=windows)
    except RecursionError:
        raise ValueError(f"Invalid Mode-B expression {text!r}: it nests too deeply to read.") from None


def _reading(text: str, *, windows: bool) -> _Reading:
    if not text or not text.strip():
        raise ValueError("Empty Mode-B expression.")
    with _PARSE_LOCK:
        _STATE.reset(text)
        _STATE.windows = windows
        parsed = cast(ParsedExpr, _parse_tree(text))
        methods, calls = list(_STATE.methods.values()), sorted(_STATE.calls.values(), key=lambda c: c.start)
        spellings = tuple(_STATE.spellings)
    legacy = tuple(sorted([*_text_legacy_spans(text), *methods], key=lambda s: (s.start, s.end)))
    if any(_is_interval(n) for n in walk_parsed(parsed)):
        parsed = _deferred_intervals(parsed, message=(
            f"Invalid Mode-B expression {text!r}: interval(n, unit) is only valid as "
            f"`x + interval(n, unit)`, `interval(n, unit) + x` or `x - interval(n, unit)`; "
            f"otherwise use date_add(x, n, unit)."
        ))
    return _Reading(parsed=parsed, legacy=legacy, calls=tuple(calls), spellings=spellings)


_IS_NONE_RE = re.compile(r"\b[Ii][Ss]\s+(?:([Nn][Oo][Tt])\s+)?None\b")
_NONE_RE = re.compile(r"(?<![\w.])None\b(?![.(])")
_IN_LIST_RE = re.compile(r"\b[Ii][Nn]\s*\[")


def _text_legacy_spans(text: str) -> List[LegacySpan]:
    """``==``, ``is [not] None``, bare ``None`` and ``[…]`` after ``IN`` in the code of ``text``."""
    regions = _regions(text)
    out: List[LegacySpan] = []
    for region in (r for r in regions if r.kind == "code"):
        chunk = text[region.start:region.end]
        out += _none_spans(chunk, offset=region.start)
        out += [LegacySpan(kind="==", start=region.start + m.start(), end=region.start + m.end(), written="==", twin="=")
                for m in re.finditer("==", chunk)]
        for m in _IN_LIST_RE.finditer(chunk):
            out += _bracket_list_spans(text, regions=regions, open_at=region.start + m.end() - 1)
    return out


def _none_spans(chunk: str, *, offset: int) -> List[LegacySpan]:
    """``is [not] None`` and bare ``None`` in code ``chunk`` starting at ``offset``."""
    is_none = list(_IS_NONE_RE.finditer(chunk))
    out = [
        LegacySpan(kind="is not None" if m.group(1) else "is None", start=offset + m.start(), end=offset + m.end(),
                   written=m.group(0), twin="IS NOT NULL" if m.group(1) else "IS NULL")
        for m in is_none
    ]
    covered = [(m.start(), m.end()) for m in is_none]
    out += [LegacySpan(kind="bare None", start=offset + m.start(), end=offset + m.end(), written="None", twin="NULL")
            for m in _NONE_RE.finditer(chunk) if not any(a <= m.start() < b for a, b in covered)]
    return out


def _bracket_list_spans(text: str, *, regions: List[_Region], open_at: int) -> List[LegacySpan]:
    """The ``[`` at ``open_at`` and its matching ``]``, each respelled as a parenthesis."""
    depth = 0
    for region in (r for r in regions if r.kind == "code" and r.end > open_at):
        for i in range(max(region.start, open_at), region.end):
            depth += {"[": 1, "]": -1}.get(text[i], 0)
            if depth == 0:
                return [LegacySpan(kind="list after IN", start=open_at, end=open_at + 1, written="[", twin="("),
                        LegacySpan(kind="list after IN", start=i, end=i + 1, written="]", twin=")")]
    return []


def _deferred_intervals(node: Any, *, message: str, memo: Optional[Dict[int, Any]] = None) -> Any:
    """``node`` with each standalone ``interval(…)`` a deferred aggregation candidate; raises ``message`` for one
    that cannot be. ``memo`` rewrites a node shared by ``args`` and ``written_args`` once."""
    memo = {} if memo is None else memo
    if isinstance(node, tuple):
        return tuple(_deferred_intervals(item, message=message, memo=memo) for item in node)
    if not isinstance(node, _BaseNode):
        return node
    if id(node) in memo:
        return memo[id(node)]
    if isinstance(node, ScalarCall) and _is_interval(node):
        deferred = _aggregation_candidate(node.written or "interval", args=node.args, kwargs=(), error=message)
        if deferred is None:
            raise ValueError(message)
        memo[id(node)] = deferred
        return deferred
    update = {f: _deferred_intervals(getattr(node, f), message=message, memo=memo) for f in type(node).model_fields}
    memo[id(node)] = node.model_copy(update=update)
    return memo[id(node)]


def _syntax_message(text: str, error: pp.ParseBaseException) -> str:
    pos = max(_STATE.farthest, error.loc)
    if pos >= len(text.rstrip()):
        return f"unexpected end of expression at column {pos + 1}."
    return f"syntax error at column {pos + 1}, near {text[pos:pos + 12]!r}."


# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------


def placeholder_names(parsed: Any) -> List[str]:
    """Names of every ``Placeholder`` in a parsed tree, in walk order."""
    return [n.name for n in walk_parsed(parsed) if isinstance(n, Placeholder)]


_ANY_BRACE_RE = re.compile(r"\{\{|\}\}|\{([^}]*)\}")
_DIGIT_BRACES_RE = re.compile(r"[0-9, ]*")
_NUMBER_TEXT_RE = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")


def _lookups(text: str, regions: List[_Region]) -> Iterator[Tuple["re.Match[str]", bool]]:
    """Each brace pair binding looks up, with whether it sits in a string literal."""
    for region in regions:
        in_string = region.kind == "string"
        for m in _ANY_BRACE_RE.finditer(text, region.start, region.end):
            if m.group(0) not in ("{{", "}}") and not _literal_braces(m, in_string=in_string):
                yield m, in_string


#: The regions whose ``{name}`` binds (a comment's and a quoted identifier's text is inert).
_BINDING_KINDS = ("code", "string")


def has_bindings(text: str) -> bool:
    """Whether ``bind_variables`` reads ``text``: a variable or a doubled-brace escape."""
    return any(_ANY_BRACE_RE.search(text, r.start, r.end) for r in _regions(text) if r.kind in _BINDING_KINDS)


def _bound_strings(text: str) -> str:
    """``text`` with each string-literal variable read as a same-length run of zeros, as binding parses a template."""
    chars = list(text)
    for m, _ in _lookups(text, [r for r in _regions(text) if r.kind == "string"]):
        if re.fullmatch(_VAR_NAME, m.group(1)):
            chars[m.start():m.end()] = "0" * (m.end() - m.start())
    return "".join(chars)


def parse_template(text: str, *, windows: bool = False) -> ParsedExpr:
    """``parse_expr`` of ``text`` before its string-literal variables are bound: the shape binding parses."""
    return parse_expr(_bound_strings(text), windows=windows)


def variable_names(text: str) -> List[str]:
    """The variables binding ``text`` looks up, in text order."""
    names: Dict[str, None] = {}
    for m, _ in _lookups(text, [r for r in _regions(text) if r.kind in _BINDING_KINDS]):
        if re.fullmatch(_VAR_NAME, m.group(1)):
            names[m.group(1)] = None
    return list(names)


def bind_variables(text: str, variables: Mapping[str, Any], *, windows: bool = False) -> str:
    """``text`` with every ``{name}`` bound as a literal value: inside a string literal its text,
    in a bare literal slot a number or boolean, as a whole ``IN`` list a list. The template is
    parsed first, so no value can change the expression's structure."""
    if not _ANY_BRACE_RE.search(text):
        return text
    try:
        regions = [r for r in _regions(text) if r.kind in _BINDING_KINDS]
    except _Problem as problem:
        raise ValueError(f"Invalid Mode-B expression {text!r}: {problem.detail}") from None
    for region in regions:
        chunk = text[region.start:region.end]
        if region.kind == "code" and ("{?" in chunk or "?}" in chunk):
            raise ValueError(f"{_OPTIONAL_BLOCKS} Got: {text!r}.")
    for m, in_string in _lookups(text, regions):
        _check_variable(m, text=text, variables=variables, in_string=in_string)
    # Strings bound and ``{{`` / ``}}`` outside strings read as braces: the grammar judges the variable slots.
    template = parse_expr(_spliced(text, variables=variables, regions=regions, kinds=("string",), code_braces=True),
                          windows=windows)
    text_slots = _text_slot_ordinals(template, variables=variables)
    return _spliced(
        text, variables=variables, regions=regions, kinds=("string", "code"), code_braces=False, text_slots=text_slots,
        in_slots=_in_slot_ordinals(template),
    )


def _in_slot_ordinals(template: Any) -> frozenset[int]:
    """Textual ordinals of the placeholders forming a whole ``IN`` right-hand side: left for ``parse_bound_expr``."""
    starts = sorted({n.start for n in walk_parsed(template) if isinstance(n, Placeholder)})
    return frozenset(
        starts.index(node.right.start)
        for node in walk_parsed(template)
        if isinstance(node, Cmp) and node.op in _IN_OPS and isinstance(node.right, Placeholder)
    )


def parse_bound_expr(text: str, variables: Mapping[str, Any], *, windows: bool = False) -> ParsedExpr:
    """``parse_expr`` of ``text``, each placeholder ``bind_variables`` left as a whole ``IN`` right-hand side bound to
    its value in ``variables``."""
    return _with_in_variables(parse_expr(text, windows=windows), variables=variables)


def _with_in_variables(node: Any, *, variables: Mapping[str, Any]) -> Any:
    if isinstance(node, tuple):
        mapped = tuple(_with_in_variables(n, variables=variables) for n in node)
        return node if all(a is b for a, b in zip(mapped, node)) else mapped
    if not isinstance(node, _BaseNode):
        return node
    if (isinstance(node, Cmp) and node.op in _IN_OPS and isinstance(node.right, Placeholder)
            and node.right.name in variables):
        value = variables[node.right.name]
        listed = isinstance(value, (list, tuple))
        bound = BoundVariable(name=node.right.name, values=tuple(value) if listed else (value,), listed=listed)
        return node.model_copy(update={"left": _with_in_variables(node.left, variables=variables), "right": bound})
    update = {f: _with_in_variables(getattr(node, f), variables=variables) for f in type(node).model_fields}
    changed = {f: v for f, v in update.items() if v is not getattr(node, f)}
    return node.model_copy(update=changed) if changed else node


def _text_slot_ordinals(template: Any, *, variables: Mapping[str, Any]) -> frozenset[int]:
    """Textual ordinals of the placeholders filling a pattern / path / replacement slot, checked here so an
    error names the variable."""
    starts = sorted({n.start for n in walk_parsed(template) if isinstance(n, Placeholder)})
    return frozenset(
        starts.index(placeholder.start)
        for node in walk_parsed(template)
        if isinstance(node, ScalarCall) and node.name in STRING_SLOTS
        for placeholder in _checked_slot_placeholders(node, variables=variables)
    )


def _checked_slot_placeholders(call: ScalarCall, *, variables: Mapping[str, Any]) -> List[Placeholder]:
    """``call``'s text-slot placeholders, each value checked against its slot."""
    found: List[Placeholder] = []
    pattern: Optional[RegexPattern] = None
    for pos in STRING_SLOTS[call.name]:
        arg = call.args[pos] if pos < len(call.args) else None
        value = _slot_text(call, pos=pos, arg=arg, variables=variables)
        if value is None:
            continue
        variable = arg.name if isinstance(arg, Placeholder) else None
        pattern = _check_slot(call.name, pos=pos, value=value, variable=variable, pattern=pattern)
        if isinstance(arg, Placeholder):
            found.append(arg)
    return found


def _slot_text(call: ScalarCall, *, pos: int, arg: Any, variables: Mapping[str, Any]) -> Optional[str]:
    """The text filling a slot: a variable's value (which must be text) or a string literal's."""
    if not isinstance(arg, Placeholder):
        value = getattr(arg, "value", None)
        return value if isinstance(value, str) else None
    value = variables.get(arg.name)
    if not isinstance(value, str):
        raise VariableError(
            f"Variable '{arg.name}' fills the {slot_name(call.name, pos)} of {call.name}() and must hold text; "
            f"got {type(value).__name__}."
        )
    return value


def _check_slot(
    fn: str, *, pos: int, value: str, variable: Optional[str], pattern: Optional[RegexPattern],
) -> Optional[RegexPattern]:
    """Checks one slot's text; returns the call's pattern so far."""
    if fn == "json_value":
        if variable:
            check_path(fn, value, variable=variable)
        return pattern
    if pos == 1:
        return check_pattern(fn, value, variable=variable)
    if variable and pattern is not None:
        check_replacement(fn, value, pattern=pattern, variable=variable)
    return pattern


def _spliced(
    text: str, *, variables: Mapping[str, Any], regions: List[_Region], kinds: Tuple[str, ...], code_braces: bool,
    text_slots: frozenset[int] = frozenset(), in_slots: frozenset[int] = frozenset(),
) -> str:
    out: List[str] = []
    cursor = 0
    ordinal = count()
    for region in regions:
        out.append(text[cursor:region.start])
        chunk = text[region.start:region.end]
        if region.kind in kinds:
            in_string = region.kind == "string"
            chunk = _ANY_BRACE_RE.sub(
                partial(
                    _bound_text, text=text, region_start=region.start, in_string=in_string, variables=variables,
                    text_slots=text_slots, in_slots=in_slots, ordinal=ordinal,
                ),
                chunk,
            )
        elif code_braces:
            chunk = chunk.replace("{{", "{").replace("}}", "}")
        out.append(chunk)
        cursor = region.end
    out.append(text[cursor:])
    return "".join(out)


def _literal_braces(match: "re.Match[str]", *, in_string: bool) -> bool:
    """A string-literal brace pair holding only digits, commas and spaces (``{3}``, ``{1, 2}``) is text."""
    return in_string and match.group(1) is not None and _DIGIT_BRACES_RE.fullmatch(match.group(1)) is not None


def _check_variable(match: "re.Match[str]", *, text: str, variables: Mapping[str, Any], in_string: bool) -> None:
    name = match.group(1)
    if not re.fullmatch(_VAR_NAME, name):
        raise VariableError(
            f"Invalid variable name '{name}' in filter: {text!r}. "
            f"Variable names must contain only letters, digits, and underscores."
        )
    if name not in variables:
        hint = " For literal braces in a string (e.g. a regex), write {{ and }}." if in_string else ""
        raise VariableError(
            f"Undefined variable '{name}' in filter: {text!r}. Available variables: {sorted(variables)}.{hint}"
        )


def _bound_text(
    match: "re.Match[str]", *, text: str, region_start: int, in_string: bool, variables: Mapping[str, Any],
    text_slots: frozenset[int], ordinal: Iterator[int], in_slots: frozenset[int] = frozenset(),
) -> str:
    bound = _bound_value_text(
        match, in_string=in_string, variables=variables, text_slots=text_slots, ordinal=ordinal, in_slots=in_slots,
    )
    start = region_start + match.start()
    # A negative value right after a ``-`` would make a ``--`` comment.
    touches_minus = not in_string and start > 0 and text[start - 1] == "-" and bound.startswith("-")
    return " " + bound if touches_minus else bound


def _bound_value_text(
    match: "re.Match[str]", *, in_string: bool, variables: Mapping[str, Any], text_slots: frozenset[int],
    ordinal: Iterator[int], in_slots: frozenset[int],
) -> str:
    whole = match.group(0)
    if whole in ("{{", "}}"):
        return whole[0]
    if _literal_braces(match, in_string=in_string):
        return whole
    name = match.group(1)
    value = variables[name]
    if in_string:
        return value_text(name, value).replace("'", "''")
    position = next(ordinal)
    if position in in_slots:
        return whole
    if position in text_slots:
        return _literal_text(name, value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_literal_text(name, v) for v in value) + "]"
    return bare_literal_text(name, value)


def value_text(name: str, value: Any) -> str:
    """A scalar variable value's text inside a string literal."""
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return _number_text(name, value)
    raise ValueError(f"Variable '{name}' must be a string, number, or list/tuple, got {type(value).__name__}")


def _number_text(name: str, value: int | float) -> str:
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"Variable '{name}' must be finite, got {value!r}")
    return str(value)


def _literal_text(name: str, value: Any) -> str:
    """One value as a literal: a string quoted, a number or boolean as itself."""
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, (list, tuple)):
        raise ValueError(f"{_LITERAL_VALUES}: variable '{name}' holds a nested list; a list's elements are scalars.")
    return value_text(name, value)


def bare_literal_text(name: str, value: Any) -> str:
    """A variable value spelled for a bare literal slot: a number or boolean, a numeric string as its number."""
    if isinstance(value, str):
        stripped = value.strip()
        if _NUMBER_TEXT_RE.fullmatch(stripped):
            return stripped
        raise ValueError(
            f"{_LITERAL_VALUES}: variable '{name}' holds the text {value!r} in a number or boolean slot; "
            f"put the placeholder inside quotes ('{{{name}}}') to use it as a string."
        )
    return value_text(name, value)


# ---------------------------------------------------------------------------
# Saved-metric dependencies
# ---------------------------------------------------------------------------


class _MetricScope(BaseModel):
    """What a saved metric's names resolve against."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    names: frozenset[str]
    aggregations: frozenset[str]
    fields: Optional[frozenset[str]] = None
    relationship_path: Optional[Callable[[Tuple[str, ...]], bool]] = None


def metric_dependency_problems(
    expressions: List[Tuple[Optional[str], str]], *, aggregations: frozenset[str] = frozenset(),
    fields: Optional[frozenset[str]] = None, relationship_path: Optional[Callable[[Tuple[str, ...]], bool]] = None,
) -> List[Optional[str]]:
    """Per ``(name, expression)``: why the saved metric cannot resolve among its peers, else ``None``.

    Outside an aggregation a bare name must be a peer metric; an aggregation must be built-in
    or one of ``aggregations``, and, given ``fields``, read only those fields or paths
    ``relationship_path`` accepts; a metric on a cycle, or reading one that cannot resolve, cannot
    resolve either."""
    scope = _MetricScope(names=frozenset(name for name, _ in expressions if name), aggregations=aggregations,
                          fields=fields, relationship_path=relationship_path)
    own: Dict[int, Optional[str]] = {}
    deps: Dict[int, set] = {}
    for i, (_name, expression) in enumerate(expressions):
        try:
            refs, problem = _metric_refs(parse_expr(expression), scope=scope)
        except ValueError as e:
            refs, problem = set(), str(e)
        own[i], deps[i] = problem, refs
    index = {name: i for i, (name, _) in enumerate(expressions) if name}
    out: List[Optional[str]] = []
    for i, (name, _) in enumerate(expressions):
        out.append(_resolution_problem(i, own=own, deps=deps, index=index, path=(name or "",)))
    return out


def _resolution_problem(
    i: int, *, own: Dict[int, Optional[str]], deps: Dict[int, set], index: Dict[str, int], path: Tuple[str, ...],
) -> Optional[str]:
    if own[i] is not None:
        return own[i]
    for dep in sorted(deps[i]):
        if dep in path:
            return f"saved metric {dep!r} has a cyclic reference: {' → '.join((*path, dep))}"
        problem = _resolution_problem(index[dep], own=own, deps=deps, index=index, path=(*path, dep))
        if problem is not None:
            return problem if dep == path[-1] else f"{dep!r} cannot resolve: {problem}"
    return None


def _metric_refs(node: Any, *, scope: _MetricScope) -> Tuple[set, Optional[str]]:
    """The peer metrics ``node`` reads outside aggregations, and the first problem found."""
    if isinstance(node, Ref):
        if node.name in scope.names:
            return {node.name}, None
        match = difflib.get_close_matches(word=node.name, possibilities=sorted(scope.names), n=1)
        hint = f" Did you mean '{match[0]}'?" if match else ""
        return set(), (
            f"'{node.name}' is not a saved metric.{hint} Reference a saved metric by its bare name, or "
            f"aggregate a field (e.g., 'sum({node.name})'). For COUNT(*), use 'count(*)'."
        )
    if isinstance(node, DottedRef):
        dotted = ".".join(node.parts)
        return set(), f"Cross-dataset metric '{dotted}' must include an aggregation (e.g., 'sum({dotted})')."
    if isinstance(node, AggCall):
        known = normalize_aggregation_name(node.agg) in BUILTIN_AGGREGATIONS or node.agg in scope.aggregations
        return set(), _aggregation_input_problem(node, scope=scope) if known else f"unknown aggregation {node.agg!r}"
    children = [node.input] if isinstance(node, TransformCall) else _operands(node)
    refs: set = set()
    for child in children:
        found, problem = _metric_refs(child, scope=scope)
        if problem is not None:
            return refs, problem
        refs |= found
    return refs, None


def _aggregation_input_problem(node: AggCall, *, scope: _MetricScope) -> Optional[str]:
    """The first name an aggregation reads that is neither a field nor a relationship path; unchecked without fields."""
    if scope.fields is None:
        return None
    inputs = [node.source, node.filter, *node.args, *(v for _, v in node.kwargs)]
    for leaf in (ref for value in inputs if value is not None for ref in walk_parsed_refs(value)):
        if isinstance(leaf, Ref) and leaf.name not in scope.fields:
            return f"aggregation {node.agg!r} reads {leaf.name!r}, which is not a field of the dataset"
        if isinstance(leaf, DottedRef) and not (scope.relationship_path is not None and scope.relationship_path(tuple(leaf.parts))):
            return f"aggregation {node.agg!r} reads {'.'.join(leaf.parts)!r}, which no relationship path reaches"
    return None


# ---------------------------------------------------------------------------
# Reference walk
# ---------------------------------------------------------------------------


def walk_parsed_refs(
    parsed: ParsedExpr,
) -> Iterator[Ref | DottedRef | AggCall]:
    """Yield the reference-bearing leaves (``Ref`` / ``DottedRef`` / ``AggCall``)
    of a tree — scope-free name extraction for schema-drift / memory tagging.

    ``AggCall`` is yielded whole (args/kwargs/filter opaque); ``TransformCall``
    descends ``input`` only; every other composite descends its operands.
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
    if isinstance(parsed, Cast):
        yield from walk_parsed_refs(parsed.source)
        return
    if isinstance(parsed, (DateTrunc, NamedArg)):
        yield from walk_parsed_refs(parsed.value)
        return
    if isinstance(parsed, (Arith, Cmp)):
        yield from walk_parsed_refs(parsed.left)
        yield from walk_parsed_refs(parsed.right)
        return
    if isinstance(parsed, UnaryOp):
        yield from walk_parsed_refs(parsed.operand)
        return
    if isinstance(parsed, BoolOp):
        for op in parsed.operands:
            yield from walk_parsed_refs(op)


# ---------------------------------------------------------------------------
# Call dispatch
# ---------------------------------------------------------------------------


# The node kinds an ``AggCall.source`` may take: a field, star, or an aggregation-free
# scalar expression, predicates included (a bare TupleLit is not a value).
_AGG_SOURCE_KINDS = (
    Ref, DottedRef, StarSource, Literal, TemporalLiteral, Placeholder, ScalarCall, DateTrunc, NamedArg, Cast, Arith,
    UnaryOp, Cmp, BoolOp,
)


def _contains_agg_or_transform(node: Any) -> bool:
    """Whether a parsed subtree contains any AggCall / TransformCall."""
    if isinstance(node, (AggCall, TransformCall)):
        return True
    return any(_contains_agg_or_transform(c) for c in _operands(node))


def _operands(node: Any) -> List[Any]:
    """The value operands of a row-level composite (empty for leaves and calls with their own scope)."""
    if isinstance(node, ScalarCall):
        return list(node.args)
    if isinstance(node, (DateTrunc, NamedArg)):
        return [node.value]
    if isinstance(node, Cast):
        return [node.source]
    if isinstance(node, (Arith, Cmp)):
        return [node.left, node.right]
    if isinstance(node, UnaryOp):
        return [node.operand]
    if isinstance(node, BoolOp):
        return list(node.operands)
    return []


def _source_leaves(node: Any):
    """Composition leaves of an agg source, treating an inner AggCall /
    TransformCall as opaque."""
    children = _operands(node)
    if not children:
        yield node
        return
    for child in children:
        yield from _source_leaves(child)


def _validated_agg_source(source: Any, *, func_name: str, original: str) -> Any:
    """Validate a functional aggregation's first argument as its source; an attached
    (re-aggregation) or mixed source is accepted."""
    leaves = list(_source_leaves(source))
    if any(isinstance(leaf, (AggCall, TransformCall)) for leaf in leaves):
        return source
    if not isinstance(source, _AGG_SOURCE_KINDS):
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: {func_name!r} cannot "
            f"aggregate a {type(source).__name__}; the aggregated expression "
            f"must be built from fields, literals, arithmetic, comparisons, "
            f"and scalar functions."
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


def _onto_path(node: Any, *, path: Tuple[str, ...], memo: Optional[Dict[int, Any]] = None) -> Any:
    """``node`` with every field leaf (and ``*``) read under the relationship ``path``; ``memo`` rewrites a node shared by
    ``args`` and ``written_args`` once."""
    memo = {} if memo is None else memo
    if id(node) in memo:
        return memo[id(node)]
    memo[id(node)] = _onto_path_node(node, path=path, memo=memo)
    return memo[id(node)]


def _onto_path_node(node: Any, *, path: Tuple[str, ...], memo: Dict[int, Any]) -> Any:
    unquoted = (False,) * len(path)
    if isinstance(node, Ref):
        return DottedRef(parts=(*path, node.name), quoted=node.quoted, quoted_parts=(*unquoted, node.quoted),
                         evaluated=node.evaluated)
    if isinstance(node, DottedRef):
        own = tuple(node.quoted_at(i) for i in range(len(node.parts)))
        return DottedRef(parts=(*path, *node.parts), quoted=node.quoted, quoted_parts=(*unquoted, *own),
                         evaluated=node.evaluated)
    if isinstance(node, StarSource):
        return DottedRef(parts=(*path, "*"))
    if isinstance(node, ScalarCall):
        args = tuple(_onto_path(a, path=path, memo=memo) for a in node.args)
        written = None if node.written_args is None else tuple(_onto_path(a, path=path, memo=memo) for a in node.written_args)
        return node.model_copy(update={"args": args, "written_args": written})
    if isinstance(node, Cast):
        written = None if node.written_args is None else tuple(_onto_path(a, path=path, memo=memo) for a in node.written_args)
        return node.model_copy(update={"source": _onto_path(node.source, path=path, memo=memo), "written_args": written})
    if isinstance(node, (DateTrunc, NamedArg)):
        return node.model_copy(update={"value": _onto_path(node.value, path=path, memo=memo)})
    if isinstance(node, (Arith, Cmp)):
        return node.model_copy(update={"left": _onto_path(node.left, path=path, memo=memo), "right": _onto_path(node.right, path=path, memo=memo)})
    if isinstance(node, UnaryOp):
        return node.model_copy(update={"operand": _onto_path(node.operand, path=path, memo=memo)})
    if isinstance(node, BoolOp):
        return node.model_copy(update={"operands": tuple(_onto_path(o, path=path, memo=memo) for o in node.operands)})
    return node


#: A transform argument that binds references; its others are scalars.
_TRANSFORM_REFERENCE_ARGUMENTS = frozenset({"partition_by"})
#: A keyword whose value is a word, never a reference.
_WORD_ARGUMENTS = frozenset({"direction"})


def _reference_children(node: Any) -> Iterator[Tuple[Any, Optional[str]]]:
    """``(child, aggregation)`` of the reference-bearing children of ``node``; ``aggregation`` names a ``*``'s call."""
    if isinstance(node, (tuple, list)):
        yield from ((item, None) for item in node)
    elif isinstance(node, TransformCall):
        yield from _transform_children(node)
    elif isinstance(node, NamedArg):
        if node.name not in _WORD_ARGUMENTS:
            yield node.value, None
    elif isinstance(node, AggCall):
        yield from _aggregation_children(node)
    elif isinstance(node, _BaseNode):
        yield from ((getattr(node, f), None) for f in type(node).model_fields if f != "written_args")


def _transform_children(node: TransformCall) -> Iterator[Tuple[Any, Optional[str]]]:
    yield node.input, None
    named = [a.value for a in node.args if isinstance(a, NamedArg) and a.name in _TRANSFORM_REFERENCE_ARGUMENTS]
    keywords = [value for name, value in node.kwargs if name in _TRANSFORM_REFERENCE_ARGUMENTS]
    yield from ((value, None) for value in (*named, *keywords))


def _aggregation_children(node: AggCall) -> Iterator[Tuple[Any, Optional[str]]]:
    yield node.source, node.agg
    yield node.args, None
    yield from ((value, None) for name, value in node.kwargs if name not in _WORD_ARGUMENTS)
    yield node.filter, None


def walk_references(node: Any, aggregation: Optional[str] = None) -> Iterator[Tuple[Any, Optional[str]]]:
    """``(reference, aggregation)`` of every reference (``Ref``, ``DottedRef``, ``*``) in a reference position of
    ``node``; ``aggregation`` names a ``*``'s aggregation."""
    if isinstance(node, (Ref, DottedRef, StarSource)):
        yield node, aggregation
        return
    for child, agg in _reference_children(node):
        yield from walk_references(child, agg)


def map_refs(node: Any, fn: Callable[[Ref | DottedRef], Any]) -> Any:
    """``node`` with each reference replaced by ``fn(ref)``; ``fn`` returns the reference itself to keep it."""
    if isinstance(node, (Ref, DottedRef)):
        return fn(node)
    if isinstance(node, (tuple, list)):
        mapped = [map_refs(n, fn) for n in node]
        return node if all(a is b for a, b in zip(mapped, node)) else type(node)(mapped)
    if not isinstance(node, _BaseNode):
        return node
    update = {f: map_refs(getattr(node, f), fn) for f in type(node).model_fields}
    changed = {f: v for f, v in update.items() if v is not getattr(node, f)}
    return node.model_copy(update=changed) if changed else node


def onto_path(node: Any, path: Sequence[str]) -> Any:
    """``node`` with every reference read under the relationship ``path``."""
    return map_refs(node, lambda ref: _onto_path_node(ref, path=tuple(path), memo={})) if path else node


def _is_literal_arg(node: Any) -> bool:
    if isinstance(node, UnaryOp) and node.op == "-":
        return isinstance(node.operand, Literal)
    return isinstance(node, (Literal, Placeholder))


def _method_call_refusal(name: str, *, args: Tuple[Any, ...], kwargs: Tuple[Tuple[str, Any], ...]) -> Optional[str]:
    """Why ``<path>.<name>(args)`` has no single functional reading, else ``None``."""
    lowered = name.lower()
    transform_only = lowered in ALL_TRANSFORMS and normalize_aggregation_name(name) not in BUILTIN_AGGREGATIONS
    if lowered in SCALAR_FUNCTIONS or lowered in DATE_PART_ALIASES or transform_only:
        return f"calls {name!r}, which is not an aggregation"
    if not args:
        return "needs the aggregated value as its first argument"
    if any(isinstance(leaf, (DottedRef, AggCall, TransformCall)) for leaf in _source_leaves(args[0])):
        return "may read only bare field names of that path"
    named = [(a.name, a.value) for a in args[1:] if isinstance(a, NamedArg)] + list(kwargs)
    if any(k.lower() == "partition_by" for k, _ in named):
        return "cannot take partition_by="
    further = [a.value if isinstance(a, NamedArg) else a for a in args[1:]]
    if not all(_is_literal_arg(v) for v in (*further, *(v for _, v in kwargs))):
        return "takes only literal further arguments"
    return None


def _method_call(
    *, path: Tuple[str, ...], name: str, args: Tuple[Any, ...], kwargs: Tuple[Tuple[str, Any], ...],
    span: Tuple[int, int],
) -> ParsedExpr:
    """``<path>.<agg>(<source>, <literals>)`` as its functional twin ``<agg>(<path>.<source>, …)``, a deprecated
    spelling; a library name is a deferred aggregation candidate."""
    original = _STATE.text
    if name.lower() in RESERVED_SCALAR_NAMES:
        args, kwargs = _keyword_args(args, kwargs)
    twin_args: Tuple[Any, ...] = (_onto_path(args[0], path=path), *args[1:]) if args else ()
    reason = _method_call_refusal(name, args=args, kwargs=kwargs)
    message = (
        f"Invalid Mode-B expression {original!r}: the method call {'.'.join(path)}.{name}(...) "
        f"{reason}; write the functional form, e.g. {name}{_canonical_call_params(twin_args, kwargs)}."
    )
    deferred = reason is not None and twin_args and name.lower() in RESERVED_SCALAR_NAMES and (
        _method_call_refusal("_", args=args, kwargs=kwargs) is None
    )
    if reason is not None and not deferred:
        raise ValueError(message)
    start, end = span
    _STATE.methods[span] = LegacySpan(
        kind="method-call", start=start, end=end, written=original[start:end],
        twin=f"{name}{_canonical_call_params(twin_args, kwargs) or '()'}",
    )
    if deferred:
        source, *rest = twin_args
        return AggCall(source=source, agg=name, args=tuple(rest), kwargs=kwargs, library_error=message)
    return _dispatch_call(name, args=twin_args, kwargs=kwargs, original=original)


def _keyword_args(
    args: Tuple[Any, ...], kwargs: Tuple[Tuple[str, Any], ...],
) -> Tuple[Tuple[Any, ...], Tuple[Tuple[str, Any], ...]]:
    """A scalar-parsed call's ``name = value`` arguments after the first as ``NamedArg``s, classified at binding."""
    out = [args[0]] if args else []
    for arg in args[1:]:
        name = keyword_spelling(arg)
        out.append(arg if name is None else NamedArg(name=name, value=arg.right))
    return tuple(out), kwargs


def aggregation_reading(call: ScalarCall | Cast) -> Optional[AggCall]:
    """The aggregation call ``call`` reads as when its owning dataset defines ``call.written``; ``None`` when it has no
    field-bearing value argument."""
    if call.written is None:
        return None
    if call.written_args is not None:
        args = call.written_args
    elif isinstance(call, ScalarCall):
        args = call.args
    else:
        return None
    return _aggregation_candidate(call.written, args=args, kwargs=(), error=None)


def _aggregation_candidate(
    func_name: str, *, args: Tuple[Any, ...], kwargs: Tuple[Tuple[str, Any], ...], error: Optional[str],
) -> Optional[AggCall]:
    """``func_name(args)`` as an aggregation call over a field-bearing value, else ``None``."""
    args, kwargs = _keyword_args(args, kwargs)
    if not args or not isinstance(args[0], _AGG_SOURCE_KINDS) or _contains_agg_or_transform(args[0]):
        return None
    if not any(isinstance(leaf, (Ref, DottedRef)) for leaf in _source_leaves(args[0])):
        return None
    return AggCall(source=args[0], agg=func_name, args=args[1:], kwargs=kwargs, library_error=error)


_SCALAR_MIN_MAX = {"min": "least", "max": "greatest"}


def _dispatch_call(  # NOSONAR(S3776) — the one call-dispatch ladder (builtin functional aggregation → transform → scalar → date-part alias → unknown-name AggCall deferral); each rung IS the documented dispatch order and splitting them hides it.
    func_name: str, *, args: Tuple[Any, ...], kwargs: Tuple[Tuple[str, Any], ...], original: str,
) -> ParsedExpr:
    # ``agg`` stores the RAW token; ``first``/``last`` dispatch at bind by the operand's type.
    if func_name.lower() in RESERVED_NAMES:
        if kwargs or len(args) != 1 or not isinstance(args[0], (Ref, DottedRef)) or args[0].evaluated:
            raise AggArgumentError(expression=original)
        return args[0].model_copy(update={"evaluated": True})
    canonical_agg = normalize_aggregation_name(func_name)
    if canonical_agg in BUILTIN_AGGREGATIONS:
        if not args:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: aggregation "
                f"{func_name!r} requires a value argument, e.g. "
                f"{func_name}(field)."
            )
        if canonical_agg in _SCALAR_MIN_MAX and len(args) == 2 and not any(isinstance(a, NamedArg) for a in args):
            scalar = _SCALAR_MIN_MAX[canonical_agg]
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: {func_name}() with two values is the "
                f"aggregation, not a scalar; use {scalar}{_canonical_call_params(args, ())}."
            )
        source = _validated_agg_source(
            args[0], func_name=func_name, original=original,
        )
        return AggCall(source=source, agg=func_name, args=args[1:], kwargs=kwargs)

    if func_name.lower() in ALL_TRANSFORMS:
        if not args:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: transform "
                f"{func_name!r} requires at least one positional argument "
                f"(the value to transform)."
            )
        _reject_bare_star_args(args, kwargs, func_name=func_name, original=original)
        return TransformCall(op=func_name.lower(), input=args[0], args=args[1:], kwargs=kwargs)

    spelling = _SPELLINGS.get(func_name.lower())
    if spelling is not None:
        bucket = func_name.lower() == "date_trunc"
        try:
            if kwargs:
                raise _Problem(f"{func_name}() takes positional arguments only.")
            if not bucket:
                _reject_bare_star_args(args, kwargs, func_name=func_name, original=original)
            elif any(isinstance(a, StarSource) for a in args):
                raise _Problem(f"date_trunc() takes a unit string literal and one value: {DATE_TRUNC_FORM}.")
            built = spelling(args)
        except _Problem as problem:
            message = f"Invalid Mode-B expression {original!r}: {problem.detail}"
            if bucket:
                message += f" Units: {', '.join(g.value for g in TimeGranularity)} or a datasource granularity."
            unknown = func_name.lower() in _ISO_PARSERS
            deferred = _aggregation_candidate(func_name, args=args, kwargs=kwargs, error=None if unknown else message)
            if deferred is not None:
                return deferred
            if unknown:
                raise UnknownFunctionError(name=func_name, location=original, suggestion=problem.detail) from None
            raise (GranularityCallError if bucket else ValueError)(message) from None
        if isinstance(built, (ScalarCall, Cast)):
            return built.model_copy(update={"written": func_name, "written_args": args})
        return built

    # A renaming spelling is its library function, written as spelled; lower case interns ``COALESCE`` / ``coalesce``.
    registered = SCALAR_SPELLING_REGISTRY.get(func_name.lower())
    library = func_name.lower() if registered is None or registered.dispatch == SPELLING_REWRITE else registered.dispatch
    if library in SCALAR_FUNCTIONS:
        if kwargs:
            message = (
                f"Invalid Mode-B expression {original!r}: scalar function "
                f"{func_name!r} does not accept keyword arguments. Pass "
                f"values positionally."
            )
            deferred = _aggregation_candidate(func_name, args=args, kwargs=kwargs, error=message)
            if deferred is None:
                raise ValueError(message)
            return deferred
        _reject_bare_star_args(args, kwargs, func_name=func_name, original=original)
        try:
            date_args = _date_args(name=library, args=args, original=original)
        except ValueError as error:
            deferred = _aggregation_candidate(func_name, args=args, kwargs=kwargs, error=str(error))
            if deferred is None:
                raise
            return deferred
        return ScalarCall(name=library, args=date_args, written=func_name)

    part = DATE_PART_ALIASES.get(func_name.lower())
    if part is not None:
        if kwargs or len(args) != 1:
            raise CallArgumentError(
                f"Invalid Mode-B expression {original!r}: date-part function {func_name!r} "
                f"takes 1 argument; got {len(args) + len(kwargs)}."
            )
        _reject_bare_star_args(args, kwargs, func_name=func_name, original=original)
        return ScalarCall(name="date_part", args=(Literal(value=part.value), args[0]))

    # Unknown name with an aggregatable first arg → AggCall candidate, validated at binding.
    if args and isinstance(args[0], _AGG_SOURCE_KINDS) and not _contains_agg_or_transform(args[0]):
        return AggCall(source=args[0], agg=func_name, args=args[1:], kwargs=kwargs)
    if args and any(isinstance(leaf, AggCall) for leaf in _source_leaves(args[0])):
        source = _validated_agg_source(args[0], func_name=func_name, original=original)
        return AggCall(source=source, agg=func_name, args=args[1:], kwargs=kwargs)

    if func_name.lower() in RESERVED_NAMES:
        raise ReservedFunctionNameError(name=func_name, where=f"Invalid Mode-B expression {original!r}")
    if func_name.lower() in GRANULARITY_NAMES:
        raise GranularityCallError.no_call_form(func_name.lower(), operand=", ".join(map(canonical_metric_text, args)))
    raise UnknownFunctionError(
        name=func_name,
        location=original,
        suggestion=unknown_call_hint(func_name) or (
            f"Mode-B accepts the closed scalar allowlist "
            f"({sorted(SCALAR_FUNCTIONS)}), transforms "
            f"({sorted(ALL_TRANSFORMS)}), and aggregations "
            f"(`sum(revenue)`); custom "
            f"aggregation names must be defined on the dataset."
        ),
    )


DATE_TRUNC_FORM = "date_trunc('<unit>', expr)"


def _date_trunc(args: Tuple[Any, ...]) -> ParsedExpr:
    """``date_trunc(unit, x)``: the time bucket of ``x``."""
    if len(args) != 2 or not (isinstance(args[0], Literal) and isinstance(args[0].value, str)) or isinstance(
        args[1], NamedArg,
    ):
        raise _Problem(f"date_trunc() takes a unit string literal and one value: {DATE_TRUNC_FORM}.")
    unit = args[0].value.lower()
    if not unit.isidentifier() or (unit not in GRANULARITY_NAMES and (
        unit in RESERVED_SCALAR_NAMES or unit in ALL_TRANSFORMS or normalize_aggregation_name(unit) in BUILTIN_AGGREGATIONS
        or unit in DATE_PART_ALIASES or unit in WEEKDAY_CALLS
    )):
        raise _Problem(f"unknown date_trunc() unit {args[0].value!r}; use a granularity such as 'month'.")
    return DateTrunc(unit=unit, value=args[1])


def _json_alias(name: str):
    def build(args: Tuple[Any, ...]) -> ParsedExpr:
        if len(args) != 2:
            raise _Problem(f"{name}(x, path) takes exactly two arguments; for a typed result use "
                           f"json_value(x, path RETURNING <type>).")
        return ScalarCall(name="json_value", args=args)
    return build


def _json_key_list(name: str):
    def build(args: Tuple[Any, ...]) -> ParsedExpr:
        keys = args[1:]
        if not keys or not all(isinstance(k, Literal) and isinstance(k.value, str) for k in keys):
            raise _Problem(f"{name}(x, 'key', …) takes string-literal keys after the value.")
        try:
            path = json_path_text(tuple(check_key(k.value) for k in keys))
        except JsonPathError as e:
            raise _Problem(f"{e}.") from None
        return ScalarCall(name="json_value", args=(args[0], Literal(value=path)))
    return build


def _ossie_unit(node: Any) -> Any:
    """Ossie's bare date unit word (``DATEDIFF(day, …)``) as SLayer's unit literal."""
    if isinstance(node, Ref) and not node.quoted and node.name.lower() in _UNIT_WORDS:
        return Literal(value=node.name.lower())
    return node


def _datediff(args: Tuple[Any, ...]) -> ParsedExpr:
    """Ossie ``DATEDIFF(unit, start, end)`` as ``date_diff``."""
    if len(args) != 3:
        raise _Problem("DATEDIFF takes (unit, start, end).")
    return _dispatch_call("date_diff", args=(_ossie_unit(args[0]), *args[1:]), kwargs=(), original=_STATE.text)


def _dateadd(args: Tuple[Any, ...]) -> ParsedExpr:
    """Ossie ``DATEADD(unit, n, ts)`` as ``date_add(ts, n, unit)``."""
    if len(args) != 3:
        raise _Problem("DATEADD takes (unit, amount, timestamp).")
    return _dispatch_call("date_add", args=(args[2], args[1], _ossie_unit(args[0])), kwargs=(), original=_STATE.text)


def _nvl2(args: Tuple[Any, ...]) -> ParsedExpr:
    """Ossie ``NVL2(e, a, b)`` as ``iif(e IS NOT NULL, a, b)``."""
    if len(args) != 3:
        raise _Problem(f"NVL2 takes (expr, not_null_result, null_result); got {len(args)} arguments.")
    return ScalarCall(name="iif", args=(Cmp(op=_IS_NOT, left=args[0], right=Literal(value=None)), *args[1:]))


def _charindex(args: Tuple[Any, ...]) -> ParsedExpr:
    """Ossie ``CHARINDEX(sub, s)`` as ``instr(s, sub)``."""
    if len(args) != 2:
        raise _Problem(f"CHARINDEX takes (substring, string); got {len(args)} arguments (a start position is not "
                       f"supported).")
    return ScalarCall(name="instr", args=(args[1], args[0]))


#: Ossie's ISO-string parsers → the cast each one is.
_ISO_PARSERS = {"to_date": "DATE", "to_timestamp": "TIMESTAMP_NTZ"}


def _iso_parser_hint(name: str) -> str:
    return (f"{name.upper()}(s) takes one ISO-8601 string and is CAST(s AS {_ISO_PARSERS[name.lower()]}); a format "
            f"string, or any other argument count, is not supported.")


def _iso_parser(name: str):
    """Ossie ``TO_DATE(s)`` / ``TO_TIMESTAMP(s)`` as the ``CAST`` it is."""
    def build(args: Tuple[Any, ...]) -> ParsedExpr:
        if len(args) != 1:
            raise _Problem(_iso_parser_hint(name))
        return Cast(source=args[0], type_text=_ISO_PARSERS[name])
    return build


def unknown_call_hint(name: str) -> Optional[str]:
    """Why a call of ``name`` that resolves to nothing is refused, when the name means something else."""
    return _iso_parser_hint(name) if name.lower() in _ISO_PARSERS else part_call_hint(name)


def _current_timestamp(args: Tuple[Any, ...]) -> ParsedExpr:
    """SQL ``CURRENT_TIMESTAMP()`` as ``now()``."""
    if args:
        raise _Problem("CURRENT_TIMESTAMP takes no arguments.")
    return _dispatch_call("now", args=(), kwargs=(), original=_STATE.text)


# The rewriting spellings of ``SCALAR_SPELLING_REGISTRY`` (a renaming one is dispatched as its library function).
_SPELLINGS: Dict[str, Callable[[Tuple[Any, ...]], ParsedExpr]] = {
    "datediff": _datediff,
    "dateadd": _dateadd,
    "current_timestamp": lambda args: _current_timestamp(args),
    "nvl2": _nvl2,
    "charindex": _charindex,
    "to_date": _iso_parser("to_date"),
    "to_timestamp": _iso_parser("to_timestamp"),
    "date_trunc": _date_trunc,
    "json_extract": _json_alias("json_extract"),
    "json_extract_scalar": _json_alias("json_extract_scalar"),
    "json_extract_path_text": _json_key_list("json_extract_path_text"),
    "jsonb_extract_path_text": _json_key_list("jsonb_extract_path_text"),
}


def keyword_spelling(node: Any) -> Optional[str]:
    """The name of a ``name = value`` argument, which a scalar call reads as a comparison."""
    if isinstance(node, Cmp) and node.single_eq and isinstance(node.left, Ref) and not node.left.quoted:
        return node.left.name
    return None


def positional_hint(name: str) -> str:
    return f"scalar functions take positional arguments; `{name} = …` here is a comparison."


def comparison_hint(name: str) -> str:
    return f"to pass a comparison instead, write `({name} = …)`."


def call_keywords(call: AggCall | TransformCall) -> List[Tuple[str, Any]]:
    """``call``'s ``name = value`` arguments, lower-cased names, before binding classifies them."""
    named = [(a.name.lower(), a.value) for a in call.args if isinstance(a, NamedArg)]
    return [*named, *((k.lower(), v) for k, v in call.kwargs)]


def classify_arguments(
    items: Sequence[Any], *, parameters: Iterable[str], call: str,
) -> Tuple[Tuple[Any, ...], Tuple[Tuple[str, Any], ...]]:
    """``items`` as ``(positional, keywords)``: a ``NamedArg`` naming one of ``parameters`` (any case) is that
    keyword, any other the comparison ``name = value``."""
    declared = list(parameters)
    positional: List[Any] = []
    keywords: List[Tuple[str, Any]] = []
    for item in items:
        param = match_name(name=item.name, declared=declared, quoted=False) if isinstance(item, NamedArg) else None
        if param is not None:
            if any(k == param for k, _ in keywords):
                raise CallArgumentError(f"{call}() got keyword argument {param!r} more than once.")
            keywords.append((param, item.value))
            continue
        if keywords:
            raise CallArgumentError(f"{call}(): a positional argument follows a keyword argument.")
        positional.append(_named_comparison(item, call=call) if isinstance(item, NamedArg) else item)
    return tuple(positional), tuple(keywords)


def _named_comparison(item: NamedArg, *, call: str) -> Cmp:
    if isinstance(item.value, (tuple, StarSource)) or (
        isinstance(item.value, DottedRef) and item.value.parts[-1:] == ("*",)
    ):
        raise CallArgumentError(
            f"{call}(): {item.name!r} is not a parameter of {call}(), so `{item.name} = …` is a comparison, "
            f"which cannot take a list or '*'."
        )
    return Cmp(op="==", left=Ref(name=item.name), right=item.value, single_eq=True)


# Position of the count argument of the date-offset calls.
_COUNT_ARG = {"date_add": 1, "interval": 0}


def _date_args(*, name: str, args: Tuple[Any, ...], original: str) -> Tuple[Any, ...]:
    """Validate and lower-case a date scalar's unit word; fold a negated numeric count literal."""
    out = list(args)
    unit = DATE_UNIT_ARGS.get(name)
    if unit is not None and unit[0] < len(out):
        pos, vocab = unit
        words = tuple(v.value for v in vocab)
        out[pos] = Literal(value=_unit_word(out[pos], name=name, words=words, original=original))
    count_pos = _COUNT_ARG.get(name)
    if count_pos is not None and count_pos < len(out):
        out[count_pos] = _folded(out[count_pos])
    return tuple(out)


def _unit_word(arg: Any, *, name: str, words: Tuple[str, ...], original: str) -> str:
    """The canonical lower-case unit (or ``date_part`` part) word of ``arg``, one of ``words``."""
    accepted = ", ".join(words)
    what = "part" if name == "date_part" else "unit"
    if name == "date_part" and _bare_part(arg, words=words):
        arg = Literal(value=arg.name)
    if not (isinstance(arg, Literal) and isinstance(arg.value, str)):
        spelled = keyword_spelling(arg)
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: the {what} of {name}() must be a "
            f"string literal, one of: {accepted}." + (f" Note: {positional_hint(spelled)}" if spelled else "")
        )
    word = arg.value.lower()
    if name == "date_part":
        if word in AMBIGUOUS_WEEKDAY_NAMES:
            raise ValueError(f"Invalid Mode-B expression {original!r}: {part_call_hint(arg.value)}")
        alias = DATE_PART_ALIASES.get(word) or PART_ONLY_WORDS.get(word)
        word = alias.value if alias is not None else word
    if word not in words:
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: unknown {name}() {what} "
            f"{arg.value!r}; accepted: {accepted}."
        )
    return word


def _bare_part(arg: Any, *, words: Tuple[str, ...]) -> bool:
    """Whether ``arg`` is an unquoted word naming a date part (or its alias), as ``EXTRACT`` takes it."""
    if not isinstance(arg, Ref) or arg.quoted or arg.evaluated:
        return False
    word = arg.name.lower()
    return word in words or word in DATE_PART_ALIASES or word in AMBIGUOUS_WEEKDAY_NAMES or word in PART_ONLY_WORDS


def _folded(node: Any) -> Any:
    """``-<numeric literal>`` as one negative literal, so both count spellings intern alike."""
    if (
        isinstance(node, UnaryOp) and node.op == "-"
        and isinstance(node.operand, Literal) and isinstance(node.operand.value, Decimal)
    ):
        return Literal(value=-node.operand.value)
    return node


def _is_interval(node: Any) -> bool:
    return isinstance(node, ScalarCall) and node.name == "interval"


def _interval_as_date_add(*, op: str, left: Any, right: Any) -> Optional[ScalarCall]:
    """``x + interval(n, u)`` / ``interval(n, u) + x`` / ``x - interval(n, u)`` as ``date_add``."""
    if _is_interval(left) == _is_interval(right) or op not in ("+", "-"):
        return None
    if op == "-" and _is_interval(left):
        return None
    interval, base = (right, left) if _is_interval(right) else (left, right)
    if len(interval.args) != 2:
        raise ValueError(check_scalar_arity(name="interval", argc=len(interval.args)))
    count, unit = interval.args
    if op == "-":
        count = _folded(UnaryOp(op="-", operand=count))
    return ScalarCall(name="date_add", args=(base, count, unit))


def walk_parsed(node: Any) -> Iterator[Any]:
    """Every node of a parsed tree, including call args, kwargs, agg sources and filters."""
    if isinstance(node, tuple):
        for item in node:
            yield from walk_parsed(item)
        return
    if not isinstance(node, _BaseNode):
        return
    yield node
    for field in type(node).model_fields:
        # ``written_args`` records a spelling as written; ``args`` is the tree.
        if field != "written_args":
            yield from walk_parsed(getattr(node, field))


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------


def drop_root_hop(hops: Sequence[str], root: Optional[str], *, head_quoted: bool) -> Tuple[str, ...]:
    """A reference's relationship ``hops`` without a leading qualifier naming the query root ``root`` (exactly when
    ``head_quoted``, else in any case)."""
    hops = tuple(hops)
    return (
        hops[1:] if root is not None and hops and match_name(name=hops[0], declared=[root], quoted=head_quoted)
        else hops
    )


def drop_root_qualifier(parts: Sequence[str], root: Optional[str], *, quoted: Sequence[bool]) -> Tuple[str, ...]:
    """``parts`` without a leading qualifier naming the query root ``root`` (``r.x`` ≡ ``x``); ``quoted`` flags
    each quoted segment."""
    parts = tuple(parts)
    if len(parts) < 2:
        return parts
    return (*drop_root_hop(parts[:-1], root, head_quoted=bool(quoted[:1]) and quoted[0]), parts[-1])


def dotted_quoting(node: Any) -> Tuple[bool, ...]:
    """Each segment's quoted flag of a parsed ``Ref`` / ``DottedRef``."""
    if isinstance(node, Ref):
        return (node.quoted,)
    return tuple(node.quoted_at(i) for i in range(len(node.parts)))


def root_relative(parsed: Any, root: Optional[str]) -> Any:
    """``parsed`` with each reference's root qualifier dropped."""
    return parsed if root is None else _root_relative(parsed, root=root, memo={})


def _root_relative(node: Any, *, root: str, memo: Dict[int, Any]) -> Any:
    if isinstance(node, tuple):
        return tuple(_root_relative(item, root=root, memo=memo) for item in node)
    if not isinstance(node, _BaseNode):
        return node
    if id(node) not in memo:
        if isinstance(node, DottedRef):
            all_quoted = dotted_quoting(node)
            parts = drop_root_qualifier(node.parts, root, quoted=all_quoted)
            quoted_parts = all_quoted[len(node.parts) - len(parts):]
            if parts == ("*",):
                memo[id(node)] = StarSource()
            elif len(parts) == 1:
                memo[id(node)] = Ref(name=parts[0], quoted=quoted_parts[0], evaluated=node.evaluated)
            else:
                memo[id(node)] = node.model_copy(update={
                    "parts": parts, "quoted_parts": quoted_parts, "quoted": any(quoted_parts),
                })
        else:
            memo[id(node)] = node.model_copy(
                update={f: _root_relative(getattr(node, f), root=root, memo=memo) for f in type(node).model_fields},
            )
    return memo[id(node)]


def referenced_paths(parsed: Any) -> Iterator[Tuple[str, ...]]:
    """The path of each reference in ``parsed``; a quoted identifier is the one name it spells."""
    for node in walk_parsed(parsed):
        if isinstance(node, Ref):
            yield (node.name,)
        elif isinstance(node, DottedRef):
            yield node.parts


def text_paths(text: str, *, root: Optional[str]) -> List[Tuple[str, ...]]:
    """``referenced_paths`` of Mode-B template ``text`` relative to ``root``; none when it does not parse
    (binding reports it)."""
    try:
        parsed = parse_template(text)
    except Exception:  # noqa: BLE001 — unparseable text reads nothing
        return []
    return [
        drop_root_qualifier((node.name,) if isinstance(node, Ref) else node.parts, root, quoted=dotted_quoting(node))
        for node in walk_parsed(parsed) if isinstance(node, (Ref, DottedRef))
    ]


_RUN_RE = re.compile(_RUN)
_RUN_CHAR_RE = re.compile(_RUN_CHAR)


def _is_path_head(text: str, *, start: int, end: int) -> bool:
    """The bare word ``text[start:end]`` opens a dotted path: no word or ``.`` before it, a ``.`` and a segment after."""
    before = text[:start].rstrip()
    after = text[end:].lstrip()
    if (start and _RUN_CHAR_RE.match(text[start - 1])) or before.endswith(".") or not after.startswith("."):
        return False
    segment = after[1:].lstrip()
    return bool(segment) and (segment[0] in '"*' or _RUN_RE.match(segment) is not None)


_PLACEHOLDER_RE = re.compile(r"\{[^{}]*\}")


def _is_bare_ref(text: str, *, start: int, end: int) -> bool:
    """The word ``text[start:end]`` stands alone: no word or ``.`` before it, no ``.`` or call after it."""
    before = text[:start].rstrip()
    after = text[end:].lstrip()
    return not ((start and _RUN_CHAR_RE.match(text[start - 1])) or before.endswith(".") or after.startswith((".", "(")))


def respell_bare_names(text: str, *, respell: Callable[[str], Optional[str]]) -> str:
    """``text`` with each unquoted bare reference ``respell`` maps to new text replaced, every other byte kept;
    text that does not parse is returned unchanged."""
    try:
        parsed = parse_template(text)
    except Exception:  # noqa: BLE001 — unparseable text is left for binding to report
        return text
    names = {n.name for n in walk_parsed(parsed) if isinstance(n, Ref)}
    out: List[str] = []
    last = 0
    for region in (r for r in _regions(text) if r.kind == "code") if names else ():
        placeholders = [m.span() for m in _PLACEHOLDER_RE.finditer(text, region.start, region.end)]
        for word in _RUN_RE.finditer(text, region.start, region.end):
            new = respell(word.group()) if word.group() in names else None
            inside = any(a <= word.start() < b for a, b in placeholders)
            if new is None or inside or not _is_bare_ref(text, start=word.start(), end=word.end()):
                continue
            out += [text[last:word.start()], new]
            last = word.end()
    return "".join([*out, text[last:]])


def respell_path_heads(text: str, *, respell: Callable[[str], Optional[str]]) -> str:
    """``text`` with each unquoted dotted-path head ``respell`` maps to a new spelling replaced, every other byte kept;
    text that does not parse is returned unchanged."""
    try:
        parse_template(text)
    except Exception:  # noqa: BLE001 — unparseable text is left for binding to report
        return text
    regions = _regions(text)
    blanked = "".join(
        re.sub(r"[^\n]", " ", text[r.start:r.end]) if r.kind == "comment" else text[r.start:r.end] for r in regions
    )
    out: List[str] = []
    last = 0
    for region in regions:
        if region.kind != "code":
            continue
        for word in _RUN_RE.finditer(text, region.start, region.end):
            if not _is_path_head(blanked, start=word.start(), end=word.end()):
                continue
            new = respell(word.group())
            if new is None or new == word.group():
                continue
            out += [text[last:word.start()], new]
            last = word.end()
    return "".join([*out, text[last:]])


class CallSource(BaseModel):
    """A whole-text call ``f(a, …)`` as written: its callee path (a ``{var}`` callee as written) and each
    argument's source text."""

    model_config = ConfigDict(frozen=True)

    callee: Tuple[str, ...]
    arguments: Tuple[str, ...]


@lru_cache(maxsize=4096)
def call_source(text: str) -> Optional[CallSource]:
    """``text`` (a template) as one call with its callee's and arguments' source text, else ``None``."""
    with _PARSE_LOCK:
        _STATE.reset(text)
        try:
            callee, located = _LOCATED_CALL.parse_string(_mask_placeholders(_bound_strings(text)), parse_all=True)
        except Exception:  # noqa: BLE001 — not a well-formed call
            return None
    words = callee[1][0]
    return CallSource(
        callee=(text[callee[0]:callee[-1]],) if isinstance(words, str) else tuple(w.name for w in words),
        arguments=tuple(text[arg[0]:arg[-1]].strip() for arg in located),
    )


# ---------------------------------------------------------------------------
# Canonical text rendering + entity-ref splitting
# ---------------------------------------------------------------------------


_CANONICAL_OPS = {"==": "="}


class _Style(Enum):
    """How the renderer spells: canonical with written quoting kept, canonical quote-blind (identity), or the pre-2.0
    result-name seed (quote-blind)."""

    CANONICAL = "canonical"
    IDENTITY = "identity"
    LEGACY = "legacy"


def _is_null(node: Any) -> bool:
    return isinstance(node, Literal) and node.value is None


def _argument_text(node: Any, *, style: _Style) -> str:
    """A non-scalar call's argument: a ``name = value`` comparison parenthesised, so it never reads as a keyword."""
    text = _render(node, style=style)
    if (style is not _Style.LEGACY and isinstance(node, Cmp) and node.op == "==" and isinstance(node.left, Ref)
            and not _reference_text(node.left, style=style).startswith('"')):
        return f"({text})"
    return text


def _canonical_call_params(
    args: Tuple[Any, ...], kwargs: Tuple[Tuple[str, Any], ...], *, bare_direction: bool = False, style: _Style = _Style.CANONICAL,
) -> str:
    parts = [
        _canonical_kwarg(a.name, a.value, bare_direction=bare_direction, style=style) if isinstance(a, NamedArg)
        else _argument_text(a, style=style)
        for a in args
    ]
    parts += [_canonical_kwarg(k, v, bare_direction=bare_direction, style=style) for k, v in kwargs]
    return f"({', '.join(parts)})" if parts else ""


def _canonical_kwarg(name: str, value: Any, *, bare_direction: bool, style: _Style) -> str:
    """``name=value``; a transform's recognised ``direction`` renders normalised (bare in the legacy seed)."""
    if bare_direction and name == "direction" and isinstance(value, Literal):
        direction = normalize_direction(value.value)
        if direction is not None:
            return direction if style is _Style.LEGACY else f"direction='{direction}'"
    return f"{name}={_canonical_kwarg_text(value, style=style)}"


def _canonical_kwarg_text(value: Any, *, style: _Style) -> str:
    if isinstance(value, tuple):
        return f"[{', '.join(_render(v, style=style) for v in value)}]"
    return _render(value, style=style)


def _quoted_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _identifier_text(name: str) -> str:
    return name if name.isidentifier() else _quoted_identifier(name)


def _probe_tree(text: str) -> Any:
    """``text`` parsed under its own scratch state, so a parse action may call it mid-parse."""
    global _STATE
    with _PARSE_LOCK:
        outer, _STATE = _STATE, _State()
        try:
            _STATE.reset(text)
            return _parse_tree(text)
        finally:
            _STATE = outer


@lru_cache(maxsize=4096)
def _reads_back_bare(name: str, *, head: bool) -> bool:
    """Whether ``name`` written bare reads back as that unquoted segment: alone at a path's head, else after a dot."""
    try:
        parsed = _probe_tree(name if head else f"x.{name}")
    except Exception:  # noqa: BLE001 — any failure to read back means it must be quoted
        return False
    if head:
        return isinstance(parsed, Ref) and parsed.name == name and not parsed.quoted and not parsed.evaluated
    return isinstance(parsed, DottedRef) and parsed.parts == ("x", name) and not parsed.quoted and not parsed.evaluated


def _segment_text(name: str, *, quoted: bool, head: bool, style: _Style) -> str:
    if style is not _Style.CANONICAL:
        return _identifier_text(name)
    return _quoted_identifier(name) if quoted or not _reads_back_bare(name, head=head) else name


def _path_segment_text(node: DottedRef, i: int, *, style: _Style) -> str:
    """Segment ``i`` of a dotted path; a ``*`` segment is the star unless written quoted (canonical style)."""
    part, quoted = node.parts[i], node.quoted_at(i)
    if part == "*" and not (quoted and style is _Style.CANONICAL):
        return part
    return _segment_text(part, quoted=quoted, head=i == 0, style=style)


def _reference_text(node: Ref | DottedRef, *, style: _Style) -> str:
    if isinstance(node, Ref):
        return _segment_text(node.name, quoted=node.quoted, head=True, style=style)
    return ".".join(_path_segment_text(node, i, style=style) for i in range(len(node.parts)))


_P_OR, _P_AND, _P_NOT, _P_CMP, _P_ADD, _P_MUL, _P_UNARY, _P_ATOM = range(1, 9)
_ARITH_PREC = {"+": _P_ADD, "-": _P_ADD, "*": _P_MUL, "/": _P_MUL, "%": _P_MUL}


def _precedence(node: Any) -> int:
    if isinstance(node, BoolOp):
        return _P_OR if node.op == "or" else _P_AND
    if isinstance(node, UnaryOp):
        return _P_NOT if node.op == "not" else _P_UNARY
    if isinstance(node, Cmp):
        return _P_CMP
    if isinstance(node, Arith):
        return _ARITH_PREC[node.op]
    if isinstance(node, Literal) and isinstance(node.value, Decimal) and node.value.is_signed():
        return _P_UNARY
    return _P_ATOM


def _operand_text(node: Any, *, minimum: int, style: _Style) -> str:
    """``node``'s canonical text, parenthesised when it binds looser than ``minimum``."""
    text = _render(node, style=style)
    return f"({text})" if _precedence(node) < minimum else text


def canonical_metric_text(parsed: Any) -> str:
    """Render a ``ParsedExpr`` to canonical functional text that parses back to the same tree, quoting included."""
    return _render(parsed, style=_Style.CANONICAL)


def quote_blind_metric_text(parsed: Any) -> str:
    """The canonical text with written quoting ignored: tells expressions apart before binding. Never parsed."""
    return _render(parsed, style=_Style.IDENTITY)


def legacy_result_name_seed(parsed: Any) -> str:
    """The text result keys are named from: the canonical text in the pre-2.0 aggregation spelling, so keys stay
    stable. Never parsed or shown."""
    return _render(parsed, style=_Style.LEGACY)


def _render(parsed: Any, *, style: _Style) -> str:  # NOSONAR(S3776) — flat per-node-kind rendering table; each branch is one spelling rule.
    if isinstance(parsed, (Ref, DottedRef)):
        text = _reference_text(parsed, style=style)
        return f"AGG({text})" if parsed.evaluated else text
    if isinstance(parsed, StarSource):
        return "*"
    if isinstance(parsed, Literal):
        if isinstance(parsed.value, str):
            return "'" + parsed.value.replace("'", "''") + "'"
        return "NULL" if parsed.value is None else str(parsed.value)
    if isinstance(parsed, TemporalLiteral):
        return f"{parsed.kind.upper()} '{parsed.text}'"
    if isinstance(parsed, (Placeholder, BoundVariable)):
        return f"{{{parsed.name}}}"
    if isinstance(parsed, TupleLit):
        return f"({', '.join(_render(e, style=style) for e in parsed.elements)})"
    if isinstance(parsed, ArrayLit):
        return f"[{', '.join(_render(e, style=style) for e in parsed.elements)}]"
    if isinstance(parsed, AggCall):
        return _canonical_agg_text(parsed, style=style)
    if isinstance(parsed, TransformCall):
        inner = _argument_text(parsed.input, style=style)
        params = _canonical_call_params(parsed.args, parsed.kwargs, bare_direction=True, style=style)
        return f"{parsed.op}({inner}{', ' + params[1:-1] if params else ''})"
    if isinstance(parsed, ScalarCall):
        return f"{parsed.name}({', '.join(_render(a, style=style) for a in parsed.args)})"
    if isinstance(parsed, DateTrunc):
        return f"date_trunc('{parsed.unit}', {_render(parsed.value, style=style)})"
    if isinstance(parsed, NamedArg):
        return f"{parsed.name}={_canonical_kwarg_text(parsed.value, style=style)}"
    if isinstance(parsed, Cast):
        fn = "TRY_CAST" if parsed.try_ else "CAST"
        return f"{fn}({_render(parsed.source, style=style)} AS {parsed.type_text})"
    if isinstance(parsed, (Arith, Cmp)):
        prec = _precedence(parsed)
        left = _operand_text(parsed.left, minimum=prec + 1 if isinstance(parsed, Cmp) else prec, style=style)
        if style is not _Style.LEGACY and isinstance(parsed, Cmp) and parsed.op in ("is", _IS_NOT) and _is_null(parsed.right):
            return f"{left} {parsed.op.upper()} NULL"
        right = _operand_text(parsed.right, minimum=prec + 1, style=style)
        return f"{left} {parsed.op if style is _Style.LEGACY else _CANONICAL_OPS.get(parsed.op, parsed.op)} {right}"
    if isinstance(parsed, UnaryOp):
        operand = _operand_text(parsed.operand, minimum=_precedence(parsed) + 1, style=style)
        return f"not {operand}" if parsed.op == "not" else f"{parsed.op}{operand}"
    if isinstance(parsed, BoolOp):
        prec = _precedence(parsed)
        return f" {parsed.op} ".join(_operand_text(o, minimum=prec + 1, style=style) for o in parsed.operands)
    return str(parsed)


def _canonical_agg_text(parsed: AggCall, *, style: _Style) -> str:
    source = _render(parsed.source, style=style)
    if style is _Style.LEGACY and isinstance(parsed.source, (Ref, DottedRef, StarSource)):
        text = f"{source}:{parsed.agg}{_canonical_call_params(parsed.args, parsed.kwargs, style=style)}"
    else:
        parts = [_argument_text(parsed.source, style=style)]
        parts += [_argument_text(a, style=style) for a in parsed.args]
        parts += [f"{k}={_canonical_kwarg_text(v, style=style)}" for k, v in parsed.kwargs]
        text = f"{parsed.agg}({', '.join(parts)})"
    if parsed.filter is not None:
        text += f" FILTER (WHERE {_render(parsed.filter, style=style)})"
    return text


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
    """``(prefix, agg_suffix)`` of an entity reference: ``sum(orders.amount)`` → ``("orders.amount", "sum")``,
    ``last(balance, updated_at)`` → ``("balance", "last(updated_at)")``; call-free text → ``(raw, None)``.

    Raises ``ValueError`` unless the text is one aggregation over a field or star.
    """
    if "(" not in raw and ":" not in raw:
        return raw, None
    parsed = parse_expr(raw)
    if not isinstance(parsed, AggCall) or parsed.filter is not None or not isinstance(
        parsed.source, (Ref, DottedRef, StarSource)
    ):
        raise ValueError(
            f"{raw!r} is not a single aggregated field reference; only "
            f"the `agg(field)` form names an entity."
        )
    source = parsed.source
    if isinstance(source, StarSource):
        prefix = "*"
    elif isinstance(source, Ref):
        prefix = source.name
    else:
        prefix = ".".join(source.parts)
    return prefix, _functional_suffix_text(raw, agg=parsed.agg)


def metric_text_at_path(expression: str, *, path: Sequence[str]) -> str:
    """``expression`` read from the dataset at the relationship ``path``, as canonical text: an aggregation's source re-rooted
    (``count(*)`` → ``count(customers.*)``), any other reference prefixed."""
    node = parse_expr(expression)
    if isinstance(node, AggCall):
        return canonical_metric_text(node.model_copy(update={"source": _onto_path(node.source, path=tuple(path))}))
    return canonical_metric_text(_onto_path(node, path=tuple(path)))


# ---------------------------------------------------------------------------
# Stored colon spans
# ---------------------------------------------------------------------------


def rewrite_colon_spans(text: str, *, units: Optional[frozenset[str]] = None) -> str:
    """``text`` with every colon span written as its functional twin (innermost first), every other byte kept;
    with ``units``, only the spans naming one of those granularities, each a ``date_trunc``.

    Raises ``ValueError`` when ``text`` does not parse.
    """
    if ":" not in text:
        return text
    with _PARSE_LOCK:
        _STATE.reset(text)
        _STATE.spans = []
        try:
            _parse_tree(text)
            spans = _STATE.spans
        finally:
            _STATE.spans = None
    picked = [s for s in spans if units is None or (s.args is None and s.name.casefold() in units)]
    unique = sorted({(s.start, s.end): s for s in picked}.values(), key=lambda s: (s.start, -s.end))
    return _rewritten(text, spans=unique, start=0, end=len(text), units=units or frozenset())


# ---------------------------------------------------------------------------
# Ossie mapping expressions
# ---------------------------------------------------------------------------


class MappingExpression(BaseModel):
    """An Ossie mapping expression: a row-level ``value``, kept only for rows where ``predicate`` holds."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    value: Any
    predicate: Any = None


_WHERE_RE = re.compile(r"(?<![\w.$])where(?![\w$])", re.IGNORECASE)
_ROW_LEVEL_REFUSED = (AggCall, TransformCall, WindowCall, StarSource)


def _top_level_where(text: str) -> Optional[re.Match[str]]:
    """The first ``WHERE`` keyword in ``text``'s code outside every parenthesis."""
    depth = 0
    for region in _regions(text):
        if region.kind != "code":
            continue
        cursor = region.start
        for match in _WHERE_RE.finditer(text, region.start, region.end):
            chunk = text[cursor:match.start()]
            depth += chunk.count("(") - chunk.count(")")
            cursor = match.start()
            if depth == 0:
                return match
        chunk = text[cursor:region.end]
        depth += chunk.count("(") - chunk.count(")")
    return None


def _closing_paren(text: str, open_at: int) -> int:
    """The offset of the ``)`` closing the ``(`` at ``open_at``; ``-1`` when unclosed."""
    depth = 0
    for region in _regions(text):
        if region.kind != "code" or region.end <= open_at:
            continue
        for i in range(max(region.start, open_at), region.end):
            depth += (text[i] == "(") - (text[i] == ")")
            if depth == 0:
                return i
    return -1


def _row_level(text: str, *, part: str) -> ParsedExpr:
    parsed = parse_expr(text, windows=True)
    for node in walk_parsed(parsed):
        if isinstance(node, _ROW_LEVEL_REFUSED):
            kind = "a window" if isinstance(node, WindowCall) else "an aggregate or a transform"
            raise ValueError(f"Mapping expression {part} {text!r} uses {kind}; a mapping expression is row-level.")
    return parsed


def parse_mapping_expression(text: str) -> MappingExpression:
    """Read ``<value>`` or ``<value> WHERE ( <predicate> )``, each a row-level expression; raises ``ValueError``."""
    where = _top_level_where(text)
    if where is None:
        return MappingExpression(value=_row_level(text, part="value"))
    value, rest = text[:where.start()], text[where.end():]
    opening = len(rest) - len(rest.lstrip())
    if not rest.strip().startswith("("):
        raise ValueError(f"Mapping expression {text!r}: the predicate after WHERE is written in parentheses.")
    close = _closing_paren(rest, opening)
    if close < 0:
        raise ValueError(f"Mapping expression {text!r}: the predicate's parenthesis is not closed.")
    if rest[close + 1:].strip():
        raise ValueError(f"Mapping expression {text!r}: nothing may follow the parenthesised predicate.")
    if not value.strip():
        raise ValueError(f"Mapping expression {text!r}: there is no value before WHERE.")
    return MappingExpression(value=_row_level(value, part="value"),
                             predicate=_row_level(rest[opening + 1:close], part="predicate"))


def _rewritten(text: str, *, spans: List[_ColonSpan], start: int, end: int, units: frozenset[str]) -> str:
    """``text[start:end]`` with its outermost colon spans replaced by their twins."""
    out: List[str] = []
    cursor = start
    for span in spans:
        if span.start < cursor or span.end > end:
            continue
        args = "" if span.args is None else _rewritten(
            text, spans=spans, start=span.args[0], end=span.args[1], units=units,
        )
        source = text[span.source[0]:span.source[1]]
        out += [text[cursor:span.start], colon_twin(source=source, name=span.name, args=args, units=units)]
        cursor = span.end
    out.append(text[cursor:end])
    return "".join(out)


__all__ = [
    "AggCall", "ArrayLit", "Arith", "BoolOp", "CallSource", "Cast", "Cmp", "DottedRef", "Literal", "ParsedExpr",
    "BoundVariable", "Placeholder", "Ref", "ScalarCall", "StarSource", "TemporalLiteral", "TransformCall", "TupleLit", "UnaryOp",
    "CallSpan", "DateTrunc", "MappingExpression", "NamedArg", "DATE_TRUNC_FORM", "call_spans", "legacy_spans",
    "parse_mapping_expression",
    "aggregation_reading", "bind_variables", "call_keywords", "call_source", "canonical_metric_text",
    "classify_arguments", "colon_twin", "comparison_hint",
    "bare_literal_text", "dotted_quoting", "drop_root_hop", "drop_root_qualifier", "has_bindings", "keyword_spelling",
    "legacy_result_name_seed", "map_refs", "metric_dependency_problems", "metric_text_at_path", "onto_path",
    "quote_blind_metric_text",
    "parse_bound_expr", "parse_expr", "parse_template", "placeholder_names", "positional_hint", "referenced_paths",
    "rewrite_colon_spans", "root_relative", "split_entity_agg_ref", "StringLiteralSpan", "string_literal_spans",
    "text_paths", "unknown_call_hint", "value_text", "variable_names", "walk_parsed", "walk_parsed_refs",
]
