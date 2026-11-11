"""The Mode-B grammar.

``parse_expr(text) -> ParsedExpr`` parses a Mode-B expression (``ModelMeasure.formula``,
query measures / dimensions / filters / order) with one SQL-compatible pyparsing grammar —
pure syntax, no scope resolution or named-measure expansion (binder's job). Every spelling
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
from functools import lru_cache, partial
from itertools import count
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
    RESERVED_NAMES,
    TimeGranularity,
    ambiguous_weekday_message,
    normalize_aggregation_name,
)
from slayer.core.errors import (
    CallArgumentError,
    GranularityCallError,
    IllegalWindowInFilterError,
    ReservedFunctionNameError,
    UnknownFunctionError,
    VariableError,
)
from slayer.core.json_path import JsonPathError, check_key, json_path_text, parse_json_path
from slayer.core.legacy_spellings import LegacySpan, note_legacy_spellings
from slayer.core.keys import DATE_UNIT_ARGS, RESERVED_SCALAR_NAMES, SCALAR_FUNCTIONS, check_scalar_arity
from slayer.core.names import match_name
from slayer.core.refs import split_agg_suffix
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
    # Written in the colon spelling ``source:agg``; not part of identity.
    colon: bool = False

    def __eq__(self, other: object) -> bool:
        return isinstance(other, AggCall) and self._identity() == other._identity()

    def __hash__(self) -> int:
        return hash(("AggCall", self._identity()))

    def _identity(self) -> tuple:
        return (self.source, self.agg, self.args, self.kwargs, self.filter, self.library_error)


class TransformCall(_BaseNode):
    op: str
    input: Any
    args: Tuple[Any, ...] = ()
    kwargs: Tuple[Tuple[str, Any], ...] = ()


class ScalarCall(_BaseNode):
    name: str
    args: Tuple[Any, ...] = ()
    # The callee and arguments as written (a function or spelling an owning model's aggregation may shadow; a
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


ParsedExpr = Union[
    Ref, DottedRef, StarSource, Literal, TemporalLiteral, Placeholder, TupleLit, ArrayLit, BoundVariable,
    AggCall, TransformCall, ScalarCall, DateTrunc, NamedArg, Cast,
    Arith, UnaryOp, Cmp, BoolOp,
]


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
    "Optional blocks '{? ... ?}' are supported only on raw-SQL model surfaces, not in Mode-B expressions."
)
_DISTINCT_ONLY = "DISTINCT is supported only as count(distinct x), i.e. count_distinct(x)"


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
    """A single-name call ``callee(…)`` at ``text[start:end]``: its callee as written and argument count."""

    model_config = ConfigDict(frozen=True)

    callee: str
    start: int
    end: int
    arguments: int


class _State:
    """Per-parse scratch: the expression text, the farthest failure offset, and the calls and method calls read."""

    def __init__(self) -> None:
        self.text = ""
        self.farthest = 0
        self.calls: Dict[Tuple[int, int], CallSpan] = {}
        self.methods: Dict[Tuple[int, int], LegacySpan] = {}

    def reset(self, text: str) -> None:
        self.text, self.farthest, self.calls, self.methods = text, 0, {}, {}


_STATE = _State()
_PARSE_LOCK = threading.Lock()


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


_RUN_CHAR = r"[^\x00-\x20\x7f!-/:-@\[-^`{-~\s]"
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


def _string(toks: List[Any]) -> Literal:
    return Literal(value=toks[0][1:-1].replace("''", "'"))


_NUMBER = _on(_t(pp.Regex(r"(?:\d+\.\d*|\.\d+|\d+)(?:[eE][+-]?\d+)?").set_name("number")), _number)
_STRING = _on(_t(pp.Regex(r"'(?:[^']|'')*'").set_name("string")), _string)
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
        raise _Problem(f"{_LITERAL_VALUES}; a variable cannot name a function, column or path.")
    return Placeholder(name=raw[1:-1], start=loc)


def _reject_placeholder(_s: str, _loc: int, _toks: pp.ParseResults) -> None:
    raise _Problem(f"{_LITERAL_VALUES}; a variable cannot name a column or path segment.")


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


_ARG_END = pp.FollowedBy(_sym(r"[,)]", "',' or ')'"))
_STAR_ARG = _on((pp.Suppress(STAR) + _ARG_END), lambda _toks: StarSource())
_PATH_STAR_ARG = _on((_PATH_STAR + _ARG_END), lambda toks: _star_path_node(toks[0][0]))
_SEQ_ITEMS = pp.Group(pp.Optional(pp.DelimitedList(expr, allow_trailing_delim=True)))
_LIST_VALUE = _on((LBRACK + _SEQ_ITEMS + RBRACK), lambda toks: tuple(toks[0]))
_TUPLE_VALUE = _on((LPAR + pp.Group(expr + pp.OneOrMore(COMMA + expr) + pp.Optional(COMMA)) + RPAR), lambda toks: tuple(toks[0]))
_DISTINCT_VALUE = _kw("distinct") + _fail(f"{_DISTINCT_ONLY}.")
_KW_VALUE = _DISTINCT_VALUE | _LIST_VALUE | _TUPLE_VALUE | _STAR_ARG | _PATH_STAR_ARG | expr
_KWARG = _on((_BARE + EQ + _KW_VALUE), lambda toks: NamedArg(name=toks[0].name, value=toks[1]))
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
    first)."""

    __slots__ = ("words", "items", "start", "end")

    def __init__(self, words: List[_Word], items: List[Any], start: int, end: int) -> None:
        self.words = words
        self.items = items
        self.start = start
        self.end = end


def _resolve(node: Any) -> Any:
    return _dispatch_pending(node) if isinstance(node, _PendingCall) else node


def _dispatch_pending(call: _PendingCall) -> Any:
    words = call.words
    name = words[-1].name
    args, kwargs, distinct = _split_args(call.items, call=name)
    if distinct:
        values = [a for a in args if not isinstance(a, NamedArg)]
        if name.lower() != "count" or len(words) > 1 or len(values) != 1 or isinstance(args[0], (StarSource, NamedArg)):
            raise _Problem(f"{_DISTINCT_ONLY}.")
        name = "count_distinct"
    if len(words) > 1:
        return _method_call(
            path=tuple(w.name for w in words[:-1]), name=name, args=args, kwargs=kwargs, span=(call.start, call.end),
        )
    if not words[0].quoted:
        _STATE.calls[(call.start, call.end)] = CallSpan(
            callee=words[0].raw, start=call.start, end=call.end, arguments=len(call.items),
        )
    return _dispatch_call(name, args=args, kwargs=kwargs, original=_STATE.text)


_CALL_ATOM = _on(pp.Located(_CALL), lambda toks: _PendingCall(
    words=list(toks[1][0]), items=list(toks[1][1]), start=toks[0], end=toks[2],
))

_COLON_SOURCE = (_on(pp.Suppress(STAR), lambda _toks: StarSource())
                 | _on(_PATH_STAR.copy(), lambda toks: _star_path_node(toks[0][0]))
                 | _on(_PATH.copy(), lambda toks: _path_node(list(toks[0]))))
_COLON_AGG = _COLON_SOURCE + COLON + _BARE + pp.Optional(LPAR + _arg_list(_ANY_ARG, _ANY_ARG) + RPAR)


def _colon_node(toks: List[Any]) -> AggCall:
    source, agg = toks[0], toks[1].name
    args, kwargs, distinct = _split_args(list(toks[2]) if len(toks) > 2 else [], call=agg)
    if distinct:
        raise _Problem(f"{_DISTINCT_ONLY}.")
    _reject_bare_star_args(args, kwargs, func_name=agg, original=_STATE.text)
    return AggCall(source=source, agg=agg, args=args, kwargs=kwargs, colon=True)


_on(_COLON_AGG, _colon_node)
_REF = _on(_PATH.copy(), lambda toks: _path_node(list(toks[0])))

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
    value, path, type_text = toks
    if type_text not in _RETURNING_TYPES:
        raise _Problem(f"json_value() RETURNING {type_text} is not supported; use one of {', '.join(sorted(_RETURNING_TYPES))}.")
    return Cast(source=ScalarCall(name="json_value", args=(value, path)), type_text=type_text, try_=True)


_on(_JSON_VALUE_RETURNING, _json_value_returning)
_POSITION = _on((pp.Suppress(_kw("position")) + LPAR + _concat_level + pp.Suppress(_kw("in")) + expr + RPAR), lambda toks: ScalarCall(name="instr", args=(toks[1], toks[0])))
_SUBSTRING = _on((pp.Suppress(_kw("substring")) + LPAR + expr + pp.Suppress(_kw("from")) + expr
              + pp.Optional(pp.Suppress(_kw("for")) + expr) + RPAR), lambda toks: ScalarCall(name="substr", args=tuple(toks)))
_TRIM_FORM = (_kw("trim") + LPAR + _kw("both", "leading", "trailing")) + _fail(
    "TRIM(BOTH | LEADING | TRAILING …) is not supported; use trim(x), ltrim(x) or rtrim(x).",
)

# -- typed literals, intervals, clock keywords ----------------------------------

_TEMPORAL = _on((_kw("date", "timestamp") + _STRING), lambda toks: TemporalLiteral(kind=toks[0].lower(), text=toks[1].value))
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
    r"(?i:case|cast|try_cast|extract|position|substring|trim|date|timestamp|time|interval|current_date|current_timestamp"
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
    raise IllegalWindowInFilterError(
        filter_expr=_STATE.text,
        source="raw OVER(...) is not allowed in Mode-B DSL",
        suggestion="use a transform instead (rank, percent_rank, dense_rank, ntile, cumsum, lag, lead, time_shift, …).",
    )


_OVER = (_kw("over") + pp.FollowedBy(LPAR)).set_parse_action(_reject_over)
_PRIMARY = _ATOM + pp.Group(pp.ZeroOrMore(pp.Group(_FILTER_CLAUSE) | _OVER))


def _primary_node(toks: List[Any]) -> Any:
    node = _resolve(toks[0])
    for clause in toks[1]:
        if not isinstance(node, AggCall):
            raise _Problem("FILTER (WHERE …) applies only to an aggregation, e.g. sum(x) FILTER (WHERE c).")
        if node.filter is not None:
            raise _Problem("an aggregation takes one FILTER (WHERE …) clause.")
        node = node.model_copy(update={"filter": clause[0]})
    return node


_on(_PRIMARY, _primary_node)


def _fold_casts(toks: pp.ParseResults) -> Any:
    items = list(toks[0])
    node = items[0]
    for type_text in items[1:]:
        node = Cast(source=node, type_text=type_text)
    return node


def _signed(toks: pp.ParseResults) -> Any:
    sign, node = toks[0]
    return UnaryOp(op="-", operand=node) if sign == "-" else node


def _pow(toks: pp.ParseResults) -> Any:
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
    return "is not" if negate else "is"


def _negated(node: Any, negate: bool) -> Any:
    return UnaryOp(op="not", operand=node) if negate else node


def _truth_test(left: Any, toks: List[Any]) -> Any:
    negate, word = toks[0], toks[1].lower()
    known = Cmp(op=_is_op(True), left=left, right=Literal(value=None))
    test = BoolOp(op="and", operands=(known, left if word == "true" else UnaryOp(op="not", operand=left)))
    return _negated(test, negate)


def _regex_operator(left: Any, toks: List[Any]) -> Any:
    """``~`` / ``~*`` / ``!~`` / ``!~*`` as ``[NOT] regexp_like(left, p[, 'i'])``."""
    op, pattern = toks
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
        return Cmp(op="not in" if negate else "in", left=left, right=right)
    equalities = tuple(Cmp(op="==", left=left, right=item) for item in right.items)
    return _negated(equalities[0] if len(equalities) == 1 else BoolOp(op="or", operands=equalities), negate)


_IN_LIST = ((LPAR + _PLACEHOLDER_ATOM + RPAR)
            | _PLACEHOLDER_ATOM
            | _on((LPAR + _SEQ_ITEMS + RPAR), lambda toks: _in_rhs_elements(list(toks[0])))
            | _on((LBRACK + _SEQ_ITEMS + RBRACK), lambda toks: _in_rhs_elements(list(toks[0])))
            | _STRING
            | _fail("the right-hand side of in / not in must be a literal list, e.g. status in ('a', 'b')."))

_CMP_SUFFIX = (
    _suffix(_sym(r"==|!=|<>|<=|>=|=|<|>", "comparison") + _concat_level,
            lambda left, t: Cmp(op=_CMP_OPS[t[0]], left=left, right=t[1], single_eq=t[0] == "="))
    | _suffix(_IS + _NOT_FLAG + pp.Suppress(_kw("distinct")) + pp.Suppress(_kw("from")) + _concat_level,
              lambda left, t: Cmp(op="is not distinct from" if t[0] else "is distinct from", left=left, right=t[1]))
    | _suffix(_IS + _NOT_FLAG + pp.Suppress(_kw("null") | pp.Regex(r"None\b")),
              lambda left, t: Cmp(op=_is_op(t[0]), left=left, right=Literal(value=None)))
    | _suffix(_IS + _NOT_FLAG + (_kw("true", "false")), _truth_test)
    | _suffix(_IS + _NOT_FLAG + _concat_level,
              lambda left, t: Cmp(op=_is_op(t[0]), left=left, right=t[1]))
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


def parse_expr(text: str) -> ParsedExpr:
    """Parse a Mode-B expression string into a ``ParsedExpr``; a deprecated spelling read is noted for its warning.

    Raises ``ValueError`` (empty / syntax / unsupported form / reserved prefix),
    ``UnknownFunctionError``, or ``IllegalWindowInFilterError`` (raw ``OVER(...)``).
    """
    reading = _read(text)
    if reading.legacy:
        note_legacy_spellings(reading.legacy)
    return reading.parsed


def legacy_spans(text: str) -> Tuple[LegacySpan, ...]:
    """The deprecated spellings in template ``text``, each with its canonical twin; none when it does not parse."""
    try:
        return _read(_bound_strings(text)).legacy
    except Exception:  # noqa: BLE001 — unparseable text has no spans
        return ()


def call_spans(text: str) -> Tuple[CallSpan, ...]:
    """The single-name calls in template ``text``; none when it does not parse."""
    try:
        return _read(_bound_strings(text)).calls
    except Exception:  # noqa: BLE001 — unparseable text has no spans
        return ()


@lru_cache(maxsize=8192)
def _read(text: str) -> _Reading:
    if not text or not text.strip():
        raise ValueError("Empty Mode-B expression.")
    with _PARSE_LOCK:
        _STATE.reset(text)
        try:
            masked = _mask_placeholders(text)
            if not masked.strip():
                raise ValueError("Empty Mode-B expression.")
            parsed = cast(ParsedExpr, _EXPRESSION.parse_string(masked, parse_all=True)[0])
        except _Problem as problem:
            raise ValueError(f"Invalid Mode-B expression {text!r}: {problem.detail}") from None
        except pp.ParseBaseException as e:
            raise ValueError(f"Invalid Mode-B expression {text!r}: {_syntax_message(text, e)}") from None
        methods, calls = list(_STATE.methods.values()), sorted(_STATE.calls.values(), key=lambda c: c.start)
    legacy = tuple(sorted([*_text_legacy_spans(text), *methods], key=lambda s: (s.start, s.end)))
    if any(_is_interval(n) for n in walk_parsed(parsed)):
        parsed = _deferred_intervals(parsed, message=(
            f"Invalid Mode-B expression {text!r}: interval(n, unit) is only valid as "
            f"`x + interval(n, unit)`, `interval(n, unit) + x` or `x - interval(n, unit)`; "
            f"otherwise use date_add(x, n, unit)."
        ))
    return _Reading(parsed=parsed, legacy=legacy, calls=tuple(calls))


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


def parse_template(text: str) -> ParsedExpr:
    """``parse_expr`` of ``text`` before its string-literal variables are bound: the shape binding parses."""
    return parse_expr(_bound_strings(text))


def variable_names(text: str) -> List[str]:
    """The variables binding ``text`` looks up, in text order."""
    names: Dict[str, None] = {}
    for m, _ in _lookups(text, [r for r in _regions(text) if r.kind in _BINDING_KINDS]):
        if re.fullmatch(_VAR_NAME, m.group(1)):
            names[m.group(1)] = None
    return list(names)


def bind_variables(text: str, variables: Mapping[str, Any]) -> str:
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
    template = parse_expr(_spliced(text, variables=variables, regions=regions, kinds=("string",), code_braces=True))
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


_IN_OPS = frozenset({"in", "not in"})


def parse_bound_expr(text: str, variables: Mapping[str, Any]) -> ParsedExpr:
    """``parse_expr`` of ``text``, each placeholder ``bind_variables`` left as a whole ``IN`` right-hand side bound to
    its value in ``variables``."""
    return _with_in_variables(parse_expr(text), variables=variables)


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
        return _value_text(name, value).replace("'", "''")
    position = next(ordinal)
    if position in in_slots:
        return whole
    if position in text_slots:
        return _literal_text(name, value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_literal_text(name, v) for v in value) + "]"
    return _bare_literal_text(name, value)


def _value_text(name: str, value: Any) -> str:
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
    return _value_text(name, value)


def _bare_literal_text(name: str, value: Any) -> str:
    if isinstance(value, str):
        stripped = value.strip()
        if _NUMBER_TEXT_RE.fullmatch(stripped):
            return stripped
        raise ValueError(
            f"{_LITERAL_VALUES}: variable '{name}' holds the text {value!r} in a number or boolean slot; "
            f"put the placeholder inside quotes ('{{{name}}}') to use it as a string."
        )
    return _value_text(name, value)


# ---------------------------------------------------------------------------
# Saved-measure dependencies
# ---------------------------------------------------------------------------


class _MeasureScope(BaseModel):
    """What a saved measure's names resolve against."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    names: frozenset[str]
    aggregations: frozenset[str]
    columns: Optional[frozenset[str]] = None
    join_path: Optional[Callable[[Tuple[str, ...]], bool]] = None


def measure_dependency_problems(
    formulas: List[Tuple[Optional[str], str]], *, aggregations: frozenset[str] = frozenset(),
    columns: Optional[frozenset[str]] = None, join_path: Optional[Callable[[Tuple[str, ...]], bool]] = None,
) -> List[Optional[str]]:
    """Per ``(name, formula)``: why the saved measure cannot resolve among its peers, else ``None``.

    Outside an aggregation a bare name must be a peer measure; an aggregation must be built-in
    or one of ``aggregations``, and, given ``columns``, read only those columns or paths
    ``join_path`` accepts; a measure on a cycle, or reading one that cannot resolve, cannot
    resolve either."""
    scope = _MeasureScope(names=frozenset(name for name, _ in formulas if name), aggregations=aggregations,
                          columns=columns, join_path=join_path)
    own: Dict[int, Optional[str]] = {}
    deps: Dict[int, set] = {}
    for i, (_name, formula) in enumerate(formulas):
        try:
            refs, problem = _measure_refs(parse_expr(formula), scope=scope)
        except ValueError as e:
            refs, problem = set(), str(e)
        own[i], deps[i] = problem, refs
    index = {name: i for i, (name, _) in enumerate(formulas) if name}
    out: List[Optional[str]] = []
    for i, (name, _) in enumerate(formulas):
        out.append(_resolution_problem(i, own=own, deps=deps, index=index, path=(name or "",)))
    return out


def _resolution_problem(
    i: int, *, own: Dict[int, Optional[str]], deps: Dict[int, set], index: Dict[str, int], path: Tuple[str, ...],
) -> Optional[str]:
    if own[i] is not None:
        return own[i]
    for dep in sorted(deps[i]):
        if dep in path:
            return f"saved measure {dep!r} has a cyclic reference: {' → '.join((*path, dep))}"
        problem = _resolution_problem(index[dep], own=own, deps=deps, index=index, path=(*path, dep))
        if problem is not None:
            return problem if dep == path[-1] else f"{dep!r} cannot resolve: {problem}"
    return None


def _measure_refs(node: Any, *, scope: _MeasureScope) -> Tuple[set, Optional[str]]:
    """The peer measures ``node`` reads outside aggregations, and the first problem found."""
    if isinstance(node, Ref):
        if node.name in scope.names:
            return {node.name}, None
        match = difflib.get_close_matches(word=node.name, possibilities=sorted(scope.names), n=1)
        hint = f" Did you mean '{match[0]}'?" if match else ""
        return set(), (
            f"'{node.name}' is not a saved measure.{hint} Reference a saved measure by its bare name, or "
            f"aggregate a column (e.g., 'sum({node.name})'). For COUNT(*), use 'count(*)'."
        )
    if isinstance(node, DottedRef):
        dotted = ".".join(node.parts)
        return set(), f"Cross-model measure '{dotted}' must include an aggregation (e.g., 'sum({dotted})')."
    if isinstance(node, AggCall):
        known = normalize_aggregation_name(node.agg) in BUILTIN_AGGREGATIONS or node.agg in scope.aggregations
        return set(), _aggregation_input_problem(node, scope=scope) if known else f"unknown aggregation {node.agg!r}"
    children = [node.input] if isinstance(node, TransformCall) else _operands(node)
    refs: set = set()
    for child in children:
        found, problem = _measure_refs(child, scope=scope)
        if problem is not None:
            return refs, problem
        refs |= found
    return refs, None


def _aggregation_input_problem(node: AggCall, *, scope: _MeasureScope) -> Optional[str]:
    """The first name an aggregation reads that is neither a column nor a join path; unchecked without columns."""
    if scope.columns is None:
        return None
    inputs = [node.source, node.filter, *node.args, *(v for _, v in node.kwargs)]
    for leaf in (ref for value in inputs if value is not None for ref in walk_parsed_refs(value)):
        if isinstance(leaf, Ref) and leaf.name not in scope.columns:
            return f"aggregation {node.agg!r} reads {leaf.name!r}, which is not a column of the model"
        if isinstance(leaf, DottedRef) and not (scope.join_path is not None and scope.join_path(tuple(leaf.parts))):
            return f"aggregation {node.agg!r} reads {'.'.join(leaf.parts)!r}, which no join path reaches"
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


# The node kinds an ``AggCall.source`` may take: a column, star, or an aggregation-free
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
            f"must be built from columns, literals, arithmetic, comparisons, "
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
    """``node`` with every column leaf (and ``*``) read under the join ``path``; ``memo`` rewrites a node shared by
    ``args`` and ``written_args`` once."""
    memo = {} if memo is None else memo
    if id(node) in memo:
        return memo[id(node)]
    memo[id(node)] = _onto_path_node(node, path=path, memo=memo)
    return memo[id(node)]


def _onto_path_node(node: Any, *, path: Tuple[str, ...], memo: Dict[int, Any]) -> Any:
    unquoted = (False,) * len(path)
    if isinstance(node, Ref):
        return DottedRef(parts=(*path, node.name), quoted=node.quoted, quoted_parts=(*unquoted, node.quoted))
    if isinstance(node, DottedRef):
        own = tuple(node.quoted_at(i) for i in range(len(node.parts)))
        return DottedRef(parts=(*path, *node.parts), quoted=node.quoted, quoted_parts=(*unquoted, *own))
    if isinstance(node, StarSource):
        return DottedRef(parts=(*path, "*"))
    if isinstance(node, ScalarCall):
        args = tuple(_onto_path(a, path=path, memo=memo) for a in node.args)
        written = None if node.written_args is None else tuple(_onto_path(a, path=path, memo=memo) for a in node.written_args)
        return node.model_copy(update={"args": args, "written_args": written})
    if isinstance(node, Cast):
        return node.model_copy(update={"source": _onto_path(node.source, path=path, memo=memo)})
    if isinstance(node, (DateTrunc, NamedArg)):
        return node.model_copy(update={"value": _onto_path(node.value, path=path, memo=memo)})
    if isinstance(node, (Arith, Cmp)):
        return node.model_copy(update={"left": _onto_path(node.left, path=path, memo=memo), "right": _onto_path(node.right, path=path, memo=memo)})
    if isinstance(node, UnaryOp):
        return node.model_copy(update={"operand": _onto_path(node.operand, path=path, memo=memo)})
    if isinstance(node, BoolOp):
        return node.model_copy(update={"operands": tuple(_onto_path(o, path=path, memo=memo) for o in node.operands)})
    return node


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
        return "may read only bare column names of that path"
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


def aggregation_reading(call: ScalarCall) -> Optional[AggCall]:
    """The aggregation call ``call`` reads as when its owning model defines ``call.written``; ``None`` when it has no
    column-bearing value argument."""
    if call.written is None:
        return None
    args = call.written_args if call.written_args is not None else call.args
    return _aggregation_candidate(call.written, args=args, kwargs=(), error=None)


def _aggregation_candidate(
    func_name: str, *, args: Tuple[Any, ...], kwargs: Tuple[Tuple[str, Any], ...], error: Optional[str],
) -> Optional[AggCall]:
    """``func_name(args)`` as an aggregation call over a column-bearing value, else ``None``."""
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
    # ``agg`` stores the RAW token so colon and functional spellings collapse to one
    # AggCall; ``first``/``last`` dispatch at bind by the operand's type.
    canonical_agg = normalize_aggregation_name(func_name)
    if canonical_agg in BUILTIN_AGGREGATIONS:
        if not args:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: aggregation "
                f"{func_name!r} requires a value argument, e.g. "
                f"{func_name}(column)."
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
            deferred = _aggregation_candidate(func_name, args=args, kwargs=kwargs, error=message)
            if deferred is None:
                raise (GranularityCallError if bucket else ValueError)(message) from None
            return deferred
        if isinstance(built, ScalarCall):
            return built.model_copy(update={"written": func_name, "written_args": args})
        return built

    # Normalised to lower case so ``COALESCE`` / ``coalesce`` intern to one key.
    if func_name.lower() in SCALAR_FUNCTIONS:
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
        name = func_name.lower()
        try:
            date_args = _date_args(name=name, args=args, original=original)
        except ValueError as error:
            deferred = _aggregation_candidate(func_name, args=args, kwargs=kwargs, error=str(error))
            if deferred is None:
                raise
            return deferred
        return ScalarCall(name=name, args=date_args, written=func_name)

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
        raise GranularityCallError.no_call_form(func_name.lower(), operand=", ".join(map(canonical_measure_text, args)))
    raise UnknownFunctionError(
        name=func_name,
        location=original,
        suggestion=ambiguous_weekday_message(func_name) if func_name.lower() in AMBIGUOUS_WEEKDAY_NAMES else (
            f"Mode-B accepts the closed scalar allowlist "
            f"({sorted(SCALAR_FUNCTIONS)}), transforms "
            f"({sorted(ALL_TRANSFORMS)}), and aggregations "
            f"(`sum(revenue)`); custom "
            f"aggregation names must be defined on the model."
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
        or unit in DATE_PART_ALIASES or unit in AMBIGUOUS_WEEKDAY_NAMES
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


# Spellings collapsed into one library node (engine P2); each name is reserved (``SCALAR_SPELLINGS``).
_SPELLINGS: Dict[str, Callable[[Tuple[Any, ...]], ParsedExpr]] = {
    "strpos": lambda args: ScalarCall(name="instr", args=args),
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
    if not (isinstance(arg, Literal) and isinstance(arg.value, str)):
        spelled = keyword_spelling(arg)
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: the {what} of {name}() must be a "
            f"string literal, one of: {accepted}." + (f" Note: {positional_hint(spelled)}" if spelled else "")
        )
    word = arg.value.lower()
    if name == "date_part":
        if word in AMBIGUOUS_WEEKDAY_NAMES:
            raise ValueError(f"Invalid Mode-B expression {original!r}: {ambiguous_weekday_message(arg.value)}")
        word = DATE_PART_ALIASES[word].value if word in DATE_PART_ALIASES else word
    if word not in words:
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: unknown {name}() {what} "
            f"{arg.value!r}; accepted: {accepted}."
        )
    return word


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
    """A reference's join ``hops`` without a leading qualifier naming the query root ``root`` (exactly when
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
                memo[id(node)] = Ref(name=parts[0], quoted=quoted_parts[0])
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


def _is_null(node: Any) -> bool:
    return isinstance(node, Literal) and node.value is None


def _argument_text(node: Any) -> str:
    """A non-scalar call's argument: a ``name = value`` comparison parenthesised, so it never reads as a keyword."""
    text = canonical_measure_text(node)
    if isinstance(node, Cmp) and node.op == "==" and isinstance(node.left, Ref) and not node.left.quoted:
        return f"({text})"
    return text


def _canonical_call_params(
    args: Tuple[Any, ...], kwargs: Tuple[Tuple[str, Any], ...], *, bare_direction: bool = False,
) -> str:
    parts = [
        _canonical_kwarg(a.name, a.value, bare_direction=bare_direction) if isinstance(a, NamedArg) else _argument_text(a)
        for a in args
    ]
    parts += [_canonical_kwarg(k, v, bare_direction=bare_direction) for k, v in kwargs]
    return f"({', '.join(parts)})" if parts else ""


def _canonical_kwarg(name: str, value: Any, *, bare_direction: bool) -> str:
    """``name=value``; a transform's recognised ``direction`` renders as its bare normalised value."""
    if bare_direction and name == "direction" and isinstance(value, Literal):
        direction = normalize_direction(value.value)
        if direction is not None:
            return direction
    return f"{name}={_canonical_kwarg_text(value)}"


def _canonical_kwarg_text(value: Any) -> str:
    if isinstance(value, tuple):
        return f"[{', '.join(canonical_measure_text(v) for v in value)}]"
    return canonical_measure_text(value)


def _identifier_text(name: str) -> str:
    return name if name.isidentifier() else '"' + name.replace('"', '""') + '"'


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


def _operand_text(node: Any, *, minimum: int) -> str:
    """``node``'s canonical text, parenthesised when it binds looser than ``minimum``."""
    text = canonical_measure_text(node)
    return f"({text})" if _precedence(node) < minimum else text


def canonical_measure_text(parsed: Any) -> str:  # NOSONAR(S3776) — flat per-node-kind rendering table; each branch is one spelling rule.
    """Render a ``ParsedExpr`` to canonical colon-spelling text that parses back to the same tree."""
    if isinstance(parsed, Ref):
        return _identifier_text(parsed.name)
    if isinstance(parsed, DottedRef):
        return ".".join(p if p == "*" else _identifier_text(p) for p in parsed.parts)
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
        return f"({', '.join(canonical_measure_text(e) for e in parsed.elements)})"
    if isinstance(parsed, ArrayLit):
        return f"[{', '.join(canonical_measure_text(e) for e in parsed.elements)}]"
    if isinstance(parsed, AggCall):
        return _canonical_agg_text(parsed)
    if isinstance(parsed, TransformCall):
        inner = _argument_text(parsed.input)
        params = _canonical_call_params(parsed.args, parsed.kwargs, bare_direction=True)
        return f"{parsed.op}({inner}{', ' + params[1:-1] if params else ''})"
    if isinstance(parsed, ScalarCall):
        return f"{parsed.name}({', '.join(canonical_measure_text(a) for a in parsed.args)})"
    if isinstance(parsed, DateTrunc):
        return f"date_trunc('{parsed.unit}', {canonical_measure_text(parsed.value)})"
    if isinstance(parsed, NamedArg):
        return f"{parsed.name}={_canonical_kwarg_text(parsed.value)}"
    if isinstance(parsed, Cast):
        fn = "TRY_CAST" if parsed.try_ else "CAST"
        return f"{fn}({canonical_measure_text(parsed.source)} AS {parsed.type_text})"
    if isinstance(parsed, (Arith, Cmp)):
        prec = _precedence(parsed)
        left = _operand_text(parsed.left, minimum=prec + 1 if isinstance(parsed, Cmp) else prec)
        if isinstance(parsed, Cmp) and parsed.op in ("is", "is not") and _is_null(parsed.right):
            return f"{left} {parsed.op.upper()} NULL"
        right = _operand_text(parsed.right, minimum=prec + 1)
        return f"{left} {_CANONICAL_OPS.get(parsed.op, parsed.op)} {right}"
    if isinstance(parsed, UnaryOp):
        operand = _operand_text(parsed.operand, minimum=_precedence(parsed) + 1)
        return f"not {operand}" if parsed.op == "not" else f"{parsed.op}{operand}"
    if isinstance(parsed, BoolOp):
        prec = _precedence(parsed)
        return f" {parsed.op} ".join(_operand_text(o, minimum=prec + 1) for o in parsed.operands)
    return str(parsed)


def _canonical_agg_text(parsed: AggCall) -> str:
    source = canonical_measure_text(parsed.source)
    if isinstance(parsed.source, (Ref, DottedRef, StarSource)):
        text = f"{source}:{parsed.agg}{_canonical_call_params(parsed.args, parsed.kwargs)}"
    else:
        # Expression source (``sum(amount - cost)``): render functionally so the
        # text can't collide with a distinct parse tree like ``amount - cost:sum``.
        parts = [_argument_text(parsed.source)]
        parts += [_argument_text(a) for a in parsed.args]
        parts += [f"{k}={_canonical_kwarg_text(v)}" for k, v in parsed.kwargs]
        text = f"{parsed.agg}({', '.join(parts)})"
    if parsed.filter is not None:
        text += f" FILTER (WHERE {canonical_measure_text(parsed.filter)})"
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
    """``(prefix, agg_suffix)`` of a single aggregated-column entity
    reference: ``sum(orders.amount)`` and the legacy ``orders.amount:sum`` split
    identically.

    Legacy colon and call-free text splits exactly like
    :func:`slayer.core.refs.split_agg_suffix`. Functional text must parse to
    an ``AggCall`` over a pure column / star source; multi-column expression
    text (``sum(a - b)``) raises ``ValueError`` — an expression is not an
    entity.
    """
    prefix, suffix = split_agg_suffix(raw)
    if suffix is not None or "(" not in raw:
        return prefix, suffix
    parsed = parse_expr(raw)
    if not isinstance(parsed, AggCall) or parsed.filter is not None or not isinstance(
        parsed.source, (Ref, DottedRef, StarSource)
    ):
        raise ValueError(
            f"{raw!r} is not a single aggregated column reference; only "
            f"the `agg(column)` form names an entity."
        )
    source = parsed.source
    if isinstance(source, StarSource):
        prefix = "*"
    elif isinstance(source, Ref):
        prefix = source.name
    else:
        prefix = ".".join(source.parts)
    return prefix, _functional_suffix_text(raw, agg=parsed.agg)


__all__ = [
    "AggCall", "ArrayLit", "Arith", "BoolOp", "CallSource", "Cast", "Cmp", "DottedRef", "Literal", "ParsedExpr",
    "BoundVariable", "Placeholder", "Ref", "ScalarCall", "StarSource", "TemporalLiteral", "TransformCall", "TupleLit", "UnaryOp",
    "CallSpan", "DateTrunc", "NamedArg", "DATE_TRUNC_FORM", "call_spans", "legacy_spans",
    "aggregation_reading", "bind_variables", "call_keywords", "call_source", "canonical_measure_text",
    "classify_arguments",
    "comparison_hint",
    "dotted_quoting", "drop_root_hop", "drop_root_qualifier", "has_bindings", "keyword_spelling", "measure_dependency_problems",
    "parse_bound_expr", "parse_expr", "parse_template", "placeholder_names", "positional_hint", "referenced_paths", "root_relative", "split_entity_agg_ref",
    "text_paths", "variable_names", "walk_parsed", "walk_parsed_refs",
]
