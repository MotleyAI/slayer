"""The Mode-B grammar.

``parse_expr(text) -> ParsedExpr`` parses a Mode-B expression (``ModelMeasure.formula``,
query measures / dimensions / filters / order) with one SQL-compatible pyparsing grammar —
pure syntax, no scope resolution or named-measure expansion (binder's job). Every spelling
of a construct collapses to one node: CASE → ``iif``, ``EXTRACT`` → ``date_part``,
``count(distinct x)`` → ``count_distinct``, ``BETWEEN`` → its conjunction, ``x::t`` →
``Cast``, interval literals → ``date_add``.
"""

from __future__ import annotations

import difflib
import re
import threading
import unicodedata
from decimal import Decimal
from functools import lru_cache
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Tuple, Union, cast

import pyparsing as pp
from pydantic import BaseModel, ConfigDict

from slayer.core.direction import normalize_direction
from slayer.core.enums import (
    ALL_TRANSFORMS,
    AMBIGUOUS_WEEKDAY_NAMES,
    BUILTIN_AGGREGATIONS,
    DATE_PART_ALIASES,
    GRANULARITY_NAMES,
    TimeGranularity,
    ambiguous_weekday_message,
    normalize_aggregation_name,
)
from slayer.core.errors import GranularityCallError, IllegalWindowInFilterError, UnknownFunctionError
from slayer.core.keys import DATE_UNIT_ARGS, SCALAR_FUNCTIONS, check_scalar_arity
from slayer.core.refs import split_agg_suffix


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


class TupleLit(_BaseNode):
    """Literal-only list RHS for ``IN`` / ``NOT IN``."""

    elements: Tuple[Literal, ...]


class AggCall(_BaseNode):
    source: Any
    agg: str
    args: Tuple[Any, ...] = ()
    kwargs: Tuple[Tuple[str, Any], ...] = ()
    # ``FILTER (WHERE …)`` predicate.
    filter: Any = None


class TransformCall(_BaseNode):
    op: str
    input: Any
    args: Tuple[Any, ...] = ()
    kwargs: Tuple[Tuple[str, Any], ...] = ()


class ScalarCall(_BaseNode):
    name: str
    args: Tuple[Any, ...] = ()


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


class BoolOp(_BaseNode):
    op: str
    operands: Tuple[Any, ...]


ParsedExpr = Union[
    Ref, DottedRef, StarSource, Literal, TemporalLiteral, Placeholder, TupleLit,
    AggCall, TransformCall, ScalarCall, Cast,
    Arith, UnaryOp, Cmp, BoolOp,
]


# ---------------------------------------------------------------------------
# Lexical helpers
# ---------------------------------------------------------------------------


# The ``__slayer_`` prefix is reserved from user input (P3).
_RESERVED_EXPR_PREFIX = "__slayer_"
# Masked ``{name}`` placeholders: the braces become private-use sentinels, so offsets hold.
_PH_OPEN, _PH_CLOSE = "", ""
_VAR_NAME = r"[A-Za-z_][A-Za-z0-9_]*"
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
    kind: str  # "code" | "string" | "ident"
    start: int
    end: int


def _regions(text: str) -> List[_Region]:
    """Split ``text`` into code, ``'…'`` string and ``"…"`` identifier spans (doubled quotes escape)."""
    out: List[_Region] = []
    i = start = 0
    n = len(text)
    while i < n:
        quote = text[i]
        if quote not in ("'", '"'):
            i += 1
            continue
        if i > start:
            out.append(_Region(kind="code", start=start, end=i))
        j = i + 1
        while j < n:
            if text[j] == quote:
                if j + 1 < n and text[j + 1] == quote:
                    j += 2
                    continue
                break
            j += 1
        end = min(j + 1, n)
        out.append(_Region(kind="string" if quote == "'" else "ident", start=i, end=end))
        i = start = end
    if start < n:
        out.append(_Region(kind="code", start=start, end=n))
    return out


def _mask_placeholders(text: str) -> str:
    """``{name}`` outside string literals → sentinel-bracketed name of the same length."""
    if _PH_OPEN in text or _PH_CLOSE in text:
        raise _Problem("unsupported character in expression.")
    parts: List[str] = []
    for region in _regions(text):
        chunk = text[region.start:region.end]
        if region.kind == "code" and ("{?" in chunk or "?}" in chunk):
            raise _Problem(_OPTIONAL_BLOCKS)
        if region.kind == "code":
            chunk = _CODE_PLACEHOLDER_RE.sub(
                lambda m: f"{_PH_OPEN}{m.group(1)}{_PH_CLOSE}" if m.group(1) else m.group(0), chunk,
            )
            if "{" in chunk or "}" in chunk:
                raise _Problem(
                    "unsupported AST node Set: a brace expression is not a variable placeholder; "
                    "write a variable as {name}."
                )
        elif region.kind == "ident" and re.search(r"\{" + _VAR_NAME + r"\}", chunk):
            raise _Problem(f"{_LITERAL_VALUES}, never an identifier.")
        parts.append(chunk)
    return "".join(parts)


class _State:
    """Per-parse scratch: the expression text and the farthest failure offset."""

    def __init__(self) -> None:
        self.text = ""
        self.farthest = 0


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
    return Literal(value=Decimal(int(text)) if text.isdigit() else Decimal(str(float(text))))


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
    return Placeholder(name=raw[1:-1])


def _reject_placeholder(_s: str, _loc: int, _toks: pp.ParseResults) -> None:
    raise _Problem(f"{_LITERAL_VALUES}; a variable cannot name a column or path segment.")


def _reject_placeholder_kwarg(_s: str, _loc: int, _toks: pp.ParseResults) -> None:
    raise _Problem(f"{_LITERAL_VALUES}; a variable cannot name a keyword argument.")


# ---------------------------------------------------------------------------
# Grammar
# ---------------------------------------------------------------------------


pp.ParserElement.enable_packrat()

expr = pp.Forward()
expr.set_name("expression")
_concat_level = pp.Forward()

_SEGMENT = _BARE | _QUOTED
_PATH = pp.Group(_SEGMENT + pp.ZeroOrMore(DOT + (_SEGMENT | _PLACEHOLDER.copy().set_parse_action(_reject_placeholder))))


def _path_node(words: List[_Word]) -> Ref | DottedRef:
    quoted = any(w.quoted for w in words)
    if len(words) == 1:
        return Ref(name=words[0].name, quoted=quoted)
    return DottedRef(parts=tuple(w.name for w in words), quoted=quoted)


_PATH_STAR = pp.Group(_PATH + DOT + pp.Suppress(STAR))


class _Kw:
    __slots__ = ("name", "value")

    def __init__(self, name: str, value: Any) -> None:
        self.name = name
        self.value = value


class _Distinct:
    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value


_ARG_END = pp.FollowedBy(_sym(r"[,)]", "',' or ')'"))
_STAR_ARG = _on((pp.Suppress(STAR) + _ARG_END), lambda _toks: StarSource())
_PATH_STAR_ARG = _on((_PATH_STAR + _ARG_END), lambda toks: DottedRef(parts=(*(w.name for w in toks[0][0]), "*"), quoted=any(w.quoted for w in toks[0][0])))
_SEQ_ITEMS = pp.Group(pp.Optional(pp.DelimitedList(expr, allow_trailing_delim=True)))
_LIST_VALUE = _on((LBRACK + _SEQ_ITEMS + RBRACK), lambda toks: tuple(toks[0]))
_TUPLE_VALUE = _on((LPAR + pp.Group(expr + pp.OneOrMore(COMMA + expr) + pp.Optional(COMMA)) + RPAR), lambda toks: tuple(toks[0]))
_DISTINCT_VALUE = _kw("distinct") + _fail(f"{_DISTINCT_ONLY}.")
_KW_VALUE = _DISTINCT_VALUE | _LIST_VALUE | _TUPLE_VALUE | _STAR_ARG | _PATH_STAR_ARG | expr
_KWARG = _on((_BARE + EQ + _KW_VALUE), lambda toks: _Kw(name=toks[0].name, value=toks[1]))
_PH_KWARG = (_PLACEHOLDER + EQ).set_parse_action(_reject_placeholder_kwarg)
_VALUE_ARG = _STAR_ARG | _PATH_STAR_ARG | expr
_DISTINCT_ARG = _on((pp.Suppress(_kw("distinct")) + _need(_VALUE_ARG, f"{_DISTINCT_ONLY}.")), lambda toks: _Distinct(value=toks[0]))
_UNPACKING = _sym(r"\*\*", "'**'") + _fail("dictionary unpacking (**kwargs) is not supported in calls.")
_ANY_ARG = _UNPACKING | _PH_KWARG | _KWARG | _DISTINCT_ARG | _VALUE_ARG
_POSITIONAL_ARG = _UNPACKING | _PH_KWARG | _DISTINCT_ARG | _VALUE_ARG


def _arg_list(first: pp.ParserElement, rest: pp.ParserElement) -> pp.ParserElement:
    return pp.Group(pp.Optional(first + pp.ZeroOrMore(COMMA + rest) + pp.Optional(COMMA)))


def _callee_is(kind: str):
    def _check(toks: pp.ParseResults) -> bool:
        name = toks.as_list()[0][-1].name
        if kind == "scalar":
            return name.lower() in SCALAR_FUNCTIONS or name.lower() in DATE_PART_ALIASES
        return normalize_aggregation_name(name) in BUILTIN_AGGREGATIONS or name in ALL_TRANSFORMS
    return _check


_SCALAR_CALL = _PATH.copy().add_condition(_callee_is("scalar")) + LPAR + _arg_list(_POSITIONAL_ARG, _POSITIONAL_ARG) + RPAR
_VALUE_FIRST_CALL = _PATH.copy().add_condition(_callee_is("value")) + LPAR + _arg_list(_POSITIONAL_ARG, _ANY_ARG) + RPAR
_OTHER_CALL = _PATH + LPAR + _arg_list(_ANY_ARG, _ANY_ARG) + RPAR
_CALL = _SCALAR_CALL | _VALUE_FIRST_CALL | _OTHER_CALL


def _split_args(items: List[Any], *, call: str) -> Tuple[Tuple[Any, ...], Tuple[Tuple[str, Any], ...], bool]:
    positional: List[Any] = []
    kwargs: List[Tuple[str, Any]] = []
    distinct = False
    for i, item in enumerate(items):
        if isinstance(item, _Kw):
            if any(k == item.name for k, _ in kwargs):
                raise _Problem(f"{call}() got keyword argument {item.name!r} more than once.")
            kwargs.append((item.name, item.value))
            continue
        if kwargs:
            raise _Problem(f"{call}(): a positional argument follows a keyword argument.")
        if isinstance(item, _Distinct):
            if i != 0:
                raise _Problem(f"{_DISTINCT_ONLY}.")
            distinct = True
            item = item.value
        if isinstance(item, tuple):
            raise _Problem(f"{call}(): a list is only valid as a keyword-argument value.")
        positional.append(item)
    return tuple(positional), tuple(kwargs), distinct


class _PendingCall:
    """A parsed call, dispatched once its postfix is known (a trailing ``OVER (`` rejects first)."""

    __slots__ = ("words", "items")

    def __init__(self, words: List[_Word], items: List[Any]) -> None:
        self.words = words
        self.items = items


def _resolve(node: Any) -> Any:
    return _dispatch_pending(node) if isinstance(node, _PendingCall) else node


def _dispatch_pending(call: _PendingCall) -> Any:
    words = call.words
    name = words[-1].name
    args, kwargs, distinct = _split_args(call.items, call=name)
    if distinct:
        if name.lower() != "count" or len(words) > 1 or len(args) != 1 or isinstance(args[0], StarSource):
            raise _Problem(f"{_DISTINCT_ONLY}.")
        name = "count_distinct"
    if len(words) > 1:
        return _method_call(path=tuple(w.name for w in words[:-1]), name=name, args=args, kwargs=kwargs)
    return _dispatch_call(name, args=args, kwargs=kwargs, original=_STATE.text)


_on(_CALL, lambda toks: _PendingCall(words=list(toks[0]), items=list(toks[1])))

_COLON_SOURCE = (_on(pp.Suppress(STAR), lambda _toks: StarSource())
                 | _on(_PATH_STAR.copy(), lambda toks: DottedRef(parts=(*(w.name for w in toks[0][0]), "*"),
                                            quoted=any(w.quoted for w in toks[0][0])))
                 | _on(_PATH.copy(), lambda toks: _path_node(list(toks[0]))))
_COLON_AGG = _COLON_SOURCE + COLON + _BARE + pp.Optional(LPAR + _arg_list(_ANY_ARG, _ANY_ARG) + RPAR)


def _colon_node(toks: List[Any]) -> AggCall:
    source, agg = toks[0], toks[1].name
    args, kwargs, distinct = _split_args(list(toks[2]) if len(toks) > 2 else [], call=agg)
    if distinct:
        raise _Problem(f"{_DISTINCT_ONLY}.")
    _reject_bare_star_args(args, kwargs, func_name=agg, original=_STATE.text)
    return AggCall(source=source, agg=agg, args=args, kwargs=kwargs)


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
_TYPE_SPEC = (pp.Group(_TYPE_WORD + pp.ZeroOrMore(_kw(*_TYPE_EXTRA_WORDS)))
              + pp.Group(pp.Optional(LPAR + pp.DelimitedList(_sym(r"\d+", "type parameter")) + RPAR)))


def _type_text(toks: List[Any]) -> str:
    words, params = toks
    text = " ".join(w.upper() for w in words)
    return f"{text}({','.join(params)})" if len(params) else text


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
_ATOM = (_CASE | _CAST | _EXTRACT | _POSITION | _SUBSTRING | _TRIM_FORM | _TEMPORAL | _TIME_LITERAL
         | _INTERVAL | _CLOCK | _CONSTANT | _PLACEHOLDER_ATOM | _NUMBER | _STRING | _PAREN
         | _CALL | _COLON_AGG | _REF)

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
_CAST_LEVEL = _on((_PRIMARY + pp.ZeroOrMore(DCOLON + _need(_TYPE_SPEC, "'::' needs a type."))), lambda toks: _fold_casts(list(toks)))


def _fold_casts(items: List[Any]) -> Any:
    node = items[0]
    for type_text in items[1:]:
        node = Cast(source=node, type_text=type_text)
    return node


# -- operator ladder -----------------------------------------------------------

_MINUS, _PLUS = _sym("-", "'-'"), _sym(r"\+", "'+'")
_power = pp.Forward()
_power_rhs = pp.Forward()
_power_rhs <<= ((_MINUS | _PLUS) + _power_rhs) | _power
_power <<= _CAST_LEVEL + pp.Optional(pp.Suppress(_sym(r"\*\*", "'**'")) + _power_rhs)


def _signed(items: List[Any]) -> Any:
    node = items[-1]
    for sign in reversed(items[:-1]):
        if sign == "-":
            node = UnaryOp(op="-", operand=node)
    return node


_on(_power_rhs, lambda toks: _signed(list(toks)))
_on(_power, lambda toks: ScalarCall(name="pow", args=(toks[0], toks[1])) if len(toks) == 2 else toks[0])
_UNARY = _on((pp.ZeroOrMore(_MINUS | _PLUS) + _power), lambda toks: _signed(list(toks)))

_FLOORDIV = _sym("//", "'//'") + _fail("'//' is not an operator; use div(a, b) or floor(a / b).")
_MUL_LEVEL = _UNARY + pp.ZeroOrMore((_FLOORDIV | _sym(r"\*(?!\*)|/(?!/)|%", "'*', '/' or '%'")) + _UNARY)
_ADD_LEVEL = _MUL_LEVEL + pp.ZeroOrMore(_sym("[+-]", "'+' or '-'") + _MUL_LEVEL)


def _fold_arith(items: List[Any]) -> Any:
    node = items[0]
    for op, right in zip(items[1::2], items[2::2]):
        node = _interval_as_date_add(op=op, left=node, right=right) or Arith(op=op, left=node, right=right)
    return node


_on(_MUL_LEVEL, lambda toks: _fold_arith(list(toks)))
_on(_ADD_LEVEL, lambda toks: _fold_arith(list(toks)))
_PIPE = _sym(r"\|(?!\|)", "'|'") + _fail("'|' is not an operator; use || to concatenate strings.")
_concat_level <<= _ADD_LEVEL + pp.ZeroOrMore(pp.Suppress(_PIPE | _sym(r"\|\|", "'||'")) + _ADD_LEVEL)


def _fold_concat(items: List[Any]) -> Any:
    node = items[0]
    for right in items[1:]:
        node = ScalarCall(name="concat", args=(node, right))
    return node


_on(_concat_level, lambda toks: _fold_concat(list(toks)))

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


def _negated(node: Any, negate: bool) -> Any:
    return UnaryOp(op="not", operand=node) if negate else node


def _truth_test(left: Any, toks: List[Any]) -> Any:
    negate, word = toks[0], toks[1].lower()
    known = Cmp(op="is not", left=left, right=Literal(value=None))
    test = BoolOp(op="and", operands=(known, left if word == "true" else UnaryOp(op="not", operand=left)))
    return _negated(test, negate)


def _in_rhs_elements(items: List[Any]) -> TupleLit:
    if not items:
        raise _Problem(
            "an empty list is not allowed on the right-hand side of in / not in (dialect-dependent SQL); "
            "use a non-empty literal list."
        )
    elements: List[Literal] = []
    for item in items:
        if (isinstance(item, UnaryOp) and item.op == "-" and isinstance(item.operand, Literal)
                and isinstance(item.operand.value, Decimal)):
            item = Literal(value=-item.operand.value)
        if not isinstance(item, Literal):
            raise _Problem(
                "every element on the right-hand side of in / not in must be a literal (string, number, or "
                f"boolean); got {type(item).__name__}."
            )
        elements.append(item)
    return TupleLit(elements=tuple(elements))


_IN_LIST = ((LPAR + _PLACEHOLDER_ATOM + RPAR)
            | _PLACEHOLDER_ATOM
            | _on((LPAR + _SEQ_ITEMS + RPAR), lambda toks: _in_rhs_elements(list(toks[0])))
            | _on((LBRACK + _SEQ_ITEMS + RBRACK), lambda toks: _in_rhs_elements(list(toks[0])))
            | _STRING
            | _fail("the right-hand side of in / not in must be a literal list, e.g. status in ('a', 'b')."))

_CMP_SUFFIX = (
    _suffix(_sym(r"==|!=|<>|<=|>=|=|<|>", "comparison") + _concat_level,
            lambda left, t: Cmp(op=_CMP_OPS[t[0]], left=left, right=t[1]))
    | _suffix(_IS + _NOT_FLAG + pp.Suppress(_kw("distinct")) + pp.Suppress(_kw("from")) + _concat_level,
              lambda left, t: Cmp(op="is not distinct from" if t[0] else "is distinct from", left=left, right=t[1]))
    | _suffix(_IS + _NOT_FLAG + pp.Suppress(_kw("null") | pp.Regex(r"None\b")),
              lambda left, t: Cmp(op="is not" if t[0] else "is", left=left, right=Literal(value=None)))
    | _suffix(_IS + _NOT_FLAG + (_kw("true", "false")), _truth_test)
    | _suffix(_IS + _NOT_FLAG + _concat_level,
              lambda left, t: Cmp(op="is not" if t[0] else "is", left=left, right=t[1]))
    | _suffix(_NOT_FLAG + pp.Suppress(_kw("in")) + _IN_LIST,
              lambda left, t: Cmp(op="not in" if t[0] else "in", left=left, right=t[1]))
    | _suffix(_NOT_FLAG + pp.Suppress(_kw("between")) + _concat_level + pp.Suppress(_kw("and")) + _concat_level,
              lambda left, t: _negated(BoolOp(op="and", operands=(
                  Cmp(op=">=", left=left, right=t[1]), Cmp(op="<=", left=left, right=t[2]),
              )), t[0]))
    | _suffix(_NOT_FLAG + _kw("like", "ilike") + _concat_level,
              lambda left, t: _negated(ScalarCall(name=t[1].lower(), args=(left, t[2])), t[0]))
    | (_NOT_FLAG + _kw("similar") + _kw("to")) + _fail("SIMILAR TO is not supported; use LIKE or ILIKE.")
    | (_kw("regexp", "rlike") + _fail("the REGEXP / RLIKE operators are not supported; use LIKE or ILIKE."))
)


def _comparison_node(toks: List[Any]) -> Any:
    left, suffixes = toks[0], list(toks[1:])
    if len(suffixes) > 1:
        raise _Problem(
            "chained comparisons are not supported. Each comparison must be a single comparison; split "
            "(e.g.) `1 < x < 10` into `1 < x and x < 10`."
        )
    return suffixes[0].build(left) if suffixes else left


_COMPARISON = _on(_concat_level + pp.ZeroOrMore(_CMP_SUFFIX), _comparison_node)
_NOT_LEVEL = _on((pp.ZeroOrMore(_kw("not")) + _COMPARISON), lambda toks: _negate_n(len(toks) - 1, toks[-1]))


def _negate_n(count: int, node: Any) -> Any:
    for _ in range(count):
        node = UnaryOp(op="not", operand=node)
    return node


def _bool_level(operand: pp.ParserElement, word: str) -> pp.ParserElement:
    level = operand + pp.ZeroOrMore(pp.Suppress(_kw(word)) + operand)
    return _on(level, lambda toks: BoolOp(op=word, operands=tuple(toks)) if len(toks) > 1 else toks[0])


_TERNARY = _kw("if") + _fail(
    "the Python conditional 'x if cond else y' is not supported; use CASE WHEN cond THEN x ELSE y END or iif(cond, x, y).",
)
expr <<= _bool_level(_bool_level(_NOT_LEVEL, "and"), "or") + pp.Optional(_TERNARY)
_EXPRESSION = (expr + pp.StringEnd().set_fail_action(_record_failure)).parse_with_tabs()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


@lru_cache(maxsize=8192)
def parse_expr(text: str) -> ParsedExpr:
    """Parse a Mode-B expression string into a ``ParsedExpr``.

    Raises ``ValueError`` (empty / syntax / unsupported form / reserved prefix),
    ``UnknownFunctionError``, or ``IllegalWindowInFilterError`` (raw ``OVER(...)``).
    """
    if not text or not text.strip():
        raise ValueError("Empty Mode-B expression.")
    with _PARSE_LOCK:
        _STATE.text, _STATE.farthest = text, 0
        try:
            masked = _mask_placeholders(text)
            parsed = cast(ParsedExpr, _EXPRESSION.parse_string(masked, parse_all=True)[0])
        except _Problem as problem:
            raise ValueError(f"Invalid Mode-B expression {text!r}: {problem.detail}") from None
        except pp.ParseBaseException as e:
            raise ValueError(f"Invalid Mode-B expression {text!r}: {_syntax_message(text, e)}") from None
    if any(_is_interval(n) for n in _walk_parsed(parsed)):
        raise ValueError(
            f"Invalid Mode-B expression {text!r}: interval(n, unit) is only valid as "
            f"`x + interval(n, unit)`, `interval(n, unit) + x` or `x - interval(n, unit)`; "
            f"otherwise use date_add(x, n, unit)."
        )
    return parsed


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
    return [n.name for n in _walk_parsed(parsed) if isinstance(n, Placeholder)]


_ANY_BRACE_RE = re.compile(r"\{\{|\}\}|\{(" + _VAR_NAME + r")\}|\{([^}]*)\}")
_NUMBER_TEXT_RE = re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?")


def bind_variables(text: str, variables: Mapping[str, Any]) -> str:
    """``text`` with every ``{name}`` bound as a literal value: inside a string literal its text,
    in a bare literal slot a number or boolean, as a whole ``IN`` list a list. The template is
    parsed first, so no value can change the expression's structure."""
    if not _ANY_BRACE_RE.search(text):
        return text
    regions = [r for r in _regions(text) if r.kind != "ident"]
    for region in regions:
        chunk = text[region.start:region.end]
        if region.kind == "code" and ("{?" in chunk or "?}" in chunk):
            raise ValueError(f"{_OPTIONAL_BLOCKS} Got: {text!r}.")
        for m in _ANY_BRACE_RE.finditer(chunk):
            _check_variable(m, text=text, variables=variables)
    # Strings bound and ``{{`` / ``}}`` outside strings read as braces: the grammar judges the variable slots.
    parse_expr(_spliced(text, variables=variables, regions=regions, kinds=("string",), code_braces=True))
    return _spliced(text, variables=variables, regions=regions, kinds=("string", "code"), code_braces=False)


def _spliced(
    text: str, *, variables: Mapping[str, Any], regions: List[_Region], kinds: Tuple[str, ...], code_braces: bool,
) -> str:
    out: List[str] = []
    cursor = 0
    for region in regions:
        out.append(text[cursor:region.start])
        chunk = text[region.start:region.end]
        if region.kind in kinds:
            in_string = region.kind == "string"
            chunk = _ANY_BRACE_RE.sub(
                lambda m: _bound_text(m, text=text, region_start=region.start, in_string=in_string, variables=variables),
                chunk,
            )
        elif code_braces:
            chunk = chunk.replace("{{", "{").replace("}}", "}")
        out.append(chunk)
        cursor = region.end
    out.append(text[cursor:])
    return "".join(out)


def _check_variable(match: "re.Match[str]", *, text: str, variables: Mapping[str, Any]) -> None:
    if match.group(0) in ("{{", "}}"):
        return
    name = match.group(1)
    if name is None:
        raise ValueError(
            f"Invalid variable name '{match.group(2)}' in filter: {text!r}. "
            f"Variable names must contain only letters, digits, and underscores."
        )
    if name not in variables:
        raise ValueError(
            f"Undefined variable '{name}' in filter: {text!r}. Available variables: {sorted(variables)}"
        )


def _bound_text(
    match: "re.Match[str]", *, text: str, region_start: int, in_string: bool, variables: Mapping[str, Any],
) -> str:
    whole = match.group(0)
    if whole in ("{{", "}}"):
        return whole[0]
    name = match.group(1)
    value = variables[name]
    if in_string:
        return _value_text(name, value).replace("'", "''")
    start = region_start + match.start()
    if _opens_in_list(text, start=start, end=region_start + match.end()):
        values = value if isinstance(value, (list, tuple)) else [value]
        if not values:
            raise ValueError(
                f"Variable '{name}' cannot be an empty list; 'IN ()' is invalid SQL. "
                f"For 'no filter' semantics, use a sentinel default."
            )
        return ", ".join(_literal_text(name, v) for v in values)
    return _bare_literal_text(name, value)


def _opens_in_list(text: str, *, start: int, end: int) -> bool:
    """Whether the placeholder at ``[start, end)`` is the whole list of an ``IN``."""
    before = text[:start].rstrip()
    if before.endswith("(") and text[end:].lstrip().startswith(")"):
        before = before[:-1].rstrip()
    word = re.search(r"([A-Za-z]+)$", before)
    return word is not None and word.group(1).lower() == "in"


def _value_text(name: str, value: Any) -> str:
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)):
        return _number_text(name, value)
    raise ValueError(f"Variable '{name}' must be a string, number, or list/tuple, got {type(value).__name__}")


def _number_text(name: str, value: int | float) -> str:
    if isinstance(value, float) and value != value or value in (float("inf"), float("-inf")):
        raise ValueError(f"Variable '{name}' must be finite, got {value!r}")
    return str(value)


def _literal_text(name: str, value: Any) -> str:
    """One value as a literal: a string quoted, a number or boolean as itself."""
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, (list, tuple)):
        raise ValueError(f"{_LITERAL_VALUES}: variable '{name}' holds a list, which fills only a whole IN list.")
    return _value_text(name, value)


def _bare_literal_text(name: str, value: Any) -> str:
    if isinstance(value, (list, tuple)):
        raise ValueError(
            f"{_LITERAL_VALUES}: variable '{name}' holds a list, which fills only a whole IN list, e.g. x in ({{{name}}})."
        )
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


def measure_dependency_problems(
    formulas: List[Tuple[Optional[str], str]], *, aggregations: frozenset[str] = frozenset(),
) -> List[Optional[str]]:
    """Per ``(name, formula)``: why the saved measure cannot resolve among its peers, else ``None``.

    Outside an aggregation a bare name must be a peer measure; an aggregation must be built-in
    or one of ``aggregations``; a measure on a cycle, or reading one that cannot resolve, cannot
    resolve either."""
    names = {name for name, _ in formulas if name}
    own: Dict[int, Optional[str]] = {}
    deps: Dict[int, set] = {}
    for i, (_name, formula) in enumerate(formulas):
        try:
            refs, problem = _measure_refs(parse_expr(formula), names=names, aggregations=aggregations)
        except (ValueError, UnknownFunctionError, IllegalWindowInFilterError, GranularityCallError) as e:
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


def _measure_refs(node: Any, *, names: set, aggregations: frozenset[str]) -> Tuple[set, Optional[str]]:
    """The peer measures ``node`` reads outside aggregations, and the first problem found."""
    if isinstance(node, Ref):
        if node.name in names:
            return {node.name}, None
        match = difflib.get_close_matches(word=node.name, possibilities=sorted(names), n=1)
        hint = f" Did you mean '{match[0]}'?" if match else ""
        return set(), (
            f"'{node.name}' is not a saved measure.{hint} Reference a saved measure by its bare name, or "
            f"aggregate a column (e.g., 'sum({node.name})'). For COUNT(*), use 'count(*)'."
        )
    if isinstance(node, DottedRef):
        dotted = ".".join(node.parts)
        return set(), f"Cross-model measure '{dotted}' must include an aggregation (e.g., 'sum({dotted})')."
    if isinstance(node, AggCall):
        known = normalize_aggregation_name(node.agg) in BUILTIN_AGGREGATIONS or node.agg in aggregations
        return set(), None if known else f"unknown aggregation {node.agg!r}"
    children = (node.input,) if isinstance(node, TransformCall) else _operands(node)
    refs: set = set()
    for child in children:
        found, problem = _measure_refs(child, names=names, aggregations=aggregations)
        if problem is not None:
            return refs, problem
        refs |= found
    return refs, None


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
    Ref, DottedRef, StarSource, Literal, TemporalLiteral, Placeholder, ScalarCall, Cast, Arith, UnaryOp, Cmp, BoolOp,
)


def _contains_agg_or_transform(node: Any) -> bool:
    """Whether a parsed subtree contains any AggCall / TransformCall."""
    if isinstance(node, (AggCall, TransformCall)):
        return True
    return any(_contains_agg_or_transform(c) for c in _operands(node))


def _operands(node: Any) -> Tuple[Any, ...]:
    """The value operands of a row-level composite (empty for leaves and calls with their own scope)."""
    if isinstance(node, ScalarCall):
        return node.args
    if isinstance(node, Cast):
        return (node.source,)
    if isinstance(node, (Arith, Cmp)):
        return (node.left, node.right)
    if isinstance(node, UnaryOp):
        return (node.operand,)
    if isinstance(node, BoolOp):
        return node.operands
    return ()


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


def _onto_path(node: Any, *, path: Tuple[str, ...]) -> Any:
    """``node`` with every column leaf (and ``*``) read under the join ``path``."""
    if isinstance(node, Ref):
        return DottedRef(parts=(*path, node.name), quoted=node.quoted)
    if isinstance(node, DottedRef):
        return DottedRef(parts=(*path, *node.parts), quoted=node.quoted)
    if isinstance(node, StarSource):
        return DottedRef(parts=(*path, "*"))
    if isinstance(node, ScalarCall):
        return node.model_copy(update={"args": tuple(_onto_path(a, path=path) for a in node.args)})
    if isinstance(node, Cast):
        return node.model_copy(update={"source": _onto_path(node.source, path=path)})
    if isinstance(node, (Arith, Cmp)):
        return node.model_copy(update={"left": _onto_path(node.left, path=path), "right": _onto_path(node.right, path=path)})
    if isinstance(node, UnaryOp):
        return node.model_copy(update={"operand": _onto_path(node.operand, path=path)})
    if isinstance(node, BoolOp):
        return node.model_copy(update={"operands": tuple(_onto_path(o, path=path) for o in node.operands)})
    return node


def _is_literal_arg(node: Any) -> bool:
    if isinstance(node, UnaryOp) and node.op == "-":
        return isinstance(node.operand, Literal)
    return isinstance(node, (Literal, Placeholder))


def _method_call_refusal(name: str, *, args: Tuple[Any, ...], kwargs: Tuple[Tuple[str, Any], ...]) -> Optional[str]:
    """Why ``<path>.<name>(args)`` has no single functional reading, else ``None``."""
    lowered = name.lower()
    transform_only = name in ALL_TRANSFORMS and normalize_aggregation_name(name) not in BUILTIN_AGGREGATIONS
    if lowered in SCALAR_FUNCTIONS or lowered in DATE_PART_ALIASES or transform_only:
        return f"calls {name!r}, which is not an aggregation"
    if not args:
        return "needs the aggregated value as its first argument"
    if any(isinstance(leaf, (DottedRef, AggCall, TransformCall)) for leaf in _source_leaves(args[0])):
        return "may read only bare column names of that path"
    if any(k == "partition_by" for k, _ in kwargs):
        return "cannot take partition_by="
    if not all(_is_literal_arg(v) for v in (*args[1:], *(v for _, v in kwargs))):
        return "takes only literal further arguments"
    return None


def _method_call(*, path: Tuple[str, ...], name: str, args: Tuple[Any, ...], kwargs: Tuple[Tuple[str, Any], ...]) -> ParsedExpr:
    """``<path>.<agg>(<source>, <literals>)`` as its functional twin ``<agg>(<path>.<source>, …)``."""
    original = _STATE.text
    twin_args = (_onto_path(args[0], path=path), *args[1:]) if args else ()
    reason = _method_call_refusal(name, args=args, kwargs=kwargs)
    if reason is not None:
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: the method call {'.'.join(path)}.{name}(...) "
            f"{reason}; write the functional form, e.g. {name}{_canonical_call_params(twin_args, kwargs)}."
        )
    return _dispatch_call(name, args=twin_args, kwargs=kwargs, original=original)


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
        if canonical_agg in _SCALAR_MIN_MAX and len(args) == 2 and not kwargs:
            scalar = _SCALAR_MIN_MAX[canonical_agg]
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: {func_name}() with two values is the "
                f"aggregation, not a scalar; use {scalar}{_canonical_call_params(args, ())}."
            )
        source = _validated_agg_source(
            args[0], func_name=func_name, original=original,
        )
        return AggCall(source=source, agg=func_name, args=args[1:], kwargs=kwargs)

    if func_name in ALL_TRANSFORMS:
        if not args:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: transform "
                f"{func_name!r} requires at least one positional argument "
                f"(the value to transform)."
            )
        _reject_bare_star_args(args, kwargs, func_name=func_name, original=original)
        return TransformCall(op=func_name, input=args[0], args=args[1:], kwargs=kwargs)

    # Normalised to lower case so ``COALESCE`` / ``coalesce`` intern to one key.
    if func_name.lower() in SCALAR_FUNCTIONS:
        if kwargs:
            raise ValueError(
                f"Invalid Mode-B expression {original!r}: scalar function "
                f"{func_name!r} does not accept keyword arguments. Pass "
                f"values positionally."
            )
        _reject_bare_star_args(args, kwargs, func_name=func_name, original=original)
        name = func_name.lower()
        return ScalarCall(name=name, args=_date_args(name=name, args=args, original=original))

    part = DATE_PART_ALIASES.get(func_name.lower())
    if part is not None:
        if kwargs or len(args) != 1:
            raise ValueError(
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

    if func_name.lower() in GRANULARITY_NAMES:
        raise GranularityCallError.wrong_shape(original)
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
        raise ValueError(
            f"Invalid Mode-B expression {original!r}: the {what} of {name}() must be a "
            f"string literal, one of: {accepted}."
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


def _walk_parsed(node: Any) -> Iterator[Any]:
    """Every node of a parsed tree, including call args, kwargs, agg sources and filters."""
    if isinstance(node, tuple):
        for item in node:
            yield from _walk_parsed(item)
        return
    if not isinstance(node, _BaseNode):
        return
    yield node
    for field in type(node).model_fields:
        yield from _walk_parsed(getattr(node, field))


# ---------------------------------------------------------------------------
# Canonical text rendering + entity-ref splitting
# ---------------------------------------------------------------------------


def _canonical_call_params(
    args: Tuple[Any, ...], kwargs: Tuple[Tuple[str, Any], ...], *, bare_direction: bool = False,
) -> str:
    parts = [canonical_measure_text(a) for a in args]
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


def canonical_measure_text(parsed: Any) -> str:  # NOSONAR(S3776) — flat per-node-kind rendering table; each branch is one spelling rule.
    """Render a ``ParsedExpr`` back to canonical legacy colon-spelling text.

    Used for alias derivation so the functional and legacy colon spellings of one
    formula sanitize to the SAME public name. Deterministic, not a
    verbatim round-trip: grouping parens are dropped and spacing normalised.
    """
    if isinstance(parsed, Ref):
        return _identifier_text(parsed.name)
    if isinstance(parsed, DottedRef):
        return ".".join(p if p == "*" else _identifier_text(p) for p in parsed.parts)
    if isinstance(parsed, StarSource):
        return "*"
    if isinstance(parsed, Literal):
        if isinstance(parsed.value, str):
            return "'" + parsed.value.replace("'", "''") + "'"
        return str(parsed.value)
    if isinstance(parsed, TemporalLiteral):
        return f"{parsed.kind.upper()} '{parsed.text}'"
    if isinstance(parsed, Placeholder):
        return f"{{{parsed.name}}}"
    if isinstance(parsed, TupleLit):
        return f"({', '.join(canonical_measure_text(e) for e in parsed.elements)})"
    if isinstance(parsed, AggCall):
        return _canonical_agg_text(parsed)
    if isinstance(parsed, TransformCall):
        inner = canonical_measure_text(parsed.input)
        params = _canonical_call_params(parsed.args, parsed.kwargs, bare_direction=True)
        return f"{parsed.op}({inner}{', ' + params[1:-1] if params else ''})"
    if isinstance(parsed, ScalarCall):
        return f"{parsed.name}({', '.join(canonical_measure_text(a) for a in parsed.args)})"
    if isinstance(parsed, Cast):
        fn = "TRY_CAST" if parsed.try_ else "CAST"
        return f"{fn}({canonical_measure_text(parsed.source)} AS {parsed.type_text})"
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


def _canonical_agg_text(parsed: AggCall) -> str:
    source = canonical_measure_text(parsed.source)
    if isinstance(parsed.source, (Ref, DottedRef, StarSource)):
        text = f"{source}:{parsed.agg}{_canonical_call_params(parsed.args, parsed.kwargs)}"
    else:
        # Expression source (``sum(amount - cost)``): render functionally so the
        # text can't collide with a distinct parse tree like ``amount - cost:sum``.
        parts = [source]
        parts += [canonical_measure_text(a) for a in parsed.args]
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
    "AggCall", "Arith", "BoolOp", "Cast", "Cmp", "DottedRef", "Literal", "ParsedExpr", "Placeholder", "Ref",
    "ScalarCall", "StarSource", "TemporalLiteral", "TransformCall", "TupleLit", "UnaryOp",
    "bind_variables", "canonical_measure_text", "measure_dependency_problems", "parse_expr", "placeholder_names", "split_entity_agg_ref", "walk_parsed_refs",
]
