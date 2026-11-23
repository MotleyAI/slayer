"""Frozen copy of the 1.9.1 Mode-B reader's pre-parse chain: whether 1.9.1 could have held a stored text.

Syntax only — 1.9.1's semantic checks (unknown functions, chained comparisons, reserved names) are not replayed.
Removed together with the stored-upgrade chain; never extended to the current grammar.
"""

import ast
import re
from collections.abc import Callable, Iterator

# The 1.9.1 names its ``=``-vs-keyword-argument scanner read.
_SCALAR_FUNCTIONS = frozenset({
    "abs", "ceil", "ceiling", "coalesce", "concat", "contains", "contains_all", "contains_any", "current_date",
    "date_add", "date_diff", "date_part", "exp", "floor", "greatest", "ifnull", "iif", "instr", "interval", "least",
    "length", "like", "ln", "log", "log10", "log2", "lower", "ltrim", "mod", "now", "nullif", "pow", "power",
    "replace", "round", "rtrim", "sign", "sqrt", "substr", "substring", "trim", "trunc", "upper",
})
_ALL_TRANSFORMS = frozenset({
    "change", "change_pct", "consecutive_periods", "cumsum", "dense_rank", "first", "lag", "last", "lead", "ntile",
    "percent_rank", "rank", "time_shift",
})

_VAR_PATTERN = re.compile(r"\{\{|\}\}|\{([a-zA-Z_]\w*)\}|\{([^}]*)\}", re.ASCII)
_PY_STRING_LITERAL_RE = re.compile(r"'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\"")
_SQL_LIKE_RE = re.compile(
    r"\b(\w+\([^()]*\)|(?:\w+\.)*\w+)\s+"
    r"([nN][oO][tT]\s+)?[lL][iI][kK][eE]\s+"
    r"('(?:\\.|[^'\\])*')"
)
_SQL_NULL_RE = re.compile(r"\b[nN][uU][lL][lL]\b")
_SQL_KEYWORD_RES = tuple(
    (re.compile(r"\b" + "".join(f"[{c}{c.upper()}]" for c in kw) + r"\b"), kw)
    for kw in ("is", "not", "and", "or", "in")
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
_CASE_TOKEN_RE = re.compile(
    r"(?P<str>'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\")"
    r"|(?P<id>[^\W\d]\w*(?:\s*\.\s*[^\W\d]\w*)*)"
    r"|(?P<lp>[(\[])"
    r"|(?P<rp>[)\]])",
    re.DOTALL,
)
_CASE_STOPS_OPERAND = frozenset({"WHEN", "THEN", "ELSE", "END"})
_CASE_KEYWORDS = _CASE_STOPS_OPERAND | {"CASE"}
_CALL_TOKEN_RE = re.compile(
    r"(?P<str>'(?:\\.|[^'\\])*'|\"(?:\\.|[^\"\\])*\")"
    r"|(?P<id>[^\W\d]\w*)"
    r"|(?P<lp>\()|(?P<rp>\))|(?P<comma>,)"
    r"|(?P<op>==|!=|<=|>=|=)"
    r"|(?P<other>\S)",
    re.DOTALL,
)
_COLON_AGG_RE = re.compile(r"(\*|[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*(?:\.\*)?):([a-zA-Z_]\w*)")
_STAR_ARG_TOKEN = "__slayer_star__"

_Token = tuple[str, str, int, int]
_Hist = list[tuple[str, str]]
_Frame = tuple[bool, str | None]


def is_old_grammar(text: str, *, filter: bool) -> bool:
    """Whether 1.9.1's reader parsed ``text`` on a query filter (``filter``) or another Mode-B surface."""
    try:
        if "{?" in text or "?}" in text:
            return False
        probed = _VAR_PATTERN.sub(_probe_placeholder, text)
        if filter:
            probed = _normalize_sql_filter_operators(probed)
        probed = _rewrite_count_distinct(_rewrite_extract(probed))
        probed = _preprocess_colons(_rewrite_case_when(probed))
        ast.parse(_preprocess_star_args(probed), mode="eval")
    except (ValueError, SyntaxError):
        return False
    return True


def _probe_placeholder(match: re.Match[str]) -> str:
    full = match.group(0)
    if full in ("{{", "}}"):
        return full[0]
    if match.group(1) is None:
        raise ValueError(full)
    return "0"


def _preprocess_colons(text: str) -> str:
    spans = [(m.start(), m.end()) for m in _PY_STRING_LITERAL_RE.finditer(text)]

    def replace(m: re.Match[str]) -> str:
        return m.group(0) if any(s <= m.start() < e for s, e in spans) else "__slayer_agg__"

    return _COLON_AGG_RE.sub(replace, text)


# --------------------------------------------------------------------------- #
# Identifier adjacency
# --------------------------------------------------------------------------- #
def _is_ident_adjacent(text: str, pos: int) -> bool:
    return 0 <= pos < len(text) and ("a" + text[pos]).isidentifier()


def _continues_ref(text: str, pos: int) -> bool:
    return _is_ident_adjacent(text, pos) or (0 <= pos < len(text) and text[pos] == ".")


def _sub_keyword_isolated(pattern: re.Pattern[str], repl: str, text: str) -> str:
    def guard(m: re.Match[str]) -> str:
        if _continues_ref(text, m.start() - 1) or _continues_ref(text, m.end()):
            return m.group(0)
        return repl
    return pattern.sub(guard, text)


# --------------------------------------------------------------------------- #
# Filter operator spellings
# --------------------------------------------------------------------------- #
def _rewrite_sql_like(text: str) -> str:
    spans = [(m.start(), m.end()) for m in _PY_STRING_LITERAL_RE.finditer(text)]

    def sub(m: re.Match[str]) -> str:
        if any(s <= m.start() < e for s, e in spans) or _continues_ref(text, m.start() - 1):
            return m.group(0)
        call = f"like({m.group(1)}, {m.group(3)})"
        return f"not {call}" if m.group(2) else call

    return _SQL_LIKE_RE.sub(sub, text)


def _normalize_sql_filter_operators(text: str) -> str:
    text = _rewrite_sql_like(text)
    parts = _PY_STRING_LITERAL_RE.split(text)
    literals = _PY_STRING_LITERAL_RE.findall(text)
    result: list[str] = []
    for i, part in enumerate(parts):
        part = _sub_keyword_isolated(_SQL_NULL_RE, "None", part)
        for kw_re, kw in _SQL_KEYWORD_RES:
            part = _sub_keyword_isolated(kw_re, kw, part)
        result.append(part.replace("<>", "!=").replace("||", "|"))
        if i < len(literals):
            result.append(literals[i])
    return _rewrite_comparison_equals("".join(result))


def _classify_paren(hist: _Hist) -> _Frame:
    prev_kind = hist[-1][0] if hist else None
    prev_text = hist[-1][1] if hist else ""
    is_call = prev_kind == "NAME" or prev_text in (")", "]")
    callee = hist[-1][1] if (is_call and prev_kind == "NAME") else None
    if callee is not None and len(hist) >= 2 and hist[-2] == ("OTHER", ":"):
        callee = None
    return is_call, callee


def _is_kwarg_equals(stack: list[_Frame], hist: _Hist) -> bool:
    top = stack[-1] if stack else None
    if top is None or not top[0]:
        return False
    callee = top[1]
    if callee is not None and callee.lower() in _SCALAR_FUNCTIONS:
        return False
    if (hist[-1][0] if hist else None) != "NAME":
        return False
    prev_prev_kind = hist[-2][0] if len(hist) >= 2 else None
    if callee is not None and callee in _ALL_TRANSFORMS:
        return prev_prev_kind == "COMMA"
    return prev_prev_kind in ("LPAREN", "COMMA")


def _push_hist(hist: _Hist, kind: str, text: str) -> None:
    hist.append((kind, text))
    if len(hist) > 2:
        del hist[0]


def _handle_pass_through(tok: str, out: list[str], stack: list[_Frame], hist: _Hist) -> None:
    out.append(tok)


def _handle_ident(tok: str, out: list[str], stack: list[_Frame], hist: _Hist) -> None:
    out.append(tok)
    _push_hist(hist, "KW" if tok in _FILTER_KEYWORDS else "NAME", tok)


def _handle_other(tok: str, out: list[str], stack: list[_Frame], hist: _Hist) -> None:
    out.append(tok)
    _push_hist(hist, "OTHER", tok)


def _handle_comma(tok: str, out: list[str], stack: list[_Frame], hist: _Hist) -> None:
    out.append(tok)
    _push_hist(hist, "COMMA", tok)


def _handle_lparen(tok: str, out: list[str], stack: list[_Frame], hist: _Hist) -> None:
    stack.append(_classify_paren(hist))
    out.append(tok)
    _push_hist(hist, "LPAREN", tok)


def _handle_rparen(tok: str, out: list[str], stack: list[_Frame], hist: _Hist) -> None:
    if stack:
        stack.pop()
    out.append(tok)
    _push_hist(hist, "OTHER", tok)


def _handle_op_eq(tok: str, out: list[str], stack: list[_Frame], hist: _Hist) -> None:
    out.append("=" if _is_kwarg_equals(stack, hist) else "==")
    _push_hist(hist, "OTHER", tok)


_HANDLERS: dict[str, Callable[[str, list[str], list[_Frame], _Hist], None]] = {
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
    out: list[str] = []
    stack: list[_Frame] = []
    hist: _Hist = []
    for m in _SCAN_TOKEN_RE.finditer(text):
        _HANDLERS[m.lastgroup or "other"](m.group(0), out, stack, hist)
    return "".join(out)


# --------------------------------------------------------------------------- #
# CASE … END
# --------------------------------------------------------------------------- #
def _case_keyword(text: str, tok: _Token) -> str | None:
    kind, val, start, end = tok
    if kind != "id" or not val.isascii():
        return None
    up = val.upper()
    if up not in _CASE_KEYWORDS or _is_ident_adjacent(text, start - 1) or _is_ident_adjacent(text, end):
        return None
    return up


def _case_has_when(text: str, toks: list[_Token], i: int) -> bool:
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
    toks = [(m.lastgroup or "", m.group(), m.start(), m.end()) for m in _CASE_TOKEN_RE.finditer(text)]
    if not any(_case_keyword(text, t) == "CASE" for t in toks):
        return text
    return _rw_value(text, toks, 0, frozenset(), 0)[0]


def _rw_value(  # NOSONAR(S3776) — frozen 1.9.1 port, suppressed there too; never refactored
    text: str, toks: list[_Token], i: int, stop_kws: frozenset[str], start_char: int,
) -> tuple[str, int]:
    parts: list[str] = []
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
    parts.append(text[last:toks[i][2] if i < len(toks) else len(text)])
    return "".join(parts).strip(), i


def _rw_case(text: str, toks: list[_Token], i: int) -> tuple[str, int]:
    i += 1
    operand, i = _rw_value(text, toks, i, _CASE_STOPS_OPERAND, toks[i - 1][3])
    branches: list[tuple[str, str]] = []
    while i < len(toks) and _case_keyword(text, toks[i]) == "WHEN":
        i += 1
        cond, i = _rw_value(text, toks, i, frozenset({"THEN"}), toks[i - 1][3])
        if not (i < len(toks) and _case_keyword(text, toks[i]) == "THEN"):
            raise ValueError("CASE: WHEN without THEN")
        i += 1
        then_val, i = _rw_value(text, toks, i, frozenset({"WHEN", "ELSE", "END"}), toks[i - 1][3])
        branches.append((cond, then_val))
    else_val = "None"
    if i < len(toks) and _case_keyword(text, toks[i]) == "ELSE":
        i += 1
        else_val, i = _rw_value(text, toks, i, frozenset({"END"}), toks[i - 1][3])
        else_val = else_val or "None"
    if not (i < len(toks) and _case_keyword(text, toks[i]) == "END"):
        raise ValueError("CASE: missing END")
    i += 1
    if not branches:
        raise ValueError("CASE: no WHEN branch")
    result = else_val
    for cond, then_val in reversed(branches):
        cond_expr = f"{operand} == {cond}" if operand else _normalize_sql_filter_operators(cond)
        result = f"iif({cond_expr}, {then_val}, {result})"
    return result, i


# --------------------------------------------------------------------------- #
# extract(<part> from x), count(distinct x)
# --------------------------------------------------------------------------- #
def _call_tokens(text: str) -> list[_Token]:
    return [(m.lastgroup or "", m.group(), m.start(), m.end()) for m in _CALL_TOKEN_RE.finditer(text)]


def _is_word(text: str, toks: list[_Token], i: int, word: str) -> bool:
    kind, val, start, end = toks[i]
    return (
        kind == "id" and val.isascii() and val.lower() == word
        and not _is_ident_adjacent(text, start - 1) and not _continues_ref(text, end)
        and not (i > 0 and toks[i - 1][1] == ".")
    )


def _matching_rp(toks: list[_Token], lp: int) -> int | None:
    depth = 0
    for j in range(lp, len(toks)):
        if toks[j][0] == "lp":
            depth += 1
        elif toks[j][0] == "rp":
            depth -= 1
            if depth == 0:
                return j
    return None


def _top_level(toks: list[_Token], lp: int, rp: int) -> Iterator[int]:
    depth = 0
    for j in range(lp + 1, rp):
        kind = toks[j][0]
        if kind == "lp":
            depth += 1
        elif kind == "rp":
            depth -= 1
        elif depth == 0:
            yield j


def _extract_call(text: str, toks: list[_Token], i: int) -> tuple[int, int] | None:
    if not (_is_word(text, toks, i, "extract") and i + 1 < len(toks) and toks[i + 1][0] == "lp"):
        return None
    rp = _matching_rp(toks, i + 1)
    if rp is None:
        return None
    frm = next((j for j in _top_level(toks, i + 1, rp) if _is_word(text, toks, j, "from")), None)
    return None if frm is None else (rp, frm)


def _rewrite_extract(text: str) -> str:
    if "extract" not in text.lower():
        return text
    toks = _call_tokens(text)
    out: list[str] = []
    last = i = 0
    while i < len(toks):
        call = _extract_call(text, toks, i)
        if call is None:
            i += 1
            continue
        rp, frm = call
        part = toks[i + 2:frm]
        operand = text[toks[frm][3]:toks[rp][2]].strip()
        if len(part) != 1 or part[0][0] not in ("id", "str") or not operand:
            raise ValueError("extract: malformed")
        word = part[0][1] if part[0][0] == "str" else f"'{part[0][1]}'"
        out += [text[last:toks[i][2]], f"date_part({word}, {_rewrite_extract(operand)})"]
        last, i = toks[rp][3], rp + 1
    out.append(text[last:])
    return "".join(out)


def _single_distinct_operand(toks: list[_Token], i: int, rp: int) -> bool:
    commas = [j for j in _top_level(toks, i - 1, rp) if toks[j][0] == "comma"]
    operand = toks[i + 1:commas[0] if commas else rp]
    if not operand or [t[1] for t in operand] == ["*"]:
        return False
    return all(toks[c + 1][0] == "id" and toks[c + 2][1] == "=" for c in commas)


def _in_call_top_level(toks: list[_Token], i: int) -> bool:
    depth = 0
    for j in range(i - 1, -1, -1):
        kind = toks[j][0]
        if kind == "rp":
            depth += 1
        elif kind == "lp":
            if depth == 0:
                return j > 0 and toks[j - 1][0] == "id"
            depth -= 1
    return False


def _is_later_distinct_arg(text: str, toks: list[_Token], i: int) -> bool:
    opens_arg = 0 < i < len(toks) - 1 and (toks[i - 1][0] == "comma" or toks[i - 1][1] == "=")
    if not (opens_arg and _is_word(text, toks, i, "distinct")):
        return False
    kind, val = toks[i + 1][:2]
    return (kind in ("id", "str", "lp") or val.isdigit() or val == "*") and _in_call_top_level(toks, i)


def _rewrite_count_distinct(text: str) -> str:
    if "distinct" not in text.lower():
        return text
    toks = _call_tokens(text)
    edits: list[tuple[int, int, str]] = []
    for i in range(2, len(toks)):
        if _is_later_distinct_arg(text, toks, i):
            raise ValueError("distinct: not count's only operand")
        if not (_is_word(text, toks, i, "distinct") and toks[i - 1][0] == "lp" and toks[i - 2][0] == "id"):
            continue
        rp = _matching_rp(toks, i - 1)
        if rp is None:
            continue
        if not (_is_word(text, toks, i - 2, "count") and _single_distinct_operand(toks, i, rp)):
            raise ValueError("distinct: not count's only operand")
        edits += [(toks[i - 2][2], toks[i - 2][3], "count_distinct"), (toks[i][2], toks[i][3], "")]
    for start, end, repl in reversed(edits):
        text = text[:start] + repl + text[end:]
    return text


# --------------------------------------------------------------------------- #
# * arguments
# --------------------------------------------------------------------------- #
def _preprocess_star_args(text: str) -> str:  # NOSONAR(S3776) — frozen 1.9.1 port, suppressed there too; never refactored
    if "*" not in text:
        return text
    toks = [(m.lastgroup or "", m.group(0)) for m in _SCAN_TOKEN_RE.finditer(text)]
    n = len(toks)

    def prev(i: int) -> int:
        j = i - 1
        while j >= 0 and toks[j][0] == "ws":
            j -= 1
        return j

    def nxt(i: int) -> int:
        j = i + 1
        while j < n and toks[j][0] == "ws":
            j += 1
        return j

    def is_call_lparen(i: int) -> bool:
        p = prev(i)
        return p >= 0 and toks[p][0] == "ident"

    def dotted_star_opens_call(dot_idx: int) -> bool:
        j = dot_idx
        while True:
            q = prev(j)
            if q < 0 or toks[q][0] != "ident":
                return False
            r = prev(q)
            if r >= 0 and toks[r] == ("other", "."):
                j = r
                continue
            return r >= 0 and toks[r][0] == "lparen" and is_call_lparen(r)

    out: list[str] = []
    for i, (kind, val) in enumerate(toks):
        if kind == "other" and val == "*":
            nx, p = nxt(i), prev(i)
            if nx < n and toks[nx][1] in (",", ")") and p >= 0 and (
                (toks[p][0] == "lparen" and is_call_lparen(p))
                or (toks[p] == ("other", ".") and dotted_star_opens_call(p))
            ):
                out.append(_STAR_ARG_TOKEN)
                continue
        out.append(val)
    return "".join(out)
