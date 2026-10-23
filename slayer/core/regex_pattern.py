"""The portable regex subset: pattern and replacement syntax, parsed once and re-emitted per dialect."""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import BaseModel, ConfigDict

#: The largest repetition count every dialect accepts (Postgres's ``RE_DUP_MAX``).
MAX_REPEAT = 255
#: Characters a backslash makes literal.
_PUNCTUATION = frozenset("\\.[](){}*+?^$|/-!\"#%&',:;<=>@_`~")
_CONTROL_ESCAPES = {"n": "\n", "t": "\t", "r": "\r"}
_CLASS_ESCAPES = frozenset("dws")
_LETTER_ESCAPES = {
    "b": "the word boundary '\\b'", "B": "the non-word-boundary '\\B'",
    "p": "the Unicode property '\\p'", "P": "the Unicode property '\\P'",
    "A": "the anchor '\\A'", "z": "the anchor '\\z'", "Z": "the anchor '\\Z'", "G": "the anchor '\\G'",
    "x": "the hex escape '\\x'", "u": "the Unicode escape '\\u'", "U": "the Unicode escape '\\U'",
    "c": "the control escape '\\c'", "e": "the escape '\\e'", "f": "the escape '\\f'", "v": "the escape '\\v'",
    "a": "the escape '\\a'", "k": "the named backreference '\\k'", "Q": "the quoting escape '\\Q'",
    "E": "the quoting escape '\\E'", "R": "the escape '\\R'", "X": "the escape '\\X'", "h": "the escape '\\h'",
    "H": "the escape '\\H'", "N": "the escape '\\N'", "K": "the escape '\\K'", "m": "the escape '\\m'",
    "M": "the escape '\\M'", "y": "the escape '\\y'", "Y": "the escape '\\Y'",
}
_GROUP_FORMS = (
    ("(?<=", "the lookbehind '(?<='"), ("(?<!", "the negative lookbehind '(?<!'"),
    ("(?=", "the lookahead '(?='"), ("(?!", "the negative lookahead '(?!'"),
)


class RegexPatternError(ValueError):
    """A pattern or replacement outside the portable subset; ``construct`` names the offending part."""

    def __init__(self, construct: str) -> None:
        self.construct = construct
        super().__init__(f"{construct} is not in the portable regex subset")


class _Node(BaseModel):
    model_config = ConfigDict(frozen=True)


class RegexChar(_Node):
    """One literal character."""

    char: str


class RegexAny(_Node):
    """``.`` — any character, newline included."""


class RegexAnchor(_Node):
    """``^`` / ``$`` — the start / end of the whole string."""

    at: Literal["start", "end"]


class RegexClass(_Node):
    """``\\d`` / ``\\w`` / ``\\s`` (ASCII), or a negation."""

    kind: Literal["d", "w", "s"]
    negated: bool = False


class RegexRange(_Node):
    low: str
    high: str


BracketItem = RegexChar | RegexRange | RegexClass


class RegexBracket(_Node):
    """``[…]`` / ``[^…]``."""

    negated: bool
    items: tuple[BracketItem, ...]


class RegexGroup(_Node):
    """A capturing group ``( … )``; ``index`` counts opening parentheses from 1."""

    index: int
    body: "RegexAlt"


class RegexRepeat(_Node):
    node: "RegexAtom"
    low: int
    high: int | None = None


RegexAtom = RegexChar | RegexAny | RegexAnchor | RegexClass | RegexBracket | RegexGroup
RegexItem = RegexAtom | RegexRepeat


class RegexAlt(_Node):
    """Alternatives, each a sequence of items."""

    branches: tuple[tuple[RegexItem, ...], ...]


class RegexPattern(_Node):
    """A parsed portable pattern; ``groups`` is its capturing-group count."""

    root: RegexAlt
    groups: int


class RegexGroupRef(_Node):
    index: int


class RegexReplacement(_Node):
    """A parsed replacement: literal text and ``\\1``-``\\9`` group references."""

    parts: tuple[str | RegexGroupRef, ...]


RegexGroup.model_rebuild()
RegexRepeat.model_rebuild()


class _Parser:
    def __init__(self, text: str) -> None:
        self.text = text
        self.pos = 0
        self.groups = 0
        self.quant_start = 0

    def peek(self, offset: int = 0) -> str:
        i = self.pos + offset
        return self.text[i] if i < len(self.text) else ""

    def at_end(self) -> bool:
        return self.pos >= len(self.text)

    def alt(self, *, nested: bool) -> RegexAlt:
        branches = [self.seq()]
        while self.peek() == "|":
            self.pos += 1
            branches.append(self.seq())
        if self.peek() == ")" and not nested:
            raise RegexPatternError("the unbalanced parenthesis ')'")
        return RegexAlt(branches=tuple(branches))

    def seq(self) -> tuple[RegexItem, ...]:
        items: list = []
        while not self.at_end() and self.peek() not in "|)":
            atom = self.atom()
            items.append(self.quantified(atom))
        return tuple(items)

    def atom(self) -> RegexAtom:
        ch = self.peek()
        if ch in "*+?":
            raise RegexPatternError(f"the quantifier '{ch}' with nothing to repeat")
        if ch == "{":
            raise RegexPatternError("the brace '{' outside a repetition {m}, {m,} or {m,n} (write \\{ for a brace)")
        self.pos += 1
        if ch == "(":
            return self.group()
        if ch == "[":
            return self.bracket()
        if ch == ".":
            return RegexAny()
        if ch == "^":
            return RegexAnchor(at="start")
        if ch == "$":
            return RegexAnchor(at="end")
        if ch == "\\":
            return self.escape(in_bracket=False)
        return RegexChar(char=ch)

    def group(self) -> RegexGroup:
        if self.peek() == "?":
            rest = self.text[self.pos - 1:]
            for prefix, construct in _GROUP_FORMS:
                if rest.startswith(prefix):
                    raise RegexPatternError(construct)
            raise RegexPatternError(f"the inline group '{rest[:3]}…'")
        self.groups += 1
        index = self.groups
        body = self.alt(nested=True)
        if self.peek() != ")":
            raise RegexPatternError("the unbalanced parenthesis '(' (an unclosed group)")
        self.pos += 1
        return RegexGroup(index=index, body=body)

    def escape(self, *, in_bracket: bool) -> RegexChar | RegexClass:
        if self.at_end():
            raise RegexPatternError("the trailing backslash '\\'")
        ch = self.peek()
        self.pos += 1
        if ch.lower() in _CLASS_ESCAPES:
            negated = ch.isupper()
            if negated and in_bracket:
                raise RegexPatternError(f"the negated class '\\{ch}' inside a bracket expression")
            return RegexClass(kind=ch.lower(), negated=negated)  # pyright: ignore[reportArgumentType]
        if ch in _CONTROL_ESCAPES:
            return RegexChar(char=_CONTROL_ESCAPES[ch])
        if ch.isdigit():
            raise RegexPatternError(f"the backreference '\\{ch}'")
        if ch in _LETTER_ESCAPES:
            raise RegexPatternError(_LETTER_ESCAPES[ch])
        if ch in _PUNCTUATION or not ch.isascii():
            return RegexChar(char=ch)
        raise RegexPatternError(f"the escape '\\{ch}'")

    def bracket(self) -> RegexBracket:
        negated = self.peek() == "^"
        if negated:
            self.pos += 1
        items: list = []
        while True:
            if self.at_end():
                raise RegexPatternError("the unterminated bracket expression '['")
            # A leading ``]`` is literal.
            if self.peek() == "]" and items:
                self.pos += 1
                return RegexBracket(negated=negated, items=tuple(items))
            items.append(self.bracket_item())

    def bracket_item(self) -> BracketItem:
        ch = self.peek()
        if ch == "[" and self.peek(1) in (":", ".", "="):
            end = self.text.find("]", self.pos)
            construct = self.text[self.pos + 1:end] if end > 0 else self.text[self.pos + 1:]
            raise RegexPatternError(f"the POSIX class '[{construct}]'")
        self.pos += 1
        item = self.escape(in_bracket=True) if ch == "\\" else RegexChar(char=ch)
        if isinstance(item, RegexChar) and self.peek() == "-" and self.peek(1) not in ("]", ""):
            return self.bracket_range(item)
        return item

    def bracket_range(self, low: RegexChar) -> RegexRange:
        self.pos += 1
        high_ch = self.peek()
        self.pos += 1
        high = self.escape(in_bracket=True) if high_ch == "\\" else RegexChar(char=high_ch)
        if not isinstance(high, RegexChar) or ord(high.char) < ord(low.char):
            raise RegexPatternError(f"the range '{low.char}-{high_ch}'")
        return RegexRange(low=low.char, high=high.char)

    def quantified(self, atom: RegexAtom) -> RegexItem:
        bounds = self.quantifier()
        if bounds is None:
            return atom
        if isinstance(atom, RegexAnchor):
            raise RegexPatternError("a quantifier on an anchor")
        nxt = self.peek()
        if nxt == "?":
            raise RegexPatternError(f"the lazy quantifier '{self.text[self.quant_start:self.pos + 1]}'")
        if nxt == "+":
            raise RegexPatternError(f"the possessive quantifier '{self.text[self.quant_start:self.pos + 1]}'")
        if nxt and nxt in "*{":
            raise RegexPatternError(f"the stacked quantifier '{self.text[self.quant_start:self.pos + 1]}'")
        low, high = bounds
        return RegexRepeat(node=atom, low=low, high=high)

    def quantifier(self) -> tuple[int, int | None] | None:
        self.quant_start = self.pos
        ch = self.peek()
        if ch in ("*", "+", "?"):
            self.pos += 1
            return {"*": (0, None), "+": (1, None), "?": (0, 1)}[ch]
        if ch != "{":
            return None
        end = self.text.find("}", self.pos)
        body = self.text[self.pos + 1:end] if end > 0 else ""
        low_text, comma, high_text = body.partition(",")
        if end < 0 or not low_text.isdigit() or not (high_text.isdigit() or not high_text):
            raise RegexPatternError("the brace '{' outside a repetition {m}, {m,} or {m,n} (write \\{ for a brace)")
        self.pos = end + 1
        low = int(low_text)
        if high_text:
            high = int(high_text)
        else:
            high = None if comma else low
        if low > MAX_REPEAT or (high is not None and high > MAX_REPEAT):
            raise RegexPatternError(f"the repetition count above {MAX_REPEAT} in '{{{body}}}'")
        if high is not None and high < low:
            raise RegexPatternError(f"the repetition '{{{body}}}' (its maximum is below its minimum)")
        return low, high


@lru_cache(maxsize=4096)
def parse_regex(text: str) -> RegexPattern:
    """``text`` as a portable pattern; ``RegexPatternError`` names the first construct outside the subset."""
    parser = _Parser(text)
    root = parser.alt(nested=False)
    return RegexPattern(root=root, groups=parser.groups)


@lru_cache(maxsize=4096)
def parse_replacement(text: str, *, groups: int) -> RegexReplacement:
    """A replacement over a pattern with ``groups`` groups: ``\\1``-``\\9`` insert a group, ``\\\\`` a backslash."""
    parts: list = []
    literal: list = []
    i = 0
    while i < len(text):
        ch = text[i]
        if ch != "\\":
            literal.append(ch)
            i += 1
            continue
        nxt = text[i + 1] if i + 1 < len(text) else ""
        if nxt == "\\":
            literal.append("\\")
        elif nxt.isdigit() and nxt != "0" and int(nxt) <= groups:
            if literal:
                parts.append("".join(literal))
                literal = []
            parts.append(RegexGroupRef(index=int(nxt)))
        elif nxt.isdigit():
            raise RegexPatternError(f"the replacement group reference '\\{nxt}' (the pattern has {groups} groups)")
        else:
            raise RegexPatternError(f"the replacement escape '\\{nxt}' (only \\1-\\9 and \\\\ are)")
        i += 2
    if literal:
        parts.append("".join(literal))
    return RegexReplacement(parts=tuple(parts))


__all__ = [
    "MAX_REPEAT", "RegexAlt", "RegexAnchor", "RegexAny", "RegexAtom", "RegexBracket", "RegexChar", "RegexClass",
    "RegexGroup", "RegexGroupRef", "RegexItem", "RegexPattern", "RegexPatternError", "RegexRange",
    "RegexRepeat", "RegexReplacement", "parse_regex", "parse_replacement",
]
