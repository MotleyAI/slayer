"""Pattern re-emission: one portable regex / LIKE pattern spelled in each engine's own syntax."""

from __future__ import annotations

from typing import List, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.like_pattern import LikePattern, LikeWildcard
from slayer.core.regex_pattern import (
    RegexAlt,
    RegexAny,
    RegexAnchor,
    RegexBracket,
    RegexChar,
    RegexClass,
    RegexGroup,
    RegexGroupRef,
    RegexItem,
    RegexPattern,
    RegexRange,
    RegexRepeat,
    RegexReplacement,
)

_META = frozenset("\\.^$|?*+()[]{}")
_SPACE = (" ", "\t", "\n", "\r", "\f", "\v")
#: ``\d`` / ``\w`` / ``\s`` as ASCII code-point ranges.
_CLASS_RANGES: dict = {
    "d": [("0", "9")],
    "w": [("A", "Z"), ("a", "z"), ("0", "9"), ("_", "_")],
    "s": [(c, c) for c in _SPACE],
}


class RegexFlavour(BaseModel):
    """How one engine spells the portable meaning."""

    model_config = ConfigDict(frozen=True)

    #: Inline ``(?s)`` (and ``(?i)``) flags; otherwise the caller passes them as a match parameter.
    inline_flags: bool = True
    #: The end-of-string anchor (``$`` where it never matches before a trailing newline).
    end_anchor: str = "$"
    #: Replacement group references: ``\1`` or ``$1``.
    group_ref: Literal["backslash", "dollar"] = "backslash"
    #: Whether ``&`` in a replacement inserts the whole match (so a literal one is escaped).
    replacement_ampersand: bool = False


POSTGRES = RegexFlavour(inline_flags=False)
RE2 = RegexFlavour()
JAVA = RegexFlavour(end_anchor="\\z", group_ref="dollar")
ICU = RegexFlavour(end_anchor="\\z", group_ref="dollar")
POSIX = RegexFlavour(inline_flags=False)
PYTHON = RegexFlavour(inline_flags=False, end_anchor="\\Z")


class _Emitter:
    def __init__(self, flavour: RegexFlavour, *, capture: Optional[int]) -> None:
        self.flavour = flavour
        self.capture = capture

    def alt(self, alt: RegexAlt) -> str:
        return "|".join("".join(self.item(i) for i in seq) for seq in alt.branches)

    def item(self, item: RegexItem) -> str:
        if not isinstance(item, RegexRepeat):
            return self.atom(item)
        low, high = item.low, item.high
        quant = {(0, None): "*", (1, None): "+", (0, 1): "?"}.get((low, high))
        if quant is None:
            quant = f"{{{low}}}" if high == low else f"{{{low},}}" if high is None else f"{{{low},{high}}}"
        return self.atom(item.node) + quant

    def atom(self, atom: object) -> str:
        if isinstance(atom, RegexChar):
            return "\\" + atom.char if atom.char in _META else atom.char
        if isinstance(atom, RegexAny):
            return "."
        if isinstance(atom, RegexAnchor):
            return "^" if atom.at == "start" else self.flavour.end_anchor
        if isinstance(atom, RegexClass):
            return self.bracket(atom.negated, [_class_ranges(atom)])
        if isinstance(atom, RegexBracket):
            return self.bracket(atom.negated, [_item_ranges(i) for i in atom.items])
        if isinstance(atom, RegexGroup):
            captured = self.capture is None or atom.index == self.capture
            return ("(" if captured else "(?:") + self.alt(atom.body) + ")"
        raise TypeError(f"unknown regex node {type(atom).__name__}")

    def bracket(self, negated: bool, ranges: List[List[Tuple[str, str]]]) -> str:
        flat = [r for group in ranges for r in group]
        return ("[^" if negated else "[") + _escaped_bracket(flat) + "]"


def _class_ranges(cls: RegexClass) -> List[Tuple[str, str]]:
    return list(_CLASS_RANGES[cls.kind])


def _item_ranges(item: object) -> List[Tuple[str, str]]:
    if isinstance(item, RegexChar):
        return [(item.char, item.char)]
    if isinstance(item, RegexRange):
        return [(item.low, item.high)]
    if isinstance(item, RegexClass):
        return _class_ranges(item)
    raise TypeError(f"unknown bracket item {type(item).__name__}")


_BRACKET_SPECIALS = frozenset("\\]^-[&~")


def _escaped_bracket(ranges: List[Tuple[str, str]]) -> str:
    def one(ch: str) -> str:
        return "\\" + ch if ch in _BRACKET_SPECIALS else ch
    return "".join(one(lo) if lo == hi else f"{one(lo)}-{one(hi)}" for lo, hi in ranges)


def emit_regex(
    pattern: RegexPattern, flavour: RegexFlavour, *, case_insensitive: bool = False,
    capture: Optional[int] = None, wrap: bool = False, anchored: bool = False,
) -> str:
    """``pattern`` in ``flavour``'s syntax. ``capture`` keeps only that group capturing (0: none); ``wrap``
    captures the whole match as group 1; ``anchored`` makes it match the whole string."""
    body = _Emitter(flavour, capture=capture).alt(pattern.root)
    if wrap:
        body = f"({body})"
    if anchored:
        body = f"^(?:{body}){flavour.end_anchor}"
    if flavour.inline_flags:
        body = ("(?si)" if case_insensitive else "(?s)") + body
    return body


def emit_replacement(replacement: RegexReplacement, flavour: RegexFlavour) -> str:
    """``replacement`` with group references and literal ``\\`` / ``$`` spelled for ``flavour``."""
    out: List[str] = []
    for part in replacement.parts:
        if isinstance(part, RegexGroupRef):
            out.append(f"${part.index}" if flavour.group_ref == "dollar" else f"\\{part.index}")
        else:
            text = part.replace("\\", "\\\\")
            if flavour.group_ref == "dollar":
                text = text.replace("$", "\\$")
            if flavour.replacement_ampersand:
                text = text.replace("&", "\\&")
            out.append(text)
    return "".join(out)


def like_text(pattern: LikePattern, *, brackets: bool = False) -> str:
    """``pattern`` as LIKE text escaped by ``\\`` (``brackets``: ``[`` as ``[[]``, SQL Server)."""
    out: List[str] = []
    for part in pattern.parts:
        if isinstance(part, LikeWildcard):
            out.append("%" if part.kind == "any" else "_")
            continue
        for ch in part:
            if ch in "%_\\":
                out.append("\\" + ch)
            elif ch == "[" and brackets:
                out.append("[[]")
            else:
                out.append(ch)
    return "".join(out)


_SENTINELS = ("\x01", "\x02", "\x03")


def _replace(value: Expression, old: str, new: str) -> Expression:
    return exp.Anonymous(this="REPLACE", expressions=[value, exp.Literal.string(old), exp.Literal.string(new)])


def like_expression(pattern: Expression, *, brackets: bool = False) -> Expression:
    """A run-time pattern in the LIKE language as ``\\``-escaped LIKE text: a lone backslash made literal
    (``brackets``: ``[`` as ``[[]``)."""
    escaped = (("\\\\", _SENTINELS[0]), ("\\%", _SENTINELS[1]), ("\\_", _SENTINELS[2]))
    value = pattern
    for old, mark in escaped:
        value = _replace(value, old, mark)
    value = _replace(value, "\\", "\\\\")
    for old, mark in escaped:
        value = _replace(value, mark, old)
    return _replace(value, "[", "[[]") if brackets else value


__all__ = [
    "ICU", "JAVA", "POSIX", "POSTGRES", "PYTHON", "RE2", "RegexFlavour", "emit_regex", "emit_replacement",
    "like_expression", "like_text",
]
