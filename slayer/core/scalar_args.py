"""Library-function argument contracts on literal values: patterns, paths, flags, groups, indices, domains."""

from __future__ import annotations

from decimal import Decimal
from types import MappingProxyType
from typing import Mapping, Optional, Tuple

from slayer.core.errors import ScalarArgumentError
from slayer.core.json_path import JsonPath, JsonPathError, parse_json_path
from slayer.core.regex_pattern import (
    RegexPattern,
    RegexPatternError,
    RegexReplacement,
    parse_regex,
    parse_replacement,
)

REGEX_FUNCTIONS = frozenset({"regexp_like", "regexp_extract", "regexp_replace"})
#: Argument positions that take only a string literal or a ``{variable}`` holding the text.
STRING_SLOTS: Mapping[str, Tuple[int, ...]] = MappingProxyType({
    "regexp_like": (1,), "regexp_extract": (1,), "regexp_replace": (1, 2), "json_value": (1,),
})
_SLOT_NAMES = {(1, "json_value"): "path", (2, "regexp_replace"): "replacement"}
#: The real domain of the inverse sines and cosines.
_UNIT_INTERVAL = (Decimal(-1), Decimal(1))
MAX_CODE_POINT = 0x10FFFF
SURROGATES = (0xD800, 0xDFFF)


def slot_name(fn: str, pos: int) -> str:
    return _SLOT_NAMES.get((pos, fn), "pattern")


def _where(variable: Optional[str]) -> str:
    return f" (from the variable {{{variable}}})" if variable else ""


def _error(summary: str, *, suggestion: str) -> ScalarArgumentError:
    return ScalarArgumentError(summary=summary, suggestion=suggestion)


def check_pattern(fn: str, text: str, *, variable: Optional[str] = None) -> RegexPattern:
    try:
        return parse_regex(text)
    except RegexPatternError as e:
        raise _error(
            f"{fn}() pattern {text!r}{_where(variable)}: {e}.",
            suggestion="Use the portable subset: literals, ., [...], [^...], * + ? {m,n}, ^ $, |, ( ), "
            "\\d \\w \\s and their negations, \\n \\t \\r, and a backslash before a metacharacter.",
        ) from None


def check_replacement(fn: str, text: str, *, pattern: RegexPattern, variable: Optional[str] = None) -> RegexReplacement:
    try:
        return parse_replacement(text, groups=pattern.groups)
    except RegexPatternError as e:
        raise _error(
            f"{fn}() replacement {text!r}{_where(variable)}: {e}.",
            suggestion="In a replacement, \\1-\\9 insert a group of the pattern and \\\\ a backslash; "
            "everything else is literal.",
        ) from None


def check_path(fn: str, text: str, *, variable: Optional[str] = None) -> JsonPath:
    try:
        return parse_json_path(text)
    except JsonPathError as e:
        raise _error(
            f"{fn}() path {text!r}{_where(variable)}: {e}.",
            suggestion="Write $ followed by .key, .\"quoted key\" and [n] steps.",
        ) from None


def check_flags(fn: str, value: object) -> bool:
    """Whether the flags literal asks for case-insensitive matching (only ``'i'`` exists)."""
    if value != "i":
        raise _error(
            f"{fn}() flag {value!r} is not supported.",
            suggestion="The only flag is the literal 'i' (case-insensitive); matching is case-sensitive without it.",
        )
    return True


def check_group(fn: str, value: object, *, pattern: RegexPattern) -> int:
    if not isinstance(value, Decimal) or value != value.to_integral_value() or not 0 <= value <= pattern.groups:
        raise _error(
            f"{fn}() group {value!r} is not a group of the pattern.",
            suggestion=f"Pass an integer literal from 0 (the whole match) to {pattern.groups} (its group count).",
        )
    return int(value)


def check_split_index(value: object) -> int:
    if not isinstance(value, Decimal) or value != value.to_integral_value() or value == 0:
        raise _error(
            f"split_part() field index {value!r} is not a non-zero integer literal.",
            suggestion="Pass 1, 2, … to count fields from the start, or -1, -2, … to count from the end.",
        )
    return int(value)


def check_domain(fn: str, position: int, value: object, *, text: str) -> None:
    """A literal argument outside ``fn``'s domain (whose column form yields NULL) is a plan-time error."""
    if not isinstance(value, Decimal):
        return
    low, high = _UNIT_INTERVAL
    if fn in ("asin", "acos") and not low <= value <= high:
        reason = "outside [-1, 1]"
    elif fn == "div" and position == 1 and value == 0:
        reason = "a zero divisor"
    elif fn == "chr" and (
        value != value.to_integral_value() or value < 0 or value > MAX_CODE_POINT
        or SURROGATES[0] <= value <= SURROGATES[1]
    ):
        reason = "not a Unicode scalar value (0 to 0x10FFFF, surrogates excluded)"
    else:
        return
    raise _error(
        f"{fn}() argument {text} is {reason}.",
        suggestion=f"Use an argument inside the domain of {fn}(); over a column, out-of-domain values yield NULL.",
    )


__all__ = [
    "MAX_CODE_POINT", "REGEX_FUNCTIONS", "STRING_SLOTS", "SURROGATES", "check_domain", "check_flags", "check_group",
    "check_path", "check_pattern", "check_replacement", "check_split_index", "slot_name",
]
