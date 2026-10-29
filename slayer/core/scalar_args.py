"""Library-function argument contracts on literal values: patterns, paths, flags, groups, indices, domains."""

from __future__ import annotations

from decimal import Decimal
from types import MappingProxyType
from typing import Callable, Mapping, Optional, Sequence, Tuple

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


_Literals = Sequence[Optional[Decimal]]
_Violation = Optional[Tuple[Tuple[int, ...], str]]


def _first(v: _Literals) -> Optional[Decimal]:
    return v[0] if v else None


def _unit_interval(v: _Literals) -> _Violation:
    first = _first(v)
    low, high = _UNIT_INTERVAL
    return ((0,), "outside [-1, 1]") if first is not None and not low <= first <= high else None


def _nonzero_divisor(v: _Literals) -> _Violation:
    return ((1,), "a zero divisor") if len(v) > 1 and v[1] == 0 else None


def _code_point(v: _Literals) -> _Violation:
    first = _first(v)
    if first is None:
        return None
    if first != first.to_integral_value() or not 1 <= first <= MAX_CODE_POINT or SURROGATES[0] <= first <= SURROGATES[1]:
        return (0,), "not a character code point (1 to 0x10FFFF, surrogates excluded)"
    return None


def _positive(v: _Literals) -> _Violation:
    first = _first(v)
    return ((0,), "not positive") if first is not None and first <= 0 else None


def _log(v: _Literals) -> _Violation:
    if len(v) != 2:
        return _positive(v) if len(v) == 1 else None
    base, x = v
    if base is not None and (base <= 0 or base == 1):
        return (0,), "not a logarithm base (positive and not 1)"
    return ((1,), "not positive") if x is not None and x <= 0 else None


def _non_negative(v: _Literals) -> _Violation:
    first = _first(v)
    return ((0,), "negative") if first is not None and first < 0 else None


def _power(v: _Literals) -> _Violation:
    if len(v) != 2:
        return None
    base, exponent = v
    if base is None or exponent is None:
        return None
    if base < 0 and exponent != exponent.to_integral_value():
        return (0, 1), "a negative base with a non-integer exponent"
    if base == 0 and exponent < 0:
        return (0, 1), "zero to a negative exponent"
    return None


_DOMAINS: Mapping[str, Callable[[_Literals], _Violation]] = MappingProxyType({
    "asin": _unit_interval, "acos": _unit_interval, "div": _nonzero_divisor, "chr": _code_point,
    "ln": _positive, "log10": _positive, "log2": _positive, "log": _log, "sqrt": _non_negative,
    "pow": _power, "power": _power,
})


def _domain_violation(fn: str, values: Sequence[object]) -> _Violation:
    """The positions and reason of literal ``values`` outside ``fn``'s domain, else ``None`` (non-literals pass)."""
    check = _DOMAINS.get(fn)
    if check is None:
        return None
    return check([value if isinstance(value, Decimal) else None for value in values])


def check_domain(fn: str, values: Sequence[object], *, texts: Sequence[str]) -> None:
    """Literal arguments outside ``fn``'s domain (whose column form yields NULL) are a plan-time error."""
    violation = _domain_violation(fn, values)
    if violation is None:
        return
    positions, reason = violation
    shown = ", ".join(texts[i] for i in positions)
    noun = "argument" if len(positions) == 1 else "arguments"
    raise _error(
        f"{fn}() {noun} {shown} {'is' if len(positions) == 1 else 'are'} {reason}.",
        suggestion=f"Use an argument inside the domain of {fn}(); over a column, out-of-domain values yield NULL.",
    )


__all__ = [
    "MAX_CODE_POINT", "REGEX_FUNCTIONS", "STRING_SLOTS", "SURROGATES", "check_domain", "check_flags", "check_group",
    "check_path", "check_pattern", "check_replacement", "check_split_index", "slot_name",
]
