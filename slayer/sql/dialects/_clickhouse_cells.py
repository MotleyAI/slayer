"""Readers for the ClickHouse HTTP driver's text cells: ``Bool`` and ``Array(T)``."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

_BOOL_TEXT = {"true": True, "false": False}
_ESCAPES = {
    "n": "\n", "t": "\t", "r": "\r", "0": "\0", "b": "\b", "f": "\f", "a": "\a", "v": "\v", "e": "\x1b",
    "\\": "\\", "'": "'", '"': '"',
}
_HEX = frozenset("0123456789abcdefABCDEF")
_WRAPPERS = ("Nullable(", "LowCardinality(")


def _unwrapped(type_code: str) -> str:
    while type_code.startswith(_WRAPPERS) and type_code.endswith(")"):
        type_code = type_code[type_code.index("(") + 1:-1]
    return type_code


def read_bool(value: Any) -> Any:
    return _BOOL_TEXT.get(value, value) if isinstance(value, str) else value


def _scalar_reader(type_code: str) -> Optional[Callable[[str], Any]]:
    """How to read one element's text; ``None`` for an element kept as text (nested lists, tuples, …)."""
    base = _unwrapped(type_code)
    if base.startswith(("Int", "UInt")):
        return int
    if base.startswith("Float"):
        return float
    if base.startswith("Decimal"):
        return Decimal
    if base == "Bool":
        return lambda text: _BOOL_TEXT[text]
    if base.startswith("Date32") or base == "Date":
        return date.fromisoformat
    if base.startswith("DateTime"):
        return datetime.fromisoformat
    if base.startswith(("String", "FixedString", "Enum", "UUID")):
        return str
    return None


def _tokens(text: str) -> list[Optional[str]]:
    """The elements of a flat ClickHouse array literal (``['a','b\\'c',NULL]``), ``NULL`` as ``None``."""
    out: list[Optional[str]] = []
    i, n = 1, len(text) - 1
    while i < n:
        if text[i] in ", ":
            i += 1
            continue
        token, i = _quoted(text, i) if text[i] == "'" else _bare(text, i, n)
        out.append(token)
    return out


def _quoted(text: str, i: int) -> tuple[str, int]:
    """The quoted string opening at ``text[i]`` and the index after its closing quote."""
    buf, i = [], i + 1
    while text[i] != "'":
        if text[i] == "\\":
            char, i = _escape(text, i)
            buf.append(char)
        else:
            buf.append(text[i])
            i += 1
    return "".join(buf), i + 1


def _escape(text: str, i: int) -> tuple[str, int]:
    """The escape at ``text[i]`` (a backslash) and the index after it; an unknown or malformed one is the
    backslash itself, as ClickHouse reads it."""
    char = text[i + 1]
    if char in _ESCAPES:
        return _ESCAPES[char], i + 2
    digits = text[i + 2:i + 4]
    if char == "x" and len(digits) == 2 and set(digits) <= _HEX:
        return chr(int(digits, 16)), i + 4
    return "\\", i + 1


def _bare(text: str, i: int, n: int) -> tuple[Optional[str], int]:
    """The unquoted token starting at ``text[i]`` (``NULL`` as ``None``) and the index of its delimiter."""
    end = text.find(",", i, n)
    end = n if end < 0 else end
    token = text[i:end].strip()
    return (None if token == "NULL" else token), end


def array_reader(type_code: str) -> Optional[Callable[[Any], Any]]:
    """The reader of an ``Array(T)`` column's text cells; ``None`` when ``T`` is not a scalar."""
    if not (type_code.startswith("Array(") and type_code.endswith(")")):
        return None
    element = _scalar_reader(type_code[len("Array("):-1])
    if element is None:
        return None
    read = element

    def read_array(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        return [None if t is None else read(t) for t in _tokens(value)]
    return read_array
