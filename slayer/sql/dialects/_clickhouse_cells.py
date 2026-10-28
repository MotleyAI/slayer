"""Readers for the ClickHouse HTTP driver's text cells: ``Bool`` and ``Array(T)``."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional

_BOOL_TEXT = {"true": True, "false": False}
_ESCAPES = {"n": "\n", "t": "\t", "r": "\r", "0": "\0", "b": "\b", "f": "\f"}
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
        ch = text[i]
        if ch in ", ":
            i += 1
        elif ch == "'":
            buf, i = [], i + 1
            while text[i] != "'":
                if text[i] == "\\":
                    i += 1
                    buf.append(_ESCAPES.get(text[i], text[i]))
                else:
                    buf.append(text[i])
                i += 1
            out.append("".join(buf))
            i += 1
        else:
            end = i
            while end < n and text[end] != ",":
                end += 1
            token = text[i:end].strip()
            out.append(None if token == "NULL" else token)
            i = end
    return out


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
