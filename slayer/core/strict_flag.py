"""The deprecated ``strict`` query flag, read one way by the stored v3→v4 migration and by fresh input."""

from typing import Any

from slayer.core.errors import UnrecognizedStrictValueError

# Legacy quoted tokens; blank/whitespace counts as false. Anything else fails closed.
_TRUE_TOKENS = frozenset({"1", "true", "t", "yes", "y", "on"})
_FALSE_TOKENS = frozenset({"0", "false", "f", "no", "n", "off", ""})


def strict_flag(value: Any) -> bool:
    """``value`` read as a ``strict`` flag: a string must be a legacy token, anything else counts by truthiness."""
    if not isinstance(value, str):
        return bool(value)
    token = value.strip().lower()
    if token in _TRUE_TOKENS:
        return True
    if token in _FALSE_TOKENS:
        return False
    raise UnrecognizedStrictValueError(value)


def without_strict(data: dict[str, Any]) -> dict[str, Any]:
    """``data`` with ``strict`` dropped; a true flag sets ``to_many_handling="error"`` unless one is given."""
    out = dict(data)
    if strict_flag(out.pop("strict")):
        out.setdefault("to_many_handling", "error")
    return out
