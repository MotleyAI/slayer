"""Names compared as Ossie compares identifiers: a regular identifier without regard to case, any other as written."""

from __future__ import annotations

import re

_REGULAR = re.compile(r"(?!\d)\w+")


def identifier_key(name: str, *, quoted: bool = False) -> str:
    """The comparison key of a declared name, or of a reference part (``quoted`` when written in double quotes)."""
    return name if quoted or not _REGULAR.fullmatch(name) else name.upper()
