"""Pre-2.0 graph labels and relationship types in a ``cypher_filter``, read as their replacements."""

from __future__ import annotations

import re

from slayer.core.vocabulary import deprecated_spelling
from slayer.core.warnings import DeprecatedInputWarning

_RENAMED = {
    "model": "Dataset", "modelcolumn": "DatasetField", "column": "Field", "measure": "Metric", "joins": "RELATIONSHIPS",
}
#: A quoted string or backquoted name, left alone; captured so ``split`` keeps it at odd indexes.
_QUOTED = re.compile(r"('(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"|`[^`]*`)")
#: A word after ``:`` outside quotes: a label or relationship type.
_LABEL = re.compile(r":\s*(\w+)")


def _cased(*, old: str, new: str) -> str:
    if old.islower():
        return new.lower()
    return new.upper() if old.isupper() and len(old) > 1 else new


def renamed_labels(cypher: str) -> tuple[str, list[DeprecatedInputWarning]]:
    """``cypher`` with each pre-2.0 label or relationship type spelled its replacement; one deprecation per form."""
    found: list[tuple[str, str]] = []

    def respell(match: re.Match[str]) -> str:
        word = match.group(1)
        new = _RENAMED.get(word.lower())
        if new is None:
            return match.group(0)
        new = _cased(old=word, new=new)
        if (word, new) not in found:
            found.append((word, new))
        return match.group(0)[: match.start(1) - match.start(0)] + new

    parts = _QUOTED.split(cypher)
    out = "".join(part if i % 2 else _LABEL.sub(respell, part) for i, part in enumerate(parts))
    return out, [deprecated_spelling(old, new) for old, new in found]
