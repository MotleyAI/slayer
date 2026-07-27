"""Naive Cypher label-filter parser for the no-advanced_search fallback (DEV-1532).

Supports only: MATCH (var:Label1:Label2:...) RETURN var.id AS id
(case-insensitive, whitespace-tolerant, no WHERE clause, no relationships).

Used by SearchService.search() when cypher_filter is supplied but LadybugDB
is not installed. Complex Cypher raises SlayerError pointing at the
advanced_search extra.
"""

from __future__ import annotations

import re

from slayer.core.errors import SlayerError


_LABEL_TO_KIND: dict[str, str] = {
    "memory": "memory",
    "datasource": "datasource",
    "model": "model",
    "modelcolumn": "column",
    "column": "column",
    "measure": "measure",
    "aggregation": "aggregation",
}

# Structured pattern: one label word optionally followed by (: word)* pairs.
# \s* is only used as a delimiter between fixed tokens (never inside a
# quantified character class that can also match \s), which avoids
# polynomial backtracking on non-matching inputs (Sonar S5852).
_NAIVE_PATTERN = re.compile(
    pattern=r"^\s*MATCH\s*\(\s*(?P<var>\w+)\s*:\s*(?P<labels>\w+(?:\s*:\s*\w+)*)\s*\)\s*RETURN\s+(?P=var)\.id\s+AS\s+id\s*$",  # NOSONAR(S5843) — structural complexity is load-bearing; each token maps to a distinct syntactic Cypher element
    flags=re.IGNORECASE,
)

_AS_ID_RE = re.compile(pattern=r"\bAS\s+id\b", flags=re.IGNORECASE)


def parse_naive_label_filter(cypher: str) -> set[str]:
    """Parse a simple MATCH (n:Label1:Label2) RETURN n.id AS id expression
    and return the set of kind strings to filter search results on.

    Raises SlayerError:
    - Missing 'AS id' alias → generic validation message.
    - Pattern doesn't match (WHERE, relationship, etc.) →
      message mentions advanced_search requirement.
    - Unknown label → message says "unknown".
    """
    if not _AS_ID_RE.search(cypher):
        raise SlayerError(
            "cypher_filter must return exactly one column aliased 'id' "
            "(e.g. 'RETURN n.id AS id')."
        )
    match = _NAIVE_PATTERN.match(cypher)
    if not match:
        raise SlayerError(
            "cypher_filter expression is too complex for the naive fallback; "
            "install the advanced_search extra: "
            "pip install motley-slayer[advanced_search]"
        )
    labels_str = match.group("labels")
    labels = [lb.strip() for lb in re.split(r"\s*:\s*", labels_str) if lb.strip()]
    kinds: set[str] = set()
    for label in labels:
        kind = _LABEL_TO_KIND.get(label.lower())
        if kind is None:
            raise SlayerError(
                f"unknown entity type {label!r} in cypher_filter; "
                f"known types: {sorted(_LABEL_TO_KIND)!r}."
            )
        kinds.add(kind)
    return kinds
