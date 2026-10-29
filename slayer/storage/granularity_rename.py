"""A stored datasource granularity named like a reserved library name is renamed, with every reference to it.

The rename map is a pure function of the stored granularity names, so a rename interrupted after some documents
were rewritten recomputes the same map and converges. References are rewritten token-exactly, so a rewritten
document is a fixed point.
"""

import re
from collections.abc import Callable, Mapping
from functools import partial
from typing import Any

from slayer.core.enums import ALL_TRANSFORMS, BUILTIN_AGGREGATIONS, normalize_aggregation_name
from slayer.core.models import reserved_granularity_names
from slayer.core.time_points import relative_unit_word
from slayer.storage.rank_direction_migration import rewrite_query_fields

#: Casefolded stored name → its new name.
Renames = Mapping[str, str]

_TOKEN = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"|[A-Za-z_]\w*|\S")


def granularity_renames(names: list[str]) -> dict[str, str]:
    """``<name>_granularity`` (else ``<name>_granularity_<n>``, smallest free ``n`` ≥ 2) for each reserved name."""
    reserved = reserved_granularity_names()
    taken = {n.casefold() for n in names}
    out: dict[str, str] = {}
    for name in names:
        if name.casefold() not in reserved:
            continue
        candidate, n = f"{name}_granularity", 2
        while candidate.casefold() in taken:
            candidate, n = f"{name}_granularity_{n}", n + 1
        taken.add(candidate.casefold())
        out[name.casefold()] = candidate
    return out


def rename_in_text(text: str, *, renames: Renames) -> str:
    """``text`` with each granularity call ``g(…)``, colon suffix ``x:g``, transform unit ``'g'`` and relative-token
    unit renamed; a call or suffix spelling a built-in aggregation or transform always was that, never ``g``."""
    pieces: list[str] = []
    last = 0
    tokens = list(_TOKEN.finditer(text))
    callees: list[str | None] = []
    for i, match in enumerate(tokens):
        token = match.group()
        before = tokens[i - 1].group() if i > 0 else ""
        after = tokens[i + 1].group() if i + 1 < len(tokens) else ""
        replacement = None
        if token == "(":
            callees.append(before.lower() if before[:1].isalpha() or before[:1] == "_" else None)
        elif token == ")" and callees:
            callees.pop()
        elif token[0] in "'\"":
            replacement = _renamed_literal(token, renames=renames, callee=callees[-1] if callees else None)
        elif token.casefold() in renames and not _always_callable(token) and (
            (after == "(" and before != ".") or (before == ":" and (i < 2 or tokens[i - 2].group() != ":"))
        ):
            replacement = renames[token.casefold()]
        if replacement is not None:
            pieces.append(text[last:match.start()] + replacement)
            last = match.end()
    return "".join(pieces) + text[last:]


def _always_callable(name: str) -> bool:
    return normalize_aggregation_name(name) in BUILTIN_AGGREGATIONS or name.lower() in ALL_TRANSFORMS


def _renamed_literal(token: str, *, renames: Renames, callee: str | None) -> str | None:
    """A quoted token naming a transform's unit or holding a relative token, renamed (either quote style: stored
    text of the pre-SQL grammar double-quotes its strings)."""
    quote = token[0]
    value = token[1:-1].replace(quote * 2, quote)
    if value.casefold() in renames and callee in ALL_TRANSFORMS:
        renamed = renames[value.casefold()]
    else:
        renamed = rename_relative(value, renames=renames)
    return None if renamed == value else quote + renamed.replace(quote, quote * 2) + quote


def rename_relative(text: str, *, renames: Renames) -> str:
    """A relative time token (``last 3 gs``, ``this g``) with its unit renamed."""
    word = relative_unit_word(text)
    if word is None:
        return text
    for stem, plural in ((word, ""), (word[:-1], "s")) if word.endswith("s") else ((word, ""),):
        if stem in renames:
            return re.sub(rf"(?i)\b{re.escape(stem)}{plural}\b", renames[stem] + plural, text, count=1)
    return text


def rename_in_query(data: dict, *, renames: Renames) -> dict:
    """A stored query dict with every granularity reference renamed (inline models and extensions included)."""
    text = partial(rename_in_text, renames=renames)
    data = rewrite_query_fields(dict(data), text)
    if isinstance(data.get("time_dimensions"), list):
        data["time_dimensions"] = [_renamed_time_dimension(td, renames=renames) for td in data["time_dimensions"]]
    source = data.get("source_model")
    if isinstance(source, dict) and "source_name" in source:
        data["source_model"] = {**source, "measures": [_renamed_formula(m, text) for m in source.get("measures") or []]}
    elif isinstance(source, dict):
        data["source_model"] = rename_in_model(source, renames=renames)
    return data


def _renamed_time_dimension(item: Any, *, renames: Renames) -> Any:
    if not isinstance(item, dict):
        return item
    out = dict(item)
    if isinstance(out.get("granularity"), str) and out["granularity"].casefold() in renames:
        out["granularity"] = renames[out["granularity"].casefold()]
    date_range = out.get("date_range")
    if isinstance(date_range, str):
        out["date_range"] = rename_relative(date_range, renames=renames)
    elif isinstance(date_range, list):
        out["date_range"] = [rename_relative(d, renames=renames) if isinstance(d, str) else d for d in date_range]
    return out


def _renamed_formula(measure: Any, text: Callable[[str], str]) -> Any:
    if isinstance(measure, dict) and isinstance(measure.get("formula"), str):
        return {**measure, "formula": text(measure["formula"])}
    return measure


def rename_in_model(data: dict, *, renames: Renames) -> dict:
    """A stored model dict with every granularity reference renamed (column granularities, measures, stages)."""
    out = dict(data)
    text = partial(rename_in_text, renames=renames)
    if isinstance(out.get("columns"), list):
        out["columns"] = [
            {**c, "granularity": renames[c["granularity"].casefold()]}
            if isinstance(c, dict) and isinstance(c.get("granularity"), str) and c["granularity"].casefold() in renames
            else c
            for c in out["columns"]
        ]
    if isinstance(out.get("measures"), list):
        out["measures"] = [_renamed_formula(m, text) for m in out["measures"]]
    if isinstance(out.get("source_queries"), list):
        out["source_queries"] = [
            rename_in_query(q, renames=renames) if isinstance(q, dict) else q for q in out["source_queries"]
        ]
    return out


def rename_in_memory(data: dict, *, renames: Renames) -> dict:
    if not isinstance(data.get("query"), dict):
        return data
    return {**data, "query": rename_in_query(data["query"], renames=renames)}


def renamed_datasource(data: dict, *, renames: Renames) -> dict:
    granularities = [
        {**g, "name": renames[g["name"].casefold()]}
        if isinstance(g, dict) and isinstance(g.get("name"), str) and g["name"].casefold() in renames else g
        for g in data.get("granularities") or []
    ]
    return {**data, "granularities": granularities}


def memory_source_model(data: dict) -> tuple[str | None, str | None]:
    """``(inline model's data_source, saved root model name)`` a stored memory's query reads, each if any."""
    query = data.get("query")
    source = query.get("source_model") if isinstance(query, dict) else None
    if isinstance(source, dict) and "source_name" not in source:
        data_source = source.get("data_source")
        return (data_source if isinstance(data_source, str) else None), None
    if isinstance(source, dict):
        source = source.get("source_name")
    return None, (source if isinstance(source, str) else None)
