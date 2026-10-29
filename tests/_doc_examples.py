"""Mode-B expression strings in the JSON / YAML examples of the docs and help topics.

Underscore-prefixed so pytest skips collection. A ``json`` fence may hold one value, several
(JSON lines), ``//`` comments, or a bare ``"key": value`` fragment. A fence whose nearest non-blank
line above is ``SKIP_MARKER`` shows a rejected input and is not read. Prose is never read.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from typing import Any

import yaml
from pydantic import BaseModel

SKIP_MARKER = "<!-- docs-guard: skip -->"
_FENCE = re.compile(r"^(?P<indent>\s*)(?P<fence>`{3,}|~{3,})\s*(?P<lang>[\w+-]*)")
_JSON_LANGS = frozenset({"json", "jsonc"})
_YAML_LANGS = frozenset({"yaml", "yml"})
#: Keys only a model or a model extension carries.
_MODEL_KEYS = frozenset({
    "sql_table", "sql", "columns", "data_source", "joins", "source_name", "default_time_dimension",
    "source_queries", "aggregations",
})
#: Mode-A subtrees of a model: raw SQL, column filters, custom-aggregation templates.
_MODEL_SKIPPED = frozenset({"sql", "sql_table", "filters", "columns", "aggregations"})
_QUERY_FIELDS = frozenset({"measures", "dimensions", "time_dimensions", "filters", "order"})


class Snippet(BaseModel):
    """One Mode-B string and where it was found."""

    path: str
    line: int
    text: str


class Malformed(BaseModel):
    """A JSON / YAML block that could not be read."""

    path: str
    line: int
    error: str


class Extraction(BaseModel):
    snippets: list[Snippet] = []
    malformed: list[Malformed] = []


# --------------------------------------------------------------------------- #
# Walking a parsed value
# --------------------------------------------------------------------------- #
def mode_b_values(node: Any) -> list[str]:
    """Every Mode-B string in a parsed query, model or fragment, in document order."""
    return list(_walk(node))


def _walk(node: Any) -> Iterator[str]:
    if isinstance(node, list):
        for item in node:
            yield from _walk(item)
    elif isinstance(node, dict):
        yield from (_model if _MODEL_KEYS & node.keys() else _query)(node)


def _model(node: dict[str, Any]) -> Iterator[str]:
    for key, value in node.items():
        if key == "measures":
            yield from _measures(value)
        elif key not in _MODEL_SKIPPED:
            yield from _walk(value)


def _query(node: dict[str, Any]) -> Iterator[str]:
    if "params" in node:  # a custom aggregation: its formula is a SQL template
        return
    for key in ("formula", "expression"):
        if isinstance(node.get(key), str):
            yield node[key]
    if isinstance(node.get("dimension"), str) and "granularity" in node:
        yield node["dimension"]
    for key, value in node.items():
        if key in _QUERY_FIELDS:
            yield from _FIELD_READERS[key](value)
        elif key not in ("formula", "expression"):
            yield from _walk(value)


def _strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, list):
        yield from (v for v in value if isinstance(v, str))


def _entries(value: Any, *, key: str) -> Iterator[str]:
    for entry in value if isinstance(value, list) else [value]:
        if isinstance(entry, str):
            yield entry
        elif isinstance(entry, dict) and isinstance(entry.get(key), str):
            yield entry[key]


def _measures(value: Any) -> Iterator[str]:
    return _entries(value, key="formula")


_FIELD_READERS = {
    "measures": _measures,
    "dimensions": lambda v: _entries(v, key="expression"),
    "time_dimensions": lambda v: _entries(v, key="dimension"),
    "filters": _strings,
    "order": lambda v: _entries(v, key="column"),
}


# --------------------------------------------------------------------------- #
# Reading blocks
# --------------------------------------------------------------------------- #
def _strip_line_comments(text: str) -> str:
    """Drop ``//`` comments outside JSON strings."""
    out: list[str] = []
    for line in text.splitlines():
        in_string = escaped = False
        for i, ch in enumerate(line):
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = in_string
            elif ch == '"':
                in_string = not in_string
            elif ch == "/" and not in_string and line[i:i + 2] == "//":
                line = line[:i]
                break
        out.append(line)
    return "\n".join(out)


def _json_values(text: str) -> list[Any]:
    """The JSON values in ``text``: one, several in sequence, or a bare ``"key": value`` fragment."""
    text = _strip_line_comments(text).strip()
    if not text:
        return []
    try:
        return _json_sequence(text)
    except json.JSONDecodeError:
        if text.startswith('"'):
            return _json_sequence("{" + text.rstrip(",") + "}")
        raise


def _json_sequence(text: str) -> list[Any]:
    decoder = json.JSONDecoder()
    values: list[Any] = []
    pos = 0
    while pos < len(text):
        value, pos = decoder.raw_decode(text, pos)
        values.append(value)
        while pos < len(text) and text[pos] in " \t\r\n,":
            pos += 1
    return values


def _yaml_values(text: str) -> list[Any]:
    return [doc for doc in yaml.safe_load_all(text) if doc is not None]


def _scalar_pattern(value: str) -> re.Pattern[str]:
    """``value`` as a whole JSON string or YAML scalar, never as part of a longer one."""
    plain = re.escape(value)
    quoted = re.escape(json.dumps(value))
    yaml_slot = r"^\s*(?:-\s+|-?\s*(?:formula|expression|dimension|column)\s*:\s+)"
    return re.compile(rf"{quoted}|{yaml_slot}['\"]?{plain}['\"]?\s*(?:#.*)?$", re.M)


def _value_lines(text: str, values: list[str], *, start: int) -> list[int]:
    """The 1-based line of each value, matched in document order."""
    lines: list[int] = []
    cursor = 0
    for value in values:
        pattern = _scalar_pattern(value)
        match = pattern.search(text, cursor) or pattern.search(text)
        if match:
            cursor = match.end()
        lines.append(start + text.count("\n", 0, match.start() if match else cursor))
    return lines


def _extract_block(text: str, *, lang: str, path: str, start: int) -> Extraction:
    try:
        values = _json_values(text) if lang in _JSON_LANGS else _yaml_values(text)
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        return Extraction(malformed=[Malformed(path=path, line=start, error=str(exc).splitlines()[0])])
    texts = mode_b_values(values)
    return Extraction(snippets=[
        Snippet(path=path, line=line, text=s)
        for s, line in zip(texts, _value_lines(text, texts, start=start), strict=True)
    ])


def extract_markdown(text: str, *, path: str) -> Extraction:
    """Mode-B strings from the ``json`` / ``yaml`` fences of a Markdown page; lines are 1-based."""
    lines = text.splitlines()
    result = Extraction()
    i = 0
    while i < len(lines):
        match = _FENCE.match(lines[i])
        if not match:
            i += 1
            continue
        indent, fence, lang = match["indent"], match["fence"], match["lang"].lower()
        end = i + 1
        while end < len(lines) and not lines[end].strip().startswith(fence):
            end += 1
        body = "\n".join(line[len(indent):] if line.startswith(indent) else line for line in lines[i + 1:end])
        previous = next((lines[j].strip() for j in range(i - 1, -1, -1) if lines[j].strip()), "")
        if (lang in _JSON_LANGS or lang in _YAML_LANGS) and previous != SKIP_MARKER:
            block = _extract_block(body, lang=lang, path=path, start=i + 2)
            result.snippets.extend(block.snippets)
            result.malformed.extend(block.malformed)
        i = end + 1
    return result


def extract_yaml_file(text: str, *, path: str) -> Extraction:
    """Mode-B strings from a whole YAML file (a stored model, query or config)."""
    return _extract_block(text, lang="yaml", path=path, start=1)
