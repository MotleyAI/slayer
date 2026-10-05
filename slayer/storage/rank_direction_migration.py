"""SlayerModel v13 / SlayerQuery v5 / Memory v3: a stored bare ``rank(`` / ``dense_rank(`` keeps its descending order.

Stored-only steps: a fresh payload's bare call stays bare and fails with the missing-direction error.
"""

import io
import tokenize
from typing import Any

from slayer.storage.migrations import register_migration, stamp_stored

_DIRECTED = frozenset({"rank", "dense_rank"})
_OPEN = frozenset({"(", "[", "{"})
_CLOSE = frozenset({")", "]", "}"})
_SKIP = frozenset({tokenize.NL, tokenize.NEWLINE, tokenize.COMMENT, tokenize.INDENT, tokenize.DEDENT})
_DESC = "direction='desc'"


def add_desc_direction(text: str) -> str:
    """Give every ``rank(`` / ``dense_rank(`` call lacking a top-level ``direction=`` a ``direction='desc'``; untokenisable text is returned unchanged."""
    try:
        tokens = [t for t in tokenize.generate_tokens(io.StringIO(text).readline) if t.type not in _SKIP]
    except (tokenize.TokenError, SyntaxError):
        return text
    if any(t.type == tokenize.ERRORTOKEN for t in tokens):
        return text
    line_starts = [0]
    for line in text.splitlines(keepends=True):
        line_starts.append(line_starts[-1] + len(line))
    inserts: list[tuple[int, str]] = []
    for i in range(len(tokens)):
        if not _is_directed_call(tokens=tokens, i=i):
            continue
        close, has_direction = _scan_call(tokens=tokens, open_index=i + 1)
        if close is None or has_direction or close == i + 2:
            continue
        before = tokens[close - 1]
        piece = f" {_DESC}" if before.string == "," else f", {_DESC}"
        row, col = tokens[close].start
        inserts.append((line_starts[row - 1] + col, piece))
    for offset, piece in sorted(inserts, reverse=True):
        text = text[:offset] + piece + text[offset:]
    return text


def _is_directed_call(*, tokens: list[tokenize.TokenInfo], i: int) -> bool:
    tok = tokens[i]
    return (
        tok.type == tokenize.NAME and tok.string in _DIRECTED
        and i + 1 < len(tokens) and tokens[i + 1].string == "("
        and not (i > 0 and tokens[i - 1].string == ".")
    )


def _scan_call(*, tokens: list[tokenize.TokenInfo], open_index: int) -> tuple[int | None, bool]:
    """Index of the ``)`` matching ``tokens[open_index]``, and whether a top-level ``direction=`` sits inside."""
    depth = 0
    has_direction = False
    for j in range(open_index, len(tokens)):
        s = tokens[j].string
        if tokens[j].type == tokenize.OP and s in _OPEN:
            depth += 1
        elif tokens[j].type == tokenize.OP and s in _CLOSE:
            depth -= 1
            if depth == 0:
                return j, has_direction
        elif depth == 1 and s == "direction" and j + 1 < len(tokens) and tokens[j + 1].string == "=":
            has_direction = True
    return None, has_direction


def _rewrite_str(value: Any) -> Any:
    return add_desc_direction(value) if isinstance(value, str) else value


def _rewrite_key(item: Any, key: str) -> Any:
    if isinstance(item, dict) and key in item:
        return {**item, key: _rewrite_str(item[key])}
    return item


def _rewrite_items(value: Any, rewrite) -> Any:
    if isinstance(value, list):
        return [rewrite(v) for v in value]
    return rewrite(value)


def _rewrite_measure(item: Any) -> Any:
    return _rewrite_str(item) if isinstance(item, str) else _rewrite_key(item, "formula")


def _rewrite_dimension(item: Any) -> Any:
    return _rewrite_str(item) if isinstance(item, str) else _rewrite_key(item, "expression")


def _rewrite_time_dimension(item: Any) -> Any:
    if isinstance(item, str):
        return _rewrite_str(item)
    return _rewrite_key(_rewrite_key(item, "dimension"), "column")


def _rewrite_order(item: Any) -> Any:
    if not isinstance(item, dict):
        return _rewrite_str(item)
    if "column" in item or "direction" in item:
        return _rewrite_key(item, "column")
    return {_rewrite_str(k): v for k, v in item.items()}


@register_migration(entity="SlayerModel", source_version=12, stored_only=True)
def _model_v12_to_v13(data: dict) -> dict:
    """Fill ``direction='desc'`` into measure formulas; mark nested stored queries as stored."""
    if isinstance(data.get("measures"), list):
        data["measures"] = [_rewrite_key(m, "formula") for m in data["measures"]]
    if isinstance(data.get("source_queries"), list):
        data["source_queries"] = [stamp_stored(q) for q in data["source_queries"]]
    return data


@register_migration(entity="SlayerQuery", source_version=4, stored_only=True)
def _query_v4_to_v5(data: dict) -> dict:
    """Fill ``direction='desc'`` into every Mode-B field; mark an inline model as stored."""
    for field, rewrite in (
        ("measures", _rewrite_measure), ("filters", _rewrite_str), ("dimensions", _rewrite_dimension),
        ("time_dimensions", _rewrite_time_dimension), ("order", _rewrite_order),
        ("main_time_dimension", _rewrite_str),
    ):
        if data.get(field) is not None:
            data[field] = _rewrite_items(data[field], rewrite)
    source = data.get("source_model")
    if isinstance(source, dict):
        if "source_name" in source:
            if isinstance(source.get("measures"), list):
                data["source_model"] = {**source, "measures": [_rewrite_key(m, "formula") for m in source["measures"]]}
        else:
            data["source_model"] = stamp_stored(source)
    return data


@register_migration(entity="Memory", source_version=2, stored_only=True)
def _memory_v2_to_v3(data: dict) -> dict:
    """Mark the bundled query as stored."""
    if isinstance(data.get("query"), dict):
        data["query"] = stamp_stored(data["query"])
    return data
