"""SlayerModel v13 / SlayerQuery v5 / Memory v3 (re-applied at v14 / v6 / v4): a stored bare ``rank(`` / ``dense_rank(`` keeps its descending order.

Stored-only steps: a fresh payload's bare call stays bare and fails with the missing-direction error.
"""

import io
import tokenize
from collections.abc import Callable
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


TextRewrite = Callable[[str], str]
#: A rewrite told the stored field it rewrites (``measures[2].formula``).
FieldRewrite = Callable[[str, str], str]


def _rewrite_key(item: Any, key: str, fn: FieldRewrite, where: str) -> Any:
    if isinstance(item, dict) and isinstance(item.get(key), str):
        return {**item, key: fn(item[key], f"{where}.{key}")}
    return item


def _rewrite_items(value: Any, rewrite, where: str) -> Any:
    if isinstance(value, list):
        return [rewrite(v, f"{where}[{i}]") for i, v in enumerate(value)]
    return rewrite(value, where)


def _rewrite_text(value: Any, fn: FieldRewrite, where: str) -> Any:
    return fn(value, where) if isinstance(value, str) else value


def _rewrite_measure(item: Any, fn: FieldRewrite, where: str) -> Any:
    return fn(item, where) if isinstance(item, str) else _rewrite_key(item, "formula", fn, where)


def _rewrite_dimension(item: Any, fn: FieldRewrite, where: str) -> Any:
    return fn(item, where) if isinstance(item, str) else _rewrite_key(item, "expression", fn, where)


def _rewrite_time_dimension(item: Any, fn: FieldRewrite, where: str) -> Any:
    if isinstance(item, str):
        return fn(item, where)
    return _rewrite_key(_rewrite_key(item, "dimension", fn, where), "column", fn, where)


def _rewrite_order(item: Any, fn: FieldRewrite, where: str) -> Any:
    if not isinstance(item, dict):
        return _rewrite_text(item, fn, where)
    if "column" in item or "direction" in item:
        # An expression sort key persists as a placeholder column plus its ``raw_formula``.
        return _rewrite_key(_rewrite_key(item, "column", fn, where), "raw_formula", fn, where)
    return {_rewrite_text(k, fn, where): v for k, v in item.items()}


def _text_only(fn: TextRewrite) -> FieldRewrite:
    return lambda text, _where: fn(text)


def _measure_formulas(measures: list, fn: FieldRewrite, where: str) -> list:
    return [_rewrite_key(m, "formula", fn, f"{where}[{i}]") for i, m in enumerate(measures)]


def rewrite_model_texts_at(data: dict, fn: FieldRewrite) -> dict:
    """Apply ``fn`` to every measure formula with its field; mark nested stored queries as stored."""
    if isinstance(data.get("measures"), list):
        data["measures"] = _measure_formulas(data["measures"], fn, "measures")
    if isinstance(data.get("source_queries"), list):
        data["source_queries"] = [stamp_stored(q) for q in data["source_queries"]]
    return data


def rewrite_model_texts(data: dict, fn: TextRewrite) -> dict:
    """Apply ``fn`` to every measure formula; mark nested stored queries as stored."""
    return rewrite_model_texts_at(data, _text_only(fn))


def _rewrite_query_fields(data: dict, fn: FieldRewrite) -> dict:
    for field, rewrite in (
        ("measures", _rewrite_measure), ("filters", _rewrite_text), ("dimensions", _rewrite_dimension),
        ("time_dimensions", _rewrite_time_dimension), ("order", _rewrite_order),
        ("main_time_dimension", _rewrite_text),
    ):
        if data.get(field) is not None:
            data[field] = _rewrite_items(data[field], lambda item, where, rewrite=rewrite: rewrite(item, fn, where), field)
    return data


def rewrite_query_fields(data: dict, fn: TextRewrite) -> dict:
    """Apply ``fn`` to every Mode-B field of the query itself."""
    return _rewrite_query_fields(data, _text_only(fn))


def rewrite_query_texts_at(data: dict, fn: FieldRewrite) -> dict:
    """Apply ``fn`` to every Mode-B field with its field path; mark an inline model as stored."""
    data = _rewrite_query_fields(data, fn)
    source = data.get("source_model")
    if isinstance(source, dict):
        if "source_name" in source:
            if isinstance(source.get("measures"), list):
                data["source_model"] = {**source, "measures": _measure_formulas(source["measures"], fn,
                                                                                "source_model.measures")}
        else:
            data["source_model"] = stamp_stored(source)
    return data


def rewrite_query_texts(data: dict, fn: TextRewrite) -> dict:
    """Apply ``fn`` to every Mode-B field; mark an inline model as stored."""
    return rewrite_query_texts_at(data, _text_only(fn))


def rewrite_model_ranks(data: dict) -> dict:
    """Fill ``direction='desc'`` into measure formulas; mark nested stored queries as stored."""
    return rewrite_model_texts(data, add_desc_direction)


def rewrite_query_ranks(data: dict) -> dict:
    """Fill ``direction='desc'`` into every Mode-B field; mark an inline model as stored."""
    return rewrite_query_texts(data, add_desc_direction)


def stamp_memory_query(data: dict) -> dict:
    """Mark the bundled query as stored."""
    if isinstance(data.get("query"), dict):
        data["query"] = stamp_stored(data["query"])
    return data


register_migration(entity="SlayerModel", source_version=12, stored_only=True)(rewrite_model_ranks)
register_migration(entity="SlayerQuery", source_version=4, stored_only=True)(rewrite_query_ranks)
register_migration(entity="Memory", source_version=2, stored_only=True)(stamp_memory_query)
