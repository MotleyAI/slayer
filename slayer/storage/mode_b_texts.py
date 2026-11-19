"""One recursive walk over a stored document's Mode-B texts, in the keys of its version: dataset metrics, query
fields, nested stages, inline datasets and extensions, and a memory's query. Mode-A text (``DatasetField.sql``,
``sql_table``, dataset ``filters``, aggregation expressions) is never visited."""

import contextlib
from collections.abc import Callable
from typing import Any

from slayer.core.legacy_spellings import LegacySpan, rewrite_spans
from slayer.core.syntax import call_spans, legacy_spans, rewrite_colon_spans
from slayer.storage.document_keys import DocumentKeys, query_keys
from slayer.storage.rank_direction_migration import rewrite_query_fields

TextRewrite = Callable[[str], str]


def rewrite_dataset_doc(data: dict, fn: TextRewrite, *, keys: DocumentKeys) -> dict:
    out = dict(data)
    if isinstance(out.get(keys.metrics), list):
        out[keys.metrics] = [_expression(m, fn, keys=keys) for m in out[keys.metrics]]
    if isinstance(out.get("source_queries"), list):
        out["source_queries"] = [
            rewrite_query_doc(q, fn, keys=keys) if isinstance(q, dict) else q for q in out["source_queries"]
        ]
    return out


def rewrite_query_doc(data: dict, fn: TextRewrite, *, keys: DocumentKeys) -> dict:
    keys = query_keys(data, enclosing=keys)
    out = rewrite_query_fields(dict(data), fn, keys)
    source = out.get(keys.population)
    if isinstance(source, dict):
        out[keys.population] = rewrite_dataset_doc(source, fn, keys=keys)
    return out


def rewrite_memory_doc(data: dict, fn: TextRewrite, *, keys: DocumentKeys) -> dict:
    if not isinstance(data.get("query"), dict):
        return data
    return {**data, "query": rewrite_query_doc(data["query"], fn, keys=keys)}


def _expression(metric: Any, fn: TextRewrite, *, keys: DocumentKeys) -> Any:
    if isinstance(metric, dict) and isinstance(metric.get(keys.expression), str):
        return {**metric, keys.expression: fn(metric[keys.expression])}
    return metric


def canonical_spellings(text: str, *, found: dict[str, LegacySpan] | None = None) -> str:
    """``text`` with each deprecated spelling replaced by its twin; ``found`` records the first span per spelling."""
    spans = legacy_spans(text)
    if found is not None:
        for span in spans:
            found.setdefault(span.kind, span)
    return rewrite_spans(text, spans) if spans else text


def bucket_calls_as_date_trunc(text: str, *, units: frozenset[str]) -> str:
    """``text`` with each one-argument call ``u(x)`` whose casefolded callee is in ``units`` spelled
    ``date_trunc('u', x)``."""
    out: list[str] = []
    cursor = 0
    for call in call_spans(text):
        unit = call.callee.casefold()
        if unit not in units or call.arguments != 1:
            continue
        paren = text.index("(", call.start + len(call.callee))
        out.append(text[cursor:call.start] + f"date_trunc('{unit}', ")
        cursor = paren + 1
    return "".join(out) + text[cursor:]


def custom_buckets_as_date_trunc(text: str, *, units: frozenset[str]) -> str:
    """``text`` with each bucket of a granularity in ``units`` — a call ``u(x)`` or a stored colon ``x:u`` — spelled
    ``date_trunc('u', x)``."""
    if not units:
        return text
    with contextlib.suppress(ValueError):
        text = rewrite_colon_spans(text, units=units)
    return bucket_calls_as_date_trunc(text, units=units)


def renamed_calls(text: str, *, old: str, new: str) -> str:
    """``text`` with each call whose casefolded callee is ``old`` calling ``new``."""
    out: list[str] = []
    cursor = 0
    for call in call_spans(text):
        if call.callee.casefold() == old:
            out.append(text[cursor:call.start] + new)
            cursor = call.start + len(call.callee)
    return "".join(out) + text[cursor:]

