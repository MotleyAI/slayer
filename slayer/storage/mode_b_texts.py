"""One recursive walk over a stored document's Mode-B texts: model measures, query fields, nested stages, inline
models and extensions, and a memory's query. Mode-A text (``Column.sql``, ``sql_table``, model ``filters``,
aggregation formulas) is never visited."""

import contextlib
from collections.abc import Callable
from typing import Any

from slayer.core.legacy_spellings import LegacySpan, rewrite_spans
from slayer.core.syntax import call_spans, legacy_spans, rewrite_colon_spans
from slayer.storage.rank_direction_migration import rewrite_query_fields

TextRewrite = Callable[[str], str]


def rewrite_model_doc(data: dict, fn: TextRewrite) -> dict:
    out = dict(data)
    if isinstance(out.get("measures"), list):
        out["measures"] = [_formula(m, fn) for m in out["measures"]]
    if isinstance(out.get("source_queries"), list):
        out["source_queries"] = [rewrite_query_doc(q, fn) if isinstance(q, dict) else q for q in out["source_queries"]]
    return out


def rewrite_query_doc(data: dict, fn: TextRewrite) -> dict:
    out = rewrite_query_fields(dict(data), fn)
    source = out.get("source_model")
    if isinstance(source, dict):
        out["source_model"] = rewrite_model_doc(source, fn)
    return out


def rewrite_memory_doc(data: dict, fn: TextRewrite) -> dict:
    if not isinstance(data.get("query"), dict):
        return data
    return {**data, "query": rewrite_query_doc(data["query"], fn)}


def _formula(measure: Any, fn: TextRewrite) -> Any:
    if isinstance(measure, dict) and isinstance(measure.get("formula"), str):
        return {**measure, "formula": fn(measure["formula"])}
    return measure


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

