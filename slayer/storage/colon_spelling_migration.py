"""SlayerModel v18 / SlayerQuery v9 / Memory v7: stored colon spellings (``x:agg``, ``col:gran``) read as their
functional twins.

Stored-only: a fresh payload's colon spelling is refused by the parser.
"""

import logging
from collections.abc import Callable
from typing import Any

from slayer.core.syntax import rewrite_colon_spans
from slayer.storage.migrations import document_label, register_migration
from slayer.storage.rank_direction_migration import (
    rewrite_model_texts_at,
    rewrite_query_texts_at,
    stamp_memory_query,
)

logger = logging.getLogger(__name__)

FieldRewrite = Callable[[str, str], str]


def _rewriter(document: str | None) -> FieldRewrite:
    def rewrite(text: str, field: str) -> str:
        try:
            return rewrite_colon_spans(text)
        except ValueError:
            # The text itself is not logged: it may hold sensitive literals.
            logger.warning("stored document %r, field %s: does not parse; left unchanged", document, field)
            return text
    return rewrite


def _order_item(item: Any, fn: FieldRewrite, where: str) -> Any:
    """``item`` rewritten; an expression key that changes is stored as the fresh functional key would be."""
    if isinstance(item, str):
        return fn(item, where)
    if not isinstance(item, dict):
        return item
    if "column" not in item and "direction" not in item:
        return {fn(k, where) if isinstance(k, str) else k: v for k, v in item.items()}
    column = item.get("column")
    raw = item.get("raw_formula")
    if not isinstance(raw, str) and isinstance(column, dict) and set(column) == {"name"}:
        raw = column["name"]
    if not isinstance(raw, str):
        return {**item, "column": fn(column, f"{where}.column")} if isinstance(column, str) else item
    rewritten = fn(raw, f"{where}.column")
    if rewritten == raw:
        return item
    return {**{k: v for k, v in item.items() if k not in ("column", "raw_formula")}, "column": rewritten}


def _rewrite_order(order: Any, fn: FieldRewrite) -> Any:
    if isinstance(order, list):
        return [_order_item(item, fn, f"order[{i}]") for i, item in enumerate(order)]
    return _order_item(order, fn, "order")


def upgrade_model_colons(data: dict) -> dict:
    return rewrite_model_texts_at(data, _rewriter(document_label(data)))


def upgrade_query_colons(data: dict) -> dict:
    fn = _rewriter(document_label(data))
    if "order" not in data:
        return rewrite_query_texts_at(data, fn)
    order = data.pop("order")
    data = rewrite_query_texts_at(data, fn)
    data["order"] = _rewrite_order(order, fn)
    return data


register_migration(entity="SlayerModel", source_version=18, stored_only=True)(upgrade_model_colons)
register_migration(entity="SlayerQuery", source_version=9, stored_only=True)(upgrade_query_colons)
register_migration(entity="Memory", source_version=7, stored_only=True)(stamp_memory_query)
