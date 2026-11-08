"""SlayerModel v16 / SlayerQuery v7 / Memory v5: a stored time dimension's ``{"name", "model"}`` column object becomes its dotted text.

Stored-only: a fresh payload is never rewritten.
"""

from typing import Any

from slayer.storage.migrations import register_migration
from slayer.storage.rank_direction_migration import rewrite_model_texts, rewrite_query_texts, stamp_memory_query


def _dotted(value: Any) -> Any:
    if isinstance(value, dict) and isinstance(name := value.get("name"), str):
        return f"{value['model']}.{name}" if value.get("model") else name
    return value


def _text_time_dimension(item: Any) -> Any:
    if not isinstance(item, dict):
        return item
    return {key: _dotted(value) if key in ("dimension", "column") else value for key, value in item.items()}


def rewrite_query_time_dimensions(data: dict) -> dict:
    """Rewrite each object-form time dimension to text; mark an inline model as stored."""
    if isinstance(data.get("time_dimensions"), list):
        data["time_dimensions"] = [_text_time_dimension(td) for td in data["time_dimensions"]]
    return rewrite_query_texts(data, lambda text: text)


def mark_model_queries(data: dict) -> dict:
    """Mark nested stored queries as stored."""
    return rewrite_model_texts(data, lambda text: text)


register_migration(entity="SlayerModel", source_version=16, stored_only=True)(mark_model_queries)
register_migration(entity="SlayerQuery", source_version=7, stored_only=True)(rewrite_query_time_dimensions)
register_migration(entity="Memory", source_version=5, stored_only=True)(stamp_memory_query)
