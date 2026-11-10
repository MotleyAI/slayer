"""SlayerModel v19 / SlayerQuery v10 / Memory v8: a stored bucket call ``<unit>(x)`` becomes ``date_trunc('<unit>', x)``,
each deprecated spelling its canonical twin, and an aggregation named ``agg`` (now reserved) ``agg_aggregation`` with
every call to it.

Stored-only. A custom-granularity call needs its datasource's names, so the storage load path rewrites it first.
"""

import logging

from slayer.core.enums import GRANULARITY_NAMES
from slayer.storage.migrations import document_label, register_migration
from slayer.storage.mode_b_texts import bucket_calls_as_date_trunc, canonical_spellings, renamed_calls
from slayer.storage.rank_direction_migration import rewrite_model_texts, rewrite_query_texts, stamp_memory_query

_RESERVED = "agg"
_RENAMED = "agg_aggregation"
_logger = logging.getLogger(__name__)


def upgraded_text(text: str) -> str:
    """One stored Mode-B text in the 2.0 spelling."""
    text = bucket_calls_as_date_trunc(text, units=GRANULARITY_NAMES)
    return renamed_calls(canonical_spellings(text), old=_RESERVED, new=_RENAMED)


def upgrade_model(data: dict) -> dict:
    """Rename a stored ``agg`` aggregation and upgrade the measure formulas; mark nested stored queries as stored."""
    aggregations = data.get("aggregations")
    if isinstance(aggregations, list):
        data["aggregations"] = _renamed_reserved(aggregations, model=document_label(data))
    return rewrite_model_texts(data, upgraded_text)


def _renamed_reserved(aggregations: list, *, model: str | None) -> list:
    names = [n for a in aggregations if isinstance(a, dict) and isinstance(n := a.get("name"), str)]
    reserved = [n for n in names if n.casefold() == _RESERVED]
    if not reserved:
        return aggregations
    if any(n.casefold() == _RENAMED for n in names):
        _logger.warning(
            "model %r: aggregation %r is the reserved function name AGG and %r is taken; it is kept, and calls to "
            "agg(…) are refused", model, reserved[0], _RENAMED,
        )
        return aggregations
    _logger.warning(
        "model %r: aggregation %r is the reserved function name AGG; renamed to %r with every call to it",
        model, reserved[0], _RENAMED,
    )
    return [
        {**a, "name": _RENAMED} if isinstance(a, dict) and a.get("name") == reserved[0] else a for a in aggregations
    ]


def upgrade_query(data: dict) -> dict:
    """Upgrade every Mode-B field; mark an inline model as stored."""
    return rewrite_query_texts(data, upgraded_text)


register_migration(entity="SlayerModel", source_version=18, stored_only=True)(upgrade_model)
register_migration(entity="SlayerQuery", source_version=9, stored_only=True)(upgrade_query)
register_migration(entity="Memory", source_version=7, stored_only=True)(stamp_memory_query)
