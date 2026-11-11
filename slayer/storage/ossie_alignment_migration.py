"""SlayerModel v20 / SlayerQuery v11 / Memory v9: a stored bucket call ``<unit>(x)`` becomes ``date_trunc('<unit>', x)``,
each deprecated spelling its canonical twin, and an aggregation named ``agg`` (now reserved) ``agg_aggregation`` with
every call to it.

Stored-only. A custom-granularity call needs its datasource's names, so the storage load path rewrites it first.
"""

import logging
from functools import partial

from slayer.core.enums import GRANULARITY_NAMES
from slayer.storage.migrations import document_label, register_migration
from slayer.storage.mode_b_texts import bucket_calls_as_date_trunc, canonical_spellings, renamed_calls
from slayer.storage.rank_direction_migration import rewrite_model_texts, rewrite_query_texts, stamp_memory_query

_RESERVED = "agg"
_RENAMED = "agg_aggregation"
_logger = logging.getLogger(__name__)


def upgraded_text(text: str, *, rename_agg: bool = True) -> str:
    """One stored Mode-B text in the 2.0 spelling; ``rename_agg`` calls ``agg(…)`` as ``agg_aggregation(…)``."""
    text = canonical_spellings(bucket_calls_as_date_trunc(text, units=GRANULARITY_NAMES))
    return renamed_calls(text, old=_RESERVED, new=_RENAMED) if rename_agg else text


def upgrade_model(data: dict) -> dict:
    """Rename a stored ``agg`` aggregation and upgrade the measure formulas; mark nested stored queries as stored."""
    aggregations = data.get("aggregations")
    rename_agg = True
    if isinstance(aggregations, list):
        data["aggregations"], rename_agg = _renamed_reserved(aggregations, model=document_label(data))
    return rewrite_model_texts(data, partial(upgraded_text, rename_agg=rename_agg))


def _renamed_reserved(aggregations: list, *, model: str | None) -> tuple[list, bool]:
    """The aggregations with a reserved ``agg`` renamed, and whether calls to it follow the rename."""
    names = [n for a in aggregations if isinstance(a, dict) and isinstance(n := a.get("name"), str)]
    reserved = [n for n in names if n.casefold() == _RESERVED]
    if not reserved:
        return aggregations, True
    if any(n.casefold() == _RENAMED for n in names):
        _logger.warning(
            "model %r: aggregation %r is the reserved function name AGG and %r is taken; it is kept, and calls to "
            "agg(…) are refused", model, reserved[0], _RENAMED,
        )
        return aggregations, False
    _logger.warning(
        "model %r: aggregation %r is the reserved function name AGG; renamed to %r with every call to it",
        model, reserved[0], _RENAMED,
    )
    return [
        {**a, "name": _RENAMED} if isinstance(a, dict) and a.get("name") == reserved[0] else a for a in aggregations
    ], True


def upgrade_query(data: dict) -> dict:
    """Upgrade every Mode-B field; mark an inline model as stored."""
    return rewrite_query_texts(data, upgraded_text)


register_migration(entity="SlayerModel", source_version=19, stored_only=True)(upgrade_model)
register_migration(entity="SlayerQuery", source_version=10, stored_only=True)(upgrade_query)
register_migration(entity="Memory", source_version=8, stored_only=True)(stamp_memory_query)
