"""SlayerModel v29: ``AGG`` is refused in a field's Mode-B expression, so a stored one reads its argument
(``AGG(x)`` → ``x``), as it ran before, with a warning. Mode-A text is left as written."""

import logging
from typing import Any

from slayer.core.syntax import unwrap_agg_calls
from slayer.storage.migrations import register_migration

_logger = logging.getLogger(__name__)


def _unwrapped(field: Any, *, dataset: Any) -> Any:
    """``field`` with ``AGG`` unwrapped in its Mode-B expression (``expression`` text, or an object's ``sql``)."""
    if not isinstance(field, dict):
        return field
    expression = field.get("expression")
    text = expression.get("sql") if isinstance(expression, dict) else expression
    new = unwrap_agg_calls(text) if isinstance(text, str) else text
    if new == text:
        return field
    _logger.warning("Dataset %r, field %r: AGG is allowed only in metrics; its expression %r now reads %r.",
                    dataset, field.get("name"), text, new)
    return {**field, "expression": {**expression, "sql": new} if isinstance(expression, dict) else new}


def upgrade_dataset(data: dict) -> dict:
    fields = data.get("fields")
    if isinstance(fields, list):
        data["fields"] = [_unwrapped(f, dataset=data.get("name")) for f in fields]
    return data


register_migration(entity="SlayerModel", source_version=28, stored_only=True)(upgrade_dataset)
