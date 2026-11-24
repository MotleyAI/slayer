"""SlayerModel v29: ``AGG`` is refused in a field's Mode-B expression, so a stored one reads its argument
(``AGG(x)`` → ``x``), as it ran before, with a warning. Mode-A text is left as written."""

import logging
from typing import Any

from slayer.core.field_expression import OSSIE_SQL, token
from slayer.core.syntax import unwrap_agg_calls
from slayer.storage.migrations import register_migration

_logger = logging.getLogger(__name__)


def _portable_entry(entry: Any) -> bool:
    return isinstance(entry, dict) and isinstance(entry.get("dialect"), str) and token(entry["dialect"]) == OSSIE_SQL


def _unwrapped_text(text: Any, *, field: dict, dataset: Any) -> Any:
    new = unwrap_agg_calls(text) if isinstance(text, str) else text
    if new != text:
        _logger.warning("Dataset %r, field %r: AGG is allowed only in metrics; its expression %r now reads %r.",
                        dataset, field.get("name"), text, new)
    return new


def _unwrapped_expression(expression: Any, *, field: dict, dataset: Any) -> Any:
    """``expression`` with ``AGG`` unwrapped in its Mode-B text: the shorthand, an object's ``sql``, else its
    ``OSSIE_SQL_2026`` entries."""
    if not isinstance(expression, dict):
        return _unwrapped_text(expression, field=field, dataset=dataset)
    if expression.get("sql") is not None:
        return {**expression, "sql": _unwrapped_text(expression["sql"], field=field, dataset=dataset)}
    entries = expression.get("dialects")
    if not isinstance(entries, list):
        return expression
    return {**expression, "dialects": [
        {**e, "expression": _unwrapped_text(e.get("expression"), field=field, dataset=dataset)}
        if _portable_entry(e) else e for e in entries
    ]}


def _unwrapped(field: Any, *, dataset: Any) -> Any:
    if not isinstance(field, dict) or "expression" not in field:
        return field
    expression = field["expression"]
    new = _unwrapped_expression(expression, field=field, dataset=dataset)
    return field if new == expression else {**field, "expression": new}


def upgrade_dataset(data: dict) -> dict:
    fields = data.get("fields")
    if isinstance(fields, list):
        data["fields"] = [_unwrapped(f, dataset=data.get("name")) for f in fields]
    return data


register_migration(entity="SlayerModel", source_version=28, stored_only=True)(upgrade_dataset)
