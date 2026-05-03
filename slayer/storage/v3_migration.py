"""v2 → v3 schema migration for SlayerQuery.

The v3 schema removes ``dry_run`` and ``explain`` from SlayerQuery — they were
execution-mode flags that had no business being persisted. Callers now pass
them as keyword arguments to ``engine.execute(...)``.

This converter strips both fields from any v2-shaped dict it sees, emits a
``logger.warning`` identifying the query (so users notice on first load after
upgrade), and a ``DeprecationWarning`` so callers passing v2-shaped dicts in
code see it in their test runs.

No manual recursion is needed for nested ``source_queries``: each nested
``SlayerQuery`` triggers its own ``@model_validator(mode="before")`` when
Pydantic deserializes the parent ``SlayerModel``, which calls this migration
again per nested query.
"""

import logging
import warnings

from slayer.storage.migrations import migrate, register_migration

logger = logging.getLogger(__name__)


@register_migration("SlayerQuery", 2)
def _query_v2_to_v3(data: dict) -> dict:
    """Drop ``dry_run`` and ``explain`` from v2 SlayerQuery dicts."""
    dropped = [k for k in ("dry_run", "explain") if k in data]
    if not dropped:
        return data
    for k in dropped:
        data.pop(k, None)
    identifier = data.get("name") or data.get("source_model") or "<anonymous>"
    msg = (
        f"SlayerQuery '{identifier}': dropped legacy field(s) "
        f"{{{', '.join(dropped)}}} during v2→v3 migration; pass these as "
        f"engine.execute(..., dry_run=..., explain=...) kwargs instead."
    )
    logger.warning(msg)
    warnings.warn(msg, DeprecationWarning, stacklevel=2)
    return data


@register_migration("SlayerModel", 2)
def _model_v2_to_v3(data: dict) -> dict:
    """v3 SlayerModel: identical at the model level, but ``source_queries``
    entries (which are raw SlayerQuery dicts, not validated model fields) are
    walked through the SlayerQuery migration chain so any nested ``dry_run``
    or ``explain`` flags get stripped.
    """
    raw = data.get("source_queries")
    if isinstance(raw, list):
        data["source_queries"] = [
            migrate("SlayerQuery", q) if isinstance(q, dict) else q
            for q in raw
        ]
    return data
