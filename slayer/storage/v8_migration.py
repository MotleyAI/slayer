"""v7 → v8 schema migration for SlayerModel.

v8 introduces one new optional field on ``SlayerModel``:

- ``source_kind: Optional[Literal["table", "view", "materialized_view"]]`` —
  what kind of database object ``sql_table`` names. Auto-ingestion sets it;
  hand-authored, ``sql``-mode and query-backed models leave it ``None``.

The forward conversion is a no-op because the field defaults to ``None`` on
the Pydantic class. ``None`` is also the *correct* value for a pre-v8 model:
we genuinely do not know what backed it, and guessing ``"table"`` would be a
fabrication that the next re-ingest would silently have to correct. The first
subsequent ingest classifies it for real.
"""

from __future__ import annotations

from slayer.storage.migrations import register_migration


@register_migration(entity="SlayerModel", source_version=7)
def _model_v7_to_v8(data: dict) -> dict:
    """No-op forward. ``source_kind`` defaults to ``None`` on validation."""
    return data
