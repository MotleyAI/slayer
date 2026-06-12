"""v6 → v7 schema migration for SlayerModel (DEV-1480).

v7 introduces two new optional fields on ``Column``:

- ``sampled_values: Optional[List[str]]`` — the structured top-N sample-value
  list paired with the existing ``sampled`` text. Carries up to 50 values
  ordered by descending frequency (alphabetical tie-break) when the
  categorical column is profiled. Stays ``None`` for overflow > 50 and for
  numeric/temporal columns.
- ``distinct_count: Optional[int]`` — the column's true cardinality at
  profile time (computed via a secondary ``count_distinct`` query when
  overflow is detected). Stays ``None`` for numeric/temporal columns.

The forward conversion is a no-op because both new fields default to
``None`` on the Pydantic class; first subsequent ingest /
``refresh-samples`` populates the cache.
"""

from __future__ import annotations

from slayer.storage.migrations import register_migration


@register_migration(entity="SlayerModel", source_version=6)
def _model_v6_to_v7(data: dict) -> dict:
    """No-op forward. The new fields default to ``None`` on validation."""
    return data
