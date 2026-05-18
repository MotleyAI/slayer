"""Sample-value profiling for ``Column.sampled`` (DEV-1375).

The internals were extracted from ``slayer/mcp/server.py``'s
``_collect_dim_profile`` so both ``inspect_model`` and the search-index
refresh hooks can call them without circular imports.

Public surface:

* :func:`profile_column` — produce the formatted ``sampled`` string for
  a single column.
* :func:`refresh_table_backed_model_sampled` — walk every non-hidden
  column on a table-backed model, profile, persist via storage. Best-
  effort: per-column failures are accumulated and returned as strings.
* :func:`refresh_all_table_backed_sampled` — same as above for every
  table-backed model in a single datasource.
* :func:`handle_edit_refresh` — invalidation entry point used by
  ``edit_model``: refresh just the changed columns, or all columns when
  the model-level filters / sql / source body changed.

sql-mode and query-backed models are silently skipped in v1; broader
coverage is tracked in DEV-1377.
"""

from __future__ import annotations

from typing import Any, Dict, List, NamedTuple, Optional, Set

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend


# ---------------------------------------------------------------------------
# Profile entry data structure (was internal to mcp/server.py)
# ---------------------------------------------------------------------------


class _DimProfileEntry(NamedTuple):
    """One row of dimension-profile output.

    Exactly one of two population modes is used:
    - Categorical (string/boolean): ``distinct_count`` and ``values`` are set.
      When cardinality exceeds the cap, both are ``None`` to signal overflow.
    - Numeric/temporal: ``min_value`` and ``max_value`` are set.
    """

    name: str
    type_str: str
    distinct_count: Optional[int]
    values: Optional[List[Any]]
    min_value: Optional[Any]
    max_value: Optional[Any]


def _format_dim_profile_value(entry: _DimProfileEntry) -> str:
    """Render a profile entry as a single-cell string.

    Plain text — no backticks; backticking happens at render time in
    ``inspect_model`` if needed (this string lives on disk in
    ``Column.sampled`` for the search index to consume).
    """
    if entry.values is not None:
        return ", ".join(str(v) for v in entry.values)
    if (
        entry.distinct_count is None
        and entry.values is None
        and entry.min_value is None
        and entry.max_value is None
    ):
        return "> 20 distinct"
    return f"{entry.min_value} .. {entry.max_value}"


async def _profile_categorical_column(
    *,
    model: SlayerModel,
    column: Column,
    engine: SlayerQueryEngine,
    max_values: int,
) -> Optional[_DimProfileEntry]:
    """Profile one string/boolean column via a one-shot distinct-values query.

    Returns ``None`` when the column query fails — caller skips the column.
    """
    try:
        q = SlayerQuery.model_validate({
            "source_model": model.name,
            "dimensions": [{"name": column.name}],
            "measures": [{"formula": "*:count"}],
            "limit": max_values + 1,
        })
        r = await engine.execute(query=q, data_source=model.data_source or None)
    except Exception:
        return None
    value_key = f"{model.name}.{column.name}"
    values = [row.get(value_key) for row in r.data]
    overflow = len(values) > max_values
    return _DimProfileEntry(
        name=column.name,
        type_str=str(column.type),
        distinct_count=None if overflow else len(values),
        values=None if overflow else values,
        min_value=None,
        max_value=None,
    )


async def _profile_numeric_temporal_columns(
    *,
    model: SlayerModel,
    columns: List[Column],
    engine: SlayerQueryEngine,
) -> Dict[str, _DimProfileEntry]:
    """Profile every numeric/temporal column in a single batched min/max query."""
    if not columns:
        return {}
    ext_columns = [
        {"name": f"_slayer_range_{c.name}", "sql": c.sql if c.sql else c.name,
         "type": str(c.type)}
        for c in columns
    ]
    measures_payload: List[Dict[str, str]] = []
    for c in columns:
        measures_payload.append({"formula": f"_slayer_range_{c.name}:min"})
        measures_payload.append({"formula": f"_slayer_range_{c.name}:max"})
    row: Dict[str, Any] = {}
    try:
        q = SlayerQuery.model_validate({
            "source_model": {"source_name": model.name, "columns": ext_columns},
            "measures": measures_payload,
        })
        r = await engine.execute(query=q, data_source=model.data_source or None)
        if r.data:
            row = r.data[0]
    except Exception:
        row = {}
    out: Dict[str, _DimProfileEntry] = {}
    for c in columns:
        mn = row.get(f"{model.name}._slayer_range_{c.name}_min")
        mx = row.get(f"{model.name}._slayer_range_{c.name}_max")
        if mn is None and mx is None:
            continue
        out[c.name] = _DimProfileEntry(
            name=c.name,
            type_str=str(c.type),
            distinct_count=None,
            values=None,
            min_value=mn,
            max_value=mx,
        )
    return out


async def _collect_dim_profile(
    *,
    model: SlayerModel,
    engine: SlayerQueryEngine,
    max_values: int = 20,
    max_dims: int = 10,
    only_columns: Optional[Set[str]] = None,
) -> List[_DimProfileEntry]:
    """Produce one profile entry per eligible column (non-hidden, non-pk).

    - string/boolean columns: distinct values (or overflow marker) via one
      query per column.
    - number/date/time columns: min and max via one batched query across
      all such columns, using a ``ModelExtension`` with transient inline
      measures.

    Caps the total number of eligible columns at ``max_dims``. Individual
    failures are swallowed — the column is simply omitted from the result.
    When ``only_columns`` is supplied, the eligibility filter is intersected
    with the set, so callers can profile a single column cheaply.
    """
    eligible = [
        c for c in model.columns
        if not c.hidden and not c.primary_key
        and (only_columns is None or c.name in only_columns)
    ][:max_dims]
    categorical = [c for c in eligible if c.type in (DataType.TEXT, DataType.BOOLEAN)]
    numeric_temporal = [
        c for c in eligible
        if c.type in (DataType.INT, DataType.DOUBLE, DataType.DATE, DataType.TIMESTAMP)
    ]

    entries: Dict[str, _DimProfileEntry] = {}
    for c in categorical:
        entry = await _profile_categorical_column(
            model=model, column=c, engine=engine, max_values=max_values,
        )
        if entry is not None:
            entries[c.name] = entry
    entries.update(
        await _profile_numeric_temporal_columns(
            model=model, columns=numeric_temporal, engine=engine,
        )
    )
    return [entries[c.name] for c in eligible if c.name in entries]


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def _is_table_backed(model: SlayerModel) -> bool:
    """Only ``sql_table`` mode supports the v1 sample-value refresh path.

    sql-mode and query-backed models are silently skipped (DEV-1377
    follow-up). This mirrors ``ingest_datasource_idempotent``'s carve-out.
    """
    return bool(model.sql_table) and not model.sql and not model.source_queries


async def profile_column(
    *,
    model: SlayerModel,
    column: Column,
    engine: SlayerQueryEngine,
) -> Optional[str]:
    """Return the formatted ``sampled`` string for ``column`` on ``model``.

    Returns ``None`` for primary-key / hidden columns and when the
    profile query fails or yields no data. Caller decides whether to
    persist the ``None`` (clearing any stale value) or skip it.
    """
    if column.hidden or column.primary_key:
        return None
    entries = await _collect_dim_profile(
        model=model, engine=engine, only_columns={column.name},
    )
    if not entries:
        return None
    return _format_dim_profile_value(entries[0])


async def refresh_table_backed_model_sampled(
    *,
    model: SlayerModel,
    engine: SlayerQueryEngine,
    storage: StorageBackend,
    only_columns: Optional[Set[str]] = None,
) -> List[str]:
    """Refresh ``Column.sampled`` for each eligible column on ``model``.

    sql-mode and query-backed models are silently skipped (returns ``[]``).
    Best-effort: a per-column profile or persistence error is captured as
    a string, the loop continues. Returns the list of error strings (empty
    on full success).
    """
    if not _is_table_backed(model):
        return []
    errors: List[str] = []
    for column in model.columns:
        if column.hidden or column.primary_key:
            continue
        if only_columns is not None and column.name not in only_columns:
            continue
        sampled: Optional[str] = None
        try:
            sampled = await profile_column(
                model=model, column=column, engine=engine,
            )
        except Exception as exc:  # NOSONAR(S112) — best-effort: see module docstring
            errors.append(f"{model.name}.{column.name}: {exc}")
            sampled = None
        try:
            await storage.update_column_sampled(
                data_source=model.data_source,
                model_name=model.name,
                column_name=column.name,
                sampled=sampled,
            )
        except Exception as exc:  # NOSONAR(S112) — best-effort: see module docstring
            errors.append(f"{model.name}.{column.name} (persist): {exc}")
    return errors


async def refresh_all_table_backed_sampled(
    *,
    engine: SlayerQueryEngine,
    storage: StorageBackend,
    data_source: str,
) -> List[str]:
    """Refresh ``Column.sampled`` for every table-backed model in
    ``data_source``. Best-effort across all models."""
    errors: List[str] = []
    identities = await storage._list_all_model_identities()
    for ds, name in identities:
        if ds != data_source:
            continue
        model = await storage.get_model(name, data_source=ds)
        if model is None:
            continue
        errors.extend(
            await refresh_table_backed_model_sampled(
                model=model, engine=engine, storage=storage,
            )
        )
    return errors


async def handle_edit_refresh(
    *,
    engine: SlayerQueryEngine,
    storage: StorageBackend,
    data_source: str,
    model_name: str,
    changed_columns: Set[str],
    model_level_change: bool,
) -> List[str]:
    """Refresh entry point for ``edit_model``.

    * ``model_level_change=True`` → refresh every non-hidden column on
      the model (used when ``SlayerModel.filters`` / ``sql`` /
      ``source_queries`` body changed and so every column's sample-value
      could be affected).
    * Otherwise refresh just the columns named in ``changed_columns``.

    DEV-1386: after the sample-value refresh, runs the embedding refresh
    over the model's subtree (model doc + visible columns + named
    measures + custom aggregations). Best-effort: per-entity embed
    failures are appended to the returned warning list, never aborting
    ``edit_model``.
    """
    model = await storage.get_model(model_name, data_source=data_source)
    if model is None:
        return [f"model {model_name!r} not found in datasource {data_source!r}"]
    only = None if model_level_change else changed_columns
    warnings = await refresh_table_backed_model_sampled(
        model=model, engine=engine, storage=storage, only_columns=only,
    )
    # Reload the model — the sample-value refresh just patched it on
    # disk, and the embedding text rendering needs the updated dict to
    # match the new content_hash.
    reloaded = await storage.get_model(model_name, data_source=data_source)
    if reloaded is not None:
        # Local import: keep embeddings off the cold-start path when the
        # extra is not installed.
        from slayer.embeddings.service import EmbeddingService

        try:
            warnings.extend(
                await EmbeddingService(storage=storage).refresh_model_subtree(
                    reloaded,
                )
            )
        except Exception as exc:  # noqa: BLE001 — best-effort
            warnings.append(
                f"{model_name}: embedding refresh failed: {exc}"
            )
    return warnings
