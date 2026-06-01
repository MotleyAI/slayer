"""Sample-value profiling for ``Column.sampled`` (DEV-1375 + DEV-1480).

The internals were extracted from ``slayer/mcp/server.py``'s
``_collect_dim_profile`` so both ``inspect_model`` and the search-index
refresh hooks can call them without circular imports.

Public surface:

* :class:`ColumnSample` — three-field result of profiling a single column.
  Carries ``sampled`` (text), ``sampled_values`` (structured top-N for
  categorical), and ``distinct_count`` (true cardinality for categorical).
* :func:`profile_column` — produce the ``ColumnSample`` for a single column.
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

DEV-1480 changes:
- Categorical cap raised from 20 → 50 distinct values.
- Categorical query orders by per-value count desc (alphabetical tie-break
  in SQL) so the persisted top-N is "most common values first".
- New ``Column.sampled_values: Optional[List[str]]`` carries the top-50
  list verbatim (no ambiguous text split). For categorical columns it is
  populated on ≤50 distinct AND on overflow when the secondary
  ``count_distinct`` query succeeds. It stays ``None`` only for
  numeric/temporal columns AND for the rare overflow branch where the
  secondary ``count_distinct`` query fails (intentional cache-miss retry
  signal — ``_is_sample_cached`` flags it stale so the next
  ``inspect_model`` / ``refresh-samples`` call retries).
- New ``Column.distinct_count: Optional[int]`` carries the true total
  cardinality; the overflow branch fires a second ``count_distinct`` query
  via a transient ``ModelExtension`` (bypassing ``Column.allowed_aggregations``
  and ``Column.filter``).
- Text ``sampled`` format unchanged for ≤ 50 distinct (top-20 joined). For
  overflow it becomes ``", ".join(top_20) + " ... (N distinct)"`` carrying
  the true total — replacing the legacy ``"> 50 distinct"`` marker.
- The internal ``_DimProfileEntry`` shape stays the same — overflow keeps
  ``values=None, distinct_count=None`` to signal "data omitted from the
  legacy entry". The richer DEV-1480 data only lives on ``ColumnSample``
  produced by ``profile_column``.

DEV-1516 additions:
- :func:`ensure_column_sample_fresh` — shared cache-aware refresh helper
  used by both ``inspect_model``'s categorical loop and the search
  service's post-fusion column-hit hook. Returns the input column on cache
  hit / non-categorical / failure, and an in-memory refreshed copy on
  success (after persisting via storage).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DEV-1480: categorical cap and public-ish result type
# ---------------------------------------------------------------------------


# Categorical cardinality cap. Lifts the legacy 20 from DEV-1375.
_MAX_CATEGORICAL_VALUES = 50
# How many of the top values get joined into the text ``sampled`` summary.
_TEXT_SAMPLE_CAP = 20


class ColumnSample(NamedTuple):
    """Three-field result of profiling a single column.

    - ``sampled`` is the human-readable string (``Column.sampled``).
    - ``sampled_values`` is the structured top-N list (``Column.sampled_values``).
      ``None`` for overflow > 50 and for numeric/temporal columns.
    - ``distinct_count`` is the true cardinality (``Column.distinct_count``).
      ``None`` for numeric/temporal columns.
    """

    sampled: Optional[str]
    sampled_values: Optional[List[str]]
    distinct_count: Optional[int]


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

    For categorical entries this is the "first ``_TEXT_SAMPLE_CAP`` values
    joined by ``, ``" form; the DEV-1480 overflow suffix is appended only
    by the higher-level ``profile_column`` flow when it has the true total
    in hand.
    """
    if entry.values is not None:
        return ", ".join(str(v) for v in entry.values[:_TEXT_SAMPLE_CAP])
    if (
        entry.distinct_count is None
        and entry.values is None
        and entry.min_value is None
        and entry.max_value is None
    ):
        # Pre-DEV-1480 legacy callers still get a textual overflow marker;
        # the DEV-1480 ``profile_column`` flow doesn't use this branch and
        # produces the richer ``", ".join(top_20) + " ... (N distinct)"``
        # form directly.
        return f"> {_MAX_CATEGORICAL_VALUES} distinct"
    return f"{entry.min_value} .. {entry.max_value}"


async def _profile_categorical_column(
    *,
    model: SlayerModel,
    column: Column,
    engine: SlayerQueryEngine,
    max_values: int,
) -> Optional[_DimProfileEntry]:
    """Profile one string/boolean column.

    DEV-1480: orders by per-value count desc with alphabetical tie-break in
    SQL, so the top-N persisted is deterministic and "most common first".
    LIMIT is ``max_values + 2`` so a single NULL row doesn't push a
    legitimate non-overflow result over the cap.

    Returns ``None`` when the column query fails — caller skips the column.
    The returned entry uses the legacy shape (``values=None, distinct_count=None``
    signals overflow); DEV-1480's true-total ``distinct_count`` is filled
    by ``profile_column``.
    """
    try:
        q = SlayerQuery.model_validate({
            "source_model": model.name,
            "dimensions": [{"name": column.name}],
            "measures": [{"formula": "*:count"}],
            "order": [
                {"column": "_count", "direction": "desc"},
                {"column": column.name, "direction": "asc"},
            ],
            "limit": max_values + 2,
        })
        r = await engine.execute(query=q, data_source=model.data_source or None)
    except Exception:
        return None
    value_key = f"{model.name}.{column.name}"
    # Filter NULL values out — they map to ``col IS NULL`` predicates, not
    # to literal-equality use cases the validator cares about.
    raw_pairs: List[Tuple[Any, Any]] = []
    count_key = f"{model.name}._count"
    for row in r.data:
        v = row.get(value_key)
        if v is None:
            continue
        raw_pairs.append((v, row.get(count_key)))
    # SQL already sorted by (count desc, value asc). Python belt-and-braces
    # re-sort guards against backends that ignore tie-break or return
    # equally-ranked rows in arbitrary order. NB: this only re-orders what
    # we received — the LIMIT cutoff is the SQL's responsibility.
    raw_pairs.sort(key=lambda p: (-(p[1] or 0), str(p[0])))
    values: List[str] = [str(v) for v, _ in raw_pairs]
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
    max_values: int = _MAX_CATEGORICAL_VALUES,
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

    DEV-1480: ``max_values`` defaults to 50 (was 20). Callers that need
    the structured top-50 + true total should use :func:`profile_column`
    per column, which fires the secondary ``count_distinct`` query on
    overflow and returns a :class:`ColumnSample`.
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
# DEV-1480: cache-validity helper
# ---------------------------------------------------------------------------


_CATEGORICAL_TYPES = (DataType.TEXT, DataType.BOOLEAN)


def _is_sample_cached(column: Column) -> bool:
    """Return ``True`` when the column's persisted sample-value cache is
    valid (no re-profile needed), ``False`` when it's missing/stale.

    Hidden/PK columns are never profiled — treat them as "cached" (the
    caller still skips them).

    For categorical columns the structured ``sampled_values`` field is
    authoritative: when it's ``None`` the cache is stale, even if a
    pre-DEV-1480 ``sampled`` text string is set. This forces v6→v7
    upgrades to re-profile categorical columns on next ``inspect_model``
    so the new structured field gets populated.

    For numeric/temporal columns ``sampled_values`` is always ``None`` —
    the legacy ``sampled`` text is the cache indicator.
    """
    if column.hidden or column.primary_key:
        return True
    if column.type in _CATEGORICAL_TYPES:
        return column.sampled_values is not None
    return column.sampled is not None


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def _is_table_backed(model: SlayerModel) -> bool:
    """Only ``sql_table`` mode supports the v1 sample-value refresh path.

    sql-mode and query-backed models are silently skipped (DEV-1377
    follow-up). This mirrors ``ingest_datasource_idempotent``'s carve-out.
    """
    return bool(model.sql_table) and not model.sql and not model.source_queries


async def _count_distinct_via_model_extension(
    *,
    model: SlayerModel,
    column: Column,
    engine: SlayerQueryEngine,
) -> Optional[int]:
    """Fire a secondary ``count_distinct`` query via a transient
    ``ModelExtension`` column.

    Bypasses both ``Column.allowed_aggregations`` (which might omit
    ``count_distinct``) and ``Column.filter`` (which would otherwise apply
    a CASE-WHEN at aggregation time and under-count). Mirrors the existing
    ``_profile_numeric_temporal_columns`` pattern.

    Returns ``None`` when the query fails.
    """
    try:
        ext_q = SlayerQuery.model_validate({
            "source_model": {
                "source_name": model.name,
                "columns": [{
                    "name": "_slayer_distinct_probe",
                    "sql": column.sql if column.sql else column.name,
                    "type": str(column.type),
                }],
            },
            "measures": [{
                "formula": "_slayer_distinct_probe:count_distinct",
            }],
        })
        r = await engine.execute(query=ext_q, data_source=model.data_source or None)
    except Exception:  # NOSONAR(S112) — best-effort: see module docstring
        return None
    if not r.data:
        return None
    raw = r.data[0].get(
        f"{model.name}._slayer_distinct_probe_count_distinct",
    )
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


async def _profile_categorical_with_total(
    *,
    model: SlayerModel,
    column: Column,
    engine: SlayerQueryEngine,
) -> Optional[ColumnSample]:
    """DEV-1480 categorical profile: top-50 by frequency + true total on
    overflow.

    Re-runs the query without the post-overflow path of
    ``_profile_categorical_column`` so we can keep the top-50 list when
    overflow is detected (the legacy entry shape would have discarded it).
    """
    # Run the top-values query directly (instead of going through
    # ``_profile_categorical_column``) so we retain the values list even
    # in the overflow case.
    try:
        q = SlayerQuery.model_validate({
            "source_model": model.name,
            "dimensions": [{"name": column.name}],
            "measures": [{"formula": "*:count"}],
            "order": [
                {"column": "_count", "direction": "desc"},
                {"column": column.name, "direction": "asc"},
            ],
            "limit": _MAX_CATEGORICAL_VALUES + 2,
        })
        r = await engine.execute(query=q, data_source=model.data_source or None)
    except Exception:  # NOSONAR(S112) — best-effort: see module docstring
        return None
    value_key = f"{model.name}.{column.name}"
    count_key = f"{model.name}._count"
    raw_pairs: List[Tuple[Any, Any]] = []
    for row in r.data:
        v = row.get(value_key)
        if v is None:
            continue
        raw_pairs.append((v, row.get(count_key)))
    raw_pairs.sort(key=lambda p: (-(p[1] or 0), str(p[0])))
    values: List[str] = [str(v) for v, _ in raw_pairs]
    overflow = len(values) > _MAX_CATEGORICAL_VALUES
    if not overflow:
        text = ", ".join(values[:_TEXT_SAMPLE_CAP])
        return ColumnSample(
            sampled=text,
            sampled_values=values,
            distinct_count=len(values),
        )
    # Overflow: fire the secondary count_distinct query for the true total.
    total = await _count_distinct_via_model_extension(
        model=model, column=column, engine=engine,
    )
    top_50 = values[:_MAX_CATEGORICAL_VALUES]
    top_20_text = ", ".join(top_50[:_TEXT_SAMPLE_CAP])
    if total is None:
        # Defensive: count_distinct query failed (transient backend error,
        # missing permission, etc.). Persist ``sampled_values=None`` rather
        # than the top-50 list so ``_is_sample_cached`` classifies the
        # column as a cache miss and the next ``inspect_model`` /
        # ``refresh-samples`` call retries the secondary query. Persisting
        # the top-50 here would mark the column "cached" forever despite
        # ``distinct_count`` being permanently None.
        return ColumnSample(
            sampled=f"> {_MAX_CATEGORICAL_VALUES} distinct",
            sampled_values=None,
            distinct_count=None,
        )
    return ColumnSample(
        sampled=f"{top_20_text} ... ({total} distinct)",
        sampled_values=top_50,
        distinct_count=total,
    )


async def profile_column(
    *,
    model: SlayerModel,
    column: Column,
    engine: SlayerQueryEngine,
) -> Optional[ColumnSample]:
    """Return the :class:`ColumnSample` for ``column`` on ``model``.

    Returns ``None`` for primary-key / hidden columns and when the
    profile query fails or yields no data. Caller decides whether to
    persist the ``None`` (clearing any stale value) or skip it.

    DEV-1480: signature widened from ``Optional[str]`` to
    ``Optional[ColumnSample]`` so the structured ``sampled_values`` and
    ``distinct_count`` are returned alongside the legacy text.
    """
    if column.hidden or column.primary_key:
        return None
    if column.type in _CATEGORICAL_TYPES:
        return await _profile_categorical_with_total(
            model=model, column=column, engine=engine,
        )
    # Numeric / temporal: route through the batched legacy entry path,
    # then build a ColumnSample with only ``sampled`` populated.
    entries = await _collect_dim_profile(
        model=model, engine=engine, only_columns={column.name},
    )
    if not entries:
        return None
    entry = entries[0]
    if entry.min_value is None and entry.max_value is None:
        return None
    return ColumnSample(
        sampled=f"{entry.min_value} .. {entry.max_value}",
        sampled_values=None,
        distinct_count=None,
    )


async def _refresh_one_column(
    *,
    model: SlayerModel,
    column: Column,
    engine: SlayerQueryEngine,
    storage: StorageBackend,
) -> List[str]:
    """Profile + persist a single column. Best-effort — returns the list of
    error strings produced (empty on full success). Extracted from
    ``refresh_table_backed_model_sampled`` to keep that function's cognitive
    complexity low.
    """
    errors: List[str] = []
    sample: Optional[ColumnSample] = None
    try:
        sample = await profile_column(model=model, column=column, engine=engine)
    except Exception as exc:  # NOSONAR(S112) — best-effort: see module docstring
        errors.append(f"{model.name}.{column.name}: {exc}")
    if sample is not None:
        sampled = sample.sampled
        sampled_values = sample.sampled_values
        distinct_count = sample.distinct_count
    else:
        sampled = sampled_values = distinct_count = None
    try:
        await storage.update_column_sampled(
            data_source=model.data_source,
            model_name=model.name,
            column_name=column.name,
            sampled=sampled,
            sampled_values=sampled_values,
            distinct_count=distinct_count,
        )
    except Exception as exc:  # NOSONAR(S112) — best-effort: see module docstring
        errors.append(f"{model.name}.{column.name} (persist): {exc}")
    return errors


async def refresh_table_backed_model_sampled(
    *,
    model: SlayerModel,
    engine: SlayerQueryEngine,
    storage: StorageBackend,
    only_columns: Optional[Set[str]] = None,
) -> List[str]:
    """Refresh ``Column.sampled``, ``Column.sampled_values``, and
    ``Column.distinct_count`` for each eligible column on ``model``.

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
        errors.extend(await _refresh_one_column(
            model=model, column=column, engine=engine, storage=storage,
        ))
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


# ---------------------------------------------------------------------------
# DEV-1516: shared cache-aware refresh helper
# ---------------------------------------------------------------------------


async def ensure_column_sample_fresh(
    *,
    model: SlayerModel,
    column: Column,
    engine: SlayerQueryEngine,
    storage: StorageBackend,
) -> Column:
    """Best-effort refresh of a stale categorical column's persisted sample.

    Used by both :func:`slayer.mcp.server.inspect_model` (categorical cache
    miss path) and :class:`slayer.search.service.SearchService` (post-fusion
    column-hit hook) so DEV-1516's "stale columns auto-refresh on the spot"
    contract has a single source of truth.

    Returns the **input column unchanged** when:

    - ``_is_sample_cached(column)`` is True (cache hit; includes hidden /
      primary-key columns by convention),
    - the column is not categorical (numeric / temporal are handled by
      ``inspect_model``'s batched min/max path; the search hook never
      refreshes them since their ``sampled`` text has no 20-vs-50 issue),
    - :func:`profile_column` returns ``None`` (e.g. transient query failure
      or no rows),
    - :func:`profile_column` raises (logged + swallowed),
    - ``storage.update_column_sampled`` raises (logged + swallowed; the
      in-memory refresh is still returned so the caller can render fresh
      data this call).

    Returns a Pydantic ``model_copy``'d column with refreshed
    ``sampled`` / ``sampled_values`` / ``distinct_count`` fields on
    success (after persisting via storage).

    Logs ``WARNING`` on profile + persist failures with
    ``(data_source, model_name, column_name)`` context so observability
    matches the pre-DEV-1516 inline implementation in ``inspect_model``.
    """
    if _is_sample_cached(column):
        return column
    if column.type not in _CATEGORICAL_TYPES:
        # Numeric / temporal: inspect_model handles them via the batched
        # min/max query. The search refresh hook intentionally skips them
        # because their ``sampled`` text is a min/max range, not a value
        # list — the 20-vs-50 distinction does not apply.
        return column
    try:
        sample = await profile_column(
            model=model, column=column, engine=engine,
        )
    except Exception as exc:  # NOSONAR(S112) — best-effort: see module docstring
        logger.warning(
            "ensure_column_sample_fresh: failed to profile %s.%s.%s: %s",
            model.data_source, model.name, column.name, exc,
        )
        return column
    if sample is None:
        # No data to persist (e.g. PK / hidden / no rows). Helper short-
        # circuits without writing — keeps cache predicate from flipping.
        return column
    try:
        await storage.update_column_sampled(
            data_source=model.data_source,
            model_name=model.name,
            column_name=column.name,
            sampled=sample.sampled,
            sampled_values=sample.sampled_values,
            distinct_count=sample.distinct_count,
        )
    except Exception as exc:  # NOSONAR(S112) — best-effort: see module docstring
        logger.warning(
            "ensure_column_sample_fresh: failed to persist sample for "
            "%s.%s.%s via update_column_sampled: %s",
            model.data_source, model.name, column.name, exc,
        )
        # Fall through: surface the in-memory refresh so the caller can
        # still render fresh data this call. Next call will retry — the
        # cache predicate still flags the column stale because the persist
        # never landed.
    return column.model_copy(update={
        "sampled": sample.sampled,
        "sampled_values": sample.sampled_values,
        "distinct_count": sample.distinct_count,
    })
