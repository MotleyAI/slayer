"""v1 → v2 schema migrations for SlayerModel and SlayerQuery.

The v2 schema collapses ``SlayerModel.dimensions`` and ``SlayerModel.metrics``
into a single ``fields`` list, and repurposes ``SlayerModel.metrics`` to hold
named formulas (the shape v1 had under ``SlayerQuery.fields``). It also renames
``SlayerQuery.fields`` to ``SlayerQuery.metrics`` to match.

These converters run as pure dict→dict transforms before Pydantic validates,
so they can rename keys, restructure nested objects, and reject unmigratable
inputs without touching the live class schema.
"""

from slayer.core.enums import BUILTIN_AGGREGATIONS
from slayer.storage.migrations import register_migration


def _migrate_dataset_extension_v1_to_v2(ext: dict) -> dict:
    """Migrate a v1 ``ModelExtension`` dict to v2.

    v1 had ``dimensions`` (Dimension objects) and ``measures`` (Measure objects).
    v2 has ``fields`` (Column objects, the merge of both) and ``measures``
    (ModelMeasure formula objects — empty after migration). No-op if the
    extension is already in v2 shape.
    """
    if not _looks_like_v1_dataset(ext):
        return ext

    out = dict(ext)
    fields_key, metrics_key = _v1_keys(out)
    fields: list[dict] = []
    seen: set[str] = set()
    collisions: list[str] = []

    for dim in out.pop("dimensions", None) or []:
        if isinstance(dim, dict):
            _record(dict(dim), fields, seen, collisions)

    for meas in out.pop(metrics_key, None) or []:
        if isinstance(meas, dict):
            _record(_migrate_metric_dict(dict(meas), fields_key=fields_key), fields, seen, collisions)

    if collisions:
        raise ValueError(
            f"Migrating ModelExtension to v2: name collision between v1 "
            f"dimension and measure: {sorted(collisions)}. Rename one before "
            f"migrating."
        )

    out[fields_key] = fields
    out[metrics_key] = []  # repurposed; users may add DatasetMetric expressions later
    return out


def _v1_keys(data: dict) -> tuple[str, str]:
    """The (fields, metrics) keys ``data`` spells: fresh input arrives already renamed to ``metrics``."""
    return ("fields", "metrics") if "metrics" in data and "measures" not in data else ("columns", "measures")


def _migrate_metric_dict(col: dict, *, fields_key: str) -> dict:
    """Apply v1-Measure-specific migrations to a measure dict bound for the
    columns list.

    - Drop the deprecated ``type: sum/avg/...`` alias; preserve user intent by
      seeding ``allowed_aggregations`` if not already present.
    - Default the type to ``number`` if absent (v1 Measures had no DataType), under the key of ``fields_key``'s
      vocabulary.
    - Force ``primary_key`` to False (v1 Measures could not be PKs).
    """
    legacy_type = col.get("type")
    if isinstance(legacy_type, str) and legacy_type.lower() in BUILTIN_AGGREGATIONS:
        col.pop("type")
        col.setdefault("allowed_aggregations", [legacy_type.lower()])
    if "type" not in col and "datatype" not in col:
        col["datatype" if fields_key == "fields" else "type"] = "number"
    col["primary_key"] = False
    return col


def _record(
    col: dict,
    fields: list[dict],
    seen: set[str],
    collisions: list[str],
) -> None:
    """Append ``col`` to ``fields`` unless its name collides with a prior entry."""
    n = col.get("name")
    if not isinstance(n, str):
        # Pydantic will raise on missing/invalid name later; just append and let it.
        fields.append(col)
        return
    if n in seen:
        collisions.append(n)
    else:
        seen.add(n)
        fields.append(col)


def _looks_like_v1_dataset(data: dict) -> bool:
    """Decide whether to actually run the v1→v2 SlayerModel converter.

    The chain walker invokes this migration whenever ``version < 2`` (including
    the no-version case for fresh Pydantic constructions, which default to v1).
    But fresh v2 constructions arrive here with v2-shaped keys (``fields``)
    that we must not clobber.

    Heuristics for "this is v1 data":
    - presence of a ``dimensions`` key (v1-only); OR
    - a ``measures`` list whose first dict element lacks a ``formula`` key
      (v2 ``ModelMeasure`` always has ``formula``; v1 ``Measure`` never did).
    """
    if "dimensions" in data:
        return True
    raw_measures = data.get(_v1_keys(data)[1])
    if not isinstance(raw_measures, (list, tuple)) or not raw_measures:
        return False
    first = raw_measures[0]
    if not isinstance(first, dict):
        return False
    # v2 DatasetMetric has an expression (required); v1 Measure didn't.
    return "formula" not in first and "expression" not in first


@register_migration("SlayerModel", 1)
def _dataset_v1_to_v2(data: dict) -> dict:
    """Merge v1 ``dimensions`` + ``measures`` into v2 ``fields``; reset ``measures``.

    Also recursively migrates ``source_queries`` entries so re-saves don't
    persist v1 inner shapes. See module docstring for the rationale.
    """
    if _looks_like_v1_dataset(data):
        _merge_v1_fields(data)
    # Even an already-merged dataset can carry v1 SlayerQuery dicts under source_queries.
    _migrate_v1_source_queries(data)
    return data


def _merge_v1_fields(data: dict) -> None:
    """v1 dimensions, then v1 measures (with measure defaults), become fields; metrics reset to empty."""
    fields_key, metrics_key = _v1_keys(data)
    columns: list[dict] = []
    seen: set[str] = set()
    collisions: list[str] = []
    for dim in data.pop("dimensions", None) or []:
        if isinstance(dim, dict):
            _record(dict(dim), columns, seen, collisions)
    for meas in data.pop(metrics_key, None) or []:
        if isinstance(meas, dict):
            _record(_migrate_metric_dict(dict(meas), fields_key=fields_key), columns, seen, collisions)
    if collisions:
        raise ValueError(
            f"Migrating model '{data.get('name', '<unknown>')}' to v2: name "
            f"collision between v1 dimension and measure: {sorted(collisions)}. "
            f"Rename one before migrating."
        )
    data[fields_key] = columns
    data[metrics_key] = []


def _migrate_v1_source_queries(data: dict) -> None:
    """Migrate each pre-v2 ``source_queries`` entry in place."""
    raw_source_queries = data.get("source_queries")
    if isinstance(raw_source_queries, list):
        migrated_sq: list = []
        for q in raw_source_queries:
            if isinstance(q, dict) and int(q.get("version") or 1) < 2:
                migrated = _query_v1_to_v2(dict(q))
                if "version" in q:
                    migrated["version"] = 2
                migrated_sq.append(migrated)
            else:
                migrated_sq.append(q)
        data["source_queries"] = migrated_sq


@register_migration("SlayerQuery", 1)
def _query_v1_to_v2(data: dict) -> dict:
    """Rename ``fields`` → ``measures``; recursively migrate inline ModelExtension."""
    if "fields" in data:
        if "measures" in data:
            raise ValueError(
                "v1 SlayerQuery has both 'fields' and 'measures'; cannot "
                "migrate unambiguously. Drop one before migrating."
            )
        data["measures"] = data.pop("fields")

    sm = data.get("source_model")
    if isinstance(sm, dict) and "source_name" in sm and (
        "dimensions" in sm or "measures" in sm
    ):
        # Inline ModelExtension — no version of its own; piggyback this query's
        # v1→v2 migration to rename dimensions→columns and convert measures.
        data["source_model"] = _migrate_dataset_extension_v1_to_v2(sm)

    return data
