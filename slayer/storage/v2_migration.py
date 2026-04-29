"""v1 → v2 schema migrations for SlayerModel and SlayerQuery.

The v2 schema collapses ``SlayerModel.dimensions`` and ``SlayerModel.measures``
into a single ``columns`` list, and repurposes ``SlayerModel.measures`` to hold
named formulas (the shape v1 had under ``SlayerQuery.fields``). It also renames
``SlayerQuery.fields`` to ``SlayerQuery.measures`` to match.

These converters run as pure dict→dict transforms before Pydantic validates,
so they can rename keys, restructure nested objects, and reject unmigratable
inputs without touching the live class schema.
"""

from slayer.core.enums import BUILTIN_AGGREGATIONS
from slayer.storage.migrations import register_migration


def _migrate_model_extension_v1_to_v2(ext: dict) -> dict:
    """Migrate a v1 ``ModelExtension`` dict to v2.

    v1 had ``dimensions`` (Dimension objects) and ``measures`` (Measure objects).
    v2 has ``columns`` (Column objects, the merge of both) and ``measures``
    (ModelMeasure formula objects — empty after migration). No-op if the
    extension is already in v2 shape.
    """
    if not _looks_like_v1_model(ext):
        return ext

    out = dict(ext)
    columns: list[dict] = []
    seen: set[str] = set()
    collisions: list[str] = []

    for dim in out.pop("dimensions", None) or []:
        if isinstance(dim, dict):
            _record(dict(dim), columns, seen, collisions)

    for meas in out.pop("measures", None) or []:
        if isinstance(meas, dict):
            _record(_migrate_measure_dict(dict(meas)), columns, seen, collisions)

    if collisions:
        raise ValueError(
            f"Migrating ModelExtension to v2: name collision between v1 "
            f"dimension and measure: {sorted(collisions)}. Rename one before "
            f"migrating."
        )

    out["columns"] = columns
    out["measures"] = []  # repurposed; users may add ModelMeasure formulas later
    return out


def _migrate_measure_dict(col: dict) -> dict:
    """Apply v1-Measure-specific migrations to a measure dict bound for the
    columns list.

    - Drop the deprecated ``type: sum/avg/...`` alias; preserve user intent by
      seeding ``allowed_aggregations`` if not already present.
    - Default ``type`` to ``number`` if absent (v1 Measures had no DataType).
    - Force ``primary_key`` to False (v1 Measures could not be PKs).
    """
    legacy_type = col.get("type")
    if isinstance(legacy_type, str) and legacy_type.lower() in BUILTIN_AGGREGATIONS:
        col.pop("type")
        col.setdefault("allowed_aggregations", [legacy_type.lower()])
    col.setdefault("type", "number")
    col["primary_key"] = False
    return col


def _record(
    col: dict,
    columns: list[dict],
    seen: set[str],
    collisions: list[str],
) -> None:
    """Append ``col`` to ``columns`` unless its name collides with a prior entry."""
    n = col.get("name")
    if not isinstance(n, str):
        # Pydantic will raise on missing/invalid name later; just append and let it.
        columns.append(col)
        return
    if n in seen:
        collisions.append(n)
    else:
        seen.add(n)
        columns.append(col)


def _looks_like_v1_model(data: dict) -> bool:
    """Decide whether to actually run the v1→v2 SlayerModel converter.

    The chain walker invokes this migration whenever ``version < 2`` (including
    the no-version case for fresh Pydantic constructions, which default to v1).
    But fresh v2 constructions arrive here with v2-shaped keys (``columns``)
    that we must not clobber.

    Heuristics for "this is v1 data":
    - presence of a ``dimensions`` key (v1-only); OR
    - a ``measures`` list whose first dict element lacks a ``formula`` key
      (v2 ``ModelMeasure`` always has ``formula``; v1 ``Measure`` never did).
    """
    if "dimensions" in data:
        return True
    raw_measures = data.get("measures")
    if not raw_measures:
        return False
    first = raw_measures[0]
    if not isinstance(first, dict):
        return False
    # v2 ModelMeasure has `formula` (required); v1 Measure didn't.
    return "formula" not in first


@register_migration("SlayerModel", 1)
def _model_v1_to_v2(data: dict) -> dict:
    """Merge v1 ``dimensions`` + ``measures`` into v2 ``columns``; reset ``measures``.

    See module docstring for the rationale.
    """
    if not _looks_like_v1_model(data):
        # Already v2 shape (or empty fresh-construction). Don't clobber the
        # caller's columns/measures.
        return data

    columns: list[dict] = []
    seen: set[str] = set()
    collisions: list[str] = []

    # (a) v1 Dimensions → Columns. All v1 Dimension fields are valid Column
    #     fields; defaults (allowed_aggregations=None, filter=None) are correct.
    for dim in data.pop("dimensions", None) or []:
        if isinstance(dim, dict):
            _record(dict(dim), columns, seen, collisions)

    # (b) v1 Measures → Columns (with measure-specific defaults).
    for meas in data.pop("measures", None) or []:
        if isinstance(meas, dict):
            _record(_migrate_measure_dict(dict(meas)), columns, seen, collisions)

    if collisions:
        raise ValueError(
            f"Migrating model '{data.get('name', '<unknown>')}' to v2: name "
            f"collision between v1 dimension and measure: {sorted(collisions)}. "
            f"Rename one before migrating."
        )

    data["columns"] = columns
    # The new measures field is the formula list — empty after migration. Users
    # can populate it post-migration with named formulas (was Query.fields).
    data["measures"] = []
    return data


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
        data["source_model"] = _migrate_model_extension_v1_to_v2(sm)

    return data
