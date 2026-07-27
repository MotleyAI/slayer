"""v4 → v5 schema migration for SlayerModel.

DEV-1361: rename DataType vocabulary to align with sqlglot's
``exp.DataType.Type`` enum, drop dead aggregation pseudo-types, and prepare
for the auto-ingestion INT/DOUBLE distinction.

This is a **pure dict transform**. The DB-introspection refinement step
(``DOUBLE → INT`` for base columns where the live SQL type is integer)
lives in ``slayer.storage.type_refinement`` and is invoked by storage
backends after this dict-migrator runs.
"""

from __future__ import annotations


from slayer.storage.migrations import register_migration


_LEGACY_TO_V5: dict[str, str] = {
    # Pre-rename canonical values.
    "string": "TEXT",
    "number": "DOUBLE",
    "integer": "INT",
    "time": "TIMESTAMP",
    "date": "DATE",
    "boolean": "BOOLEAN",
}

_LEGACY_PSEUDO: frozenset[str] = frozenset({
    "count", "count_distinct", "sum", "avg", "min", "max", "last",
})


def _coerce_type_string(t: str | None) -> tuple[bool, str | None]:
    """Map a single ``type`` value. Returns ``(should_set, new_value)``.

    * Legacy lowercase canonical → mapped uppercase.
    * Pseudo-type → drop (caller should remove the field).
    * Already uppercase / unknown → pass through unchanged.
    """
    if not isinstance(t, str):
        return True, t
    if t in _LEGACY_TO_V5:
        return True, _LEGACY_TO_V5[t]
    if t in _LEGACY_PSEUDO:
        return False, None
    return True, t


def _rewrite_columns_in_place(columns: list) -> None:
    for col in columns:
        if not isinstance(col, dict):
            continue
        if "type" not in col:
            continue
        keep, mapped = _coerce_type_string(col["type"])
        if not keep:
            del col["type"]
        elif mapped != col["type"]:
            col["type"] = mapped


def _walk_inline_source_models(d: dict) -> None:
    """Recurse into ``source_queries[].source_model`` inline dicts so multi-
    stage models migrate cleanly.
    """
    sqs = d.get("source_queries")
    if not isinstance(sqs, list):
        return
    for sq in sqs:
        if not isinstance(sq, dict):
            continue
        sm = sq.get("source_model")
        if isinstance(sm, dict):
            cols = sm.get("columns")
            if isinstance(cols, list):
                _rewrite_columns_in_place(cols)
            # Recurse one more level — nested inline source_queries are rare
            # but supported by the model shape.
            _walk_inline_source_models(sm)


@register_migration("SlayerModel", 4)
def _model_v4_to_v5(data: dict) -> dict:
    """Rename legacy ``DataType`` values on every column; strip pseudo-type
    values so the field falls through to its default after Pydantic loads.
    """
    cols = data.get("columns")
    if isinstance(cols, list):
        _rewrite_columns_in_place(cols)
    _walk_inline_source_models(data)
    return data
