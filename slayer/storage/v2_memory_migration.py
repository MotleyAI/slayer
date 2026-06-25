"""Memory v1 → v2 schema migration (DEV-1428).

v1: ``Memory.id: int``. v2: ``Memory.id: str``. The converter stringifies
the id. The forbidden-charset guard on the v2 model handles validation
on construction; legacy ints were always positive and decimal-only, so
stringification produces a valid v2 id without further checks.

Legacy ``memories.yaml`` may carry duplicate rows where the SAME logical
id exists in both int and str form (``{"id": 42}`` and ``{"id": "42"}``).
This converter normalises rows in place. Dedupe at the multi-row level
(the YAML / SQLite backends seeing both forms in their corpus) lives in
the storage backends — they reduce duplicates before loading rows through
this converter.
"""

from typing import Any

from slayer.storage.migrations import register_migration


@register_migration("Memory", 1)
def _memory_v1_to_v2(data: dict[str, Any]) -> dict[str, Any]:
    """v1 → v2: stringify the id field. Default sentinel preserved."""
    raw_id = data.get("id", "")
    if isinstance(raw_id, bool):
        raise ValueError(
            f"Cannot migrate Memory v1 → v2: id must not be a bool ({raw_id!r})."
        )
    if isinstance(raw_id, int):
        data["id"] = str(raw_id)
    elif isinstance(raw_id, str):
        data["id"] = raw_id
    else:
        raise ValueError(
            f"Cannot migrate Memory v1 → v2: id must be int or str; "
            f"got {type(raw_id).__name__}."
        )
    return data
