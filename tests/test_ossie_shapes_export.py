"""The ``SLAYER`` payload of an Ossie export carries the final shapes; relationships map to Ossie's key lists.

Spec: compatibility/ossie-vocabulary › The Ossie export payload carries the vocabulary (An export uses the new keys).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from tests._osi_export_fixtures import customers, monthly_revenue, rich_orders
from tests._osi_export_helpers import entities, export, make_env, payload, relationships, save, slayer_entries

OSSIE_NAMES = {"String", "Integer", "Decimal", "Float", "Boolean", "Date", "Time", "DateTime", "DateTimeTz", "Opaque",
               "Array"}
PRE_2_0_SHAPE_KEYS = {"join_pairs", "source_name", "columns", "measures", "joins", "formula", "target_model",
                      "source_model", "filters"}


def walk(value: Any, path: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], Any]]:
    """Every ``(key path, value)`` under ``value``, except inside a number ``format``."""
    out: list[tuple[tuple[str, ...], Any]] = []
    if isinstance(value, dict):
        for k, v in value.items():
            if k != "format":
                out += [((*path, k), v), *walk(v, (*path, k))]
    elif isinstance(value, list):
        for v in value:
            out += walk(v, path)
    return out


async def exported(tmp_path: Path) -> dict[str, Any]:
    env = await make_env(tmp_path)
    await save(env, customers(), rich_orders(), monthly_revenue())
    return (await export(env)).document


async def test_payloads_carry_no_pre_2_0_shape(tmp_path: Path) -> None:
    bodies = [payload(e) for e in entities(await exported(tmp_path)) if slayer_entries(e)]
    assert bodies
    pairs = [p for b in bodies for p in walk(b)]
    assert not PRE_2_0_SHAPE_KEYS & {path[-1] for path, _ in pairs}
    assert {v for path, v in pairs if path[-1] in ("datatype", "element_type") and v is not None} <= OSSIE_NAMES
    for path, value in pairs:
        if path[-1] == "metrics" and "source_queries" in path:
            for entry in value:
                assert isinstance(entry, str) or "metric" in entry, (path, entry)


async def test_relationships_carry_key_lists(tmp_path: Path) -> None:
    rels = relationships(await exported(tmp_path))
    assert rels
    for rel in rels:
        assert rel["from_columns"]
        assert len(rel["from_columns"]) == len(rel["to_columns"])
