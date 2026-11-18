"""``unreachable_columns`` drops a column only when no join reaches the path it reads."""

from __future__ import annotations

import pytest

from slayer.core.models import DatasetField, DatasetRelationship, SlayerDataset
from slayer.importers.metrics import unreachable_fields

CHAIN = ("a", "b", "c", "d", "e")


def _chain(reader_sql: str) -> dict[str, SlayerDataset]:
    """``a`` → ``b`` → … → ``e``, each joined to the next; ``a`` has a column ``r`` reading ``reader_sql``."""
    models = {}
    for i, name in enumerate(CHAIN):
        nxt = CHAIN[i + 1] if i + 1 < len(CHAIN) else None
        columns = [DatasetField(name="id"), DatasetField(name="v")]
        joins = []
        if nxt is not None:
            columns.append(DatasetField(name=f"{nxt}_id"))
            joins.append(DatasetRelationship(to=nxt, join_pairs=[[f"{nxt}_id", "id"]]))
        if name == "a":
            columns.append(DatasetField(name="r", sql=reader_sql))
        models[name] = SlayerDataset(name=name, sql_table=name, data_source="d", fields=columns, relationships=joins)
    return models


def _problems(reader_sql: str) -> dict[str, str]:
    models = _chain(reader_sql)
    return unreachable_fields(models["a"], datasets_by_name=models, dialect="duckdb")


@pytest.mark.parametrize("sql", ["b.c.d.e.v", "a.b.c.d.e.v", "b.v + 1", "a.v"],
                         ids=["five-part", "host-anchored-six-part", "one-hop", "own-column"])
def test_reachable_path_is_kept(sql: str) -> None:
    assert _problems(sql) == {}


@pytest.mark.parametrize("sql", ["b.c.x.v", "a.x.v", "a.b.missing"],
                         ids=["broken-hop", "host-anchored-broken-hop", "missing-leaf"])
def test_unreachable_path_is_reported(sql: str) -> None:
    assert list(_problems(sql)) == ["r"]
