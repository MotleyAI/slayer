"""``unreachable_columns`` drops a column only when no join reaches the path it reads."""

from __future__ import annotations

import pytest

from slayer.core.models import Column, ModelJoin, SlayerModel
from slayer.importers.measures import unreachable_columns

CHAIN = ("a", "b", "c", "d", "e")


def _chain(reader_sql: str) -> dict[str, SlayerModel]:
    """``a`` → ``b`` → … → ``e``, each joined to the next; ``a`` has a column ``r`` reading ``reader_sql``."""
    models = {}
    for i, name in enumerate(CHAIN):
        nxt = CHAIN[i + 1] if i + 1 < len(CHAIN) else None
        columns = [Column(name="id"), Column(name="v")]
        joins = []
        if nxt is not None:
            columns.append(Column(name=f"{nxt}_id"))
            joins.append(ModelJoin(target_model=nxt, join_pairs=[[f"{nxt}_id", "id"]]))
        if name == "a":
            columns.append(Column(name="r", sql=reader_sql))
        models[name] = SlayerModel(name=name, sql_table=name, data_source="d", columns=columns, joins=joins)
    return models


def _problems(reader_sql: str) -> dict[str, str]:
    models = _chain(reader_sql)
    return unreachable_columns(models["a"], models_by_name=models, dialect="duckdb")


@pytest.mark.parametrize("sql", ["b.c.d.e.v", "a.b.c.d.e.v", "b.v + 1", "a.v"],
                         ids=["five-part", "host-anchored-six-part", "one-hop", "own-column"])
def test_reachable_path_is_kept(sql: str) -> None:
    assert _problems(sql) == {}


@pytest.mark.parametrize("sql", ["b.c.x.v", "a.x.v", "a.b.missing"],
                         ids=["broken-hop", "host-anchored-broken-hop", "missing-leaf"])
def test_unreachable_path_is_reported(sql: str) -> None:
    assert list(_problems(sql)) == ["r"]
