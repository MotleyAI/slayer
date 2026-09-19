"""DEV-1943 §5 — the one seeded executing-engine context.

``seeded_exec_engine`` seeds a temp db, yields a working engine + its path, and
on exit closes the query engine and invalidates the datasource's factory engine
*before* the temp dir is removed — so no engine outlives its file. Signature
pinned here (implement §5.1 follows it):

    async with seeded_exec_engine(dialect=..., seed=fn, models=[...]) as (engine, db_path): ...
"""

from __future__ import annotations

import sqlite3
import sys
import warnings
from pathlib import Path
from unittest.mock import patch

import pytest

from slayer.core.enums import DataType
from slayer.core.models import Column, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.sql import engine_factory
from tests._engine_helpers import seeded_exec_engine

_ON_313 = sys.version_info >= (3, 13)


def _seed_nums(dialect: str):
    def seed(db_path: str) -> None:
        if dialect == "duckdb":
            duckdb = pytest.importorskip("duckdb")
            con = duckdb.connect(db_path)
            con.execute("CREATE TABLE nums (v INTEGER)")
            con.executemany("INSERT INTO nums VALUES (?)", [(i,) for i in range(3)])
            con.close()
        else:
            con = sqlite3.connect(db_path)
            con.execute("CREATE TABLE nums (v INTEGER)")
            con.executemany("INSERT INTO nums VALUES (?)", [(i,) for i in range(3)])
            con.commit()
            con.close()
    return seed


_MODEL = SlayerModel(
    name="nums", data_source="test", sql_table="nums",
    columns=[Column(name="v", sql="v", type=DataType.INT)],
)


@pytest.mark.parametrize("dialect", ["sqlite", "duckdb"])
async def test_seeded_exec_engine_smoke(dialect: str) -> None:
    if dialect == "duckdb":
        pytest.importorskip("duckdb")
    engine_factory.reset_cache()

    holder: dict[str, object] = {}
    events: list[str] = []
    db_exists_at_invalidate: dict[str, object] = {}
    real_close = SlayerQueryEngine.close
    real_invalidate = engine_factory.invalidate_engine

    def watching_close(self):
        events.append("close")
        return real_close(self)

    def watching_invalidate(datasource):
        events.append("invalidate")
        db_exists_at_invalidate["value"] = Path(str(holder.get("path"))).exists()
        return real_invalidate(datasource)

    with warnings.catch_warnings(record=True) as caught, \
            patch.object(SlayerQueryEngine, "close", watching_close), \
            patch.object(engine_factory, "invalidate_engine", side_effect=watching_invalidate):
        warnings.simplefilter("always")
        async with seeded_exec_engine(
            dialect=dialect, seed=_seed_nums(dialect), models=[_MODEL],
        ) as (engine, db_path):
            holder["path"] = db_path
            assert Path(db_path).exists()
            response = await engine.execute(
                SlayerQuery(source_model="nums", measures=["*:count"]),
            )
            assert response.data[0]["nums._count"] == 3
        # D9: close() ran, then invalidate_engine(), while the db file still existed
        # (before the temp dir was removed).
        assert events == ["close", "invalidate"], events
        assert db_exists_at_invalidate.get("value") is True, "invalidated after the db file was gone"
        assert not any(str(holder["path"]) in key[0] for key in engine_factory._engine_cache), (
            "factory still holds the datasource after the context exited"
        )
    if _ON_313:
        unclosed = [w for w in caught if "unclosed database" in str(w.message).lower()]
        assert not unclosed, [str(w.message) for w in unclosed]
