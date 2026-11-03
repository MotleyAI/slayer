"""A catalog relation loads into the facade's DuckDB as built statements, its values bound as parameters."""

from __future__ import annotations

from typing import Any

import duckdb
import pytest

import slayer.facade.catalog_sql as catalog_sql
from slayer.core.enums import DataType
from slayer.facade.catalog import build_catalog_grouped_by_schema
from slayer.facade.catalog_sql import CatalogRelation, CatalogSqlExecutor
from slayer.facade.rows import FacadeColumn

ROWS = [
    {"MixedCase": "it's", 'say "hi"': "a ? b", "n": 1},
    {"MixedCase": "?", 'say "hi"': "''", "n": 2},
    {"MixedCase": None, 'say "hi"': "x", "n": 3},
]
RELATION = CatalogRelation(name='Odd "Tbl"', columns=[
    FacadeColumn(name="MixedCase", type=DataType.TEXT),
    FacadeColumn(name='say "hi"', type=DataType.TEXT),
    FacadeColumn(name="n", type=DataType.INT),
], rows=ROWS)


class _Recording:
    """A DuckDB connection that records every statement it runs."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, sink: list[str]) -> None:
        self._conn = conn
        self._sink = sink

    def execute(self, sql: str, *args: Any) -> Any:
        self._sink.append(sql)
        return self._conn.execute(sql, *args)

    def executemany(self, sql: str, *args: Any) -> Any:
        self._sink.append(sql)
        return self._conn.executemany(sql, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


@pytest.fixture
def loaded(monkeypatch: pytest.MonkeyPatch) -> tuple[CatalogSqlExecutor, list[str]]:
    seen: list[str] = []
    real = duckdb.connect
    monkeypatch.setattr(catalog_sql.duckdb, "connect", lambda *a, **k: _Recording(real(*a, **k), seen))
    executor = CatalogSqlExecutor(catalog=build_catalog_grouped_by_schema(models_by_datasource={}),
                                  datasource="ds", extra_relations=[RELATION])
    return executor, seen


def test_rows_round_trip(loaded: tuple[CatalogSqlExecutor, list[str]]) -> None:
    executor, _ = loaded
    got = executor._conn.execute('SELECT "MixedCase", "say ""hi""", n FROM "Odd ""Tbl""" ORDER BY n').fetchall()
    assert got == [(r["MixedCase"], r['say "hi"'], r["n"]) for r in ROWS]


def test_values_are_bound_never_spelled(loaded: tuple[CatalogSqlExecutor, list[str]]) -> None:
    _, seen = loaded
    (insert,) = [s for s in seen if s.lstrip().upper().startswith("INSERT") and "Tbl" in s]
    assert insert.count("?") == len(RELATION.columns)
    assert "it''s" not in insert and "a ? b" not in insert
