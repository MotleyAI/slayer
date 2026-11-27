"""The table columns a statement's result columns read unchanged, and SQLite's declared-type read over them (spec:
queries/time-of-day › Database time-of-day columns ingest as Time)."""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot

from slayer.core.enums import DataType
from slayer.core.models import DatasourceConfig
from slayer.database.client import SlayerSQLClient
from slayer.sql.column_sources import ColumnSource, column_sources
from slayer.sql.dialects.sqlite import SqliteDialect
from slayer.storage.sqlite_conn import transaction


def _sources(sql: str) -> list[ColumnSource]:
    return column_sources(sqlglot.parse_one(sql, dialect="sqlite"))


def _one(result: str, column: str, table: str = "clocks", schema: str | None = None) -> ColumnSource:
    return ColumnSource(table=table, schema_name=schema, result=result, column=column)


class TestColumnSources:
    @pytest.mark.parametrize(("sql", "expected"), [
        ("SELECT opens FROM clocks", [_one("opens", "opens")]),
        ("SELECT c.opens AS o FROM clocks AS c", [_one("o", "opens")]),
        ("SELECT opens FROM aux.clocks", [_one("opens", "opens", schema="aux")]),
        ("SELECT * FROM clocks", [ColumnSource(table="clocks")]),
        ("SELECT c.* FROM clocks AS c JOIN shops AS s ON s.id = c.id", [ColumnSource(table="clocks")]),
        ("SELECT o FROM (SELECT opens AS o FROM clocks) AS q", [_one("o", "opens")]),
        ("SELECT opens AS o FROM (SELECT * FROM clocks) AS q", [_one("o", "opens")]),
        ("SELECT * FROM (SELECT opens FROM clocks) AS q LIMIT 1", [_one("opens", "opens")]),
        ("WITH q AS (SELECT opens FROM clocks) SELECT opens AS o FROM q", [_one("o", "opens")]),
        ("SELECT opens AS o FROM (SELECT * FROM shops AS s JOIN clocks AS c ON s.id = c.id) AS q",
         [_one("o", "opens", table="shops"), _one("o", "opens")]),
        ("SELECT c.opens, s.id FROM clocks AS c JOIN shops AS s ON s.id = c.id",
         [_one("opens", "opens"), _one("id", "id", table="shops")]),
        ("WITH q(x) AS (SELECT opens FROM clocks) SELECT x FROM q", [_one("x", "opens")]),
        ("SELECT x FROM (SELECT opens FROM clocks) AS q(x)", [_one("x", "opens")]),
        ("WITH q(x, y) AS (SELECT id, opens FROM clocks) SELECT y AS o FROM q", [_one("o", "opens")]),
    ])
    def test_a_plain_column_read(self, sql: str, expected: list[ColumnSource]) -> None:
        assert _sources(sql) == expected

    @pytest.mark.parametrize("sql", [
        "SELECT upper(label) AS opens FROM clocks",
        "SELECT opens || '' AS opens FROM clocks",
        "SELECT opens FROM clocks UNION ALL SELECT opens FROM clocks",
        "SELECT opens FROM clocks AS c JOIN shops AS s ON s.id = c.id",
        "SELECT o FROM (SELECT upper(opens) AS o FROM clocks) AS q",
        "SELECT 1 AS opens",
        "WITH q(x) AS (SELECT * FROM clocks) SELECT x FROM q",
    ])
    def test_anything_else_has_no_source(self, sql: str) -> None:
        assert _sources(sql) == []


class TestSqliteDeclaredTypes:
    @pytest.fixture
    async def client(self, tmp_path: Path):
        path = str(tmp_path / "live.db")
        with transaction(path) as con:
            con.execute("CREATE TABLE clocks (id INTEGER PRIMARY KEY, opens TIME, closes TIME(3), zoned TIMETZ, "
                        "label TEXT, empty TIME)")
            con.execute("INSERT INTO clocks VALUES (1, '09:30:00', '17:00:00', '09:30:00+02', '10:30', NULL)")
            con.execute("CREATE TABLE shops (id INTEGER PRIMARY KEY, name TEXT)")
            con.execute("INSERT INTO shops VALUES (1, 'a')")
        client = SlayerSQLClient(datasource=DatasourceConfig(name="d", type="sqlite", database=path))
        try:
            yield client
        finally:
            await client.aclose()
            client.close()

    @pytest.mark.parametrize(("sql", "expected"), [
        ("SELECT opens, closes, zoned, label, empty FROM clocks",
         {"opens": "Time", "closes": "Time", "zoned": "Opaque", "label": "String", "empty": "Time"}),
        ("SELECT * FROM clocks",
         {"id": "Integer", "opens": "Time", "closes": "Time", "zoned": "Opaque", "label": "String", "empty": "Time"}),
        ("SELECT c.opens AS o, label AS opens FROM clocks AS c", {"o": "Time", "opens": "String"}),
        ("SELECT o FROM (SELECT opens AS o FROM clocks) AS q", {"o": "Time"}),
        ("SELECT opens || '' AS computed, label FROM clocks", {"computed": "String", "label": "String"}),
        ("SELECT opens FROM (SELECT * FROM shops AS s JOIN clocks AS c ON s.id = c.id) AS q", {"opens": "Time"}),
    ])
    async def test_a_statement_column(self, client: SlayerSQLClient, sql: str, expected: dict[str, str]) -> None:
        types = await client.get_result_column_types(sql)
        assert {name: found.type.value for name, found in types.items()} == expected

    async def test_candidates_that_disagree_keep_the_census_type(self, client: SlayerSQLClient,
                                                                 tmp_path: Path) -> None:
        with transaction(str(tmp_path / "live.db")) as con:
            con.execute("CREATE TABLE stalls (id INTEGER PRIMARY KEY, opens TEXT)")
            con.execute("INSERT INTO stalls VALUES (1, 'early')")
        sql = "SELECT opens AS o FROM (SELECT * FROM stalls AS s JOIN clocks AS c ON s.id = c.id) AS q"
        types = await client.get_result_column_types(sql)
        assert types["o"].type is DataType.STRING

    @pytest.mark.parametrize(("sql", "schema_args"), [
        ("SELECT opens FROM clocks", ["'clocks')"]), ("SELECT opens FROM aux.clocks", ["'clocks', 'aux')"]),
    ])
    def test_an_unqualified_table_is_resolved_as_the_statement_resolves_it(self, sql: str,
                                                                         schema_args: list[str]) -> None:
        probe = SqliteDialect().declared_types_select(_sources(sql))
        assert probe is not None
        assert [a for a in schema_args if a in probe.sql(dialect="sqlite")] == schema_args, probe.sql(dialect="sqlite")

    async def test_time_text_in_a_text_column_stays_text(self, client: SlayerSQLClient) -> None:
        types = await client.get_result_column_types("SELECT label FROM clocks")
        assert types["label"].type is DataType.STRING  # '10:30' text in a TEXT column stays text
