"""A SQLite database holding every catalog shape SQLite key and column reads meet."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy import event

from slayer.core.models import DatasourceConfig
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from slayer.sql.dialects.reflection import TableLocation
from slayer.storage.sqlite_conn import transaction

ATTACHED = "other"

MAIN_DDL = """
CREATE TABLE parents (id INTEGER PRIMARY KEY, label TEXT);
CREATE TABLE implicit_child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents);
CREATE TABLE comp_pk (a INTEGER, b TEXT, v REAL, PRIMARY KEY (a, b));
CREATE TABLE comp_child (id INTEGER PRIMARY KEY, ca INTEGER, cb TEXT, FOREIGN KEY (ca, cb) REFERENCES comp_pk (a, b));
CREATE TABLE no_rowid (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID;
CREATE TABLE uniq_c (id INTEGER PRIMARY KEY, email TEXT UNIQUE, x INTEGER, y INTEGER, UNIQUE (x, y));
CREATE TABLE uniq_i (id INTEGER PRIMARY KEY, code TEXT, flag INTEGER, code2 TEXT, name TEXT, tag TEXT);
CREATE UNIQUE INDEX ux_code ON uniq_i (code);
CREATE UNIQUE INDEX ux_partial ON uniq_i (flag) WHERE flag IS NOT NULL;
CREATE UNIQUE INDEX ux_mixed ON uniq_i (code2, lower(name));
CREATE INDEX ix_tag ON uniq_i (tag);
CREATE TABLE keyless (a INTEGER, b TEXT);
CREATE VIEW v_keyless AS SELECT a, b FROM keyless;
CREATE TABLE gen (
    id INTEGER PRIMARY KEY, a INTEGER,
    twice INTEGER GENERATED ALWAYS AS (a * 2) VIRTUAL,
    shout TEXT GENERATED ALWAYS AS (upper(a)) STORED
);
CREATE TABLE affinity (
    c_int INT, c_integer INTEGER, c_tinyint TINYINT, c_bigint BIGINT, c_unsigned UNSIGNED BIG INT,
    c_varchar VARCHAR(20), c_nchar NCHAR(5), c_text TEXT, c_clob CLOB, c_blob BLOB, c_none,
    c_real REAL, c_double DOUBLE, c_double_precision DOUBLE PRECISION, c_float FLOAT,
    c_numeric NUMERIC, c_decimal DECIMAL(10, 5), c_boolean BOOLEAN, c_date DATE, c_datetime DATETIME,
    c_timestamp TIMESTAMP, c_floating_point FLOATING POINT, c_charint CHARINT, c_string STRING, c_xyz XYZ
);
CREATE VIRTUAL TABLE docs USING fts5(title, body);
"""

ATTACHED_DDL = """
CREATE TABLE comp_pk (x INTEGER PRIMARY KEY, a INTEGER, b TEXT UNIQUE);
"""

TEMP_DDL = "CREATE TEMP TABLE IF NOT EXISTS tmp_keys (id INTEGER PRIMARY KEY, code TEXT UNIQUE, parent_id INTEGER)"

MAIN_TABLES = (
    "parents", "implicit_child", "comp_pk", "comp_child", "no_rowid", "uniq_c", "uniq_i", "keyless", "gen",
    "affinity", "docs",
)
VIEW = "v_keyless"
VIRTUAL = "docs"
MISSING = "no_such_table"


class Corpus(BaseModel):
    datasource: DatasourceConfig
    attached_path: str


def build_corpus(directory: Path) -> Corpus:
    main, attached = directory / "corpus.db", directory / "attached.db"
    for path, ddl in ((main, MAIN_DDL), (attached, ATTACHED_DDL)):
        with transaction(str(path)) as conn:
            conn.executescript(ddl)
    return Corpus(
        datasource=DatasourceConfig(name="corpus", type="sqlite", database=str(main)), attached_path=str(attached),
    )


def corpus_locations(*, schema: str | None) -> list[TableLocation]:
    """Every main-database table and the view, under ``schema`` (``None`` or ``main``)."""
    return [TableLocation(name=n, schema_name=schema) for n in (*MAIN_TABLES, VIEW)]


@asynccontextmanager
async def corpus_client(corpus: Corpus) -> AsyncGenerator[tuple[SlayerSQLClient, sa.Engine]]:
    """A default client whose every connection attaches ``other`` and holds the temp table."""
    ds = corpus.datasource
    engine_factory.invalidate_engine(ds)
    engine = engine_factory.get_engine(ds)

    @event.listens_for(engine, "connect")
    def _attach(dbapi_conn: Any, _rec: Any) -> None:
        dbapi_conn.execute(f"ATTACH DATABASE '{corpus.attached_path}' AS {ATTACHED}")
        dbapi_conn.execute(TEMP_DDL)

    client = SlayerSQLClient(datasource=ds)
    try:
        yield client, engine
    finally:
        await client.aclose()
        client.close()
        engine_factory.invalidate_engine(ds)


@contextmanager
def statement_errors(engine: sa.Engine) -> Generator[list[str]]:
    """Every statement that fails on ``engine`` while the block runs."""
    failed: list[str] = []

    def _record(context: Any) -> None:
        failed.append(f"{context.statement}: {context.original_exception}")

    event.listen(engine, "handle_error", _record)
    try:
        yield failed
    finally:
        event.remove(engine, "handle_error", _record)
