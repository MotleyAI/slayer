"""Importers type columns from sources named with the source dialect's identifier rules (spec:
importers/meaning-preservation › Column types come from the database first)."""

from __future__ import annotations

import contextlib
from collections.abc import Generator, Iterable
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy import event
from sqlglot.errors import ParseError

from slayer.core.enums import DataType
from slayer.core.errors import NotReadOnlyQueryError
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.cube.converter import CubeToSlayerConverter
from slayer.cube.models import CubeProject
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtProject
from slayer.importers.column_types import LiveSource
from slayer.ingest_report import ConversionResult
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.models import OSIDocument
from tests._dev2171_fixtures import column, typing_notes
from tests._engine_helpers import disposable_engine
from tests._osi_helpers import ds, f, flat

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

DS = "ds"
MIXED = '"Sales"."Order Items"'
RESERVED = "order"
DOTTED = '"sales.v2".orders'

_MIXED_SEED = (
    'CREATE SCHEMA "Sales"',
    'CREATE TABLE "Sales"."Order Items" (qty INTEGER, placed_at TIMESTAMP)',
    "INSERT INTO \"Sales\".\"Order Items\" VALUES (3, '2026-01-01 10:00:00')",
)
_DOTTED_SEED = (
    'CREATE SCHEMA "sales.v2"',
    'CREATE TABLE "sales.v2".orders (qty INTEGER)',
    'INSERT INTO "sales.v2".orders VALUES (5)',
)
_COMMON_SEED = ('CREATE TABLE "order" (id INTEGER, amount INTEGER)', 'INSERT INTO "order" VALUES (1, 10)')
_CATALOG_SEED = ("CREATE SCHEMA sales", "CREATE TABLE sales.orders (qty INTEGER)", "INSERT INTO sales.orders VALUES (7)")


class Live:
    """A seeded live database: its engine, the statements run on it, and the catalog it reaches."""

    def __init__(self, *, engine: sa.Engine, catalog: str | None, schemas: bool) -> None:
        self.engine = engine
        self.catalog = catalog
        self.schemas = schemas
        self.statements: list[str] = []
        event.listen(engine, "before_cursor_execute",
                     lambda _c, _cur, statement, *_a: self.statements.append(statement))

    @property
    def mixed(self) -> str:
        """The mixed-case table name this backend can hold."""
        return MIXED if self.schemas else '"Order Items"'

    def ran(self, fragment: str) -> list[str]:
        return [s for s in self.statements if fragment in s]


def _run(engine: sa.Engine, statements: Iterable[str]) -> None:
    with engine.connect() as conn:
        for statement in statements:
            conn.execute(sa.text(statement))
        conn.commit()


@contextlib.contextmanager
def _duckdb(tmp_path: Path, *, decoy: bool = False) -> Generator[Live]:
    """DuckDB with catalog ``analytics``; ``decoy`` attaches catalog ``sales`` (holding ``v2.orders``) instead
    of the ``"Sales"`` schema, which DuckDB's case-insensitive names would make ambiguous."""
    analytics, sales = tmp_path / "analytics.duckdb", tmp_path / "sales.duckdb"
    with disposable_engine(f"duckdb:///{analytics}") as engine:
        _run(engine, _CATALOG_SEED)
    with disposable_engine(f"duckdb:///{sales}") as engine:
        _run(engine, ("CREATE SCHEMA v2", "CREATE TABLE v2.orders (qty VARCHAR)", "INSERT INTO v2.orders VALUES ('x')"))
    with disposable_engine(f"duckdb:///{tmp_path / 'shop.duckdb'}") as engine:
        _run(engine, (*(() if decoy else _MIXED_SEED), *_DOTTED_SEED, *_COMMON_SEED))

        @event.listens_for(engine, "connect")
        def _attach(dbapi_connection: Any, _record: Any) -> None:
            dbapi_connection.execute(f"ATTACH '{analytics}' AS analytics (READ_ONLY)")
            if decoy:
                dbapi_connection.execute(f"ATTACH '{sales}' AS sales (READ_ONLY)")

        engine.dispose()
        yield Live(engine=engine, catalog="analytics", schemas=True)


@contextlib.contextmanager
def _sqlite(tmp_path: Path) -> Generator[Live]:
    with disposable_engine(f"sqlite:///{tmp_path / 'shop.db'}") as engine:
        _run(engine, ('CREATE TABLE "Order Items" (qty INTEGER, placed_at TIMESTAMP)',
                      "INSERT INTO \"Order Items\" VALUES (3, '2026-01-01 10:00:00')", *_COMMON_SEED))
        yield Live(engine=engine, catalog=None, schemas=False)


@contextlib.contextmanager
def _postgres(request: pytest.FixtureRequest) -> Generator[Live]:
    conn = request.getfixturevalue("postgresql")
    cur = conn.cursor()
    for statement in (*_MIXED_SEED, *_DOTTED_SEED, *_COMMON_SEED, *_CATALOG_SEED):
        cur.execute(statement)
    conn.commit()
    info = conn.info
    url = DatasourceConfig(name=DS, type="postgres", host=info.host, port=info.port, database=info.dbname,
                           username=info.user, password="").get_connection_string()
    with disposable_engine(url) as engine:
        yield Live(engine=engine, catalog=info.dbname, schemas=True)


@pytest.fixture(params=["sqlite", "duckdb", "postgres"])
def live(request: pytest.FixtureRequest, tmp_path: Path) -> Generator[Live]:
    backends = {"sqlite": lambda: _sqlite(tmp_path), "duckdb": lambda: _duckdb(tmp_path),
                "postgres": lambda: _postgres(request)}
    with backends[request.param]() as found:
        yield found


@pytest.fixture
def duck(tmp_path: Path) -> Generator[Live]:
    with _duckdb(tmp_path, decoy=True) as found:
        yield found


def _schemas(live: Live) -> None:
    if not live.schemas:
        pytest.skip("SQLite has no schemas")


# ── importers ─────────────────────────────────────────────────────────────

def _cube(live: Live, *cubes: dict[str, Any]) -> tuple[dict[str, SlayerModel], list[str]]:
    project = CubeProject.model_validate({"cubes": list(cubes)})
    result = CubeToSlayerConverter(project=project, data_source=DS, sa_engine=live.engine).convert()
    return {m.name: m for m in result.models}, [i.message for i in result.report.issues]


def _dbt(live: Live, semantic_models: list[dict[str, Any]],
         regular_models: Iterable[dict[str, Any]] = ()) -> tuple[dict[str, SlayerModel], list[str]]:
    project = DbtProject.model_validate({"semantic_models": semantic_models, "regular_models": list(regular_models)})
    result = DbtToSlayerConverter(project=project, data_source=DS, sa_engine=live.engine,
                                  target_dialect=live.engine.dialect.name).convert()
    return {m.name: m for m in result.models}, _messages(result)


def _osi(live: Live, document: dict[str, Any]) -> tuple[dict[str, SlayerModel], list[str]]:
    result = OsiToSlayerConverter(documents=[OSIDocument.model_validate(document)], data_source=DS,
                                  sa_engine=live.engine).convert()
    return {m.name: m for m in result.models}, _messages(result)


def _messages(result: ConversionResult) -> list[str]:
    return [e.message for e in (*result.warnings, *result.unconverted_metrics)]


def _typed(model: SlayerModel, name: str) -> DataType:
    return column(model, name).type


def _probed(live: Live) -> DataType:
    """An integer probe's type: ``INT`` where the driver names types (DuckDB), else its numeric category."""
    return DataType.INT if live.engine.dialect.name == "duckdb" else DataType.DOUBLE


def _items_cube(sql_table: str) -> dict[str, Any]:
    return {"name": "items", "sql_table": sql_table, "dimensions": [
        {"name": "qty", "sql": "{CUBE}.qty", "type": "number"},
        {"name": "placed_at", "sql": "{CUBE}.placed_at", "type": "time"},
    ]}


def _qty_cube(sql_table: str) -> dict[str, Any]:
    return {"name": "items", "sql_table": sql_table,
            "dimensions": [{"name": "qty", "sql": "{CUBE}.qty", "type": "number"}]}


# ── spec scenarios ────────────────────────────────────────────────────────

def test_mixed_case_schema_qualified_table(live: Live) -> None:
    models, messages = _cube(live, _items_cube(live.mixed))
    assert _typed(models["items"], "qty") is DataType.INT
    assert _typed(models["items"], "placed_at") is DataType.TIMESTAMP
    assert typing_notes(messages) == []


def test_reserved_word_table_cube(live: Live) -> None:
    models, messages = _cube(live, {"name": "orders", "sql_table": RESERVED,
                                    "dimensions": [{"name": "amount", "sql": "{CUBE}.amount", "type": "number"}]})
    assert _typed(models["orders"], "amount") is DataType.INT
    assert typing_notes(messages) == []


def test_reserved_word_table_dbt(live: Live) -> None:
    models, messages = _dbt(live, [{"name": "orders", "model": RESERVED,
                                    "dimensions": [{"name": "amount", "type": "categorical"}]}])
    assert _typed(models["orders"], "amount") is DataType.INT
    assert typing_notes(messages) == []


def test_catalog_qualified_table(live: Live) -> None:
    if live.catalog is None:
        pytest.skip("SQLite reaches no catalog")
    models, messages = _cube(live, _qty_cube(f"{live.catalog}.sales.orders"))
    assert _typed(models["items"], "qty") is DataType.INT
    assert typing_notes(messages) == []


def test_unreachable_catalog_takes_the_reason_path(live: Live) -> None:
    models, messages = _dbt(live, [{"name": "m", "model": "elsewhere.sales.orders",
                                    "dimensions": [{"name": "qty", "type": "categorical"}]}])
    assert _typed(models["m"], "qty") is not DataType.INT
    assert len(typing_notes(messages)) == 1


def test_quoted_name_containing_a_dot(live: Live) -> None:
    _schemas(live)
    models, messages = _cube(live, _qty_cube(DOTTED))
    assert _typed(models["items"], "qty") is DataType.INT
    assert typing_notes(messages) == []


def test_quoted_dotted_schema_is_not_a_three_part_name(duck: Live) -> None:
    """``sales.v2.orders`` (catalog ``sales``) holds a text ``qty``; ``"sales.v2".orders`` an integer one."""
    dotted, _ = _cube(duck, _qty_cube(DOTTED))
    three_part, _ = _cube(duck, _qty_cube("sales.v2.orders"))
    assert _typed(dotted["items"], "qty") is DataType.INT
    assert _typed(three_part["items"], "qty") is DataType.TEXT


def _doubled(model: str, *, dimension: str = "doubled", expr: str = "qty * 2") -> dict[str, Any]:
    return {"name": "m", "model": model, "dimensions": [{"name": dimension, "type": "categorical", "expr": expr}]}


def test_expression_probe_over_a_mixed_case_table(live: Live) -> None:
    models, messages = _dbt(live, [_doubled(live.mixed)])
    assert _typed(models["m"], "doubled") is _probed(live)
    assert typing_notes(messages) == []


def test_expression_probe_over_a_reserved_word_table(live: Live) -> None:
    models, _ = _dbt(live, [_doubled(RESERVED, expr="amount * 2")])
    assert _typed(models["m"], "doubled") is _probed(live)


@pytest.mark.parametrize("source_sql", [
    'SELECT qty FROM {mixed}',
    'SELECT amount AS qty FROM "order"',
])
def test_expression_probe_over_sql_selecting_from_a_quoted_table(live: Live, source_sql: str) -> None:
    regular = {"name": "src", "raw_code": source_sql.format(mixed=live.mixed)}
    models, messages = _dbt(live, [_doubled("src")], regular_models=[regular])
    assert _typed(models["m"], "doubled") is _probed(live)
    assert typing_notes(messages) == []


def test_cube_expression_probe_over_a_reserved_word_table(live: Live) -> None:
    models, _ = _cube(live, {"name": "orders", "sql_table": RESERVED,
                             "dimensions": [{"name": "doubled", "sql": "{CUBE}.amount * 2", "type": "string"}]})
    assert _typed(models["orders"], "doubled") is _probed(live)


def test_osi_expression_probe_over_a_reserved_word_table(live: Live) -> None:
    models, _ = _osi(live, flat([ds("orders", source=RESERVED, fields=[f("id"), f("doubled", "amount * 2", datatype="String")])]))
    assert _typed(models["orders"], "doubled") is _probed(live)


def test_osi_expression_probe_over_a_mixed_case_table(live: Live) -> None:
    models, _ = _osi(live, flat([ds("items", source=live.mixed, fields=[f("qty"), f("doubled", "qty * 2", datatype="String")])]))
    assert _typed(models["items"], "doubled") is _probed(live)


def test_osi_sql_source_selecting_from_a_reserved_word_table(live: Live) -> None:
    models, _ = _osi(live, flat([ds("orders", source='SELECT id, amount FROM "order"',
                                    fields=[f("id"), f("amount"), f("doubled", "amount * 2", datatype="String")])]))
    assert _typed(models["orders"], "doubled") is _probed(live)


@pytest.mark.parametrize("source_sql", [
    "SELECT ((( not valid",
    "SELECT id INTO archived FROM \"order\"",
    "DELETE FROM \"order\" RETURNING id",
])
def test_source_sql_the_importer_cannot_read(live: Live, source_sql: str) -> None:
    regular = {"name": "src", "raw_code": source_sql}
    models, messages = _dbt(live, [{"name": "m", "model": "src", "dimensions": [
        {"name": "id", "type": "categorical"}],
        "measures": [{"name": "total", "agg": "sum", "expr": "id"}]}], regular_models=[regular])
    assert _typed(models["m"], "id_col") is DataType.DOUBLE
    assert len(typing_notes(messages)) == 1
    assert live.ran("_slayer_src") == []


# ── LiveSource ────────────────────────────────────────────────────────────

def _source(live: Live, *, sql_table: str | None = None, sql: str | None = None) -> LiveSource:
    name = live.engine.dialect.name
    return LiveSource(engine=live.engine, data_source=DS, db_type=name,
                      dialect="postgres" if name == "postgresql" else name, sql_table=sql_table, sql=sql)


def test_probe_over_a_table_source_selects_from_that_table(live: Live) -> None:
    source = _source(live, sql_table=live.mixed)
    assert source.probe("qty * 2") == (_probed(live), None)
    (probe,) = live.ran("_slayer_probe")
    assert live.mixed in probe


def test_probe_over_a_sql_source_selects_from_its_subquery(live: Live) -> None:
    source = _source(live, sql=f"SELECT qty FROM {live.mixed};")
    assert source.probe("qty * 2") == (_probed(live), None)
    (probe,) = live.ran("_slayer_probe")
    assert "_slayer_src" in probe
    assert live.mixed in probe


def test_unreadable_probe_expression_is_none_without_a_round_trip(live: Live) -> None:
    source = _source(live, sql_table=live.mixed)
    assert source.probe("qty +* ((") is None
    assert live.ran("_slayer_probe") == []


def test_statement_keyword_function_still_probes(live: Live) -> None:
    """``REPLACE(...)`` reads as a statement on SQLite unless parsed in expression context."""
    source = _source(live, sql_table=RESERVED)
    assert source.probe("REPLACE(CAST(amount AS VARCHAR), '1', '2')") == (DataType.TEXT, None)


def test_sql_source_is_described_by_one_statement(live: Live) -> None:
    source = _source(live, sql=f"SELECT qty, placed_at FROM {live.mixed}")
    assert source.column("qty") in ((DataType.INT, None), (DataType.DOUBLE, None))
    assert len([s for s in live.statements if s.lstrip().upper().startswith("SELECT")]) == 1


@pytest.mark.parametrize("sql", ["SELECT ((( not valid", 'DELETE FROM "order" RETURNING id'])
def test_unreadable_or_modifying_sql_source_is_refused_before_execution(live: Live, sql: str) -> None:
    with pytest.raises((NotReadOnlyQueryError, ParseError)):  # LiveDatasource.source turns either into a reason
        _source(live, sql=sql)
    assert live.statements == []
