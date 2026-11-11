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
from slayer.core.query import SlayerQuery
from slayer.cube.models import CubeProject
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtProject
from slayer.dbt.parser import parse_dbt_project
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.importers.column_types import LiveDatasource, LiveSource
from slayer.ingest_report import ConversionResult
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.models import OSIDocument
from slayer.database.client import DelegatingSqlClient, ResultColumnType, SqlClient, TypeProbe
from slayer.sql.dialects import SqliteDialect
from slayer.storage.yaml_storage import YAMLStorage
from tests._dev2171_fixtures import collect_over_engine, column, typing_notes, write_yaml
from tests._engine_helpers import disposable_engine
from tests._osi_helpers import ds, f, flat, rel

pytest.importorskip("pytest_postgresql")

from pytest_postgresql import factories  # ALLOW(import-not-top): optional DB driver, gated by pytest.importorskip above

postgresql_proc = factories.postgresql_proc(port=None)
postgresql = factories.postgresql("postgresql_proc")

DS = "ds"
MIXED = '"Sales"."Order Items"'
UNQUOTED_MIXED = "Sales.OrderItems"
RESERVED = "order"
DOTTED = '"sales.v2".orders'

_MIXED_SEED = (
    'CREATE SCHEMA "Sales"',
    'CREATE TABLE "Sales"."Order Items" (qty INTEGER, placed_at TIMESTAMP)',
    "INSERT INTO \"Sales\".\"Order Items\" VALUES (3, '2026-01-01 10:00:00')",
    'CREATE TABLE "Sales"."OrderItems" (qty INTEGER)',
    'INSERT INTO "Sales"."OrderItems" VALUES (4)',
)
_DOTTED_SEED = (
    'CREATE SCHEMA "sales.v2"',
    'CREATE TABLE "sales.v2".orders (qty INTEGER)',
    'INSERT INTO "sales.v2".orders VALUES (5)',
)
_COMMON_SEED = ('CREATE TABLE "order" (id INTEGER, amount INTEGER)', 'INSERT INTO "order" VALUES (1, 10)')
_CATALOG_SEED = ("CREATE SCHEMA sales", "CREATE TABLE sales.orders (qty INTEGER)", "INSERT INTO sales.orders VALUES (7)")
# Postgres folds an unquoted ``Sales.OrderItems`` / ``OrderItems`` to these decoys.
_DECOY_SEED = (
    "CREATE TABLE sales.orderitems (qty TEXT)", "INSERT INTO sales.orderitems VALUES ('t')",
    'CREATE TABLE "OrderItems" (qty INTEGER)', 'INSERT INTO "OrderItems" VALUES (4)',
    "CREATE TABLE orderitems (qty TEXT)", "INSERT INTO orderitems VALUES ('t')",
    'CREATE TABLE payments (id INTEGER, orderamount INTEGER, "OrderAmount" TEXT)',
    "INSERT INTO payments VALUES (1, 7, 'x')",
    "CREATE TABLE custs (id INTEGER, region TEXT)", "INSERT INTO custs VALUES (1, 'n')",
    "CREATE TABLE ords (id INTEGER, cust_id INTEGER)", "INSERT INTO ords VALUES (10, 1)",
)


class Live:
    """A seeded live database: its engine, the statements run on it, and the catalog it reaches."""

    def __init__(self, *, engine: sa.Engine, datasource: DatasourceConfig, catalog: str | None,
                 schemas: bool) -> None:
        self.engine = engine
        self.datasource = datasource
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
    shop = tmp_path / "shop.duckdb"
    with disposable_engine(f"duckdb:///{shop}") as engine:
        _run(engine, (*(() if decoy else _MIXED_SEED), *_DOTTED_SEED, *_COMMON_SEED))

        @event.listens_for(engine, "connect")
        def _attach(dbapi_connection: Any, _record: Any) -> None:
            dbapi_connection.execute(f"ATTACH '{analytics}' AS analytics (READ_ONLY)")
            if decoy:
                dbapi_connection.execute(f"ATTACH '{sales}' AS sales (READ_ONLY)")

        engine.dispose()
        yield Live(engine=engine, datasource=DatasourceConfig(name=DS, type="duckdb", database=str(shop)),
                   catalog="analytics", schemas=True)


@contextlib.contextmanager
def _sqlite(tmp_path: Path) -> Generator[Live]:
    shop = tmp_path / "shop.db"
    with disposable_engine(f"sqlite:///{shop}") as engine:
        _run(engine, ('CREATE TABLE "Order Items" (qty INTEGER, placed_at TIMESTAMP)',
                      "INSERT INTO \"Order Items\" VALUES (3, '2026-01-01 10:00:00')", *_COMMON_SEED))
        yield Live(engine=engine, datasource=DatasourceConfig(name=DS, type="sqlite", database=str(shop)),
                   catalog=None, schemas=False)


@contextlib.contextmanager
def _postgres(request: pytest.FixtureRequest) -> Generator[Live]:
    conn = request.getfixturevalue("postgresql")
    cur = conn.cursor()
    for statement in (*_MIXED_SEED, *_DOTTED_SEED, *_COMMON_SEED, *_CATALOG_SEED, *_DECOY_SEED):
        cur.execute(statement)
    conn.commit()
    info = conn.info
    datasource = DatasourceConfig(name=DS, type="postgres", host=info.host, port=info.port, database=info.dbname,
                                  username=info.user, password="")
    with disposable_engine(datasource.get_connection_string()) as engine:
        yield Live(engine=engine, datasource=datasource, catalog=info.dbname, schemas=True)


@pytest.fixture(params=["sqlite", "duckdb", "postgres"])
def live(request: pytest.FixtureRequest, tmp_path: Path) -> Generator[Live]:
    backends = {"sqlite": lambda: _sqlite(tmp_path), "duckdb": lambda: _duckdb(tmp_path),
                "postgres": lambda: _postgres(request)}
    with backends[request.param]() as found:
        yield found


@pytest.fixture
def pg(request: pytest.FixtureRequest) -> Generator[Live]:
    with _postgres(request) as found:
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
    converter = CubeToSlayerConverter(project=project, data_source=DS)
    collect_over_engine(collect=lambda client: converter.collect_metadata(client=client), engine=live.engine)
    result = converter.convert()
    return {m.name: m for m in result.models}, [i.message for i in result.report.issues]


def _dbt(live: Live, semantic_models: list[dict[str, Any]],
         regular_models: Iterable[dict[str, Any]] = ()) -> tuple[dict[str, SlayerModel], list[str]]:
    project = DbtProject.model_validate({"semantic_models": semantic_models, "regular_models": list(regular_models)})
    converter = DbtToSlayerConverter(project=project, data_source=DS, target_dialect=live.engine.dialect.name)
    collect_over_engine(collect=lambda client: converter.collect_metadata(client=client), engine=live.engine)
    result = converter.convert()
    return {m.name: m for m in result.models}, _messages(result)


def _osi(live: Live, document: dict[str, Any]) -> tuple[dict[str, SlayerModel], list[str]]:
    converter = OsiToSlayerConverter(documents=[OSIDocument.model_validate(document)], data_source=DS)
    collect_over_engine(collect=lambda client: converter.collect_metadata(client=client), engine=live.engine)
    result = converter.convert()
    return {m.name: m for m in result.models}, _messages(result)


def _messages(result: ConversionResult) -> list[str]:
    return [e.message for e in (*result.warnings, *result.unconverted_metrics)]


def _typed(model: SlayerModel, name: str) -> DataType:
    return column(model, name).type


def _probed(live: Live) -> DataType:
    """An integer probe's type: ``INT`` on every backend (the typed probe certifies integers)."""
    return DataType.INT


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


async def _rows(live: Live, tmp_path: Path, models: Iterable[SlayerModel],
                payload: dict[str, Any]) -> list[dict[str, Any]]:
    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    await storage.save_datasource(live.datasource)
    for model in models:
        await storage.save_model(model)
    live.engine.dispose()  # SLayer opens a DuckDB file read-only, which a held read-write connection refuses
    engine = SlayerQueryEngine(storage=storage)
    try:
        return (await engine.execute(SlayerQuery.model_validate(payload))).data
    finally:
        await engine.aclose()


async def _grouped(live: Live, tmp_path: Path, models: dict[str, SlayerModel], model: str, dimension: str) -> list[Any]:
    rows = await _rows(live, tmp_path, models.values(), {"source_model": model, "dimensions": [dimension]})
    return [r[f"{model}.{dimension}"] for r in rows]


async def test_unquoted_mixed_case_table_types_the_table_queries_read(live: Live, tmp_path: Path) -> None:
    """Postgres folds an unquoted ``Sales.OrderItems`` to ``sales.orderitems``; DuckDB reads it case-insensitively."""
    _schemas(live)
    models, messages = _cube(live, _qty_cube(UNQUOTED_MIXED))
    folds = live.engine.dialect.name == "postgresql"
    assert _typed(models["items"], "qty") is (DataType.TEXT if folds else DataType.INT)
    assert typing_notes(messages) == []
    assert await _grouped(live, tmp_path, models, "items", "qty") == (["t"] if folds else [4])


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

#: The connectivity check ``LiveDatasource.collect`` runs first.
_PING = "SELECT 1"


class _ProbeRecorder(DelegatingSqlClient):
    """Forwards everything, recording each type probe it is handed."""

    def __init__(self, *, inner: SqlClient, probes: list[TypeProbe]) -> None:
        super().__init__(inner=inner)
        self._probes = probes

    async def get_result_column_types(self, sql: str) -> dict[str, ResultColumnType]:
        assert isinstance(sql, TypeProbe)
        self._probes.append(sql)
        return await self.inner.get_result_column_types(sql)


def _probe_statements(live: Live, probe: TypeProbe, *, columns: list[str]) -> list[str]:
    """``probe`` run verbatim, followed on SQLite by exactly its integer-certifying census."""
    census = probe.census_sql(columns, dialect=SqliteDialect()) if live.engine.dialect.name == "sqlite" else None
    return [probe.sql, *([census] if census is not None else [])]


class _Collected:
    """One source read through ``LiveDatasource.collect`` over the live engine, a probe collected on demand."""

    def __init__(self, live: Live, *, sql_table: str | None, sql: str | None) -> None:
        self._live, self._sql_table, self._sql = live, sql_table, sql
        #: The type probes handed to the client, in order.
        self.probes: list[TypeProbe] = []

    def read(self) -> LiveSource | str:
        """The collected source, or why it could not be read."""
        return self._read(None)

    def _read(self, expr: str | None) -> LiveSource | str:
        name = self._live.engine.dialect.name
        datasource = LiveDatasource(offline_reason=None, data_source=DS, db_type=name,
                                    dialect="postgres" if name == "postgresql" else name)

        def convert() -> None:
            found = datasource.source(sql_table=self._sql_table, sql=self._sql)
            if expr is not None and isinstance(found, LiveSource):
                found.probe(expr)

        collect_over_engine(collect=lambda client: datasource.collect(
            client=_ProbeRecorder(inner=client, probes=self.probes), convert=convert), engine=self._live.engine)
        return datasource.source(sql_table=self._sql_table, sql=self._sql)

    def probe(self, expr: str) -> tuple[DataType, DataType | None] | None:
        found = self._read(expr)
        return found.probe(expr) if isinstance(found, LiveSource) else None

    def column(self, physical: str) -> tuple[DataType, DataType | None] | None:
        found = self._read(None)
        return found.column(physical) if isinstance(found, LiveSource) else None


def _source(live: Live, *, sql_table: str | None = None, sql: str | None = None) -> _Collected:
    return _Collected(live, sql_table=sql_table, sql=sql)


def test_probe_over_a_table_source_selects_from_that_table(live: Live) -> None:
    source = _source(live, sql_table=live.mixed)
    assert source.probe("qty * 2") == (_probed(live), None)
    (probe,) = [p for p in source.probes if "_slayer_probe" in p]
    assert live.ran("_slayer_probe") == _probe_statements(live, probe, columns=["_slayer_probe"])
    assert live.mixed in probe


def test_probe_over_a_sql_source_selects_from_its_subquery(live: Live) -> None:
    source = _source(live, sql=f"SELECT qty FROM {live.mixed};")
    assert source.probe("qty * 2") == (_probed(live), None)
    (probe,) = [p for p in source.probes if "_slayer_probe" in p]
    assert live.ran("_slayer_probe") == _probe_statements(live, probe, columns=["_slayer_probe"])
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
    selects = [s for s in live.statements if s.lstrip().upper().startswith("SELECT") and s != _PING]
    (describe,) = source.probes
    assert selects == _probe_statements(live, describe, columns=["qty", "placed_at"])


@pytest.mark.parametrize("sql", ["SELECT ((( not valid", 'DELETE FROM "order" RETURNING id'])
def test_unreadable_or_modifying_sql_source_is_refused_before_execution(live: Live, sql: str) -> None:
    found = _source(live, sql=sql).read()
    assert isinstance(found, str)
    assert "could not be read" in found
    assert found.endswith((NotReadOnlyQueryError.__name__, ParseError.__name__)), found
    assert [s for s in live.statements if s != _PING] == []


# ── Postgres folds unquoted identifiers (spec: importers/meaning-preservation, importers/dbt-import) ──

def _importer_models(live: Live, importer: str, table: str) -> tuple[dict[str, SlayerModel], str]:
    """``(models, model name)`` of an import whose one model reads ``qty`` from ``table``."""
    if importer == "cube":
        return _cube(live, _qty_cube(table))[0], "items"
    if importer == "dbt":
        regular = {"name": "src", "raw_code": f"SELECT qty FROM {table}"}
        return _dbt(live, [{"name": "m", "model": "src", "dimensions": [{"name": "qty", "type": "categorical"}]}],
                    regular_models=[regular])[0], "m"
    return _osi(live, flat([ds("items", source=table, fields=[f("qty")])]))[0], "items"


@pytest.mark.parametrize("importer", ["cube", "dbt", "osi"])
@pytest.mark.parametrize(("table", "typed", "value"), [
    (UNQUOTED_MIXED, DataType.TEXT, "t"),
    ('"Sales"."OrderItems"', DataType.INT, 4),
])
async def test_postgres_reads_a_source_table_with_its_folding(pg: Live, tmp_path: Path, importer: str, table: str,
                                                              typed: DataType, value: Any) -> None:
    models, name = _importer_models(pg, importer, table)
    sql_source_integer = importer == "dbt" and typed is DataType.INT
    assert _typed(models[name], "qty") is (_probed(pg) if sql_source_integer else typed)
    assert await _grouped(pg, tmp_path, models, name, "qty") == [value]


async def test_mixed_case_column_reads_the_folded_physical_column(pg: Live, tmp_path: Path) -> None:
    models, _ = _cube(pg, {"name": "payments", "sql_table": "payments", "dimensions": [
        {"name": "id", "sql": "{CUBE}.id", "type": "number", "primary_key": True},
        {"name": "amount", "sql": "{CUBE}.OrderAmount", "type": "number"}],
        "measures": [{"name": "total", "type": "sum", "sql": "{CUBE}.OrderAmount"}]})
    assert _typed(models["payments"], "amount") is DataType.INT
    assert await _grouped(pg, tmp_path, models, "payments", "amount") == [7]
    (row,) = await _rows(pg, tmp_path / "measure", models.values(), {
        "source_model": "payments", "measures": [{"formula": "total", "name": "v"}]})
    assert row["payments.v"] == 7


async def test_names_local_to_a_source_query(pg: Live, tmp_path: Path) -> None:
    models, _ = _cube(pg, {"name": "recent", "dimensions": [{"name": "total", "sql": "{CUBE}.total", "type": "number"}],
                           "sql": "WITH Recent AS (SELECT Qty AS Total FROM Sales.OrderItems) "
                                  "SELECT Total FROM Recent ORDER BY Total"})
    assert _typed(models["recent"], "total") is DataType.TEXT
    assert await _grouped(pg, tmp_path, models, "recent", "total") == ["t"]


async def test_osi_reference_to_another_datasets_field(pg: Live, tmp_path: Path) -> None:
    models, messages = _osi(pg, flat(
        [ds("ords", fields=[f("id"), f("cust_id"), f("up", "UPPER(Customers.Region)", datatype="String")]),
         ds("Customers", source="custs", fields=[f("id"), f("Region", "region")])],
        relationships=[rel("ords_customers", src="ords", to="Customers", from_columns=["cust_id"],
                           to_columns=["id"])]))
    assert [m for m in messages if "'up'" in m] == []
    assert await _grouped(pg, tmp_path, models, "ords", "up") == ["N"]


def _dbt_files(live: Live, tmp_path: Path, *, quoting: dict[str, Any] | None, include_hidden: bool = False,
               ) -> dict[str, SlayerModel]:
    root = tmp_path / "dbt"
    write_yaml(root / "dbt_project.yml", {"name": "shop", "version": "1.0", "config-version": 2,
                                          **({"quoting": quoting} if quoting is not None else {})})
    schema: dict[str, Any] = {"version": 2, "models": [{"name": "OrderItems"}]} if include_hidden else {
        "version": 2, "semantic_models": [{"name": "items", "model": "ref('OrderItems')",
                                           "dimensions": [{"name": "qty", "type": "categorical"}]}]}
    write_yaml(root / "models" / "schema.yml", schema)
    converter = DbtToSlayerConverter(project=parse_dbt_project(str(root)), data_source=DS,
                                     include_hidden_models=include_hidden, target_dialect="postgres")
    collect_over_engine(collect=lambda client: converter.collect_metadata(client=client), engine=live.engine)
    result = converter.convert()
    return {m.name: m for m in result.models}


@pytest.mark.parametrize(("quoting", "typed", "value"), [
    (None, DataType.INT, 4),
    ({"identifier": False}, DataType.TEXT, "t"),
])
async def test_dbt_relation_follows_the_quoting_policy(pg: Live, tmp_path: Path, quoting: dict[str, Any] | None,
                                                       typed: DataType, value: Any) -> None:
    models = _dbt_files(pg, tmp_path, quoting=quoting)
    assert _typed(models["items"], "qty") is typed
    assert await _grouped(pg, tmp_path, models, "items", "qty") == [value]


@pytest.mark.parametrize(("quoting", "typed"), [(None, DataType.INT), ({"identifier": False}, DataType.TEXT)])
def test_dbt_hidden_model_lookup_follows_the_quoting_policy(pg: Live, tmp_path: Path, quoting: dict[str, Any] | None,
                                                           typed: DataType) -> None:
    models = _dbt_files(pg, tmp_path, quoting=quoting, include_hidden=True)
    assert _typed(models["OrderItems"], "qty") is typed
