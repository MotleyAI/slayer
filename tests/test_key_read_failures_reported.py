"""A lost key read is listed on the key read, the ingest results and every ingest surface; a re-ingest fills it."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import duckdb
import pytest
import sqlalchemy as sa
from fastapi.testclient import TestClient
from sqlalchemy import event

from slayer.api.server import create_app
from slayer.async_utils import run_sync
from slayer.cli import _run_ingest
from slayer.core.enums import JoinCardinality
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.database import engine_factory
from slayer.database.client import SlayerSQLClient
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema import ingestion
from slayer.live_schema.ingestion import IngestionScanReport, ingest_datasource_idempotent
from slayer.live_schema.schema_drift import IdempotentIngestResult
from slayer.mcp.server import create_mcp_server
from slayer.sql.dialects.reflection import TableKeys, TableLocation
from slayer.storage.sqlite_conn import transaction
from slayer.storage.yaml_storage import YAMLStorage
from tests._gateway_harness import DS_NAME, seed_shop, shop_datasource
from tests._key_read_failure import LOST, fail_key_read


@pytest.fixture
def shop(tmp_path: Path) -> Generator[DatasourceConfig]:
    db = str(tmp_path / "shop.db")
    seed_shop(db)
    ds = shop_datasource(db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


@pytest.fixture
async def storage(tmp_path: Path, shop: DatasourceConfig) -> YAMLStorage:
    store = YAMLStorage(base_dir=str(tmp_path / "store"))
    await store.save_datasource(shop)
    return store


@contextlib.asynccontextmanager
async def _client(ds: DatasourceConfig) -> AsyncGenerator[SlayerSQLClient]:
    client = SlayerSQLClient(datasource=ds)
    try:
        yield client
    finally:
        await client.aclose()
        client.close()


async def _scan(ds: DatasourceConfig, **kwargs: Any) -> IngestionScanReport:
    async with _client(ds) as client:
        return await ingestion.ingest_datasource_report_async(ds, client=client, **kwargs)


async def _ingest(ds: DatasourceConfig, storage: YAMLStorage) -> IdempotentIngestResult:
    engine = SlayerQueryEngine(storage=storage)
    try:
        return await ingest_datasource_idempotent(datasource=ds, storage=storage, client=engine.client_for(ds))
    finally:
        await engine.aclose()


def _lost_reads(entries: list[Any], *, table: str = "customers") -> list[Any]:
    return [e for e in entries if e.location.name == table]


def _assert_one_lost_primary_key(entries: list[Any], *, table: str = "customers", schema: str | None = "main") -> None:
    assert len(entries) == 1, entries
    entry = entries[0]
    assert (entry.location.name, entry.location.schema_name, entry.read) == (table, schema, "primary key")
    assert LOST in entry.error


def _pk(model: SlayerModel) -> list[str]:
    return [c.name for c in model.columns if c.primary_key]


def _cardinality(model: SlayerModel, target: str) -> JoinCardinality | None:
    return next(j.cardinality for j in model.joins if j.target_model == target)


def _naming_the_loss(text: str) -> list[str]:
    return [line for line in text.splitlines() if "customers" in line and "primary key" in line.lower() and LOST in line]


# ---------------------------------------------------------------------------
# The key read
# ---------------------------------------------------------------------------

# What each read of ``t`` yields when it succeeds.
_KEYS_BY_READ: dict[str, Any] = {
    "primary key": ["id"],
    "foreign keys": [(["parent_id"], "parent")],
    "unique constraints": ["code"],
    "unique indexes": ["alt"],
}


def _key_shape(keys: TableKeys) -> dict[str, Any]:
    """Each read's keys, ``None`` where it yielded nothing."""
    unique = {tuple(s) for s in keys.unique_key_sets}
    shape = {
        "primary key": keys.primary_key,
        "foreign keys": [(fk.columns, fk.referred_table) for fk in keys.foreign_keys],
        "unique constraints": ["code"] if ("code",) in unique else [],
        "unique indexes": ["alt"] if ("alt",) in unique else [],
    }
    return {read: value or None for read, value in shape.items()}


class TestKeyRead:
    @pytest.fixture
    def keyed(self, tmp_path: Path) -> Generator[DatasourceConfig]:
        db = tmp_path / "keyed.db"
        with transaction(str(db)) as conn:
            conn.executescript(
                """
                CREATE TABLE parent (id INTEGER PRIMARY KEY);
                CREATE TABLE t (
                    id INTEGER PRIMARY KEY, code TEXT UNIQUE, alt TEXT, parent_id INTEGER REFERENCES parent(id)
                );
                CREATE UNIQUE INDEX ux_alt ON t (alt);
                """
            )
        ds = DatasourceConfig(name="keyed", type="sqlite", database=str(db))
        try:
            yield ds
        finally:
            engine_factory.invalidate_engine(ds)

    @pytest.mark.parametrize("read", list(_KEYS_BY_READ))
    async def test_the_default_client_lists_a_failed_read(
        self, keyed: DatasourceConfig, monkeypatch: pytest.MonkeyPatch, read: str,
    ) -> None:
        fail_key_read(monkeypatch, table="t", reads=(read,))
        async with _client(keyed) as client:
            keys = await client.reflect_table_keys(TableLocation(name="t"))
        assert _key_shape(keys) == {r: (None if r == read else shape) for r, shape in _KEYS_BY_READ.items()}
        assert [f.read for f in keys.failed_reads] == [read]
        assert LOST in keys.failed_reads[0].error

    async def test_a_clean_read_lists_nothing(self, keyed: DatasourceConfig) -> None:
        async with _client(keyed) as client:
            keys = await client.reflect_table_keys(TableLocation(name="t"))
        assert _key_shape(keys) == _KEYS_BY_READ
        assert keys.failed_reads == []


# ---------------------------------------------------------------------------
# Ingest results
# ---------------------------------------------------------------------------


class _PreChangeGateway:
    """A gateway client whose key read lists no failed read."""

    def __init__(self, inner: SlayerSQLClient) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def reflect_table_keys(self, location: TableLocation) -> TableKeys:
        keys = await self._inner.reflect_table_keys(location)
        return TableKeys(
            primary_key=keys.primary_key, foreign_keys=keys.foreign_keys, unique_key_sets=keys.unique_key_sets,
        )


class _RepeatingGateway(_PreChangeGateway):
    """A gateway client that lists every failed read twice."""

    async def reflect_table_keys(self, location: TableLocation) -> TableKeys:
        keys = await self._inner.reflect_table_keys(location)
        return keys.model_copy(update={"failed_reads": [*keys.failed_reads, *keys.failed_reads]})


class TestIngestResults:
    async def test_the_scan_reports_the_lost_read(self, shop: DatasourceConfig, monkeypatch: pytest.MonkeyPatch) -> None:
        fail_key_read(monkeypatch, table="customers")
        report = await _scan(shop)
        _assert_one_lost_primary_key(report.key_read_failures)
        models = {m.name: m for m in report.models}
        assert report.skipped == []
        assert _pk(models["customers"]) == []
        assert _cardinality(models["orders"], "customers") is None

    async def test_the_idempotent_ingest_reports_the_lost_read(
        self, shop: DatasourceConfig, storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fail_key_read(monkeypatch, table="customers")
        result = await _ingest(shop, storage)
        _assert_one_lost_primary_key(result.key_read_failures)
        customers = await storage.get_model("customers", data_source=DS_NAME)
        assert customers is not None
        assert _pk(customers) == []

    async def test_a_table_later_skipped_is_still_reported(
        self, shop: DatasourceConfig, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fail_key_read(monkeypatch, table="customers")
        real = ingestion._columns_to_model

        def _refuse_customers(*args: Any, **kwargs: Any) -> SlayerModel:
            if kwargs.get("sql_table") == "customers":
                raise ValueError("synthetic per-object failure")
            return real(*args, **kwargs)

        monkeypatch.setattr(ingestion, "_columns_to_model", _refuse_customers)
        report = await _scan(shop)
        assert [s.table_name for s in report.skipped] == ["customers"]
        _assert_one_lost_primary_key(report.key_read_failures)

    async def test_same_named_tables_in_two_schemas_are_reported_apart(self, tmp_path: Path) -> None:
        path = tmp_path / "two.duckdb"
        with contextlib.closing(duckdb.connect(str(path))) as con:
            for schema in ("a", "b"):
                con.execute(f"CREATE SCHEMA {schema}")
                con.execute(f"CREATE TABLE {schema}.orders (id INTEGER PRIMARY KEY, amount DOUBLE)")
        ds = DatasourceConfig(name="two", type="duckdb", database=str(path))

        def _fail_a(_conn: Any, _cursor: Any, statement: str, *_rest: Any) -> None:
            low = statement.lower()
            if "duckdb_constraints" in low and "'orders'" in low and "'a'" in low and "primary key" in low:
                raise SystemError(LOST)

        event.listen(sa.engine.Engine, "before_cursor_execute", _fail_a)
        try:
            report = await _scan(ds, schemas=["a", "b"])
        finally:
            event.remove(sa.engine.Engine, "before_cursor_execute", _fail_a)
            engine_factory.invalidate_engine(ds)
        _assert_one_lost_primary_key(report.key_read_failures, table="orders", schema="a")

    async def test_each_location_and_read_is_reported_once(
        self, shop: DatasourceConfig, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fail_key_read(monkeypatch, table="customers", reads=("primary key", "unique indexes"))
        async with _client(shop) as inner:
            report = await ingestion.ingest_datasource_report_async(shop, client=cast(Any, _RepeatingGateway(inner)))
        lost = sorted((e.location.name, e.location.schema_name, e.read) for e in report.key_read_failures)
        assert lost == [("customers", "main", "primary key"), ("customers", "main", "unique indexes")]

    async def test_a_client_listing_nothing_reports_nothing(
        self, shop: DatasourceConfig, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fail_key_read(monkeypatch, table="customers")
        async with _client(shop) as inner:
            gateway = cast(Any, _PreChangeGateway(inner))
            report = await ingestion.ingest_datasource_report_async(shop, client=gateway)
        assert report.key_read_failures == []
        assert {"customers", "orders"} <= {m.name for m in report.models}

    async def test_a_reingest_fills_the_lost_key(
        self, shop: DatasourceConfig, storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with monkeypatch.context() as failing:
            fail_key_read(failing, table="customers")
            await _ingest(shop, storage)
        result = await _ingest(shop, storage)
        assert result.key_read_failures == []
        customers = await storage.get_model("customers", data_source=DS_NAME)
        orders = await storage.get_model("orders", data_source=DS_NAME)
        assert customers is not None
        assert orders is not None
        assert _pk(customers) == ["id"]
        assert _cardinality(orders, "customers") is JoinCardinality.MANY_TO_ONE


# ---------------------------------------------------------------------------
# Surfaces: each names the loss; statuses are those of a clean ingest
# ---------------------------------------------------------------------------


def _cli_ingest(store: Path, capsys: pytest.CaptureFixture[str]) -> tuple[int | str | None, str]:
    args = SimpleNamespace(
        datasource=DS_NAME, schema=None, include=None, exclude=None, include_views=True, surface_internals=False,
        storage=str(store), models_dir=None,
    )
    code: int | str | None = None
    try:
        _run_ingest(args)
    except SystemExit as exc:
        code = exc.code
    return code, capsys.readouterr().out


class TestSurfaces:
    async def test_mcp_create_datasource(
        self, tmp_path: Path, shop: DatasourceConfig, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fail_key_read(monkeypatch, table="customers")
        server = create_mcp_server(storage=YAMLStorage(base_dir=str(tmp_path / "mcp_store")))
        blocks, _ = cast(Any, await server.call_tool(
            name="create_datasource", arguments={"name": DS_NAME, "type": "sqlite", "database": shop.database},
        ))
        assert len(_naming_the_loss(blocks[0].text)) == 1, blocks[0].text

    async def test_mcp_ingest_datasource_models(
        self, storage: YAMLStorage, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fail_key_read(monkeypatch, table="customers")
        server = create_mcp_server(storage=storage)
        blocks, _ = cast(Any, await server.call_tool(
            name="ingest_datasource_models", arguments={"datasource_name": DS_NAME},
        ))
        assert len(_naming_the_loss(blocks[0].text)) == 1, blocks[0].text

    def test_cli(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
        clean_db, lossy_db = tmp_path / "clean.db", tmp_path / "lossy.db"
        outcomes = []
        for db, store in ((clean_db, tmp_path / "clean_store"), (lossy_db, tmp_path / "lossy_store")):
            seed_shop(str(db))
            ds = shop_datasource(str(db))
            run_sync(YAMLStorage(base_dir=str(store)).save_datasource(ds))
            with monkeypatch.context() as patch:
                if db is lossy_db:
                    fail_key_read(patch, table="customers")
                try:
                    outcomes.append(_cli_ingest(store, capsys))
                finally:
                    engine_factory.invalidate_engine(ds)
        (clean_code, _), (lossy_code, lossy_out) = outcomes
        assert lossy_code == clean_code
        assert len(_naming_the_loss(lossy_out)) == 1, lossy_out

    def test_rest(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        statuses, bodies = [], []
        for lossy in (False, True):
            db = tmp_path / f"rest_{lossy}.db"
            seed_shop(str(db))
            ds = shop_datasource(str(db))
            store = YAMLStorage(base_dir=str(tmp_path / f"rest_store_{lossy}"))
            run_sync(store.save_datasource(ds))
            with monkeypatch.context() as patch:
                if lossy:
                    fail_key_read(patch, table="customers")
                try:
                    with TestClient(create_app(storage=store)) as http:
                        response = http.post("/ingest", json={"datasource": DS_NAME})
                finally:
                    engine_factory.invalidate_engine(ds)
            statuses.append(response.status_code)
            bodies.append(response.json())
        assert statuses[1] == statuses[0]
        entries = _lost_reads(
            [SimpleNamespace(location=SimpleNamespace(**e["location"]), **{k: e[k] for k in ("read", "error")})
             for e in bodies[1]["key_read_failures"]],
        )
        _assert_one_lost_primary_key(entries)
