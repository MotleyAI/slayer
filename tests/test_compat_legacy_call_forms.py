"""SLayer 1.5.0 Python call forms keep working with one FutureWarning; the engine-supplied forms replace them.

Spec: compatibility/legacy-call-forms. Legacy calls are written as the 1.5.0 docs, examples and notebooks wrote them.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import inspect
import sys
import tempfile
import warnings
from collections.abc import Callable, Coroutine, Generator, Iterator
from pathlib import Path
from typing import Any, TypeVar

import pytest
import sqlalchemy as sa
from pydantic import BaseModel, ConfigDict, Field

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtProject, DbtRegularModel
from slayer.engine import ingestion, schema_drift
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ingest_report import ConversionResult
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.models import OSIDocument
from slayer.sql import engine_factory
from slayer.sql.client import SlayerSQLClient
from slayer.storage.yaml_storage import YAMLStorage
from tests._compat_helpers import assert_one_deprecation, future_warnings, next_line, recorded
from tests._engine_helpers import disposable_engine
from tests._gateway_harness import (
    DS_NAME,
    SHOP_DDL,
    GatewayFactory,
    backend_of,
    no_direct_access,
    orders_model,
    seed_shop,
    shop_datasource,
    shop_storage,
)
from tests._osi_helpers import run_converter, seed_db
from tests._stored_upgrade_fixtures import raw_store

_T = TypeVar("_T")

ORDERS_PROJECT = DbtProject(
    semantic_models=[], metrics=[],
    regular_models=[DbtRegularModel(name="orders", schema_name="main", alias="orders", description="Orders")],
)
ORDERS_OSI = OSIDocument.model_validate({
    "version": "0.2.0.dev0", "name": "shop",
    "datasets": [{
        "name": "orders", "source": "orders",
        "fields": [{"name": "status", "expression": {"sql": "status"}},
                   {"name": "big", "expression": {"sql": "amount * 2"}}],
    }],
})
V7_ORDERS = {
    "version": 7, "name": "orders", "sql_table": "orders", "data_source": DS_NAME,
    "columns": [
        {"name": "id", "type": "INT", "primary_key": True},
        {"name": "quantity", "type": "DOUBLE"},
        {"name": "amount", "type": "DOUBLE"},
    ],
}
COUNT_ORDERS = {"source_model": "orders", "measures": [{"formula": "*:count", "name": "n"}]}

INGEST = ("ingest_datasource()", ("engine.ingest_sync(", "engine.ingest(", "client=engine.client_for("))
REPORT = ("ingest_datasource_report()", ("ingest_datasource_report_async(", "client=engine.client_for("))
IDEMPOTENT = ("ingest_datasource_idempotent()", ("engine.ingest_idempotent", "client=engine.client_for("))
VALIDATE = ("validate_datasource()", ("client=engine.client_for(",))
COLLECT = ("collect_metadata(client=",)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _assert_one_deprecation(
    caught: list[warnings.WarningMessage], *, call: str, remedies: tuple[str, ...], line: int | None = None,
) -> None:
    assert_one_deprecation(caught, call=call, remedies=remedies, filename=__file__, line=line)


def _dumps(models: list[SlayerModel]) -> list[dict]:
    return [m.model_dump(mode="json") for m in sorted(models, key=lambda m: m.name)]


@contextlib.contextmanager
def _engine(base: Path, *, datasource: DatasourceConfig, **kwargs: Any) -> Generator[SlayerQueryEngine]:
    storage = YAMLStorage(base_dir=str(base))
    run_sync(storage.save_datasource(datasource))
    engine = SlayerQueryEngine(storage=storage, **kwargs)
    try:
        yield engine
    finally:
        engine.close()


def _stored(engine: SlayerQueryEngine) -> list[dict]:
    models, _ = engine.run_sync(engine.storage.load_models(data_source=DS_NAME))
    return _dumps(models)


def _with_client(
    datasource: DatasourceConfig, work: Callable[[SlayerQueryEngine], Coroutine[Any, Any, _T]],
) -> _T:
    """``work`` run through a throwaway engine's own client: the engine-supplied reference form."""
    with tempfile.TemporaryDirectory() as tmp, _engine(Path(tmp), datasource=datasource) as engine:
        return engine.run_sync(work(engine))


def _ghost_model() -> SlayerModel:
    return SlayerModel(
        name="ghost", sql_table="ghost", data_source=DS_NAME,
        columns=[Column(name="id", sql="id", type=DataType.INT, primary_key=True)],
    )


class _Ledger(BaseModel):
    """Query engines and SQL clients built and closed while installed."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    engines: list[Any] = Field(default_factory=list)
    closed_engines: list[Any] = Field(default_factory=list)
    clients: list[Any] = Field(default_factory=list)
    closed_clients: list[Any] = Field(default_factory=list)

    def all_closed(self) -> bool:
        return all(any(e is c for c in self.closed_engines) for e in self.engines) and all(
            any(cl is c for c in self.closed_clients) for cl in self.clients
        )


@pytest.fixture
def ledger(monkeypatch: pytest.MonkeyPatch) -> _Ledger:
    led = _Ledger()
    real_init, real_close = SlayerQueryEngine.__init__, SlayerQueryEngine.close
    real_client_init, real_client_close = SlayerSQLClient.__init__, SlayerSQLClient.close

    def _init(self: SlayerQueryEngine, *args: Any, **kwargs: Any) -> None:
        real_init(self, *args, **kwargs)
        led.engines.append(self)

    def _close(self: SlayerQueryEngine) -> None:
        led.closed_engines.append(self)
        real_close(self)

    def _client_init(self: SlayerSQLClient, *args: Any, **kwargs: Any) -> None:
        real_client_init(self, *args, **kwargs)
        led.clients.append(self)

    def _client_close(self: SlayerSQLClient) -> None:
        led.closed_clients.append(self)
        real_client_close(self)

    monkeypatch.setattr(SlayerQueryEngine, "__init__", _init)
    monkeypatch.setattr(SlayerQueryEngine, "close", _close)
    monkeypatch.setattr(SlayerSQLClient, "__init__", _client_init)
    monkeypatch.setattr(SlayerSQLClient, "close", _client_close)
    return led


@pytest.fixture(params=["sqlite", "duckdb"])
def shop(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[DatasourceConfig]:
    """The gateway shop database, file-backed."""
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    path = seed_db(directory=db_dir, dialect=request.param, statements=SHOP_DDL)
    ds = DatasourceConfig(name=DS_NAME, type=request.param, database=str(path))
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


@pytest.fixture
def sqlite_shop(tmp_path: Path) -> Iterator[DatasourceConfig]:
    db = str(tmp_path / "shop.db")
    seed_shop(db)
    ds = shop_datasource(db)
    try:
        yield ds
    finally:
        engine_factory.invalidate_engine(ds)


# --------------------------------------------------------------------------- #
# Ingestion
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("scope", [{}, {"schema": "main"}], ids=["examples", "getting-started"])
def test_sync_ingest_datasource_as_1_5_0_wrote_it(shop: DatasourceConfig, scope: dict) -> None:
    expected = _with_client(shop, lambda e: ingestion.ingest_datasource_async(shop, client=e.client_for(shop), **scope))
    with recorded() as caught:
        line = next_line()
        models = ingestion.ingest_datasource(datasource=shop, **scope)
    assert _dumps(models) == _dumps(expected)
    _assert_one_deprecation(caught, call=INGEST[0], remedies=INGEST[1], line=line)


def test_sync_ingest_datasource_report_as_1_5_0_wrote_it(shop: DatasourceConfig) -> None:
    expected = _with_client(shop, lambda e: ingestion.ingest_datasource_report_async(shop, client=e.client_for(shop)))
    with recorded() as caught:
        line = next_line()
        report = ingestion.ingest_datasource_report(shop)
    assert report.model_dump(mode="json") == expected.model_dump(mode="json")
    _assert_one_deprecation(caught, call=REPORT[0], remedies=REPORT[1], line=line)


def test_sync_ingestion_keeps_its_1_5_0_positional_order(sqlite_shop: DatasourceConfig) -> None:
    with recorded() as caught:
        models = ingestion.ingest_datasource(sqlite_shop, ["orders"])
        report = ingestion.ingest_datasource_report(sqlite_shop, None, ["orders"])
    assert [m.name for m in models] == ["orders"]
    assert "orders" not in {m.name for m in report.models}
    assert len(future_warnings(caught)) == 2


def test_idempotent_ingestion_as_1_5_0_wrote_it(shop: DatasourceConfig, tmp_path: Path) -> None:
    legacy_store = YAMLStorage(base_dir=str(tmp_path / "legacy"))
    run_sync(legacy_store.save_datasource(shop))
    with recorded() as caught:
        line = next_line()
        result = run_sync(ingestion.ingest_datasource_idempotent(datasource=shop, storage=legacy_store))
    with _engine(tmp_path / "reference", datasource=shop) as engine:
        expected = engine.run_sync(ingestion.ingest_datasource_idempotent(
            datasource=shop, storage=engine.storage, client=engine.client_for(shop),
        ))
        expected_stored = _stored(engine)
    with _engine(tmp_path / "legacy", datasource=shop) as reader:
        stored = _stored(reader)
    assert result.model_dump(mode="json") == expected.model_dump(mode="json")
    assert stored == expected_stored
    _assert_one_deprecation(caught, call=IDEMPOTENT[0], remedies=IDEMPOTENT[1], line=line)


def test_the_idempotent_fallback_loads_models_needing_refinement(shop: DatasourceConfig, tmp_path: Path) -> None:
    legacy_store = run_sync(raw_store(backend="yaml", base=str(tmp_path / "legacy"), datasource=shop, models=[V7_ORDERS]))
    with recorded() as caught:
        result = run_sync(ingestion.ingest_datasource_idempotent(datasource=shop, storage=legacy_store))
    reference_store = run_sync(raw_store(backend="yaml", base=str(tmp_path / "ref"), datasource=shop, models=[V7_ORDERS]))
    engine = SlayerQueryEngine(storage=reference_store)
    try:
        expected = engine.run_sync(ingestion.ingest_datasource_idempotent(
            datasource=shop, storage=engine.storage, client=engine.client_for(shop),
        ))
        expected_stored = _stored(engine)
    finally:
        engine.close()
    with _engine(tmp_path / "legacy", datasource=shop) as reader:
        stored = _stored(reader)
    assert result.errors == []
    assert result.model_dump(mode="json") == expected.model_dump(mode="json")
    assert stored == expected_stored
    assert len(future_warnings(caught)) == 1


# --------------------------------------------------------------------------- #
# Drift validation
# --------------------------------------------------------------------------- #
async def test_validate_datasource_without_a_client(shop: DatasourceConfig, tmp_path: Path) -> None:
    models = [orders_model(), _ghost_model()]
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path / "store")))
    try:
        expected = await schema_drift.validate_datasource(datasource=shop, models=models, client=engine.client_for(shop))
    finally:
        await engine.aclose()
        engine.close()
    with recorded() as caught:
        line = next_line()
        entries = await schema_drift.validate_datasource(datasource=shop, models=models)
    assert [e.model_dump(mode="json") for e in entries] == [e.model_dump(mode="json") for e in expected]
    assert "ghost" in {e.model_name for e in entries}
    _assert_one_deprecation(caught, call=VALIDATE[0], remedies=VALIDATE[1], line=line)


@pytest.mark.parametrize("with_client", [False, True], ids=["no-client", "client"])
async def test_validate_datasource_ignores_legacy_sql_clients(
    sqlite_shop: DatasourceConfig, tmp_path: Path, with_client: bool,
) -> None:
    models = [orders_model(), _ghost_model()]
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path / "store")))
    try:
        client = engine.client_for(sqlite_shop)
        expected = await schema_drift.validate_datasource(datasource=sqlite_shop, models=models, client=client)
        extra: dict[str, Any] = {"client": client} if with_client else {}
        with recorded() as caught:
            entries = await schema_drift.validate_datasource(
                datasource=sqlite_shop, models=models, sql_clients={}, **extra,
            )
    finally:
        await engine.aclose()
        engine.close()
    assert [e.model_dump(mode="json") for e in entries] == [e.model_dump(mode="json") for e in expected]
    _assert_one_deprecation(caught, call=VALIDATE[0], remedies=VALIDATE[1])


# --------------------------------------------------------------------------- #
# Converters
# --------------------------------------------------------------------------- #
def _legacy_converter(kind: str, *, sa_engine: sa.Engine, dialect: str, keyword: bool = False) -> Any:
    """Constructed as 1.5.0 code did: ``sa_engine`` third positionally, or by keyword."""
    if kind == "dbt":
        if keyword:
            return DbtToSlayerConverter(
                project=ORDERS_PROJECT, data_source=DS_NAME, sa_engine=sa_engine, include_hidden_models=True,
            )
        return DbtToSlayerConverter(ORDERS_PROJECT, DS_NAME, sa_engine, include_hidden_models=True)
    if keyword:
        return OsiToSlayerConverter(
            documents=[ORDERS_OSI], data_source=DS_NAME, sa_engine=sa_engine, target_dialect=dialect,
        )
    return OsiToSlayerConverter([ORDERS_OSI], DS_NAME, sa_engine, target_dialect=dialect)


def _client_converter(kind: str, *, dialect: str | None) -> Any:
    if kind == "dbt":
        return DbtToSlayerConverter(project=ORDERS_PROJECT, data_source=DS_NAME, include_hidden_models=True)
    return OsiToSlayerConverter(documents=[ORDERS_OSI], data_source=DS_NAME, target_dialect=dialect)


def _converter_name(kind: str) -> str:
    return "DbtToSlayerConverter" if kind == "dbt" else "OsiToSlayerConverter"


def _reference(kind: str, *, dialect: str, tmp_path: Path) -> tuple[ConversionResult, Path]:
    """The client-based conversion over a file-backed twin of the shop database."""
    db_dir = tmp_path / f"ref-{dialect}"
    db_dir.mkdir()
    path = seed_db(directory=db_dir, dialect=dialect, statements=SHOP_DDL)
    result = run_converter(
        _client_converter(kind, dialect=dialect),
        datasource=DatasourceConfig(name=DS_NAME, type=dialect, database=str(path)),
    )
    return result, path


def _seed(sa_engine: sa.Engine) -> None:
    with sa_engine.connect() as conn:
        for statement in SHOP_DDL:
            conn.execute(sa.text(statement))
        conn.commit()


def _order_count(sa_engine: sa.Engine) -> int:
    with sa_engine.connect() as conn:
        return conn.execute(sa.text("SELECT count(*) FROM orders")).scalar_one()


async def _convert_in_loop(converter: Any) -> ConversionResult:
    return converter.convert()


@pytest.mark.parametrize("kind", ["dbt", "osi"])
@pytest.mark.parametrize("in_loop", [False, True], ids=["sync", "running-loop"])
@pytest.mark.parametrize("keyword", [False, True], ids=["positional", "keyword"])
def test_converter_over_an_in_memory_engine(kind: str, in_loop: bool, keyword: bool, tmp_path: Path) -> None:
    expected, _ = _reference(kind, dialect="sqlite", tmp_path=tmp_path)
    with disposable_engine("sqlite://") as sa_engine:
        _seed(sa_engine)
        with recorded() as caught:
            converter = _legacy_converter(kind, sa_engine=sa_engine, dialect="sqlite", keyword=keyword)
            result = asyncio.run(_convert_in_loop(converter)) if in_loop else converter.convert()
    assert result.models
    assert result.model_dump(mode="json") == expected.model_dump(mode="json")
    _assert_one_deprecation(caught, call=_converter_name(kind), remedies=COLLECT)


@pytest.mark.parametrize("kind", ["dbt", "osi"])
def test_converter_over_a_file_backed_duckdb_engine(kind: str, tmp_path: Path) -> None:
    expected, path = _reference(kind, dialect="duckdb", tmp_path=tmp_path)
    with disposable_engine(f"duckdb:///{path}") as sa_engine, recorded() as caught:
        result = _legacy_converter(kind, sa_engine=sa_engine, dialect="duckdb").convert()
    assert result.models
    assert result.model_dump(mode="json") == expected.model_dump(mode="json")
    assert len(future_warnings(caught)) == 1


def _spy_ownership(monkeypatch: pytest.MonkeyPatch, sa_engine: sa.Engine) -> list[str]:
    """Record SLayer disposing, invalidating or draining ``sa_engine``."""
    calls: list[str] = []

    def _record(name: str, real: Callable[..., Any], *, engine_arg: bool = False) -> Callable[..., Any]:
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            if not engine_arg or (args and args[0] is sa_engine):
                calls.append(name)
            return real(*args, **kwargs)

        return _wrapped

    monkeypatch.setattr(sa_engine, "dispose", _record("dispose", sa_engine.dispose))
    monkeypatch.setattr(sa_engine.pool, "dispose", _record("pool.dispose", sa_engine.pool.dispose))
    monkeypatch.setattr(
        engine_factory, "release_idle", _record("release_idle", engine_factory.release_idle, engine_arg=True),
    )
    monkeypatch.setattr(engine_factory, "invalidate_engine", _record("invalidate_engine", engine_factory.invalidate_engine))
    return calls


_AUTH_REFUSAL = "invalid_grant: token has been expired or revoked"


@pytest.mark.parametrize("kind", ["dbt", "osi"])
@pytest.mark.parametrize("auth_failure", [False, True], ids=["success", "auth-failure"])
def test_the_callers_engine_survives(kind: str, auth_failure: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    refusals: list[str] = []

    def _refuse(*args: Any, **kwargs: Any) -> Any:
        refusals.append("connect")
        raise RuntimeError(_AUTH_REFUSAL)

    with disposable_engine("sqlite://") as sa_engine:
        _seed(sa_engine)
        calls = _spy_ownership(monkeypatch, sa_engine)
        with recorded():
            converter = _legacy_converter(kind, sa_engine=sa_engine, dialect="sqlite")
            if auth_failure:
                with pytest.MonkeyPatch.context() as mp:
                    mp.setattr(sa_engine, "connect", _refuse)
                    try:
                        result = converter.convert()
                    except Exception as exc:  # noqa: BLE001 — a refused conversion may raise or report
                        assert _AUTH_REFUSAL in str(exc)
                    else:
                        assert any(_AUTH_REFUSAL in w.message for w in result.warnings)
            else:
                converter.convert()
        during_conversion = list(calls)
        count = _order_count(sa_engine)
    assert during_conversion == []
    assert count == 4
    assert bool(refusals) == auth_failure


def test_a_caller_engine_client_never_disposes_the_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    with disposable_engine("sqlite://") as sa_engine:
        _seed(sa_engine)
        calls = _spy_ownership(monkeypatch, sa_engine)
        datasource = DatasourceConfig(name=DS_NAME, type="sqlite", database=":memory:")
        client = SlayerSQLClient(datasource=datasource, sa_engine=sa_engine)  # type: ignore[call-arg]
        rows = run_sync(client.execute("SELECT count(*) AS n FROM orders")).rows
        run_sync(client.aclose())
        client.close()
        del client
        gc.collect()
        during_lifecycle = list(calls)
        count = _order_count(sa_engine)
    assert rows == [{"n": 4}]
    assert during_lifecycle == []
    assert count == 4


# --------------------------------------------------------------------------- #
# New forms
# --------------------------------------------------------------------------- #
def test_client_supplied_forms_are_silent(shop: DatasourceConfig, tmp_path: Path) -> None:
    with _engine(tmp_path / "store", datasource=shop) as engine:
        client = engine.client_for(shop)
        with recorded() as caught:
            ingestion.ingest_datasource(shop, client=client)
            ingestion.ingest_datasource_report(shop, client=client)
            engine.run_sync(ingestion.ingest_datasource_async(shop, client=client))
            engine.run_sync(ingestion.ingest_datasource_report_async(shop, client=client))
            engine.run_sync(ingestion.ingest_datasource_idempotent(datasource=shop, storage=engine.storage, client=client))
            engine.run_sync(schema_drift.validate_datasource(datasource=shop, models=[orders_model()], client=client))
            for kind in ("dbt", "osi"):
                converter = _client_converter(kind, dialect=shop.type)
                engine.run_sync(converter.collect_metadata(client=client))
                converter.convert()
    assert future_warnings(caught) == []


def test_the_async_ingestion_names_require_a_client(sqlite_shop: DatasourceConfig) -> None:
    with pytest.raises(TypeError):
        ingestion.ingest_datasource_async(sqlite_shop)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        ingestion.ingest_datasource_report_async(sqlite_shop)  # type: ignore[call-arg]


def test_a_client_survives_a_sync_ingestion(shop: DatasourceConfig, tmp_path: Path) -> None:
    with _engine(tmp_path / "store", datasource=shop) as engine:
        client = engine.client_for(shop)
        models = ingestion.ingest_datasource(shop, client=client)
        for model in models:
            engine.run_sync(engine.storage.save_model(model))
        response = engine.execute_sync(COUNT_ORDERS)
        assert engine.client_for(shop) is client
    assert response.data == [{"orders.n": 4}]


@pytest.mark.skipif(sys.version_info < (3, 12), reason="inspect.markcoroutinefunction is 3.12+")
def test_the_async_entry_points_still_read_as_coroutine_functions() -> None:
    assert inspect.iscoroutinefunction(ingestion.ingest_datasource_idempotent)
    assert inspect.iscoroutinefunction(schema_drift.validate_datasource)
    assert not inspect.iscoroutinefunction(ingestion.ingest_datasource)
    assert not inspect.iscoroutinefunction(ingestion.ingest_datasource_report)


# --------------------------------------------------------------------------- #
# Engine ingestion methods
# --------------------------------------------------------------------------- #
def test_engine_ingestion_one_liners_equal_the_free_functions(shop: DatasourceConfig, tmp_path: Path) -> None:
    with _engine(tmp_path / "methods", datasource=shop) as engine:
        expected = engine.run_sync(ingestion.ingest_datasource_async(shop, client=engine.client_for(shop), schema="main"))
        with recorded() as caught:
            models = engine.ingest_sync(shop, schema="main")
            awaited = engine.run_sync(engine.ingest(shop, schema="main"))
            result = engine.ingest_idempotent_sync(shop)
        stored = _stored(engine)
    with _engine(tmp_path / "free", datasource=shop) as twin:
        expected_result = twin.run_sync(ingestion.ingest_datasource_idempotent(
            datasource=shop, storage=twin.storage, client=twin.client_for(shop),
        ))
        expected_stored = _stored(twin)
    assert _dumps(models) == _dumps(expected) == _dumps(awaited)
    assert result.model_dump(mode="json") == expected_result.model_dump(mode="json")
    assert stored == expected_stored
    assert future_warnings(caught) == []


async def test_engine_ingest_idempotent_reads_through_its_client_factory(
    sqlite_shop: DatasourceConfig, tmp_path: Path,
) -> None:
    factory = GatewayFactory()
    engine = SlayerQueryEngine(
        storage=await shop_storage(str(tmp_path / "store"), datasource=sqlite_shop), client_factory=factory,
    )
    try:
        with no_direct_access(backends=[backend_of(sqlite_shop)]) as guard, recorded() as caught:
            result = await engine.ingest_idempotent(sqlite_shop)
    finally:
        await engine.aclose()
        engine.close()
    assert guard.attempts == []
    assert factory.built == [sqlite_shop]
    assert factory.all_calls()
    assert result.errors == []
    assert result.additions
    assert future_warnings(caught) == []


# --------------------------------------------------------------------------- #
# Fallback engines
# --------------------------------------------------------------------------- #
_ASYNC_NO_CLIENT: dict[str, Callable[[DatasourceConfig, Any], Coroutine[Any, Any, Any]]] = {
    "idempotent": lambda ds, store: ingestion.ingest_datasource_idempotent(datasource=ds, storage=store),
    "validate": lambda ds, _store: schema_drift.validate_datasource(datasource=ds, models=[orders_model()]),
}
_NO_CLIENT: dict[str, Callable[[DatasourceConfig, Any], Any]] = {
    "ingest": lambda ds, _store: ingestion.ingest_datasource(ds),
    "report": lambda ds, _store: ingestion.ingest_datasource_report(ds),
    **{name: (lambda ds, store, form=form: run_sync(form(ds, store))) for name, form in _ASYNC_NO_CLIENT.items()},
}
_CLIENT_READS = (
    "ping", "list_schemas", "default_schema", "current_catalog", "list_objects", "reflect_columns",
    "reflect_table_keys", "table_comment", "schema_description", "get_column_types", "execute",
)


@pytest.mark.parametrize("form", sorted(_NO_CLIENT))
def test_a_no_client_call_closes_its_engine(
    form: str, sqlite_shop: DatasourceConfig, tmp_path: Path, ledger: _Ledger,
) -> None:
    store = run_sync(shop_storage(str(tmp_path / "store"), datasource=sqlite_shop))
    with recorded():
        _NO_CLIENT[form](sqlite_shop, store)
    assert ledger.engines
    assert ledger.all_closed()


@pytest.mark.parametrize("form", sorted(_NO_CLIENT))
def test_a_failing_no_client_call_closes_its_engine(form: str, tmp_path: Path, ledger: _Ledger) -> None:
    unreachable = DatasourceConfig(name=DS_NAME, type="sqlite", database=str(tmp_path / "missing" / "shop.db"))
    store = run_sync(shop_storage(str(tmp_path / "store"), datasource=unreachable))
    with recorded(), contextlib.suppress(Exception):
        _NO_CLIENT[form](unreachable, store)
    assert ledger.engines
    assert ledger.all_closed()


@pytest.mark.parametrize("form", sorted(_ASYNC_NO_CLIENT))
async def test_a_cancelled_no_client_call_closes_its_engine(
    form: str, sqlite_shop: DatasourceConfig, tmp_path: Path, ledger: _Ledger, monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()

    async def _blocked(*args: Any, **kwargs: Any) -> Any:
        started.set()
        await asyncio.sleep(3600)

    for name in _CLIENT_READS:
        monkeypatch.setattr(SlayerSQLClient, name, _blocked)
    store = await shop_storage(str(tmp_path / "store"), datasource=sqlite_shop)
    with recorded():
        task = asyncio.ensure_future(_ASYNC_NO_CLIENT[form](sqlite_shop, store))
    await asyncio.wait_for(started.wait(), timeout=10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert ledger.engines
    assert ledger.all_closed()


def test_another_engines_client_factory_is_not_used(sqlite_shop: DatasourceConfig, tmp_path: Path) -> None:
    factory = GatewayFactory()
    with _engine(tmp_path / "host", datasource=sqlite_shop, client_factory=factory), recorded():
        models = ingestion.ingest_datasource(sqlite_shop)
    assert models
    assert factory.built == []


def test_an_unawaited_no_client_coroutine_builds_nothing(
    sqlite_shop: DatasourceConfig, tmp_path: Path, ledger: _Ledger,
) -> None:
    store = YAMLStorage(base_dir=str(tmp_path / "store"))
    with recorded() as caught:
        idempotent = ingestion.ingest_datasource_idempotent(datasource=sqlite_shop, storage=store)
        drift = schema_drift.validate_datasource(datasource=sqlite_shop, models=[orders_model()])
    idempotent.close()
    drift.close()
    assert ledger.engines == []
    assert ledger.clients == []
    assert len(future_warnings(caught)) == 2


def test_a_scope_error_in_a_no_client_call_leaks_nothing(
    sqlite_shop: DatasourceConfig, tmp_path: Path, ledger: _Ledger,
) -> None:
    store = YAMLStorage(base_dir=str(tmp_path / "store"))
    with recorded():
        idempotent = ingestion.ingest_datasource_idempotent(
            datasource=sqlite_shop, storage=store, schema="main", all_schemas=True,
        )
        with pytest.raises(ValueError, match="all_schemas"):
            run_sync(idempotent)
    with recorded(), pytest.raises(ValueError, match="all_schemas"):
        ingestion.ingest_datasource(sqlite_shop, schema="main", all_schemas=True)
    assert ledger.all_closed()
