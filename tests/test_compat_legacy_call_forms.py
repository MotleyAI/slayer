"""Ingestion, drift validation and the importer converters read the database only through the caller's client.

Spec: compatibility/legacy-call-forms.
"""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable, Generator, Iterator
from pathlib import Path
from typing import Any

import pytest

from slayer.async_utils import run_sync
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.database import engine_factory
from slayer.dbt.converter import DbtToSlayerConverter
from slayer.dbt.models import DbtProject, DbtRegularModel
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.live_schema import ingestion, schema_drift
from slayer.osi.converter import OsiToSlayerConverter
from slayer.osi.models import OSIDocument
from slayer.storage.yaml_storage import YAMLStorage
from tests._compat_helpers import future_warnings, recorded
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
from tests._osi_helpers import seed_db

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
COUNT_ORDERS = {"source_model": "orders", "measures": [{"formula": "count(*)", "name": "n"}]}



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
# Calls without a client
# --------------------------------------------------------------------------- #
_NO_CLIENT: dict[str, Callable[[DatasourceConfig, YAMLStorage], Any]] = {
    "ingest": lambda ds, _store: ingestion.ingest_datasource(ds),  # pyright: ignore[reportCallIssue]
    "report": lambda ds, _store: ingestion.ingest_datasource_report(ds),  # pyright: ignore[reportCallIssue]
    "idempotent": lambda ds, store: run_sync(ingestion.ingest_datasource_idempotent(datasource=ds, storage=store)),  # pyright: ignore[reportCallIssue]
    "validate": lambda ds, _store: run_sync(schema_drift.validate_datasource(datasource=ds, models=[orders_model()])),  # pyright: ignore[reportCallIssue]
}


@pytest.mark.parametrize("form", sorted(_NO_CLIENT))
def test_a_call_without_a_client_is_refused(form: str, tmp_path: Path) -> None:
    db = tmp_path / "never.db"
    ds = DatasourceConfig(name=DS_NAME, type="sqlite", database=str(db))
    call, store = _NO_CLIENT[form], YAMLStorage(base_dir=str(tmp_path / "store"))
    with recorded(), pytest.raises(TypeError):
        call(ds, store)
    assert not db.exists()


async def test_validate_datasource_takes_no_sql_clients(sqlite_shop: DatasourceConfig, tmp_path: Path) -> None:
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path / "store")))
    try:
        models, client = [orders_model()], engine.client_for(sqlite_shop)
        with recorded(), pytest.raises(TypeError):
            await schema_drift.validate_datasource(
                datasource=sqlite_shop, models=models, client=client, sql_clients={},  # pyright: ignore[reportCallIssue]
            )
    finally:
        await engine.aclose()
        engine.close()


def test_a_scope_error_fails_before_reading(sqlite_shop: DatasourceConfig, tmp_path: Path) -> None:
    with _engine(tmp_path / "store", datasource=sqlite_shop) as engine:
        client = engine.client_for(sqlite_shop)
        with pytest.raises(ValueError, match="all_schemas"):
            ingestion.ingest_datasource(sqlite_shop, client=client, schema="main", all_schemas=True)
        idempotent = ingestion.ingest_datasource_idempotent(
            datasource=sqlite_shop, storage=engine.storage, client=client, schema="main", all_schemas=True,
        )
        with pytest.raises(ValueError, match="all_schemas"):
            engine.run_sync(idempotent)


# --------------------------------------------------------------------------- #
# Converters
# --------------------------------------------------------------------------- #
def _client_converter(kind: str, *, dialect: str | None) -> Any:
    if kind == "dbt":
        return DbtToSlayerConverter(project=ORDERS_PROJECT, data_source=DS_NAME, include_hidden_models=True)
    return OsiToSlayerConverter(documents=[ORDERS_OSI], data_source=DS_NAME, target_dialect=dialect)

@pytest.mark.parametrize("kind", ["dbt", "osi"])
def test_a_positional_converter_argument_is_refused(kind: str) -> None:
    converter, source = (DbtToSlayerConverter, ORDERS_PROJECT) if kind == "dbt" else (OsiToSlayerConverter, [ORDERS_OSI])
    with recorded(), pytest.raises(TypeError):
        converter(source, DS_NAME, True)  # pyright: ignore[reportCallIssue, reportArgumentType]


@pytest.mark.parametrize("kind", ["dbt", "osi"])
def test_a_converter_takes_no_sa_engine(kind: str) -> None:
    converter: Any = DbtToSlayerConverter if kind == "dbt" else OsiToSlayerConverter
    source = {"project": ORDERS_PROJECT} if kind == "dbt" else {"documents": [ORDERS_OSI]}
    with recorded(), pytest.raises(TypeError):
        converter(**source, data_source=DS_NAME, sa_engine=None)


@pytest.mark.parametrize("kind", ["dbt", "osi"])
async def test_convert_reads_only_the_collected_metadata(
    kind: str, sqlite_shop: DatasourceConfig, tmp_path: Path,
) -> None:
    factory = GatewayFactory()
    engine = SlayerQueryEngine(
        storage=await shop_storage(str(tmp_path / "store"), datasource=sqlite_shop), client_factory=factory,
    )
    try:
        converter = _client_converter(kind, dialect=sqlite_shop.type)
        await converter.collect_metadata(client=engine.client_for(sqlite_shop))
        collected = len(factory.all_calls())
        with no_direct_access(backends=[backend_of(sqlite_shop)]) as guard:
            result = converter.convert()
    finally:
        await engine.aclose()
        engine.close()
    assert collected
    assert len(factory.all_calls()) == collected
    assert guard.attempts == []
    assert result.models


# --------------------------------------------------------------------------- #
# Client-taking forms
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
