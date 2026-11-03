"""A bare ``sql_table`` resolves in the connection's default schema (the BigQuery URL dataset), however many schemas exist."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

from slayer.core.enums import DataType
from slayer.core.errors import SchemaDriftError
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine import ingestion
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import (
    DriftSnapshot,
    EditModelDelete,
    validate_datasource,
)
from slayer.engine.schema_scope import (
    default_schema_ref,
    resolve_ingest_scope,
)
from slayer.sql.client import DelegatingSqlClient, ObjectListing, ObjectNotFound, SlayerSQLClient
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.reflection import ReflectedColumn, ReflectedForeignKey, TableKeys, TableLocation
from slayer.sql.engine_factory import client_cache_key
from slayer.storage.yaml_storage import YAMLStorage
from tests._engine_helpers import disposable_engine

pytest.importorskip("sqlalchemy_bigquery")

_URL_DATASET = "ds_a"
_DS = DatasourceConfig(name="bq", type="bigquery", connection_string=f"bigquery://p/{_URL_DATASET}")
_TWO_DATASETS = [_URL_DATASET, "ds_b"]

_Layout = dict[str, dict[str, dict[str, DataType]]]

_ID_NAME = {"id": DataType.INT, "name": DataType.TEXT}
_WITH_SKILLS = {**_ID_NAME, "skills": DataType.TEXT}


class _Inspector:
    """The ``sqlalchemy-bigquery`` shape: no ``default_schema_name``; ``None`` schemas = listing fails."""

    def __init__(self, schemas: list[str] | None) -> None:
        self.default_schema_name = None
        self._schemas = schemas

    def get_schema_names(self) -> list[str]:
        if self._schemas is None:
            raise RuntimeError("schema listing failed")
        return list(self._schemas)


class _LayoutClient(DelegatingSqlClient):
    """Serves ``layout`` (dataset → table → columns) as the live project; no dataset means the URL one, as in the driver."""

    def __init__(self, sa_engine: sa.Engine, layout: _Layout, *, schemas: list[str] | None) -> None:
        super().__init__(inner=SlayerSQLClient(datasource=_DS))
        self._sa_engine = sa_engine
        self._layout = layout
        self._inspector = _Inspector(schemas)

    def _tables(self, schema: str | None) -> dict[str, dict[str, DataType]]:
        return self._layout.get(schema or _URL_DATASET, {})

    async def ping(self) -> None:
        return None

    async def list_schemas(self) -> list[str]:
        return self._inspector.get_schema_names()

    async def default_schema(self) -> str | None:
        return dialect_for_ds_type(_DS.type).reflect_default_schema(sa_engine=self._sa_engine, inspector=self._inspector)

    async def current_catalog(self) -> str | None:
        return None

    async def list_objects(self, schema: str | None) -> ObjectListing:
        return ObjectListing(tables=list(self._tables(schema)))

    async def reflect_columns(self, location: TableLocation) -> list[ReflectedColumn]:
        columns = self._tables(location.schema_name).get(location.name)
        if columns is None:
            raise ObjectNotFound(f"table {location.name!r} not found")
        return [ReflectedColumn(name=name, type=type_) for name, type_ in columns.items()]

    async def reflect_table_keys(self, location: TableLocation) -> TableKeys:
        return TableKeys()

    async def table_comment(self, location: TableLocation) -> str | None:
        return None

    async def schema_description(self, schema: str | None, configured_schema: str | None) -> str | None:
        return None


@pytest.fixture
def bq_engine() -> Iterator[sa.Engine]:
    """Offline BigQuery engine on the URL dataset; any SQL it would run fails."""
    with (
        disposable_engine(f"bigquery://p/{_URL_DATASET}?user_supplied_client=true") as engine,
        patch.object(engine, "connect", MagicMock(side_effect=RuntimeError("offline"))),
    ):
        yield engine


def _model(name: str, *, sql_table: str, columns: dict[str, DataType]) -> SlayerModel:
    return SlayerModel(
        name=name,
        data_source=_DS.name,
        sql_table=sql_table,
        columns=[Column(name=c, type=t) for c, t in columns.items()],
    )


def _live(engine: sa.Engine, layout: _Layout, *, schemas: list[str] | None) -> _LayoutClient:
    return _LayoutClient(engine, layout, schemas=schemas)


def _verdicts(entries: list[Any]) -> list[tuple[str, str, list[str]]]:
    return sorted(
        (type(e).__name__, e.model_name, list(e.remove.columns) if isinstance(e, EditModelDelete) else [])
        for e in entries
    )


class TestScope:
    async def test_all_schemas_marks_the_url_dataset_default(self, bq_engine: sa.Engine) -> None:
        scope = await resolve_ingest_scope(
            client=_live(bq_engine, {}, schemas=_TWO_DATASETS),
            requested=None, all_schemas=True, datasource_schema=None,
        )
        assert {(r.name, r.is_default) for r in scope.schemas} == {(_URL_DATASET, True), ("ds_b", False)}

    async def test_default_scope_is_the_url_dataset(self, bq_engine: sa.Engine) -> None:
        scope = await resolve_ingest_scope(
            client=_live(bq_engine, {}, schemas=_TWO_DATASETS),
            requested=None, all_schemas=False, datasource_schema=None,
        )
        assert [(r.name, r.is_default) for r in scope.schemas] == [(_URL_DATASET, True)]
        assert scope.other_schemas == ["ds_b"]

    async def test_default_schema_ref_is_the_url_dataset(self, bq_engine: sa.Engine) -> None:
        ref = await default_schema_ref(_live(bq_engine, {}, schemas=_TWO_DATASETS))
        assert (ref.name, ref.is_default) == (_URL_DATASET, True)


@pytest.mark.parametrize("snapshot", [False, True], ids=["validate_models", "snapshot"])
class TestValidateModels:
    async def _validate(self, models: list[SlayerModel], *, client: _LayoutClient, snapshot: bool) -> list[Any]:
        return await validate_datasource(
            datasource=_DS, models=models, client=client,
            snapshot=DriftSnapshot(created_at=0.0) if snapshot else None,
        )

    async def test_a_bare_model_in_the_url_dataset_survives_a_second_dataset(
        self, bq_engine: sa.Engine, snapshot: bool,
    ) -> None:
        layout = {_URL_DATASET: {"contractors": _ID_NAME}, "ds_b": {"other": _ID_NAME}}
        models = [
            _model("contractors", sql_table="contractors", columns=_ID_NAME),
            _model("gone", sql_table="gone", columns=_ID_NAME),
        ]
        entries = await self._validate(
            models, client=_live(bq_engine, layout, schemas=_TWO_DATASETS), snapshot=snapshot,
        )
        assert _verdicts(entries) == [("WholeModelDelete", "gone", [])]

    async def test_column_drift_on_a_bare_model_is_an_edit(self, bq_engine: sa.Engine, snapshot: bool) -> None:
        layout = {_URL_DATASET: {"contractors": _ID_NAME}, "ds_b": {}}
        entries = await self._validate(
            [_model("contractors", sql_table="contractors", columns=_WITH_SKILLS)],
            client=_live(bq_engine, layout, schemas=_TWO_DATASETS), snapshot=snapshot,
        )
        assert _verdicts(entries) == [("EditModelDelete", "contractors", ["skills"])]

    async def test_a_bare_name_never_resolves_to_another_dataset(
        self, bq_engine: sa.Engine, snapshot: bool,
    ) -> None:
        layout = {_URL_DATASET: {"contractors": _ID_NAME}, "ds_b": {"orphans": _ID_NAME}}
        models = [
            _model("contractors", sql_table="contractors", columns=_ID_NAME),
            _model("orphans", sql_table="orphans", columns=_ID_NAME),
        ]
        entries = await self._validate(
            models, client=_live(bq_engine, layout, schemas=_TWO_DATASETS), snapshot=snapshot,
        )
        assert _verdicts(entries) == [("WholeModelDelete", "orphans", [])]

    async def test_a_same_named_table_elsewhere_does_not_shadow_the_default(
        self, bq_engine: sa.Engine, snapshot: bool,
    ) -> None:
        layout = {_URL_DATASET: {"contractors": _ID_NAME}, "ds_b": {"contractors": _WITH_SKILLS}}
        models = [
            _model("contractors", sql_table="contractors", columns=_WITH_SKILLS),
            _model("b_contractors", sql_table="ds_b.contractors", columns=_WITH_SKILLS),
        ]
        entries = await self._validate(
            models, client=_live(bq_engine, layout, schemas=_TWO_DATASETS), snapshot=snapshot,
        )
        assert _verdicts(entries) == [("EditModelDelete", "contractors", ["skills"])]


@pytest.fixture
async def query_engine(tmp_path: Path) -> AsyncIterator[SlayerQueryEngine]:
    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    await storage.save_datasource(_DS)
    engine = SlayerQueryEngine(storage=storage)
    try:
        yield engine
    finally:
        await engine.aclose()


_UNRELATED = OperationalError("SELECT 1", {}, Exception("quota exceeded"))


@pytest.mark.parametrize("schemas", [_TWO_DATASETS, None], ids=["listed", "schema_list_unreadable"])
class TestQueryTimeAttribution:
    """``schema_list_unreadable`` resolves through the enumeration-failure fallback."""

    async def test_an_unchanged_bare_model_lets_the_original_error_propagate(
        self, bq_engine: sa.Engine, query_engine: SlayerQueryEngine, schemas: list[str] | None,
    ) -> None:
        await query_engine.storage.save_model(_model("contractors", sql_table="contractors", columns=_ID_NAME))
        layout = {_URL_DATASET: {"contractors": _ID_NAME}, "ds_b": {"other": _ID_NAME}}
        query_engine._sql_clients[client_cache_key(_DS)] = _live(bq_engine, layout, schemas=schemas)
        await query_engine._maybe_raise_schema_drift(
            err=_UNRELATED, datasource=_DS, read_set={"contractors"},
        )

    async def test_a_dropped_column_on_a_bare_model_is_attributed(
        self, bq_engine: sa.Engine, query_engine: SlayerQueryEngine, schemas: list[str] | None,
    ) -> None:
        await query_engine.storage.save_model(_model("contractors", sql_table="contractors", columns=_WITH_SKILLS))
        layout = {_URL_DATASET: {"contractors": _ID_NAME}, "ds_b": {"other": _ID_NAME}}
        query_engine._sql_clients[client_cache_key(_DS)] = _live(bq_engine, layout, schemas=schemas)
        with pytest.raises(SchemaDriftError) as exc:
            await query_engine._maybe_raise_schema_drift(
                err=_UNRELATED, datasource=_DS, read_set={"contractors"},
            )
        assert _verdicts(exc.value.to_delete) == [("EditModelDelete", "contractors", ["skills"])]


async def test_reingest_with_the_url_dataset_heals_a_bare_model(bq_engine: sa.Engine, tmp_path: Path) -> None:
    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    await storage.save_datasource(_DS)
    await storage.save_model(_model("contractors", sql_table="contractors", columns=_ID_NAME))
    fresh = _model("contractors", sql_table=f"{_URL_DATASET}.contractors", columns=_ID_NAME)
    layout = {_URL_DATASET: {"contractors": _ID_NAME}, "ds_b": {"other": _ID_NAME}}
    with patch.object(
        ingestion, "ingest_datasource_report_async", return_value=ingestion.IngestionScanReport(models=[fresh]),
    ):
        result = await ingestion.ingest_datasource_idempotent(
            datasource=_DS, storage=storage, client=_live(bq_engine, layout, schemas=_TWO_DATASETS),
            schema=_URL_DATASET,
        )
    stored = await storage.get_model("contractors", data_source=_DS.name)
    assert stored is not None
    assert stored.sql_table == f"{_URL_DATASET}.contractors"
    assert result.skipped == []


async def test_a_same_dataset_foreign_key_becomes_a_join(bq_engine: sa.Engine) -> None:
    keys = {
        "orders": TableKeys(
            primary_key=["id"],
            foreign_keys=[ReflectedForeignKey(
                columns=["customer_id"], referred_schema=_URL_DATASET,
                referred_table="customers", referred_columns=["id"],
            )],
        ),
        "customers": TableKeys(primary_key=["id"]),
    }
    with patch.object(ingestion, "reflect_table_keys", side_effect=lambda **kw: keys[kw["table_name"]]):
        joins = await ingestion._generate_joins(
            client=_live(bq_engine, {}, schemas=_TWO_DATASETS), source_table="orders", schema=None,
            table_set={"orders", "customers"},
        )
    assert [j.target_model for j in joins] == ["customers"]
