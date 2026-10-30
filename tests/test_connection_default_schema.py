"""A bare ``sql_table`` resolves in the connection's default schema (the BigQuery URL dataset), however many schemas exist."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError

import slayer.engine.schema_drift as schema_drift
from slayer.core.enums import DataType
from slayer.core.errors import SchemaDriftError
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.engine import ingestion
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import (
    DriftSnapshot,
    EditModelDelete,
    LiveTable,
    validate_datasource,
)
from slayer.engine.statement_reads import StatementReads
from slayer.engine.schema_scope import (
    SchemaRef,
    default_schema_ref,
    resolve_ingest_scope,
)
from slayer.sql.dialects.reflection import ReflectedForeignKey, TableKeys
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


def _inspector(schemas: list[str] | None) -> sa.engine.Inspector:
    return cast(sa.engine.Inspector, _Inspector(schemas))


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


def _live(engine: sa.Engine, layout: _Layout, *, schemas: list[str] | None) -> ExitStack:
    """Serve ``layout`` (dataset → table → columns) as the live project; no dataset means the URL one, as in the driver."""

    def tables(ref: SchemaRef | None) -> dict[str, dict[str, DataType]]:
        return layout.get((ref.name if ref else None) or _URL_DATASET, {})

    def listing(*, inspector: Any, ref: SchemaRef, include_views: bool = True) -> list[SimpleNamespace]:
        return [SimpleNamespace(name=t) for t in tables(ref)]

    def introspect(*, inspector: Any, sa_engine: Any, table_name: str, ref: SchemaRef | None) -> LiveTable:
        return LiveTable(columns=tables(ref)[table_name])

    stack = ExitStack()
    stack.enter_context(
        patch.object(schema_drift._LiveConnection, "open", return_value=(engine, _Inspector(schemas)))
    )
    stack.enter_context(patch.object(ingestion, "list_ingestable_objects", side_effect=listing))
    stack.enter_context(patch.object(schema_drift, "_introspect_one_table", side_effect=introspect))
    stack.enter_context(patch.object(ingestion.engine_factory, "get_engine", return_value=engine))
    stack.enter_context(patch.object(ingestion.engine_factory, "release_idle"))
    stack.enter_context(patch.object(ingestion.sa, "inspect", return_value=_Inspector(schemas)))
    return stack


def _verdicts(entries: list[Any]) -> list[tuple[str, str, list[str]]]:
    return sorted(
        (type(e).__name__, e.model_name, list(e.remove.columns) if isinstance(e, EditModelDelete) else [])
        for e in entries
    )


class TestScope:
    def test_all_schemas_marks_the_url_dataset_default(self, bq_engine: sa.Engine) -> None:
        scope = resolve_ingest_scope(
            inspector=_inspector(_TWO_DATASETS), sa_engine=bq_engine,
            requested=None, all_schemas=True, datasource_schema=None,
        )
        assert {(r.name, r.is_default) for r in scope.schemas} == {(_URL_DATASET, True), ("ds_b", False)}

    def test_default_scope_is_the_url_dataset(self, bq_engine: sa.Engine) -> None:
        scope = resolve_ingest_scope(
            inspector=_inspector(_TWO_DATASETS), sa_engine=bq_engine,
            requested=None, all_schemas=False, datasource_schema=None,
        )
        assert [(r.name, r.is_default) for r in scope.schemas] == [(_URL_DATASET, True)]
        assert scope.other_schemas == ["ds_b"]

    def test_default_schema_ref_is_the_url_dataset(self, bq_engine: sa.Engine) -> None:
        ref = default_schema_ref(_inspector(_TWO_DATASETS), bq_engine)
        assert (ref.name, ref.is_default) == (_URL_DATASET, True)


@pytest.mark.parametrize("snapshot", [False, True], ids=["validate_models", "snapshot"])
class TestValidateModels:
    async def _validate(self, models: list[SlayerModel], *, snapshot: bool) -> list[Any]:
        return await validate_datasource(
            datasource=_DS, models=models, snapshot=DriftSnapshot(created_at=0.0) if snapshot else None,
        )

    async def test_a_bare_model_in_the_url_dataset_survives_a_second_dataset(
        self, bq_engine: sa.Engine, snapshot: bool,
    ) -> None:
        layout = {_URL_DATASET: {"contractors": _ID_NAME}, "ds_b": {"other": _ID_NAME}}
        models = [
            _model("contractors", sql_table="contractors", columns=_ID_NAME),
            _model("gone", sql_table="gone", columns=_ID_NAME),
        ]
        with _live(bq_engine, layout, schemas=_TWO_DATASETS):
            entries = await self._validate(models, snapshot=snapshot)
        assert _verdicts(entries) == [("WholeModelDelete", "gone", [])]

    async def test_column_drift_on_a_bare_model_is_an_edit(self, bq_engine: sa.Engine, snapshot: bool) -> None:
        layout = {_URL_DATASET: {"contractors": _ID_NAME}, "ds_b": {}}
        with _live(bq_engine, layout, schemas=_TWO_DATASETS):
            entries = await self._validate(
                [_model("contractors", sql_table="contractors", columns=_WITH_SKILLS)], snapshot=snapshot,
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
        with _live(bq_engine, layout, schemas=_TWO_DATASETS):
            entries = await self._validate(models, snapshot=snapshot)
        assert _verdicts(entries) == [("WholeModelDelete", "orphans", [])]

    async def test_a_same_named_table_elsewhere_does_not_shadow_the_default(
        self, bq_engine: sa.Engine, snapshot: bool,
    ) -> None:
        layout = {_URL_DATASET: {"contractors": _ID_NAME}, "ds_b": {"contractors": _WITH_SKILLS}}
        models = [
            _model("contractors", sql_table="contractors", columns=_WITH_SKILLS),
            _model("b_contractors", sql_table="ds_b.contractors", columns=_WITH_SKILLS),
        ]
        with _live(bq_engine, layout, schemas=_TWO_DATASETS):
            entries = await self._validate(models, snapshot=snapshot)
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
        with _live(bq_engine, layout, schemas=schemas):
            await query_engine._maybe_raise_schema_drift(
                err=_UNRELATED, datasource=_DS, read_set={"contractors"},
                reads=StatementReads(columns={("contractors", "id"), ("contractors", "name")}),
            )

    async def test_a_dropped_column_on_a_bare_model_is_attributed(
        self, bq_engine: sa.Engine, query_engine: SlayerQueryEngine, schemas: list[str] | None,
    ) -> None:
        await query_engine.storage.save_model(_model("contractors", sql_table="contractors", columns=_WITH_SKILLS))
        layout = {_URL_DATASET: {"contractors": _ID_NAME}, "ds_b": {"other": _ID_NAME}}
        with _live(bq_engine, layout, schemas=schemas), pytest.raises(SchemaDriftError) as exc:
            await query_engine._maybe_raise_schema_drift(
                err=_UNRELATED, datasource=_DS, read_set={"contractors"},
                reads=StatementReads(columns={("contractors", "skills")}),
            )
        assert _verdicts(exc.value.to_delete) == [("EditModelDelete", "contractors", ["skills"])]


async def test_reingest_with_the_url_dataset_heals_a_bare_model(bq_engine: sa.Engine, tmp_path: Path) -> None:
    storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
    await storage.save_datasource(_DS)
    await storage.save_model(_model("contractors", sql_table="contractors", columns=_ID_NAME))
    fresh = _model("contractors", sql_table=f"{_URL_DATASET}.contractors", columns=_ID_NAME)
    layout = {_URL_DATASET: {"contractors": _ID_NAME}, "ds_b": {"other": _ID_NAME}}
    with (
        _live(bq_engine, layout, schemas=_TWO_DATASETS),
        patch.object(ingestion, "ingest_datasource_report", return_value=ingestion.IngestionScanReport(models=[fresh])),
    ):
        result = await ingestion.ingest_datasource_idempotent(datasource=_DS, storage=storage, schema=_URL_DATASET)
    stored = await storage.get_model("contractors", data_source=_DS.name)
    assert stored is not None
    assert stored.sql_table == f"{_URL_DATASET}.contractors"
    assert result.skipped == []


def test_a_same_dataset_foreign_key_becomes_a_join(bq_engine: sa.Engine) -> None:
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
        joins = ingestion._generate_joins(
            _inspector(_TWO_DATASETS), "orders", None, {"orders", "customers"}, sa_engine=bq_engine,
        )
    assert [j.target_model for j in joins] == ["customers"]
