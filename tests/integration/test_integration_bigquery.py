"""Live BigQuery integration tests for comment/description ingestion.

Runs against a real GCP project: requires ``GCP_PROJECT_ID`` plus Application
Default Credentials (``GOOGLE_APPLICATION_CREDENTIALS`` or ambient ADC), and a
service account allowed to create/delete datasets in that project. Skips
cleanly when the env var or credentials are absent (local dev machines);
fails loudly when credentials exist but lack dataset-create rights, so a CI
permissions regression is visible rather than silently skipped.

A uniquely-named temporary dataset (with a description) is created, populated
with commented tables, ingested, and deleted on teardown.
"""

import os
import uuid
from collections.abc import AsyncIterator, Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.errors import SchemaDriftError
from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.core.query import SlayerQuery
from slayer.engine import ingestion
from slayer.engine.ingestion import (
    ingest_datasource_idempotent,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.engine.schema_drift import EditModelDelete, validate_datasource
from slayer.sql import engine_factory
from slayer.sql.client import SlayerSQLClient, SqlClient
from slayer.storage.yaml_storage import YAMLStorage
from tests._cast_fixtures import check_try_cast_targets
from tests._dev2120_fixtures import (
    DOCS_ROWS,
    LIB,
    check_library,
    check_native_json,
    contractors_model,
    contractors_statements,
    lib_model,
    server_literal,
    server_table_statements as dev2120_table_statements,
)
from tests._dev2137_fixtures import as_list, assignments_model, assignments_statements, check_lists, num
from tests._list_mixed_numbers import check_mixed_numbers, mixed_numbers_model, mixed_numbers_statements
from tests._sql_mode_retype_fixtures import (
    check_query_carries_finding,
    check_reingest_retypes,
    check_validation_finding,
    sql_shapes,
    stale_sql_model,
)
from tests._reserved_alias_probe import (
    BIGQUERY_KEYWORDS,
    keyword_universe,
    unquoted_alias_failures,
)

pytest.importorskip("sqlalchemy_bigquery")
bigquery = pytest.importorskip("google.cloud.bigquery")
_api_errors = pytest.importorskip("google.api_core.exceptions")
BadRequest, Forbidden = _api_errors.BadRequest, _api_errors.Forbidden
DefaultCredentialsError = pytest.importorskip("google.auth.exceptions").DefaultCredentialsError

pytestmark = pytest.mark.integration

_PROJECT = os.environ.get("GCP_PROJECT_ID")
_DATASET_DESCRIPTION = "Dataset for SLayer ingestion tests"


@pytest.fixture(scope="module")
def bq_dataset():
    """Temp dataset with a description + commented tables; dropped on teardown."""
    if not _PROJECT:
        pytest.skip("GCP_PROJECT_ID not set — live BigQuery tests need a billing project")
    try:
        client = bigquery.Client(project=_PROJECT)
    except DefaultCredentialsError as exc:
        pytest.skip(f"No Google Application Default Credentials: {exc}")
    dataset_id = f"slayer_test_{uuid.uuid4().hex[:12]}"
    dataset = bigquery.Dataset(f"{_PROJECT}.{dataset_id}")
    dataset.description = _DATASET_DESCRIPTION
    try:
        client.create_dataset(dataset)
    except Forbidden as exc:
        client.close()
        pytest.fail(
            f"Service account cannot create datasets in {_PROJECT} "
            f"(grant roles/bigquery.user or dataEditor): {exc}"
        )
    except Exception:
        client.close()
        raise
    try:
        orders = bigquery.Table(
            f"{_PROJECT}.{dataset_id}.orders",
            schema=[
                bigquery.SchemaField("id", "INTEGER", description="Order id"),
                bigquery.SchemaField(
                    "amount", "FLOAT", description="Order amount in USD"
                ),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField(
                    "payload",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField(
                            "city", "STRING", description="City in the payload"
                        ),
                    ],
                    description="Structured payload",
                ),
            ],
        )
        orders.description = "All orders"
        client.create_table(orders)
        client.create_table(bigquery.Table(
            f"{_PROJECT}.{dataset_id}.plain",
            schema=[bigquery.SchemaField("x", "INTEGER")],
        ))
        yield client, dataset_id
    finally:
        client.delete_dataset(
            f"{_PROJECT}.{dataset_id}", delete_contents=True, not_found_ok=True
        )
        client.close()


def _ds_config(dataset_id: str, *, name: str = "bqtest", description: str | None = None):
    # Dataset-qualified URL so table names reflect bare (not "dataset.table").
    return DatasourceConfig(
        name=name,
        type="bigquery",
        connection_string=f"bigquery://{_PROJECT}/{dataset_id}",
        description=description,
    )


async def _ingest(ds: DatasourceConfig, **kwargs: Any) -> list[SlayerModel]:
    sql_client = SlayerSQLClient(datasource=ds)
    try:
        return await ingestion.ingest_datasource_async(ds, client=sql_client, **kwargs)
    finally:
        await sql_client.aclose()
        sql_client.close()


@pytest.fixture
async def client_for(tmp_path: Path) -> AsyncIterator[Callable[[DatasourceConfig], SqlClient]]:
    """An engine's client builder; its clients are released at teardown."""
    engine = SlayerQueryEngine(storage=YAMLStorage(base_dir=str(tmp_path / "client_engine")))
    try:
        yield engine.client_for
    finally:
        await engine.aclose()
        engine.close()


@pytest.fixture(scope="module")
def bq_models(bq_dataset):
    _, dataset_id = bq_dataset
    models = run_sync(_ingest(_ds_config(dataset_id), schema=dataset_id))
    return {m.name: m for m in models}, dataset_id


class TestBigQueryCommentIngestion:
    def test_column_descriptions_imported(self, bq_models) -> None:
        models, _ = bq_models
        cols = {c.name: c for c in models["orders"].columns}
        assert cols["id"].description == "Order id"
        assert cols["amount"].description == "Order amount in USD"
        assert cols["status"].description is None

    def test_table_description_imported(self, bq_models) -> None:
        models, _ = bq_models
        assert models["orders"].description == "All orders"
        assert models["plain"].description is None

    def test_record_column_description(self, bq_models) -> None:
        models, _ = bq_models
        cols = {c.name: c for c in models["orders"].columns}
        assert cols["payload"].description == "Structured payload"
        # Flattened RECORD subfields (dotted names) never become columns.
        assert not any("." in name for name in cols)

    def test_types_sanity(self, bq_models) -> None:
        models, _ = bq_models
        cols = {c.name: c for c in models["orders"].columns}
        assert cols["id"].type is DataType.INT
        assert cols["amount"].type is DataType.DOUBLE


class TestBigQueryDatasetDescription:
    async def test_dataset_description_fills_datasource(
        self, bq_dataset, tmp_path: Path, client_for
    ) -> None:
        _, dataset_id = bq_dataset
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        ds = _ds_config(dataset_id)
        await storage.save_datasource(ds)

        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, client=client_for(ds), schema=dataset_id
        )
        assert result.datasource_described is True
        loaded = await storage.get_datasource("bqtest")
        assert loaded.description == _DATASET_DESCRIPTION

        # Re-ingest with the reloaded datasource: no-op, nothing re-reported.
        result2 = await ingest_datasource_idempotent(
            datasource=loaded, storage=storage, client=client_for(loaded), schema=dataset_id
        )
        assert result2.datasource_described is False

    async def test_preset_description_untouched(
        self, bq_dataset, tmp_path: Path, client_for
    ) -> None:
        _, dataset_id = bq_dataset
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        ds = _ds_config(dataset_id, name="bqtest2", description="user text")
        await storage.save_datasource(ds)

        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, client=client_for(ds), schema=dataset_id
        )
        assert result.datasource_described is False
        loaded = await storage.get_datasource("bqtest2")
        assert loaded.description == "user text"


class TestBigQueryReingest:
    async def test_fill_if_empty_and_preservation(
        self, bq_dataset, tmp_path: Path, client_for
    ) -> None:
        _, dataset_id = bq_dataset
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        ds = _ds_config(dataset_id)
        await storage.save_datasource(ds)
        await ingest_datasource_idempotent(
            datasource=ds, storage=storage, client=client_for(ds), schema=dataset_id
        )

        loaded = await storage.get_model("orders", data_source="bqtest")
        for c in loaded.columns:
            if c.name == "amount":
                c.description = None
            if c.name == "status":
                c.description = "hand-authored"
        await storage.save_model(loaded)

        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, client=client_for(ds), schema=dataset_id
        )
        addition = next(a for a in result.additions if a.model_name == "orders")
        assert addition.described_columns == ["amount"]
        loaded2 = await storage.get_model("orders", data_source="bqtest")
        cols = {c.name: c for c in loaded2.columns}
        assert cols["amount"].description == "Order amount in USD"
        assert cols["status"].description == "hand-authored"

    async def test_new_commented_column_arrives_with_description(
        self, bq_dataset, tmp_path: Path, client_for
    ) -> None:
        client, dataset_id = bq_dataset
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        ds = _ds_config(dataset_id)
        await storage.save_datasource(ds)
        await ingest_datasource_idempotent(
            datasource=ds, storage=storage, client=client_for(ds), schema=dataset_id
        )

        table = client.get_table(f"{_PROJECT}.{dataset_id}.orders")
        table.schema = list(table.schema) + [
            bigquery.SchemaField("discount", "FLOAT", description="Discount applied"),
        ]
        client.update_table(table, ["schema"])

        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, client=client_for(ds), schema=dataset_id
        )
        addition = next(a for a in result.additions if a.model_name == "orders")
        assert "discount" in addition.new_columns
        assert "discount" not in addition.described_columns
        loaded = await storage.get_model("orders", data_source="bqtest")
        discount = next(c for c in loaded.columns if c.name == "discount")
        assert discount.description == "Discount applied"


@pytest.fixture
def second_dataset(bq_dataset):
    """Another dataset in the project, holding a same-named ``plain`` twin."""
    client, _ = bq_dataset
    other_id = f"slayer_test_{uuid.uuid4().hex[:12]}"
    client.create_dataset(bigquery.Dataset(f"{_PROJECT}.{other_id}"))
    try:
        client.create_table(bigquery.Table(
            f"{_PROJECT}.{other_id}.plain",
            schema=[bigquery.SchemaField("ghost", "STRING")],
        ))
        yield other_id
    finally:
        client.delete_dataset(f"{_PROJECT}.{other_id}", delete_contents=True, not_found_ok=True)


class TestBigQueryDriftWithSecondDataset:
    async def test_bare_models_resolve_in_the_url_dataset(self, bq_dataset, second_dataset, client_for) -> None:
        _, dataset_id = bq_dataset
        ds = _ds_config(dataset_id, name="bqdrift")
        models = await ingestion.ingest_datasource_async(ds, client=client_for(ds))
        assert {m.sql_table for m in models} == {"orders", "plain"}
        plain = next(m for m in models if m.name == "plain")
        plain.columns.append(Column(name="ghost", type=DataType.TEXT))

        entries = await validate_datasource(datasource=ds, models=models, client=client_for(ds))

        assert [
            (type(e).__name__, e.model_name, e.remove.columns if isinstance(e, EditModelDelete) else None)
            for e in entries
        ] == [("EditModelDelete", "plain", ["ghost"])]


async def test_default_schema_is_the_url_dataset_without_a_request(bq_dataset, monkeypatch) -> None:
    bq_client, dataset_id = bq_dataset
    second = f"{_PROJECT}.slayer_test_{uuid.uuid4().hex[:12]}"
    bq_client.create_dataset(bigquery.Dataset(second))
    ds = _ds_config(dataset_id, name="bqdefault")

    def _no_request(*_args, **_kwargs):
        raise AssertionError("a request was made to resolve the default schema")

    sa_engine = engine_factory.get_engine(ds)
    monkeypatch.setattr(sa_engine, "connect", _no_request)
    monkeypatch.setattr(sa_engine, "raw_connection", _no_request)
    client = SlayerSQLClient(datasource=ds)
    try:
        assert await client.default_schema() == dataset_id
    finally:
        client.close()
        engine_factory.invalidate_engine(ds)
        bq_client.delete_dataset(second, delete_contents=True, not_found_ok=True)


def test_every_bigquery_alias_failure_is_quoted(bq_dataset) -> None:
    client, dataset_id = bq_dataset
    dry_run = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)

    def fails(sql: str) -> bool:
        try:
            client.query(sql, job_config=dry_run)
        except BadRequest:
            return True
        return False

    assert unquoted_alias_failures(
        dialect="bigquery", words=keyword_universe(BIGQUERY_KEYWORDS), table=f"{dataset_id}.plain", column="x",
        fails=fails, workers=8,
    ) == []


# ---------------------------------------------------------------------------
# Portable Mode-B library (spec: queries/scalar-functions, regex, json, mixed-type-coercion, string-matching)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def bq_dev2120_storage(bq_dataset, tmp_path_factory):
    client, dataset_id = bq_dataset
    for stmt in dev2120_table_statements(LIB, dialect="bigquery", prefix=f"{dataset_id}."):
        client.query(stmt).result()
    rows = ", ".join(f"({rid}, PARSE_JSON({server_literal(doc, kind='TEXT', dialect='bigquery')}))"
                     for rid, doc in DOCS_ROWS)
    client.query(f"CREATE TABLE {dataset_id}.docs (id INT64, payload JSON)").result()
    client.query(f"INSERT INTO {dataset_id}.docs VALUES {rows}").result()
    client.query(f"CREATE TABLE {dataset_id}.events (id INT64, zoned_ts TIMESTAMP, wall_ts DATETIME)").result()
    client.query(f"INSERT INTO {dataset_id}.events VALUES {_EVENT_ROWS}").result()
    ds = _ds_config(dataset_id)
    storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("bq_dev2120")))
    run_sync(storage.save_datasource(ds))
    run_sync(storage.save_model(lib_model(data_source=ds.name)))
    for model in ingestion.ingest_datasource(datasource=ds, include_tables=["docs", "events"]):
        run_sync(storage.save_model(model))
    return storage


async def test_dev2120_library(bq_dev2120_storage) -> None:
    await check_library(SlayerQueryEngine(storage=bq_dev2120_storage))


async def test_try_cast_integer_targets(bq_dev2120_storage) -> None:
    await check_try_cast_targets(SlayerQueryEngine(storage=bq_dev2120_storage), model=LIB.name)


async def test_dev2120_native_json_column(bq_dev2120_storage) -> None:
    await check_native_json(SlayerQueryEngine(storage=bq_dev2120_storage))


_EVENT_ROWS = ("(1, TIMESTAMP '2024-02-29 10:00:00 UTC', DATETIME '2024-02-29 10:00:00'), "
               "(2, TIMESTAMP '2025-06-01 23:30:00 UTC', DATETIME '2025-06-01 23:30:00')")


async def test_dev2120_zoned_timestamp_column(bq_dev2120_storage) -> None:
    model = await bq_dev2120_storage.get_model("events", data_source="bqtest")
    db_types = {c.name: c.db_type for c in model.columns if c.name in ("zoned_ts", "wall_ts")}
    assert db_types == {"zoned_ts": "TIMESTAMP", "wall_ts": None}
    engine = SlayerQueryEngine(storage=bq_dev2120_storage)
    for column in ("zoned_ts", "wall_ts"):
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "events", "dimensions": ["id", {"expression": f"date_add({column}, 1, 'day')", "name": "v"}],
            "filters": [f"{column} < now()", f"{column} > date_add(now(), -36500, 'day')"],
        }))
        got = {int(r["events.id"]): r["events.v"] for r in resp.data}
        assert got == {1: datetime(2024, 3, 1, 10, 0), 2: datetime(2025, 6, 2, 23, 30)}, (column, resp.sql)
        resp = await engine.execute(SlayerQuery.model_validate({
            "source_model": "events",
            "dimensions": ["id", column, {"expression": f"month({column})", "name": "bucket"}],
        }))
        rows = {int(r["events.id"]): (r[f"events.{column}"], r["events.bucket"]) for r in resp.data}
        assert rows == {1: (datetime(2024, 2, 29, 10, 0), datetime(2024, 2, 1)),
                        2: (datetime(2025, 6, 1, 23, 30), datetime(2025, 6, 1))}, (column, resp.sql)


# ---------------------------------------------------------------------------
# List columns: REPEATED fields (spec: models/list-columns, models/schema-drift, queries/list-functions,
# queries/list-children, aggregations/collect)
# ---------------------------------------------------------------------------


def _stale_contractors(data_source: str) -> SlayerModel:
    """``contractors`` as stored before list columns: ``skills`` as TEXT."""
    return SlayerModel(name="stale", sql_table="contractors", data_source=data_source, columns=[
        Column(name="id", type=DataType.INT, primary_key=True), Column(name="skills", type=DataType.TEXT)])


@pytest.fixture(scope="module")
def bq_lists(bq_dataset, tmp_path_factory):
    client, dataset_id = bq_dataset
    prefix = f"{dataset_id}."
    for stmt in [*contractors_statements(dialect="bigquery", prefix=prefix),
                 *assignments_statements(dialect="bigquery", prefix=prefix),
                 *mixed_numbers_statements(dialect="bigquery", prefix=prefix)]:
        client.query(stmt).result()
    ds = _ds_config(dataset_id)
    storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("bq_lists")))
    run_sync(storage.save_datasource(ds))
    for model in (contractors_model(data_source=ds.name), assignments_model(data_source=ds.name),
                  mixed_numbers_model(data_source=ds.name), _stale_contractors(ds.name)):
        run_sync(storage.save_model(model))
    return storage, ds


async def test_lists(bq_lists) -> None:
    await check_lists(SlayerQueryEngine(storage=bq_lists[0]), dialect="bigquery")


async def test_mixed_numeric_lists(bq_lists) -> None:
    await check_mixed_numbers(SlayerQueryEngine(storage=bq_lists[0]))


def test_repeated_field_ingested(bq_lists) -> None:
    (model,) = ingestion.ingest_datasource(datasource=bq_lists[1], include_tables=["contractors"])
    skills = model.get_column("skills")
    assert skills is not None
    assert (skills.type, skills.element_type) == (DataType.ARRAY, DataType.TEXT)
    assert "STRING" in (skills.db_type or "").upper(), skills.db_type


async def test_refused_group_by_on_a_stale_list_column_is_drift(bq_lists) -> None:
    engine = SlayerQueryEngine(storage=bq_lists[0])
    stale = SlayerQuery.model_validate({"source_model": "stale", "dimensions": ["skills"]})
    with pytest.raises(SchemaDriftError) as info:
        await engine.execute(stale)
    assert "skills" in str(info.value), str(info.value)
    assert "ingest" in str(info.value).lower(), str(info.value)


# NUMERIC / BIGNUMERIC ingest as DOUBLE: collect's typed ``[]`` fallback must unify with their ARRAY_AGG.
_AMOUNT_ROWS = ("(1, 'a', NUMERIC '1.5', BIGNUMERIC '1.5', 1), (2, 'a', NUMERIC '2.25', BIGNUMERIC '2.25', 2), "
                "(3, 'a', NUMERIC '1.5', BIGNUMERIC '1.5', 1), (4, 'b', NULL, NULL, NULL)")
_AMOUNT_COLUMNS = ("n", "big", "i")


@pytest.fixture(scope="module")
def bq_amounts(bq_dataset, tmp_path_factory):
    client, dataset_id = bq_dataset
    client.query(f"CREATE TABLE {dataset_id}.amounts "
                 "(id INT64, team STRING, n NUMERIC, big BIGNUMERIC, i INT64)").result()
    client.query(f"INSERT INTO {dataset_id}.amounts VALUES {_AMOUNT_ROWS}").result()
    ds = _ds_config(dataset_id)
    storage = YAMLStorage(base_dir=str(tmp_path_factory.mktemp("bq_amounts")))
    run_sync(storage.save_datasource(ds))
    for model in ingestion.ingest_datasource(datasource=ds, include_tables=["amounts"]):
        run_sync(storage.save_model(model))
    return storage


async def test_collect_over_exact_numerics(bq_amounts) -> None:
    resp = await SlayerQueryEngine(storage=bq_amounts).execute(SlayerQuery.model_validate({
        "source_model": "amounts", "dimensions": ["team"],
        "measures": [{"formula": f"collect({c})", "name": f"{c}_set"} for c in _AMOUNT_COLUMNS],
    }))
    got = {r["amounts.team"]: {c: [num(v) for v in as_list(r[f"amounts.{c}_set"]) or ()] for c in _AMOUNT_COLUMNS}
           for r in resp.data}
    assert got == {"a": {"n": [1.5, 2.25], "big": [1.5, 2.25], "i": [1.0, 2.0]},
                   "b": {"n": [], "big": [], "i": []}}, resp.sql
    assert all(r[f"amounts.{c}_set"] is not None for r in resp.data for c in _AMOUNT_COLUMNS), resp.data


# Last in the module: re-ingest re-types the shared ``stale`` model too. The BigQuery driver reports a
# REPEATED result column by its element type, so only sqlglot typing finds the list (no untypeable shape).
@pytest.mark.parametrize(("sql", "column", "element"), sql_shapes(table="contractors", column="scores", untypeable=None))
async def test_sql_mode_list_retype(bq_lists, sql: str, column: str, element: DataType) -> None:
    storage, ds = bq_lists
    engine = SlayerQueryEngine(storage=storage)
    await storage.save_model(stale_sql_model(sql=sql, column=column, data_source=ds.name))
    await check_validation_finding(engine, data_source=ds.name, column=column)
    await check_query_carries_finding(engine, column=column)
    await check_reingest_retypes(engine, datasource=ds, table="contractors", column=column, element=element)
