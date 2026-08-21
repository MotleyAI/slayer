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
from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy_bigquery")

from google.api_core.exceptions import Forbidden  # noqa: E402
from google.auth.exceptions import DefaultCredentialsError  # noqa: E402
from google.cloud import bigquery  # noqa: E402

from slayer.core.enums import DataType  # noqa: E402
from slayer.core.models import DatasourceConfig  # noqa: E402
from slayer.engine.ingestion import (  # noqa: E402
    ingest_datasource,
    ingest_datasource_idempotent,
)
from slayer.storage.yaml_storage import YAMLStorage  # noqa: E402

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


@pytest.fixture(scope="module")
def bq_models(bq_dataset):
    _, dataset_id = bq_dataset
    models = ingest_datasource(
        datasource=_ds_config(dataset_id), schema=dataset_id
    )
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
        self, bq_dataset, tmp_path: Path
    ) -> None:
        _, dataset_id = bq_dataset
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        ds = _ds_config(dataset_id)
        await storage.save_datasource(ds)

        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, schema=dataset_id
        )
        assert result.datasource_described is True
        loaded = await storage.get_datasource("bqtest")
        assert loaded.description == _DATASET_DESCRIPTION

        # Re-ingest with the reloaded datasource: no-op, nothing re-reported.
        result2 = await ingest_datasource_idempotent(
            datasource=loaded, storage=storage, schema=dataset_id
        )
        assert result2.datasource_described is False

    async def test_preset_description_untouched(
        self, bq_dataset, tmp_path: Path
    ) -> None:
        _, dataset_id = bq_dataset
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        ds = _ds_config(dataset_id, name="bqtest2", description="user text")
        await storage.save_datasource(ds)

        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, schema=dataset_id
        )
        assert result.datasource_described is False
        loaded = await storage.get_datasource("bqtest2")
        assert loaded.description == "user text"


class TestBigQueryReingest:
    async def test_fill_if_empty_and_preservation(
        self, bq_dataset, tmp_path: Path
    ) -> None:
        _, dataset_id = bq_dataset
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        ds = _ds_config(dataset_id)
        await storage.save_datasource(ds)
        await ingest_datasource_idempotent(
            datasource=ds, storage=storage, schema=dataset_id
        )

        loaded = await storage.get_model("orders", data_source="bqtest")
        for c in loaded.columns:
            if c.name == "amount":
                c.description = None
            if c.name == "status":
                c.description = "hand-authored"
        await storage.save_model(loaded)

        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, schema=dataset_id
        )
        addition = next(a for a in result.additions if a.model_name == "orders")
        assert addition.described_columns == ["amount"]
        loaded2 = await storage.get_model("orders", data_source="bqtest")
        cols = {c.name: c for c in loaded2.columns}
        assert cols["amount"].description == "Order amount in USD"
        assert cols["status"].description == "hand-authored"

    async def test_new_commented_column_arrives_with_description(
        self, bq_dataset, tmp_path: Path
    ) -> None:
        client, dataset_id = bq_dataset
        storage = YAMLStorage(base_dir=str(tmp_path / "storage"))
        ds = _ds_config(dataset_id)
        await storage.save_datasource(ds)
        await ingest_datasource_idempotent(
            datasource=ds, storage=storage, schema=dataset_id
        )

        table = client.get_table(f"{_PROJECT}.{dataset_id}.orders")
        table.schema = list(table.schema) + [
            bigquery.SchemaField("discount", "FLOAT", description="Discount applied"),
        ]
        client.update_table(table, ["schema"])

        result = await ingest_datasource_idempotent(
            datasource=ds, storage=storage, schema=dataset_id
        )
        addition = next(a for a in result.additions if a.model_name == "orders")
        assert "discount" in addition.new_columns
        assert "discount" not in addition.described_columns
        loaded = await storage.get_model("orders", data_source="bqtest")
        discount = next(c for c in loaded.columns if c.name == "discount")
        assert discount.description == "Discount applied"
