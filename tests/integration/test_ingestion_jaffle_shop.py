"""End-to-end auto-ingestion test against the Jaffle Shop DuckDB.

Generate the Jaffle Shop demo into a temp DuckDB, call
``ingest_datasource_async``, and assert
the resulting datasets conform to the v2 unified-fields shape (one ``DatasetField``
list per dataset, ``metrics`` library left empty by ingestion, no dotted
field names, relationships resolved to direct FK pairs only). Also round-trips the
ingested datasets through YAMLStorage to confirm no v1 keys leak to disk.
"""

import asyncio
import os
import shutil
import tempfile
from pathlib import Path

import pytest
import yaml

import duckdb

from slayer.async_utils import run_sync
from slayer.core.enums import DataType
from slayer.core.format import NumberFormatType
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.demo.jaffle_shop import (
    create_schema,
    generate_data,
    load_data,
)
from slayer.live_schema import ingestion
from slayer.database.client import SlayerSQLClient
from slayer.storage import migrations as mig
from slayer.storage.yaml_storage import YAMLStorage

from tests.integration._demo_build import NOTEBOOK_DEMO_DB

pytestmark = pytest.mark.integration



@pytest.fixture(scope="module")
def jaffle_duckdb_path(tmp_path_factory: pytest.TempPathFactory) -> str:
    """Path to a Jaffle Shop DuckDB file. Reuses the project-wide cached DB
    at docs/examples/jaffle_data/demo/jaffle_shop.duckdb when present; falls
    back to ``jafgen`` for fresh checkouts."""
    tmpdir = tmp_path_factory.mktemp("jaffle_ingest")
    db_path = tmpdir / "jaffle_ingest.duckdb"

    if NOTEBOOK_DEMO_DB.exists():
        shutil.copy(NOTEBOOK_DEMO_DB, db_path)
        return str(db_path)

    try:
        data_dir = generate_data(output_dir=str(tmpdir), years=1)
    except (FileNotFoundError, RuntimeError) as exc:
        pytest.skip(f"Jaffle shop prerequisite missing: {exc}")

    conn = duckdb.connect(str(db_path))
    try:
        create_schema(conn=conn)
        load_data(conn=conn, data_dir=data_dir)
    finally:
        conn.close()
    return str(db_path)


async def _ingest(ds: DatasourceConfig) -> list[SlayerDataset]:
    client = SlayerSQLClient(datasource=ds)
    try:
        return await ingestion.ingest_datasource_async(ds, client=client)
    finally:
        await client.aclose()
        client.close()


@pytest.fixture(scope="module")
def jaffle_datasets(jaffle_duckdb_path: str) -> dict[str, SlayerDataset]:
    """Ingest the Jaffle Shop DuckDB and return datasets keyed by name."""
    ds = DatasourceConfig(
        name="jaffle_test",
        type="duckdb",
        database=jaffle_duckdb_path,
    )
    models = run_sync(_ingest(ds))
    return {m.name: m for m in models}


# Expected non-joined fields per ingested dataset. The values are dicts of
# ``field_name -> (DataType, primary_key, format_type_or_None)``. Fields
# whose source name contained a dot (joined fields) are excluded by
# ``_fields_to_dataset`` and so are not listed here.
_EXPECTED_FIELDS: dict[str, dict[str, tuple]] = {
    "customers": {
        "id": (DataType.STRING, True, None),
        "name": (DataType.STRING, False, None),
    },
    "regions": {
        "id": (DataType.STRING, True, None),
        "name": (DataType.STRING, False, None),
    },
    "stores": {
        "id": (DataType.STRING, True, None),
        "name": (DataType.STRING, False, None),
        "opened_at": (DataType.DATE, False, None),
        "tax_rate": (DataType.FLOAT, False, NumberFormatType.FLOAT),
        "region_id": (DataType.STRING, False, None),
    },
    "products": {
        "sku": (DataType.STRING, True, None),
        "name": (DataType.STRING, False, None),
        "type": (DataType.STRING, False, None),
        "price": (DataType.DECIMAL, False, NumberFormatType.FLOAT),
        "description": (DataType.STRING, False, None),
        "ingredients": (DataType.ARRAY, False, None),
    },
    "orders": {
        "id": (DataType.STRING, True, None),
        "customer_id": (DataType.STRING, False, None),
        "ordered_at": (DataType.DATE, False, None),
        "store_id": (DataType.STRING, False, None),
        "subtotal": (DataType.DECIMAL, False, NumberFormatType.FLOAT),
        "tax_paid": (DataType.DECIMAL, False, NumberFormatType.FLOAT),
        "order_total": (DataType.DECIMAL, False, NumberFormatType.FLOAT),
        "status": (DataType.STRING, False, None),
        "item_skus": (DataType.ARRAY, False, None),
    },
    "items": {
        "id": (DataType.STRING, True, None),
        "order_id": (DataType.STRING, False, None),
        "sku": (DataType.STRING, False, None),
    },
    "supplies": {
        "id": (DataType.STRING, True, None),
        "name": (DataType.STRING, False, None),
        "cost": (DataType.DECIMAL, False, NumberFormatType.FLOAT),
        "perishable": (DataType.STRING, False, None),
        "sku": (DataType.STRING, True, None),
    },
    "tweets": {
        "id": (DataType.STRING, True, None),
        "user_id": (DataType.STRING, False, None),
        "tweeted_at": (DataType.DATE, False, None),
        "content": (DataType.STRING, False, None),
    },
}


# Expected direct relationships per dataset. Values are sets of (target_dataset, src_col, tgt_col).
_EXPECTED_RELATIONSHIPS: dict[str, set] = {
    "customers": set(),
    "regions": set(),
    "stores": {
        ("regions", "region_id", "id"),
    },
    "products": set(),
    "orders": {
        ("customers", "customer_id", "id"),
        ("stores", "store_id", "id"),
    },
    "items": {
        ("orders", "order_id", "id"),
        ("products", "sku", "sku"),
    },
    "supplies": {
        ("products", "sku", "sku"),
    },
    "tweets": {
        ("customers", "user_id", "id"),
    },
}


def test_all_jaffle_models_produced(jaffle_datasets: dict[str, SlayerDataset]) -> None:
    assert set(jaffle_datasets.keys()) == set(_EXPECTED_FIELDS.keys())


@pytest.mark.parametrize("model_name", sorted(_EXPECTED_FIELDS.keys()))
def test_jaffle_model_columns_v2_shape(jaffle_datasets: dict[str, SlayerDataset], model_name: str) -> None:
    model: SlayerDataset = jaffle_datasets[model_name]
    expected = _EXPECTED_FIELDS[model_name]

    by_name = {c.name: c for c in model.fields}
    assert set(by_name.keys()) == set(expected.keys())

    for col_name, (data_type, is_pk, fmt_type) in expected.items():
        col = by_name[col_name]
        assert col.datatype == data_type, f"{model_name}.{col_name}: type"
        assert col.primary_key == is_pk, f"{model_name}.{col_name}: primary_key"
        if fmt_type is None:
            assert col.format is None, f"{model_name}.{col_name}: format should be None"
        else:
            assert col.format is not None, f"{model_name}.{col_name}: format should be set"
            assert col.format.type == fmt_type, f"{model_name}.{col_name}: format.type"


def test_jaffle_models_have_empty_measures_library(jaffle_datasets: dict[str, SlayerDataset]) -> None:
    """Auto-ingestion never populates the named-metrics expression library."""
    for name, model in jaffle_datasets.items():
        assert model.metrics == [], f"{name} should have metrics == []"


def test_jaffle_no_dotted_column_names(jaffle_datasets: dict[str, SlayerDataset]) -> None:
    """Joined fields live on the target dataset and must not leak into the source."""
    for name, model in jaffle_datasets.items():
        for col in model.fields:
            assert "." not in col.name, (
                f"{name}.{col.name}: ingestion should never emit dotted field names"
            )


@pytest.mark.parametrize("model_name", sorted(_EXPECTED_RELATIONSHIPS.keys()))
def test_jaffle_model_joins(jaffle_datasets: dict[str, SlayerDataset], model_name: str) -> None:
    model = jaffle_datasets[model_name]
    expected = _EXPECTED_RELATIONSHIPS[model_name]

    actual: set = set()
    for j in model.relationships:
        # Each relationship in auto-ingestion has exactly one column pair (direct FKs only).
        assert len(j.column_pairs) == 1, f"{model_name} → {j.to}: expected one pair"
        src_col, tgt_col = j.column_pairs[0]
        actual.add((j.to, src_col, tgt_col))

    assert actual == expected


async def test_jaffle_yaml_round_trip_omits_v1_keys(jaffle_datasets: dict[str, SlayerDataset]) -> None:
    """Saving an ingested dataset to YAML must produce current-schema shape with no v1 keys."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        for model in jaffle_datasets.values():
            await storage.save_dataset(model)

        for model_name, model in jaffle_datasets.items():
            # v4: files are nested under models/<data_source>/<name>.yaml.
            path = os.path.join(
                storage.models_dir, model.data_source, f"{model_name}.yaml"
            )
            raw = await asyncio.to_thread(Path(path).read_text)
            on_disk = yaml.safe_load(raw)
            assert on_disk["version"] == mig.CURRENT_VERSIONS["SlayerModel"], model_name
            assert "fields" in on_disk, model_name
            assert "dimensions" not in on_disk, (
                f"{model_name}: v1 'dimensions' key should not be on disk"
            )
            # ``metrics`` MAY appear (as []) in the DatasetMetric library;
            # if present, it must not contain v1 measure shape (sql + agg etc).
            for entry in on_disk.get("metrics") or []:
                assert "expression" in entry, f"{model_name}: metric entry must carry 'expression'"
