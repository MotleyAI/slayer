"""End-to-end auto-ingestion test against the Jaffle Shop DuckDB.

This is the integration test called out in S4 (issue #48): generate the
Jaffle Shop demo into a temp DuckDB, call ``ingest_datasource``, and assert
the resulting models conform to the v2 unified-columns shape (one ``Column``
list per model, ``measures`` library left empty by ingestion, no dotted
column names, joins resolved to direct FK pairs only). Also round-trips the
ingested models through YAMLStorage to confirm no v1 keys leak to disk.
"""

import asyncio
import os
import shutil
import tempfile
from pathlib import Path
from typing import Dict

import pytest
import yaml

pytest.importorskip("duckdb")
pytest.importorskip("jafgen")

import duckdb

from slayer.core.enums import DataType
from slayer.core.format import NumberFormatType
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.demo.jaffle_shop import (
    DemoDependencyError,
    create_schema,
    generate_data,
    load_data,
)
from slayer.engine.ingestion import ingest_datasource
from slayer.storage import migrations as mig
from slayer.storage.yaml_storage import YAMLStorage


pytestmark = pytest.mark.integration

# Cached 3-year demo DB maintained by tests/integration/test_notebooks.py's
# session-scoped _ensure_jaffle_db fixture. Reusing it avoids ~6s of
# `jafgen` subprocess time per pytest invocation.
_CACHED_JAFFLE_DB = (
    Path(__file__).resolve().parent.parent.parent
    / "docs" / "examples" / "jaffle_data" / "demo" / "jaffle_shop.duckdb"
)


@pytest.fixture(scope="module")
def jaffle_duckdb_path(tmp_path_factory):
    """Path to a Jaffle Shop DuckDB file. Reuses the project-wide cached DB
    at docs/examples/jaffle_data/demo/jaffle_shop.duckdb when present; falls
    back to ``jafgen`` for fresh checkouts."""
    tmpdir = tmp_path_factory.mktemp("jaffle_ingest")
    db_path = tmpdir / "jaffle_ingest.duckdb"

    if _CACHED_JAFFLE_DB.exists():
        shutil.copy(_CACHED_JAFFLE_DB, db_path)
        return str(db_path)

    try:
        data_dir = generate_data(output_dir=str(tmpdir), years=1)
    except DemoDependencyError as exc:
        pytest.skip(f"Jaffle shop prerequisite missing: {exc}")
    except (FileNotFoundError, RuntimeError) as exc:
        pytest.skip(f"Jaffle shop prerequisite missing: {exc}")

    conn = duckdb.connect(str(db_path))
    try:
        create_schema(conn=conn)
        load_data(conn=conn, data_dir=data_dir)
    finally:
        conn.close()
    return str(db_path)


@pytest.fixture(scope="module")
def jaffle_models(jaffle_duckdb_path):
    """Ingest the Jaffle Shop DuckDB and return models keyed by name."""
    ds = DatasourceConfig(
        name="jaffle_test",
        type="duckdb",
        database=jaffle_duckdb_path,
    )
    models = ingest_datasource(datasource=ds)
    return {m.name: m for m in models}


# Expected non-joined columns per ingested model. The values are dicts of
# ``column_name -> (DataType, primary_key, format_type_or_None)``. Columns
# whose source name contained a dot (joined columns) are excluded by
# ``_columns_to_model`` and so are not listed here.
_EXPECTED_COLUMNS: Dict[str, Dict[str, tuple]] = {
    "customers": {
        "id": (DataType.TEXT, True, None),
        "name": (DataType.TEXT, False, None),
    },
    "stores": {
        "id": (DataType.TEXT, True, None),
        "name": (DataType.TEXT, False, None),
        "opened_at": (DataType.DATE, False, None),
        "tax_rate": (DataType.DOUBLE, False, NumberFormatType.FLOAT),
    },
    "products": {
        "sku": (DataType.TEXT, True, None),
        "name": (DataType.TEXT, False, None),
        "type": (DataType.TEXT, False, None),
        "price": (DataType.DOUBLE, False, NumberFormatType.FLOAT),
        "description": (DataType.TEXT, False, None),
    },
    "orders": {
        "id": (DataType.TEXT, True, None),
        "customer_id": (DataType.TEXT, False, None),
        "ordered_at": (DataType.DATE, False, None),
        "store_id": (DataType.TEXT, False, None),
        "subtotal": (DataType.DOUBLE, False, NumberFormatType.FLOAT),
        "tax_paid": (DataType.DOUBLE, False, NumberFormatType.FLOAT),
        "order_total": (DataType.DOUBLE, False, NumberFormatType.FLOAT),
    },
    "order_items": {
        "id": (DataType.TEXT, True, None),
        "order_id": (DataType.TEXT, False, None),
        "sku": (DataType.TEXT, False, None),
        # DEV-1361: integer DB column now narrows to DataType.INT.
        "quantity": (DataType.INT, False, NumberFormatType.INTEGER),
    },
    "supplies": {
        "id": (DataType.TEXT, True, None),
        "name": (DataType.TEXT, False, None),
        "cost": (DataType.DOUBLE, False, NumberFormatType.FLOAT),
        "perishable": (DataType.TEXT, False, None),
        "sku": (DataType.TEXT, True, None),
    },
    "tweets": {
        "id": (DataType.TEXT, True, None),
        "user_id": (DataType.TEXT, False, None),
        "tweeted_at": (DataType.DATE, False, None),
        "content": (DataType.TEXT, False, None),
    },
}


# Expected direct joins per model. Values are sets of (target_model, src_col, tgt_col).
_EXPECTED_JOINS: Dict[str, set] = {
    "customers": set(),
    "stores": set(),
    "products": set(),
    "orders": {
        ("customers", "customer_id", "id"),
        ("stores", "store_id", "id"),
    },
    "order_items": {
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


def test_all_jaffle_models_produced(jaffle_models):
    assert set(jaffle_models.keys()) == set(_EXPECTED_COLUMNS.keys())


@pytest.mark.parametrize("model_name", sorted(_EXPECTED_COLUMNS.keys()))
def test_jaffle_model_columns_v2_shape(jaffle_models, model_name):
    model: SlayerModel = jaffle_models[model_name]
    expected = _EXPECTED_COLUMNS[model_name]

    by_name = {c.name: c for c in model.columns}
    assert set(by_name.keys()) == set(expected.keys())

    for col_name, (data_type, is_pk, fmt_type) in expected.items():
        col = by_name[col_name]
        assert col.type == data_type, f"{model_name}.{col_name}: type"
        assert col.primary_key == is_pk, f"{model_name}.{col_name}: primary_key"
        if fmt_type is None:
            assert col.format is None, f"{model_name}.{col_name}: format should be None"
        else:
            assert col.format is not None, f"{model_name}.{col_name}: format should be set"
            assert col.format.type == fmt_type, f"{model_name}.{col_name}: format.type"


def test_jaffle_models_have_empty_measures_library(jaffle_models):
    """Auto-ingestion never populates the named-measures formula library."""
    for name, model in jaffle_models.items():
        assert model.measures == [], f"{name} should have measures == []"


def test_jaffle_no_dotted_column_names(jaffle_models):
    """Joined columns live on the target model and must not leak into the source."""
    for name, model in jaffle_models.items():
        for col in model.columns:
            assert "." not in col.name, (
                f"{name}.{col.name}: ingestion should never emit dotted column names"
            )


@pytest.mark.parametrize("model_name", sorted(_EXPECTED_JOINS.keys()))
def test_jaffle_model_joins(jaffle_models, model_name):
    model = jaffle_models[model_name]
    expected = _EXPECTED_JOINS[model_name]

    actual: set = set()
    for j in model.joins:
        # Each join in auto-ingestion has exactly one column pair (direct FKs only).
        assert len(j.join_pairs) == 1, f"{model_name} → {j.target_model}: expected one pair"
        src_col, tgt_col = j.join_pairs[0]
        actual.add((j.target_model, src_col, tgt_col))

    assert actual == expected


async def test_jaffle_yaml_round_trip_omits_v1_keys(jaffle_models):
    """Saving an ingested model to YAML must produce current-schema shape with no v1 keys."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        for model in jaffle_models.values():
            await storage.save_model(model)

        for model_name, model in jaffle_models.items():
            # v4: files are nested under models/<data_source>/<name>.yaml.
            path = os.path.join(
                storage.models_dir, model.data_source, f"{model_name}.yaml"
            )
            raw = await asyncio.to_thread(Path(path).read_text)
            on_disk = yaml.safe_load(raw)
            assert on_disk["version"] == mig.CURRENT_VERSIONS["SlayerModel"], model_name
            assert "columns" in on_disk, model_name
            assert "dimensions" not in on_disk, (
                f"{model_name}: v1 'dimensions' key should not be on disk"
            )
            # ``measures`` MAY appear (as []) in the v2 ModelMeasure library;
            # if present, it must not contain v1 measure shape (sql + agg etc).
            for entry in on_disk.get("measures") or []:
                assert "formula" in entry, f"{model_name}: v2 measure entry must carry 'formula'"
