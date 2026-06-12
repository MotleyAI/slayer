"""v6 → v7 schema migration for SlayerModel (DEV-1480).

v7 adds two new optional fields:
- ``Column.sampled_values: Optional[List[str]]`` — structured sibling to the
  ``sampled`` text string, carrying up to 50 top-by-frequency distinct values.
- ``Column.distinct_count: Optional[int]`` — total distinct values at profile
  time (always set when a categorical column is profiled).

The migration itself is a no-op forward — existing v6 payloads load with both
new fields = None on every column; first subsequent ingest / refresh-samples
populates the cache.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile

import pytest
import yaml

from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.storage import migrations as mig
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


def test_current_slayer_model_version_is_v7() -> None:
    assert mig.CURRENT_VERSIONS["SlayerModel"] == 7


def test_slayer_model_default_version_is_v7() -> None:
    m = SlayerModel(name="orders", sql_table="orders", data_source="ds")
    assert m.version == 7


def test_slayer_model_dump_writes_v7() -> None:
    m = SlayerModel(name="orders", sql_table="orders", data_source="ds")
    assert m.model_dump(mode="json", exclude_none=True)["version"] == 7


def test_v6_to_v7_no_op_forward() -> None:
    """The v6→v7 step migrator is a no-op — the new fields default to None
    via Pydantic validation, not via the migrator."""
    step = mig._REGISTRY[("SlayerModel", 6)]
    out = step({
        "version": 6,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "ds",
        "columns": [{"name": "status", "type": "TEXT", "sampled": "paid"}],
    })
    # The dict is returned untouched — the new fields are not introduced by
    # the migrator; the orchestrator bumps the ``version`` key.
    assert out["columns"] == [
        {"name": "status", "type": "TEXT", "sampled": "paid"},
    ]


def test_v6_payload_loads_with_new_fields_none() -> None:
    """A persisted v6 model with ``sampled`` set but no ``sampled_values`` /
    ``distinct_count`` loads cleanly and both new fields default to None."""
    raw = {
        "version": 6,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "ds",
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "status", "type": "TEXT", "sampled": "paid, refunded"},
            {"name": "amount", "type": "DOUBLE", "sampled": "0.0 .. 9999.99"},
        ],
    }
    m = SlayerModel.model_validate(raw)
    assert m.version == 7
    # The new optional fields default to None — no explicit migration step,
    # just Pydantic field defaults.
    for col in m.columns:
        assert col.sampled_values is None
        assert col.distinct_count is None
    # The existing ``sampled`` text values survive untouched.
    assert m.get_column("status").sampled == "paid, refunded"
    assert m.get_column("amount").sampled == "0.0 .. 9999.99"


def test_v7_payload_round_trips_with_populated_fields() -> None:
    """A v7 payload carrying both new fields round-trips through
    ``model_validate`` and ``model_dump`` losslessly."""
    raw = {
        "version": 7,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "ds",
        "columns": [
            {
                "name": "status",
                "type": "TEXT",
                "sampled": "paid, refunded, cancelled",
                "sampled_values": ["paid", "refunded", "cancelled"],
                "distinct_count": 3,
            },
        ],
    }
    m = SlayerModel.model_validate(raw)
    assert m.version == 7
    col = m.columns[0]
    assert col.sampled == "paid, refunded, cancelled"
    assert col.sampled_values == ["paid", "refunded", "cancelled"]
    assert col.distinct_count == 3
    dumped = m.model_dump(mode="json", exclude_none=True)
    assert dumped["columns"][0]["sampled_values"] == ["paid", "refunded", "cancelled"]
    assert dumped["columns"][0]["distinct_count"] == 3


def test_v7_payload_with_overflow_marker_round_trips() -> None:
    """Overflow case: ``sampled_values`` carries the top-50 list,
    ``distinct_count`` carries the true total which exceeds 50."""
    raw = {
        "version": 7,
        "name": "households",
        "sql_table": "households",
        "data_source": "ds",
        "columns": [
            {
                "name": "city",
                "type": "TEXT",
                "sampled": "São Paulo, Rio, ... (1234 distinct)",
                "sampled_values": ["São Paulo", "Rio"],  # truncated to 2 for brevity
                "distinct_count": 1234,
            },
        ],
    }
    m = SlayerModel.model_validate(raw)
    col = m.columns[0]
    assert col.distinct_count == 1234
    assert col.sampled_values == ["São Paulo", "Rio"]


def test_v7_payload_with_empty_list_round_trips() -> None:
    """All-NULL categorical column: ``sampled_values=[]``, ``distinct_count=0``,
    text ``sampled=""``."""
    raw = {
        "version": 7,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "ds",
        "columns": [
            {
                "name": "notes",
                "type": "TEXT",
                "sampled": "",
                "sampled_values": [],
                "distinct_count": 0,
            },
        ],
    }
    m = SlayerModel.model_validate(raw)
    col = m.columns[0]
    assert col.sampled == ""
    assert col.sampled_values == []
    assert col.distinct_count == 0


# ---------------------------------------------------------------------------
# Round-trip via storage backends
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_yaml_round_trips_v6_payload_to_v7_with_new_fields_none() -> None:
    """Seed a raw ``version: 6`` YAML file directly on disk and confirm
    ``get_model`` runs the v6→v7 migration so both new fields default to
    None on every column."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))

        v6_path = os.path.join(tmpdir, "models", "ds", "orders.yaml")
        os.makedirs(os.path.dirname(v6_path), exist_ok=True)
        with open(v6_path, "w") as f:  # NOSONAR(S7493) — test seed; mirrors YAMLStorage's sync-in-async pattern
            yaml.dump({
                "version": 6,
                "name": "orders",
                "sql_table": "orders",
                "data_source": "ds",
                "columns": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "status", "type": "TEXT", "sampled": "paid"},
                ],
            }, f, sort_keys=False)

        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        assert loaded.version == 7
        for col in loaded.columns:
            assert col.sampled_values is None
            assert col.distinct_count is None


@pytest.mark.asyncio
async def test_sqlite_round_trips_v6_payload_to_v7_with_new_fields_none() -> None:
    """Same as the YAML test, but seeded directly into the SQLite
    ``models`` table via raw SQL so the v6→v7 migration actually runs on
    load."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/storage.db"
        storage = SQLiteStorage(db_path=db_path)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))

        v6_payload = {
            "version": 6,
            "name": "orders",
            "sql_table": "orders",
            "data_source": "ds",
            "columns": [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "status", "type": "TEXT", "sampled": "paid"},
            ],
        }
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)",
                ("ds", "orders", json.dumps(v6_payload)),
            )

        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        assert loaded.version == 7
        for col in loaded.columns:
            assert col.sampled_values is None
            assert col.distinct_count is None


# ---------------------------------------------------------------------------
# Backward compat: v5 → v6 → v7 chain still works
# ---------------------------------------------------------------------------


def test_v5_payload_walks_through_chain_to_v7() -> None:
    """End-to-end migration from v5 through v6 to v7 via the orchestrator."""
    raw = {
        "version": 5,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "ds",
        "columns": [{"name": "amount", "type": "DOUBLE"}],
    }
    out = mig.migrate("SlayerModel", raw)
    assert out["version"] == 7
