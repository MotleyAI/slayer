"""v5 → v6 schema migration for SlayerModel (DEV-1375).

v6 adds a single new optional field: ``Column.sampled: Optional[str] = None``.
The migration itself is a no-op forward — existing payloads load with
``sampled=None`` everywhere; first subsequent ingest / refresh-samples
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


def test_v5_to_v6_no_op_forward() -> None:
    """Pin the v5→v6 step in isolation, independent of CURRENT_VERSIONS.

    DEV-1480 bumps CURRENT_VERSIONS to 7, so calling ``mig.migrate(...)``
    would walk past v6 to v7. Use the per-step registry entry instead.
    """
    step = mig._REGISTRY[("SlayerModel", 5)]
    out = step({
        "version": 5,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "ds",
        "columns": [{"name": "id", "type": "INT"}],
    })
    # The v5→v6 step is a no-op forward — the migrator does not bump the
    # ``version`` key itself (the orchestrator does). It returns the dict
    # untouched.
    assert out["columns"] == [{"name": "id", "type": "INT"}]


def test_v5_payload_loads_with_sampled_none() -> None:
    """A v5 payload still loads cleanly. After DEV-1480 the chain walks
    v5 → v6 → v7, so the resulting model is current-version; the v5→v6 step
    contribution we pin here is that ``sampled`` defaults to None when
    absent."""
    raw = {
        "version": 5,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "ds",
        "columns": [
            {"name": "id", "type": "INT", "primary_key": True},
            {"name": "amount", "type": "DOUBLE"},
        ],
    }
    m = SlayerModel.model_validate(raw)
    for col in m.columns:
        assert col.sampled is None


def test_v6_payload_round_trips_with_sampled_value() -> None:
    """A v6 payload with a populated ``sampled`` string survives the walk
    forward to the current version unchanged. Pins that the v5→v6 leg's
    output shape (with ``sampled``) is the input shape for v6→v7."""
    raw = {
        "version": 6,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "ds",
        "columns": [
            {"name": "amount", "type": "DOUBLE", "sampled": "0.0 .. 9999.99"},
        ],
    }
    m = SlayerModel.model_validate(raw)
    assert m.columns[0].sampled == "0.0 .. 9999.99"
    dumped = m.model_dump(mode="json", exclude_none=True)
    assert dumped["columns"][0]["sampled"] == "0.0 .. 9999.99"


# ---------------------------------------------------------------------------
# Round-trip via storage backends
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_yaml_round_trips_v5_payload_to_v6_with_sampled_none() -> None:
    """Seed a raw ``version: 5`` YAML file directly on disk (no
    ``sampled`` field) and confirm ``get_model`` runs the v5→v6 migration
    so the loaded model is v6 with ``sampled=None`` on every column."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))

        v5_path = os.path.join(tmpdir, "models", "ds", "orders.yaml")
        os.makedirs(os.path.dirname(v5_path), exist_ok=True)
        with open(v5_path, "w") as f:  # NOSONAR(S7493) — test seeds a tempfile; mirrors YAMLStorage's sync-in-async pattern
            yaml.dump({
                "version": 5,
                "name": "orders",
                "sql_table": "orders",
                "data_source": "ds",
                "columns": [
                    {"name": "id", "type": "INT", "primary_key": True},
                    {"name": "amount", "type": "DOUBLE"},
                ],
            }, f, sort_keys=False)

        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        assert {c.name: c.sampled for c in loaded.columns} == {
            "id": None, "amount": None,
        }


@pytest.mark.asyncio
async def test_sqlite_round_trips_v5_payload_to_v6_with_sampled_none() -> None:
    """Same as the YAML test, but seeded directly into the SQLite
    ``models`` table via raw SQL so the v5→v6 migration actually runs on
    load."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = f"{tmpdir}/storage.db"
        storage = SQLiteStorage(db_path=db_path)
        await storage.save_datasource(DatasourceConfig(
            name="ds", type="sqlite", database=":memory:",
        ))

        v5_payload = {
            "version": 5,
            "name": "orders",
            "sql_table": "orders",
            "data_source": "ds",
            "columns": [
                {"name": "id", "type": "INT", "primary_key": True},
                {"name": "amount", "type": "DOUBLE"},
            ],
        }
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO models (data_source, name, data) VALUES (?, ?, ?)",
                ("ds", "orders", json.dumps(v5_payload)),
            )

        loaded = await storage.get_model("orders", data_source="ds")
        assert loaded is not None
        assert {c.name: c.sampled for c in loaded.columns} == {
            "id": None, "amount": None,
        }


# ---------------------------------------------------------------------------
# Backward compat: v4 → v5 → v6 chain still works (v5→v6 leg in isolation)
# ---------------------------------------------------------------------------


def test_v4_payload_walks_through_chain_to_v6() -> None:
    """Walk v4 input through the v4→v5 then v5→v6 step migrators directly.

    Independent of CURRENT_VERSIONS so DEV-1480's bump to v7 doesn't break
    the assertion that the v5→v6 leg preserves the v4→v5 output shape.
    """
    raw = {
        "version": 4,
        "name": "orders",
        "sql_table": "orders",
        "data_source": "ds",
        "columns": [{"name": "amount", "type": "number"}],  # legacy lowercase v4
    }
    after_v4_v5 = mig._REGISTRY[("SlayerModel", 4)](dict(raw))
    after_v5_v6 = mig._REGISTRY[("SlayerModel", 5)](dict(after_v4_v5))
    # v4→v5 normalised the legacy lowercase to canonical "DOUBLE"
    assert after_v5_v6["columns"][0]["type"] == "DOUBLE"
