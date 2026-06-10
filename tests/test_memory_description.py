"""DEV-1549: Memory.description field — model-layer + storage round-trip.

Tests cover:
* Optional ``description`` field accepted on ``Memory``.
* Empty / whitespace-only ``description`` normalised to ``None``
  (Codex finding #1).
* ``description`` longer than 500 chars hard-rejected at the Pydantic
  layer.
* Boundary at exactly 500 chars accepted.
* ``description`` round-trips through YAMLStorage and SQLiteStorage.
* Legacy v2 row without a ``description`` key loads with
  ``description=None`` (no migrator needed; additive optional field).
* ``Memory.learning`` whitespace-only rejected at the Pydantic layer
  (Codex finding #4).
* ``MemoryService.save_memory`` propagates ``description`` and
  surfaces over-length as a friendly error.
"""

from __future__ import annotations

import asyncio
import os
import tempfile

import pytest
import yaml
from pydantic import ValidationError

from slayer.core.models import Column, DatasourceConfig, SlayerModel
from slayer.memories.models import Memory
from slayer.memories.service import MemoryService
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


# ---------------------------------------------------------------------------
# Pydantic-layer behaviour
# ---------------------------------------------------------------------------


def test_description_field_exists_and_defaults_none() -> None:
    mem = Memory(learning="x")
    assert mem.description is None


def test_description_accepts_short_string() -> None:
    mem = Memory(learning="x", description="short summary")
    assert mem.description == "short summary"


def test_description_empty_string_normalised_to_none() -> None:
    """Codex#1: empty string is not a deliberate-empty preview."""
    mem = Memory(learning="x", description="")
    assert mem.description is None


def test_description_whitespace_only_normalised_to_none() -> None:
    """Codex#1: whitespace-only is treated as absent."""
    mem = Memory(learning="x", description="   \n\t  ")
    assert mem.description is None


def test_description_at_500_chars_accepted() -> None:
    long = "a" * 500
    mem = Memory(learning="x", description=long)
    assert mem.description == long


def test_description_over_500_chars_rejected() -> None:
    with pytest.raises(ValidationError) as excinfo:
        Memory(learning="x", description="a" * 501)
    assert "500" in str(excinfo.value)


def test_learning_required_and_non_empty() -> None:
    with pytest.raises(ValidationError):
        Memory(learning="")


def test_learning_whitespace_only_rejected_at_pydantic_layer() -> None:
    """Codex#4: model-layer enforcement, not just service-layer."""
    with pytest.raises(ValidationError):
        Memory(learning="   \n\t ")


# ---------------------------------------------------------------------------
# Schema snapshot: description in JSON schema
# ---------------------------------------------------------------------------


def test_memory_json_schema_includes_description() -> None:
    schema = Memory.model_json_schema()
    assert "description" in schema["properties"]
    # Optional[str] → type allows null
    prop = schema["properties"]["description"]
    # Pydantic emits anyOf with null for Optional[str]; either shape is fine.
    assert "string" in str(prop) or "anyOf" in prop


def test_memory_json_schema_keeps_learning_required() -> None:
    schema = Memory.model_json_schema()
    assert "learning" in schema.get("required", [])
    # description is optional → must not appear in required
    assert "description" not in schema.get("required", [])


# ---------------------------------------------------------------------------
# Storage round-trip — YAML + SQLite
# ---------------------------------------------------------------------------


async def _seed_orders(storage) -> None:
    await storage.save_datasource(
        DatasourceConfig(name="mydb", type="sqlite", database=":memory:")
    )
    await storage.save_model(SlayerModel(
        name="orders",
        sql_table="orders",
        data_source="mydb",
        columns=[
            Column(name="id", sql="id", primary_key=True),
            Column(name="amount", sql="amount"),
        ],
    ))


@pytest.mark.asyncio
async def test_description_round_trip_yaml() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await _seed_orders(storage)
        saved = await storage.save_memory(
            learning="amount in cents",
            entities=["mydb.orders.amount"],
            description="cents column note",
        )
        reloaded = await storage.get_memory(saved.id)
        assert reloaded.description == "cents column note"
        assert reloaded.learning == "amount in cents"


@pytest.mark.asyncio
async def test_description_round_trip_sqlite() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "store.db")
        storage = SQLiteStorage(db_path=db_path)
        await _seed_orders(storage)
        saved = await storage.save_memory(
            learning="amount in cents",
            entities=["mydb.orders.amount"],
            description="cents column note",
        )
        reloaded = await storage.get_memory(saved.id)
        assert reloaded.description == "cents column note"


@pytest.mark.asyncio
async def test_description_yaml_persists_on_disk() -> None:
    """memories.yaml on disk literally contains the description field."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await _seed_orders(storage)
        await storage.save_memory(
            learning="x",
            entities=["mydb.orders.amount"],
            description="d",
        )
        memories_path = os.path.join(tmpdir, "memories.yaml")

        def _read_yaml() -> list:
            with open(memories_path, "r") as f:
                return yaml.safe_load(f)

        rows = await asyncio.to_thread(_read_yaml)
        assert any(r.get("description") == "d" for r in rows)


# ---------------------------------------------------------------------------
# Legacy load — v2 row without ``description`` defaults to None
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_legacy_v2_row_without_description_loads_with_none() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await _seed_orders(storage)
        # Write a legacy-shaped row directly (no ``description`` key).
        legacy = [{
            "version": 2,
            "id": "1",
            "learning": "legacy",
            "entities": ["mydb.orders.amount"],
            "query": None,
            "created_at": "2026-01-01T00:00:00+00:00",
        }]
        memories_path = os.path.join(tmpdir, "memories.yaml")

        def _write_yaml() -> None:
            with open(memories_path, "w") as f:
                yaml.safe_dump(legacy, f)

        await asyncio.to_thread(_write_yaml)
        reloaded = await storage.get_memory("1")
        assert reloaded.description is None
        assert reloaded.learning == "legacy"


# ---------------------------------------------------------------------------
# MemoryService propagation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_service_save_memory_threads_description() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await _seed_orders(storage)
        service = MemoryService(storage=storage)
        resp = await service.save_memory(
            learning="x",
            linked_entities=["mydb.orders.amount"],
            description="summary",
        )
        reloaded = await storage.get_memory(resp.memory_id)
        assert reloaded.description == "summary"


@pytest.mark.asyncio
async def test_service_save_memory_rejects_over_500_chars() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await _seed_orders(storage)
        service = MemoryService(storage=storage)
        with pytest.raises((ValidationError, ValueError)):
            await service.save_memory(
                learning="x",
                linked_entities=["mydb.orders.amount"],
                description="a" * 501,
            )


@pytest.mark.asyncio
async def test_service_save_memory_normalises_empty_description() -> None:
    """Empty-string description from REST/MCP body normalises to None."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = YAMLStorage(base_dir=tmpdir)
        await _seed_orders(storage)
        service = MemoryService(storage=storage)
        resp = await service.save_memory(
            learning="x",
            linked_entities=["mydb.orders.amount"],
            description="",
        )
        reloaded = await storage.get_memory(resp.memory_id)
        assert reloaded.description is None
