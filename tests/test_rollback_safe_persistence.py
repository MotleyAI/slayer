"""A store written by this release omits unset fields (spec: models/stored-upgrade)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest
import yaml

from slayer.core.enums import DataType
from slayer.core.models import DatasetField, DatasourceConfig, DatasetMetric, SlayerDataset
from slayer.core.query import OrderItem, SlayerQuery, TimeDimension
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage

DS = "shop"
BACKENDS = ["yaml", "sqlite"]


def _storage(*, backend: str, base: Path) -> StorageBackend:
    if backend == "yaml":
        return YAMLStorage(base_dir=str(base))
    return SQLiteStorage(db_path=str(base / "store.db"))


def _ordered_query() -> SlayerQuery:
    """Leaves every field added by this release unset: ``OrderItem.nulls``, ``TimeDimension.name``."""
    return SlayerQuery.model_validate({
        "population": "orders",
        "metrics": [{"metric": "sum(amount)", "alias": "rev"}],
        "dimensions": ["status"],
        "time_dimensions": [TimeDimension(field="ordered_at", granularity="month")],
        "order": [OrderItem.model_validate({"field": "status", "direction": "desc"})],
    })


def _orders() -> SlayerDataset:
    return SlayerDataset(
        name="orders", sql_table="orders", data_source=DS,
        fields=[
            DatasetField(name="order_id", datatype=DataType.INTEGER, primary_key=True),
            DatasetField(name="status", datatype=DataType.STRING),
            DatasetField(name="ordered_at", datatype=DataType.DATETIME),
            DatasetField(name="amount", datatype=DataType.FLOAT),
        ],
        metrics=[DatasetMetric(name="revenue", expression="sum(amount)")],
    )


def _saved_query() -> SlayerDataset:
    return SlayerDataset(name="rev_by_status", data_source=DS, source_queries=[_ordered_query()])


async def _populate(storage: StorageBackend) -> None:
    await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite", database="/nonexistent/shop.db"))
    await storage.save_dataset(_orders())
    await storage.save_dataset(_saved_query())
    await storage.save_memory(id="m_ordered", learning="Revenue by status.", entities=["orders"],
                              query=_ordered_query())


def _yaml_text(path: str) -> str:
    with open(path, encoding="utf-8") as f:  # NOSONAR(S7493) — test read
        return f.read()


def _yaml_memory(base: Path, memory_id: str) -> dict[str, Any]:
    text = _yaml_text(str(base / "memories" / f"{memory_id}.md"))
    head, _, _ = text.removeprefix("---\n").partition("\n---\n")
    return yaml.safe_load(head)


def _stored(*, backend: str, base: Path) -> dict[str, dict[str, Any]]:
    """The raw stored document of each kind, keyed by kind; a YAML memory is its frontmatter."""
    if backend == "yaml":
        return {
            "SlayerModel": yaml.safe_load(_yaml_text(str(base / "models" / DS / "orders.yaml"))),
            "saved query": yaml.safe_load(_yaml_text(str(base / "models" / DS / "rev_by_status.yaml"))),
            "DatasourceConfig": yaml.safe_load(_yaml_text(str(base / "datasources" / f"{DS}.yaml"))),
            "Memory": _yaml_memory(base, "m_ordered"),
        }
    with transaction(str(base / "store.db")) as conn:
        def row(sql: str, *args: str) -> dict[str, Any]:
            return json.loads(conn.execute(sql, args).fetchone()[0])

        return {
            "SlayerModel": row("SELECT data FROM models WHERE name = ?", "orders"),
            "saved query": row("SELECT data FROM models WHERE name = ?", "rev_by_status"),
            "DatasourceConfig": row("SELECT data FROM datasources WHERE name = ?", DS),
            "Memory": row("SELECT data FROM memories WHERE id = ?", "m_ordered"),
        }


_ROOT = {"SlayerModel": SlayerDataset, "saved query": SlayerDataset, "DatasourceConfig": DatasourceConfig,
         "Memory": Memory}


def _nulls(value: Any, *, path: str) -> list[str]:
    if value is None:
        return [path]
    if isinstance(value, dict):
        return [p for k, v in value.items() for p in _nulls(v, path=f"{path}.{k}")]
    if isinstance(value, list):
        return [p for i, v in enumerate(value) for p in _nulls(v, path=f"{path}[{i}]")]
    return []


@pytest.fixture(params=BACKENDS)
async def stored(request: pytest.FixtureRequest, tmp_path: Path) -> dict[str, dict[str, Any]]:
    storage = _storage(backend=request.param, base=tmp_path)
    await _populate(storage)
    return _stored(backend=request.param, base=tmp_path)


class TestNoUnsetField:
    @pytest.mark.parametrize("kind", list(_ROOT))
    async def test_no_stored_value_is_null(self, stored: dict[str, dict[str, Any]], kind: str) -> None:
        assert _nulls(stored[kind], path=kind) == []

    async def test_memory_order_carries_no_nulls_key(self, stored: dict[str, dict[str, Any]]) -> None:
        order = stored["Memory"]["query"]["order"]
        assert all("nulls" not in item for item in order), order


class TestYamlMemoryLayout:
    async def test_id_and_learning_stay_out_of_the_frontmatter(self, tmp_path: Path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_memory(id="m_plain", learning="Plain body.\n", entities=["orders"])
        path = tmp_path / "memories" / "m_plain.md"
        assert os.path.exists(path)
        text = _yaml_text(str(path))
        front = _yaml_memory(tmp_path, "m_plain")
        assert "id" not in front
        assert "learning" not in front
        assert text.endswith("\n---\nPlain body.\n"), text

    async def test_a_memory_without_unset_fields_is_byte_stable(self, tmp_path: Path) -> None:
        storage = YAMLStorage(base_dir=str(tmp_path))
        await storage.save_memory(id="m_plain", learning="Plain body.", entities=["orders"],
                                  description="preview")
        path = str(tmp_path / "memories" / "m_plain.md")
        before = _yaml_text(path)
        memory = await storage.get_memory("m_plain")
        await storage.save_memory(id=memory.id, learning=memory.learning, entities=memory.entities,
                                  description=memory.description)
        assert _yaml_text(path) == before
        front = {k: v for k, v in memory.model_dump(mode="json").items()
                 if k not in ("id", "learning") and v is not None}
        dumped = yaml.safe_dump(front, sort_keys=True, default_flow_style=False, allow_unicode=True)
        assert before == f"---\n{dumped}---\nPlain body.", before
