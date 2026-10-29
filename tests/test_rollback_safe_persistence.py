"""A store written by this release reads on the previous release (spec: models/stored-upgrade)."""

from __future__ import annotations

import json
import os
import typing
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import BaseModel

from slayer.core.enums import DataType
from slayer.core.models import Column, DatasourceConfig, ModelMeasure, SlayerModel
from slayer.core.query import OrderItem, SlayerQuery, TimeDimension
from slayer.memories.models import Memory
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_conn import transaction
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._v1_4_1_field_sets import V1_4_1_FIELD_SETS

DS = "shop"
BACKENDS = ["yaml", "sqlite"]


def _storage(*, backend: str, base: Path) -> StorageBackend:
    if backend == "yaml":
        return YAMLStorage(base_dir=str(base))
    return SQLiteStorage(db_path=str(base / "store.db"))


def _ordered_query() -> SlayerQuery:
    """Leaves every field added by this release unset: ``OrderItem.nulls``, ``TimeDimension.name``."""
    return SlayerQuery.model_validate({
        "source_model": "orders",
        "measures": [{"formula": "sum(amount)", "name": "rev"}],
        "dimensions": ["status"],
        "time_dimensions": [TimeDimension(dimension="ordered_at", granularity="month")],
        "order": [OrderItem.model_validate({"column": {"name": "status"}, "direction": "desc"})],
    })


def _orders() -> SlayerModel:
    return SlayerModel(
        name="orders", sql_table="orders", data_source=DS,
        columns=[
            Column(name="order_id", type=DataType.INT, primary_key=True),
            Column(name="status", type=DataType.TEXT),
            Column(name="ordered_at", type=DataType.TIMESTAMP),
            Column(name="amount", type=DataType.DOUBLE),
        ],
        measures=[ModelMeasure(name="revenue", formula="sum(amount)")],
    )


def _saved_query() -> SlayerModel:
    return SlayerModel(name="rev_by_status", data_source=DS, source_queries=[_ordered_query()])


async def _populate(storage: StorageBackend) -> None:
    await storage.save_datasource(DatasourceConfig(name=DS, type="sqlite", database="/nonexistent/shop.db"))
    await storage.save_model(_orders())
    await storage.save_model(_saved_query())
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


_ROOT = {"SlayerModel": SlayerModel, "saved query": SlayerModel, "DatasourceConfig": DatasourceConfig,
         "Memory": Memory}


def _nulls(value: Any, *, path: str) -> list[str]:
    if value is None:
        return [path]
    if isinstance(value, dict):
        return [p for k, v in value.items() for p in _nulls(v, path=f"{path}.{k}")]
    if isinstance(value, list):
        return [p for i, v in enumerate(value) for p in _nulls(v, path=f"{path}[{i}]")]
    return []


def _model_classes(annotation: Any) -> list[type[BaseModel]]:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return [annotation]
    return [c for arg in typing.get_args(annotation) for c in _model_classes(arg)]


def _field_of(cls: type[BaseModel], key: str) -> Any:
    for name, info in cls.model_fields.items():
        if key in (name, info.alias):
            return info.annotation
    return None


def _rejections(value: Any, *, candidates: list[type[BaseModel]], path: str) -> list[str]:
    """Keys a v1.4.1 ``extra="forbid"`` class would reject, at every nested level."""
    if isinstance(value, list):
        return [r for i, v in enumerate(value) for r in _rejections(v, candidates=candidates, path=f"{path}[{i}]")]
    if not isinstance(value, dict) or not candidates:
        return []
    fitting = [c for c in candidates if all(_field_of(c, k) is not None for k in value)]
    cls = fitting[0] if fitting else candidates[0]
    out: list[str] = []
    known, forbid = V1_4_1_FIELD_SETS.get(cls.__name__, (frozenset(), False))
    if forbid and set(value) - known:
        out.append(f"{path}: {cls.__name__} rejects {sorted(set(value) - known)}")
    for key, item in value.items():
        out += _rejections(item, candidates=_model_classes(_field_of(cls, key)), path=f"{path}.{key}")
    return out


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


class TestPreviousReleaseFieldSets:
    @pytest.mark.parametrize("kind", list(_ROOT))
    async def test_every_key_is_known_to_the_previous_release(
        self, stored: dict[str, dict[str, Any]], kind: str,
    ) -> None:
        assert _rejections(stored[kind], candidates=[_ROOT[kind]], path=kind) == []

    def test_the_snapshot_covers_every_forbid_class(self) -> None:
        forbid = {name for name, (_, f) in V1_4_1_FIELD_SETS.items() if f}
        assert forbid == {"SlayerQuery", "OrderItem", "ComputedDimension", "ModelExtension"}


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
