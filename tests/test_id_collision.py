"""Case-collision rejection for datasource / model / memory ids.

Ids are filenames in the YAML backend, so saving an id that differs only
by case from an existing one raises IdCollisionError there (exact-id
re-saves remain upserts). SQLite keys are case-sensitive and stores
case-variant ids distinctly.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterator

import pytest
import yaml

from slayer.core.errors import IdCollisionError
from slayer.core.models import DatasourceConfig, SlayerModel
from slayer.storage.base import (
    _find_case_colliding_id,
    _fs_equivalence_key,
    resolve_storage,
)
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def storage() -> Iterator[YAMLStorage]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=tmpdir)


@pytest.fixture
def sqlite_storage() -> Iterator[SQLiteStorage]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield SQLiteStorage(db_path=os.path.join(tmpdir, "test.db"))


def _ds(name: str, host: str = "h") -> DatasourceConfig:
    return DatasourceConfig(name=name, type="postgres", host=host)


def _model(name: str, data_source: str = "db", table: str = "t") -> SlayerModel:
    return SlayerModel(name=name, data_source=data_source, sql_table=table)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestHelpers:
    def test_key_casefolds(self) -> None:
        assert _fs_equivalence_key("Orders") == _fs_equivalence_key("orders")
        assert _fs_equivalence_key("a") != _fs_equivalence_key("b")

    def test_exact_match_is_not_a_collision(self) -> None:
        assert _find_case_colliding_id("orders", ["orders"]) is None

    def test_case_variant_found(self) -> None:
        assert _find_case_colliding_id("Orders", ["orders"]) == "orders"

    def test_variant_reported_even_with_exact_match(self) -> None:
        assert _find_case_colliding_id("orders", ["orders", "Orders"]) == "Orders"

    def test_unrelated_ids_do_not_collide(self) -> None:
        assert _find_case_colliding_id("orders", ["customers"]) is None


# ---------------------------------------------------------------------------
# YAML: datasources
# ---------------------------------------------------------------------------


class TestDatasourceCollision:
    async def test_case_variant_rejected(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(_ds("db", host="first"))
        with pytest.raises(IdCollisionError) as exc_info:
            await storage.save_datasource(_ds("DB", host="second"))
        assert exc_info.value.kind == "datasource"
        assert exc_info.value.new_id == "DB"
        assert exc_info.value.existing_id == "db"
        loaded = await storage.get_datasource("db")
        assert loaded.host == "first"

    async def test_exact_resave_upserts(self, storage: YAMLStorage) -> None:
        await storage.save_datasource(_ds("db", host="first"))
        await storage.save_datasource(_ds("db", host="second"))
        loaded = await storage.get_datasource("db")
        assert loaded.host == "second"

    async def test_collides_with_model_data_source(
        self, storage: YAMLStorage,
    ) -> None:
        # Models under an orphan datasource reserve its name.
        await storage.save_model(_model("orders", data_source="db"))
        with pytest.raises(IdCollisionError):
            await storage.save_datasource(_ds("DB"))
        await storage.save_datasource(_ds("db"))


# ---------------------------------------------------------------------------
# YAML: models
# ---------------------------------------------------------------------------


class TestModelCollision:
    async def test_case_variant_name_rejected(
        self, storage: YAMLStorage,
    ) -> None:
        await storage.save_model(_model("orders", table="first"))
        with pytest.raises(IdCollisionError) as exc_info:
            await storage.save_model(_model("Orders", table="second"))
        assert exc_info.value.kind == "model"
        assert exc_info.value.new_id == "Orders"
        assert exc_info.value.existing_id == "orders"
        assert exc_info.value.data_source == "db"
        loaded = await storage.get_model("orders", data_source="db")
        assert loaded.sql_table == "first"

    async def test_exact_resave_upserts(self, storage: YAMLStorage) -> None:
        await storage.save_model(_model("orders", table="first"))
        await storage.save_model(_model("orders", table="second"))
        loaded = await storage.get_model("orders", data_source="db")
        assert loaded.sql_table == "second"

    async def test_same_name_in_other_datasource_ok(
        self, storage: YAMLStorage,
    ) -> None:
        await storage.save_model(_model("orders", data_source="db_a"))
        await storage.save_model(_model("Orders", data_source="db_b"))
        assert await storage.get_model("orders", data_source="db_a") is not None
        assert await storage.get_model("Orders", data_source="db_b") is not None

    async def test_case_variant_data_source_rejected(
        self, storage: YAMLStorage,
    ) -> None:
        await storage.save_datasource(_ds("db"))
        with pytest.raises(IdCollisionError) as exc_info:
            await storage.save_model(_model("orders", data_source="DB"))
        assert exc_info.value.kind == "datasource"

    async def test_data_source_vs_other_models_rejected(
        self, storage: YAMLStorage,
    ) -> None:
        await storage.save_model(_model("orders", data_source="db"))
        with pytest.raises(IdCollisionError):
            await storage.save_model(_model("customers", data_source="DB"))

    async def test_validate_false_bypasses(self, storage: YAMLStorage) -> None:
        # The migration write-back path must stay able to persist legacy data.
        await storage.save_model(_model("orders"))
        await storage.save_model(_model("Orders"), _validate=False)


# ---------------------------------------------------------------------------
# YAML: memories (rejection also covered in test_memory_string_ids.py)
# ---------------------------------------------------------------------------


class TestMemoryCollision:
    async def test_exact_upsert_allowed(self, storage: YAMLStorage) -> None:
        await storage.save_memory(
            id="kb.x", learning="one", entities=["mydb.orders"],
        )
        await storage.save_memory(
            id="kb.x", learning="two", entities=["mydb.orders"],
        )
        assert (await storage.get_memory("kb.x")).learning == "two"

    async def test_error_attrs(self, storage: YAMLStorage) -> None:
        await storage.save_memory(
            id="Kb.X", learning="one", entities=["mydb.orders"],
        )
        with pytest.raises(IdCollisionError) as exc_info:
            await storage.save_memory(
                id="kb.x", learning="two", entities=["mydb.orders"],
            )
        assert exc_info.value.kind == "memory"
        assert exc_info.value.new_id == "kb.x"
        assert exc_info.value.existing_id == "Kb.X"


# ---------------------------------------------------------------------------
# SQLite: case-variant ids are distinct identities, no rejection
# ---------------------------------------------------------------------------


class TestSqliteAllowsCaseVariants:
    async def test_datasources(self, sqlite_storage: SQLiteStorage) -> None:
        await sqlite_storage.save_datasource(_ds("db", host="lower"))
        await sqlite_storage.save_datasource(_ds("DB", host="upper"))
        assert (await sqlite_storage.get_datasource("db")).host == "lower"
        assert (await sqlite_storage.get_datasource("DB")).host == "upper"

    async def test_models(self, sqlite_storage: SQLiteStorage) -> None:
        await sqlite_storage.save_model(_model("orders", table="lower"))
        await sqlite_storage.save_model(_model("Orders", table="upper"))
        low = await sqlite_storage.get_model("orders", data_source="db")
        up = await sqlite_storage.get_model("Orders", data_source="db")
        assert low.sql_table == "lower"
        assert up.sql_table == "upper"

    async def test_memories(self, sqlite_storage: SQLiteStorage) -> None:
        await sqlite_storage.save_memory(
            id="X", learning="upper", entities=["mydb.orders"],
        )
        await sqlite_storage.save_memory(
            id="x", learning="lower", entities=["mydb.orders"],
        )
        assert (await sqlite_storage.get_memory("X")).learning == "upper"
        assert (await sqlite_storage.get_memory("x")).learning == "lower"

    async def test_wrapped_sqlite_allows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = resolve_storage(os.path.join(tmpdir, "test.db"))
            await storage.save_model(_model("orders"))
            await storage.save_model(_model("Orders"))


# ---------------------------------------------------------------------------
# The production wrapper (resolve_storage → JoinSyncStorage) over YAML
# ---------------------------------------------------------------------------


class TestJoinSyncWrapped:
    async def test_collisions_raise_through_wrapper(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            storage = resolve_storage(tmpdir)
            await storage.save_datasource(_ds("db"))
            with pytest.raises(IdCollisionError):
                await storage.save_datasource(_ds("DB"))
            await storage.save_model(_model("orders"))
            with pytest.raises(IdCollisionError):
                await storage.save_model(_model("Orders"))
            await storage.save_memory(
                id="X", learning="a", entities=["mydb.orders"],
            )
            with pytest.raises(IdCollisionError):
                await storage.save_memory(
                    id="x", learning="b", entities=["mydb.orders"],
                )


# ---------------------------------------------------------------------------
# Layout migrations refuse case-colliding targets before writing anything
# ---------------------------------------------------------------------------


class TestMigrationPreChecks:
    def test_legacy_memories_yaml_case_pair_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            legacy = os.path.join(tmpdir, "memories.yaml")
            with open(legacy, "w") as f:
                yaml.safe_dump(
                    [
                        {"id": "X", "learning": "upper", "entities": []},
                        {"id": "x", "learning": "lower", "entities": []},
                    ],
                    f,
                )
            with pytest.raises(ValueError, match="differ only by"):
                YAMLStorage(base_dir=tmpdir)
            assert os.path.exists(legacy)
            mem_dir = os.path.join(tmpdir, "memories")
            md_files = (
                [f for f in os.listdir(mem_dir) if f.endswith(".md")]
                if os.path.isdir(mem_dir)
                else []
            )
            assert md_files == []

    def test_flat_models_case_variant_datasources_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            models_dir = os.path.join(tmpdir, "models")
            os.makedirs(models_dir)
            for fname, ds in (("a.yaml", "DB"), ("b.yaml", "db")):
                with open(os.path.join(models_dir, fname), "w") as f:
                    yaml.safe_dump(
                        {"name": fname[:-5], "data_source": ds, "sql_table": "t"},
                        f,
                    )
            with pytest.raises(ValueError, match="differs only by case"):
                YAMLStorage(base_dir=tmpdir)
            assert sorted(os.listdir(models_dir)) == ["a.yaml", "b.yaml"]

    def test_flat_model_vs_existing_v4_case_variant_refused(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            models_dir = os.path.join(tmpdir, "models")
            os.makedirs(os.path.join(models_dir, "db"))
            with open(os.path.join(models_dir, "db", "Orders.yaml"), "w") as f:
                yaml.safe_dump(
                    {"name": "Orders", "data_source": "db", "sql_table": "t"}, f,
                )
            with open(os.path.join(models_dir, "orders.yaml"), "w") as f:
                yaml.safe_dump(
                    {"name": "orders", "data_source": "db", "sql_table": "t"}, f,
                )
            with pytest.raises(ValueError, match="differs only by case"):
                YAMLStorage(base_dir=tmpdir)
            assert os.path.exists(os.path.join(models_dir, "orders.yaml"))
            assert os.path.exists(os.path.join(models_dir, "db", "Orders.yaml"))
