"""Semantic-model hooks default to unsupported; a backend implementing them gets the document path.

Spec: storage/custom-backends › Semantic-model hooks default to unsupported.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
from collections.abc import AsyncGenerator, Callable, Generator
from pathlib import Path
from typing import Any

import pytest

from slayer.core.errors import StoredDocumentLoadError
from slayer.core.models import DatasourceConfig, SlayerDataset
from slayer.embeddings.models import Embedding
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.mcp.server import create_mcp_server
from slayer.memories.models import Memory
from slayer.storage.base import _STORAGE_REGISTRY, StorageBackend, register_storage, resolve_storage
from slayer.storage.document_loading import Loaded

from tests._dev1737_fixtures import seed_backend
from tests._dev2243_fixtures import DS, SALES, error
from tests._dev2277_fixtures import DS as SEED_DS
from tests._dev2277_fixtures import SEED_TABLES, by, seed_datasets
from tests._engine_helpers import owned_query_engine
from tests._mcp_results import text_blocks

UNSUPPORTED = "does not support semantic-model documents"


class DatasetsOnlyBackend(StorageBackend):
    """Dataset, datasource and memory hooks only, all in memory."""

    def __init__(self) -> None:
        self.datasets: dict[tuple[str, str], SlayerDataset] = {}
        self.datasources: dict[str, DatasourceConfig] = {}
        self.memories: dict[str, Memory] = {}
        self.embeddings: dict[tuple[str, str], Embedding] = {}
        self.priority: list[str] = []

    async def _save_dataset_impl(self, dataset: SlayerDataset) -> None:
        self.datasets[(dataset.data_source, dataset.name)] = dataset

    async def _list_all_dataset_identities(self) -> list[tuple[str, str]]:
        return list(self.datasets)

    async def get_dataset(self, name: str, data_source: str | None = None) -> SlayerDataset | None:
        return next((m for (ds, n), m in self.datasets.items() if n == name and data_source in (None, ds)), None)

    async def _delete_dataset_row(self, *, data_source: str, name: str) -> bool:
        return self.datasets.pop((data_source, name), None) is not None

    async def update_field_sampled(
        self, *, data_source: str, dataset_name: str, field_name: str,
        sampled: str | None, sampled_values: list[str] | None, distinct_count: int | None,
    ) -> None:
        return None

    async def _save_datasource_impl(self, datasource: DatasourceConfig) -> None:
        self.datasources[datasource.name] = datasource

    async def get_datasource(self, name: str) -> DatasourceConfig | None:
        return self.datasources.get(name)

    async def list_datasources(self) -> list[str]:
        return sorted(self.datasources)

    async def _delete_datasource_row(self, name: str) -> bool:
        return self.datasources.pop(name, None) is not None

    async def get_datasource_priority(self) -> list[str]:
        return list(self.priority)

    async def _set_datasource_priority_raw(self, priority: list[str]) -> None:
        self.priority = list(priority)

    async def _save_memory_row(self, memory: Memory) -> None:
        self.memories[memory.id] = memory

    async def _get_memory_row(self, memory_id: str) -> Memory | None:
        return self.memories.get(memory_id)

    async def _list_memories_rows(self, *, entities: list[str] | None) -> Loaded[Memory]:
        failed: list[StoredDocumentLoadError] = []
        return list(self.memories.values()), failed

    async def _delete_memory_row(self, memory_id: str) -> bool:
        return self.memories.pop(memory_id, None) is not None

    async def _next_memory_seq(self) -> str:
        return str(len(self.memories) + 1)

    async def save_embedding(self, row: Embedding) -> None:
        self.embeddings[(row.canonical_id, row.embedding_model_name)] = row

    async def get_embedding(self, *, canonical_id: str, embedding_model_name: str) -> Embedding | None:
        return self.embeddings.get((canonical_id, embedding_model_name))

    async def list_embeddings(self, *, embedding_model_name: str) -> list[Embedding]:
        return [e for (_, m), e in self.embeddings.items() if m == embedding_model_name]

    async def delete_embeddings_for_canonical(self, *, canonical_id_prefix: str) -> int:
        doomed = [k for k in self.embeddings if k[0] == canonical_id_prefix or k[0].startswith(f"{canonical_id_prefix}.")]
        for key in doomed:
            del self.embeddings[key]
        return len(doomed)


class DocumentBackend(DatasetsOnlyBackend):
    """Also implements the semantic-model hooks over a dict, and fingerprints all its content."""

    def __init__(self) -> None:
        super().__init__()
        self.documents: dict[tuple[str, str], str] = {}
        self.log: list[str] = []

    async def _save_semantic_model_text_impl(self, *, data_source: str, name: str, text: str) -> None:
        self.log.append("save")
        self.documents[(data_source, name)] = text

    async def _get_semantic_model_text_impl(self, *, data_source: str, name: str) -> str | None:
        return self.documents.get((data_source, name))

    async def _list_semantic_model_identities(self) -> list[tuple[str, str]]:
        return sorted(self.documents)

    async def _delete_semantic_model_row(self, *, data_source: str, name: str) -> bool:
        self.log.append("delete")
        return self.documents.pop((data_source, name), None) is not None

    async def _patch_semantic_model_text(self, *, data_source: str, name: str, patch: Callable[[str], str]) -> str:
        self.log.append("patch")
        text = patch(self.documents[(data_source, name)])
        self.documents[(data_source, name)] = text
        return text

    async def graph_fingerprint(self) -> str | None:
        payload = {
            "datasets": sorted(f"{ds}/{n}:{m.model_dump_json()}" for (ds, n), m in self.datasets.items()),
            "documents": sorted(f"{ds}/{n}:{t}" for (ds, n), t in self.documents.items()),
        }
        return hashlib.sha256(json.dumps(payload).encode()).hexdigest()


@pytest.fixture
def registered() -> Generator[str]:
    """``memdict://`` resolves to a fresh ``DatasetsOnlyBackend``."""
    register_storage("memdict", lambda _rest: DatasetsOnlyBackend())
    try:
        yield "memdict://store"
    finally:
        _STORAGE_REGISTRY.pop("memdict", None)


@contextlib.asynccontextmanager
async def _engine(storage: StorageBackend, *, tmp_path: Path, data_source: str,
                  datasets: tuple[SlayerDataset, ...] = ()) -> AsyncGenerator[SlayerQueryEngine]:
    db_path = str(tmp_path / "seed.db")
    seed_backend("sqlite", db_path, SEED_TABLES)
    ds = DatasourceConfig(name=data_source, type="sqlite", database=db_path)
    async with owned_query_engine(storage=storage, datasources=[ds]) as engine:
        for dataset in datasets:
            await storage.save_dataset(dataset)
        yield engine


async def _mcp(storage: StorageBackend, tool: str, **arguments: Any) -> str:
    """One MCP ``tool`` call on a fresh server: its text, or the raised error's text."""
    server = create_mcp_server(storage=storage, _seed_help=False)
    try:
        blocks = text_blocks(await server.call_tool(name=tool, arguments=arguments))
        return "\n".join(getattr(b, "text", "") for b in blocks)
    except Exception as exc:  # noqa: BLE001 — the refusal text is the assertion subject
        return f"{type(exc).__name__}: {exc}"
    finally:
        await getattr(server, "_slayer_engine").aclose()


class TestABackendWithoutTheHooks:
    async def test_its_datasets_still_load_and_answer_queries(self, registered: str, tmp_path: Path) -> None:
        """Spec: storage/custom-backends › A backend without the hooks (datasets keep working)."""
        storage = resolve_storage(registered)
        assert isinstance(storage, DatasetsOnlyBackend)
        async with _engine(storage, tmp_path=tmp_path, data_source=SEED_DS, datasets=tuple(seed_datasets())) as engine:
            resp = await engine.execute({"population": "orders", "dimensions": ["customers.region"],
                                         "metrics": ["orders.total_revenue"]})
        assert by(resp, "customers.region", "orders.total_revenue") == {"EAST": 350.0, "WEST": 300.0}

    async def test_listing_documents_returns_none(self, registered: str) -> None:
        """Spec: storage/custom-backends › A backend without the hooks (listing returns none)."""
        storage = resolve_storage(registered)
        assert await storage.list_semantic_models() == []
        assert await storage.list_semantic_models(data_source=DS) == []

    async def test_create_semantic_model_fails_with_the_unsupported_error(self, registered: str, tmp_path: Path) -> None:
        """Spec: storage/custom-backends › A backend without the hooks (create_semantic_model refused)."""
        storage = resolve_storage(registered)
        async with _engine(storage, tmp_path=tmp_path, data_source=DS):
            reply = await _mcp(storage, "create_semantic_model", name="sales", data_source=DS, content=SALES)
        assert "SemanticModelsUnsupportedError" in reply, reply
        assert "DatasetsOnlyBackend" in reply, reply
        assert UNSUPPORTED in reply, reply
        assert await storage.list_datasets(data_source=DS) == []

    async def test_save_semantic_model_text_fails_naming_the_backend(self, registered: str) -> None:
        """Spec: storage/custom-backends › A backend without the hooks (storage write refused)."""
        storage = resolve_storage(registered)
        with pytest.raises(error("SemanticModelsUnsupportedError")) as info:
            await storage.save_semantic_model_text(data_source=DS, name="sales", text=SALES)
        assert "DatasetsOnlyBackend" in str(info.value)
        assert UNSUPPORTED in str(info.value)

    @pytest.mark.parametrize("operation", ["get", "delete"])
    async def test_reading_or_deleting_a_document_fails_naming_the_backend(self, registered: str, operation: str) -> None:
        """Spec: storage/custom-backends › Semantic-model hooks default to unsupported (reads refused)."""
        storage = resolve_storage(registered)
        call = (storage.get_semantic_model_text("sales", data_source=DS) if operation == "get"
                else storage.delete_semantic_model("sales", data_source=DS))
        with pytest.raises(error("SemanticModelsUnsupportedError")) as info:
            await call
        assert "DatasetsOnlyBackend" in str(info.value)


class TestABackendImplementingTheHooks:
    async def test_the_document_path_works(self, tmp_path: Path) -> None:
        """Spec: storage/custom-backends › Semantic-model hooks default to unsupported (implemented hooks)."""
        storage = DocumentBackend()
        async with _engine(storage, tmp_path=tmp_path, data_source=DS) as engine:
            await storage.save_semantic_model_text(data_source=DS, name="sales", text=SALES)
            listed = await storage.list_semantic_models()
            text = await storage.get_semantic_model_text("sales", data_source=DS)
            names = sorted(await storage.list_datasets(data_source=DS))
            resp = await engine.execute({"metrics": ["revenue_per_customer"]})
        assert listed == [(DS, "sales")]
        assert text == SALES
        assert names == ["customers", "orders"]
        (row,) = resp.data
        assert float(row["revenue_per_customer"]) == pytest.approx(162.5)

    async def test_the_fingerprint_changes_with_document_content_and_membership(self, tmp_path: Path) -> None:
        """Spec: storage/custom-backends › Semantic-model hooks default to unsupported (fingerprint contract)."""
        storage = DocumentBackend()
        async with _engine(storage, tmp_path=tmp_path, data_source=DS) as engine:
            empty = await storage.graph_fingerprint()
            await storage.save_semantic_model_text(data_source=DS, name="sales", text=SALES)
            stored = await storage.graph_fingerprint()
            customers = await storage.get_dataset("customers", data_source=DS)
            assert customers is not None
            await engine.save_dataset(customers.model_copy(update={"description": "Customer master."}))
            edited = await storage.graph_fingerprint()
            edited_text = await storage.get_semantic_model_text("sales", data_source=DS)
            assert await storage.delete_semantic_model("sales", data_source=DS)
            deleted = await storage.graph_fingerprint()
            left = await storage.list_datasets(data_source=DS)
        assert len({empty, stored, edited}) == 3
        assert deleted != edited
        assert "patch" in storage.log
        assert edited_text is not None
        assert "Customer master." in edited_text
        assert storage.documents == {}
        assert left == []
