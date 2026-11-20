"""A store seeded before the rename loses ``help.models`` and ``help.joins`` and their embeddings on reseed, and
search and inspect serve ``help.datasets`` and ``help.relationships`` in their place, across a restart.

Spec: compatibility/ossie-vocabulary › Surfaces follow the vocabulary (Retired help topics leave a warm store).

The pre-2.0 spellings below are deliberate: keep this file out of the vocabulary respelling pass.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from slayer.embeddings.models import Embedding
from slayer.memories.help_seed import RETIRED_HELP_IDS, seed_help_memories
from slayer.search.service import SearchService
from slayer.storage.base import StorageBackend
from slayer.storage.sqlite_storage import SQLiteStorage
from slayer.storage.yaml_storage import YAMLStorage
from tests._deprecated_input_helpers import recorded
from tests._dataset_access_fixtures import mcp_text

OLD_TOPICS = ("help.models", "help.joins")
NEW_TOPICS = ("help.datasets", "help.relationships")
EMBEDDING_MODEL = "test-embedding-model"
QUESTION = "datasets fields metrics relationships dotted paths joined aggregates"


def open_store(*, backend: str, base: Path) -> StorageBackend:
    if backend == "yaml":
        return YAMLStorage(base_dir=str(base))
    return SQLiteStorage(db_path=os.path.join(base, "store.db"))


async def pre_rename_store(*, backend: str, base: Path) -> StorageBackend:
    """A warm store holding the pre-rename help topics, each with an embedding row."""
    storage = open_store(backend=backend, base=base)
    for topic_id in OLD_TOPICS:
        await storage.save_memory(id=topic_id, learning=f"Pre-rename body of {topic_id}.", entities=[])
        await storage.save_embedding(Embedding(
            canonical_id=f"memory:{topic_id}", embedding_model_name=EMBEDDING_MODEL, entity_kind="memory",
            content_hash=f"hash-{topic_id}", embedding=[0.1, 0.2, 0.3],
        ))
    return storage


async def assert_renamed(storage: StorageBackend) -> None:
    for topic_id in OLD_TOPICS:
        assert await storage.get_memory_row(topic_id) is None, topic_id
        assert await storage.get_embedding(
            canonical_id=f"memory:{topic_id}", embedding_model_name=EMBEDDING_MODEL,
        ) is None, topic_id
    hits = [h.id for h in (await SearchService(storage=storage).search(question=QUESTION, max_results=50)).results]
    for topic_id in NEW_TOPICS:
        assert any(h.endswith(topic_id) for h in hits), (topic_id, hits)
    for topic_id in OLD_TOPICS:
        assert not any(h.endswith(topic_id) for h in hits), (topic_id, hits)
    for old, new in zip(OLD_TOPICS, NEW_TOPICS, strict=True):
        expected = await mcp_text(storage, "inspect", entity_type="memory", reference=f"memory:{new}", compact=False)
        with recorded():
            converted = await mcp_text(storage, "inspect", entity_type="memory", reference=f"memory:{old}",
                                       compact=False)
        assert expected in converted
        assert "Pre-rename body" not in converted
        assert any(old in line and new in line and "3.0" in line for line in converted.splitlines()), converted


def test_the_old_topics_are_retired() -> None:
    assert set(OLD_TOPICS) <= set(RETIRED_HELP_IDS)
    assert not set(NEW_TOPICS) & set(RETIRED_HELP_IDS)


@pytest.mark.parametrize("backend", ["yaml", "sqlite"])
async def test_reseeding_a_warm_store_replaces_the_old_topics(tmp_path: Path, backend: str) -> None:
    storage = await pre_rename_store(backend=backend, base=tmp_path)
    await seed_help_memories(storage)
    await assert_renamed(storage)


@pytest.mark.parametrize("backend", ["yaml", "sqlite"])
async def test_the_old_topics_stay_gone_across_a_restart(tmp_path: Path, backend: str) -> None:
    await seed_help_memories(await pre_rename_store(backend=backend, base=tmp_path))
    reopened = open_store(backend=backend, base=tmp_path)
    assert await seed_help_memories(reopened) == 0
    await assert_renamed(reopened)
