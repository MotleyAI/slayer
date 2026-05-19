"""``memory:<id>`` branch in ``resolve_entity`` (DEV-1428).

Critical invariant: the ``memory:`` branch runs at the TOP of
``resolve_entity``, before ``_strip_agg_suffix``, so ``memory:abc`` parses
as memory branch (not as prefix ``memory`` + agg ``abc``).
"""

from __future__ import annotations

import os
import tempfile
from typing import Iterator

import pytest

from slayer.core.errors import EntityResolutionError
from slayer.memories.resolver import canonical_id_rooted_at, resolve_entity
from slayer.storage.base import StorageBackend
from slayer.storage.yaml_storage import YAMLStorage


@pytest.fixture
def storage() -> Iterator[StorageBackend]:
    with tempfile.TemporaryDirectory() as tmpdir:
        yield YAMLStorage(base_dir=os.path.join(tmpdir, "store"))


class TestMemoryBranchResolve:
    async def test_resolve_memory_int_id(self, storage: StorageBackend) -> None:
        await storage.save_memory(
            learning="x", entities=["mydb.orders"],
        )
        result = await resolve_entity("memory:1", storage=storage)
        assert result.canonical_forms == ["memory:1"]

    async def test_resolve_memory_string_id(
        self, storage: StorageBackend,
    ) -> None:
        await storage.save_memory(
            id="kb.policy.42",
            learning="x",
            entities=["mydb.orders"],
        )
        result = await resolve_entity(
            "memory:kb.policy.42", storage=storage,
        )
        assert result.canonical_forms == ["memory:kb.policy.42"]

    async def test_resolve_memory_id_with_letters(
        self, storage: StorageBackend,
    ) -> None:
        # ``memory:abc`` must be parsed as memory branch (NOT as prefix
        # "memory" + agg "abc"). This pins resolver ordering.
        await storage.save_memory(
            id="abc", learning="x", entities=["mydb.orders"],
        )
        result = await resolve_entity("memory:abc", storage=storage)
        assert result.canonical_forms == ["memory:abc"]

    async def test_resolve_memory_absent_raises(
        self, storage: StorageBackend,
    ) -> None:
        with pytest.raises(EntityResolutionError):
            await resolve_entity("memory:nonexistent", storage=storage)

    async def test_resolve_memory_empty_id_raises(
        self, storage: StorageBackend,
    ) -> None:
        with pytest.raises(EntityResolutionError):
            await resolve_entity("memory:", storage=storage)

    async def test_resolve_memory_charset_violation_raises(
        self, storage: StorageBackend,
    ) -> None:
        # ``memory:abc:def`` is a charset violation in the id portion
        # ('` :`' forbidden).
        with pytest.raises(EntityResolutionError):
            await resolve_entity("memory:abc:def", storage=storage)

    async def test_bare_name_does_not_resolve_to_memory(
        self, storage: StorageBackend,
    ) -> None:
        # A bare name ``42`` should NOT resolve to a memory; only the
        # ``memory:`` prefix triggers the memory branch.
        await storage.save_memory(
            id="42", learning="x", entities=["mydb.orders"],
        )
        # No datasource named "42" exists; the resolver should raise.
        with pytest.raises(EntityResolutionError):
            await resolve_entity("42", storage=storage)


class TestCanonicalIdRootedAt:
    def test_memory_canonical_never_matches_datasource(self) -> None:
        # ``memory:<id>`` canonical ids are datasource-agnostic.
        assert not canonical_id_rooted_at(
            canonical_id="memory:42", datasource="memory",
        )
        assert not canonical_id_rooted_at(
            canonical_id="memory:kb.policy.42", datasource="mydb",
        )
        assert not canonical_id_rooted_at(
            canonical_id="memory:abc", datasource="abc",
        )
