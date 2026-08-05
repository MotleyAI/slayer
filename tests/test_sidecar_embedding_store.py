"""Unit tests for ``SidecarEmbeddingStore`` (DEV-1405).

The helper owns a single SQLite file with one ``embeddings`` table.
Both ``YAMLStorage`` and ``SQLiteStorage`` delegate the embedding CRUD
surface to it; tests here pin the helper's contract directly so the
backend-level tests in ``test_embeddings_storage.py`` only need to
verify wiring.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from collections.abc import Iterator

import pytest

from slayer.embeddings.models import Embedding, EntityKind
from slayer.storage.sidecar_embedding_store import SidecarEmbeddingStore


@pytest.fixture
def db_path() -> Iterator[str]:
    with tempfile.TemporaryDirectory() as tmp:
        yield os.path.join(tmp, "embeddings.db")


@pytest.fixture
def store(db_path: str) -> SidecarEmbeddingStore:
    return SidecarEmbeddingStore(db_path=db_path)


def _embed(
    *,
    canonical_id: str,
    model: str = "openai/test-embedding",
    kind: EntityKind = "memory",
    text_hash: str = "h0",
    vector: list[float] | None = None,
) -> Embedding:
    return Embedding(
        canonical_id=canonical_id,
        embedding_model_name=model,
        entity_kind=kind,
        content_hash=text_hash,
        embedding=vector if vector is not None else [0.1, 0.2, 0.3],
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------


def test_init_creates_table_and_index(db_path: str) -> None:
    SidecarEmbeddingStore(db_path=db_path)
    with sqlite3.connect(db_path) as conn:
        tables = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        indexes = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }
    assert "embeddings" in tables
    assert "idx_embeddings_model" in indexes


def test_init_is_idempotent(db_path: str) -> None:
    SidecarEmbeddingStore(db_path=db_path)
    # Second construction against the same path must not raise.
    SidecarEmbeddingStore(db_path=db_path)


def test_two_stores_at_two_paths_are_independent() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path_a = os.path.join(tmp, "a.db")
        path_b = os.path.join(tmp, "b.db")
        store_a = SidecarEmbeddingStore(db_path=path_a)
        store_b = SidecarEmbeddingStore(db_path=path_b)
        # Files exist and are different.
        assert os.path.exists(path_a)
        assert os.path.exists(path_b)
        assert store_a.db_path != store_b.db_path


# ---------------------------------------------------------------------------
# save / get round trip
# ---------------------------------------------------------------------------


async def test_save_get_round_trip(store: SidecarEmbeddingStore) -> None:
    row = _embed(
        canonical_id="memory:1",
        kind="memory",
        text_hash="h-orig",
        vector=[0.5, -0.25, 1.5],
    )
    await store.save(row)
    fetched = await store.get(
        canonical_id="memory:1",
        embedding_model_name="openai/test-embedding",
    )
    assert fetched is not None
    assert fetched.canonical_id == "memory:1"
    assert fetched.entity_kind == "memory"
    assert fetched.content_hash == "h-orig"
    assert fetched.embedding == [0.5, -0.25, 1.5]
    # created_at must round-trip with UTC tz.
    assert fetched.created_at == datetime(2026, 1, 1, tzinfo=timezone.utc)


async def test_get_unknown_returns_none(store: SidecarEmbeddingStore) -> None:
    assert await store.get(
        canonical_id="memory:42",
        embedding_model_name="openai/test-embedding",
    ) is None


async def test_save_is_upsert(store: SidecarEmbeddingStore) -> None:
    """Same (canonical_id, embedding_model_name) twice replaces in place."""
    first = _embed(canonical_id="x", text_hash="a", vector=[1.0, 1.0, 1.0])
    second = _embed(canonical_id="x", text_hash="b", vector=[2.0, 2.0, 2.0])
    await store.save(first)
    await store.save(second)
    fetched = await store.get(
        canonical_id="x", embedding_model_name="openai/test-embedding",
    )
    assert fetched is not None
    assert fetched.content_hash == "b"
    assert fetched.embedding == [2.0, 2.0, 2.0]
    rows = await store.list_for_model(
        embedding_model_name="openai/test-embedding",
    )
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# save_many / get_many — batched APIs
# ---------------------------------------------------------------------------


async def test_save_many_persists_all(store: SidecarEmbeddingStore) -> None:
    rows = [
        _embed(canonical_id=f"e{i}", text_hash=f"h{i}", vector=[float(i)] * 3)
        for i in range(5)
    ]
    await store.save_many(rows)
    listed = await store.list_for_model(
        embedding_model_name="openai/test-embedding",
    )
    assert {r.canonical_id for r in listed} == {f"e{i}" for i in range(5)}


async def test_save_many_empty_is_noop(
    store: SidecarEmbeddingStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty input must not connect to SQLite, must not raise."""
    from slayer.storage import sidecar_embedding_store as _mod

    real_connect = _mod.sqlite3.connect
    connect_calls: list[tuple] = []

    def _spy(*args, **kwargs):
        connect_calls.append(args)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(_mod.sqlite3, "connect", _spy)
    await store.save_many([])
    # Short-circuit: no connection opened.
    assert connect_calls == []
    # Outcome: no rows were written either.
    listed = await store.list_for_model(
        embedding_model_name="openai/test-embedding",
    )
    assert listed == []


async def test_save_many_atomic_within_batch(
    store: SidecarEmbeddingStore,
) -> None:
    """save_many writes every row in one transaction — listing after the
    call returns every row's final content_hash."""
    rows = [
        _embed(canonical_id="dup", text_hash="first", vector=[1.0] * 3),
        _embed(canonical_id="dup", text_hash="second", vector=[2.0] * 3),
    ]
    await store.save_many(rows)
    fetched = await store.get(
        canonical_id="dup",
        embedding_model_name="openai/test-embedding",
    )
    assert fetched is not None
    # INSERT OR REPLACE: the second wins.
    assert fetched.content_hash == "second"


async def test_get_many_returns_dict(store: SidecarEmbeddingStore) -> None:
    await store.save_many([
        _embed(canonical_id="a", text_hash="ha"),
        _embed(canonical_id="b", text_hash="hb"),
        _embed(canonical_id="c", text_hash="hc"),
    ])
    out = await store.get_many(
        canonical_ids=["a", "c", "missing"],
        embedding_model_name="openai/test-embedding",
    )
    assert set(out.keys()) == {"a", "c"}
    assert out["a"].content_hash == "ha"
    assert out["c"].content_hash == "hc"


async def test_get_many_filters_by_model_name(
    store: SidecarEmbeddingStore,
) -> None:
    """A canonical_id present under model A must not surface in a
    get_many request for model B."""
    await store.save(_embed(canonical_id="x", model="openai/a"))
    await store.save(_embed(canonical_id="x", model="openai/b"))
    out_a = await store.get_many(
        canonical_ids=["x"], embedding_model_name="openai/a",
    )
    out_b = await store.get_many(
        canonical_ids=["x"], embedding_model_name="openai/b",
    )
    assert set(out_a.keys()) == {"x"}
    assert out_a["x"].embedding_model_name == "openai/a"
    assert set(out_b.keys()) == {"x"}
    assert out_b["x"].embedding_model_name == "openai/b"


async def test_get_many_chunks_large_id_lists(
    store: SidecarEmbeddingStore,
) -> None:
    """REGRESSION (CodeRabbit DEV-1405): get_many must chunk the IN
    clause so it never exceeds SQLite's MAX_VARIABLE_NUMBER (32766 on
    most builds). Use 2000 ids — well above the 900 chunk size — and
    verify every id round-trips intact."""
    rows = [
        _embed(canonical_id=f"e{i}", text_hash=f"h{i}")
        for i in range(2000)
    ]
    await store.save_many(rows)
    out = await store.get_many(
        canonical_ids=[f"e{i}" for i in range(2000)],
        embedding_model_name="openai/test-embedding",
    )
    assert len(out) == 2000
    assert out["e0"].content_hash == "h0"
    assert out["e1999"].content_hash == "h1999"


async def test_get_many_empty_input_returns_empty(
    store: SidecarEmbeddingStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``canonical_ids=[]`` must short-circuit without hitting SQLite."""
    await store.save(_embed(canonical_id="x"))
    from slayer.storage import sidecar_embedding_store as _mod

    real_connect = _mod.sqlite3.connect
    connect_calls: list[tuple] = []

    def _spy(*args, **kwargs):
        connect_calls.append(args)
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(_mod.sqlite3, "connect", _spy)
    out = await store.get_many(
        canonical_ids=[],
        embedding_model_name="openai/test-embedding",
    )
    assert out == {}
    assert connect_calls == []


# ---------------------------------------------------------------------------
# list_for_model
# ---------------------------------------------------------------------------


async def test_list_for_model_filters(store: SidecarEmbeddingStore) -> None:
    await store.save(_embed(canonical_id="a", model="openai/x"))
    await store.save(_embed(canonical_id="b", model="openai/x"))
    await store.save(_embed(canonical_id="c", model="openai/y"))
    rows_x = await store.list_for_model(embedding_model_name="openai/x")
    rows_y = await store.list_for_model(embedding_model_name="openai/y")
    assert {r.canonical_id for r in rows_x} == {"a", "b"}
    assert {r.canonical_id for r in rows_y} == {"c"}


# ---------------------------------------------------------------------------
# delete_for_canonical — descendant semantics, prefix-greedy regressions
# ---------------------------------------------------------------------------


async def test_delete_for_canonical_exact_match_only_when_no_descendants(
    store: SidecarEmbeddingStore,
) -> None:
    """``memory:<int>`` ids have no dotted descendants; exact-match arm
    handles them."""
    await store.save(_embed(canonical_id="memory:1"))
    removed = await store.delete_for_canonical(canonical_id_prefix="memory:1")
    assert removed == 1
    assert await store.get(
        canonical_id="memory:1",
        embedding_model_name="openai/test-embedding",
    ) is None


async def test_delete_memory_prefix_does_not_match_numeric_siblings(
    store: SidecarEmbeddingStore,
) -> None:
    """REGRESSION (DEV-1405): the previous LIKE 'memory:4%' pattern
    nuked memory:42, memory:43, memory:400 along with memory:4. The
    fix matches exact id OR a strict ``.``-descendant — memory ids have
    no descendants, so only memory:4 must go."""
    for cid in ("memory:4", "memory:42", "memory:43", "memory:400"):
        await store.save(_embed(canonical_id=cid))
    removed = await store.delete_for_canonical(canonical_id_prefix="memory:4")
    assert removed == 1
    remaining = {
        r.canonical_id for r in await store.list_for_model(
            embedding_model_name="openai/test-embedding",
        )
    }
    assert remaining == {"memory:42", "memory:43", "memory:400"}


async def test_delete_datasource_prefix_does_not_match_char_siblings(
    store: SidecarEmbeddingStore,
) -> None:
    """REGRESSION (DEV-1405): deleting ds ``orders`` must not nuke
    ``orders_archive``, ``orders123``, or their descendants."""
    for cid in (
        "orders",
        "orders.foo",
        "orders.foo.bar",
        "orders_archive",
        "orders_archive.foo",
        "orders123",
    ):
        await store.save(_embed(canonical_id=cid))
    removed = await store.delete_for_canonical(canonical_id_prefix="orders")
    assert removed == 3
    remaining = {
        r.canonical_id for r in await store.list_for_model(
            embedding_model_name="openai/test-embedding",
        )
    }
    assert remaining == {
        "orders_archive", "orders_archive.foo", "orders123",
    }


async def test_delete_model_prefix_does_not_match_sibling_models(
    store: SidecarEmbeddingStore,
) -> None:
    """REGRESSION (DEV-1405): deleting model ``orders.customers`` must
    not nuke ``orders.customers_v2`` or ``orders.customers_v2.id``."""
    for cid in (
        "orders.customers",
        "orders.customers.id",
        "orders.customers_v2",
        "orders.customers_v2.id",
    ):
        await store.save(_embed(canonical_id=cid))
    removed = await store.delete_for_canonical(
        canonical_id_prefix="orders.customers",
    )
    assert removed == 2
    remaining = {
        r.canonical_id for r in await store.list_for_model(
            embedding_model_name="openai/test-embedding",
        )
    }
    assert remaining == {"orders.customers_v2", "orders.customers_v2.id"}


async def test_delete_for_canonical_no_match_returns_zero(
    store: SidecarEmbeddingStore,
) -> None:
    await store.save(_embed(canonical_id="x"))
    removed = await store.delete_for_canonical(canonical_id_prefix="no.match")
    assert removed == 0
    assert len(await store.list_for_model(
        embedding_model_name="openai/test-embedding",
    )) == 1


async def test_delete_escapes_like_wildcards(
    store: SidecarEmbeddingStore,
) -> None:
    """Wildcard literals in canonical ids would be invalid input today,
    but the LIKE-escape contract is worth pinning. A prefix containing a
    literal ``%`` must not match arbitrary characters."""
    # The helper currently exposes no validator on canonical_id, so the
    # public ``save_many`` will accept these "wildcard" strings as-is.
    # That's exactly the input we want: the test pins that ``%``/``_``
    # are escaped in the LIKE pattern so ``a%b`` matches only ``a%b``
    # (and its strict ``.``-descendants), never ``aXb``.
    rows = [
        _embed(canonical_id="a%b"),
        _embed(canonical_id="aXb"),
        _embed(canonical_id="a%b.child"),
    ]
    await store.save_many(rows)
    removed = await store.delete_for_canonical(canonical_id_prefix="a%b")
    # Should hit a%b (exact) and a%b.child (descendant), but not aXb.
    assert removed == 2
    remaining = {
        r.canonical_id for r in await store.list_for_model(
            embedding_model_name="openai/test-embedding",
        )
    }
    assert remaining == {"aXb"}
