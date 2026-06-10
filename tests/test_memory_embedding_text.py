"""DEV-1549 Codex#5: ``Memory.description`` is included in the embedded
text so descriptions are semantically searchable. The cascade-strip
path (DEV-1428) only mutates ``entities``, so embedding content hashes
still skip recompute when only tags change.
"""

from __future__ import annotations

from slayer.memories.models import Memory
from slayer.search.render import render_memory_text_for_embedding


def test_embedding_text_without_description_uses_learning_only() -> None:
    mem = Memory(learning="amount in cents", description=None)
    text = render_memory_text_for_embedding(memory=mem)
    assert text == "amount in cents"


def test_embedding_text_includes_description_when_set() -> None:
    mem = Memory(
        learning="amount stored as integer cents",
        description="cents column",
    )
    text = render_memory_text_for_embedding(memory=mem)
    assert "amount stored as integer cents" in text
    assert "cents column" in text


def test_embedding_text_hash_stable_when_only_entities_change() -> None:
    """Cascade-strip rewrites ``entities`` only — the embedded text must
    not change so the embedding refresh hash-skip survives (DEV-1428
    invariant)."""
    a = Memory(
        learning="x", description="d",
        entities=["mydb.orders.amount"],
    )
    b = Memory(
        learning="x", description="d",
        entities=["mydb.orders.amount", "mydb.orders.status"],
    )
    assert render_memory_text_for_embedding(memory=a) == (
        render_memory_text_for_embedding(memory=b)
    )


def test_embedding_text_changes_when_description_changes() -> None:
    """Description IS part of the embedded text now, so editing it
    must invalidate the hash (otherwise the new description never
    surfaces semantically)."""
    a = Memory(learning="x", description="old")
    b = Memory(learning="x", description="new")
    assert render_memory_text_for_embedding(memory=a) != (
        render_memory_text_for_embedding(memory=b)
    )
