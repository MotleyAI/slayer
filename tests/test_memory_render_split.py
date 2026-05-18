"""DEV-1428: split memory rendering between tantivy and embedding paths.

* ``render_memory_text(memory)`` retains current shape — learning text
  PLUS the "Tagged entities: ..." line. Used by tantivy indexer.
* ``render_memory_text_for_embedding(memory)`` returns ``memory.learning``
  ONLY — entity tags excluded so cascade-strip never changes the content
  hash.
"""

from __future__ import annotations

from slayer.memories.models import Memory
from slayer.search import render as _render_mod
from slayer.search.render import render_memory_text


def _render_for_embedding(memory: Memory) -> str:
    # Deferred lookup so collection succeeds before Phase 2 lands the
    # new symbol; the test below asserts its existence directly.
    fn = getattr(_render_mod, "render_memory_text_for_embedding", None)
    assert fn is not None, (
        "render_memory_text_for_embedding not implemented yet "
        "(Phase 2.4)"
    )
    return fn(memory=memory)


class TestRenderSplit:
    def test_tantivy_renderer_includes_tags(self) -> None:
        m = Memory(
            id="1",
            learning="amount is cents",
            entities=["mydb.orders.amount"],
        )
        text = render_memory_text(memory=m)
        assert "amount is cents" in text
        assert "Tagged entities" in text
        assert "mydb.orders.amount" in text

    def test_embedding_renderer_excludes_tags(self) -> None:
        m = Memory(
            id="1",
            learning="amount is cents",
            entities=["mydb.orders.amount"],
        )
        text = _render_for_embedding(m)
        assert text == "amount is cents"
        assert "Tagged entities" not in text

    def test_embedding_renderer_strips_tag_change(self) -> None:
        m1 = Memory(
            id="1",
            learning="x",
            entities=["mydb.orders.amount", "mydb.deleted"],
        )
        m2 = m1.model_copy(update={"entities": ["mydb.orders.amount"]})
        # The whole point: cascade-strip rewrites tags but the embedded
        # text doesn't change → content hash unchanged.
        assert _render_for_embedding(m1) == _render_for_embedding(m2)
