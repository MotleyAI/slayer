"""DEV-1943 §6.2 — the Python docs tell readers to close an engine over an
in-memory SQLite datasource, so the private engine is disposed."""

from __future__ import annotations

from pathlib import Path

_PYTHON_DOCS = Path(__file__).resolve().parent.parent / "docs" / "getting-started" / "python.md"


def test_python_docs_mention_engine_close() -> None:
    text = _PYTHON_DOCS.read_text()
    assert "engine.close()" in text, "python.md must document engine.close()"
    # The guidance must tie close() to in-memory datasources in the same paragraph.
    paragraphs = [p for p in text.split("\n\n") if "engine.close()" in p]
    assert any("memory" in p.lower() for p in paragraphs), (
        "engine.close() must be documented alongside in-memory SQLite datasources"
    )
