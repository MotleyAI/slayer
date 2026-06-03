"""Semantic search over memories + canonical entities.

Public surface re-exported here for ergonomic ``from slayer.search import …``
usage. The ``SearchService`` orchestrator runs up to three retrieval
channels — entity-overlap BM25 over memories, tantivy full-text over the
unioned corpus, and optional dense embedding similarity gated by the
``advanced_search`` extra — and fuses the memory rankings (and entity
rankings, for channels 2 and 3) via Reciprocal Rank Fusion.
"""

from slayer.search.service import (
    EntityHit,
    MemoryHit,
    SearchResponse,
    SearchService,
)

__all__ = [
    "EntityHit",
    "MemoryHit",
    "SearchResponse",
    "SearchService",
]
