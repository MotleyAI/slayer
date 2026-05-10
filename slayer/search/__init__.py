"""Semantic search over memories + canonical entities (DEV-1375).

Public surface re-exported here for ergonomic ``from slayer.search import …``
usage. The ``SearchService`` orchestrator runs two parallel retrieval
channels (entity-overlap BM25 over memories + tantivy full-text over the
unioned corpus) and fuses the memory-channel results via RRF.
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
