"""Semantic search over memories + canonical entities.

Public surface re-exported here for ergonomic ``from slayer.search import …``
usage. The ``SearchService`` orchestrator runs up to three retrieval
channels — entity-overlap BM25 over memories, tantivy full-text over the
unioned corpus, and optional dense embedding similarity gated by the
``advanced_search`` extra — and fuses all hits via Reciprocal Rank Fusion
into a single ranked ``results`` list.
"""

from slayer.search.service import (
    SearchHit,
    SearchResponse,
    SearchService,
)

__all__ = [
    "SearchHit",
    "SearchResponse",
    "SearchService",
]
