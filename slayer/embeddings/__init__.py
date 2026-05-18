"""Embedding-based semantic search channel (DEV-1386).

Exposes the persisted ``Embedding`` row. The ``EmbeddingService`` orchestrator
and the litellm client wrapper are intentionally not re-exported here — they
import from ``slayer.storage.base``, which imports from this package, so eager
re-export would create a cycle. Import them directly from
``slayer.embeddings.service`` / ``slayer.embeddings.client`` when needed.
"""

from slayer.embeddings.models import Embedding

__all__ = ["Embedding"]
