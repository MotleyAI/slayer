"""Embedding storage + litellm client wrapper (DEV-1386).

The orchestrator that owns the refresh pipeline + cosine ranking lives
in :mod:`slayer.search.retrievers.embeddings` as of DEV-1514. This
package exposes only the persisted ``Embedding`` row; the litellm
client wrapper is in :mod:`slayer.embeddings.client` and the cosine
helpers are in :mod:`slayer.embeddings.ranker`.
"""

from slayer.embeddings.models import Embedding

__all__ = ["Embedding"]
