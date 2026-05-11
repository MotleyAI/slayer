"""Litellm wrapper for embedding generation (DEV-1386).

This module is the only place that imports ``litellm`` (and only lazily).
When the ``embedding_search`` extra is not installed, ``is_available()``
returns ``False`` and every call returns the no-op shape — the caller is
expected to short-circuit and skip the embedding channel entirely.

Environment contract: ``SLAYER_EMBEDDING_MODEL`` overrides the default
``openai/text-embedding-3-small``. Provider credentials
(``OPENAI_API_KEY``, ``AZURE_API_KEY``, etc.) are read by litellm itself
per its standard env-var conventions.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import List, Optional


DEFAULT_EMBEDDING_MODEL = "openai/text-embedding-3-small"
SLAYER_EMBEDDING_MODEL_ENV = "SLAYER_EMBEDDING_MODEL"


_log = logging.getLogger(__name__)


def current_model() -> str:
    """Resolve the active embedding model name from the environment."""
    value = os.environ.get(SLAYER_EMBEDDING_MODEL_ENV)
    if value is not None and value.strip():
        return value.strip()
    return DEFAULT_EMBEDDING_MODEL


@lru_cache(maxsize=1)
def is_available() -> bool:
    """Return True iff the ``embedding_search`` extra is installed.

    Cached for the lifetime of the process — installing a package mid-run
    is not a supported scenario. Tests that need to toggle availability
    should patch this symbol directly.
    """
    try:
        import litellm  # noqa: F401
        return True
    except ImportError:
        return False


async def embed_batch(
    texts: List[str], *, model: Optional[str] = None,
) -> List[Optional[List[float]]]:
    """Embed a batch of texts via ``litellm.aembedding``.

    Returns one vector per input text in input order. On any exception
    (rate limit, bad key, network), logs a warning and returns
    ``[None] * len(texts)`` — callers persist only the non-None entries.

    Empty ``texts`` short-circuits to ``[]`` without an API call.
    """
    if not texts:
        return []
    if not is_available():
        return [None] * len(texts)
    resolved_model = model or current_model()
    try:
        import litellm
        response = await litellm.aembedding(model=resolved_model, input=texts)
    except Exception as exc:
        _log.warning(
            "embed_batch failed for model=%s (n=%d): %s",
            resolved_model, len(texts), exc,
        )
        return [None] * len(texts)
    data = getattr(response, "data", None) or []
    out: List[Optional[List[float]]] = []
    for entry in data:
        if isinstance(entry, dict):
            vec = entry.get("embedding")
        else:
            vec = getattr(entry, "embedding", None)
        if isinstance(vec, list) and all(isinstance(v, (int, float)) for v in vec):
            out.append([float(v) for v in vec])
        else:
            out.append(None)
    # If litellm returned fewer rows than requested, pad with None.
    while len(out) < len(texts):
        out.append(None)
    return out[: len(texts)]


_QUERY_CACHE: "dict[tuple[str, str], List[float]]" = {}
_QUERY_CACHE_MAX = 64


async def embed_query(text: str, *, model: Optional[str] = None) -> Optional[List[float]]:
    """Embed a single query string with a small process-wide LRU cache.

    Returns ``None`` when the extra is not installed or the embedding call
    failed — the search service skips channel 3 in that case.
    """
    if not text or not text.strip():
        return None
    resolved_model = model or current_model()
    key = (resolved_model, text)
    cached = _QUERY_CACHE.get(key)
    if cached is not None:
        return cached
    result = await embed_batch([text], model=resolved_model)
    vec = result[0] if result else None
    if vec is None:
        return None
    if len(_QUERY_CACHE) >= _QUERY_CACHE_MAX:
        # Evict the oldest entry. Insertion order is preserved by dict in
        # Python 3.7+, so the first iterated key is the oldest.
        oldest_key = next(iter(_QUERY_CACHE))
        _QUERY_CACHE.pop(oldest_key, None)
    _QUERY_CACHE[key] = vec
    return vec


def _reset_query_cache() -> None:
    """Test hook: clear the in-process query embedding cache."""
    _QUERY_CACHE.clear()
