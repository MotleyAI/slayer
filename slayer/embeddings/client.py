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
    """Return True iff the embedding channel is usable.

    Two conditions, both required:

    1. The ``embedding_search`` extra is installed (``litellm`` imports).
    2. The configured embedding model has a usable API key in the
       environment, per ``litellm.validate_environment``.

    Both "extra not installed" and "extra installed but no API key" yield
    ``False`` — the write-side refresh hooks short-circuit silently in
    that case, and the search service emits a single user-visible
    warning into ``SearchResponse.warnings``. This distinction matters
    on CI where the extra is installed (for unit-test imports) but no
    provider key is configured: per-entity refresh warnings would
    otherwise spam ``save_memory`` / ``ingest`` / ``edit_model``
    responses for a "feature not configured" case.

    A genuine runtime error (rate limit, network blip, revoked key) is
    a separate code path: ``embed_batch`` catches the exception there
    and per-entity warnings *do* bubble up, surfacing the failure to
    the user.

    Cached for the lifetime of the process; tests should clear with
    ``is_available.cache_clear()`` after touching env vars or patching
    the symbol.
    """
    try:
        import litellm
    except ImportError:
        return False
    try:
        validation = litellm.validate_environment(model=current_model())
    except Exception:  # noqa: BLE001 — unknown model / litellm version drift
        # Trust the user and let the actual embed call surface any error.
        return True
    return bool(validation.get("keys_in_environment", False))


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

    LRU semantics: on a cache hit, refresh recency by re-inserting the
    key at the end of the insertion-order dict. Eviction pops the
    oldest entry (front of the dict). Without the on-hit refresh the
    cache degenerates into FIFO and frequently-used keys still age out.
    """
    if not text or not text.strip():
        return None
    resolved_model = model or current_model()
    key = (resolved_model, text)
    cached = _QUERY_CACHE.get(key)
    if cached is not None:
        # Move-to-end on hit so eviction pops the genuinely least-
        # recently-used entry, not just the oldest inserted one.
        _QUERY_CACHE.pop(key, None)
        _QUERY_CACHE[key] = cached
        return cached
    result = await embed_batch([text], model=resolved_model)
    vec = result[0] if result else None
    if vec is None:
        return None
    if len(_QUERY_CACHE) >= _QUERY_CACHE_MAX:
        oldest_key = next(iter(_QUERY_CACHE))
        _QUERY_CACHE.pop(oldest_key, None)
    _QUERY_CACHE[key] = vec
    return vec


def _reset_query_cache() -> None:
    """Test hook: clear the in-process query embedding cache."""
    _QUERY_CACHE.clear()
