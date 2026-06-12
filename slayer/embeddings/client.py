"""Litellm wrapper for embedding generation (DEV-1386).

This module is the only place that imports ``litellm`` (and only lazily).
When the ``advanced_search`` extra is not installed, ``is_available()``
returns ``False`` and every call returns the no-op shape — the caller is
expected to short-circuit and skip the embedding channel entirely.

Environment contract: ``SLAYER_EMBEDDING_MODEL`` overrides the default
``openai/text-embedding-3-small``. Provider credentials
(``OPENAI_API_KEY``, ``AZURE_API_KEY``, etc.) are read by litellm itself
per its standard env-var conventions.

DEV-1557: ``embed_batch`` no longer treats one over-cap input as a
batch-killer. Each text is token-truncated to the model's reported cap
(minus a 256-token margin) via ``truncate_text_for_model`` before the
call; if the batch still raises ``BadRequestError``, we fall back to
embedding each text individually so good inputs survive.
"""

from __future__ import annotations

import inspect
import logging
import os
import warnings
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple, Type


DEFAULT_EMBEDDING_MODEL = "openai/text-embedding-3-small"
SLAYER_EMBEDDING_MODEL_ENV = "SLAYER_EMBEDDING_MODEL"


_log = logging.getLogger(__name__)


# litellm's GLOBAL_LOGGING_WORKER enqueues an async_success_handler coroutine
# after every aembedding call. Under run_sync (notebook / CLI) each call gets a
# fresh event loop that is torn down before the worker drains its queue, so
# litellm's next call nils _queue on loop-change detection and GC surfaces the
# orphans as RuntimeWarnings. The work is litellm-internal telemetry with no
# off-switch — filter the one warning at the import-time boundary.
warnings.filterwarnings(
    "ignore",
    message=r"coroutine 'Logging\.async_success_handler' was never awaited",
    category=RuntimeWarning,
)


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

    1. The ``advanced_search`` extra is installed (``litellm`` imports).
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


# DEV-1557: 256-token margin between the model's reported cap and our
# usable budget. text-embedding-3-small reports cap=8191 from litellm
# and accepts up to 8192 tokens server-side; 256 leaves room for any
# provider-side BOS/role-token overhead and minor cap-introspection
# drift across litellm versions, while still keeping the budget useful
# for very large caps (Voyage's 32K → 31744-token budget).
_TRUNCATE_MARGIN_TOKENS = 256
_CAP_FALLBACK = 8192
_PREVIEW_CHARS = 80


_CAP_CACHE: Dict[str, int] = {}


def _try_get_max_tokens(model: str) -> Optional[int]:
    """Best-effort ``litellm.utils.get_max_tokens`` lookup. Returns the
    cap if it's a positive int, else ``None``. Never raises."""
    try:
        from litellm import utils as litellm_utils
    except Exception:  # noqa: BLE001 — litellm absent or import-broken
        return None
    try:
        cap = litellm_utils.get_max_tokens(model)
    except Exception:  # noqa: BLE001 — litellm model-map drift / network blip
        return None
    if isinstance(cap, int) and cap > 0:
        return cap
    return None


def _resolve_model_cap(resolved_model: str) -> Optional[int]:
    """Look up the model's max-token cap, trying provider-prefixed name
    first and the bare name as a fallback. Caches only positive-int
    successes so a transient lookup failure isn't sticky."""
    cached = _CAP_CACHE.get(resolved_model)
    if cached is not None:
        return cached
    cap = _try_get_max_tokens(resolved_model)
    if cap is None:
        bare = resolved_model.rsplit("/", 1)[-1]
        if bare and bare != resolved_model:
            cap = _try_get_max_tokens(bare)
    if cap is not None:
        _CAP_CACHE[resolved_model] = cap
    return cap


def _clear_cap_cache() -> None:
    """Test hook (matches the ``cache_clear`` shape used by lru_cache)."""
    _CAP_CACHE.clear()


# Surface ``cache_clear`` on the function itself so test fixtures can
# clear it uniformly with the lru-cached encoder helper below.
_resolve_model_cap.cache_clear = _clear_cap_cache  # type: ignore[attr-defined]


@lru_cache(maxsize=8)
def _resolve_encoder(bare_model_name: str) -> Any:
    """Return a tiktoken encoder for the given bare model name (with
    provider prefix already stripped). Falls back to ``cl100k_base`` on
    KeyError. Raises ``ImportError`` if tiktoken is unavailable —
    callers must catch and degrade to identity truncation."""
    import tiktoken  # noqa: PLC0415 — lazy import gated by advanced_search
    try:
        return tiktoken.encoding_for_model(bare_model_name)
    except KeyError:
        return tiktoken.get_encoding("cl100k_base")


def _strip_provider_prefix(model: str) -> str:
    return model.rsplit("/", 1)[-1] if "/" in model else model


def truncate_text_for_model(
    text: str, *, model: Optional[str] = None,
) -> str:
    """Truncate ``text`` to the resolved model's token cap minus a
    fixed 256-token margin (DEV-1557).

    Returns ``text`` unchanged when already under budget (no decode
    round-trip). Head-keep slicing — the prefix of the input is
    preserved because SLayer's rendered entity / memory text leads
    with the most signal-rich fields.

    Defensive: if tiktoken is unavailable (the lazy import in
    ``_resolve_encoder`` raises) or any other encoder-resolution
    failure surfaces, returns ``text`` unchanged. The per-input retry
    in :func:`embed_batch` is what saves the batch when truncation
    degrades to identity.
    """
    resolved_model = model or current_model()
    try:
        encoder = _resolve_encoder(_strip_provider_prefix(resolved_model))
    except Exception:  # noqa: BLE001 — tiktoken missing / unknown failure
        return text

    tokens = encoder.encode(text)
    cap = _resolve_model_cap(resolved_model) or _CAP_FALLBACK
    budget = max(0, cap - _TRUNCATE_MARGIN_TOKENS)

    if len(tokens) <= budget:
        return text

    truncated_tokens = tokens[:budget]
    truncated = encoder.decode(truncated_tokens) if truncated_tokens else ""
    _log.warning(
        "truncated text for model=%s: original_tokens=%d post_tokens=%d "
        "original_chars=%d post_chars=%d preview=%r",
        resolved_model, len(tokens), len(truncated_tokens),
        len(text), len(truncated), text[:_PREVIEW_CHARS],
    )
    return truncated


def _get_bad_request_exception_classes() -> Tuple[Type[BaseException], ...]:
    """Return the exception class(es) representing litellm's
    ``BadRequestError`` — empty tuple if neither ``litellm`` nor
    ``litellm.exceptions`` exposes one. Tuple form lets us use the
    result directly in an ``except`` clause; an empty tuple safely
    catches nothing so the generic-exception fallback path takes over.
    """
    classes: List[Type[BaseException]] = []
    try:
        import litellm  # noqa: PLC0415 — lazy
    except Exception:  # noqa: BLE001
        return ()
    cls = getattr(litellm, "BadRequestError", None)
    if inspect.isclass(cls) and issubclass(cls, BaseException):
        classes.append(cls)
    try:
        from litellm import exceptions as _exc_mod  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        _exc_mod = None
    if _exc_mod is not None:
        cls2 = getattr(_exc_mod, "BadRequestError", None)
        if (
            inspect.isclass(cls2)
            and issubclass(cls2, BaseException)
            and cls2 not in classes
        ):
            classes.append(cls2)
    return tuple(classes)


def _parse_vectors(response: Any, n: int) -> List[Optional[List[float]]]:
    """Pack a litellm aembedding response into ``n`` per-slot vectors,
    padding short responses with ``None``."""
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
    while len(out) < n:
        out.append(None)
    return out[:n]


async def _per_input_retry(
    litellm: Any,
    resolved_model: str,
    truncated_texts: List[str],
    bad_request_classes: Tuple[Type[BaseException], ...],
) -> List[Optional[List[float]]]:
    """Embed each text in ``truncated_texts`` individually. On a
    per-text ``BadRequestError`` the slot is ``None`` and the loop
    continues; on any other exception we treat it as a global failure
    shape (rate limit / auth / network), mark the current slot and
    every remaining slot ``None``, and return early."""
    results: List[Optional[List[float]]] = []
    for idx, text in enumerate(truncated_texts):
        try:
            response = await litellm.aembedding(
                model=resolved_model, input=[text],
            )
        except bad_request_classes as exc:
            _log.warning(
                "embed_batch per-input retry: BadRequestError at idx=%d "
                "for model=%s: %s — slot marked None",
                idx, resolved_model, exc,
            )
            results.append(None)
            continue
        except Exception as exc:  # noqa: BLE001 — see docstring
            _log.warning(
                "embed_batch per-input retry: %s at idx=%d for model=%s: "
                "%s — global failure shape, aborting retries "
                "(remaining %d slot(s) None)",
                type(exc).__name__, idx, resolved_model, exc,
                len(truncated_texts) - len(results) - 1,
            )
            results.append(None)
            results.extend([None] * (len(truncated_texts) - len(results)))
            return results
        results.extend(_parse_vectors(response, 1))
    return results


async def embed_batch(
    texts: List[str], *, model: Optional[str] = None,
) -> List[Optional[List[float]]]:
    """Embed a batch of texts via ``litellm.aembedding`` (DEV-1557).

    Each text is preemptively truncated to fit the resolved model's
    token cap (see :func:`truncate_text_for_model`). If the batch call
    still raises ``litellm.BadRequestError``, we fall back to embedding
    each text individually so a single over-cap input no longer
    poisons the whole batch.

    Returns one vector (or ``None``) per input text in input order:

    * Empty ``texts`` → ``[]`` (no API call).
    * ``is_available()`` False → ``[None] * len(texts)`` (no truncation,
      no API call).
    * Batch ``BadRequestError`` → per-input retry; each text gets its
      own success/failure verdict.
    * Any other batch exception (rate limit / auth / network) → log
      warning, return ``[None] * len(texts)``.
    """
    if not texts:
        return []
    if not is_available():
        return [None] * len(texts)
    resolved_model = model or current_model()
    truncated = [
        truncate_text_for_model(t, model=resolved_model) for t in texts
    ]
    import litellm  # noqa: PLC0415 — lazy
    bad_request_classes = _get_bad_request_exception_classes()
    try:
        response = await litellm.aembedding(
            model=resolved_model, input=truncated,
        )
    except bad_request_classes as exc:
        _log.warning(
            "embed_batch BadRequestError for model=%s (n=%d): %s — "
            "falling back to per-input retry",
            resolved_model, len(truncated), exc,
        )
        return await _per_input_retry(
            litellm, resolved_model, truncated, bad_request_classes,
        )
    except Exception as exc:
        _log.warning(
            "embed_batch failed for model=%s (n=%d): %s",
            resolved_model, len(truncated), exc,
        )
        return [None] * len(truncated)
    return _parse_vectors(response, len(truncated))


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
