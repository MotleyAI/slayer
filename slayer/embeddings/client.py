"""Litellm wrapper for embedding generation; the only (lazy) importer of ``litellm``.

``SLAYER_EMBEDDING_MODEL`` overrides the default model; litellm reads provider credentials itself.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import os
import warnings
from functools import lru_cache
from typing import Any


DEFAULT_EMBEDDING_MODEL = "openai/text-embedding-3-small"
SLAYER_EMBEDDING_MODEL_ENV = "SLAYER_EMBEDDING_MODEL"


_log = logging.getLogger(__name__)


# litellm's logging worker orphans this coroutine when run_sync tears down its loop; telemetry only.
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
    """True iff ``litellm`` imports and the configured model has an API key; cached per process."""
    try:
        import litellm  # ALLOW(import-not-top): optional dep, lazy
    except ImportError:
        return False
    try:
        validation = litellm.validate_environment(model=current_model())
    except Exception:  # noqa: BLE001 — unknown model / litellm version drift
        # Trust the user and let the actual embed call surface any error.
        return True
    return bool(validation.get("keys_in_environment", False))


async def is_available_async() -> bool:
    """``is_available()`` run in a worker thread, so its lazy import never blocks the loop."""
    return await asyncio.to_thread(is_available)


# Headroom below the reported cap for provider token overhead and cap drift.
_TRUNCATE_MARGIN_TOKENS = 256
_CAP_FALLBACK = 8192
_HASH_PREFIX_CHARS = 16


_CAP_CACHE: dict[str, int] = {}


def _try_get_max_tokens(model: str) -> int | None:
    """The model's positive-int cap from litellm, else ``None``; never raises."""
    try:
        from litellm import utils as litellm_utils  # ALLOW(import-not-top): optional dep, lazy
    except Exception:  # noqa: BLE001 — litellm absent or import-broken
        return None
    try:
        cap = litellm_utils.get_max_tokens(model)
    except Exception:  # noqa: BLE001 — litellm model-map drift / network blip
        return None
    if isinstance(cap, int) and cap > 0:
        return cap
    return None


def _resolve_model_cap(resolved_model: str) -> int | None:
    """Max-token cap by prefixed then bare name; only successes are cached."""
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
    _CAP_CACHE.clear()


_resolve_model_cap.cache_clear = _clear_cap_cache  # type: ignore[attr-defined]


@lru_cache(maxsize=8)
def _resolve_encoder(bare_model_name: str) -> Any:
    """Tiktoken encoder for a bare model name, else ``cl100k_base``; raises ``ImportError`` without tiktoken."""
    import tiktoken  # noqa: PLC0415  # ALLOW(import-not-top): optional dep, lazy
    try:
        return tiktoken.encoding_for_model(bare_model_name)
    except KeyError:
        return tiktoken.get_encoding("cl100k_base")


def _strip_provider_prefix(model: str) -> str:
    return model.rsplit("/", 1)[-1] if "/" in model else model


def truncate_text_for_model(
    text: str, *, model: str | None = None,
) -> str:
    """Keep the head of ``text`` within the model's token cap minus a margin; unchanged on any encoder failure."""
    resolved_model = model or current_model()
    try:
        encoder = _resolve_encoder(_strip_provider_prefix(resolved_model))
    except Exception:  # noqa: BLE001 — tiktoken missing / unknown failure
        return text

    # User text may contain literal special tokens such as ``<|endoftext|>``.
    try:
        tokens = encoder.encode(text, disallowed_special=())
    except Exception:  # noqa: BLE001 — tokenisation failure → degrade gracefully
        return text
    cap = _resolve_model_cap(resolved_model) or _CAP_FALLBACK
    budget = max(0, cap - _TRUNCATE_MARGIN_TOKENS)

    if len(tokens) <= budget:
        return text

    truncated_tokens = tokens[:budget]
    truncated = encoder.decode(truncated_tokens) if truncated_tokens else ""
    # A hash, not a preview, so user content stays out of the logs.
    text_digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[
        :_HASH_PREFIX_CHARS
    ]
    _log.warning(
        "truncated text for model=%s: original_tokens=%d post_tokens=%d "
        "original_chars=%d post_chars=%d sha256_prefix=%s",
        resolved_model, len(tokens), len(truncated_tokens),
        len(text), len(truncated), text_digest,
    )
    return truncated


def _get_bad_request_exception_classes() -> tuple[type[BaseException], ...]:
    """litellm's ``BadRequestError`` class(es) for an ``except`` clause; empty when absent."""
    classes: list[type[BaseException]] = []
    try:
        import litellm  # noqa: PLC0415  # ALLOW(import-not-top): optional dep, lazy
    except Exception:  # noqa: BLE001
        return ()
    cls = getattr(litellm, "BadRequestError", None)
    if inspect.isclass(cls) and issubclass(cls, BaseException):
        classes.append(cls)
    try:
        from litellm import exceptions as _exc_mod  # noqa: PLC0415  # ALLOW(import-not-top): optional dep, lazy
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


def _parse_vectors(response: Any, n: int) -> list[list[float] | None]:
    """``n`` vectors from an aembedding response, ``None``-padded."""
    data = getattr(response, "data", None) or []
    out: list[list[float] | None] = []
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
    truncated_texts: list[str],
    bad_request_classes: tuple[type[BaseException], ...],
) -> list[list[float] | None]:
    """Embed each text alone: a ``BadRequestError`` voids its slot; any other error voids the rest."""
    results: list[list[float] | None] = []
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


def _truncate_all(texts: list[str], model: str) -> list[str]:
    return [truncate_text_for_model(t, model=model) for t in texts]


async def embed_batch(
    texts: list[str], *, model: str | None = None,
) -> list[list[float] | None]:
    """One vector or ``None`` per text, in order; a batch ``BadRequestError`` retries per text."""
    if not texts:
        return []
    if not await is_available_async():
        return [None] * len(texts)
    resolved_model = model or current_model()
    truncated = await asyncio.to_thread(
        _truncate_all, texts, resolved_model,
    )
    import litellm  # noqa: PLC0415  # ALLOW(import-not-top): optional dep, lazy
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


_QUERY_CACHE: "dict[tuple[str, str], list[float]]" = {}
_QUERY_CACHE_MAX = 64


async def embed_query(text: str, *, model: str | None = None) -> list[float] | None:
    """Embed one query through a process-wide LRU cache; ``None`` when unavailable or failed."""
    if not text or not text.strip():
        return None
    resolved_model = model or current_model()
    key = (resolved_model, text)
    cached = _QUERY_CACHE.get(key)
    if cached is not None:
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
