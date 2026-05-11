"""Unit tests for ``slayer.embeddings.client`` (DEV-1386).

Covers env-var resolution, batch error fallback, query-cache LRU
behaviour, and the missing-extra short-circuit path. ``litellm`` is
mocked at the import boundary — no live API calls are made.
"""

from __future__ import annotations

import asyncio
from typing import Any, List, Optional

import pytest

from slayer.embeddings import client as embedding_client


@pytest.fixture(autouse=True)
def _reset_caches() -> None:
    """Clear per-process caches between tests so env-var changes propagate."""
    embedding_client.is_available.cache_clear()
    embedding_client._reset_query_cache()


def test_current_model_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SLAYER_EMBEDDING_MODEL", raising=False)
    assert embedding_client.current_model() == "openai/text-embedding-3-small"


def test_current_model_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLAYER_EMBEDDING_MODEL", "voyage/voyage-3")
    assert embedding_client.current_model() == "voyage/voyage-3"


def test_current_model_blank_env_uses_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SLAYER_EMBEDDING_MODEL", "   ")
    assert embedding_client.current_model() == "openai/text-embedding-3-small"


async def test_embed_batch_empty_input_short_circuits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: List[Any] = []

    async def fail(*_a: Any, **_kw: Any) -> Any:
        called.append("called")
        raise AssertionError("should not be called for empty input")

    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    # Even if available, empty input must not hit the SDK.
    assert await embedding_client.embed_batch([], model="openai/x") == []
    assert called == []


async def test_embed_batch_no_extra_returns_none_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(embedding_client, "is_available", lambda: False)
    result = await embedding_client.embed_batch(
        ["a", "b", "c"], model="openai/x",
    )
    assert result == [None, None, None]


async def test_embed_batch_calls_litellm_with_resolved_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: embed_batch dispatches to litellm.aembedding with the
    resolved model and packs the result into per-input vector lists."""
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    monkeypatch.setenv("SLAYER_EMBEDDING_MODEL", "openai/test-model")
    captured: dict = {}

    class _FakeResponse:
        def __init__(self, data: List[dict]) -> None:
            self.data = data

    async def fake_aembedding(*, model: str, input: List[str]) -> _FakeResponse:
        captured["model"] = model
        captured["input"] = list(input)
        return _FakeResponse(
            [{"embedding": [float(i)] * 4} for i, _ in enumerate(input)]
        )

    import litellm
    monkeypatch.setattr(litellm, "aembedding", fake_aembedding)
    vectors = await embedding_client.embed_batch(["a", "b"])
    assert captured["model"] == "openai/test-model"
    assert captured["input"] == ["a", "b"]
    assert vectors == [[0.0, 0.0, 0.0, 0.0], [1.0, 1.0, 1.0, 1.0]]


async def test_embed_batch_swallows_exception_and_returns_none_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)

    async def boom(*_a: Any, **_kw: Any) -> Any:
        raise RuntimeError("rate limit")

    import litellm
    monkeypatch.setattr(litellm, "aembedding", boom)
    result = await embedding_client.embed_batch(
        ["x", "y"], model="openai/x",
    )
    assert result == [None, None]


async def test_embed_batch_pads_short_response_with_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When litellm returns fewer rows than requested, the missing tail
    is padded with ``None`` so the caller can still zip back to inputs."""
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)

    class _FakeResponse:
        def __init__(self) -> None:
            self.data = [{"embedding": [1.0, 2.0]}]

    async def short_response(*_a: Any, **_kw: Any) -> Any:
        return _FakeResponse()

    import litellm
    monkeypatch.setattr(litellm, "aembedding", short_response)
    result = await embedding_client.embed_batch(["a", "b", "c"])
    assert result == [[1.0, 2.0], None, None]


async def test_embed_query_caches_repeated_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    call_count = {"n": 0}

    async def fake_embed_batch(
        texts: List[str], *, model: Optional[str] = None,
    ) -> List[Optional[List[float]]]:
        call_count["n"] += 1
        return [[float(call_count["n"])] * 3 for _ in texts]

    monkeypatch.setattr(embedding_client, "embed_batch", fake_embed_batch)
    a = await embedding_client.embed_query("repeated", model="m")
    b = await embedding_client.embed_query("repeated", model="m")
    assert a == [1.0, 1.0, 1.0]
    assert b == [1.0, 1.0, 1.0]  # cached → no second call
    assert call_count["n"] == 1


async def test_embed_query_empty_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    assert await embedding_client.embed_query("") is None
    assert await embedding_client.embed_query("   ") is None


async def test_embed_query_returns_none_on_extra_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(embedding_client, "is_available", lambda: False)
    assert await embedding_client.embed_query("hello") is None


def test_event_loop_imports() -> None:
    """Sanity: this module's async helpers can be invoked from a fresh
    loop without import side-effects."""
    assert asyncio.iscoroutinefunction(embedding_client.embed_batch)
    assert asyncio.iscoroutinefunction(embedding_client.embed_query)
