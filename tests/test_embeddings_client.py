"""Unit tests for ``slayer.embeddings.client`` (DEV-1386).

Covers env-var resolution, batch error fallback, query-cache LRU
behaviour, and the missing-extra short-circuit path. ``litellm`` is
mocked at the import boundary — no live API calls are made.
"""

from __future__ import annotations

import asyncio
from typing import Any, List, Optional, Tuple

import pytest

from slayer.embeddings import client as embedding_client


@pytest.fixture(autouse=True)
def _reset_caches() -> None:
    """Clear the query-embedding cache between tests so a prior stub's
    response doesn't leak in. ``is_available`` is reset to a False stub
    by the conftest autouse fixture, so it is not re-cleared here."""
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
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    # Even if available, empty input must not hit the SDK.
    assert await embedding_client.embed_batch([], model="openai/x") == []


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

    async def fake_aembedding(*, model: str, input: List[str]) -> _FakeResponse:  # NOSONAR(S7503) — stub matches litellm.aembedding async signature
        captured["model"] = model
        captured["input"] = list(input)
        return _FakeResponse(
            [{"embedding": [float(i)] * 4} for i, _ in enumerate(input)]
        )

    litellm = pytest.importorskip("litellm")
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

    litellm = pytest.importorskip("litellm")
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

    async def short_response(*_a: Any, **_kw: Any) -> Any:  # NOSONAR(S7503) — stub matches litellm.aembedding async signature
        return _FakeResponse()

    litellm = pytest.importorskip("litellm")
    monkeypatch.setattr(litellm, "aembedding", short_response)
    result = await embedding_client.embed_batch(["a", "b", "c"])
    assert result == [[1.0, 2.0], None, None]


async def test_embed_query_caches_repeated_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    call_count = {"n": 0}

    async def fake_embed_batch(  # NOSONAR(S7503) — stub matches embed_batch async signature
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


# ---------------------------------------------------------------------------
# DEV-1557 — per-text truncation + per-input retry on BadRequestError
# ---------------------------------------------------------------------------


def _clear_cap_caches() -> None:
    """Clear the lru caches DEV-1557 added so per-test monkeypatches stick."""
    for name in ("_resolve_model_cap", "_resolve_encoder"):
        fn = getattr(embedding_client, name, None)
        if fn is not None and hasattr(fn, "cache_clear"):
            fn.cache_clear()


@pytest.fixture(autouse=True)
def _clear_dev1557_caches() -> None:
    _clear_cap_caches()


def test_truncate_text_for_model_short_input_is_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inputs already under the cap return the SAME string object (no
    decode round-trip). Codex finding #9: prove via ``is``."""
    litellm = pytest.importorskip("litellm")
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 8191)
    text = "hi there"
    out = embedding_client.truncate_text_for_model(
        text, model="openai/text-embedding-3-small",
    )
    assert out is text


def test_truncate_text_for_model_truncates_over_cap_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An input that exceeds (cap - 256) tokens is sliced to fit.
    Codex finding #2: assert head-keep — the result must equal
    decode(encode(long_text)[:budget]) so an implementation that
    drops from the wrong side, returns ``""``, or hashes the input
    cannot pass."""
    pytest.importorskip("litellm")
    tiktoken = pytest.importorskip("tiktoken")
    import litellm  # noqa: F401 — ensures the import-skip above gates
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 300)
    long_text = "word " * 2000  # ~2000 tokens, well over the 300-cap budget
    out = embedding_client.truncate_text_for_model(
        long_text, model="openai/text-embedding-3-small",
    )
    enc = tiktoken.get_encoding("cl100k_base")
    expected = enc.decode(enc.encode(long_text)[: 300 - 256])
    assert out == expected
    assert len(enc.encode(out)) <= 300 - 256


def test_truncate_text_for_model_unknown_cap_falls_back_to_8192(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When get_max_tokens returns None for every form of the model
    name, the function uses an 8192-token cap fallback."""
    pytest.importorskip("litellm")
    tiktoken = pytest.importorskip("tiktoken")
    import litellm
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: None)
    # 9000 cl100k tokens > 8192 - 256 budget
    long_text = "word " * 9000
    out = embedding_client.truncate_text_for_model(
        long_text, model="some/unknown-model",
    )
    enc = tiktoken.get_encoding("cl100k_base")
    assert len(enc.encode(out)) <= 8192 - 256


def test_truncate_text_for_model_get_max_tokens_raises_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A raising get_max_tokens is treated the same as None — fall back
    to the 8192 cap. Codex finding #3: a transient failure must NOT be
    cached as 8192 forever (see the no-cache-poisoning test below)."""
    pytest.importorskip("litellm")
    tiktoken = pytest.importorskip("tiktoken")
    import litellm

    def boom(_m: str) -> int:
        raise RuntimeError("model map fetch failed")

    monkeypatch.setattr(litellm.utils, "get_max_tokens", boom)
    out = embedding_client.truncate_text_for_model(
        "word " * 9000, model="some/unknown",
    )
    enc = tiktoken.get_encoding("cl100k_base")
    assert len(enc.encode(out)) <= 8192 - 256


def test_truncate_text_for_model_unknown_encoder_falls_back_to_cl100k(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When tiktoken.encoding_for_model raises KeyError, we fall back to
    cl100k_base. Truncation still respects the budget."""
    pytest.importorskip("litellm")
    tiktoken = pytest.importorskip("tiktoken")
    import litellm

    def raise_key(_name: str) -> Any:
        raise KeyError("no such model")

    monkeypatch.setattr(tiktoken, "encoding_for_model", raise_key)
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 500)
    long_text = "word " * 2000
    out = embedding_client.truncate_text_for_model(
        long_text, model="something/exotic",
    )
    enc = tiktoken.get_encoding("cl100k_base")
    assert len(enc.encode(out)) <= 500 - 256


def test_truncate_text_for_model_strips_provider_prefix_for_encoder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """tiktoken.encoding_for_model receives the BARE model name
    (provider prefix stripped). cl100k_base shortcut works if the
    encoder lookup is correct."""
    tiktoken = pytest.importorskip("tiktoken")
    pytest.importorskip("litellm")
    import litellm
    seen: dict = {}
    real_encoding_for_model = tiktoken.encoding_for_model

    def spy(name: str) -> Any:
        seen["name"] = name
        return real_encoding_for_model(name)

    monkeypatch.setattr(tiktoken, "encoding_for_model", spy)
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 8191)
    embedding_client.truncate_text_for_model(
        "x", model="openai/text-embedding-3-small",
    )
    assert seen["name"] == "text-embedding-3-small"


def test_truncate_text_for_model_get_max_tokens_prefixed_then_bare(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding #2: if get_max_tokens returns None for the
    provider-prefixed form, retry with the bare model name before
    falling back to 8192."""
    pytest.importorskip("litellm")
    import litellm
    calls: List[str] = []

    def fake_get_max(model: str) -> Optional[int]:
        calls.append(model)
        if model == "openai/text-embedding-3-small":
            return None
        if model == "text-embedding-3-small":
            return 8191
        return None

    monkeypatch.setattr(litellm.utils, "get_max_tokens", fake_get_max)
    # Short text → identity, but lookup must be called twice.
    embedding_client.truncate_text_for_model(
        "hi", model="openai/text-embedding-3-small",
    )
    assert calls == [
        "openai/text-embedding-3-small",
        "text-embedding-3-small",
    ]


def test_truncate_text_for_model_warns_on_truncation(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The warning carries enough context for an operator to correlate
    it back to the source memory/entity — model, original token count,
    post-truncation token count, and a sha256 prefix of the input —
    without leaking the embedded content itself."""
    pytest.importorskip("litellm")
    tiktoken = pytest.importorskip("tiktoken")
    import hashlib
    import litellm
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 300)
    long_text = ("PREVIEWABLE_TOKEN " * 2000).strip()
    enc = tiktoken.get_encoding("cl100k_base")
    original_token_count = len(enc.encode(long_text))
    expected_hash_prefix = hashlib.sha256(long_text.encode("utf-8")).hexdigest()[:16]
    caplog.set_level("WARNING", logger="slayer.embeddings.client")
    embedding_client.truncate_text_for_model(
        long_text, model="openai/text-embedding-3-small",
    )
    truncation_records = [
        r for r in caplog.records if "truncat" in r.message.lower()
    ]
    assert truncation_records, (
        "expected a truncation WARNING; got: " + repr(caplog.records)
    )
    msg = truncation_records[0].getMessage()
    assert "openai/text-embedding-3-small" in msg
    assert str(original_token_count) in msg, (
        f"original token count {original_token_count} missing: {msg}"
    )
    assert str(300 - 256) in msg, (
        f"post-truncation token count {300 - 256} missing: {msg}"
    )
    assert expected_hash_prefix in msg, (
        f"sha256 prefix {expected_hash_prefix} missing: {msg}"
    )
    # The warning must NOT include any of the original text content —
    # privacy regression guard.
    assert "PREVIEWABLE_TOKEN" not in msg, (
        f"warning message leaked text content: {msg}"
    )


def test_truncate_text_for_model_no_warn_on_identity_return(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    pytest.importorskip("litellm")
    import litellm
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 8191)
    caplog.set_level("WARNING", logger="slayer.embeddings.client")
    embedding_client.truncate_text_for_model(
        "hi", model="openai/text-embedding-3-small",
    )
    assert not [r for r in caplog.records if "truncat" in r.message.lower()]


def test_truncate_text_for_model_cap_below_margin_returns_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding #1: budget must clamp at zero — never slice with a
    negative index, which would silently keep most of the string."""
    pytest.importorskip("litellm")
    import litellm
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 10)
    out = embedding_client.truncate_text_for_model(
        "word " * 1000, model="openai/text-embedding-3-small",
    )
    assert out == ""


def test_truncate_text_for_model_tiktoken_import_failure_is_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round-2 finding #1: when tiktoken can't be imported (or
    the encoder helper raises), truncation degrades to an identity
    passthrough — over-cap text still flows through unchanged and the
    per-input retry in embed_batch is what saves the batch."""
    pytest.importorskip("litellm")
    import litellm
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 300)

    def boom(_name: str) -> Any:
        raise ImportError("tiktoken missing")

    # Patch the helper that resolves the encoder so we don't have to
    # actually uninstall tiktoken. Implementation must catch this and
    # degrade gracefully.
    monkeypatch.setattr(embedding_client, "_resolve_encoder", boom)
    long_text = "word " * 2000
    out = embedding_client.truncate_text_for_model(
        long_text, model="openai/text-embedding-3-small",
    )
    assert out == long_text  # identity passthrough on encoder failure


def test_truncate_text_for_model_handles_tiktoken_special_token_literal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex PR-#177 finding: tiktoken's default ``encode`` raises on
    strings containing reserved special tokens like ``<|endoftext|>``.
    A user-controlled memory description could trip it, propagating
    the exception out of ``embed_batch`` and regressing the all-None
    contract. Implementation must pass ``disallowed_special=()`` so
    these literals are tokenised as regular text and don't poison the
    batch."""
    pytest.importorskip("litellm")
    import litellm
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 300)
    # Long enough that truncation actually fires; contains the
    # offending literal that default tiktoken would reject.
    payload = ("hello <|endoftext|> world " * 500)
    # Must not raise. Identity or truncated output both acceptable —
    # the contract is "no exception".
    out = embedding_client.truncate_text_for_model(
        payload, model="openai/text-embedding-3-small",
    )
    assert isinstance(out, str)


async def test_embed_batch_survives_special_token_literal_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end companion to the truncation test above. A memory
    text containing ``<|endoftext|>`` must not poison the whole
    batch — pre-DEV-1557 the bare ``except Exception`` saved us; with
    truncation now in front of the call, we have to make sure the
    pre-pass doesn't reintroduce a hard crash."""
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    litellm = pytest.importorskip("litellm")
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 300)

    class _Resp:
        def __init__(self, n: int) -> None:
            self.data = [{"embedding": [float(i)] * 3} for i in range(n)]

    async def fake_aembedding(*, model: str, input: List[str]) -> Any:  # NOSONAR(S7503) — stub matches litellm.aembedding async signature
        return _Resp(len(input))

    monkeypatch.setattr(litellm, "aembedding", fake_aembedding)
    result = await embedding_client.embed_batch(
        ["normal", "hello <|endoftext|> world " * 500],
        model="openai/text-embedding-3-small",
    )
    assert all(v is not None for v in result), result


def test_truncate_text_for_model_get_max_tokens_raises_then_bare_used(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round-2 finding #5: a raising prefixed lookup must still
    trigger the bare-name retry. If the bare lookup succeeds with a
    small cap, the small cap takes effect — not the 8192 fallback."""
    pytest.importorskip("litellm")
    tiktoken = pytest.importorskip("tiktoken")
    import litellm
    calls: List[str] = []

    def fake_get_max(model: str) -> Optional[int]:
        calls.append(model)
        if model == "openai/text-embedding-3-small":
            raise RuntimeError("transient")
        if model == "text-embedding-3-small":
            return 400
        return None

    monkeypatch.setattr(litellm.utils, "get_max_tokens", fake_get_max)
    long_text = "word " * 2000
    out = embedding_client.truncate_text_for_model(
        long_text, model="openai/text-embedding-3-small",
    )
    assert calls == [
        "openai/text-embedding-3-small",
        "text-embedding-3-small",
    ]
    enc = tiktoken.get_encoding("cl100k_base")
    assert len(enc.encode(out)) <= 400 - 256


def test_truncate_text_for_model_non_positive_cap_falls_back_to_8192(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex round-2 finding #7: a zero or negative cap from
    get_max_tokens is bogus — fall back to 8192 (and don't cache the
    bogus value)."""
    pytest.importorskip("litellm")
    tiktoken = pytest.importorskip("tiktoken")
    import litellm
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 0)
    long_text = "word " * 9000
    out = embedding_client.truncate_text_for_model(
        long_text, model="openai/text-embedding-3-small",
    )
    enc = tiktoken.get_encoding("cl100k_base")
    # 8192 fallback budget after 256 margin = 7936.
    assert len(enc.encode(out)) <= 8192 - 256


def test_truncate_text_for_model_failure_not_cached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding #3 (round 1) + #8 (round 2): a transient failure
    of get_max_tokens must not poison the lru cache with the 8192
    fallback. A subsequent successful lookup with a SMALLER cap must
    take effect — we prove this by sending a long string the 2nd call
    and asserting it was truncated to that smaller cap, not 8192."""
    pytest.importorskip("litellm")
    tiktoken = pytest.importorskip("tiktoken")
    import litellm
    call = {"n": 0}

    def fake_get_max(_m: str) -> Optional[int]:
        call["n"] += 1
        if call["n"] == 1:
            raise RuntimeError("transient")
        return 400

    monkeypatch.setattr(litellm.utils, "get_max_tokens", fake_get_max)
    # First call observes the failure → falls back to 8192 cap. Use a
    # short input so we don't waste tokens; this call just exists to
    # poison a misimplemented cache.
    embedding_client.truncate_text_for_model(
        "hi", model="openai/text-embedding-3-small",
    )
    # Second call must hit get_max_tokens again. We send a long input
    # so the 400-cap minus 256-margin = 144-token budget actually fires.
    long_text = "word " * 2000
    out = embedding_client.truncate_text_for_model(
        long_text, model="openai/text-embedding-3-small",
    )
    assert call["n"] >= 2, (
        "get_max_tokens was called only once — the transient failure "
        "was incorrectly cached"
    )
    enc = tiktoken.get_encoding("cl100k_base")
    assert len(enc.encode(out)) <= 400 - 256, (
        "second-call output exceeded the 400-cap budget — the "
        "transient failure was cached as 8192 and the real 400-cap "
        "did not take effect"
    )


# ---------------------------------------------------------------------------
# embed_batch — truncation + per-input retry
# ---------------------------------------------------------------------------


def _make_bad_request_error() -> Exception:
    """Build a real litellm.BadRequestError for tests. Catches the same
    class path the implementation looks up at runtime."""
    import litellm
    return litellm.BadRequestError(
        message="Invalid 'input[1]': maximum input length is 8192 tokens.",
        model="text-embedding-3-small",
        llm_provider="openai",
    )


class _EmbeddingResp:
    """Tiny response shape that mirrors what ``litellm.aembedding``
    returns — ``.data`` is a list of ``{"embedding": [...]}`` dicts.
    Shared by the per-input retry tests."""

    def __init__(self, n: int) -> None:
        self.data = [{"embedding": [float(i)] * 3} for i in range(n)]


def _install_per_input_aembedding_stub(
    monkeypatch: pytest.MonkeyPatch,
    *,
    on_single: Any,
) -> Tuple[List[List[str]], List[str]]:
    """Wire up a fake ``litellm.aembedding`` for per-input-retry tests.

    The fake records every (input, model) the implementation calls it
    with into the returned lists, and dispatches to ``on_single`` for
    per-input retry calls. Multi-element batch calls always raise the
    shared ``BadRequestError`` sentinel so the retry path kicks in.
    ``on_single(text)`` returns either a vector list (success) or
    raises an exception that the implementation will route per its
    retry policy.
    """
    litellm = pytest.importorskip("litellm")
    bad = _make_bad_request_error()
    seen_inputs: List[List[str]] = []
    seen_models: List[str] = []

    async def fake_aembedding(*, model: str, input: List[str]) -> Any:  # NOSONAR(S7503) — stub matches litellm.aembedding async signature
        seen_inputs.append(list(input))
        seen_models.append(model)
        if len(input) > 1:
            raise bad
        on_single(input[0])
        return _EmbeddingResp(1)

    monkeypatch.setattr(litellm, "aembedding", fake_aembedding)
    return seen_inputs, seen_models


async def test_embed_batch_truncates_over_cap_inputs_happy_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One over-cap input + several short inputs → all slots succeed,
    and litellm.aembedding received the TRUNCATED form for the long
    text (not the original)."""
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    litellm = pytest.importorskip("litellm")
    tiktoken = pytest.importorskip("tiktoken")
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 300)

    long_text = "word " * 2000  # over the 44-token budget
    captured_inputs: List[List[str]] = []

    class _Resp:
        def __init__(self, n: int) -> None:
            self.data = [{"embedding": [float(i)] * 3} for i in range(n)]

    async def fake_aembedding(*, model: str, input: List[str]) -> Any:  # NOSONAR(S7503) — stub matches litellm.aembedding async signature
        captured_inputs.append(list(input))
        return _Resp(len(input))

    monkeypatch.setattr(litellm, "aembedding", fake_aembedding)
    result = await embedding_client.embed_batch(
        ["short a", long_text, "short b"],
        model="openai/text-embedding-3-small",
    )
    assert all(v is not None for v in result), result
    assert len(captured_inputs) == 1
    sent = captured_inputs[0]
    assert sent[0] == "short a"
    assert sent[2] == "short b"
    # Codex round-2 finding #2: assert HEAD-keep, not just length.
    enc = tiktoken.get_encoding("cl100k_base")
    expected = enc.decode(enc.encode(long_text)[: 300 - 256])
    assert sent[1] == expected
    assert sent[1] != long_text


async def test_embed_batch_per_input_retry_on_bad_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Batch raises BadRequestError → per-input retry. One text raises
    again on retry → that slot None, the rest succeed. Codex round-2
    finding #10: also assert every aembedding call (batch + retries)
    uses the SAME resolved model string."""
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    litellm = pytest.importorskip("litellm")
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 8191)

    bad = _make_bad_request_error()

    def on_single(text: str) -> None:
        if text == "BAD":
            raise bad

    seen_inputs, seen_models = _install_per_input_aembedding_stub(
        monkeypatch, on_single=on_single,
    )
    result = await embedding_client.embed_batch(
        ["good 1", "BAD", "good 2"],
        model="openai/text-embedding-3-small",
    )
    assert result[0] is not None
    assert result[1] is None
    assert result[2] is not None
    # Codex finding #9: verify every input was actually attempted.
    per_input_calls = [c for c in seen_inputs if len(c) == 1]
    flattened = [c[0] for c in per_input_calls]
    assert flattened == ["good 1", "BAD", "good 2"]
    assert set(seen_models) == {"openai/text-embedding-3-small"}


async def test_embed_batch_per_input_retry_breaks_on_non_bad_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding #4 (round 1): during per-input retry, a
    non-BadRequestError on slot k → break out; slot k and every
    subsequent slot are None, no additional aembedding calls are made
    past the failure. Codex round-2 finding #10: also assert every
    aembedding call uses the SAME resolved model string."""
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    litellm = pytest.importorskip("litellm")
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 8191)

    def on_single(text: str) -> None:
        if text == "RATE_LIMIT":
            raise RuntimeError("rate limit, please retry")

    seen_inputs, seen_models = _install_per_input_aembedding_stub(
        monkeypatch, on_single=on_single,
    )
    result = await embedding_client.embed_batch(
        ["good 1", "RATE_LIMIT", "good 2", "good 3"],
        model="openai/text-embedding-3-small",
    )
    assert result[0] is not None
    assert result[1] is None
    assert result[2] is None  # NOSONAR(S125) — explanatory comment: not attempted; broken-out
    assert result[3] is None  # NOSONAR(S125) — explanatory comment: not attempted; broken-out
    per_input_inputs = [c[0] for c in seen_inputs if len(c) == 1]
    # Stop after the rate-limit error; "good 2" and "good 3" never tried.
    assert per_input_inputs == ["good 1", "RATE_LIMIT"]
    assert set(seen_models) == {"openai/text-embedding-3-small"}


async def test_embed_batch_batch_non_bad_request_returns_all_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Batch raises a non-BadRequestError (rate limit) → all None,
    per-input retry NOT invoked. Codex round-2 finding #4: also assert
    pre-truncation HAS happened — feed an over-cap input and inspect
    the batch's ``input`` arg to confirm truncation ran before the
    failure (regression-proof against an implementation that skips
    truncation when it 'knows' the batch will fail)."""
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    litellm = pytest.importorskip("litellm")
    tiktoken = pytest.importorskip("tiktoken")
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 300)

    call_count = {"n": 0}
    captured_inputs: List[List[str]] = []
    captured_models: List[str] = []

    async def fake_aembedding(*, model: str, input: List[str]) -> Any:  # NOSONAR(S7503) — stub matches litellm.aembedding async signature
        call_count["n"] += 1
        captured_inputs.append(list(input))
        captured_models.append(model)
        raise RuntimeError("rate limit")

    monkeypatch.setattr(litellm, "aembedding", fake_aembedding)
    long_text = "word " * 2000
    result = await embedding_client.embed_batch(
        ["short a", long_text, "short b"],
        model="openai/text-embedding-3-small",
    )
    assert result == [None, None, None]
    assert call_count["n"] == 1  # batch tried, NO per-input retries
    assert captured_models == ["openai/text-embedding-3-small"]
    # Truncation must have run before the failing call.
    sent_long = captured_inputs[0][1]
    enc = tiktoken.get_encoding("cl100k_base")
    assert len(enc.encode(sent_long)) <= 300 - 256
    assert sent_long != long_text


async def test_embed_batch_all_bad_requests_returns_all_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Codex finding #9: when every per-input retry also raises
    BadRequestError, all slots are None AND every input was attempted
    (so a regression to early-exit can't go unnoticed)."""
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    litellm = pytest.importorskip("litellm")
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 8191)

    bad = _make_bad_request_error()

    def on_single(_text: str) -> None:
        raise bad

    seen_inputs, _ = _install_per_input_aembedding_stub(
        monkeypatch, on_single=on_single,
    )
    result = await embedding_client.embed_batch(
        ["a", "b", "c"], model="openai/text-embedding-3-small",
    )
    assert result == [None, None, None]
    per_input_inputs = [c[0] for c in seen_inputs if len(c) == 1]
    assert per_input_inputs == ["a", "b", "c"]


async def test_embed_batch_no_truncation_when_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """is_available False short-circuits before truncation runs and
    before any aembedding call is made."""
    monkeypatch.setattr(embedding_client, "is_available", lambda: False)
    litellm = pytest.importorskip("litellm")
    truncate_calls = {"n": 0}
    aembedding_calls = {"n": 0}

    real_truncate = embedding_client.truncate_text_for_model

    def spy_truncate(text: str, *, model: Optional[str] = None) -> str:
        truncate_calls["n"] += 1
        return real_truncate(text, model=model)

    async def fake_aembedding(*, model: str, input: List[str]) -> Any:  # NOSONAR(S7503) — stub matches litellm.aembedding async signature
        aembedding_calls["n"] += 1
        return None

    monkeypatch.setattr(
        embedding_client, "truncate_text_for_model", spy_truncate,
    )
    monkeypatch.setattr(litellm, "aembedding", fake_aembedding)
    result = await embedding_client.embed_batch(
        ["a", "b"], model="openai/text-embedding-3-small",
    )
    assert result == [None, None]
    assert truncate_calls["n"] == 0
    assert aembedding_calls["n"] == 0


# ---------------------------------------------------------------------------
# embed_query regression (Codex finding #6)
# ---------------------------------------------------------------------------


async def test_embed_query_truncates_long_input_and_returns_vector(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """embed_query wraps embed_batch([single]). A long query string
    must truncate cleanly and surface a vector."""
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    litellm = pytest.importorskip("litellm")
    tiktoken = pytest.importorskip("tiktoken")
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 300)

    captured: dict = {}

    class _Resp:
        def __init__(self) -> None:
            self.data = [{"embedding": [1.0, 2.0, 3.0]}]

    async def fake_aembedding(*, model: str, input: List[str]) -> Any:  # NOSONAR(S7503) — stub matches litellm.aembedding async signature
        captured["input"] = list(input)
        return _Resp()

    monkeypatch.setattr(litellm, "aembedding", fake_aembedding)
    long_text = "word " * 2000
    vec = await embedding_client.embed_query(
        long_text, model="openai/text-embedding-3-small",
    )
    assert vec == [1.0, 2.0, 3.0]
    # Codex round-2 finding #2: assert HEAD-keep, not just length.
    enc = tiktoken.get_encoding("cl100k_base")
    expected = enc.decode(enc.encode(long_text)[: 300 - 256])
    assert captured["input"][0] == expected


async def test_embed_query_returns_none_when_per_input_retry_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """embed_query → embed_batch([text]) where the batch raises
    BadRequestError AND the per-input retry also raises → returns
    None. Codex round-2 finding #3: assert TWO aembedding calls
    (initial batch + per-input retry) — otherwise this test passes
    against the existing broken implementation that catches everything
    on the first call."""
    monkeypatch.setattr(embedding_client, "is_available", lambda: True)
    litellm = pytest.importorskip("litellm")
    monkeypatch.setattr(litellm.utils, "get_max_tokens", lambda _m: 8191)

    bad = _make_bad_request_error()
    call_count = {"n": 0}

    async def fake_aembedding(*, model: str, input: List[str]) -> Any:  # NOSONAR(S7503) — stub matches litellm.aembedding async signature
        call_count["n"] += 1
        raise bad

    monkeypatch.setattr(litellm, "aembedding", fake_aembedding)
    vec = await embedding_client.embed_query(
        "hi", model="openai/text-embedding-3-small",
    )
    assert vec is None
    assert call_count["n"] == 2, (
        "expected 1 batch call + 1 per-input retry = 2; got "
        f"{call_count['n']}"
    )
