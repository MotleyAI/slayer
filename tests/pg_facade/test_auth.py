"""Tests for slayer.pg_facade.auth — bind-address / TLS / password checks."""

from __future__ import annotations

import pytest

from slayer.pg_facade.auth import (
    AuthOutcome,
    Authenticator,
    StaticTokenAuthenticator,
    _is_loopback,
    validate_bind_address,
    validate_tls_pair,
    verify_password,
)


@pytest.mark.parametrize("host", ["127.0.0.1", "127.5.5.5", "::1", "localhost"])
def test_loopback_hosts_recognised(host: str) -> None:
    assert _is_loopback(host) is True


@pytest.mark.parametrize("host", ["0.0.0.0", "10.0.0.5", "192.168.1.1", "example.com"])  # NOSONAR(S1313)
def test_non_loopback_hosts_rejected(host: str) -> None:
    assert _is_loopback(host) is False


def test_loopback_no_token_ok() -> None:
    validate_bind_address(host="127.0.0.1", token=None)
    validate_bind_address(host="::1", token=None)
    validate_bind_address(host="localhost", token=None)


def test_non_loopback_no_token_errors() -> None:
    with pytest.raises(ValueError) as exc_info:
        validate_bind_address(host="0.0.0.0", token=None)
    assert "$SLAYER_PG_TOKEN" in str(exc_info.value)


def test_non_loopback_with_token_ok() -> None:
    validate_bind_address(host="0.0.0.0", token="secret")


def test_non_loopback_with_authenticator_ok() -> None:
    # A custom password-prompting authenticator satisfies the bind rule.
    validate_bind_address(host="0.0.0.0", token=None, authenticated=True)


def test_non_loopback_authenticated_false_still_errors() -> None:
    with pytest.raises(ValueError):
        validate_bind_address(host="0.0.0.0", token=None, authenticated=False)


def test_tls_pair_both_none_ok() -> None:
    validate_tls_pair(cert=None, key=None)


def test_tls_pair_both_set_ok() -> None:
    validate_tls_pair(cert="/cert", key="/key")


def test_tls_pair_only_cert_errors() -> None:
    with pytest.raises(ValueError):
        validate_tls_pair(cert="/cert", key=None)


def test_tls_pair_only_key_errors() -> None:
    with pytest.raises(ValueError):
        validate_tls_pair(cert=None, key="/key")


def test_verify_password_none_token_accepts_any_nonempty() -> None:
    assert verify_password("anything", None) is True
    assert verify_password("", None) is False


def test_verify_password_constant_time_match() -> None:
    assert verify_password("s3cret", "s3cret") is True
    assert verify_password("wrong", "s3cret") is False


def test_verify_password_empty_rejected_even_with_token() -> None:
    assert verify_password("", "s3cret") is False


# --- pluggable authenticator -------------------------------------------------


def test_static_authenticator_satisfies_protocol() -> None:
    assert isinstance(StaticTokenAuthenticator("s3cret"), Authenticator)


def test_static_authenticator_no_token_skips_prompt() -> None:
    auth = StaticTokenAuthenticator(None)
    assert auth.requires_password is False


async def test_static_authenticator_no_token_accepts() -> None:
    auth = StaticTokenAuthenticator(None)
    outcome = await auth.authenticate(username="u", password=None, database="d")
    assert outcome.ok is True


def test_static_authenticator_with_token_prompts() -> None:
    assert StaticTokenAuthenticator("s3cret").requires_password is True


async def test_static_authenticator_password_match() -> None:
    auth = StaticTokenAuthenticator("s3cret")
    assert (await auth.authenticate(username="u", password="s3cret", database=None)).ok is True
    assert (await auth.authenticate(username="u", password="nope", database=None)).ok is False


def test_auth_outcome_defaults() -> None:
    outcome = AuthOutcome(ok=False)
    assert outcome.principal is None
    assert outcome.message == "password authentication failed"
