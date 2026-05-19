"""Tests for slayer.flight.auth — bearer-token gRPC middleware + startup checks."""

from __future__ import annotations

import pytest

import pyarrow.flight as fl

from slayer.flight.auth import (
    BearerTokenMiddlewareFactory,
    _is_loopback,
    validate_bind_address,
    validate_tls_pair,
)


def _start_call(factory: BearerTokenMiddlewareFactory, headers: dict):
    return factory.start_call(info=None, headers=headers)


# --- _is_loopback ------------------------------------------------------------


@pytest.mark.parametrize("host", ["127.0.0.1", "127.5.5.5", "::1", "localhost"])
def test_loopback_hosts_recognised(host: str) -> None:
    assert _is_loopback(host) is True


@pytest.mark.parametrize("host", ["0.0.0.0", "10.0.0.5", "192.168.1.1", "example.com"])  # NOSONAR(S1313) — RFC1918 test fixtures, never live addresses
def test_non_loopback_hosts_rejected(host: str) -> None:
    assert _is_loopback(host) is False


# --- validate_bind_address ---------------------------------------------------


def test_loopback_no_token_ok() -> None:
    validate_bind_address(host="127.0.0.1", token=None)
    validate_bind_address(host="::1", token=None)
    validate_bind_address(host="localhost", token=None)


def test_non_loopback_no_token_errors() -> None:
    with pytest.raises(ValueError) as exc_info:
        validate_bind_address(host="0.0.0.0", token=None)
    assert "$SLAYER_FLIGHT_TOKEN" in str(exc_info.value)


def test_non_loopback_with_token_ok() -> None:
    validate_bind_address(host="0.0.0.0", token="secret")


# --- validate_tls_pair -------------------------------------------------------


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


# --- middleware: token configured --------------------------------------------


def test_middleware_accepts_correct_bearer_token() -> None:
    factory = BearerTokenMiddlewareFactory(token="s3cret")
    mw = _start_call(factory, {"authorization": "Bearer s3cret"})
    assert mw is not None


def test_middleware_accepts_correct_bearer_token_bytes_value() -> None:
    """Some gRPC client implementations send header values as bytes."""
    factory = BearerTokenMiddlewareFactory(token="s3cret")
    mw = _start_call(factory, {"authorization": b"Bearer s3cret"})
    assert mw is not None


def test_middleware_accepts_case_insensitive_bearer_prefix() -> None:
    factory = BearerTokenMiddlewareFactory(token="s3cret")
    mw = _start_call(factory, {"authorization": "bearer s3cret"})
    assert mw is not None


def test_middleware_rejects_wrong_token() -> None:
    factory = BearerTokenMiddlewareFactory(token="s3cret")
    with pytest.raises(fl.FlightUnauthenticatedError) as exc_info:
        _start_call(factory, {"authorization": "Bearer different"})
    assert "invalid bearer token" in str(exc_info.value)


def test_middleware_rejects_missing_token() -> None:
    factory = BearerTokenMiddlewareFactory(token="s3cret")
    with pytest.raises(fl.FlightUnauthenticatedError):
        _start_call(factory, {})


def test_middleware_rejects_malformed_authorization() -> None:
    """A non-bearer Authorization header is treated as missing."""
    factory = BearerTokenMiddlewareFactory(token="s3cret")
    with pytest.raises(fl.FlightUnauthenticatedError):
        _start_call(factory, {"authorization": "Basic dXNlcjpwYXNz"})


# --- middleware: no-auth (loopback) ------------------------------------------


def test_middleware_unauthenticated_passes_when_no_token_configured() -> None:
    factory = BearerTokenMiddlewareFactory(token=None)
    mw = _start_call(factory, {})
    assert mw is not None


# --- environmentId handling --------------------------------------------------


def test_middleware_logs_environment_id(caplog) -> None:
    """`environmentId` header should be log-only (INFO) and not affect auth."""
    factory = BearerTokenMiddlewareFactory(token="t")
    with caplog.at_level("INFO", logger="slayer.flight.auth"):
        mw = _start_call(factory, {"authorization": "Bearer t", "environmentid": "42"})
    assert mw is not None
    assert any("environmentId=42" in r.message for r in caplog.records)
