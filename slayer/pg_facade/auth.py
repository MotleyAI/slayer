"""Auth + bind-address rules for the Postgres facade (DEV-1486).

* :func:`validate_bind_address` — startup-time check refusing to bind a
  non-loopback address without a configured token (mirrors the Flight facade).
* :func:`validate_tls_pair` — TLS cert/key must be supplied together.
* :func:`verify_password` — constant-time cleartext-password check used during
  the ``AuthenticationCleartextPassword`` exchange.

Cloned from ``slayer/flight/auth.py`` rather than shared: the pg facade is
pyarrow-free and the Flight module imports ``pyarrow.flight`` for its gRPC
middleware.
"""

from __future__ import annotations

import hmac
import ipaddress
import logging
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

logger = logging.getLogger(__name__)


_LOOPBACK_NETWORKS = (
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("::1/128"),
)


def _is_loopback(host: str) -> bool:
    """Return True iff ``host`` is a loopback literal (127.0.0.0/8 or ::1).

    ``localhost`` is accepted as a sentinel without DNS resolution.
    """
    if host == "localhost":
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    return any(ip in net for net in _LOOPBACK_NETWORKS)


def validate_bind_address(
    *, host: str, token: str | None, authenticated: bool = False
) -> None:
    """Raise ``ValueError`` if binding a non-loopback address without auth.

    ``authenticated`` is set by callers that supply a custom password
    authenticator instead of a static token.
    """
    if token or authenticated:
        return
    if _is_loopback(host):
        return
    raise ValueError(
        f"--token or $SLAYER_PG_TOKEN is required when binding to a "
        f"non-loopback address (host={host!r})"
    )


def validate_tls_pair(*, cert: str | None, key: str | None) -> None:
    """TLS cert/key must be supplied together or not at all."""
    if (cert is None) != (key is None):
        raise ValueError(
            "Both --tls-cert and --tls-key are required to enable TLS; "
            "providing only one is an error."
        )


def verify_password(client_password: str, expected: str | None) -> bool:
    """Constant-time cleartext-password check.

    When no token is configured (``expected is None``) any non-empty password
    is accepted (loopback dev mode); an empty password is always rejected.
    """
    if not client_password:
        return False
    if expected is None:
        return True
    return hmac.compare_digest(client_password, expected)


# ---------------------------------------------------------------------------
# Pluggable authentication
# ---------------------------------------------------------------------------
#
# The facade ships with the static-token check above, but a host application
# (e.g. Motley Storyline) needs to validate the cleartext password against its
# own identity store and scope the connection to a tenant. ``Authenticator``
# is the seam for that: the connection hands over the startup ``user`` /
# ``database`` parameters plus the cleartext password and gets back an
# ``AuthOutcome`` whose opaque ``principal`` it carries for the rest of the
# session (datasource scoping, RLS, logging).


@dataclass
class AuthOutcome:
    """Result of an authentication attempt.

    ``principal`` is opaque to the facade — a host-defined object (tenant id,
    user, allowed-datasource set) attached to the connection on success.
    ``message`` is surfaced to the client only on failure and should stay
    generic (don't leak which factor failed).
    """

    ok: bool
    principal: object | None = None
    message: str = "password authentication failed"


@runtime_checkable
class Authenticator(Protocol):
    """Validates a Postgres-facade login.

    ``requires_password`` controls the wire handshake: when False the facade
    skips the ``AuthenticationCleartextPassword`` exchange and sends
    ``AuthenticationOk`` directly (loopback dev mode), still calling
    ``authenticate`` with ``password=None`` so the hook can veto.
    """

    @property
    def requires_password(self) -> bool: ...

    async def authenticate(
        self, *, username: str | None, password: str | None, database: str | None
    ) -> AuthOutcome: ...


class StaticTokenAuthenticator:
    """Default ``Authenticator``: the legacy single-shared-token behaviour.

    Wraps :func:`verify_password` so existing deployments and tests are
    unchanged. With no token configured it accepts any non-empty password
    (and, with ``requires_password`` False, skips the prompt entirely).
    """

    def __init__(self, token: str | None) -> None:
        self._token = token

    @property
    def requires_password(self) -> bool:
        return self._token is not None

    async def authenticate(
        self, *, username: str | None, password: str | None, database: str | None
    ) -> AuthOutcome:
        # requires_password is False here, so the facade passed password=None;
        # treat the no-token loopback case as an unconditional accept.
        if self._token is None:
            return AuthOutcome(ok=True)
        if password is not None and verify_password(password, self._token):
            return AuthOutcome(ok=True)
        return AuthOutcome(ok=False)
