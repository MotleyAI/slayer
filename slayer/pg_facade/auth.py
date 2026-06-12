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
from typing import Optional

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


def validate_bind_address(*, host: str, token: Optional[str]) -> None:
    """Raise ``ValueError`` if binding a non-loopback address without a token."""
    if token:
        return
    if _is_loopback(host):
        return
    raise ValueError(
        f"--token or $SLAYER_PG_TOKEN is required when binding to a "
        f"non-loopback address (host={host!r})"
    )


def validate_tls_pair(*, cert: Optional[str], key: Optional[str]) -> None:
    """TLS cert/key must be supplied together or not at all."""
    if (cert is None) != (key is None):
        raise ValueError(
            "Both --tls-cert and --tls-key are required to enable TLS; "
            "providing only one is an error."
        )


def verify_password(client_password: str, expected: Optional[str]) -> bool:
    """Constant-time cleartext-password check.

    When no token is configured (``expected is None``) any non-empty password
    is accepted (loopback dev mode); an empty password is always rejected.
    """
    if not client_password:
        return False
    if expected is None:
        return True
    return hmac.compare_digest(client_password, expected)
