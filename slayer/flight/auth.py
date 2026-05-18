"""Bearer-token auth for the Flight SQL facade (DEV-1390 §4.3).

Two surfaces:

* :class:`BearerTokenMiddlewareFactory` — pyarrow Flight server
  middleware that validates the ``authorization`` gRPC metadata
  header on every RPC.
* :func:`validate_bind_address` — startup-time check that refuses
  to bind a non-loopback address without a configured token.

The middleware honours the dbt-SL JDBC URL convention:
``token=<secret>`` is forwarded as ``Authorization: Bearer <secret>``;
``environmentId=<n>`` is forwarded too and surfaces as the
``environmentid`` (lowercased per gRPC convention) header. We log
``environmentid`` at INFO for traceability and otherwise ignore it.
"""

from __future__ import annotations

import ipaddress
import logging
from typing import Optional

import pyarrow.flight as fl

logger = logging.getLogger(__name__)


_LOOPBACK_NETWORKS = (
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("::1/128"),
)


def _is_loopback(host: str) -> bool:
    """Return True iff ``host`` is a loopback literal (127.0.0.0/8 or ::1).

    Hostnames like ``localhost`` resolve to loopback on every reasonable
    system but we don't perform DNS at startup; instead we accept
    ``localhost`` as a sentinel.
    """
    if host == "localhost":
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    for net in _LOOPBACK_NETWORKS:
        if ip in net:
            return True
    return False


def validate_bind_address(*, host: str, token: Optional[str]) -> None:
    """Raise ``ValueError`` if the server is about to bind a non-loopback
    address without a configured token (§4.3 / §7.1).
    """
    if token:
        return
    if _is_loopback(host):
        return
    raise ValueError(
        f"--token or $SLAYER_FLIGHT_TOKEN is required when binding to a "
        f"non-loopback address (host={host!r})"
    )


def validate_tls_pair(*, cert: Optional[str], key: Optional[str]) -> None:
    """TLS cert/key must be supplied together or not at all (§4.4)."""
    if (cert is None) != (key is None):
        raise ValueError(
            "Both --tls-cert and --tls-key are required to enable TLS; "
            "providing only one is an error."
        )


class _BearerTokenMiddleware(fl.ServerMiddleware):
    """No-op once-per-call middleware; auth check happened in the factory."""

    def __init__(self, *, environment_id: Optional[str] = None) -> None:
        self._environment_id = environment_id

    def call_completed(self, exception: Optional[BaseException]) -> None:
        if exception is not None and self._environment_id is not None:
            logger.debug(
                "Flight SQL call (environmentId=%s) failed: %r",
                self._environment_id, exception,
            )

    def sending_headers(self) -> dict:
        return {}


class BearerTokenMiddlewareFactory(fl.ServerMiddlewareFactory):
    """Validate ``Authorization: Bearer <token>`` on every incoming RPC.

    Construct with the configured token (or ``None`` for no-auth mode).
    When no token is configured, every incoming RPC is accepted
    unauthenticated. The defence against non-loopback exposure is the
    startup-time :func:`validate_bind_address` check — pyarrow's
    ``ServerMiddlewareFactory.start_call(info, headers)`` does not
    expose the remote peer address (``CallInfo`` only carries
    ``method``), so middleware-level peer enforcement is not feasible
    without a custom ``ServerCallContext`` wrapper.
    """

    def __init__(self, *, token: Optional[str]) -> None:
        self._expected = token

    def start_call(
        self, info: fl.CallInfo, headers: dict
    ) -> Optional[fl.ServerMiddleware]:
        # Extract and lowercase header keys (gRPC standardises to lowercase
        # but client implementations differ).
        normalised = {
            (k.lower() if isinstance(k, str) else k.decode().lower()):
            (v[0] if isinstance(v, list) and v else v)
            for k, v in (headers or {}).items()
        }
        env_id_raw = normalised.get("environmentid")
        environment_id: Optional[str] = None
        if isinstance(env_id_raw, (bytes, bytearray)):
            environment_id = env_id_raw.decode("utf-8", errors="replace")
        elif isinstance(env_id_raw, str):
            environment_id = env_id_raw
        if environment_id:
            logger.info("Flight SQL request environmentId=%s", environment_id)

        auth_raw = normalised.get("authorization")
        provided: Optional[str] = None
        if isinstance(auth_raw, (bytes, bytearray)):
            auth_raw = auth_raw.decode("utf-8", errors="replace")
        if isinstance(auth_raw, str) and auth_raw.lower().startswith("bearer "):
            provided = auth_raw[len("Bearer "):].strip()

        if self._expected is None:
            # No-auth mode. Server startup already rejected non-loopback
            # binds via validate_bind_address; pyarrow CallInfo does not
            # expose the peer address at this layer, so we cannot recheck.
            return _BearerTokenMiddleware(environment_id=environment_id)

        if provided is None:
            raise fl.FlightUnauthenticatedError("Missing bearer token")
        if provided != self._expected:
            raise fl.FlightUnauthenticatedError("invalid bearer token")

        return _BearerTokenMiddleware(environment_id=environment_id)
