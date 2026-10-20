"""HS256 tokens for the SLayer Gateway's ``auth.hs256_secret`` (dev and service accounts)."""

import base64
import hashlib
import hmac
import json
import time
from collections.abc import Mapping
from typing import Any


def _b64url_json(value: Mapping[str, Any]) -> str:
    raw = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def sign_gateway_token(
    claims: Mapping[str, Any],
    secret: str,
    *,
    ttl_seconds: int = 3600,
    now: int | None = None,
) -> str:
    """Sign ``claims`` (``sub``, ``tenant``, ``roles``, …) as an HS256 JWT; byte-identical to the TS ``signGatewayToken``."""
    key = secret.encode()
    if len(key) < 32:
        raise ValueError("Use a secret of at least 32 bytes")
    issued = int(time.time()) if now is None else now
    unsigned = f"{_b64url_json({'alg': 'HS256', 'typ': 'JWT'})}.{_b64url_json({'iat': issued, 'exp': issued + ttl_seconds, **claims})}"
    signature = hmac.new(key, unsigned.encode(), hashlib.sha256).digest()
    return f"{unsigned}.{base64.urlsafe_b64encode(signature).rstrip(b'=').decode()}"
