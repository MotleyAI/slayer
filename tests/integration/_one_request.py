"""Shared check that a warm statement costs the database one request."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from typing import Any

from slayer.sql.client import SlayerSQLClient

MARKERS = ("one-request-m1", "one-request-m2", "one-request-m3")
PATHS = ("async", "sync")


def run_three(client: SlayerSQLClient, *, path: str, sql: Callable[[str], str]) -> list[list[dict[str, Any]]]:
    """Each marker's statement, in order, on ``path``: one event loop (async) or three ``execute_sync`` calls."""
    statements = [sql(marker) for marker in MARKERS]
    try:
        if path == "async":
            async def _all() -> list[list[dict[str, Any]]]:
                try:
                    return [(await client.execute(s)).rows for s in statements]
                finally:
                    await client.aclose()

            return asyncio.run(_all())
        return [client.execute_sync(s).rows for s in statements]
    finally:
        client.close()


def assert_one_request_each(requests: Sequence[str]) -> None:
    """``requests`` (one session's, in order) hold exactly the second and third markers' after the first's."""
    found = [next((i for i, r in enumerate(requests) if marker in r), None) for marker in MARKERS]
    assert None not in found, f"markers {MARKERS} not all on one session: {list(requests)}"
    first, _, last = found
    assert first is not None and last is not None
    window = list(requests[first + 1:last + 1])
    assert len(window) == 2 and MARKERS[1] in window[0] and MARKERS[2] in window[1], window
