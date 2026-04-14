"""Async/sync bridge utilities.

SLayer is async-first internally. These helpers let sync callers
(notebooks, CLI, scripts) use the async API without managing event loops.
"""

import asyncio
import concurrent.futures
from typing import Any, Coroutine, TypeVar

T = TypeVar("T")


def run_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Run an async coroutine synchronously.

    Handles three scenarios:
    1. No event loop running → use asyncio.run()
    2. Inside Jupyter/existing loop → run in a thread to avoid nesting
    3. General case → create a new loop
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No loop running — simplest case
        return asyncio.run(coro)

    # Loop already running (Jupyter, async framework, etc.)
    # Run in a thread to avoid "cannot run nested event loop" error
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()
