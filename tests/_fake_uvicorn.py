"""A stand-in ``uvicorn`` module whose ``Server.run`` reports the served app instead of serving."""

from __future__ import annotations

import types
from collections.abc import Callable
from typing import Any


def fake_uvicorn(on_run: Callable[[Any, str, int], None] = lambda app, host, port: None) -> types.ModuleType:
    """``uvicorn`` with ``Config``/``Server``; ``Server.run`` calls ``on_run(app, host, port)``."""

    class Config:
        def __init__(self, app: Any, host: str = "127.0.0.1", port: int = 8000, **kwargs: Any) -> None:
            self.app, self.host, self.port = app, host, port

    class Server:
        def __init__(self, config: Config) -> None:
            self.config = config
            self.should_exit = False

        def run(self, sockets: Any = None) -> None:
            on_run(self.config.app, self.config.host, self.config.port)

    module = types.ModuleType("uvicorn")
    module.Config = Config  # type: ignore[attr-defined]
    module.Server = Server  # type: ignore[attr-defined]
    return module
