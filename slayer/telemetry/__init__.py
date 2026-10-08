"""Anonymous usage telemetry for processes started by the ``slayer`` CLI."""

from slayer.telemetry.recorder import (
    HttpError,
    counting,
    flush,
    is_active,
    observe_datasources,
    observe_executed,
    observe_mcp_client,
    observe_model_count,
    observe_query,
    observe_storage,
    record,
    reset,
    shutdown,
    start,
)

__all__ = [
    "HttpError",
    "counting",
    "flush",
    "is_active",
    "observe_datasources",
    "observe_executed",
    "observe_mcp_client",
    "observe_model_count",
    "observe_query",
    "observe_storage",
    "record",
    "reset",
    "shutdown",
    "start",
]
