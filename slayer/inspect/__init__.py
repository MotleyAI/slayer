"""DEV-1588: shared single-entity inspection core.

Exposes :class:`slayer.inspect.service.InspectService` (the shared core
behind the MCP ``inspect`` tool + REST/CLI/SlayerClient surfaces) and the
model-render helpers extracted out of ``slayer/mcp/server.py`` so the
``inspect`` surfaces and the legacy ``inspect_model`` MCP tool share one
implementation.
"""
