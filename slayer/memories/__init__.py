"""Agent-memory layer (DEV-1357 v2).

Exposes the unified ``Memory`` row and the typed response models
returned by the three MCP / REST / CLI / client surfaces
(``save_memory``, ``forget_memory``, ``recall_memories``). The
``MemoryService`` orchestrator and entity resolver are intentionally
not re-exported here — they import ``slayer.storage.base``, which
itself imports from this package, so eager re-export would create a
cycle. Import them directly from ``slayer.memories.service`` /
``.resolver`` when needed.
"""

from slayer.memories.models import (
    ForgetMemoryResponse,
    Memory,
    RecallHit,
    RecallResponse,
    SaveMemoryResponse,
)

__all__ = [
    "ForgetMemoryResponse",
    "Memory",
    "RecallHit",
    "RecallResponse",
    "SaveMemoryResponse",
]
