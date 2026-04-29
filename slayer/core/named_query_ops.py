"""Operations on :class:`NamedQuery` that span the storage and engine layers.

The :func:`save_named_query` helper is the canonical save flow used by every
entry point (MCP tool, CLI subcommand, HTTP route). It guarantees that a
NamedQuery is integrity-checked (dry-run executes without hitting the database)
*before* it is persisted, so a failing dry-run never produces a partial write.
"""

from slayer.core.models import NamedQuery
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.storage.base import StorageBackend


async def save_named_query(
    query: NamedQuery,
    *,
    storage: StorageBackend,
    engine: SlayerQueryEngine,
) -> None:
    """Validate *query* via dry-run execution, then persist it.

    Order matters: dry-run failures must abort the save before any write
    happens. Storage handles the bidirectional name-collision check between
    models and queries.
    """
    await engine.validate_named_query(query)
    await storage.save_query(query)
