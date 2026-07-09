"""DEV-1658: SLayer's conceptual help, seeded as predefined memories.

The old standalone ``help()`` tool/subcommand duplicated the memory system with
a fixed content set. Instead, the topic bodies under ``help_content/*.md`` are
seeded as real memories with fixed ids (``help.intro`` … ``help.workflow``) and
retrieved through the ordinary ``inspect(entity_type="memory")`` / ``search``
surfaces.

``seed_help_memories(storage)`` is idempotent: upsert-always, but it skips the
write (and the embedding fan-out) when the stored ``learning`` + ``description``
already match the shipped content, so a warm store is a cheap no-op. Seeded
memories carry **no entities**, so they never surface in a model's Learnings
section (that section filters by entity overlap).

Content lives in ``help_content/NN_name.md``; the ``NN_`` prefix fixes the
teaching order and is stripped to form the topic key. ``00_intro`` is the entry
point that lists the deep-dive topics.
"""

from __future__ import annotations

from importlib.resources import files

from pydantic import BaseModel

from slayer.storage.base import StorageBackend

_CONTENT_SUBDIR = "help_content"
_ID_PREFIX = "help."

#: Authored one-line previews (<=500 chars) surfaced by search(compact=True)
#: and inspect(compact=True). Keyed by the topic key (``NN_`` prefix stripped).
_DESCRIPTIONS: dict[str, str] = {
    "intro": "What SLayer is, the core entities, the query shape, and the biggest gotchas.",
    "queries": "Anatomy of a SlayerQuery: source_model, measures, dimensions, filters, order, limit.",
    "formulas": "Writing measure formulas: colon aggregations, arithmetic, and saved measures.",
    "aggregations": "Built-in and custom aggregations, colon syntax, *:count, and allowed_aggregations.",
    "transforms": "cumsum, time_shift, change, the rank family, lag/lead, and their wrapping rules.",
    "time": "Time dimensions, granularities, and time-ordered formula resolution.",
    "filters": "WHERE vs HAVING routing, filters on measures/transforms, and {variable} placeholders.",
    "joins": "Reaching joined data via dotted paths and how joins auto-resolve.",
    "models": "What a model is: columns, measures, source modes, and model-level filters.",
    "extending": "Ad hoc columns/measures/joins via ModelExtension and saving queries as models.",
    "workflow": "Recommended tool-chaining order for an agent: inspect -> search -> inspect -> query.",
}


class HelpTopic(BaseModel):
    """One seeded help memory: a fixed id, the migrated topic body, and an
    authored one-line preview."""

    id: str
    learning: str
    description: str


def _strip_numeric_prefix(stem: str) -> str:
    """``"01_queries"`` -> ``"queries"``; leave other stems unchanged."""
    if len(stem) >= 3 and stem[0].isdigit() and stem[1].isdigit() and stem[2] == "_":
        return stem[3:]
    return stem


def _load_topics() -> tuple[HelpTopic, ...]:
    """Load ``help_content/*.md`` once at import, in filesystem (``NN_``) order."""
    content_dir = files(__package__) / _CONTENT_SUBDIR
    topics: list[HelpTopic] = []
    for entry in sorted(content_dir.iterdir(), key=lambda e: e.name):
        if not entry.name.endswith(".md"):
            continue
        key = _strip_numeric_prefix(entry.name[: -len(".md")])
        description = _DESCRIPTIONS.get(key)
        if description is None:
            raise ValueError(
                f"help topic {key!r} has no authored description in "
                f"_DESCRIPTIONS; add one."
            )
        topics.append(HelpTopic(
            id=f"{_ID_PREFIX}{key}",
            learning=entry.read_text(encoding="utf-8"),
            description=description,
        ))
    return tuple(topics)


HELP_TOPICS: tuple[HelpTopic, ...] = _load_topics()


async def seed_help_memories(storage: StorageBackend) -> int:
    """Idempotently seed the help topics as memories. Returns the number of
    rows actually written (0 on a warm, unchanged store).

    Upsert-always with skip-if-unchanged: an existing ``help.*`` row whose
    ``learning`` + ``description`` already match the shipped content is left
    untouched (no write, no embedding refresh). Changed/absent rows are saved
    with empty ``entities`` (so they never pollute Learnings sections), and the
    embedding channel is refreshed via ``SearchService.upsert_memory`` — the
    storage layer does not embed on its own.
    """
    written = 0
    for topic in HELP_TOPICS:
        existing = await storage.get_memory_row(topic.id)
        if (
            existing is not None
            and existing.learning == topic.learning
            and existing.description == topic.description
            # Also require the invariant metadata to already hold — otherwise a
            # help.* id someone tagged with entities / a query (but with matching
            # text) would skip the rewrite and keep polluting Learnings / recall.
            and existing.entities == []
            and existing.query is None
        ):
            continue
        memory = await storage.save_memory(
            id=topic.id,
            learning=topic.learning,
            description=topic.description,
            entities=[],
        )
        # Embedding/retriever fan-out (DEV-1658 / Codex): storage.save_memory
        # only persists the row. Local import mirrors MemoryService.save_memory
        # — keeps the search module off the critical-path import graph.
        from slayer.search.service import SearchService

        await SearchService(storage=storage).upsert_memory(memory)
        written += 1
    return written
