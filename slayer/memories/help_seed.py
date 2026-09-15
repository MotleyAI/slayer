"""Conceptual help, seeded as predefined memories (``help.intro`` …) from
``help_content/NN_name.md`` (``NN_`` fixes teaching order, stripped for the
topic key) and read via ``inspect(entity_type="memory")`` / ``search``.
Seeding is idempotent (upsert, skip-if-unchanged) and seeds no entities, so
help never surfaces in a model's Learnings section."""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Mapping, Sequence
from importlib.resources import files

from pydantic import BaseModel

from slayer.core.errors import MemoryNotFoundError
from slayer.storage.base import StorageBackend

_CONTENT_SUBDIR = "help_content"
_ID_PREFIX = "help."

#: Authored one-line previews (<=500 chars) surfaced by search(compact=True)
#: and inspect(compact=True). Keyed by the topic key (``NN_`` prefix stripped).
_DESCRIPTIONS: dict[str, str] = {
    "intro": "What {{product}} is, the judgment calls queries require, and the deep-dive topics.",
    "models": "Authoring models: columns, saved measures, custom aggregations, joins, filters, query-backed models, result keys.",
    "workflow": "Tool-chaining for discovery, query building, and connecting databases, plus an error decoder.",
}

#: Former built-in topic ids (query-language content now lives on the ``query``
#: tool's docstring and schema). Seeding deletes them from warm stores so
#: retired bodies stop being served; host-namespaced ids are never touched.
RETIRED_HELP_IDS: tuple[str, ...] = (
    "help.queries",
    "help.formulas",
    "help.aggregations",
    "help.transforms",
    "help.time",
    "help.filters",
    "help.joins",
    "help.extending",
)


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


#: Host-substitutable tokens. ``{{name}}`` syntax — NOT str.format/Template,
#: because the content is full of single-brace JSON examples.
DEFAULT_HELP_CONTEXT: dict[str, str] = {
    "product": "SLayer",
}

_PLACEHOLDER_RE = re.compile(r"\{\{(\w+)\}\}")


def _render(text: str, context: Mapping[str, str]) -> str:
    """Substitute ``{{name}}`` tokens; an unknown token raises at load rather
    than shipping a typo to an agent."""
    def _sub(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in context:
            raise KeyError(
                f"help content references unknown placeholder '{{{{{key}}}}}'; "
                f"known tokens: {', '.join(sorted(context))}"
            )
        return context[key]

    return _PLACEHOLDER_RE.sub(_sub, text)


def load_help_topics(
    *, context: Mapping[str, str] | None = None,
) -> tuple[HelpTopic, ...]:
    """Built-in help topics in teaching order; ``context`` overrides
    :data:`DEFAULT_HELP_CONTEXT` (pair with :func:`merge_help_topics`)."""
    ctx = {**DEFAULT_HELP_CONTEXT, **(context or {})}
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
            learning=_render(entry.read_text(encoding="utf-8"), ctx),
            description=_render(description, ctx),
        ))
    return tuple(topics)


def merge_help_topics(
    base: Sequence[HelpTopic],
    *,
    override: Mapping[str, HelpTopic] | None = None,
    extra: Sequence[HelpTopic] = (),
) -> tuple[HelpTopic, ...]:
    """Compose a host's topic set: ``override`` replaces by id (teaching order
    kept; unknown id raises — the built-in was renamed/removed upstream);
    ``extra`` appends host topics (namespace them, e.g. ``help.motley.x``)."""
    override = dict(override or {})
    unknown = sorted(set(override) - {topic.id for topic in base})
    if unknown:
        raise ValueError(
            f"override targets no built-in help topic: {', '.join(unknown)}. "
            f"Known ids: {', '.join(topic.id for topic in base)}."
        )
    # A key/id mismatch would silently drop the topic the override targets.
    mismatched = sorted(
        f"{key} -> {topic.id}" for key, topic in override.items() if topic.id != key
    )
    if mismatched:
        raise ValueError(
            f"override topic id must equal its key: {', '.join(mismatched)}."
        )
    merged = [override.get(topic.id, topic) for topic in base]
    merged.extend(extra)
    # Duplicate ids seed last-write-wins, losing a body silently.
    duplicates = sorted(
        topic_id
        for topic_id, count in Counter(topic.id for topic in merged).items()
        if count > 1
    )
    if duplicates:
        raise ValueError(
            f"duplicate help topic ids after merge: {', '.join(duplicates)}. "
            f"Namespace host-specific topics (e.g. 'help.motley.x')."
        )
    return tuple(merged)


def _load_topics() -> tuple[HelpTopic, ...]:
    """Back-compat alias for :func:`load_help_topics` with default context."""
    return load_help_topics()


HELP_TOPICS: tuple[HelpTopic, ...] = load_help_topics()


async def seed_help_memories(
    storage: StorageBackend, *, topics: Sequence[HelpTopic] | None = None,
) -> int:
    """Idempotently seed the help topics; returns rows actually written (0 on
    a warm, unchanged store). :data:`RETIRED_HELP_IDS` rows are deleted first;
    unchanged rows are skipped (no write, no embedding refresh); written rows
    fan out to ``SearchService.upsert_memory`` (storage never embeds)."""
    for stale_id in RETIRED_HELP_IDS:
        try:
            await storage.delete_memory(stale_id)
        except MemoryNotFoundError:
            pass
    written = 0
    for topic in (HELP_TOPICS if topics is None else topics):
        existing = await storage.get_memory_row(topic.id)
        if (
            existing is not None
            and existing.learning == topic.learning
            and existing.description == topic.description
            # Invariant metadata must hold too, else a tagged row keeps polluting Learnings.
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
        # Local import keeps search off the critical-path import graph.
        from slayer.search.service import SearchService

        await SearchService(storage=storage).upsert_memory(memory)
        written += 1
    return written
