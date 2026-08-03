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

import re
from collections import Counter
from collections.abc import Mapping, Sequence
from importlib.resources import files

from pydantic import BaseModel

from slayer.storage.base import StorageBackend

_CONTENT_SUBDIR = "help_content"
_ID_PREFIX = "help."

#: Authored one-line previews (<=500 chars) surfaced by search(compact=True)
#: and inspect(compact=True). Keyed by the topic key (``NN_`` prefix stripped).
_DESCRIPTIONS: dict[str, str] = {
    "intro": "What {{product}} is, the core entities, the query shape, and the biggest gotchas.",
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


#: Host-substitutable tokens in the shipped content. An embedding host (e.g. a
#: hosted SLayer that renames the query tool) overrides these instead of forking
#: the markdown. Written ``{{name}}`` — deliberately NOT ``str.format`` /
#: ``string.Template`` syntax, because the content is full of single-brace JSON
#: examples (``{"source_model": "orders"}``) and ``'$'`` currency symbols.
DEFAULT_HELP_CONTEXT: dict[str, str] = {
    "product": "SLayer",
}

_PLACEHOLDER_RE = re.compile(r"\{\{(\w+)\}\}")


def _render(text: str, context: Mapping[str, str]) -> str:
    """Substitute ``{{name}}`` tokens from ``context``.

    An unknown token raises rather than rendering literally — a typo in the
    shipped content should fail loudly at load, not ship ``{{prodcut}}`` to an
    agent.
    """
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
    """SLayer's built-in help topics, in teaching (``NN_``) order.

    ``context`` overrides :data:`DEFAULT_HELP_CONTEXT` so a host can rename the
    product or the query tool without copying the markdown. Pair with
    :func:`merge_help_topics` to replace or extend individual topics.
    """
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
    """Compose a host's topic set from SLayer's.

    ``override`` replaces topics by id, keeping ``base``'s teaching order, so a
    host only ships the bodies that genuinely differ. ``extra`` appends
    host-specific topics — give those a namespaced id (e.g. ``help.motley.x``)
    so they can't collide with a future built-in.

    Raises when an ``override`` id isn't in ``base``: that means the built-in was
    renamed or removed upstream and the host's copy is silently dead.
    """
    override = dict(override or {})
    unknown = sorted(set(override) - {topic.id for topic in base})
    if unknown:
        raise ValueError(
            f"override targets no built-in help topic: {', '.join(unknown)}. "
            f"Known ids: {', '.join(topic.id for topic in base)}."
        )
    # A value whose own id differs from its key replaces the built-in with a
    # topic seeded under that other id — so the topic it was meant to replace
    # silently stops being served (e.g. keyed help.workflow, id help.workflows
    # removes help.workflow from the set entirely).
    mismatched = sorted(
        f"{key} -> {topic.id}" for key, topic in override.items() if topic.id != key
    )
    if mismatched:
        raise ValueError(
            f"override topic id must equal its key: {', '.join(mismatched)}."
        )
    merged = [override.get(topic.id, topic) for topic in base]
    merged.extend(extra)
    # Two topics sharing an id would seed last-write-wins, so one body is lost
    # with no error. Usually an ``extra`` that collides with a built-in.
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
    for topic in (HELP_TOPICS if topics is None else topics):
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
