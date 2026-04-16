"""Conceptual help for SLayer.

Content is authored as ``.md`` files under :mod:`slayer.help.topics`. Topic
names are discovered at module import time by scanning that directory, so
adding a new topic is a matter of dropping a new ``NN_name.md`` file in —
no Python changes needed.

Public API:

* :func:`render_help` — return the intro when called with no topic, or the
  content of the requested topic. Returns a friendly error string (never
  raises) for unknown topics.
* :func:`available_topic_names` — ordered tuple of topic keys (excluding the
  intro).
* :data:`TOPIC_SUMMARY_LINE` — one-line string listing every topic, reused by
  the MCP tool description and the CLI subparser epilog.

Filenames in ``topics/`` use an ``NN_name.md`` convention (e.g.
``01_queries.md``) so that sorted filesystem iteration gives a stable
teaching order. The ``NN_`` prefix is stripped to form the topic key.
``00_intro.md`` is treated specially as the intro body returned when the
caller asks for no topic in particular.
"""

from __future__ import annotations

from importlib.resources import files
from typing import Optional

__all__ = (
    "render_help",
    "available_topic_names",
    "TOPIC_SUMMARY_LINE",
    "INTRO_KEY",
)

INTRO_KEY = "intro"

_TOPICS_SUBDIR = "topics"


def _strip_numeric_prefix(stem: str) -> str:
    """Turn ``"01_queries"`` into ``"queries"``; leave other stems unchanged."""
    if len(stem) >= 3 and stem[0].isdigit() and stem[1].isdigit() and stem[2] == "_":
        return stem[3:]
    return stem


def _discover() -> tuple[str, dict[str, str], tuple[str, ...]]:
    """Scan ``topics/*.md`` once at module load.

    Returns ``(intro_body, topic_bodies, ordered_topic_keys)``. ``topic_bodies``
    does not contain the intro. ``ordered_topic_keys`` preserves filesystem
    sort order (driven by the ``NN_`` prefix).
    """
    topics_dir = files(__name__) / _TOPICS_SUBDIR
    intro_body = ""
    bodies: dict[str, str] = {}
    order: list[str] = []

    entries = sorted(topics_dir.iterdir(), key=lambda e: e.name)
    for entry in entries:
        if not entry.is_file() or not entry.name.endswith(".md"):
            continue
        stem = entry.name[: -len(".md")]
        key = _strip_numeric_prefix(stem)
        body = entry.read_text(encoding="utf-8").rstrip() + "\n"
        if key == INTRO_KEY:
            intro_body = body
        else:
            bodies[key] = body
            order.append(key)

    return intro_body, bodies, tuple(order)


_INTRO_BODY, _TOPIC_BODIES, _TOPIC_ORDER = _discover()

TOPIC_SUMMARY_LINE = (
    "Available help topics: " + ", ".join(_TOPIC_ORDER) + "."
    if _TOPIC_ORDER
    else "No help topics are installed."
)


def available_topic_names() -> tuple[str, ...]:
    """Return the ordered tuple of topic keys (intro excluded)."""
    return _TOPIC_ORDER


def render_help(topic: Optional[str] = None) -> str:
    """Render help content.

    * No topic (``None``, empty, or whitespace-only): return the intro body.
    * Known topic (case-insensitive, leading/trailing whitespace ignored):
      return that topic's body.
    * Unknown topic: return a friendly ``"Unknown topic 'X'. Available: ..."``
      string. Never raises.
    """
    if topic is None or not str(topic).strip():
        return _INTRO_BODY
    key = str(topic).strip().lower()
    body = _TOPIC_BODIES.get(key)
    if body is not None:
        return body
    return (
        f"Unknown help topic '{topic}'. "
        f"Available topics: {', '.join(_TOPIC_ORDER)}."
    )
