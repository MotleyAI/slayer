"""SlayerModel v24 / SlayerQuery v15 / Memory v11: a stored date-only comparison keeps its whole-day meaning.

Stored-only, and gated by each nested document's own version. Whether an operand is a TIMESTAMP is read from the
datasource's stored models, which the storage load path provides (``typing_with``); a comparison whose operand cannot be
typed is left as written and logged.
"""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterable

from pydantic import BaseModel, Field

from slayer.core.date_only import scan_date_only
from slayer.core.enums import DataType
from slayer.core.errors import AmbiguousReferenceError
from slayer.core.names import match_name
from slayer.core.syntax import string_literal_spans
from slayer.core.time_points import is_date_only
from slayer.core.legacy_spellings import rewrite_spans
from slayer.storage.migrations import document_label, register_migration, stamp_stored
from slayer.storage.rank_direction_migration import rewrite_query_fields, stamp_memory_query

#: The first versions whose date-only comparisons are midnight.
UPGRADED_AT = {"SlayerModel": 24, "SlayerQuery": 15, "Memory": 11}

_logger = logging.getLogger(__name__)


def _named(name: Any, declared: Iterable[Any]) -> str | None:
    """The declared name an unquoted ``name`` reaches; ``None`` when none or several do."""
    if not isinstance(name, str):
        return None
    try:
        return match_name(name=name, declared=[d for d in declared if isinstance(d, str)], quoted=False)
    except AmbiguousReferenceError:
        return None


def _same(a: Any, b: Any) -> bool:
    return isinstance(a, str) and isinstance(b, str) and a.casefold() == b.casefold()


def stored_type(*, key: Any, columns: Any) -> str | None:
    """The stored ``type`` of the column named ``key`` when it is a valid ``DataType``."""
    if not isinstance(columns, list):
        return None
    named = [c for c in columns if isinstance(c, dict)]
    found = _named(key, (c.get("name") for c in named))
    col = next((c for c in named if found is not None and c.get("name") == found), None)
    try:
        return DataType(col["type"]).value if col is not None and "type" in col else None
    except ValueError:
        return None


def extended(*, key: str, model: dict, extensions: list[dict]) -> list:
    """``model[key]`` plus the ``key`` entries of each of ``extensions`` that extends ``model``."""
    own = model.get(key)
    added = [x for e in extensions if _same(e.get("source_name"), model.get("name")) and isinstance(e.get(key), list)
             for x in e[key]]
    return [*(own if isinstance(own, list) else []), *added]


class StoredModels(BaseModel):
    """A datasource's raw stored model dicts by name."""

    data_source: str | None = None
    models: dict[str, dict] = Field(default_factory=dict)

    def host(self, source: Any) -> dict | None:
        """The raw stored model named ``source``."""
        found = _named(source, self.models)
        return self.models[found] if found is not None else None

    def column_type(self, *, host: dict, parts: list[str] | tuple[str, ...], extensions: list[dict]) -> str | None:
        """The stored type of the column ``parts`` names from ``host``, walking stored joins plus ``extensions``."""
        current = host
        quals, leaf = list(parts[:-1]), parts[-1]
        if quals and _same(quals[0], current.get("name")):
            quals = quals[1:]
        for hop in quals:
            peer = self._hop(current=current, token=hop, extensions=extensions)
            if peer is None:
                return None
            current = peer
        return stored_type(key=leaf, columns=extended(key="columns", model=current, extensions=extensions))

    def _hop(self, *, current: dict, token: str, extensions: list[dict]) -> dict | None:
        """The model ``token`` reaches from ``current`` by the join walker's precedence: an incident edge's name, else the
        model at its other end; ``None`` when nothing or several edges match."""
        edges = self._incident(current=current, extensions=extensions)
        for tier in (0, 1):
            try:
                found = match_name(name=token, declared=[n for e in edges for n in (e[tier],) if n], quoted=False)
            except AmbiguousReferenceError:
                return None
            if found is not None:
                hits = [e for e in edges if e[tier] == found]
                return self.host(hits[0][1]) if len(hits) == 1 else None
        return None

    def _incident(self, *, current: dict, extensions: list[dict]) -> list[tuple[str | None, str]]:
        """``(edge name, model at the other end)`` per stored join incident to ``current``, declared on either side."""
        name = current.get("name")
        out = [(j.get("name"), j.get("target_model")) for j in extended(key="joins", model=current, extensions=extensions)
               if isinstance(j, dict)]
        for other in self.models.values():
            if not _same(other.get("name"), name):
                out.extend((j.get("name"), other.get("name"))
                           for j in extended(key="joins", model=other, extensions=extensions)
                           if isinstance(j, dict) and _same(j.get("target_model"), name))
        return [(n if isinstance(n, str) else None, t) for n, t in out if isinstance(t, str)]


_typing: ContextVar[StoredModels | None] = ContextVar("slayer_date_only_typing", default=None)


@contextmanager
def typing_with(models: StoredModels) -> Generator[None]:
    """Inside the block, stored operands are typed with ``models``."""
    token = _typing.set(models)
    try:
        yield
    finally:
        _typing.reset(token)


def needs_typing(entity: str, data: Any) -> bool:
    """Whether stored ``data``, or a document nested in it, predates the upgrade and holds a date-only literal."""
    if not isinstance(data, dict):
        return False
    if entity == "Memory":
        return needs_typing("SlayerQuery", data.get("query"))
    texts = _query_texts(data) if entity == "SlayerQuery" else _model_texts(data)
    if int(data.get("version", 1)) < UPGRADED_AT[entity] and any(map(_holds_date_literal, texts)):
        return True
    return entity == "SlayerModel" and any(needs_typing("SlayerQuery", q) for q in data.get("source_queries") or [])


def _holds_date_literal(text: str) -> bool:
    return any(is_date_only(s.value) for s in string_literal_spans(text))


def _model_texts(data: dict) -> list[str]:
    """``data``'s measure formulas and column filters."""
    out: list[str] = []
    for key, field in (("measures", "formula"), ("columns", "filter")):
        items = data.get(key)
        if isinstance(items, list):
            out.extend(i[field] for i in items if isinstance(i, dict) and isinstance(i.get(field), str))
    return out


def _query_texts(data: dict) -> list[str]:
    """Every Mode-B text of ``data`` the upgrade reads: its own fields and its extensions' texts."""
    out: list[str] = []

    def collect(text: str) -> str:
        out.append(text)
        return text

    rewrite_query_fields(dict(data), collect)
    raw = data.get("extensions")
    for e in raw if isinstance(raw, list) else []:
        if isinstance(e, dict):
            out.extend(_model_texts(e))
    return out


def _upgraded(text: str, *, label: str | None, host: dict | None, extensions: list[dict]) -> str:
    scan = scan_date_only(text)
    if not scan.sites and not scan.unlocated:
        return text
    models = _typing.get() or StoredModels()
    rewrite, untyped = [], scan.unlocated > 0
    for site in scan.sites:
        t = models.column_type(host=host, parts=site.parts, extensions=extensions) if host else None
        if t == DataType.TIMESTAMP.value:
            rewrite.append(site)
        untyped = untyped or t is None
    if untyped:
        _logger.warning(
            "stored document %r: a date-only comparison whose operand could not be typed is left as written, and "
            "now compares with midnight: %s", label, text.replace("\r", "\\r").replace("\n", "\\n"),
        )
    return rewrite_spans(text, rewrite)


def _with_text(item: Any, key: str, fn) -> Any:
    if isinstance(item, dict) and isinstance(item.get(key), str):
        return {**item, key: fn(item[key])}
    return item


def _upgraded_model_texts(data: dict, *, label: str | None, host: dict | None, extensions: list[dict]) -> dict:
    """``data``'s measure formulas and column filters upgraded, typed from ``host``."""

    def fn(text: str) -> str:
        return _upgraded(text, label=label, host=host, extensions=extensions)

    for key, field in (("measures", "formula"), ("columns", "filter")):
        if isinstance(data.get(key), list):
            data[key] = [_with_text(item, field, fn) for item in data[key]]
    return data


def upgrade_model(data: dict) -> dict:
    """Upgrade the measure formulas and column filters; mark nested stored queries as stored."""
    data = _upgraded_model_texts(data, label=document_label(data), host=data, extensions=[])
    if isinstance(data.get("source_queries"), list):
        data["source_queries"] = [stamp_stored(q) for q in data["source_queries"]]
    return data


def upgrade_query(data: dict) -> dict:
    """Upgrade every Mode-B field and each extension's measures and column filters."""
    models = _typing.get()
    label = document_label(data)
    raw = data.get("extensions")
    extensions = [e for e in raw if isinstance(e, dict)] if isinstance(raw, list) else []
    host = models.host(data.get("source_model")) if models else None
    data = rewrite_query_fields(data, lambda text: _upgraded(text, label=label, host=host, extensions=extensions))
    if isinstance(data.get("extensions"), list):
        data["extensions"] = [
            _upgraded_model_texts(dict(e), label=label, extensions=extensions,
                                  host=models.host(e.get("source_name")) if models else None)
            if isinstance(e, dict) else e
            for e in data["extensions"]
        ]
    return data


register_migration(entity="SlayerModel", source_version=23, stored_only=True)(upgrade_model)
register_migration(entity="SlayerQuery", source_version=14, stored_only=True)(upgrade_query)
register_migration(entity="Memory", source_version=10, stored_only=True)(stamp_memory_query)
