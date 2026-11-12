"""SlayerModel v23 / SlayerQuery v15 / Memory v11: a stored date-only comparison keeps its whole-day meaning.

Stored-only, and gated by each nested document's own version. Whether an operand is a TIMESTAMP is read from the
datasource's stored models, which the storage load path provides (``typing_with``); a comparison whose operand cannot be
typed is left as written and logged.
"""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from pydantic import BaseModel, Field

from slayer.core.date_only import scan_date_only
from slayer.core.enums import DataType
from slayer.core.legacy_spellings import rewrite_spans
from slayer.storage.migrations import document_label, register_migration, stamp_stored
from slayer.storage.rank_direction_migration import rewrite_query_fields, stamp_memory_query

#: The first versions whose date-only comparisons are midnight.
UPGRADED_AT = {"SlayerModel": 23, "SlayerQuery": 15, "Memory": 11}

_logger = logging.getLogger(__name__)


def stored_type(*, key: Any, columns: Any) -> str | None:
    """The stored ``type`` of the column named ``key`` when it is a valid ``DataType``."""
    if not isinstance(columns, list):
        return None
    col = next((c for c in columns if isinstance(c, dict) and c.get("name") == key), None)
    try:
        return DataType(col["type"]).value if col is not None and "type" in col else None
    except ValueError:
        return None


def extended(*, key: str, model: dict, extensions: list[dict]) -> list:
    """``model[key]`` plus the ``key`` entries of each of ``extensions`` that extends ``model``."""
    own = model.get(key)
    added = [x for e in extensions if e.get("source_name") == model.get("name") and isinstance(e.get(key), list)
             for x in e[key]]
    return [*(own if isinstance(own, list) else []), *added]


class StoredModels(BaseModel):
    """A datasource's raw stored model dicts by name."""

    data_source: str | None = None
    models: dict[str, dict] = Field(default_factory=dict)

    def host(self, source: Any) -> dict | None:
        """The raw stored model named ``source``."""
        return self.models.get(source) if isinstance(source, str) else None

    def column_type(self, *, host: dict, parts: list[str] | tuple[str, ...], extensions: list[dict]) -> str | None:
        """The stored type of the column ``parts`` names from ``host``, walking stored joins plus ``extensions``."""
        current = host
        quals, leaf = list(parts[:-1]), parts[-1]
        if quals and quals[0] == current.get("name"):
            quals = quals[1:]
        for hop in quals:
            joins = [j for j in extended(key="joins", model=current, extensions=extensions) if isinstance(j, dict)]
            target = next((j.get("target_model") for j in joins if j.get("name") == hop), None) or next(
                (hop for j in joins if not j.get("name") and j.get("target_model") == hop), None)
            peer = self.models.get(target) if isinstance(target, str) else None
            if peer is None:
                return None
            current = peer
        return stored_type(key=leaf, columns=extended(key="columns", model=current, extensions=extensions))


_typing: ContextVar[StoredModels | None] = ContextVar("slayer_date_only_typing", default=None)


@contextmanager
def typing_with(models: StoredModels) -> Generator[None]:
    """Inside the block, stored operands are typed with ``models``."""
    token = _typing.set(models)
    try:
        yield
    finally:
        _typing.reset(token)


def below_upgrade(entity: str, data: Any) -> bool:
    """Whether stored ``data`` or a document nested in it predates the upgrade."""
    if not isinstance(data, dict):
        return False
    if int(data.get("version", 1)) < UPGRADED_AT[entity]:
        return True
    if entity == "Memory":
        return below_upgrade("SlayerQuery", data.get("query"))
    if entity == "SlayerQuery":
        return False
    return any(below_upgrade("SlayerQuery", q) for q in data.get("source_queries") or [])


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


register_migration(entity="SlayerModel", source_version=22, stored_only=True)(upgrade_model)
register_migration(entity="SlayerQuery", source_version=14, stored_only=True)(upgrade_query)
register_migration(entity="Memory", source_version=10, stored_only=True)(stamp_memory_query)
