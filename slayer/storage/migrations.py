"""Schema migration registry for persisted SLayer entities.

Migrations run as pure dict→dict transforms BEFORE Pydantic validates the data,
so they can rename fields, restructure shapes, or fill in defaults that the
target schema requires. They are triggered automatically by a
``model_validator(mode="before")`` on each persisted class — every caller that
does ``Model.model_validate(dict)`` gets migrations transparently, regardless
of which storage backend produced the dict.

Per-entity versions evolve independently. ``CURRENT_VERSIONS[entity]`` is the
version that ``.save_*()`` will write today.
"""

from typing import Any
from collections.abc import Callable

# Per-entity current version. Bump independently when an entity's schema changes.
CURRENT_VERSIONS: dict[str, int] = {
    "SlayerModel": 10,
    "SlayerQuery": 4,
    "DatasourceConfig": 2,
    "Memory": 2,
    "Embedding": 1,
}

# Registry: (entity_name, source_version) -> converter producing source_version+1.
_REGISTRY: dict[tuple[str, int], Callable[[dict], dict]] = {}


def register_migration(
    entity: str, source_version: int
) -> Callable[[Callable[[dict], dict]], Callable[[dict], dict]]:
    """Register a converter from ``source_version`` to ``source_version+1``.

    Used as a decorator::

        @register_migration("SlayerModel", 1)
        def _v1_to_v2(data: dict) -> dict:
            ...
            return data
    """

    def deco(fn: Callable[[dict], dict]) -> Callable[[dict], dict]:
        key = (entity, source_version)
        if key in _REGISTRY:
            raise ValueError(
                f"Duplicate migration for {entity} v{source_version}"
            )
        _REGISTRY[key] = fn
        return fn

    return deco


@register_migration("SlayerModel", 9)
def _model_v9_to_v10(data: dict) -> dict:
    """v10: per-doc no-op; the cross-document exact-inverse join dedup runs in
    the storage load path (``_migrate_and_refine_on_load``)."""
    return data


# Legacy quoted ``strict`` tokens; blank/whitespace counts as false. Anything
# outside both sets fails closed rather than silently reading as broadcast.
_STRICT_TRUE_TOKENS = frozenset({"1", "true", "t", "yes", "y", "on"})
_STRICT_FALSE_TOKENS = frozenset({"0", "false", "f", "no", "n", "off", ""})


@register_migration(entity="SlayerQuery", source_version=3)
def _query_v3_to_v4(data: dict) -> dict:
    """v4: retire ``strict`` — ``strict: true`` → ``to_many_handling: "error"``;
    ``false``/absent drops to the ``"broadcast"`` default."""
    if "strict" not in data:
        return data
    strict = data.pop("strict")
    if isinstance(strict, str):
        token = strict.strip().lower()
        if token in _STRICT_TRUE_TOKENS:
            strict = True
        elif token in _STRICT_FALSE_TOKENS:
            strict = False
        else:
            raise ValueError(
                f"Unrecognized legacy strict value {strict!r}; use to_many_handling instead"
            )
    if strict:
        data.setdefault("to_many_handling", "error")
    return data


def migrate(entity: str, data: Any) -> Any:
    """Walk migrations from ``data['version']`` up to ``CURRENT_VERSIONS[entity]``.

    Non-dict inputs (e.g. an already-built model instance passed to
    ``model_validate``) pass through untouched. Dicts whose ``version`` is
    higher than ``CURRENT_VERSIONS[entity]`` also pass through — Pydantic's
    default ``extra="ignore"`` lets older code load forward-versioned files
    on a best-effort basis.
    """
    if not isinstance(data, dict):
        return data
    if entity not in CURRENT_VERSIONS:
        raise KeyError(f"Unknown entity '{entity}' in migrate()")
    data = dict(data)  # never mutate caller's payload
    target = CURRENT_VERSIONS[entity]
    current = int(data.get("version", 1))
    while current < target:
        fn = _REGISTRY.get((entity, current))
        if fn is None:
            raise RuntimeError(
                f"No migration registered for {entity} v{current} → v{current + 1}"
            )
        data = fn(dict(data))
        current += 1
        data["version"] = current
    data.setdefault("version", target)
    return data


# Register concrete migrations. The import is deferred to the bottom of this
# module to avoid a circular import (the v2 module imports BUILTIN_AGGREGATIONS
# from slayer.core.enums and must register against the register_migration
# decorator defined above).
from slayer.storage import v2_migration  # noqa: E402, F401  # ALLOW(import-not-top): circular — migration modules import register_migration from here
from slayer.storage import v2_memory_migration  # noqa: E402, F401  # ALLOW(import-not-top): circular — migration modules import register_migration from here
from slayer.storage import v2_datasource_migration  # noqa: E402, F401  # ALLOW(import-not-top): circular — migration modules import register_migration from here
from slayer.storage import v3_migration  # noqa: E402, F401  # ALLOW(import-not-top): circular — migration modules import register_migration from here
from slayer.storage import v4_migration  # noqa: E402, F401  # ALLOW(import-not-top): circular — migration modules import register_migration from here
from slayer.storage import v5_migration  # noqa: E402, F401  # ALLOW(import-not-top): circular — migration modules import register_migration from here
from slayer.storage import v6_migration  # noqa: E402, F401  # ALLOW(import-not-top): circular — migration modules import register_migration from here
from slayer.storage import v7_migration  # noqa: E402, F401  # ALLOW(import-not-top): circular — migration modules import register_migration from here
from slayer.storage import v8_migration  # noqa: E402, F401  # ALLOW(import-not-top): circular — migration modules import register_migration from here
from slayer.storage import v9_migration  # noqa: E402, F401  # ALLOW(import-not-top): circular — migration modules import register_migration from here
