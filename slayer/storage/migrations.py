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

from typing import Any, Callable, Dict, Tuple

# Per-entity current version. Bump independently when an entity's schema changes.
CURRENT_VERSIONS: Dict[str, int] = {
    "SlayerModel": 1,
    "SlayerQuery": 1,
    "DatasourceConfig": 1,
    "NamedQuery": 1,
}

# Registry: (entity_name, source_version) -> converter producing source_version+1.
_REGISTRY: Dict[Tuple[str, int], Callable[[dict], dict]] = {}


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
