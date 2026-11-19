"""SlayerModel v27: a bare name in a row definition reads the source column, so each stored filter's bare reference to
a sibling field is written as that field's path. A field's ``sql`` converts on load (``slayer.core.models``,
``slayer.sql.definitions``); ``source.`` paths are respelled in the storage load pipeline, which knows the peers."""

from collections.abc import Callable, Mapping
from typing import Any

from slayer.core.syntax import respell_path_heads
from slayer.sql.root_scope import qualified_siblings, respelled_qualifier
from slayer.storage.migrations import register_migration

#: Stored datasets below this version read ``source.`` as a path and bare filter names as fields.
FIELD_EXPRESSION_VERSION = 27
_SOURCE = "source"


def _source_hop_target(relationships: list[Any]) -> str | None:
    """The dataset a stored ``source.`` path walks to: a relationship named ``source``, else one to a dataset so named."""
    for relationship in relationships:
        name = relationship.get("name") or relationship.get("to")
        if isinstance(name, str) and name.casefold() == _SOURCE:
            to = relationship.get("to")
            return to if isinstance(to, str) else None
    return None


def _respelled_texts(data: dict, *, mode_b: Callable[[str], str], native: Callable[[str], str]) -> list[tuple[str, dict]]:
    """Rewrite ``data``'s definitions in place; ``(label, changed?)`` pairs name each definition that changed."""
    changed: list[tuple[str, dict]] = []

    def apply(owner: dict, key: str, fn: Callable[[str], str], label: str) -> None:
        text = owner.get(key)
        if isinstance(text, str) and (new := fn(text)) != text:
            owner[key] = new
            changed.append((label, owner))

    for field in data.get("fields") or []:
        if not isinstance(field, dict):
            continue
        label = f"field {field.get('name')!r}"
        apply(field, "sql", native, label)
        apply(field, "filter", native, label)
        expression = field.get("expression")
        if isinstance(expression, dict):
            apply(expression, "sql", mode_b, label)
            for entry in expression.get("dialects") or []:
                if isinstance(entry, dict):
                    apply(entry, "expression", native, label)
        else:
            apply(field, "expression", mode_b, label)
    for metric in data.get("metrics") or []:
        if isinstance(metric, dict):
            apply(metric, "expression", mode_b, f"metric {metric.get('name')!r}")
    filters = data.get("filters")
    if isinstance(filters, list):
        holder = {str(i): f for i, f in enumerate(filters)}
        for key in list(holder):
            apply(holder, key, native, f"dataset filter {holder[key]!r}")
        data["filters"] = list(holder.values())
    return changed


def respell_source_paths(data: dict, *, peer_relationships: Mapping[str, list[Any]]) -> dict:
    """A stored document from before v27 with each ``source.`` path respelled by its target dataset's name, so it keeps
    walking the relationship; raises naming the definition when that name is not a unique hop."""
    relationships = [r for r in data.get("relationships") or [] if isinstance(r, dict)]
    target = _source_hop_target(relationships)
    if target is None:
        return data
    changed = _respelled_texts(
        data,
        mode_b=lambda t: respell_path_heads(t, respell=lambda head: target if head.casefold() == _SOURCE else None),
        native=lambda t: respelled_qualifier(t, old=_SOURCE, new=target, dialect=None),
    )
    if not changed:
        return data
    edges = [r for r in relationships if r.get("to") == target]
    edges += [r for r in peer_relationships.get(target, []) if isinstance(r, dict) and r.get("to") == data.get("name")]
    if len(edges) != 1 or target.casefold() == _SOURCE:
        raise ValueError(
            f"Dataset {data.get('name')!r}: {changed[0][0]} reads 'source.' as the relationship to {target!r}, but "
            f"'source.' now reads the dataset's source column and {target!r} names no unique relationship to respell "
            f"it with; spell the path by a relationship name."
        )
    return data


def _expanding(fields: list[Any]) -> set[str]:
    """The fields a bare name read before this version: a derived ``sql`` or a ``filter``."""
    out: set[str] = set()
    for field in fields:
        name = field.get("name") if isinstance(field, dict) else None
        sql = field.get("sql") if isinstance(field, dict) else None
        if isinstance(name, str) and (field.get("filter") or (isinstance(sql, str) and sql.strip() != name)):
            out.add(name)
    return out


def _qualified(text: Any, *, dataset: str, siblings: set[str]) -> Any:
    return qualified_siblings(text, dataset=dataset, siblings=siblings, dialect=None) if isinstance(text, str) else text


def upgrade_dataset(data: dict) -> dict:
    name, fields = data.get("name"), data.get("fields")
    if not isinstance(name, str) or not isinstance(fields, list):
        return data
    expanding = _expanding(fields)
    data["fields"] = [
        {**f, "filter": _qualified(f["filter"], dataset=name, siblings=expanding - {f.get("name")})}
        if isinstance(f, dict) and f.get("filter") else f
        for f in fields
    ]
    if isinstance(data.get("filters"), list):
        data["filters"] = [_qualified(f, dataset=name, siblings=expanding) for f in data["filters"]]
    return data


register_migration(entity="SlayerModel", source_version=26, stored_only=True)(upgrade_dataset)
