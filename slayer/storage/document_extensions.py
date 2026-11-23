"""``SLAYER`` extension entries of a stored document applied to its decomposed datasets.

An entry's SLayer-only attributes always apply; what it restores over Ossie content (a formula, a value/filter split,
a source) applies only while its fingerprint matches the entity. An unusable entry is ignored with a note."""

from __future__ import annotations

from collections.abc import Collection
from typing import Any

from pydantic import BaseModel

from slayer.core import slayer_extension as ext
from slayer.core.enums import JoinType, RelationshipCardinality
from slayer.core.field_expression import DialectEntry, ExpressionObject
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, ExtensionNote, SlayerDataset
from slayer.core.ossie_expression import OSIExpression
from slayer.core.query import SlayerQuery
from slayer.core.semantic_model import OssieExpression, OssieRelationship
from slayer.engine.binding import bind_dataset_expression
from slayer.sql.ossie_dialects import expression_digest, source_digest

__all__ = ["Entry", "Restored", "apply_dataset_entry", "apply_field_entry", "metric_entry", "read_entry",
           "relocate_metrics", "restore_metrics",
           "STALE"]

STALE = "its SLAYER entry was written for an earlier version of it (edited by hand; the fingerprint differs)"
_ALWAYS = ("hidden", "format", "meta", "element_type", "db_type", "sampled", "sampled_values", "distinct_count")


class Entry:
    """An entity's decoded ``SLAYER`` payload and whether it still matches the entity."""

    def __init__(self, payload: Any, *, matched: bool) -> None:
        self.payload = payload
        self.matched = matched


def digest(expression: OssieExpression) -> list[Any]:
    return expression_digest(OSIExpression.model_validate(expression.model_dump(exclude_none=True)))


def read_entry(extensions: list[Any] | None, *, kind: str, entity: str, ossie: Any, label: str,
               notes: list[ExtensionNote]) -> Entry | None:
    """The entity's usable ``SLAYER`` entry; ``None`` (with a note when one is present but unusable) otherwise."""
    entries = [e for e in extensions or [] if ext.is_slayer(e)]
    if not entries:
        return None

    def note(message: str) -> None:
        notes.append(ExtensionNote(kind=kind, entity=entity, message=f"{label}: {message}; ignored"))

    if len(entries) > 1:
        note(f"it carries {len(entries)} SLAYER extension entries (several); keep exactly one")
        return None
    try:
        payload = ext.decode(entries[0].data)
    except ValueError as exc:
        version = entries[0].data.get("version") if isinstance(entries[0].data, dict) else None
        reason = "of an unknown version" if version not in ext.KNOWN_VERSIONS else "malformed"
        note(f"its SLAYER extension entry is {reason} ({str(exc).splitlines()[0]})")
        return None
    if payload.kind != kind or payload.entity != entity:
        note(f"its SLAYER extension entry describes {payload.kind} {payload.entity!r}, not this {kind}")
        return None
    return Entry(payload, matched=ext.matches(payload, ossie=ossie))


def _dialect_only(sql: str, *, target: str) -> ExpressionObject:
    """A version-1 or -2 payload's field ``sql``: native text of the datasource's dialect."""
    return ExpressionObject(dialects=[DialectEntry(dialect=target, expression=sql)])


def apply_field_entry(field: DatasetField, entry: Entry | None, *, notes: list[ExtensionNote],
                      label: str, target: str) -> DatasetField:
    """``target`` (the datasource's Ossie dialect token) tags an older payload's field ``sql``."""
    if entry is None:
        return field
    content: ext.FieldContent = entry.payload.field
    update: dict[str, Any] = {k: v for k in _ALWAYS if (v := getattr(content, k)) is not None and v is not False}
    if entry.matched:
        update.update({k: v for k in ("filter", "granularity", "allowed_aggregations")
                       if (v := getattr(content, k)) is not None})
        if content.expression is not None:
            update["expression"] = content.expression
        elif content.sql is not None:
            update["expression"] = _dialect_only(content.sql, target=target)
        if content.datatype is not None:
            update["datatype"] = content.datatype
    elif any(getattr(content, k) is not None for k in ("expression", "filter", "granularity", "allowed_aggregations")):
        notes.append(ExtensionNote(kind="field", entity=field.name, message=f"{label}: {STALE}; its definition is "
                                   "read from the document"))
    return field.model_copy(update=update)


def apply_dataset_entry(dataset: SlayerDataset, entry: Entry | None, *, notes: list[ExtensionNote],
                        target: str) -> SlayerDataset:
    """``target`` (the datasource's Ossie dialect token) tags an older payload's SLayer-only field ``sql``."""
    if entry is None:
        return dataset
    payload: ext.DatasetPayload = entry.payload
    update: dict[str, Any] = {"hidden": payload.hidden, "access_tags": list(payload.access_tags),
                              "aggregations": list(payload.aggregations), "meta": payload.meta}
    if payload.default_time_dimension is None or dataset.get_field(payload.default_time_dimension) is not None:
        update["default_time_dimension"] = payload.default_time_dimension
    source = payload.source
    if source != ext.SourceStructure():
        if not entry.matched:
            notes.append(ExtensionNote(kind="dataset", entity=dataset.name,
                                       message=f"dataset {dataset.name!r}: {STALE}; its source is the document's"))
        elif source.source_queries:
            update.update(sql=None, sql_table=None, filters=[], query_variables=dict(source.query_variables),
                          source_queries=[SlayerQuery.model_validate(q) for q in source.source_queries])
        else:
            update.update(sql_table=source.sql_table, sql=source.sql, filters=list(source.filters or []))
    taken = {f.name.casefold() for f in dataset.fields} | {m.name.casefold() for m in dataset.metrics}
    fields = list(dataset.fields)
    for extra in payload.fields:
        if extra.name.casefold() in taken:
            notes.append(ExtensionNote(kind="field", entity=extra.name, message=(
                f"SLayer-only field {extra.name!r} collides with a declared name; ignored")))
            continue
        taken.add(extra.name.casefold())
        if (sql := payload.legacy_sql.get(extra.name)) is not None:
            extra = extra.model_copy(update={"expression": _dialect_only(sql, target=target)})
        fields.append(extra)
    update["fields"] = fields
    update["metrics"] = [*dataset.metrics, *payload.metrics]
    update["relationships"] = [*dataset.relationships, *payload.relationships]
    return dataset.model_copy(update=update)


class Restored(BaseModel):
    """A metric restored from its ``SLAYER`` entry: its document form on ``origin``, restored on ``home`` as ``name``
    (another dataset of the document for an export's relocated metric)."""

    origin: str
    home: str
    name: str
    core: DatasetMetric


def metric_entry(metric: DatasetMetric, entry: Entry | None, *, home: str, members: set[str],
                 notes: list[ExtensionNote], label: str) -> tuple[DatasetMetric, Restored | None]:
    """``metric`` restored from its matching entry, and the restoration (its binding is checked once every dataset is
    built); a matching entry naming another dataset of the document (``members``, case-folded) relocates it there."""
    if entry is None:
        return metric, None
    payload: ext.MetricPayload = entry.payload
    if payload.dataset.casefold() != home.casefold():
        if entry.matched and payload.dataset.casefold() in members:
            restored = payload.metric.model_copy(update={"description": metric.description
                                                          or payload.metric.description})
            return restored, Restored(origin=home, home=payload.dataset, name=restored.name, core=metric)
        reason = "which the document does not hold" if payload.dataset.casefold() not in members else STALE
        notes.append(ExtensionNote(kind="metric", entity=metric.name, message=(
            f"{label}: its SLAYER entry names dataset {payload.dataset!r}, {reason}; ignored")))
        return metric, None
    if not entry.matched:
        if payload.metric.expression != metric.expression:
            notes.append(ExtensionNote(kind="metric", entity=metric.name, message=f"{label}: {STALE}; its formula "
                                       "is read from the document"))
        return metric.model_copy(update={"label": payload.metric.label, "meta": payload.metric.meta}), None
    restored = payload.metric.model_copy(update={"name": metric.name,
                                                 "description": metric.description or payload.metric.description})
    return restored, Restored(origin=home, home=home, name=metric.name, core=metric)


def _named(dataset: SlayerDataset, name: str) -> int | None:
    return next((i for i, m in enumerate(dataset.metrics) if m.name.casefold() == name.casefold()), None)


def relocate_metrics(datasets: dict[str, SlayerDataset], *, restored: list[Restored]) -> None:
    """Move each relocated metric to its home dataset; one whose name is taken there stays in its document form."""
    by_fold = {name.casefold(): name for name in datasets}
    for item in [r for r in restored if r.home.casefold() != r.origin.casefold()]:
        origin, home = datasets[item.origin], datasets[by_fold[item.home.casefold()]]
        i = _named(origin, item.name)
        assert i is not None
        metric = origin.metrics.pop(i)
        if _named(home, item.name) is not None or home.get_field(item.name) is not None:
            origin.metrics.insert(i, item.core)
            restored.remove(item)
            origin.extension_notes.append(ExtensionNote(kind="metric", entity=item.core.name, message=(
                f"metric {item.core.name!r}: its SLAYER entry restores it as {home.name}.{item.name}, a name "
                f"{home.name!r} already holds; ignored")))
            continue
        home.metrics.append(metric)
        item.home = home.name


def restore_metrics(datasets: dict[str, SlayerDataset], *, restored: list[Restored], dialect: str) -> None:
    """Revert each restored metric that does not bind to its document form, with a note."""
    peers = list(datasets.values())
    for item in restored:
        dataset = datasets[item.home]
        i = _named(dataset, item.name)
        assert i is not None
        metric = dataset.metrics[i]
        try:
            bind_dataset_expression(metric.text, dataset=dataset, datasets=peers, dialect=dialect)
        except Exception as exc:  # noqa: BLE001 — any binding failure keeps the document form
            del dataset.metrics[i]
            origin = datasets[item.origin]
            origin.metrics.insert(len(origin.metrics) if origin is not dataset else i, item.core)
            origin.extension_notes.append(ExtensionNote(kind="metric", entity=item.core.name, message=(
                f"metric {item.core.name!r}: its SLAYER formula {metric.text!r} does not bind "
                f"({str(exc).splitlines()[0]}); ignored")))


def restored_relationship(rel: OssieRelationship, *, datasets: Collection[str],
                          notes: list[ExtensionNote]) -> tuple[str, DatasetRelationship]:
    """The one ``(dataset, relationship)`` a load restores document relationship ``rel`` as: its ``SLAYER`` entry's sole
    declaration when usable, else a many-to-one LEFT relationship on ``from``."""
    entry = read_entry(rel.nonportable_extensions, kind="relationship", entity=rel.name,
                       ossie=ext.relationship_digest(from_dataset=rel.from_dataset, to=rel.to,
                                                     from_columns=list(rel.from_columns),
                                                     to_columns=list(rel.to_columns)),
                       label=f"relationship {rel.name!r}", notes=notes)
    default = DatasetRelationship(name=rel.name, to=rel.to, from_columns=list(rel.from_columns),
                                  to_columns=list(rel.to_columns), cardinality=RelationshipCardinality.MANY_TO_ONE,
                                  join_type=JoinType.LEFT)
    return _declared_or(rel, entry, datasets=datasets, notes=notes) or (rel.from_dataset, default)


def _declared_or(rel: OssieRelationship, entry: Entry | None, *, datasets: Collection[str],
                 notes: list[ExtensionNote]) -> tuple[str, DatasetRelationship] | None:
    """``entry``'s sole declaration when usable; ``None`` (noted when an entry is present) otherwise."""
    if entry is None:
        return None
    if not entry.matched:
        notes.append(ExtensionNote(kind="relationship", entity=rel.name,
                                   message=f"relationship {rel.name!r}: {STALE}; read as a many-to-one relationship"))
        return None
    declaration = ext.sole_declaration(entry.payload, matched=True)
    if declaration is None:
        notes.append(ExtensionNote(kind="relationship", entity=rel.name, message=(
            f"relationship {rel.name!r}: its SLAYER entry declares {len(entry.payload.declarations)} relationships, "
            "where one relationship restores exactly one; read as a many-to-one relationship")))
        return None
    if declaration.dataset not in datasets or declaration.relationship.to not in datasets:
        notes.append(ExtensionNote(kind="relationship", entity=rel.name, message=(
            f"relationship {rel.name!r}: its SLAYER entry declares it on {declaration.dataset!r} to "
            f"{declaration.relationship.to!r}, which are not both in the document; read as a many-to-one "
            "relationship")))
        return None
    return declaration.dataset, declaration.relationship.model_copy()


def source_ossie(source: str, *, reader: str) -> str:
    return source_digest(source, reader=reader)
