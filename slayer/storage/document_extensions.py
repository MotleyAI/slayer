"""``SLAYER`` extension entries of a stored document applied to its decomposed datasets.

An entry's SLayer-only attributes always apply; what it restores over Ossie content (a formula, a value/filter split,
a source) applies only while its fingerprint matches the entity. An unusable entry is ignored with a note."""

from __future__ import annotations

from typing import Any

from slayer.core import slayer_extension as ext
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, ExtensionNote, SlayerDataset
from slayer.core.ossie_expression import OSIExpression
from slayer.core.query import SlayerQuery
from slayer.core.semantic_model import OssieExpression
from slayer.engine.binding import bind_dataset_expression
from slayer.sql.ossie_dialects import expression_digest, source_digest

__all__ = ["Entry", "apply_dataset_entry", "apply_field_entry", "metric_entry", "read_entry", "restore_metrics",
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


def apply_field_entry(field: DatasetField, entry: Entry | None, *, notes: list[ExtensionNote],
                      label: str) -> DatasetField:
    if entry is None:
        return field
    content: ext.FieldContent = entry.payload.field
    update: dict[str, Any] = {k: v for k in _ALWAYS if (v := getattr(content, k)) is not None and v is not False}
    if entry.matched:
        update.update({k: v for k in ("filter", "granularity", "allowed_aggregations")
                       if (v := getattr(content, k)) is not None})
        if content.expression is not None:
            update["expression"] = content.expression
        if content.datatype is not None:
            update["datatype"] = content.datatype
    elif any(getattr(content, k) is not None for k in ("expression", "filter", "granularity", "allowed_aggregations")):
        notes.append(ExtensionNote(kind="field", entity=field.name, message=f"{label}: {STALE}; its definition is "
                                   "read from the document"))
    return field.model_copy(update=update)


def apply_dataset_entry(dataset: SlayerDataset, entry: Entry | None, *, notes: list[ExtensionNote]) -> SlayerDataset:
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
        fields.append(extra)
    update["fields"] = fields
    update["metrics"] = [*dataset.metrics, *payload.metrics]
    update["relationships"] = [*dataset.relationships, *payload.relationships]
    return dataset.model_copy(update=update)


def metric_entry(metric: DatasetMetric, entry: Entry | None, *, home: str, notes: list[ExtensionNote],
                 label: str) -> tuple[DatasetMetric, bool]:
    """``metric`` restored from its matching entry, and whether it was (its binding is checked once every dataset is
    built)."""
    if entry is None:
        return metric, False
    payload: ext.MetricPayload = entry.payload
    if payload.dataset.casefold() != home.casefold():
        notes.append(ExtensionNote(kind="metric", entity=metric.name, message=(
            f"{label}: its SLAYER entry names dataset {payload.dataset!r}, which is not its dataset {home!r}; "
            "ignored")))
        return metric, False
    if not entry.matched:
        if payload.metric.expression != metric.expression:
            notes.append(ExtensionNote(kind="metric", entity=metric.name, message=f"{label}: {STALE}; its formula "
                                       "is read from the document"))
        return metric.model_copy(update={"label": payload.metric.label, "meta": payload.metric.meta}), False
    restored = payload.metric.model_copy(update={"name": metric.name,
                                                 "description": metric.description or payload.metric.description})
    return restored, True


def restore_metrics(datasets: dict[str, SlayerDataset], *, restored: list[tuple[str, DatasetMetric]],
                    dialect: str) -> None:
    """Revert each restored metric (``(dataset, document form)``) that does not bind, with a note."""
    peers = list(datasets.values())
    for home, core in restored:
        dataset = datasets[home]
        i, metric = next((i, m) for i, m in enumerate(dataset.metrics) if m.name == core.name)
        try:
            bind_dataset_expression(metric.text, dataset=dataset, datasets=peers, dialect=dialect)
        except Exception as exc:  # noqa: BLE001 — any binding failure keeps the document form
            dataset.metrics[i] = core
            dataset.extension_notes.append(ExtensionNote(kind="metric", entity=metric.name, message=(
                f"metric {metric.name!r}: its SLAYER formula {metric.text!r} does not bind "
                f"({str(exc).splitlines()[0]}); ignored")))


def relationship_declarations(rel: Any, entry: Entry | None, *, datasets: dict[str, SlayerDataset],
                              default: DatasetRelationship, notes: list[ExtensionNote]) -> list[tuple[str, DatasetRelationship]]:
    """The ``(dataset, relationship)`` declarations ``rel`` stands for: its entry's when usable, else the default."""
    fallback = [(rel.from_dataset, default)]
    if entry is None:
        return fallback
    if not entry.matched:
        notes.append(ExtensionNote(kind="relationship", entity=rel.name,
                                   message=f"relationship {rel.name!r}: {STALE}; read as a many-to-one relationship"))
        return fallback
    out = []
    for declaration in entry.payload.declarations:
        if declaration.dataset not in datasets or declaration.relationship.to not in datasets:
            notes.append(ExtensionNote(kind="relationship", entity=rel.name, message=(
                f"relationship {rel.name!r}: its SLAYER entry declares it on {declaration.dataset!r} to "
                f"{declaration.relationship.to!r}, which are not both in the document; read as a many-to-one "
                "relationship")))
            return fallback
        out.append((declaration.dataset, declaration.relationship.model_copy()))
    return out


def source_ossie(source: str, *, reader: str) -> str:
    return source_digest(source, reader=reader)
