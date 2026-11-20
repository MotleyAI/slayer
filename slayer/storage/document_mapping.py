"""A SLayer dataset written into its owning Ossie document: Ossie-expressible content as Ossie core, everything else as
``SLAYER`` non-portable extension entries, so portable content never acquires a SLayer-only meaning."""

from __future__ import annotations

import copy
from typing import Any

from slayer.core import slayer_extension as ext
from slayer.core.enums import JoinType, RelationshipCardinality
from slayer.core.field_expression import Expression, ExpressionObject
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.core.ossie_expression import OSIExpression
from slayer.core.semantic_model import ossie_validity_errors
from slayer.sql.ossie_dialects import expression_digest, source_digest
from slayer.sql.ossie_source import parse_source

__all__ = ["OSSIE_CORE_TYPES", "as_expression", "core_source", "dataset_payload", "document_payload", "field_payload", "ossie_expression", "relationship_name",
           "with_dataset", "with_document_payload", "without_dataset"]

OSSIE_CORE_TYPES = frozenset({"String", "Integer", "Decimal", "Float", "Boolean", "Date", "Time", "DateTime",
                              "DateTimeTz", "Opaque"})
_FIELD_ONLY = ("hidden", "format", "granularity", "allowed_aggregations", "meta", "filter", "element_type", "db_type",
               "sampled", "sampled_values", "distinct_count")


def ossie_expression(expression: Expression) -> dict[str, Any]:
    if isinstance(expression, str):
        return {"sql": expression}
    out: dict[str, Any] = {}
    if expression.sql is not None:
        out["sql"] = expression.sql
    if expression.dialects:
        out["dialects"] = [{"dialect": d.dialect, "expression": d.expression} for d in expression.dialects]
    return out


def as_expression(ossie: dict[str, Any]) -> Expression:
    """An Ossie expression object as SLayer's expression (the shorthand when it is only portable ``sql``)."""
    if set(ossie) == {"sql"}:
        return ossie["sql"]
    return ExpressionObject.model_validate(ossie)


def relationship_name(*, src: str, relationship: DatasetRelationship) -> str:
    return relationship.name or f"{src}_{relationship.to}"


def _fold(name: Any) -> str:
    return str(name).casefold()


def _find(items: list[dict[str, Any]] | None, name: str) -> dict[str, Any] | None:
    return next((i for i in items or [] if _fold(i.get("name")) == _fold(name)), None)


def _set(entity: dict[str, Any], key: str, value: Any) -> None:
    if value is None or value == []:
        entity.pop(key, None)
    else:
        entity[key] = value


def _set_slayer(entity: dict[str, Any], entry: dict[str, Any] | None) -> None:
    """Replace ``entity``'s ``SLAYER`` entry with ``entry`` (none when ``None``), keeping foreign entries."""
    kept = [e for e in entity.get("nonportable_extensions") or [] if not ext.is_slayer(e)]
    _set(entity, "nonportable_extensions", [*kept, entry] if entry is not None else kept)


def _slayer_data(entity: dict[str, Any]) -> dict[str, Any] | None:
    found = [e for e in entity.get("nonportable_extensions") or [] if ext.is_slayer(e)]
    return found[0].get("data") if len(found) == 1 and isinstance(found[0].get("data"), dict) else None


def field_payload(field: DatasetField, *, core_expression: dict[str, Any]) -> dict[str, Any] | None:
    """``field``'s SLayer-only attributes as a ``SLAYER`` entry; ``None`` when it has none."""
    own = ossie_expression(field.expression) if field.expression is not None else None
    content = ext.FieldContent(
        expression=field.expression if own is not None and own != core_expression else None,
        datatype=field.datatype if field.datatype is not None and field.datatype.value not in OSSIE_CORE_TYPES else None,
        **{k: getattr(field, k) for k in _FIELD_ONLY},
    )
    if content == ext.FieldContent():
        return None
    return ext.encode(ext.FieldPayload(entity=field.name, field=content),
                      ossie=expression_digest(OSIExpression.model_validate(core_expression)))


def core_source(source: str) -> tuple[str | None, str | None]:
    """``(sql_table, sql)`` an Ossie ``source`` loads as."""
    parsed = parse_source(source)
    return (None, parsed.query) if parsed.is_query else (source.strip(), None)


def dataset_payload(dataset: SlayerDataset, *, source: str, reader: str, fields: list[DatasetField],
                    metrics: list[DatasetMetric], relationships: list[DatasetRelationship]) -> dict[str, Any] | None:
    """``dataset``'s SLayer-only attributes and members as a ``SLAYER`` entry; ``None`` when it has none."""
    structure = ext.SourceStructure()
    if dataset.source_queries or dataset.filters or (dataset.sql_table, dataset.sql) != core_source(source):
        structure = ext.SourceStructure(
            sql_table=dataset.sql_table, sql=dataset.sql, filters=list(dataset.filters) or None,
            source_queries=[q.model_dump(mode="json", exclude_none=True) for q in dataset.source_queries or []] or None,
            query_variables=dict(dataset.query_variables or {}),
        )
    payload = ext.DatasetPayload(
        entity=dataset.name, source=structure, hidden=dataset.hidden, access_tags=list(dataset.access_tags),
        default_time_dimension=dataset.default_time_dimension, meta=dataset.meta,
        aggregations=list(dataset.aggregations), fields=fields, metrics=metrics, relationships=relationships,
    )
    if payload == ext.DatasetPayload(entity=dataset.name, source=ext.SourceStructure()):
        return None
    return ext.encode(payload, ossie=source_digest(source, reader=reader))


class _Writer:
    """Writes one dataset into a copy of a document."""

    def __init__(self, doc: dict[str, Any], *, old: SlayerDataset | None, new: SlayerDataset, reader: str) -> None:
        self.doc = copy.deepcopy(doc)
        self.old = old
        self.new = new
        self.reader = reader
        self.entry = _find(self.doc["datasets"], old.name if old is not None else new.name)
        if self.entry is None:
            self.entry = {"name": new.name, "source": new.sql_table or new.sql or new.name}
            self.doc["datasets"].append(self.entry)
        payload = ext.decode(data) if (data := _slayer_data(self.entry)) is not None else None
        held = payload if isinstance(payload, ext.DatasetPayload) else None
        self.extra_fields = {_fold(f.name) for f in held.fields} if held else set()
        self.extra_metrics = {_fold(m.name) for m in held.metrics} if held else set()
        self.slayer_fields: list[DatasetField] = []
        self.slayer_metrics: list[DatasetMetric] = []
        self.slayer_relationships: list[DatasetRelationship] = []

    def _try(self, apply: Any) -> bool:
        """Apply ``apply`` to the document when it adds no Ossie validity error; ``False`` (unchanged) otherwise."""
        trial = copy.deepcopy(self.doc)
        apply(trial)
        if not set(ossie_validity_errors(trial)) <= set(ossie_validity_errors(self.doc)):
            return False
        self.doc.clear()
        self.doc.update(trial)
        self.entry = _find(self.doc["datasets"], self.new.name)
        return True

    def run(self) -> dict[str, Any]:
        entry = self.entry
        assert entry is not None
        if self.old is None or self.new.description != self.old.description:
            _set(entry, "description", self.new.description)
        if self.old is not None and (self.new.sql_table, self.new.sql) != (self.old.sql_table, self.old.sql) and (
                self.new.sql_table or self.new.sql):
            entry["source"] = self.new.sql_table or self.new.sql
        self._fields()
        self._keys()
        self._metrics()
        self._relationships()
        entry = self.entry
        assert entry is not None
        _set_slayer(entry, dataset_payload(self.new, source=entry["source"], reader=self.reader,
                                           fields=self.slayer_fields, metrics=self.slayer_metrics,
                                           relationships=self.slayer_relationships))
        return self.doc

    @staticmethod
    def _removed(before: list[str], after: list[str]) -> set[str]:
        """Case-folded names the save removes: read before, absent after."""
        return {_fold(n) for n in before} - {_fold(n) for n in after}

    # ---- fields ----

    def _old_field(self, name: str) -> DatasetField | None:
        return self.old.get_field(name) if self.old is not None else None

    def _fields(self) -> None:
        gone = self._removed([f.name for f in self.old.fields] if self.old else [], [f.name for f in self.new.fields])
        entry = self.entry
        assert entry is not None
        _set(entry, "fields", [f for f in entry.get("fields") or [] if _fold(f.get("name")) not in gone])
        for field in self.new.fields:
            if field.load_warning is not None and field == self._old_field(field.name):
                continue
            if _fold(field.name) in self.extra_fields or not self._core_field(field):
                self.slayer_fields.append(field)

    def _core_field(self, field: DatasetField) -> bool:
        old = self._old_field(field.name)
        name = field.name
        expression = ossie_expression(field.expression if field.expression is not None else name)

        def apply(doc: dict[str, Any]) -> None:
            entity = _find(_find(doc["datasets"], self.new.name).setdefault("fields", []), name)  # type: ignore[union-attr]
            if entity is None:
                entity = {"name": name, "expression": expression}
                _find(doc["datasets"], self.new.name)["fields"].append(entity)  # type: ignore[index]
            elif old is None or field.expression != old.expression:
                entity["expression"] = expression
            if old is None or field.label != old.label:
                _set(entity, "label", field.label)
            if old is None or field.description != old.description:
                _set(entity, "description", field.description)
            if old is None or field.datatype != old.datatype:
                core_type = field.datatype.value if field.datatype is not None else None
                _set(entity, "datatype", core_type if core_type in OSSIE_CORE_TYPES else None)
            _set_slayer(entity, field_payload(field, core_expression=entity["expression"]))

        if old is not None and field == old:
            return _find(self.entry.get("fields"), name) is not None  # type: ignore[union-attr]
        if self._try(apply):
            return True
        held = _find(self.entry.get("fields"), name)  # type: ignore[union-attr]
        if held is None:
            return False
        # Held in core but not valid there: the core keeps its text, the SLAYER entry the whole definition.
        content = ext.FieldContent(expression=field.expression, datatype=field.datatype,
                                   **{k: getattr(field, k) for k in _FIELD_ONLY})
        _set_slayer(held, ext.encode(ext.FieldPayload(entity=name, field=content),
                                     ossie=expression_digest(OSIExpression.model_validate(held["expression"]))))
        return True

    def _keys(self) -> None:
        if self.old is not None and [(f.name, f.primary_key, f.unique) for f in self.new.fields] == [
                (f.name, f.primary_key, f.unique) for f in self.old.fields]:
            return
        core = {_fold(f["name"]) for f in self.entry.get("fields") or []}  # type: ignore[union-attr]
        primary = [f.name for f in self.new.fields if f.primary_key and _fold(f.name) in core]
        singles = [[f.name] for f in self.new.fields if f.unique and _fold(f.name) in core]
        composite = [k for k in self.entry.get("unique_keys") or [] if len(k) > 1]  # type: ignore[union-attr]

        def apply(doc: dict[str, Any]) -> None:
            entity = _find(doc["datasets"], self.new.name)
            assert entity is not None
            _set(entity, "primary_key", primary or None)
            _set(entity, "unique_keys", [*singles, *composite] or None)

        self._try(apply)

    # ---- metrics ----

    def _metrics(self) -> None:
        gone = self._removed([m.name for m in self.old.metrics] if self.old else [], [m.name for m in self.new.metrics])
        entry = self.entry
        assert entry is not None
        _set(entry, "metrics", [m for m in entry.get("metrics") or [] if _fold(m.get("name")) not in gone])
        for metric in self.new.metrics:
            old = next((m for m in self.old.metrics if _fold(m.name) == _fold(metric.name)), None) if self.old else None
            if metric.load_warning is not None and metric == old:
                continue
            if _fold(metric.name) in self.extra_metrics or not self._core_metric(metric, old=old):
                self.slayer_metrics.append(metric)

    def _core_metric(self, metric: DatasetMetric, *, old: DatasetMetric | None) -> bool:
        name = metric.name
        held = _find(self.entry.get("metrics"), name)  # type: ignore[union-attr]
        if old is not None and metric == old:
            return held is not None
        expression = ossie_expression(metric.expression)
        slayer_only = metric.label is not None or metric.meta is not None or (
            metric.datatype is not None and metric.datatype.value not in OSSIE_CORE_TYPES)

        def apply(doc: dict[str, Any]) -> None:
            home = _find(doc["datasets"], self.new.name)
            assert home is not None
            entity = _find(home.setdefault("metrics", []), name)
            if entity is None:
                entity = {"name": name, "expression": expression}
                home["metrics"].append(entity)
            elif old is None or metric.expression != old.expression:
                entity["expression"] = expression
            if old is None or metric.description != old.description:
                _set(entity, "description", metric.description)
            if old is None or metric.datatype != old.datatype:
                core_type = metric.datatype.value if metric.datatype is not None else None
                _set(entity, "datatype", core_type if core_type in OSSIE_CORE_TYPES else None)
            _set_slayer(entity, self._metric_entry(metric, core=entity["expression"]) if slayer_only else None)

        if self._try(apply):
            return True
        if held is not None:
            _set_slayer(held, self._metric_entry(metric, core=held["expression"]))
            return True
        return False

    def _metric_entry(self, metric: DatasetMetric, *, core: dict[str, Any]) -> dict[str, Any]:
        return ext.encode(ext.MetricPayload(entity=metric.name, dataset=self.new.name, metric=metric),
                          ossie=expression_digest(OSIExpression.model_validate(core)))

    # ---- relationships ----

    def _declared(self, rel: dict[str, Any]) -> bool:
        """``rel`` is one of this dataset's: declared on it by default or by its ``SLAYER`` entry."""
        data = _slayer_data(rel)
        if data is not None:
            try:
                payload = ext.decode(data)
            except ValueError:
                payload = None
            if isinstance(payload, ext.RelationshipPayload):
                return any(_fold(d.dataset) == _fold(self.new.name) for d in payload.declarations)
        return _fold(rel.get("from")) == _fold(self.new.name)

    def _relationships(self) -> None:
        if self.old is not None and self.new.relationships == self.old.relationships:
            return
        name = self.new.name
        rels = [r for r in self.doc.get("relationships") or [] if not self._declared(r)]
        mine = {_fold(r.get("name")): r for r in self.doc.get("relationships") or [] if self._declared(r)}
        self.doc["relationships"] = rels
        for relationship in self.new.relationships:
            rel_name = relationship_name(src=name, relationship=relationship)
            held = mine.get(_fold(rel_name))
            if not self._core_relationship(relationship, rel_name=rel_name, held=held):
                self.slayer_relationships.append(relationship)
        if not self.doc["relationships"]:
            self.doc.pop("relationships")

    def _core_relationship(self, relationship: DatasetRelationship, *, rel_name: str,
                           held: dict[str, Any] | None) -> bool:
        name = self.new.name
        sources = [a for a, _ in relationship.column_pairs]
        targets = [b for _, b in relationship.column_pairs]
        default = (relationship.cardinality in (None, RelationshipCardinality.MANY_TO_ONE)
                   and relationship.join_type is JoinType.LEFT and relationship.description is None
                   and relationship.meta is None and relationship.name is not None)
        shapes = [(name, relationship.to, sources, targets), (relationship.to, name, targets, sources)]
        if relationship.cardinality is RelationshipCardinality.ONE_TO_MANY:
            shapes.reverse()
        for src, to, from_columns, to_columns in shapes:
            plain = default and src == name
            entity = {**(held or {}), "name": rel_name, "from": src, "to": to, "from_columns": list(from_columns),
                      "to_columns": list(to_columns)}
            entry = None if plain else ext.encode(
                ext.RelationshipPayload(entity=rel_name, declarations=[
                    ext.RelationshipDeclaration(dataset=name, relationship=relationship)]),
                ossie=ext.relationship_digest(from_dataset=src, to=to, from_columns=list(from_columns),
                                              to_columns=list(to_columns)))
            _set_slayer(entity, entry)
            if self._try(lambda doc, e=entity: doc.setdefault("relationships", []).append(e)):
                return True
        return False


def with_dataset(doc: dict[str, Any], *, old: SlayerDataset | None, new: SlayerDataset, reader: str) -> dict[str, Any]:
    """``doc`` with ``new`` written over ``old`` (its decomposed current form; ``None`` adds it)."""
    return _Writer(doc, old=old, new=new, reader=reader).run()


def document_payload(doc: dict[str, Any]) -> ext.DocumentPayload:
    """``doc``'s document-level ``SLAYER`` payload (empty when it has none)."""
    data = _slayer_data(doc)
    try:
        payload = ext.decode(data) if data is not None else None
    except ValueError:
        payload = None
    return payload if isinstance(payload, ext.DocumentPayload) else ext.DocumentPayload(entity=str(doc.get("name")))


def with_document_payload(doc: dict[str, Any], payload: ext.DocumentPayload) -> None:
    empty = payload.model_copy(update={"metrics": []}) == ext.DocumentPayload(entity=payload.entity)
    _set_slayer(doc, None if empty and not payload.metrics else ext.encode(payload, ossie=doc.get("name")))


def without_dataset(doc: dict[str, Any], *, name: str, reads: Any) -> dict[str, Any]:
    """``doc`` without dataset ``name`` and the relationships touching it; each model-scoped metric reading it (per
    ``reads(expression) -> set of case-folded dataset names``) moves to the document's ``SLAYER`` entry."""
    out = copy.deepcopy(doc)
    out["datasets"] = [d for d in out["datasets"] if _fold(d.get("name")) != _fold(name)]
    rels = [r for r in out.get("relationships") or [] if _fold(name) not in (_fold(r.get("from")), _fold(r.get("to")))]
    _set(out, "relationships", rels or None)
    kept, moved = [], []
    for metric in out.get("metrics") or []:
        (moved if _fold(name) in reads(metric.get("expression") or {}) else kept).append(metric)
    if moved:
        _set(out, "metrics", kept or None)
        payload = document_payload(out)
        payload.metrics.extend(ext.ModelMetricContent(
            name=m["name"], expression=as_expression(m["expression"]),
            description=m.get("description"), datatype=m.get("datatype")) for m in moved)
        with_document_payload(out, payload)
    return out
