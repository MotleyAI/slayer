"""A SLayer dataset written into its owning Ossie document: Ossie-expressible content as Ossie core, everything else as
``SLAYER`` non-portable extension entries, so portable content never acquires a SLayer-only meaning."""

from __future__ import annotations

import copy
import itertools
from typing import Any

from slayer.core import slayer_extension as ext
from slayer.core.enums import DataType, JoinType, RelationshipCardinality
from slayer.core.field_expression import Expression, ExpressionObject
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.core.ossie_expression import OSIExpression
from slayer.core.semantic_model import OssieRelationship, ossie_validity_errors
from slayer.sql.ossie_dialects import expression_digest, source_digest
from slayer.sql.ossie_source import parse_source
from slayer.storage.document_extensions import restored_relationship

__all__ = ["OSSIE_CORE_TYPES", "as_expression", "core_source", "dataset_payload", "document_payload", "field_payload", "ossie_expression", "relationship_name",
           "with_dataset", "with_document_payload", "without_dataset"]

OSSIE_CORE_TYPES = frozenset({"String", "Integer", "Decimal", "Float", "Boolean", "Date", "Time", "DateTime",
                              "DateTimeTz", "Opaque"})
def core_datatype(datatype: DataType | None) -> str | None:
    """A field's Ossie core ``datatype``: its own when Ossie has it, ``Opaque`` for a list (its ``SLAYER`` entry keeps
    the list type)."""
    if datatype is None:
        return None
    return datatype.value if datatype.value in OSSIE_CORE_TYPES else "Opaque"


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


def _edge_identity(relationship: DatasetRelationship) -> tuple[str | None, str | None, tuple[tuple[str, str], ...]]:
    """A dataset's relationship as native traversal tells it apart: its name, else its target and key pairs."""
    if relationship.name is not None:
        return _fold(relationship.name), None, ()
    return None, _fold(relationship.to), tuple(relationship.column_pairs)


def _free_name(base: str, *, taken: set[str]) -> str:
    """``base``, else the first ``base_<n>`` no relationship of the document holds."""
    candidates = (base if n == 1 else f"{base}_{n}" for n in itertools.count(1))
    return next(c for c in candidates if _fold(c) not in taken)


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


def _slayer_payload(entity: dict[str, Any]) -> ext.Payload | None:
    """``entity``'s decoded ``SLAYER`` payload; ``None`` when it has none or it does not decode."""
    data = _slayer_data(entity)
    try:
        return ext.decode(data) if data is not None else None
    except ValueError:
        return None


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

    def __init__(self, doc: dict[str, Any], *, old: SlayerDataset | None, new: SlayerDataset, reader: str,
                 core: bool = True) -> None:
        self.doc = copy.deepcopy(doc)
        #: Whether Ossie core may change; otherwise everything new or changed goes to SLAYER entries.
        self.core = core
        self.old = old
        self.new = new
        self.reader = reader
        self.entry: dict[str, Any] | None = _find(self.doc["datasets"], old.name if old is not None else new.name)
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
        if not self.core:
            return False
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

    def _write_core_field(self, doc: dict[str, Any], *, field: DatasetField, old: DatasetField | None,
                          expression: dict[str, Any]) -> None:
        name = field.name
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
            _set(entity, "datatype", core_datatype(field.datatype))
        _set_slayer(entity, field_payload(field, core_expression=entity["expression"]))

    def _core_field(self, field: DatasetField) -> bool:
        old = self._old_field(field.name)
        name = field.name
        expression = ossie_expression(field.expression if field.expression is not None else name)
        if old is not None and field == old:
            return _find(self.entry.get("fields"), name) is not None  # type: ignore[union-attr]
        if self._try(lambda doc: self._write_core_field(doc, field=field, old=old, expression=expression)):
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

    def _relocated(self) -> dict[str, tuple[dict[str, Any], dict[str, Any]]]:
        """Another dataset's core metrics whose ``SLAYER`` entry restores them on this dataset (an export's relocated
        metrics), by case-folded restored name: ``(that dataset's entry, the metric)``."""
        out: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        for dataset in self.doc["datasets"]:
            if _fold(dataset.get("name")) == _fold(self.new.name):
                continue
            for metric in dataset.get("metrics") or []:
                payload = _slayer_payload(metric)
                if isinstance(payload, ext.MetricPayload) and _fold(payload.dataset) == _fold(self.new.name):
                    out[_fold(payload.metric.name)] = (dataset, metric)
        return out

    def _old_metric(self, name: str) -> DatasetMetric | None:
        return next((m for m in self.old.metrics if _fold(m.name) == _fold(name)), None) if self.old else None

    def _metrics(self) -> None:
        gone = self._removed([m.name for m in self.old.metrics] if self.old else [], [m.name for m in self.new.metrics])
        relocated = self._relocated()
        changed = {_fold(m.name) for m in self.new.metrics if m != self._old_metric(m.name)}
        for name in (gone | changed) & set(relocated):
            dataset, held = relocated.pop(name)
            _set(dataset, "metrics", [m for m in dataset.get("metrics") or [] if m is not held])
        entry = self.entry
        assert entry is not None
        _set(entry, "metrics", [m for m in entry.get("metrics") or [] if _fold(m.get("name")) not in gone])
        for metric in self.new.metrics:
            old = self._old_metric(metric.name)
            if metric == old and (metric.load_warning is not None or _fold(metric.name) in relocated):
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

        if self._try(lambda doc: self._write_core_metric(doc, metric=metric, old=old, expression=expression,
                                                         slayer_only=slayer_only)):
            return True
        if held is not None:
            _set_slayer(held, self._metric_entry(metric, core=held["expression"]))
            return True
        return False

    def _write_core_metric(self, doc: dict[str, Any], *, metric: DatasetMetric, old: DatasetMetric | None,
                           expression: dict[str, Any], slayer_only: bool) -> None:
        name = metric.name
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

    def _metric_entry(self, metric: DatasetMetric, *, core: dict[str, Any]) -> dict[str, Any]:
        return ext.encode(ext.MetricPayload(entity=metric.name, dataset=self.new.name, metric=metric),
                          ossie=expression_digest(OSIExpression.model_validate(core)))

    # ---- relationships ----

    def _restored(self, rel: dict[str, Any]) -> tuple[str, DatasetRelationship]:
        """The ``(dataset, relationship)`` a load restores document relationship ``rel`` as."""
        return restored_relationship(OssieRelationship.model_validate(rel), notes=[],
                                     datasets=[d["name"] for d in self.doc.get("datasets") or []])

    def _relationships(self) -> None:
        if self.old is not None and self.new.relationships == self.old.relationships:
            return
        name = self.new.name
        rels: list[dict[str, Any]] = []
        mine: dict[tuple[str | None, str | None, tuple[tuple[str, str], ...]], list[dict[str, Any]]] = {}
        for rel in self.doc.get("relationships") or []:
            owner, restored = self._restored(rel)
            if _fold(owner) == _fold(name):
                mine.setdefault(_edge_identity(restored), []).append(rel)
            else:
                rels.append(rel)
        self.doc["relationships"] = rels
        taken = {_fold(r.get("name")) for r in [*rels, *(h for held in mine.values() for h in held)]}
        for relationship in self.new.relationships:
            queue = mine.get(_edge_identity(relationship))
            held = queue.pop(0) if queue else None
            rel_name = held["name"] if held is not None else _free_name(
                relationship_name(src=name, relationship=relationship), taken=taken)
            taken.add(_fold(rel_name))
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
                   and relationship.meta is None and relationship.name == rel_name)
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


def _own_meta(meta: dict[str, Any] | None) -> dict[str, Any] | None:
    """``meta`` without the ``osi_*`` keys a load reads from the document's own content."""
    kept = {k: v for k, v in (meta or {}).items() if not k.startswith("osi_")}
    return kept or None


def _without_ossie_meta(dataset: SlayerDataset) -> SlayerDataset:
    return dataset.model_copy(update={
        "meta": _own_meta(dataset.meta),
        "fields": [f.model_copy(update={"meta": _own_meta(f.meta)}) for f in dataset.fields],
        "metrics": [m.model_copy(update={"meta": _own_meta(m.meta)}) for m in dataset.metrics],
        "relationships": [r.model_copy(update={"meta": _own_meta(r.meta)}) for r in dataset.relationships],
    })


def with_dataset(doc: dict[str, Any], *, old: SlayerDataset | None, new: SlayerDataset, reader: str,
                 core: bool = True) -> dict[str, Any]:
    """``doc`` with ``new`` written over ``old`` (its decomposed current form; ``None`` adds it); ``core=False`` keeps
    Ossie core as it is, writing every change as ``SLAYER`` entries. The ``osi_*`` metadata keys are never written."""
    return _Writer(doc, old=_without_ossie_meta(old) if old is not None else None, new=_without_ossie_meta(new),
                   reader=reader, core=core).run()


def document_payload(doc: dict[str, Any]) -> ext.DocumentPayload:
    """``doc``'s document-level ``SLAYER`` payload (empty when it has none)."""
    payload = _slayer_payload(doc)
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
