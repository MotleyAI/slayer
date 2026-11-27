"""A SLayer dataset written into its owning Ossie document: Ossie-expressible content as Ossie core, everything else as
``SLAYER`` non-portable extension entries, so portable content never acquires a SLayer-only meaning."""

from __future__ import annotations

import copy
import itertools
import warnings
from typing import Any

from pydantic import BaseModel
from sqlglot.errors import SqlglotError

from slayer.core import slayer_extension as ext
from slayer.core.enums import DataType, JoinType, RelationshipCardinality
from slayer.core.field_expression import DialectEntry, Expression, ExpressionObject, token
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.core.ossie_steps import ossie_from
from slayer.core.ossie_expression import OSIExpression
from slayer.core.semantic_model import OssieExpression, OssieRelationship, ossie_validity_errors
from slayer.core.deprecation import outside_stacklevel
from slayer.core.warnings import NormalizationWarning, SlayerNormalizationWarning
from slayer.sql.naming import as_sql_dialect
from slayer.sql.ossie_dialects import ANSI_SQL, OSSIE_SQL, Candidate, expression_digest, read, reader_for, render, source_digest
from slayer.sql.ossie_source import ossie_source, parse_source, source_table
from slayer.storage.document_extensions import restored_relationship
from slayer.storage.ossie_render import Admission

__all__ = ["OSSIE_CORE_TYPES", "as_expression", "core_source", "dataset_payload", "document_payload", "field_payload",
           "ossie_expression", "portable_form", "relationship_name", "runnable_field", "stored_source", "with_dataset",
           "with_document_payload", "without_dataset"]

OSSIE_CORE_TYPES = frozenset({"String", "Integer", "Decimal", "Float", "Boolean", "Date", "Time", "DateTime",
                              "DateTimeTz", "Opaque"})
def core_datatype(datatype: DataType | None) -> str | None:
    """A field's Ossie core ``datatype``: its own when Ossie has it, ``Opaque`` for a list (its ``SLAYER`` entry keeps
    the list type)."""
    if datatype is None:
        return None
    return datatype.value if datatype.value in OSSIE_CORE_TYPES else "Opaque"


_FIELD_ONLY = ("hidden", "format", "granularity", "time_zone", "allowed_aggregations", "meta", "filter", "element_type",
               "db_type", "sampled", "sampled_values", "distinct_count")


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


def _held_entities(relationships: list[DatasetRelationship],
                   held: list[tuple[DatasetRelationship, dict[str, Any]]]) -> list[dict[str, Any] | None]:
    """Each relationship's document entity: the one restoring it by identity, else (an unnamed relationship whose keys
    changed) a leftover unnamed one to the same target, in order; ``None`` for a new relationship."""
    left = list(held)
    out: list[dict[str, Any] | None] = []
    for relationship in relationships:
        found = next((h for h in left if _edge_identity(h[0]) == _edge_identity(relationship)), None)
        if found is not None:
            left.remove(found)
        out.append(found[1] if found is not None else None)
    for i, relationship in enumerate(relationships):
        found = next((h for h in left if out[i] is None and relationship.name is None and h[0].name is None
                      and _fold(h[0].to) == _fold(relationship.to)), None)
        if found is not None:
            left.remove(found)
            out[i] = found[1]
    return out


def _free_name(base: str, *, taken: set[str]) -> str:
    """``base``, else the first ``base_<n>`` no relationship of the document holds."""
    candidates = (base if n == 1 else f"{base}_{n}" for n in itertools.count(1))
    return next(c for c in candidates if _fold(c) not in taken)


def _find(items: list[dict[str, Any]] | None, name: str) -> dict[str, Any] | None:
    """The item spelled ``name`` exactly, else the one matching it ignoring case."""
    return next((i for i in items or [] if i.get("name") == name), None) or next(
        (i for i in items or [] if _fold(i.get("name")) == _fold(name)), None)


def _member(items: list[dict[str, Any]] | None, name: str) -> dict[str, Any] | None:
    """The field or metric entity spelled ``name`` exactly: a case twin is another entity."""
    return next((i for i in items or [] if i.get("name") == name), None)


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


def portable_form(expression: OssieExpression) -> str | None:
    """An expression's portable SQL: its ``sql``, else its ``OSSIE_SQL`` entry."""
    if expression.sql:
        return expression.sql
    return next((d.expression for d in expression.dialects or [] if token(d.dialect) == OSSIE_SQL), None)


def runnable_field(expression: OssieExpression, *, target: str) -> tuple[Expression, str | None]:
    """A field ``expression`` as SLayer runs it on ``target`` (an Ossie dialect token), and why it cannot run, if so.
    One with no portable or target entry gets an in-memory ``target`` entry translated from a readable one."""
    entries = [DialectEntry(dialect=d.dialect, expression=d.expression) for d in expression.dialects or []]
    if expression.sql is not None and not entries:
        return expression.sql, None
    obj = ExpressionObject(sql=expression.sql, dialects=entries or None)
    if portable_form(expression) is not None or any(token(e.dialect) == target for e in entries):
        return obj, None
    readable = [e for e in entries if reader_for(e.dialect) is not None]
    ordered = [e for e in readable if token(e.dialect) == ANSI_SQL] + [e for e in readable
                                                                     if token(e.dialect) != ANSI_SQL]
    target_reader = reader_for(target)
    for entry in ordered:
        candidate = Candidate(dialect=entry.dialect, text=entry.expression)
        try:
            text = render(candidate, read(candidate), target=target_reader) if target_reader else entry.expression
        except SqlglotError:
            continue
        translated = DialectEntry(dialect=target, expression=text)
        return obj.model_copy(update={"dialects": [*entries, translated]}), None
    found = ", ".join(dict.fromkeys(e.dialect for e in entries)) or "none"
    return obj, f"has no readable SQL expression (dialects: {found})"


def _loaded_form(core: dict[str, Any], *, target: str) -> Expression | None:
    """The expression a load reads core expression ``core`` as on ``target``; ``None`` when it does not read."""
    try:
        return runnable_field(OssieExpression.model_validate(core), target=target)[0]
    except ValueError:
        return None


def field_payload(field: DatasetField, *, core_expression: dict[str, Any], target: str) -> dict[str, Any] | None:
    """``field``'s SLayer-only attributes as a ``SLAYER`` entry; ``None`` when it has none. An expression that is only
    the core one as a load reads it on ``target`` is the core's, never written."""
    own = ossie_expression(field.expression) if field.expression is not None else None
    restated = own is None or own == core_expression or field.expression == _loaded_form(core_expression,
                                                                                          target=target)
    content = ext.FieldContent(
        expression=None if restated else field.expression,
        datatype=field.datatype if field.datatype is not None and field.datatype.value not in OSSIE_CORE_TYPES else None,
        **{k: getattr(field, k) for k in _FIELD_ONLY},
    )
    if content == ext.FieldContent():
        return None
    return ext.encode(ext.FieldPayload(entity=field.name, field=content),
                      ossie=expression_digest(OSIExpression.model_validate(core_expression)))


def core_source(source: str, *, reader: str | None = None) -> tuple[str | None, str | None]:
    """``(sql_table, sql)`` an Ossie ``source`` loads as on a ``reader`` (sqlglot name) database: a table read by
    Ossie's identifier rule, spelled for SLayer's table-name rule."""
    dialect = as_sql_dialect(reader)
    parsed = parse_source(source, dialect=dialect)
    return (None, parsed.query) if parsed.is_query else (source_table(source, dialect=dialect), None)


def stored_source(dataset: SlayerDataset, *, reader: str | None) -> str:
    """The core ``source`` a writer stores for ``dataset``: its table spelled for Ossie's identifier rule, its SQL, or
    (query-backed) its name."""
    if dataset.sql_table:
        return ossie_source(dataset.sql_table, dialect=as_sql_dialect(reader))
    return dataset.sql or dataset.name


def _names_source(dataset: SlayerDataset, *, source: str, reader: str) -> bool:
    """Whether core ``source`` reads as ``dataset``'s own table or SQL."""
    sql_table, sql = core_source(source, reader=reader)
    if dataset.sql is not None or sql is not None:
        return (dataset.sql or "").strip() == (sql or "").strip() and not dataset.sql_table
    dialect = as_sql_dialect(reader)
    return dataset.sql_table is not None and sql_table is not None and ossie_source(
        dataset.sql_table, dialect=dialect) == ossie_source(sql_table, dialect=dialect)


def _stored_query(query: Any) -> dict[str, Any]:
    """A source query as written: the keys it was given, and its version."""
    if not isinstance(query, BaseModel):
        return dict(query)
    return {"version": getattr(query, "version"), **query.model_dump(mode="json", exclude_unset=True, exclude_none=True)}


def dataset_payload(dataset: SlayerDataset, *, source: str, reader: str, fields: list[DatasetField],
                    metrics: list[DatasetMetric], relationships: list[DatasetRelationship]) -> dict[str, Any] | None:
    """``dataset``'s SLayer-only attributes and members as a ``SLAYER`` entry; ``None`` when it has none."""
    structure = ext.SourceStructure()
    if dataset.source_kind is not None:
        structure = ext.SourceStructure(source_kind=dataset.source_kind)
    if dataset.source_queries or dataset.filters or dataset.query_variables or not _names_source(
            dataset, source=source, reader=reader):
        structure = ext.SourceStructure(
            sql_table=dataset.sql_table, source_kind=structure.source_kind, sql=dataset.sql,
            filters=list(dataset.filters) or None,
            source_queries=[_stored_query(q) for q in dataset.source_queries or []] or None,
            query_variables=dict(dataset.query_variables or {}),
        )
    payload = ext.DatasetPayload(
        entity=dataset.name, source=structure, hidden=dataset.hidden, access_tags=list(dataset.access_tags),
        default_time_dimension=dataset.default_time_dimension, meta=dataset.meta,
        aggregations=list(dataset.aggregations), fields=fields, metrics=metrics, relationships=relationships,
        enforced_keys=[list(k) for k in dataset.enforced_keys],
    )
    if payload == ext.DatasetPayload(entity=dataset.name, source=ext.SourceStructure()):
        return None
    return ext.encode(payload, ossie=source_digest(source, reader=reader))


def _lift(entity: dict[str, Any], meta: dict[str, Any] | None, *, keys: tuple[str, ...]) -> None:
    """Read ``meta``'s ``osi_<key>`` values (the 1.x importer's stash of Ossie content) into the new ``entity``: every
    stashed foreign extension core accepts; the rest stays in the stash."""
    for key in keys:
        value = (meta or {}).get(f"osi_{key}")
        if key == "nonportable_extensions":
            value = _valid_extensions(value)
        if value is None or value == []:
            continue
        if key == "is_time":
            entity["dimension"] = {"is_time": bool(value)}
        else:
            entity[key] = copy.deepcopy(value)


def _valid_extensions(value: Any) -> list[dict[str, Any]]:
    return [e for e in value if isinstance(e, dict) and ext.checked(vendor_name=e.get("vendor_name"),
                                                                   data=e.get("data")) is not None] \
        if isinstance(value, list) else []


def rejected_extensions(meta: dict[str, Any] | None) -> list[Any]:
    """The stashed foreign extensions of ``meta`` core rejects."""
    value = (meta or {}).get("osi_nonportable_extensions")
    valid = _valid_extensions(value)
    return [e for e in value if e not in valid] if isinstance(value, list) else []


class _Writer:
    """Writes one dataset into a copy of a document."""

    def __init__(self, doc: dict[str, Any], *, old: SlayerDataset | None, new: SlayerDataset, reader: str,
                 core: bool = True, lifted: SlayerDataset | None = None, dialect_known: bool = True,
                 admission: Admission | None = None) -> None:
        self.doc = copy.deepcopy(doc)
        #: Whether Ossie core may change; otherwise everything new or changed goes to SLAYER entries.
        self.core = core
        self.old = old
        self.new = new
        self.reader = reader
        self.target = reader.upper()
        #: Whether ``reader`` is the datasource's own dialect, not a default.
        self.dialect_known = dialect_known
        #: What core holds of the dataset, as the export renders it; ``None``: whatever is valid Ossie.
        self.admission = admission
        #: The dataset as given, its ``osi_*`` metadata read into each entity the save creates.
        self.lifted = lifted
        self.entry: dict[str, Any] | None = _find(self.doc["datasets"], old.name) if old is not None else next(
            (d for d in self.doc["datasets"] if d.get("name") == new.name), None)
        if self.entry is None:
            self.entry = {"name": new.name, "source": stored_source(new, reader=reader)}
            _lift(self.entry, lifted.meta if lifted is not None else None, keys=("ai_context", "nonportable_extensions"))
            self.doc["datasets"].append(self.entry)
        payload = ext.decode(data) if (data := _slayer_data(self.entry)) is not None else None
        held = payload if isinstance(payload, ext.DatasetPayload) else None
        self.extra_fields = {_fold(f.name) for f in held.fields} if held else set()
        self.extra_metrics = {_fold(m.name) for m in held.metrics} if held else set()
        self.slayer_fields: list[DatasetField] = []
        self.slayer_metrics: list[DatasetMetric] = []
        self.slayer_relationships: list[DatasetRelationship] = []
        self._lifted_meta: dict[int, dict[str, Any] | None] = {}

    def _keyed(self, dataset: str, keys: Any) -> bool:
        """Whether ``keys`` are the primary key or a unique key of ``dataset`` in the document."""
        entity = self.entry if _fold(dataset) == _fold(self.new.name) else _find(self.doc.get("datasets") or [], dataset)
        if not isinstance(entity, dict):
            return False
        wanted = {_fold(k) for k in keys}
        candidates = [entity.get("primary_key") or [], *(entity.get("unique_keys") or [])]
        return any(isinstance(c, list) and c and {_fold(str(k)) for k in c} == wanted for c in candidates)

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
            entry["source"] = stored_source(self.new, reader=self.reader)
        self._fields()
        self._keys()
        self._settle_stashed_keys()
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
        entity = _member(_find(doc["datasets"], self.new.name).setdefault("fields", []), name)  # type: ignore[union-attr]
        if entity is None:
            entity = {"name": name, "expression": expression}
            lifted = self.lifted.get_field(name) if self.lifted is not None else None
            _lift(entity, lifted.meta if lifted is not None else None,
                  keys=("ai_context", "is_time", "nonportable_extensions"))
            _find(doc["datasets"], self.new.name)["fields"].append(entity)  # type: ignore[index]
        elif old is None or field.expression != old.expression:
            entity["expression"] = expression
        if old is None or field.label != old.label:
            _set(entity, "label", field.label)
        if old is None or field.description != old.description:
            _set(entity, "description", field.description)
        if old is None or field.datatype != old.datatype:
            _set(entity, "datatype", core_datatype(field.datatype))
        _set_slayer(entity, field_payload(field, core_expression=entity["expression"], target=self.target))

    def _core_field(self, field: DatasetField) -> bool:
        old = self._old_field(field.name)
        name = field.name
        if self.admission is not None:
            if name not in self.admission.fields:
                if old is not None and field == old:  # unchanged: as stored
                    return _member(self.entry.get("fields"), name) is not None  # type: ignore[union-attr]
                _set(self.entry, "fields", [f for f in self.entry.get("fields") or []  # type: ignore[union-attr]
                                            if _fold(f.get("name")) != _fold(name)])
                return False
            expression = self.admission.fields[name]
        elif field.expression is None and field.sql is not None:  # a deprecated dialect ``sql``: its own entry
            if not self.dialect_known:
                return False
            expression = {"dialects": [{"dialect": self.target, "expression": field.sql}]}
        else:
            expression = ossie_expression(field.expression if field.expression is not None else name)
        if old is not None and field == old:
            return _member(self.entry.get("fields"), name) is not None  # type: ignore[union-attr]
        if self._try(lambda doc: self._write_core_field(doc, field=field, old=old, expression=expression)):
            return True
        held = _member(self.entry.get("fields"), name)  # type: ignore[union-attr]
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
        if self.admission is not None:
            admitted = self.admission
            primary = [n for n in admitted.primary_key if _fold(n) in core] if all(
                _fold(n) in core for n in admitted.primary_key) else []
            singles, composite = [], [list(k) for k in admitted.unique_keys if all(_fold(n) in core for n in k)]
        elif self.old is None and self.lifted is not None:
            for key in (self.lifted.meta or {}).get("osi_unique_keys") or []:
                if isinstance(key, list) and len(key) > 1 and list(key) not in composite and all(
                        _fold(n) in core for n in key):
                    composite.append(list(key))

        def apply(doc: dict[str, Any]) -> None:
            entity = _find(doc["datasets"], self.new.name)
            assert entity is not None
            _set(entity, "primary_key", primary or None)
            _set(entity, "unique_keys", [*singles, *composite] or None)

        self._try(apply)

    def _settle_stashed_keys(self) -> None:
        """Leave in the stash only the stashed unique keys core does not hold."""
        stash = (self.new.meta or {}).get("osi_unique_keys")
        if not stash:
            return
        entry = self.entry or {}
        held = [{_fold(str(n)) for n in k} for k in [entry.get("primary_key") or [], *(entry.get("unique_keys") or [])]
                if isinstance(k, list) and k]
        rest = [k for k in stash if not (isinstance(k, list) and {_fold(str(n)) for n in k} in held)]
        meta = {k: v for k, v in (self.new.meta or {}).items() if k != "osi_unique_keys"}
        self.new = self.new.model_copy(update={"meta": {**meta, "osi_unique_keys": rest} if rest else meta or None})

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
        held = _member(self.entry.get("metrics"), name)  # type: ignore[union-attr]
        if old is not None and metric == old:
            return held is not None
        authored = ossie_expression(metric.expression)
        if self.admission is not None and name not in self.admission.metrics:
            if held is not None:  # changed into SLayer-only: out of core
                _set(self.entry, "metrics", [m for m in self.entry.get("metrics") or []  # type: ignore[union-attr]
                                             if m is not held])
            return False
        expression = self.admission.metrics[name] if self.admission is not None else authored
        slayer_only = metric.label is not None or metric.meta is not None or expression != authored or (
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
        entity = _member(home.setdefault("metrics", []), name)
        if entity is None:
            entity = {"name": name, "expression": expression}
            lifted = next((m for m in self.lifted.metrics if _fold(m.name) == _fold(name)), None) \
                if self.lifted is not None else None
            _lift(entity, lifted.meta if lifted is not None else None, keys=("ai_context", "nonportable_extensions"))
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

    def _cross_model(self, relationships: list[DatasetRelationship]) -> list[DatasetRelationship]:
        """Write the relationships into another model's datasets to the document's ``SLAYER`` entry; the others."""
        name = self.new.name
        members = {_fold(d.get("name")) for d in self.doc["datasets"]}
        payload = document_payload(self.doc)
        held = [d for d in payload.relationships if _fold(d.dataset) != _fold(name)]
        # Core has no place for these, so their own Ossie content (``osi_*`` meta) travels with them.
        crossing = [r.model_copy(update={"meta": self._lifted_meta.get(id(r), r.meta)})
                    for r in relationships if _fold(r.to) not in members]
        payload.relationships = [*held, *(ext.RelationshipDeclaration(dataset=name, relationship=r) for r in crossing)]
        with_document_payload(self.doc, payload)
        return [r for r in relationships if _fold(r.to) in members]

    def _relationships(self) -> None:
        if self.old is not None and self.new.relationships == self.old.relationships:
            return
        name = self.new.name
        if self.lifted is not None:
            self._lifted_meta = {id(r): lifted.meta for r, lifted in zip(self.new.relationships,
                                                                        self.lifted.relationships)}
        within = self._cross_model(list(self.new.relationships))
        rels: list[dict[str, Any]] = []
        mine: list[tuple[DatasetRelationship, dict[str, Any]]] = []
        for rel in self.doc.get("relationships") or []:
            owner, restored = self._restored(rel)
            if _fold(owner) == _fold(name):
                mine.append((restored, rel))
            else:
                rels.append(rel)
        self.doc["relationships"] = rels
        taken = {_fold(r.get("name")) for r in [*rels, *(h for _, h in mine)]}
        for relationship, held in zip(within, _held_entities(within, mine)):
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
        default = (relationship.cardinality is RelationshipCardinality.MANY_TO_ONE and not relationship.trusted
                   and relationship.join_type is JoinType.LEFT and relationship.description is None
                   and relationship.meta is None and relationship.name == rel_name)
        if self.admission is not None:
            position = next((i for i, r in enumerate(self.new.relationships) if r is relationship), None)
            shape = self.admission.relationships.get(position) if position is not None else None
            if shape is None:
                return False
            shapes = [(shape[0], shape[1], shape[2], shape[3])]
        else:
            many = ossie_from(declaring=name, to=relationship.to, cardinality=relationship.cardinality,
                              declaring_keys=sources, to_keys=targets, keyed=self._keyed,
                              document_from=relationship.document_from)
            if relationship.cardinality is RelationshipCardinality.MANY_TO_MANY:  # no Ossie direction
                return False
            shapes = [(name, relationship.to, sources, targets), (relationship.to, name, targets, sources)]
            if many is not None and _fold(many) != _fold(name):
                shapes.reverse()
        for src, to, from_columns, to_columns in shapes:
            plain = default and src == name
            entity = {**(held or {}), "name": rel_name, "from": src, "to": to, "from_columns": list(from_columns),
                      "to_columns": list(to_columns)}
            if held is None:
                _lift(entity, self._lifted_meta.get(id(relationship)), keys=("ai_context", "nonportable_extensions"))
            entry = None if plain else ext.encode(
                ext.RelationshipPayload(entity=rel_name, declarations=[
                    ext.RelationshipDeclaration(dataset=name, relationship=relationship)]),
                ossie=ext.relationship_digest(from_dataset=src, to=to, from_columns=list(from_columns),
                                              to_columns=list(to_columns)))
            _set_slayer(entity, entry)
            if self._try(lambda doc, e=entity: doc.setdefault("relationships", []).append(e)):
                return True
        return False


#: The ``osi_*`` stash keys core holds once lifted (``osi_unique_keys`` keeps the keys core rejects).
_LIFTED = ("osi_ai_context", "osi_is_time", "osi_nonportable_extensions")


def _own_meta(meta: dict[str, Any] | None) -> dict[str, Any] | None:
    """``meta`` without the ``osi_*`` content a load reads from the document's own core; stashed content core rejects
    stays."""
    kept = {k: v for k, v in (meta or {}).items() if k not in _LIFTED}
    if rejected := rejected_extensions(meta):
        kept["osi_nonportable_extensions"] = rejected
    return kept or None


def _without_ossie_meta(dataset: SlayerDataset) -> SlayerDataset:
    return dataset.model_copy(update={
        "meta": _own_meta(dataset.meta),
        "fields": [f.model_copy(update={"meta": _own_meta(f.meta)}) for f in dataset.fields],
        "metrics": [m.model_copy(update={"meta": _own_meta(m.meta)}) for m in dataset.metrics],
        "relationships": [r.model_copy(update={"meta": _own_meta(r.meta)}) for r in dataset.relationships],
    })


def with_dataset(doc: dict[str, Any], *, old: SlayerDataset | None, new: SlayerDataset, reader: str,
                 core: bool = True, dialect_known: bool = True, admission: Admission | None = None) -> dict[str, Any]:
    """``doc`` with ``new`` written over ``old`` (its decomposed current form; ``None`` adds it); ``core=False`` keeps
    Ossie core as it is, writing every change as ``SLAYER`` entries. The ``osi_*`` stash goes into core, but for the
    entries core rejects, which stay in ``SLAYER`` metadata with a warning."""
    _warn_rejected_stash(old=old, new=new)
    return _Writer(doc, old=_without_ossie_meta(old) if old is not None else None, new=_without_ossie_meta(new),
                   reader=reader, core=core, lifted=new, dialect_known=dialect_known, admission=admission).run()


def _stashed(dataset: SlayerDataset) -> dict[str, list[Any]]:
    entities = [("", dataset.meta), *((f"field {f.name!r} of ", f.meta) for f in dataset.fields),
                *((f"metric {m.name!r} of ", m.meta) for m in dataset.metrics),
                *((f"relationship {r.name or r.to!r} of ", r.meta) for r in dataset.relationships)]
    return {where: found for where, meta in entities if (found := rejected_extensions(meta))}


def _warn_rejected_stash(*, old: SlayerDataset | None, new: SlayerDataset) -> None:
    """Warn of each stashed foreign extension Ossie core rejects that the save newly keeps in ``SLAYER`` metadata."""
    before = _stashed(old) if old is not None else {}
    for where, entries in _stashed(new).items():
        for entry in entries:
            if entry not in before.get(where, []):
                warnings.warn(SlayerNormalizationWarning(NormalizationWarning(
                    rule_id="ossie_stash_kept", rewritten=False, original=f"nonportable extension {entry!r}",
                    normalized="Ossie core rejects it (a vendor name and a JSON object are required), so it stays "
                               "in the SLAYER entry and export reports it", location=f"{where}dataset {new.name!r}",
                )), stacklevel=outside_stacklevel())


def document_payload(doc: dict[str, Any]) -> ext.DocumentPayload:
    """``doc``'s document-level ``SLAYER`` payload (empty when it has none)."""
    payload = _slayer_payload(doc)
    return payload if isinstance(payload, ext.DocumentPayload) else ext.DocumentPayload(entity=str(doc.get("name")))


def with_document_payload(doc: dict[str, Any], payload: ext.DocumentPayload) -> None:
    empty = payload.model_copy(update={"metrics": [], "fingerprint": ""}) == ext.DocumentPayload(entity=payload.entity)
    _set_slayer(doc, None if empty and not payload.metrics else ext.encode(payload, ossie=doc.get("name")))


def without_dataset(doc: dict[str, Any], *, name: str, reads: Any) -> dict[str, Any]:
    """``doc`` without dataset ``name`` and the relationships touching it; each model-scoped metric reading it (per
    ``reads(expression) -> set of case-folded dataset names``) moves to the document's ``SLAYER`` entry."""
    out = copy.deepcopy(doc)
    out["datasets"] = [d for d in out["datasets"] if _fold(d.get("name")) != _fold(name)]
    rels = [r for r in out.get("relationships") or [] if _fold(name) not in (_fold(r.get("from")), _fold(r.get("to")))]
    _set(out, "relationships", rels or None)
    payload = document_payload(out)
    crossing = [d for d in payload.relationships if _fold(name) not in (_fold(d.dataset), _fold(d.relationship.to))]
    if crossing != payload.relationships:
        payload.relationships = crossing
        with_document_payload(out, payload)
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
