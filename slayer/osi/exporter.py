"""Export one datasource's SLayer datasets as a current-spec Ossie document.

Everything Ossie defines with SLayer's meaning goes out as Ossie; SLayer-only
content travels in one ``SLAYER`` non-portable extension per entity; everything else
is reported (entity, reason, workaround), never silently dropped.
"""

from __future__ import annotations

from typing import Any

import warnings

from pydantic import BaseModel, Field
from sqlglot import exp

from slayer.core.document_text import edit_text, parse_document_text, without_vendor
from slayer.core.deprecation import outside_stacklevel
from slayer.core.errors import (
    OssieDocumentInvalidError, StoredDocumentLoadError, UnreadableDocumentError,
)
from slayer.core.warnings import SlayerUnloadableDocumentWarning, UnloadableDocumentWarning
from slayer.core.field_expression import OSSIE_SQL, token
from slayer.core.enums import JoinType, RelationshipCardinality
from slayer.core.models import (
    DatasourceConfig,
    SlayerDataset,
)
from slayer.engine.query_engine import SlayerQueryEngine
from slayer.ingest_report import ConversionReport, ConversionWarning
from slayer.core import slayer_extension as ext
from slayer.sql.ossie_dialects import expression_digest, source_digest
from slayer.sql.ossie_source import ossie_source
from slayer.osi.yaml_writer import dump_yaml
from slayer.core.ossie_expression import OSIExpression
from slayer.core.ossie_validity import parse_ossie_sql
from slayer.core.portable_sql import portable_sql
from slayer.sql.dialects import dialect_for_ds_type
from slayer.sql.dialects.base import SqlDialect
from slayer.storage.ossie_render import Admission, OssieRenderer, RenderedDataset, RenderedMetric, admission, slayer_meta

OSSIE_VERSION = "0.3.0.dev0"


class OsiExportError(Exception):
    """An export request names an unknown datasource or dataset, or nothing is exportable."""


class OsiExportResult(ConversionReport):
    """The exported Ossie document, its text and its report."""
    document: dict[str, Any] = Field(default_factory=dict)
    text: str = ""


def _payload_of(entity: dict[str, Any]) -> Any:
    """``entity``'s sole ``SLAYER`` payload, decoded; ``None`` when it has none or it does not decode."""
    found = [e for e in entity.get("nonportable_extensions") or [] if isinstance(e, dict) and ext.is_slayer(e)]
    if len(found) != 1 or not isinstance(found[0].get("data"), dict):
        return None
    try:
        return ext.decode(found[0]["data"])
    except ValueError:
        return None


def _as_ossie(expression: Any) -> dict[str, Any]:
    if isinstance(expression, str):
        return {"sql": expression}
    return expression.model_dump(exclude_none=True) if isinstance(expression, BaseModel) else dict(expression or {})


def _canonical(form: dict[str, Any]) -> tuple[Any, list[tuple[str, str]]]:
    """An Ossie expression's portable SQL in canonical spelling and its dialect entries, less any entry restating it."""
    text = form.get("sql")
    portable = portable_sql(str(text)) if text is not None else None
    entries = [(token(str(d.get("dialect"))), str(d.get("expression"))) for d in form.get("dialects") or []
               if isinstance(d, dict)]
    return portable, sorted(e for e in entries if portable is None or portable_sql(e[1]) != portable)


def _same_expression(held: Any, core: Any, *, on: str | None = None) -> bool:
    """Whether two Ossie expressions say the same: portable SQL compared canonically, a dialect entry restating it
    redundant; with ``on`` (a dialect token), whether they read the same there."""
    held_form, core_form = _as_ossie(held), dict(core or {})
    if held_form == core_form or _canonical(held_form) == _canonical(core_form):
        return True
    return on is not None and None not in (read := [_read_on(f, on) for f in (held_form, core_form)]) \
        and read[0] == read[1]


def _read_on(form: dict[str, Any], dialect: str) -> str | None:
    """The text an Ossie expression reads on ``dialect``: its entry there, else its portable SQL (canonical)."""
    own = next((str(d.get("expression")) for d in form.get("dialects") or []
                if isinstance(d, dict) and token(str(d.get("dialect"))) == dialect), None)
    text = own if own is not None else form.get("sql")
    return (portable_sql(str(text)) or str(text)) if text is not None else None


def _field_changes_core(field: dict[str, Any]) -> bool:
    payload = _payload_of(field)
    if not isinstance(payload, ext.FieldPayload):
        return False
    content = payload.field
    return content.filter is not None or content.sql is not None or (
        content.expression is not None and not _same_expression(content.expression, field.get("expression")))


def _metric_changes_core(metric: dict[str, Any]) -> bool:
    """Whether a stored metric's ``SLAYER`` entry restores a definition other than core's text."""
    payload = _payload_of(metric)
    return isinstance(payload, ext.MetricPayload) and not _same_expression(payload.metric.expression,
                                                                           metric.get("expression"))


def _source_changes_core(source: ext.SourceStructure, *, core: Any, dialect: SqlDialect) -> bool:
    """Whether a dataset's ``SLAYER`` source structure means anything but its core ``source``."""
    if source.filters or source.source_queries or source.sql or source.query_variables:
        return True
    return source.sql_table is not None and ossie_source(source.sql_table, dialect=dialect) != ossie_source(
        str(core or ""), dialect=dialect)


def _dataset_changes_core(entity: dict[str, Any], *, dialect: SqlDialect, members: bool = True) -> bool:
    """Whether a stored dataset's own ``SLAYER`` entry changes what its core means (a row filter, a query-backed or
    ``sql``-structured source) or, with ``members``, adds SLayer-only fields, metrics or relationships."""
    payload = _payload_of(entity)
    return isinstance(payload, ext.DatasetPayload) and (
        _source_changes_core(payload.source, core=entity.get("source"), dialect=dialect)
        or (members and bool(payload.fields or payload.metrics or payload.relationships)))


def changes_core(entity: dict[str, Any], *, dialect: SqlDialect) -> bool:
    """Whether a stored dataset's ``SLAYER`` content may change what its core means: its own entry does, or a field
    or metric entry restores another definition than core's text (which the export then checks by rendering it)."""
    return _dataset_changes_core(entity, dialect=dialect) or any(
        _field_changes_core(f) for f in entity.get("fields") or []) or any(
        _metric_changes_core(m) for m in entity.get("metrics") or [])


def _without_crossing(extensions: list[dict[str, Any]], *, name: str) -> list[dict[str, Any]]:
    """A model's extension entries without the relationships its ``SLAYER`` entry holds into other models (an
    export writes them as core relationships)."""
    out = []
    for entry in extensions:
        payload = _payload_of({"nonportable_extensions": [entry]}) if ext.is_slayer(entry) else None
        if isinstance(payload, ext.DocumentPayload) and payload.relationships:
            kept = payload.model_copy(update={"relationships": []})
            if kept.model_copy(update={"fingerprint": ""}) != ext.DocumentPayload(entity=kept.entity):
                out.append(ext.encode(kept, ossie=name))
            continue
        out.append(entry)
    return out


class _Exporter(OssieRenderer):
    """Exports a datasource: its stored datasets as stored where their ``SLAYER`` content changes no core, the rest
    rendered."""

    def __init__(self, *, config: DatasourceConfig, datasets: dict[str, SlayerDataset], selected: list[str],
                 virtual: dict[str, SlayerDataset | Exception], extensions: bool,
                 resolved: dict[str, SlayerDataset | Exception] | None = None,
                 documents: list[dict[str, Any]] | None = None,
                 crossing: list[ext.RelationshipDeclaration] | None = None) -> None:
        super().__init__(config=config, datasets=datasets, selected=selected, virtual=virtual, extensions=extensions,
                         resolved=resolved)
        #: The stored models (resolved), merged.
        self.documents = documents or []
        entities = {d["name"]: d for doc in self.documents for d in doc["datasets"]}
        #: Whether ``selected`` is a ``--datasets`` subset.
        self.subset = set(selected) != set(datasets)
        #: Each exported loaded dataset's stored entity.
        self.stored = {n: e for n, e in entities.items() if n in datasets and n in selected}
        #: Stored datasets SLayer loads inert: exported as written (a subset holds none).
        self.inert = {} if self.subset else {n: e for n, e in entities.items() if n not in datasets}
        self._entities = entities
        self._admissions: dict[str, Admission] = {}
        #: Stored datasets whose SLAYER content changes nothing in their core: exported as stored.
        self.fixed = {n: e for n, e in self.stored.items() if not self._changes_core(n, e)}
        #: Relationships the stored models hold from one model's dataset into another's.
        self.crossing = crossing or []

    def _admission(self, name: str) -> Admission:
        """What core holds of loaded dataset ``name`` as the export renders it."""
        if name not in self._admissions:
            self._admissions[name] = admission(
                self.datasets[name], config=self.config, source=str(self._entities[name].get("source")),
                peers=self.datasets, stored=self._entities)
        return self._admissions[name]

    def _report_members(self, name: str, entity: dict[str, Any]) -> None:
        """Report a stored dataset's SLayer-only fields and metrics, which travel in its stored entry."""
        payload = _payload_of(entity)
        if not isinstance(payload, ext.DatasetPayload) or not (payload.fields or payload.metrics):
            return
        members = {f.name for f in payload.fields} | {m.name for m in payload.metrics}
        self.notes += [n for n in self._admission(name).notes
                       if n.category == "extension_only" and (n.metric_name in members or n.field_name in members)]

    def _renders_members(self, name: str, entity: dict[str, Any]) -> bool:
        """Whether the rendering puts one of a stored dataset's SLayer-only members into core."""
        payload = _payload_of(entity)
        if not isinstance(payload, ext.DatasetPayload) or not (
                payload.fields or payload.metrics or payload.relationships):
            return False
        admitted = self._admission(name)
        placed = {n for _, n in admitted.placed} | set(admitted.model_scoped)
        own = self.datasets[name].relationships
        return any(f.name in admitted.fields for f in payload.fields) or any(
            m.name in placed for m in payload.metrics) or any(
            i in admitted.relationships for i, r in enumerate(own) if r in payload.relationships)

    def _field_restated(self, home: str, field: dict[str, Any]) -> bool:
        """Whether a stored field means what its ``SLAYER`` entry restores: its core is that definition rendered."""
        if not _field_changes_core(field):
            return True
        rendered = self._admission(home).fields.get(str(field.get("name")))
        return rendered is not None and _same_expression(rendered, field.get("expression"), on=self.token)

    def _metric_restated(self, holder: str, metric: dict[str, Any]) -> bool:
        """Whether a stored metric means what its ``SLAYER`` entry restores: its core is that definition rendered,
        on its home dataset or relocated here."""
        payload = _payload_of(metric)
        if not _metric_changes_core(metric) or not isinstance(payload, ext.MetricPayload):
            return True
        if payload.dataset not in self.datasets or payload.dataset not in self._entities:
            return False
        rendered = self._admission(payload.dataset).placed.get((holder, payload.metric.name))
        return rendered is not None and _same_expression(rendered, metric.get("expression"), on=self.token)

    def _changes_core(self, name: str, entity: dict[str, Any]) -> bool:
        # SLayer-only members ride in the stored entry unless the rendering puts one in core, or must be reported.
        if _dataset_changes_core(entity, dialect=self.dialect, members=not self.extensions or self.subset):
            return True
        if self._renders_members(name, entity):
            return True
        return not all(self._field_restated(name, f) for f in entity.get("fields") or []) or not all(
            self._metric_restated(name, m) for m in entity.get("metrics") or [])

    @property
    def entries(self) -> list[ConversionWarning]:
        return [ConversionWarning.model_validate(n.model_dump(exclude={"field_name"})) for n in self.notes]

    # ---- top level ----

    def run(self) -> OsiExportResult:
        for name, entity in self.fixed.items():
            if not self._refused_access(name):
                self._fixed_dataset(name, entity)
                self._report_members(name, entity)
        for name in self.selected:
            if name not in self.fixed:
                self._dataset(name)
        if not self.osi_datasets:
            reasons = "; ".join(e.message for e in self.entries) or "it has no datasets"
            raise OsiExportError(f"Nothing to export from datasource {self.config.name!r}: {reasons}")
        own = [n for n in self.osi_datasets if n not in self.fixed]
        for name in own:
            self._fields(name)
        for name in own:
            self._keys(name)
        for name in own:
            if name in self.stored:
                continue  # its relationships are its models' own, exported with them
            for relationship in self.datasets[name].relationships:
                self._relationship(name, relationship)
        for declaration in self.crossing:
            if declaration.dataset in self.osi_datasets and declaration.relationship.to in self.osi_datasets:
                self._relationship(declaration.dataset, declaration.relationship)
        for name, entity in self.stored.items():  # the SLayer-only ones: reported, or exported where they now render
            payload = _payload_of(entity)
            if isinstance(payload, ext.DatasetPayload) and name in self.osi_datasets:
                for relationship in payload.relationships:
                    self._relationship(name, relationship)
        metrics = [m for name in own for m in self._metrics(name)]
        self._name_entities(metrics)
        self.exported = {(m.home.casefold(), (m.metric.name or "").casefold()): (m.dataset, m.name)
                         for m in metrics if m.scope == "dataset" and m.name}
        for metric in metrics:
            self._place(metric)
        return OsiExportResult(document=self._document(), warnings=self.entries)

    # ---- datasets and sources ----

    # ---- document ----

    def _shared_semantic_model(self) -> dict[str, Any] | None:
        shared = [(ds.dataset.meta or {}).get("osi_semantic_model") for ds in self.osi_datasets.values()]
        first = shared[0]
        if isinstance(first, dict) and first.get("name") and all(s == first for s in shared):
            return first
        return None

    def _document_header(self, semantic: dict[str, Any] | None) -> dict[str, Any]:
        """Version, name, description and AI context: the shared semantic model's, else the datasource's."""
        document: dict[str, Any] = {"version": OSSIE_VERSION}
        if semantic is None:
            document["name"] = self.config.name
            if self.config.description:
                document["description"] = self.config.description
            return document
        document["name"] = semantic["name"]
        if semantic.get("description"):
            document["description"] = semantic["description"]
        if semantic.get("ai_context"):
            document["ai_context"] = semantic["ai_context"]
        return document

    def _document(self) -> dict[str, Any]:
        semantic = self._shared_semantic_model()
        root: list[Any] = []
        if semantic is None and len(self.documents) == 1:  # the export is that one stored model: its own root
            only = self.documents[0]
            root = [e for e in only.get("nonportable_extensions") or [] if not ext.is_slayer(e)]
            if only.get("name") != self.config.name:  # the datasource model's root is the datasource's
                semantic = {k: only.get(k) for k in ("name", "description", "ai_context")}
        document = self._document_header(semantic)
        entities = {name: self._fixed_entity(name, ds) if name in self.fixed else self._overlaid(name, ds)
                    if name in self.stored else self._dataset_entity(ds) for name, ds in self.osi_datasets.items()}
        entities.update({name: self._verbatim(entity) for name, entity in self.inert.items()})
        document["datasets"] = [entities[name] for name in sorted(entities)]
        relationships = sorted(self.relationships,
                               key=lambda r: (r.from_dataset, r.to, r.from_columns, r.to_columns))
        held = set(entities)
        merged_rels = [self._stored_relationship(r) for doc in self.documents for r in doc.get("relationships") or []
                       if self._holds_ends(r, held=held)]
        merged_rels += [self._relationship_entity(r) for r in relationships]
        if merged_rels:
            document["relationships"] = merged_rels
        merged_metrics = [self._verbatim(m) for doc in self.documents for m in doc.get("metrics") or []
                          if self._reads_held(m, held=held)]
        merged_metrics += self.model_metrics
        if merged_metrics:
            document["metrics"] = merged_metrics
        foreign = self._foreign((semantic or {}).get("nonportable_extensions") or root, owner="the document",
                                dataset=None)
        if self.documents and self.extensions:
            foreign.append(ext.encode(ext.DocumentPayload(entity=document["name"], documents=[
                self._boundary(doc, held=held) for doc in self.documents]), ossie=document["name"]))
        if foreign:
            document["nonportable_extensions"] = foreign
        return document

    def _verbatim(self, entity: dict[str, Any]) -> dict[str, Any]:
        """A stored document's entity as written, its ``SLAYER`` entries without sampled values or the ``osi_*``
        stash; without ``SLAYER`` entries when extensions are off."""
        if not self.extensions:
            return without_vendor(entity, ext.VENDOR)
        if "source" not in entity:
            return entity
        self._report_stash(entity)
        return exported_dataset(entity, reader=self.target)

    def _report_stash(self, entity: dict[str, Any]) -> None:
        """Report the Ossie content a stored dataset's ``SLAYER`` entries keep because core rejected it."""
        name = str(entity.get("name"))
        payload = _payload_of(entity)
        meta = payload.meta if isinstance(payload, ext.DatasetPayload) else None
        for key in (meta or {}).get("osi_unique_keys") or []:
            self._report(f"The unique key {key} of {name!r} names fields that are not plain, unfiltered fields; "
                         f"not exported.", severity="dropped", category="key",
                         suggestion="Key on plain, unfiltered fields of the dataset.", dataset=name)
        members = [(f"{name!r}", meta), *((repr(f.get("name")), _member_meta(f)) for f in entity.get("fields") or []),
                   *((repr(m.get("name")), _member_meta(m)) for m in entity.get("metrics") or [])]
        for owner, held in members:
            self._foreign((held or {}).get("osi_nonportable_extensions") or [], owner=owner, dataset=name)

    def _stored_relationship(self, entity: dict[str, Any]) -> dict[str, Any]:
        """A stored relationship as written; its SLayer-only join semantics reported lossy when extensions are off."""
        payload = _payload_of(entity)
        if isinstance(payload, ext.RelationshipPayload):
            for declaration in payload.declarations:
                rel = declaration.relationship
                if (rel.cardinality not in (None, RelationshipCardinality.MANY_TO_ONE)
                        or rel.join_type is not JoinType.LEFT or rel.trusted):
                    arity = rel.cardinality.value if rel.cardinality else "no cardinality"
                    self._lossy(f"Relationship {entity.get('name')!r} ({arity}, {rel.join_type.value}"
                                f"{', trusted' if rel.trusted else ''}) imports back as a plain untrusted many-to-one left join from {entity.get('from')!r}.",
                                dataset=declaration.dataset)
        return self._verbatim(entity)

    def _place(self, metric: RenderedMetric) -> None:
        if metric.scope == "dataset" and metric.dataset != metric.home and self._holds_relocated(metric):
            return  # exported with the stored dataset that already holds it
        super()._place(metric)

    def _holds_relocated(self, metric: RenderedMetric) -> bool:
        """Whether the stored entity of the dataset a metric relocates to already holds it, restored to its home."""
        for held in (self.stored.get(metric.dataset) or {}).get("metrics") or []:
            payload = _payload_of(held)
            if isinstance(payload, ext.MetricPayload) and payload.dataset == metric.home and (
                    payload.metric.name == metric.metric.name):
                return True
        return False

    def _holds_ends(self, relationship: dict[str, Any], *, held: set[str]) -> bool:
        """Whether the export holds both datasets of a stored relationship; one it does not is reported."""
        missing = sorted({str(relationship.get(k)) for k in ("from", "to")} - held)
        if any(m in self.datasets and m not in self.selected for m in missing):  # a refused one is reported already
            self._report(f"Relationship {relationship.get('name')!r} from {relationship.get('from')!r} to "
                         f"{relationship.get('to')!r}: {', '.join(map(repr, missing))} not exported; not exported.",
                         severity="dropped", category="subset", suggestion=self._needs_suggestion(missing),
                         dataset=str(relationship.get("from")))
        return not missing

    def _reads_held(self, metric: dict[str, Any], *, held: set[str]) -> bool:
        """Whether a stored model-scoped metric reads only exported datasets; one that does not is reported."""
        text = _as_ossie(metric.get("expression")).get("sql") or ""
        tree, _ = parse_ossie_sql(text, OSSIE_SQL) if text else (None, None)
        reads = {c.table for c in tree.find_all(exp.Column) if c.table} if tree is not None else set()
        missing = sorted(reads - held)
        if missing:
            self._report(f"Metric {metric.get('name')!r} reads {', '.join(map(repr, missing))}, which the export "
                         f"leaves out; not exported.", severity="dropped", category="metric",
                         suggestion=self._needs_suggestion(missing), metric=str(metric.get("name")))
        return not missing

    def _boundary(self, doc: dict[str, Any], *, held: set[str]) -> ext.DocumentBoundary:
        """A stored model's part of the merged document: its own top level, datasets and model metrics, the model
        metrics its datasets' SLayer-only content renders included."""
        own = {d["name"] for d in doc["datasets"]}
        rendered = [str(m["name"]) for m in self.model_metrics
                    if isinstance(p := _payload_of(m), ext.MetricPayload) and p.dataset in own]
        return ext.DocumentBoundary(
            name=doc["name"], description=doc.get("description"), ai_context=doc.get("ai_context"),
            datasets=sorted(n for n in own if n in held),
            relationships=[r["name"] for r in doc.get("relationships") or []],
            metrics=list(dict.fromkeys([*(m["name"] for m in doc.get("metrics") or []), *rendered])),
            nonportable_extensions=_without_crossing(list(doc.get("nonportable_extensions") or []),
                                                     name=doc["name"]),
        )

    def _fixed_entity(self, name: str, ds: RenderedDataset) -> dict[str, Any]:
        """A stored dataset as stored, with the metrics another dataset's export places on it."""
        entity = self._verbatim(self.fixed[name])
        return {**entity, "metrics": [*(entity.get("metrics") or []), *ds.metrics]} if ds.metrics else entity

    def _overlaid(self, name: str, ds: RenderedDataset) -> dict[str, Any]:
        """A stored dataset whose ``SLAYER`` content changes its core: its stored Ossie keys, with the source, fields and
        metrics that content changes rendered as for a dataset with no stored form."""
        stored = self.stored[name]
        keeps = {"fields": lambda e: self._field_restated(name, e), "metrics": lambda e: self._metric_restated(name, e)}
        kept = {key: {e["name"] for e in stored.get(key) or [] if keep(e)} for key, keep in keeps.items()}
        # Entities kept from core are not SLayer-only as well.
        rendered = self._dataset_entity(ds.model_copy(update={
            "slayer_fields": [f for f in ds.slayer_fields if f.name not in kept["fields"]],
            "slayer_metrics": [m for m in ds.slayer_metrics if m.name not in kept["metrics"]]}))
        out = {k: v for k, v in stored.items() if k not in ("fields", "metrics", "nonportable_extensions")}
        out["source"] = rendered["source"]
        for key, keep in keeps.items():
            held = {e["name"]: e for e in stored.get(key) or [] if keep(e)}
            fresh = {e["name"]: e for e in rendered.get(key) or []}
            names = [e["name"] for e in stored.get(key) or [] if e["name"] in held or e["name"] in fresh]
            names += [n for n in fresh if n not in names]
            entries = [self._verbatim(held[n]) if n in held else fresh[n] for n in names]
            if entries:
                out[key] = entries
        extensions = [e for e in stored.get("nonportable_extensions") or [] if not ext.is_slayer(e)]
        extensions += [e for e in rendered.get("nonportable_extensions") or [] if ext.is_slayer(e)]
        if extensions:
            out["nonportable_extensions"] = extensions
        return out


async def _stored_datasets(*, storage: Any, datasource: str) -> dict[str, SlayerDataset]:
    stored: dict[str, SlayerDataset] = {}
    for name in sorted(await storage.list_datasets(data_source=datasource)):
        dataset = await storage.get_dataset(name, data_source=datasource)
        if dataset is not None:
            stored[name] = dataset
    return stored


async def _expanded_query_backed(
    *, engine: SlayerQueryEngine, stored: dict[str, SlayerDataset], selected: list[str],
) -> dict[str, SlayerDataset | Exception]:
    """Each selected query-backed dataset's virtual expansion, or the exception expanding it raised."""
    virtual: dict[str, SlayerDataset | Exception] = {}
    for name in selected:
        if stored[name].source_queries:
            try:
                virtual[name] = await engine.expand_query_backed_dataset(stored[name])
            except Exception as exc:  # noqa: BLE001 — reported on the dataset
                virtual[name] = exc
    return virtual


async def _stored_documents(*, storage: Any, datasource: str, loaded: set[str],
                            skipped: list[StoredDocumentLoadError]) -> list[tuple[str, str, Any]]:
    """The name, stored text and resolved document of each stored model named in ``loaded`` that still reads; one that
    no longer reads goes to ``skipped``."""
    documents: list[tuple[str, str, Any]] = []
    for _, name in await storage.list_semantic_models(datasource):
        if name not in loaded:
            continue
        try:
            found = await storage.semantic_model_document(name, data_source=datasource)
            if found is not None:
                documents.append((name, found[0], found[1]))
        except (UnreadableDocumentError, OssieDocumentInvalidError) as exc:  # edited since it loaded
            skipped.append(StoredDocumentLoadError(kind="semantic_model", name=name, data_source=datasource, cause=exc))
    return documents


def _crossing(documents: list[dict[str, Any]]) -> list[ext.RelationshipDeclaration]:
    """The relationships the models hold from one model's dataset into another's."""
    out: list[ext.RelationshipDeclaration] = []
    for doc in documents:
        payload = _payload_of(doc)
        if isinstance(payload, ext.DocumentPayload):
            out += payload.relationships
    return out


def _cleaned(payload: Any) -> Any:
    """``payload`` as an export writes it: without sampled values or the ``osi_*`` stash export regenerates."""
    if isinstance(payload, ext.FieldPayload):
        return payload.model_copy(update={"field": payload.field.model_copy(update={
            "sampled": None, "sampled_values": None, "distinct_count": None, "meta": slayer_meta(payload.field.meta)})})
    if isinstance(payload, ext.MetricPayload):
        return payload.model_copy(update={"metric": payload.metric.model_copy(
            update={"meta": slayer_meta(payload.metric.meta)})})
    if isinstance(payload, ext.DatasetPayload):
        return payload.model_copy(update={"meta": slayer_meta(payload.meta)})
    return payload


def _exported_entry(entity: dict[str, Any], *, ossie: Any) -> dict[str, Any]:
    """A stored entity with its ``SLAYER`` entry as an export writes it (dropped when nothing is left)."""
    payload = _payload_of(entity)
    cleaned = _cleaned(payload) if payload is not None else None
    if cleaned is None or cleaned == payload:
        return entity
    cleaned._written = None  # re-encoded: its content is what it says now
    kept = [e for e in entity.get("nonportable_extensions") or [] if not ext.is_slayer(e)]
    if not (isinstance(cleaned, ext.FieldPayload) and cleaned.field == ext.FieldContent()):
        kept.append(ext.encode(cleaned, ossie=ossie))
    out = {k: v for k, v in entity.items() if k != "nonportable_extensions"}
    return {**out, "nonportable_extensions": kept} if kept else out


def _member_meta(entity: dict[str, Any]) -> dict[str, Any] | None:
    payload = _payload_of(entity)
    if isinstance(payload, ext.FieldPayload):
        return payload.field.meta
    return payload.metric.meta if isinstance(payload, ext.MetricPayload) else None


def _member_digest(entity: dict[str, Any]) -> Any:
    return expression_digest(OSIExpression.model_validate(entity.get("expression") or {}))


def exported_dataset(entity: dict[str, Any], *, reader: str) -> dict[str, Any]:
    """A stored dataset entity, its fields and metrics with their ``SLAYER`` entries as an export writes them."""
    out = _exported_entry(entity, ossie=source_digest(str(entity.get("source")), reader=reader))
    for key in ("fields", "metrics"):
        if out.get(key):
            out = {**out, key: [_exported_entry(m, ossie=_member_digest(m)) for m in out[key]]}
    return out


def _verbatim_text(documents: list[tuple[str, str, Any]], *, stored: dict[str, SlayerDataset],
                   datasets: list[str] | None, dialect: SqlDialect) -> str | None:
    """The one stored model's text when the export is that model as written: one flat document none of whose
    datasets' ``SLAYER`` content changes its core, holding every exported dataset, and no subset asked for."""
    if datasets is not None or len(documents) != 1:
        return None
    name, text, data = documents[0]
    flat = "dataset_refs" not in (parse_document_text(text) or {})
    if not flat or any(changes_core(d, dialect=dialect) or exported_dataset(d, reader=dialect.sqlglot_name) != d
                       for d in data["datasets"]):
        return None
    return text if all(d.semantic_model == name for d in stored.values()) else None


def _left_out(skipped: list[StoredDocumentLoadError]) -> list[ConversionWarning]:
    """A report entry, and an ``unloadable_document`` warning, for each document the export leaves out."""
    out: list[ConversionWarning] = []
    for error in skipped:
        payload = UnloadableDocumentWarning(document_kind="semantic_model", data_source=error.data_source, name=error.name,
                                            cause=error.cause_text)
        warnings.warn(SlayerUnloadableDocumentWarning(payload), stacklevel=outside_stacklevel())
        out.append(ConversionWarning(message=payload.human_message(), category="unloadable_document", severity="dropped"))
    return out


async def export_osi(*, engine: SlayerQueryEngine, datasource: str, datasets: list[str] | None = None,
                     extensions: bool = True) -> OsiExportResult:
    """Export ``datasource``'s datasets (or the ``datasets`` subset) as an Ossie document and its report."""
    storage = engine.storage
    config = await storage.get_datasource(datasource)
    if config is None:
        raise OsiExportError(f"Datasource {datasource!r} not found.")
    stored = await _stored_datasets(storage=storage, datasource=datasource)
    skipped = [e for e in (await storage.load_datasets(data_source=datasource))[1] if e.kind == "semantic_model"]
    documents = await _stored_documents(storage=storage, datasource=datasource, skipped=skipped,
                                        loaded={d.semantic_model for d in stored.values() if d.semantic_model})
    readable = {name for name, _, _ in documents}
    stored = {n: d for n, d in stored.items() if not d.semantic_model or d.semantic_model in readable}
    left_out = _left_out(skipped)
    if not stored and skipped:
        raise skipped[0]  # nothing else to export: the document's own load error
    unknown = [m for m in datasets or [] if m not in stored]
    if unknown:
        raise OsiExportError(f"Unknown dataset(s) in datasource {datasource!r}: {', '.join(unknown)}.")
    selected = sorted(set(datasets)) if datasets is not None else sorted(stored)
    virtual = await _expanded_query_backed(engine=engine, stored=stored, selected=selected)
    resolved = await engine.resolved_datasets(list(stored.values()), datasource=config)
    parsed = [data for _, _, data in documents]
    # Without extensions a dataset's access tags would be dropped: such a dataset needs the report path.
    plain = extensions or not any(d.access_tags for d in stored.values())
    verbatim = _verbatim_text(documents, stored=stored, datasets=datasets,
                              dialect=dialect_for_ds_type(config.type)) if plain else None
    if verbatim is not None:
        text = verbatim if extensions else edit_text(verbatim, lambda data: without_vendor(data, ext.VENDOR))
        return OsiExportResult(document=parse_document_text(text), text=text, warnings=left_out)
    result = _Exporter(config=config, datasets=stored, selected=selected, virtual=virtual, extensions=extensions,
                       resolved=resolved, documents=parsed, crossing=_crossing(parsed)).run()
    result.warnings = [*left_out, *result.warnings]
    result.text = dump_yaml(result.document)
    return result


async def export_ontology(*, storage: Any, name: str) -> str:
    """Stored ontology ``name``'s text, as written."""
    text = await storage.get_ontology_text(name) if name in await storage.list_ontologies() else None
    if text is None:
        raise OsiExportError(f"Ontology {name!r} not found.")
    return text


async def export_ontology_mapping(*, storage: Any, data_source: str, name: str) -> str:
    """Stored mapping ``name`` of ``data_source``'s text, as written."""
    held = (data_source, name) in await storage.list_ontology_mappings(data_source)
    text = await storage.get_ontology_mapping_text(name, data_source) if held else None
    if text is None:
        raise OsiExportError(f"Mapping {name!r} not found in datasource {data_source!r}.")
    return text
