"""Decompose a stored Ossie semantic-model document into native datasets, relationships and model-scoped metrics.

A load never converts: each construct keeps its Ossie shape. An expression with no form SLayer can run on the
datasource gets an in-memory entry for it, never written back.
"""

from __future__ import annotations

import functools
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.errors import SqlglotError

from slayer.core.enums import DataType, JoinType, RelationshipCardinality
from slayer.core.errors import OssieDocumentInvalidError, StoredDocumentLoadError
from slayer.core.field_expression import DialectEntry, Expression, ExpressionObject, token
from slayer.core import slayer_extension as ext
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, ExtensionNote, SlayerDataset
from slayer.core.ossie_validity import columns, is_agg_call, parse_ossie_sql
from slayer.core.semantic_model import (
    ModelScopedMetric, OssieDataset, OssieExpression, SemanticModelDocument, ossie_validity_errors,
)
from slayer.sql.dialects import dialect_for_ds_type, ossie_token
from slayer.sql.ossie_dialects import (
    ANSI_SQL, OSSIE_SQL, PORTABLE_READER, Candidate, read, reader_for, render, source_digest,
)
from slayer.storage.document_mapping import core_source
from slayer.storage.document_extensions import (
    apply_dataset_entry, apply_field_entry, digest as entry_digest, metric_entry, read_entry,
    relationship_declarations, restore_metrics,
)

__all__ = ["Decomposition", "DocumentIndex", "build_index", "checked_document", "decompose", "expression_datasets",
           "parse_document_text"]

_OSSIE_TYPES = {t.value: t for t in DataType if t is not DataType.ARRAY}
WINDOW_WARNING = "uses a window function (OVER), which SLayer cannot evaluate yet"


class Decomposition(BaseModel):
    """One document's native datasets and model-scoped metrics."""

    name: str
    data_source: str
    datasets: list[SlayerDataset]
    metrics: list[ModelScopedMetric]
    notes: list[ExtensionNote] = []


def parse_document_text(text: str) -> Any:
    """A document's YAML or JSON text as data; raises ``ValueError`` when it does not parse."""
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"the document is not valid YAML or JSON: {exc}") from exc


def checked_document(*, text: str, name: str) -> SemanticModelDocument:
    """``text`` as a valid document named ``name``; raises ``OssieDocumentInvalidError`` with every reason."""
    try:
        data = parse_document_text(text)
    except ValueError as exc:
        raise OssieDocumentInvalidError(name=name, reasons=[str(exc)]) from exc
    reasons = ossie_validity_errors(data)
    if not reasons and data["name"] != name:
        reasons = [f"its name {data['name']!r} differs from the name it is stored under ({name!r})"]
    if reasons:
        raise OssieDocumentInvalidError(name=name, reasons=reasons)
    return SemanticModelDocument.model_validate(data)


def _windowed(expression: OssieExpression) -> bool:
    texts = [(OSSIE_SQL, expression.sql)] if expression.sql else []
    texts += [(d.dialect, d.expression) for d in expression.dialects or []]
    for dialect, text in texts:
        tree, _ = parse_ossie_sql(text, dialect if token(dialect) != ANSI_SQL else ANSI_SQL)
        if tree is not None and tree.find(exp.Window) is not None:
            return True
    return False


def _portable(expression: OssieExpression) -> str | None:
    if expression.sql:
        return expression.sql
    return next((d.expression for d in expression.dialects or [] if token(d.dialect) == OSSIE_SQL), None)


def _runnable(expression: OssieExpression, *, target: str) -> tuple[Expression, str | None]:
    """``expression`` as SLayer runs it on ``target`` (an Ossie dialect token), and why it cannot run, if so."""
    entries = [DialectEntry(dialect=d.dialect, expression=d.expression) for d in expression.dialects or []]
    if expression.sql is not None and not entries:
        return expression.sql, None
    obj = ExpressionObject(sql=expression.sql, dialects=entries or None)
    if _portable(expression) is not None or any(token(e.dialect) == target for e in entries):
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


def _runnable_metric(expression: OssieExpression, *, target: str) -> tuple[Expression, str | None]:
    """A metric runs its portable ``sql``: one without it gets an in-memory one from its ANSI_SQL entry, else its
    target-native entry, else its first readable entry translated."""
    entries = [DialectEntry(dialect=d.dialect, expression=d.expression) for d in expression.dialects or []]
    if expression.sql is not None and not entries:
        return expression.sql, None
    obj = ExpressionObject(sql=expression.sql, dialects=entries or None)
    if _portable(expression) is not None:
        return obj, None
    ansi = [e for e in entries if token(e.dialect) == ANSI_SQL]
    native = [e for e in entries if token(e.dialect) == target]
    if ansi or native:
        return obj.model_copy(update={"sql": (ansi or native)[0].expression}), None
    for entry in entries:
        if reader_for(entry.dialect) is None:
            continue
        candidate = Candidate(dialect=entry.dialect, text=entry.expression)
        try:
            return obj.model_copy(update={"sql": render(candidate, read(candidate), target=PORTABLE_READER)}), None
        except SqlglotError:
            continue
    found = ", ".join(dict.fromkeys(e.dialect for e in entries)) or "none"
    return obj, f"has no readable SQL expression (dialects: {found})"


def _source(dataset: OssieDataset) -> dict[str, Any]:
    sql_table, sql = core_source(dataset.source)
    return {"sql": sql} if sql is not None else {"sql_table": sql_table}


def _field(field: Any, *, keys: tuple[set[str], set[str]], target: str, notes: list[ExtensionNote],
           dataset: str) -> DatasetField:
    primary, unique = keys
    expression, problem = _runnable(field.expression, target=target)
    if expression == field.name:
        expression = None  # reads the column named like the field
    warning = WINDOW_WARNING if _windowed(field.expression) else problem
    built = DatasetField(
        name=field.name, expression=expression,
        datatype=_OSSIE_TYPES[field.datatype] if field.datatype else None,
        primary_key=field.name in primary, unique=field.name in unique,
        label=field.label, description=field.description, load_warning=warning,
    )
    label = f"field {field.name!r} of {dataset!r}"
    entry = read_entry(field.nonportable_extensions, kind="field", entity=field.name,
                       ossie=entry_digest(field.expression), label=label, notes=notes)
    return apply_field_entry(built, entry, notes=notes, label=label, target=target)


def _metric(metric: Any, *, target: str, windowed_fields: set[str]) -> DatasetMetric:
    expression, problem = _runnable_metric(metric.expression, target=target)
    warning = problem
    if _windowed(metric.expression):
        warning = WINDOW_WARNING
    elif windowed_fields and _reads(metric.expression, windowed_fields):
        warning = "reads a windowed field, which SLayer cannot evaluate yet"
    return DatasetMetric(name=metric.name, expression=expression, description=metric.description,
                         datatype=_OSSIE_TYPES[metric.datatype] if metric.datatype else None, load_warning=warning)


def _reads(expression: OssieExpression, names: set[str]) -> bool:
    text = _portable(expression)
    tree, _ = parse_ossie_sql(text, OSSIE_SQL) if text else (None, None)
    return tree is not None and any(not c.table and c.name.casefold() in names for c in tree.find_all(exp.Column))


def _dataset(dataset: OssieDataset, *, data_source: str, target: str, reader: str, document: str,
             restored: list[tuple[str, DatasetMetric]]) -> SlayerDataset:
    primary = set(dataset.primary_key or [])
    unique = {k[0] for k in dataset.unique_keys or [] if len(k) == 1}
    notes: list[ExtensionNote] = []
    fields = [_field(f, keys=(primary, unique), target=target, notes=notes, dataset=dataset.name)
              for f in dataset.fields or []]
    windowed = {f.name.casefold() for f in fields if f.load_warning == WINDOW_WARNING}
    metrics = []
    for raw in dataset.metrics or []:
        label = f"metric {raw.name!r} of {dataset.name!r}"
        entry = read_entry(raw.nonportable_extensions, kind="metric", entity=raw.name,
                           ossie=entry_digest(raw.expression), label=label, notes=notes)
        core = _metric(raw, target=target, windowed_fields=windowed)
        metric, was_restored = metric_entry(core, entry, home=dataset.name, notes=notes, label=label)
        if was_restored:
            restored.append((dataset.name, core))
        metrics.append(metric)
    built = SlayerDataset(
        name=dataset.name, data_source=data_source, description=dataset.description, fields=fields,
        semantic_model=document, metrics=metrics, **_source(dataset),
    )
    entry = read_entry(dataset.nonportable_extensions, kind="dataset", entity=dataset.name,
                       ossie=source_digest(dataset.source, reader=reader), label=f"dataset {dataset.name!r}",
                       notes=notes)
    built = apply_dataset_entry(built, entry, notes=notes, target=target)
    built.extension_notes = notes
    return built


@functools.lru_cache(maxsize=256)
def _decomposed(text: str, name: str, data_source: str, ds_type: str | None) -> Decomposition:
    doc = checked_document(text=text, name=name)
    target = ossie_token(ds_type)
    reader = dialect_for_ds_type(ds_type).sqlglot_name
    restored: list[tuple[str, DatasetMetric]] = []
    datasets = {d.name: _dataset(d, data_source=data_source, target=target, reader=reader, document=doc.name,
                                 restored=restored) for d in doc.datasets}
    for rel in doc.relationships or []:
        notes = datasets[rel.from_dataset].extension_notes
        default = DatasetRelationship(
            name=rel.name, to=rel.to, from_columns=list(rel.from_columns), to_columns=list(rel.to_columns),
            cardinality=RelationshipCardinality.MANY_TO_ONE, join_type=JoinType.LEFT,
        )
        entry = read_entry(rel.nonportable_extensions, kind="relationship", entity=rel.name,
                           ossie=ext.relationship_digest(from_dataset=rel.from_dataset, to=rel.to,
                                                         from_columns=list(rel.from_columns),
                                                         to_columns=list(rel.to_columns)),
                           label=f"relationship {rel.name!r}", notes=notes)
        for owner, declared in relationship_declarations(rel, entry, datasets=datasets, default=default, notes=notes):
            datasets[owner].relationships.append(declared)
    restore_metrics(datasets, restored=restored, dialect=reader)
    metrics = []
    for m in doc.metrics or []:
        expression, problem = _runnable_metric(m.expression, target=target)
        metrics.append(ModelScopedMetric(
            name=m.name, semantic_model=doc.name, data_source=data_source, expression=expression,
            description=m.description, ai_context=m.ai_context,
            datatype=_OSSIE_TYPES[m.datatype] if m.datatype else None,
            load_warning=WINDOW_WARNING if _windowed(m.expression) else problem,
        ))
    names = {m.name.casefold() for m in metrics}
    for metric in metrics:
        if metric.load_warning is None:
            metric.load_warning = model_scoped_problem(metric.expression, metric_names=names)
    notes: list[ExtensionNote] = []
    entry = read_entry(doc.nonportable_extensions, kind="semantic_model", entity=doc.name, ossie=doc.name,
                       label=f"semantic model {doc.name!r}", notes=notes)
    if entry is not None:
        metrics += [ModelScopedMetric(name=m.name, semantic_model=doc.name, data_source=data_source,
                                      expression=m.expression, description=m.description, datatype=m.datatype)
                    for m in entry.payload.metrics]
    return Decomposition(name=doc.name, data_source=data_source, datasets=list(datasets.values()), metrics=metrics,
                         notes=notes)


def decompose(*, text: str, name: str, data_source: str, ds_type: str | None) -> Decomposition:
    """``text`` decomposed for a datasource of ``ds_type``; raises ``OssieDocumentInvalidError`` when invalid."""
    return _decomposed(text, name, data_source, ds_type).model_copy(deep=True)


def _aggregated(node: exp.Expr) -> bool:
    """Inside an aggregate call, its ``FILTER`` clause or an ``AGG(d.m)`` call."""
    parent = node.parent
    while parent is not None:
        if isinstance(parent, exp.AggFunc | exp.Filter) or is_agg_call(parent):
            return True
        parent = parent.parent
    return False


def model_scoped_problem(expression: Expression, *, metric_names: set[str]) -> str | None:
    """Why SLayer cannot evaluate a model-scoped metric (a field read unqualified or outside an aggregate, an aggregate
    naming no dataset), else ``None``; an aggregate over several datasets evaluates as in a query."""
    text = expression if isinstance(expression, str) else (
        expression.sql or next((d.expression for d in expression.dialects or []), None))
    tree, _ = parse_ossie_sql(text, OSSIE_SQL) if text else (None, None)
    if tree is None:
        return None
    for column in columns(tree, OSSIE_SQL):
        if _aggregated(column):
            if not column.table:
                return f"reads the unqualified field {column.name!r}; qualify it by its dataset"
        elif column.table:
            return f"reads {column.table}.{column.name} outside an aggregate"
        elif column.name.casefold() not in metric_names:
            return f"reads the unqualified field {column.name!r}; qualify it by its dataset"
    for aggregate in tree.find_all(exp.AggFunc):
        if not _aggregated(aggregate) and not any(c.table for c in aggregate.find_all(exp.Column)):
            return f"aggregates {aggregate.sql()} over no dataset; name its dataset, as in COUNT(<dataset>.*)"
    return None


def expression_datasets(expression: Expression) -> tuple[set[str], set[str]]:
    """The datasets a model-scoped metric reads and the bare names it uses (other metrics), case-folded."""
    text = expression if isinstance(expression, str) else (
        expression.sql or next((d.expression for d in expression.dialects or []), None))
    tree, _ = parse_ossie_sql(text, OSSIE_SQL) if text else (None, None)
    if tree is None:
        return set(), set()
    columns = list(tree.find_all(exp.Column))
    return {c.table.casefold() for c in columns if c.table}, {c.name.casefold() for c in columns if not c.table}


class DocumentIndex(BaseModel):
    """A datasource's documents resolved against its namespace: what loads, who owns what, and what failed."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    data_source: str
    #: Loadable document datasets by case-folded name.
    datasets: dict[str, SlayerDataset] = {}
    #: Owning document of every document dataset (quarantined ones included) by case-folded name.
    owners: dict[str, str] = {}
    metrics: dict[str, ModelScopedMetric] = {}
    #: Case-folded names unloadable everywhere, standalone copies included.
    quarantined: dict[str, str] = {}
    failures: list[StoredDocumentLoadError] = []

    def failure(self, *, kind: str, name: str, cause: str) -> None:
        self.failures.append(StoredDocumentLoadError(kind=kind, name=name, data_source=self.data_source,
                                                     cause=ValueError(cause)))


def build_index(*, data_source: str, documents: list[tuple[str, str]], standalone: list[str],
                ds_type: str | None) -> DocumentIndex:
    """Decompose ``documents`` (``(name, text)``) of ``data_source`` and quarantine every name two owners claim."""
    index = DocumentIndex(data_source=data_source)
    decomposed: list[Decomposition] = []
    for name, text in documents:
        try:
            decomposed.append(decompose(text=text, name=name, data_source=data_source, ds_type=ds_type))
        except Exception as exc:  # noqa: BLE001 — one unloadable document never hides the others
            index.failures.append(StoredDocumentLoadError(kind="semantic_model", name=name, data_source=data_source,
                                                          cause=exc))
    claims: dict[str, list[tuple[str, str]]] = {}
    for name in standalone:
        claims.setdefault(name.casefold(), []).append(("standalone dataset", name))
    for doc in decomposed:
        for ds in doc.datasets:
            claims.setdefault(ds.name.casefold(), []).append((f"semantic model {doc.name!r}", ds.name))
            index.owners[ds.name.casefold()] = doc.name
    for folded, owners in claims.items():
        if len(owners) > 1:
            index.quarantined[folded] = " and ".join(f"{o} ({n!r})" for o, n in owners)
            for owner, spelled in owners:
                index.failure(kind="dataset", name=spelled,
                              cause=f"{spelled!r} is declared by {index.quarantined[folded]}; rename one of them")
    known = {n.casefold() for n in standalone} | set(claims)
    for doc in decomposed:
        _admit(index, doc, known=known)
    return index


def _admit(index: DocumentIndex, doc: Decomposition, *, known: set[str]) -> None:
    """Admit ``doc``'s datasets and metrics, dropping each dependent of a quarantined dataset with a failure."""
    gone = set(index.quarantined)
    for ds in doc.datasets:
        if ds.name.casefold() in gone:
            for rel in ds.relationships:
                index.failure(kind="relationship", name=rel.name or rel.to,
                              cause=f"its dataset {ds.name!r} is unloadable: {index.quarantined[ds.name.casefold()]}")
            continue
        kept = []
        for rel in ds.relationships:
            if rel.to.casefold() in gone:
                index.failure(kind="relationship", name=rel.name or rel.to,
                              cause=f"its dataset {rel.to!r} is unloadable: {index.quarantined[rel.to.casefold()]}")
            else:
                kept.append(rel)
        ds.relationships = kept
        index.datasets[ds.name.casefold()] = ds
    pending = {m.name.casefold(): m for m in doc.metrics}
    dropped: set[str] = set()
    changed = True
    while changed:
        changed = False
        for folded, metric in list(pending.items()):
            tables, bare = expression_datasets(metric.expression)
            cause = next((t for t in sorted(tables) if t in gone), None) or next(
                (b for b in sorted(bare) if b in dropped), None)
            if cause is not None:
                index.failure(kind="metric", name=metric.name, cause=f"it reads {cause!r}, which is unloadable")
                dropped.add(folded)
                del pending[folded]
                changed = True
    for metric in pending.values():
        missing = sorted(t for t in expression_datasets(metric.expression)[0] if t not in known)
        if missing and metric.load_warning is None:
            metric.load_warning = f"reads {', '.join(repr(m) for m in missing)}, which no longer exist"
    index.metrics.update(pending)
