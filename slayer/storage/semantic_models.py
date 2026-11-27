"""Decompose a stored Ossie semantic-model document into native datasets, relationships and model-scoped metrics.

A load never converts: each construct keeps its Ossie shape. An expression with no form SLayer can run on the
datasource gets an in-memory entry for it, never written back.
"""

from __future__ import annotations

import functools
from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict
from sqlglot import exp
from sqlglot.errors import SqlglotError

from slayer.core.document_text import parse_document_text
from slayer.core.enums import DataType, RelationshipCardinality
from slayer.core.errors import (
    FieldExpressionError, OssieDocumentInvalidError, OssieErrorCode, StoredDocumentLoadError, UnreadableDocumentError,
)
from slayer.core.field_expression import (
    DialectEntry, Expression, ExpressionObject, portable_text, running_form, token,
)
from slayer.core.granularity import CustomGranularity, granularity_key
from slayer.core.syntax import parse_expr
from slayer.core import slayer_extension as ext
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, ExtensionNote, SlayerDataset, name_problem
from slayer.core.ossie_validity import (
    SOURCE_MEASURE_REASON, columns, is_agg_call, parse_ossie_sql, reads_source_measure,
)
from slayer.core.semantic_model import (
    MetricProblem, ModelScopedAnalysis, ModelScopedMetric, OssieDataset, OssieExpression, SemanticModelDocument,
    ossie_validity_errors,
)
from slayer.sql.dialects import dialect_for_ds_type, ossie_token
from slayer.sql.ossie_dialects import (
    ANSI_SQL, OSSIE_SQL, PORTABLE_READER, Candidate, read, reader_for, render, source_digest,
)
from slayer.storage.document_mapping import core_source, portable_form, runnable_field
from slayer.storage.document_extensions import (
    Restored, apply_dataset_entry, apply_field_entry, digest as entry_digest, metric_entry, read_entry,
    relocate_metrics, restore_metrics, restored_relationship,
)
from slayer.storage.model_metric_rules import (
    aggregate_call, aggregate_window_key, chain_warning, document_names, unevaluable_chain, unresolved_metrics,
    unresolved_references,
)

__all__ = ["Decomposition", "DocumentIndex", "analyse_model_scoped", "build_index", "checked_document",
           "decompose", "expression_datasets", "load_problem", "metric_graph", "unevaluable_targets",
           "warn_unevaluable_dependents"]

_OSSIE_TYPES = {t.value: t for t in DataType if t is not DataType.ARRAY}
#: Why Ossie defers a model-scoped metric holding a window (``E_DEFERRED_WINDOWED_METRIC``).
DEFERRED_WINDOW = "uses a window function (OVER), which Ossie defers in a model-scoped metric"


class Ignored(BaseModel):
    """A stored entity SLayer cannot reference yet, loaded inert."""

    kind: str
    name: str
    reason: str
    #: The dataset holding it (a field, metric or relationship); ``None`` for a dataset.
    dataset: str | None = None

    def note(self) -> ExtensionNote:
        return ExtensionNote(kind=self.kind, entity=self.name, message=self.message())

    def message(self) -> str:
        where = f" of dataset {self.dataset!r}" if self.dataset is not None else ""
        return (f"{self.kind} {self.name!r}{where} is not loaded: {self.reason}; SLayer cannot reference this name "
                f"yet, so it is kept as written")


class Decomposition(BaseModel):
    """One document's native datasets and model-scoped metrics."""

    name: str
    data_source: str
    datasets: list[SlayerDataset]
    metrics: list[ModelScopedMetric]
    notes: list[ExtensionNote] = []
    #: Case-folded metrics warned only for reading an unevaluable entity through another.
    chained: set[str] = set()
    #: Entities whose names SLayer cannot reference yet, loaded inert.
    ignored: list[Ignored] = []
    #: Datasets that fail to load, each with its error; the rest of the document loads.
    failed: list[tuple[str, Any]] = []


def checked_document(*, text: str, name: str) -> SemanticModelDocument:
    """``text`` as a valid document named ``name``; raises ``OssieDocumentInvalidError`` with every reason."""
    try:
        data = parse_document_text(text)
    except UnreadableDocumentError as exc:
        raise OssieDocumentInvalidError(name=name, reasons=exc.reasons) from exc
    reasons = ossie_validity_errors(data)
    if not reasons and data["name"] != name:
        reasons = [f"its name {data['name']!r} differs from the name it is stored under ({name!r})"]
    if reasons:
        raise OssieDocumentInvalidError(name=name, reasons=reasons)
    return SemanticModelDocument.model_validate(data)


def _unparseable(text: str | None) -> str | None:
    """Why SLayer's expression reader rejects ``text``, else ``None``."""
    if text is None:
        return None
    try:
        parse_expr(text)
    except ValueError as exc:
        return f"is not an expression SLayer can parse: {exc}"
    return None


def _unparseable_field(expression: Expression | None, *, target: str, owner: str) -> str | None:
    """Why the portable form ``expression`` runs on ``target`` does not parse; ``None`` for a native entry."""
    if expression is None:
        return None
    try:
        form = running_form(owner=owner, expression=expression, dialect=target)
    except FieldExpressionError:  # reported when the field is read
        return None
    return None if form.native else _unparseable(form.text)


_portable = portable_form
_runnable = runnable_field


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


def _source(dataset: OssieDataset, *, reader: str) -> dict[str, Any]:
    sql_table, sql = core_source(dataset.source, reader=reader)
    return {"sql": sql} if sql is not None else {"sql_table": sql_table}


def _built(build: Any, *, kind: str, name: str, dataset: str | None, ignored: list[Ignored]) -> Any:
    """``build()``; ``None`` (``name`` recorded in ``ignored``) when its name is one SLayer cannot reference."""
    problem = name_problem(kind, name)
    if problem is not None:
        ignored.append(Ignored(kind=kind, name=name, reason=problem, dataset=dataset))
        return None
    return build()


def _field(field: Any, *, keys: tuple[set[str], set[str]], target: str, notes: list[ExtensionNote],
           dataset: str) -> DatasetField:
    primary, unique = keys
    expression, problem = _runnable(field.expression, target=target)
    if expression == field.name:
        expression = None  # reads the column named like the field
    built = DatasetField(
        name=field.name, expression=expression,
        datatype=_OSSIE_TYPES[field.datatype] if field.datatype else None,
        primary_key=field.name in primary, unique=field.name in unique,
        label=field.label, description=field.description,
    )
    label = f"field {field.name!r} of {dataset!r}"
    entry = read_entry(field.nonportable_extensions, kind="field", entity=field.name,
                       ossie=entry_digest(field.expression), label=label, notes=notes)
    built = apply_field_entry(built, entry, notes=notes, label=label, target=target)
    # A restored expression replaces the document form, so only its own readability counts.
    warning = problem if built.expression == expression else None
    warning = warning or _unparseable_field(built.expression, target=target, owner=f"Field {field.name!r}")
    return built.model_copy(update={"meta": with_ossie_meta(built.meta, field), "load_warning": warning})


def with_ossie_meta(meta: dict[str, Any] | None, entity: Any) -> dict[str, Any] | None:
    """``meta`` plus ``entity``'s ``ai_context`` and foreign extensions under the 1.x importer's ``osi_*`` keys."""
    out = dict(meta or {})
    if entity.ai_context is not None:
        ai = entity.ai_context
        out["osi_ai_context"] = ai.model_dump(exclude_none=True) if isinstance(ai, BaseModel) else ai
    foreign = [e.model_dump(exclude_none=True) for e in entity.nonportable_extensions or [] if not ext.is_slayer(e)]
    kept = out.pop("osi_nonportable_extensions", None)  # stashed ones core rejected, from the SLAYER entry
    foreign += [e for e in kept or [] if e not in foreign]
    if foreign:
        out["osi_nonportable_extensions"] = foreign
    return out or None


def _metric(metric: Any, *, target: str) -> DatasetMetric:
    expression, problem = _runnable_metric(metric.expression, target=target)
    warning = problem or _unparseable(portable_text(expression))
    if reads_source_measure(_portable(metric.expression)):
        warning = SOURCE_MEASURE_REASON
    return DatasetMetric(name=metric.name, expression=expression, description=metric.description,
                         datatype=_OSSIE_TYPES[metric.datatype] if metric.datatype else None, load_warning=warning)


def _dataset(dataset: OssieDataset, *, data_source: str, target: str, reader: str, document: str,
             members: set[str], restored: list[Restored], ignored: list[Ignored]) -> SlayerDataset:
    primary = set(dataset.primary_key or [])
    unique = {k[0] for k in dataset.unique_keys or [] if len(k) == 1}
    notes: list[ExtensionNote] = []
    own: list[Ignored] = []
    built_fields = [_built(lambda f=f: _field(f, keys=(primary, unique), target=target, notes=notes,
                                               dataset=dataset.name),
                           kind="field", name=f.name, dataset=dataset.name, ignored=own)
                    for f in dataset.fields or []]
    fields = [f for f in built_fields if f is not None]
    metrics = []
    for raw in dataset.metrics or []:
        if name_problem("metric", raw.name) is not None:
            _built(lambda: None, kind="metric", name=raw.name, dataset=dataset.name, ignored=own)
            continue
        label = f"metric {raw.name!r} of {dataset.name!r}"
        entry = read_entry(raw.nonportable_extensions, kind="metric", entity=raw.name,
                           ossie=entry_digest(raw.expression), label=label, notes=notes)
        core = _metric(raw, target=target)
        metric, restoration = metric_entry(core, entry, home=dataset.name, members=members, notes=notes,
                                           label=label)
        if restoration is not None:
            restored.append(restoration)
        metrics.append(metric.model_copy(update={"meta": with_ossie_meta(metric.meta, raw)}))
    built = SlayerDataset(
        name=dataset.name, data_source=data_source, description=dataset.description, fields=fields,
        semantic_model=document, metrics=metrics, **_source(dataset, reader=reader),
    )
    entry = read_entry(dataset.nonportable_extensions, kind="dataset", entity=dataset.name,
                       ossie=source_digest(dataset.source, reader=reader), label=f"dataset {dataset.name!r}",
                       notes=notes)
    built = apply_dataset_entry(built, entry, notes=notes, target=target)
    built.meta = with_ossie_meta(built.meta, dataset)
    built.extension_notes = [*notes, *(i.note() for i in own)]
    ignored.extend(own)
    return built


def _declared_relationships(datasets: dict[str, SlayerDataset], doc: SemanticModelDocument, *,
                            ignored: list[Ignored]) -> None:
    """Attach each core relationship of ``doc`` to the dataset its ``SLAYER`` entry (else its ``from``) names."""
    for rel in doc.relationships or []:
        holder = datasets.get(rel.from_dataset)
        if holder is None or rel.to not in datasets:
            continue  # it touches a dataset loaded inert
        if name_problem("relationship", rel.name) is not None:
            _built(lambda: None, kind="relationship", name=rel.name, dataset=rel.from_dataset, ignored=ignored)
            holder.extension_notes.append(ignored[-1].note())
            continue
        owner, declared = restored_relationship(rel, datasets=datasets, notes=holder.extension_notes)
        restored = declared.model_copy(update={"meta": with_ossie_meta(declared.meta, rel)})
        if _implied_from(restored, owner=owner) != rel.from_dataset:  # core's direction beats the declared one
            restored = restored.from_document(rel.from_dataset)
        datasets[owner].relationships.append(restored)


def _implied_from(relationship: DatasetRelationship, *, owner: str) -> str | None:
    """The many side a relationship's own cardinality names, if it names one."""
    if relationship.cardinality in (RelationshipCardinality.MANY_TO_ONE, RelationshipCardinality.ONE_TO_ONE):
        return owner
    return relationship.to if relationship.cardinality is RelationshipCardinality.ONE_TO_MANY else None


def _crossing_relationships(datasets: dict[str, SlayerDataset], entry: Any) -> None:
    """Attach the model's ``SLAYER`` relationships into another model's datasets to their declaring datasets."""
    by_fold = {n.casefold(): d for n, d in datasets.items()}
    for declaration in entry.payload.relationships if entry is not None else []:
        holder = by_fold.get(declaration.dataset.casefold())
        if holder is not None:
            holder.relationships.append(declaration.relationship.model_copy())


@functools.lru_cache(maxsize=256)
def _decomposed(text: str, name: str, data_source: str, ds_type: str | None,
                granularities: tuple[str, ...]) -> Decomposition:
    """``granularities``: the datasource's custom granularities as JSON (a hashable cache key)."""
    defined = {granularity_key(g.name): g for g in map(CustomGranularity.model_validate_json, granularities)}
    doc = checked_document(text=text, name=name)
    target = ossie_token(ds_type)
    reader = dialect_for_ds_type(ds_type).sqlglot_name
    restored: list[Restored] = []
    ignored: list[Ignored] = []
    members = {d.name.casefold() for d in doc.datasets}
    datasets: dict[str, SlayerDataset] = {}
    failed: list[tuple[str, Any]] = []
    for d in doc.datasets:
        try:
            found = _built(lambda d=d: _dataset(d, data_source=data_source, target=target, reader=reader,
                                                document=doc.name, members=members, restored=restored,
                                                ignored=ignored),
                           kind="dataset", name=d.name, dataset=None, ignored=ignored)
        except Exception as exc:  # noqa: BLE001 — one unloadable dataset never hides its model's others
            failed.append((d.name, exc))
            continue
        if found is not None:
            datasets[d.name] = found
    relocate_metrics(datasets, restored=restored)
    _declared_relationships(datasets, doc, ignored=ignored)
    notes: list[ExtensionNote] = []
    entry = read_entry(doc.nonportable_extensions, kind="semantic_model", entity=doc.name, ossie=doc.name,
                       label=f"semantic model {doc.name!r}", notes=notes)
    _crossing_relationships(datasets, entry)
    restore_metrics(datasets, restored=restored, dialect=reader, granularities=defined)
    metrics = []
    for m in doc.metrics or []:
        expression, problem = _runnable_metric(m.expression, target=target)
        metrics.append(ModelScopedMetric(
            name=m.name, semantic_model=doc.name, data_source=data_source, expression=expression,
            description=m.description, ai_context=m.ai_context,
            datatype=_OSSIE_TYPES[m.datatype] if m.datatype else None,
            load_warning=problem or _unparseable(portable_text(expression)),
        ))
    documented = len(metrics)
    if entry is not None:
        metrics += [ModelScopedMetric(name=m.name, semantic_model=doc.name, data_source=data_source,
                                      expression=m.expression, description=m.description, datatype=m.datatype,
                                      slayer_held=True)
                    for m in entry.payload.metrics]
    names = {m.name.casefold() for m in metrics}
    for i, metric in enumerate(metrics):
        metric.analysis = analyse_model_scoped(metric.expression, metric_names=names)
        if i < documented and metric.load_warning is None:
            metric.load_warning = load_problem(metric.analysis)
    chained = warn_unevaluable_dependents(metrics, datasets=list(datasets.values()))
    return Decomposition(name=doc.name, data_source=data_source, datasets=list(datasets.values()), metrics=metrics,
                         notes=notes, chained=chained, ignored=ignored, failed=failed)


def decompose(*, text: str, name: str, data_source: str, ds_type: str | None,
              granularities: Sequence[CustomGranularity]) -> Decomposition:
    """``text`` decomposed for a datasource of ``ds_type`` defining ``granularities``; raises
    ``OssieDocumentInvalidError`` when invalid."""
    key = tuple(g.model_dump_json() for g in granularities)
    return _decomposed(text, name, data_source, ds_type, key).model_copy(deep=True)


def _aggregated(node: exp.Expr) -> bool:
    """Inside an aggregate call, its ``FILTER`` clause or an ``AGG(d.m)`` call."""
    parent = node.parent
    while parent is not None:
        if aggregate_call(parent) or (isinstance(parent, exp.Filter) and not isinstance(parent.parent, exp.Window)):
            return True
        parent = parent.parent
    return False


def _parsed(expression: Expression) -> exp.Expr | None:
    """A model-scoped metric's portable text (else its first dialect's) parsed; ``None`` when it does not parse."""
    text = expression if isinstance(expression, str) else (
        expression.sql or next((d.expression for d in expression.dialects or []), None))
    tree, _ = parse_ossie_sql(text, OSSIE_SQL) if text else (None, None)
    return tree


def analyse_model_scoped(expression: Expression, *, metric_names: set[str]) -> ModelScopedAnalysis:
    """A model-scoped metric's components, bare metric names and problems (``wide_table_semantics.md`` §5.2)."""
    tree = _parsed(expression)
    if tree is None:
        return ModelScopedAnalysis()
    problems: list[MetricProblem] = []
    names: dict[str, None] = {}
    for column in columns(tree, OSSIE_SQL):
        name = column.name.casefold()
        if not _aggregated(column) and not column.table and name in metric_names:
            names[name] = None
            continue
        if aggregate_window_key(column):
            continue  # a grain member of the window's cells, judged when queried
        problem = _reference_problem(column)
        if problem is not None:
            problems.append(problem)
    homes: dict[str, None] = {}
    for aggregate in _components(tree):
        home, problem = _component(aggregate)
        if problem is not None:
            problems.append(problem)
        if home is not None:
            homes[home] = None
    if tree.find(exp.Window) is not None:
        problems.append(MetricProblem(code=OssieErrorCode.E_DEFERRED_WINDOWED_METRIC, reason=DEFERRED_WINDOW))
    return ModelScopedAnalysis(homes=list(homes), metrics=list(names), problems=problems)


def _reference_problem(column: exp.Column) -> MetricProblem | None:
    """Why Ossie rejects a field reference that names no metric."""
    unqualified = f"reads the unqualified field {column.name!r}; qualify it by its dataset"
    if _aggregated(column):
        return None if column.table else MetricProblem(code=OssieErrorCode.E_AMBIGUOUS_MEASURE_GRAIN,
                                                       reason=unqualified)
    if column.table:
        return MetricProblem(code=OssieErrorCode.E_UNAGGREGATED_FINER_GRAIN_REFERENCE,
                             reason=f"reads {column.table}.{column.name} outside an aggregate")
    return MetricProblem(code=OssieErrorCode.E_UNAGGREGATED_FINER_GRAIN_REFERENCE, reason=unqualified)


def _components(tree: exp.Expr) -> list[exp.Expr]:
    """The aggregate calls no other aggregate call contains, ``AGG(d.m)`` included; a call's own ``FILTER`` does not
    contain it, and the function a window applies is no aggregate call."""
    return [n for n in tree.walk() if aggregate_call(n) and not _enclosed(n)]


def _enclosed(node: exp.Expr) -> bool:
    parent = node.parent
    while parent is not None:
        if aggregate_call(parent):
            return True
        parent = parent.parent
    return False


def _component(aggregate: exp.Expr) -> tuple[str | None, MetricProblem | None]:
    """An aggregate call's home dataset, and why Ossie rejects it."""
    # A qualified star (``COUNT(orders.*)``) names its dataset too.
    tables = {c.table for c in columns(aggregate, OSSIE_SQL) if c.table} | {
        c.table for c in aggregate.find_all(exp.Column) if isinstance(c.this, exp.Star) and c.table}
    if is_agg_call(aggregate):
        return (next(iter(tables)) if len(tables) == 1 else None), None
    if not tables:
        return None, MetricProblem(code=OssieErrorCode.E_AMBIGUOUS_MEASURE_GRAIN, reason=(
            f"aggregates {aggregate.sql()} over no dataset; name its dataset, as in COUNT(<dataset>.*)"))
    if len({t.casefold() for t in tables}) > 1:
        return None, MetricProblem(code=OssieErrorCode.E_AMBIGUOUS_MEASURE_GRAIN, evaluable=True, reason=(
            f"aggregates {aggregate.sql()} over several datasets ({', '.join(sorted(tables))})"))
    home = next(iter(tables))
    clause = aggregate.parent if isinstance(aggregate.parent, exp.Filter) else None
    others = sorted({c.table for c in columns(clause.expression, OSSIE_SQL) if c.table.casefold() != home.casefold()}
                    ) if clause is not None and clause.expression is not None else []
    if others:
        return home, MetricProblem(code=OssieErrorCode.E_AMBIGUOUS_MEASURE_GRAIN, evaluable=True, reason=(
            f"the FILTER of {aggregate.sql()} reads {', '.join(others)}, not its own dataset {home}"))
    return home, None


def load_problem(analysis: ModelScopedAnalysis) -> str | None:
    """Why SLayer cannot evaluate the analysed metric: its first problem SLayer does not evaluate, a window aside."""
    return next((p.reason for p in analysis.problems
                 if not p.evaluable and p.code is not OssieErrorCode.E_DEFERRED_WINDOWED_METRIC), None)


def unevaluable_targets(metrics: list[ModelScopedMetric], *, datasets: list[SlayerDataset]) -> dict[str, tuple[str, str]]:
    """Every warned dataset metric (``d.m``) and model-scoped metric by case-folded name: ``(spelling, warning)``."""
    out = {f"{d.name}.{m.name}".casefold(): (f"{d.name}.{m.name}", m.load_warning)
           for d in datasets for m in d.metrics if m.load_warning is not None}
    out.update({m.name.casefold(): (m.name, m.load_warning) for m in metrics if m.load_warning is not None})
    return out


def _metric_reads(expression: Expression) -> set[str]:
    """The ``d.m`` and bare names a model-scoped metric reads, case-folded."""
    tree = _parsed(expression)
    return {(f"{c.table}.{c.name}" if c.table else c.name).casefold()
            for c in (tree.find_all(exp.Column) if tree is not None else [])}


def metric_graph(metrics: list[ModelScopedMetric]) -> dict[str, tuple[str, set[str]]]:
    """Each model-scoped metric by case-folded name: its spelling and what it reads."""
    return {m.name.casefold(): (m.name, _metric_reads(m.expression)) for m in metrics}


def warn_unevaluable_dependents(metrics: list[ModelScopedMetric], *, datasets: list[SlayerDataset]) -> set[str]:
    """Warn each model-scoped metric reaching an entity SLayer cannot evaluate, naming the chain; the warned names,
    case-folded."""
    targets = unevaluable_targets(metrics, datasets=datasets)
    graph = metric_graph(metrics)
    warned: set[str] = set()
    for metric in metrics:
        if metric.load_warning is not None:
            continue
        found = unevaluable_chain(graph[metric.name.casefold()][1] - {metric.name.casefold()}, targets=targets,
                                  graph=graph)
        if found is not None:
            metric.load_warning = chain_warning(*found)
            warned.add(metric.name.casefold())
    return warned


def expression_datasets(expression: Expression) -> tuple[set[str], set[str]]:
    """The datasets a model-scoped metric reads and the bare names it uses (other metrics), case-folded."""
    tree = _parsed(expression)
    if tree is None:
        return set(), set()
    columns = list(tree.find_all(exp.Column))
    return {c.table.casefold() for c in columns if c.table}, {c.name.casefold() for c in columns if not c.table}


class DocumentIndex(BaseModel):
    """A datasource's documents resolved against its namespace: what loads, who owns what, and what failed."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    data_source: str
    #: Loadable document datasets by exact name.
    datasets: dict[str, SlayerDataset] = {}
    #: Owning document of every document dataset (quarantined ones included) by exact name.
    owners: dict[str, str] = {}
    #: Owning model of every dataset by its exact name.
    members: dict[str, str] = {}
    metrics: dict[str, ModelScopedMetric] = {}
    #: Every model-scoped metric each document stores, loadable or not.
    stored_metrics: dict[str, list[ModelScopedMetric]] = {}
    #: Exact names unloadable everywhere, standalone copies included.
    quarantined: dict[str, str] = {}
    #: Exact names of document datasets that fail to load, with their errors.
    broken: dict[str, StoredDocumentLoadError] = {}
    failures: list[StoredDocumentLoadError] = []
    #: Entities loaded inert, and dataset documents no model lists (``kind`` ``dataset_document``).
    ignored: list[Ignored] = []
    #: Exact names of the 2.0 standalone datasets the datasource model presents.
    legacy: set[str] = set()

    def failure(self, *, kind: str, name: str, cause: str) -> None:
        self.failures.append(StoredDocumentLoadError(kind=kind, name=name, data_source=self.data_source,
                                                     cause=ValueError(cause)))


def build_index(*, data_source: str, documents: list[tuple[str, str]], standalone: list[str],
                ds_type: str | None, granularities: Sequence[CustomGranularity]) -> DocumentIndex:
    """Decompose ``documents`` (``(name, text)``) of ``data_source`` and quarantine every name two owners claim
    exactly (names differing only in case are distinct datasets)."""
    index = DocumentIndex(data_source=data_source)
    decomposed: list[Decomposition] = []
    for name, text in documents:
        try:
            decomposed.append(decompose(text=text, name=name, data_source=data_source, ds_type=ds_type,
                                        granularities=granularities))
        except Exception as exc:  # noqa: BLE001 — one unloadable document never hides the others
            index.failures.append(StoredDocumentLoadError(kind="semantic_model", name=name, data_source=data_source,
                                                          cause=exc))
    _quarantine(index, decomposed, standalone=standalone)
    for doc in decomposed:
        for ds in doc.datasets:
            _admit_dataset(index, ds, gone=set(index.quarantined))
    _admit_metrics(index, decomposed)
    for doc in decomposed:
        index.ignored.extend(doc.ignored)
    return index


def _quarantine(index: DocumentIndex, decomposed: list[Decomposition], *, standalone: list[str]) -> None:
    """Record each document dataset's owner, and quarantine every name two owners claim."""
    claims: dict[str, list[tuple[str, str]]] = {}
    for name in standalone:
        claims.setdefault(name, []).append(("standalone dataset", name))
    for doc in decomposed:
        for ds in [*(d.name for d in doc.datasets), *(n for n, _ in doc.failed)]:
            claims.setdefault(ds, []).append((f"semantic model {doc.name!r}", ds))
            index.owners[ds] = doc.name
            index.members[ds] = doc.name
        for ds, exc in doc.failed:
            index.broken[ds] = StoredDocumentLoadError(kind="dataset", name=ds, data_source=index.data_source,
                                                       cause=exc)
            index.failures.append(index.broken[ds])
    for claimed, owners in claims.items():
        if len(owners) > 1:
            index.quarantined[claimed] = " and ".join(f"{o} ({n!r})" for o, n in owners)
            for _, spelled in owners:
                index.failure(kind="dataset", name=spelled,
                              cause=f"{spelled!r} is declared by {index.quarantined[claimed]}; rename one of them")


def _admit_dataset(index: DocumentIndex, ds: SlayerDataset, *, gone: set[str]) -> None:
    if ds.name in gone:
        for rel in ds.relationships:
            index.failure(kind="relationship", name=rel.name or rel.to,
                          cause=f"its dataset {ds.name!r} is unloadable: {index.quarantined[ds.name]}")
        return
    kept = []
    for rel in ds.relationships:
        if rel.to in gone:
            index.failure(kind="relationship", name=rel.name or rel.to,
                          cause=f"its dataset {rel.to!r} is unloadable: {index.quarantined[rel.to]}")
        else:
            kept.append(rel)
    ds.relationships = kept
    index.datasets[ds.name] = ds


def _admit_metrics(index: DocumentIndex, decomposed: list[Decomposition]) -> None:
    """Admit every document's model-scoped metrics; one reading what is not there (an unloadable dataset too), or such
    a metric through others, fails to load."""
    metrics = [m for doc in decomposed for m in doc.metrics]
    names = {m.name.casefold() for m in metrics}
    direct = {}
    quarantined = {q.casefold() for q in index.quarantined}
    for doc in decomposed:
        index.stored_metrics[doc.name] = doc.metrics
        fields, dataset_metrics = document_names(
            [index.datasets[d.name] for d in doc.datasets if d.name in index.datasets])
        for metric in doc.metrics:
            gone = sorted(t for t in expression_datasets(metric.expression)[0] if t in quarantined)
            direct[metric.name.casefold()] = {gone[0]: (gone[0], "is unloadable")} if gone else unresolved_references(
                metric.expression, fields=fields, dataset_metrics=dataset_metrics, metric_names=names,
                document=doc.name, data_source=index.data_source, bare_metrics=metric.slayer_held)
    failed = unresolved_metrics(direct, graph=metric_graph(metrics))
    for metric in metrics:
        why = failed.get(metric.name.casefold())
        if why:
            index.failure(kind="metric", name=metric.name, cause="; ".join(why))
        else:
            index.metrics[metric.name.casefold()] = metric
