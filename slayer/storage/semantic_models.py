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
from slayer.core.enums import DataType
from slayer.core.errors import (
    FieldExpressionError, OssieDocumentInvalidError, OssieErrorCode, StoredDocumentLoadError, UnreadableDocumentError,
)
from slayer.core.field_expression import (
    DialectEntry, Expression, ExpressionObject, portable_text, running_form, token,
)
from slayer.core.granularity import CustomGranularity, granularity_key
from slayer.core.syntax import parse_expr
from slayer.core import slayer_extension as ext
from slayer.core.models import DatasetField, DatasetMetric, ExtensionNote, SlayerDataset
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
from slayer.storage.document_mapping import core_source
from slayer.storage.document_extensions import (
    Restored, apply_dataset_entry, apply_field_entry, digest as entry_digest, metric_entry, read_entry,
    relocate_metrics, restore_metrics, restored_relationship,
)

__all__ = ["Decomposition", "DocumentIndex", "aggregate_window_key", "analyse_model_scoped", "build_index", "chain_warning", "checked_document",
           "decompose", "expression_datasets", "load_problem", "metric_graph", "unevaluable_chain", "unevaluable_targets",
           "warn_unevaluable_dependents"]

_OSSIE_TYPES = {t.value: t for t in DataType if t is not DataType.ARRAY}
#: Why Ossie defers a model-scoped metric holding a window (``E_DEFERRED_WINDOWED_METRIC``).
DEFERRED_WINDOW = "uses a window function (OVER), which Ossie defers in a model-scoped metric"


class Decomposition(BaseModel):
    """One document's native datasets and model-scoped metrics."""

    name: str
    data_source: str
    datasets: list[SlayerDataset]
    metrics: list[ModelScopedMetric]
    notes: list[ExtensionNote] = []
    #: Case-folded metrics warned only for reading an unevaluable entity through another.
    chained: set[str] = set()


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
             members: set[str], restored: list[Restored]) -> SlayerDataset:
    primary = set(dataset.primary_key or [])
    unique = {k[0] for k in dataset.unique_keys or [] if len(k) == 1}
    notes: list[ExtensionNote] = []
    fields = [_field(f, keys=(primary, unique), target=target, notes=notes, dataset=dataset.name)
              for f in dataset.fields or []]
    metrics = []
    for raw in dataset.metrics or []:
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
        semantic_model=document, metrics=metrics, **_source(dataset),
    )
    entry = read_entry(dataset.nonportable_extensions, kind="dataset", entity=dataset.name,
                       ossie=source_digest(dataset.source, reader=reader), label=f"dataset {dataset.name!r}",
                       notes=notes)
    built = apply_dataset_entry(built, entry, notes=notes, target=target)
    built.meta = with_ossie_meta(built.meta, dataset)
    built.extension_notes = notes
    return built


@functools.lru_cache(maxsize=256)
def _decomposed(text: str, name: str, data_source: str, ds_type: str | None,
                granularities: tuple[str, ...]) -> Decomposition:
    """``granularities``: the datasource's custom granularities as JSON (a hashable cache key)."""
    defined = {granularity_key(g.name): g for g in map(CustomGranularity.model_validate_json, granularities)}
    doc = checked_document(text=text, name=name)
    target = ossie_token(ds_type)
    reader = dialect_for_ds_type(ds_type).sqlglot_name
    restored: list[Restored] = []
    members = {d.name.casefold() for d in doc.datasets}
    datasets = {d.name: _dataset(d, data_source=data_source, target=target, reader=reader, document=doc.name,
                                 members=members, restored=restored) for d in doc.datasets}
    relocate_metrics(datasets, restored=restored)
    for rel in doc.relationships or []:
        owner, declared = restored_relationship(rel, datasets=datasets, notes=datasets[rel.from_dataset].extension_notes)
        datasets[owner].relationships.append(
            declared.model_copy(update={"meta": with_ossie_meta(declared.meta, rel)}).from_document(rel.from_dataset))
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
    notes: list[ExtensionNote] = []
    entry = read_entry(doc.nonportable_extensions, kind="semantic_model", entity=doc.name, ossie=doc.name,
                       label=f"semantic model {doc.name!r}", notes=notes)
    documented = len(metrics)
    if entry is not None:
        metrics += [ModelScopedMetric(name=m.name, semantic_model=doc.name, data_source=data_source,
                                      expression=m.expression, description=m.description, datatype=m.datatype)
                    for m in entry.payload.metrics]
    names = {m.name.casefold() for m in metrics}
    for i, metric in enumerate(metrics):
        metric.analysis = analyse_model_scoped(metric.expression, metric_names=names)
        if i < documented and metric.load_warning is None:
            metric.load_warning = load_problem(metric.analysis)
    chained = warn_unevaluable_dependents(metrics, datasets=list(datasets.values()))
    return Decomposition(name=doc.name, data_source=data_source, datasets=list(datasets.values()), metrics=metrics,
                         notes=notes, chained=chained)


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
        if _aggregate_call(parent) or (isinstance(parent, exp.Filter) and not isinstance(parent.parent, exp.Window)):
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


def aggregate_window_key(column: exp.Column) -> bool:
    """Whether ``column`` is a partition or order key of a window over an aggregate."""
    node: exp.Expr = column
    while node.parent is not None:
        parent = node.parent
        if isinstance(parent, exp.Window) and node is not parent.this:
            head = _unfiltered(parent.this) if parent.this is not None else None
            return any(_aggregate_call(n) for n in parent.walk() if n is not parent.this and n is not head)
        node = parent
    return False


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
    return [n for n in tree.walk() if _aggregate_call(n) and not _enclosed(n)]


def _aggregate_call(node: exp.Expr) -> bool:
    if isinstance(_unfiltered(node).parent, exp.Window):
        return False
    return isinstance(node, exp.AggFunc) or is_agg_call(node)


def _unfiltered(node: exp.Expr) -> exp.Expr:
    return node.parent if isinstance(node.parent, exp.Filter) and node.parent.this is node else node


def _enclosed(node: exp.Expr) -> bool:
    parent = node.parent
    while parent is not None:
        if _aggregate_call(parent):
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


def unevaluable_chain(reads: set[str], *, targets: dict[str, tuple[str, str]],
                      graph: dict[str, tuple[str, set[str]]]) -> tuple[list[str], str] | None:
    """The shortest chain from ``reads`` through model-scoped metrics (``graph``: folded name → spelling, reads) to an
    unevaluable target, and that target's warning; cycle-safe."""
    frontier = [(name, []) for name in sorted(reads)]
    seen: set[str] = set()
    while frontier:
        nxt = []
        for name, path in frontier:
            if name in seen:
                continue
            seen.add(name)
            if name in targets:
                spelling, warning = targets[name]
                return [*path, spelling], warning
            if name in graph:
                spelling, further = graph[name]
                nxt += [(n, [*path, spelling]) for n in sorted(further)]
        frontier = nxt
    return None


def chain_warning(chain: list[str], warning: str) -> str:
    return f"reads {' → '.join(chain)}, which {warning}"


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
                ds_type: str | None, granularities: Sequence[CustomGranularity]) -> DocumentIndex:
    """Decompose ``documents`` (``(name, text)``) of ``data_source`` and quarantine every name two owners claim."""
    index = DocumentIndex(data_source=data_source)
    decomposed: list[Decomposition] = []
    for name, text in documents:
        try:
            decomposed.append(decompose(text=text, name=name, data_source=data_source, ds_type=ds_type,
                                        granularities=granularities))
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
        _admit_dataset(index, ds, gone=gone)
    _admit_metrics(index, doc, gone=gone, known=known)


def _admit_dataset(index: DocumentIndex, ds: SlayerDataset, *, gone: set[str]) -> None:
    if ds.name.casefold() in gone:
        for rel in ds.relationships:
            index.failure(kind="relationship", name=rel.name or rel.to,
                          cause=f"its dataset {ds.name!r} is unloadable: {index.quarantined[ds.name.casefold()]}")
        return
    kept = []
    for rel in ds.relationships:
        if rel.to.casefold() in gone:
            index.failure(kind="relationship", name=rel.name or rel.to,
                          cause=f"its dataset {rel.to!r} is unloadable: {index.quarantined[rel.to.casefold()]}")
        else:
            kept.append(rel)
    ds.relationships = kept
    index.datasets[ds.name.casefold()] = ds


def _admit_metrics(index: DocumentIndex, doc: Decomposition, *, gone: set[str], known: set[str]) -> None:
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
