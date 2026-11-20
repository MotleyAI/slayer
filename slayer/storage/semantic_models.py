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
from slayer.core.models import DatasetField, DatasetMetric, DatasetRelationship, SlayerDataset
from slayer.core.ossie_validity import parse_ossie_sql
from slayer.core.semantic_model import (
    ModelScopedMetric, OssieDataset, OssieExpression, SemanticModelDocument, ossie_validity_errors,
)
from slayer.sql.dialects import ossie_token
from slayer.sql.ossie_dialects import ANSI_SQL, OSSIE_SQL, Candidate, read, reader_for, render
from slayer.sql.ossie_source import parse_source

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


def _source(dataset: OssieDataset) -> dict[str, Any]:
    parsed = parse_source(dataset.source)
    return {"sql": parsed.query} if parsed.is_query else {"sql_table": dataset.source.strip()}


def _field(field: Any, *, keys: tuple[set[str], set[str]], target: str) -> DatasetField:
    primary, unique = keys
    expression, problem = _runnable(field.expression, target=target)
    warning = WINDOW_WARNING if _windowed(field.expression) else problem
    return DatasetField(
        name=field.name, expression=expression,
        datatype=_OSSIE_TYPES[field.datatype] if field.datatype else None,
        primary_key=field.name in primary, unique=field.name in unique,
        label=field.label, description=field.description, load_warning=warning,
    )


def _metric(metric: Any, *, target: str, windowed_fields: set[str]) -> DatasetMetric:
    expression, problem = _runnable(metric.expression, target=target)
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


def _dataset(dataset: OssieDataset, *, data_source: str, target: str) -> SlayerDataset:
    primary = set(dataset.primary_key or [])
    unique = {k[0] for k in dataset.unique_keys or [] if len(k) == 1}
    fields = [_field(f, keys=(primary, unique), target=target) for f in dataset.fields or []]
    windowed = {f.name.casefold() for f in fields if f.load_warning == WINDOW_WARNING}
    return SlayerDataset(
        name=dataset.name, data_source=data_source, description=dataset.description, fields=fields,
        metrics=[_metric(m, target=target, windowed_fields=windowed) for m in dataset.metrics or []],
        **_source(dataset),
    )


@functools.lru_cache(maxsize=256)
def _decomposed(text: str, name: str, data_source: str, target: str) -> Decomposition:
    doc = checked_document(text=text, name=name)
    datasets = {d.name: _dataset(d, data_source=data_source, target=target) for d in doc.datasets}
    for rel in doc.relationships or []:
        datasets[rel.from_dataset].relationships.append(DatasetRelationship(
            name=rel.name, to=rel.to, from_columns=list(rel.from_columns), to_columns=list(rel.to_columns),
            cardinality=RelationshipCardinality.MANY_TO_ONE, join_type=JoinType.LEFT,
        ))
    metrics = []
    for m in doc.metrics or []:
        expression, problem = _runnable(m.expression, target=target)
        metrics.append(ModelScopedMetric(
            name=m.name, semantic_model=doc.name, data_source=data_source, expression=expression,
            description=m.description, ai_context=m.ai_context,
            datatype=_OSSIE_TYPES[m.datatype] if m.datatype else None,
            load_warning=WINDOW_WARNING if _windowed(m.expression) else problem,
        ))
    return Decomposition(name=doc.name, data_source=data_source, datasets=list(datasets.values()), metrics=metrics)


def decompose(*, text: str, name: str, data_source: str, ds_type: str | None) -> Decomposition:
    """``text`` decomposed for a datasource of ``ds_type``; raises ``OssieDocumentInvalidError`` when invalid."""
    return _decomposed(text, name, data_source, ossie_token(ds_type)).model_copy(deep=True)


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
    for doc in decomposed:
        _admit(index, doc)
    return index


def _admit(index: DocumentIndex, doc: Decomposition) -> None:
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
    index.metrics.update(pending)
