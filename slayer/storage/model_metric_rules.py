"""Rules for model-scoped metrics: the save-time rules for those SLayer authors (names, references read like a query,
``AGG`` and cycles), and the one resolver of their references that saves, loads and guards share.

A stored or imported document is never judged by the authoring rules; it loads, and what Ossie rejects at query time
loads unevaluable."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import AbstractSet

from pydantic import BaseModel
from sqlglot import exp

from slayer.core.enums import ALL_TRANSFORMS, BUILTIN_AGGREGATIONS, RESERVED_NAMES
from slayer.core.errors import (
    AggArgumentError, AggFilterError, MetricCycleError, ModelMetricNameError, ModelMetricReferenceError,
    UnevaluableEntityError, UnqualifiedModelMetricReferenceError,
)
from slayer.core.field_expression import Expression, ExpressionObject, portable_text
from slayer.core.keys import RESERVED_SCALAR_NAMES
from slayer.core.ossie_validity import agg_calls, columns, is_agg_call, parse_ossie_sql
from slayer.core.models import SlayerDataset
from slayer.core.semantic_model import ModelScopedMetric

__all__ = ["WINDOW_WARNING", "DocumentScope", "chain_warning", "check_model_metric", "metric_cycle",
           "metric_references", "unevaluable_chain", "unresolved_metrics", "unresolved_references",
           "visible_model_metrics"]

WINDOW_WARNING = "uses a window function (OVER), which SLayer cannot evaluate yet"
_OSSIE_SQL = "OSSIE_SQL_2026"
RESERVED_WORDS = frozenset({"true", "false", "null", "and", "or", "not", "is", "in", "between", "like", "case", "when",
                            "then", "else", "end", "distinct", "filter", "where", "over", "as", "select", "from"})

#: A metric's unresolved references by case-folded spelling: ``(spelling, why)``, read as "reads <spelling>, which <why>".
Unresolved = dict[str, tuple[str, str]]


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


class DocumentScope(BaseModel):
    """What a model-scoped metric of one document may name; every name case-folded."""

    #: This document's datasets: their field and dataset-metric names.
    fields: dict[str, set[str]]
    dataset_metrics: dict[str, set[str]]
    #: Every dataset name of the datasource (any document or standalone).
    datasource_datasets: set[str]
    #: Every model-scoped metric name of the datasource, this edit's included.
    model_metrics: set[str]
    #: Entities SLayer cannot evaluate (this document's ``d.m``, or a datasource metric name): spelling and warning.
    unevaluable: dict[str, tuple[str, str]] = {}
    #: The datasource's model-scoped metrics this edit keeps: spelling and what each reads.
    metric_graph: dict[str, tuple[str, set[str]]] = {}


def _aggregated(node: exp.Expr) -> bool:
    parent = node.parent
    while parent is not None:
        if isinstance(parent, exp.AggFunc | exp.Filter) and not is_agg_call(parent):
            return True
        parent = parent.parent
    return False


def _inside_agg(node: exp.Expr) -> bool:
    parent = node.parent
    while parent is not None:
        if is_agg_call(parent):
            return True
        parent = parent.parent
    return False


def _check_name(name: str, *, scope: DocumentScope) -> None:
    folded = name.casefold()
    if folded in RESERVED_WORDS:
        raise ModelMetricNameError(name=name, reason="it is a reserved word")
    if folded in BUILTIN_AGGREGATIONS | RESERVED_SCALAR_NAMES | RESERVED_NAMES | ALL_TRANSFORMS:
        raise ModelMetricNameError(name=name, reason="it is a library function name")
    if folded in scope.datasource_datasets:
        raise ModelMetricNameError(name=name, reason="a dataset of the datasource has that name")


def _check_agg(call: exp.Expr, args: list[exp.Expr], *, text: str, scope: DocumentScope) -> None:
    if isinstance(call.parent, exp.Filter):
        raise AggFilterError(expression=text)
    arg = args[0] if len(args) == 1 else None
    if not isinstance(arg, exp.Column):
        raise AggArgumentError(expression=text, reason="its argument is not a single dataset.metric reference")
    if not arg.table:
        what = "a model-scoped metric" if arg.name.casefold() in scope.model_metrics else "not qualified"
        raise AggArgumentError(expression=text, reason=f"{arg.name!r} is {what}")
    table, leaf = arg.table.casefold(), arg.name.casefold()
    if table not in scope.fields:
        raise AggArgumentError(expression=text, reason=f"{arg.table!r} is not a dataset of this semantic model")
    if leaf in scope.fields[table]:
        raise AggArgumentError(expression=text, reason=f"{arg.table}.{arg.name} is a field")
    if leaf not in scope.dataset_metrics.get(table, set()):
        raise AggArgumentError(expression=text, reason=f"{arg.name!r} is not a metric of {arg.table!r}")


def _check_column(column: exp.Column, *, name: str, scope: DocumentScope, unresolved: Unresolved) -> None:
    table, leaf = column.table.casefold(), column.name.casefold()
    reference = f"{column.table}.{column.name}" if column.table else column.name
    if not table:
        if leaf in scope.model_metrics:
            return
        if _aggregated(column):
            raise UnqualifiedModelMetricReferenceError(metric=name, reference=reference)
        raise ModelMetricReferenceError(metric=name, reference=reference,
                                        reason="it is neither a model-scoped metric nor a qualified field")
    if table not in scope.fields:
        if table == "source":
            reason = "a model-scoped metric reads fields, never source columns"
        elif table in scope.datasource_datasets:
            reason = f"{column.table!r} is not a dataset of this semantic model"
        else:
            reason = f"{column.table!r} is not a dataset"
        raise ModelMetricReferenceError(metric=name, reference=reference, reason=reason)
    if f"{table}.{leaf}" in unresolved:
        reason = (f"a dataset metric is evaluated as AGG({reference})" if leaf in scope.dataset_metrics.get(table, set())
                  else f"{column.table!r} declares no field {column.name!r}")
        raise ModelMetricReferenceError(metric=name, reference=reference, reason=reason)
    if not _aggregated(column):
        raise ModelMetricReferenceError(metric=name, reference=reference,
                                        reason="a field is read only inside an aggregate")


def check_model_metric(*, name: str, text: str, scope: DocumentScope) -> bool:
    """Refuse a model-scoped metric SLayer authors that breaks a naming or reference rule; ``True`` when Ossie does not
    define it (it reads another model-scoped metric by name, or an aggregate reads several datasets or filters on
    another), so it is stored in a ``SLAYER`` entry."""
    _check_name(name, scope=scope)
    tree, problem = parse_ossie_sql(text, _OSSIE_SQL)
    if tree is None:
        raise ModelMetricReferenceError(metric=name, reference=text, reason=f"it does not parse ({problem})")
    if tree.find(exp.Window) is not None:
        raise UnevaluableEntityError(entity=f"Metric {name!r}", reason=WINDOW_WARNING)
    for call, args in agg_calls(tree):
        _check_agg(call, args, text=text, scope=scope)
    reads = {(f"{c.table}.{c.name}" if c.table else c.name).casefold() for c in tree.find_all(exp.Column)}
    found = unevaluable_chain(reads - {name.casefold()}, targets=scope.unevaluable, graph=scope.metric_graph)
    if found is not None:
        raise UnevaluableEntityError(entity=f"Metric {name!r}", reason=chain_warning(*found))
    unresolved = unresolved_references(text, fields=scope.fields, dataset_metrics=scope.dataset_metrics,
                                       metric_names=scope.model_metrics, document="", data_source="")
    reads_metric = False
    for column in columns(tree, _OSSIE_SQL):
        if _inside_agg(column):
            continue
        _check_column(column, name=name, scope=scope, unresolved=unresolved)
        reads_metric = reads_metric or (not column.table and column.name.casefold() in scope.model_metrics)
    return reads_metric or any(_beyond_ossie(aggregate) for aggregate in tree.find_all(exp.AggFunc)
                               if not is_agg_call(aggregate) and not _aggregated(aggregate))


def _beyond_ossie(aggregate: exp.Expr) -> bool:
    """Whether an aggregate reads several datasets, or filters on a dataset it does not read."""
    homes = {c.table.casefold() for c in aggregate.find_all(exp.Column) if c.table}
    filtered = ({c.table.casefold() for c in aggregate.parent.expression.find_all(exp.Column) if c.table}
                if isinstance(aggregate.parent, exp.Filter) else set())
    return len(homes) > 1 or bool(filtered - homes)


def _cycle_path(path: list[str], nxt: str) -> list[str]:
    return [*path[path.index(nxt):], nxt] if nxt in path else [*path, nxt]


def metric_cycle(dependencies: dict[str, set[str]]) -> None:
    """Refuse a cycle among model-scoped metrics (``name → names it reads``, case-folded)."""
    state: dict[str, int] = {}

    def visit(node: str, path: list[str]) -> None:
        state[node] = 1
        for nxt in sorted(dependencies.get(node, set())):
            if state.get(nxt) == 1:
                raise MetricCycleError(_cycle_path(path, nxt=nxt))
            if nxt in dependencies and state.get(nxt) is None:
                visit(nxt, [*path, nxt])
        state[node] = 2

    for start in sorted(dependencies):
        if state.get(start) is None:
            visit(start, [start])


def _texts(expression: Expression) -> list[str]:
    """A model-scoped metric's portable text, else every dialect entry's."""
    portable = portable_text(expression)
    return [portable] if portable is not None else [
        e.expression for e in (expression.dialects or [] if isinstance(expression, ExpressionObject) else [])]


def metric_references(expression: Expression) -> tuple[set[str], set[str]]:
    """``(datasets, model-scoped metrics)`` a model-scoped metric's every form names, case-folded."""
    datasets: set[str] = set()
    metrics: set[str] = set()
    for text in _texts(expression):
        tree, _ = parse_ossie_sql(text, _OSSIE_SQL)
        for column in tree.find_all(exp.Column) if tree is not None else []:
            (datasets if column.table else metrics).add((column.table or column.name).casefold())
    return datasets, metrics


def _unresolved(column: exp.Column, *, fields: Mapping[str, AbstractSet[str]],
                dataset_metrics: Mapping[str, AbstractSet[str]], metric_names: AbstractSet[str],
                gone_metrics: AbstractSet[str], document: str, data_source: str) -> tuple[str, str, str] | None:
    """``(key, spelling, why)`` when ``column`` names what is not there."""
    table, leaf = column.table.casefold(), column.name.casefold()
    if not table:
        if _aggregated(column) or _inside_agg(column) or leaf in metric_names or leaf not in gone_metrics:
            return None  # any other bare name is an unqualified field, which Ossie rejects as such
        return leaf, column.name, f"is not a model-scoped metric of datasource {data_source!r}"
    if table not in fields:
        return None if table == "source" else (
            table, column.table, f"is not a dataset of semantic model {document!r}")
    if isinstance(column.this, exp.Star):
        return None
    key = f"{table}.{leaf}"
    if _inside_agg(column):
        return None if leaf in dataset_metrics.get(table, set()) else (
            key, f"AGG({column.table}.{column.name})", f"{column.table!r} declares no metric {column.name!r}")
    if leaf in fields[table]:
        return None
    if leaf in dataset_metrics.get(table, set()):
        return key, f"{column.table}.{column.name}", f"is a dataset metric, evaluated as AGG({column.table}.{column.name})"
    return key, f"{column.table}.{column.name}", f"{column.table!r} declares no field {column.name!r}"


def unresolved_references(expression: Expression, *, fields: Mapping[str, AbstractSet[str]],
                          dataset_metrics: Mapping[str, AbstractSet[str]], metric_names: AbstractSet[str],
                          document: str, data_source: str, gone_metrics: AbstractSet[str] = frozenset()) -> Unresolved:
    """The references of a model-scoped metric of ``document`` that its datasets (``fields`` and ``dataset_metrics``
    by dataset) and the datasource's ``metric_names`` do not resolve, every name case-folded; a bare name is a metric
    reference only while it names a metric, or one of ``gone_metrics`` a change removes. Never raises."""
    out: Unresolved = {}
    for text in _texts(expression):
        tree, _ = parse_ossie_sql(text, _OSSIE_SQL)
        if tree is None:
            continue
        stars = (c for c in tree.find_all(exp.Column) if isinstance(c.this, exp.Star) and c.table)
        for column in (*columns(tree, _OSSIE_SQL), *stars):
            found = _unresolved(column, fields=fields, dataset_metrics=dataset_metrics, metric_names=metric_names,
                                gone_metrics=gone_metrics, document=document, data_source=data_source)
            if found is not None:
                out.setdefault(found[0], (found[1], found[2]))
    return out


def document_names(datasets: Sequence[SlayerDataset]) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """``(fields, dataset metrics)`` of a document's ``datasets`` by dataset, case-folded."""
    return ({d.name.casefold(): {f.name.casefold() for f in d.fields} for d in datasets},
            {d.name.casefold(): {m.name.casefold() for m in d.metrics} for d in datasets})


def unresolved_metrics(direct: Mapping[str, Unresolved], *,
                       graph: Mapping[str, tuple[str, set[str]]]) -> dict[str, list[str]]:
    """Each model-scoped metric (case-folded) with ``direct`` unresolved references, or reading one through others
    (``graph``: folded name → spelling, reads): why, naming the chain for the latter."""
    out = {k: [chain_warning([s], why) for s, why in v.values()] for k, v in direct.items() if v}
    targets = {}
    for k, v in direct.items():
        if v and k in graph:
            spelling, why = v[min(v)]
            targets[k] = (f"{graph[k][0]} → {spelling}", why)
    for name, (_, reads) in graph.items():
        found = None if name in out else unevaluable_chain(reads - {name}, targets=targets, graph=dict(graph))
        if found is not None:
            out[name] = [chain_warning(*found)]
    return out


def visible_model_metrics(metrics: list[ModelScopedMetric], *, hidden_datasets: set[str]) -> list[ModelScopedMetric]:
    """``metrics`` without any that reads a hidden dataset or such a metric, transitively."""
    references = {m.name.casefold(): metric_references(m.expression) for m in metrics}
    hidden: set[str] = set()
    changed = True
    while changed:
        changed = False
        for name, (datasets, names) in references.items():
            if name not in hidden and (datasets & hidden_datasets or names & hidden):
                hidden.add(name)
                changed = True
    return [m for m in metrics if m.name.casefold() not in hidden]
