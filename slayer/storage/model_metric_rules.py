"""Save-time rules for model-scoped metrics SLayer authors: names, references (read like a query), ``AGG`` and cycles.

A stored or imported document is never judged by these rules; it loads, and what Ossie rejects at query time loads
unevaluable."""

from __future__ import annotations

import warnings

from pydantic import BaseModel
from sqlglot import exp

from slayer.core.enums import ALL_TRANSFORMS, BUILTIN_AGGREGATIONS, RESERVED_NAMES
from slayer.core.deprecation import outside_stacklevel
from slayer.core.errors import (
    AggArgumentError, AggFilterError, MetricCycleError, ModelMetricNameError, ModelMetricReferenceError,
    UnevaluableEntityError,
    UnqualifiedModelMetricReferenceError,
)
from slayer.core.field_expression import Expression, ExpressionObject, portable_text
from slayer.core.keys import RESERVED_SCALAR_NAMES
from slayer.core.ossie_validity import agg_calls, columns, is_agg_call, parse_ossie_sql
from slayer.core.models import SlayerDataset
from slayer.core.semantic_model import ModelScopedMetric
from slayer.core.syntax import WindowCall, parse_expr, unwindowable_function, walk_parsed
from slayer.storage.semantic_models import aggregate_window_key, chain_warning, unevaluable_chain

__all__ = ["DocumentScope", "check_model_metric", "metric_cycle", "metric_references", "visible_model_metrics",
           "warn_broken_model_metrics"]

_OSSIE_SQL = "OSSIE_SQL_2026"
RESERVED_WORDS = frozenset({"true", "false", "null", "and", "or", "not", "is", "in", "between", "like", "case", "when",
                            "then", "else", "end", "distinct", "filter", "where", "over", "as", "select", "from"})


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


def _check_column(column: exp.Column, *, name: str, scope: DocumentScope) -> None:
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
    if leaf in scope.fields[table]:
        if not _aggregated(column) and not aggregate_window_key(column):
            raise ModelMetricReferenceError(metric=name, reference=reference,
                                            reason="a field is read only inside an aggregate")
        return
    if leaf in scope.dataset_metrics.get(table, set()):
        raise ModelMetricReferenceError(metric=name, reference=reference,
                                        reason=f"a dataset metric is evaluated as AGG({reference})")
    raise ModelMetricReferenceError(metric=name, reference=reference,
                                    reason=f"{column.table!r} declares no field {column.name!r}")


def _check_windows(name: str, *, text: str) -> None:
    """Refuse a window applying a function Ossie does not window; text the expression reader rejects passes."""
    try:
        parsed = parse_expr(text)
    except ValueError:
        return
    for node in walk_parsed(parsed):
        refused = unwindowable_function(node) if isinstance(node, WindowCall) else None
        if refused is not None:
            raise ModelMetricReferenceError(
                metric=name, reference=text,
                reason=f"`{refused}` cannot run under OVER: a window applies a ranking or offset function or an "
                f"Ossie aggregation")


def check_model_metric(*, name: str, text: str, scope: DocumentScope) -> bool:
    """Refuse a model-scoped metric SLayer authors that breaks a naming or reference rule; ``True`` when Ossie does not
    define it (it reads another model-scoped metric by name, or an aggregate reads several datasets or filters on
    another), so it is stored in a ``SLAYER`` entry."""
    _check_name(name, scope=scope)
    tree, problem = parse_ossie_sql(text, _OSSIE_SQL)
    if tree is None:
        raise ModelMetricReferenceError(metric=name, reference=text, reason=f"it does not parse ({problem})")
    _check_windows(name, text=text)
    for call, args in agg_calls(tree):
        _check_agg(call, args, text=text, scope=scope)
    reads = {(f"{c.table}.{c.name}" if c.table else c.name).casefold() for c in tree.find_all(exp.Column)}
    found = unevaluable_chain(reads - {name.casefold()}, targets=scope.unevaluable, graph=scope.metric_graph)
    if found is not None:
        raise UnevaluableEntityError(entity=f"Metric {name!r}", reason=chain_warning(*found))
    reads_metric = False
    for column in columns(tree, _OSSIE_SQL):
        if _inside_agg(column):
            continue
        _check_column(column, name=name, scope=scope)
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


def metric_references(expression: Expression) -> tuple[set[str], set[str]]:
    """``(datasets, model-scoped metrics)`` a model-scoped metric's every form names, case-folded."""
    portable = portable_text(expression)
    texts = [portable] if portable is not None else [
        e.expression for e in (expression.dialects or [] if isinstance(expression, ExpressionObject) else [])]
    datasets: set[str] = set()
    metrics: set[str] = set()
    for text in texts:
        tree, _ = parse_ossie_sql(text, _OSSIE_SQL)
        for column in tree.find_all(exp.Column) if tree is not None else []:
            (datasets if column.table else metrics).add((column.table or column.name).casefold())
    return datasets, metrics


def _leaf_references(expression: Expression) -> set[tuple[str, str]]:
    """``(dataset, field or metric)`` of each qualified reference a model-scoped metric's every form names, folded."""
    portable = portable_text(expression)
    texts = [portable] if portable is not None else [
        e.expression for e in (expression.dialects or [] if isinstance(expression, ExpressionObject) else [])]
    out: set[tuple[str, str]] = set()
    for text in texts:
        tree, _ = parse_ossie_sql(text, _OSSIE_SQL)
        out.update((c.table.casefold(), c.name.casefold()) for c in (tree.find_all(exp.Column) if tree else [])
                   if c.table)
    return out


def warn_broken_model_metrics(
    *, dataset: SlayerDataset, stored: SlayerDataset | None, metrics: list[ModelScopedMetric],
) -> None:
    """Warn once per model-scoped metric reading a field or metric of ``dataset`` that the save removes."""
    if stored is None:
        return
    kept = {n.casefold() for n in [*(f.name for f in dataset.fields), *(m.name for m in dataset.metrics)]}
    removed = {n.casefold() for n in [*(f.name for f in stored.fields), *(m.name for m in stored.metrics)]} - kept
    home = dataset.name.casefold()
    for metric in metrics:
        for table, leaf in sorted(_leaf_references(metric.expression)):
            if table == home and leaf in removed:
                warnings.warn(
                    f"Saving dataset {dataset.name!r}: model-scoped metric {metric.name!r} of semantic model "
                    f"{metric.semantic_model!r} reads {dataset.name}.{leaf}, which the save removes.",
                    UserWarning, stacklevel=outside_stacklevel(),
                )


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
