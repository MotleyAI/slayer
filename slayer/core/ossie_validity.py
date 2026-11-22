"""Ossie's semantic validity rules for a schema-valid semantic-model document (upstream ``validate.py``'s errors).

Names compare case-insensitively (queries/identifier-case), so a case-only collision is an error here.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from typing import Any

import sqlglot
from pydantic import BaseModel
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import ParseError, TokenError

__all__ = ["PORTABLE_DIALECT", "SOURCE_MEASURE_REASON", "SQLGLOT_DIALECTS", "agg_arguments", "agg_calls", "columns",
           "is_agg_call", "parse_ossie_sql", "portable_construct_problem", "portable_problem", "reads_source_measure",
           "semantic_errors", "source_measure", "unwindowed_aggregates"]

PORTABLE_DIALECT = "OSSIE_SQL_2026"
#: Ossie dialect tokens upstream syntax-checks, each with its sqlglot reader (``None`` = the default reader).
SQLGLOT_DIALECTS: dict[str, str | None] = {
    "ANSI_SQL": None, PORTABLE_DIALECT: None, "SNOWFLAKE": "snowflake", "DATABRICKS": "databricks",
    "BIGQUERY": "bigquery", "TRINO": "trino", "CLICKHOUSE": "clickhouse", "POSTGRES": "postgres",
    "REDSHIFT": "redshift",
}
_SOURCE = "source"
_DATE_PART_FUNCTIONS = {"DATEADD", "DATE_PART", "DATEPART", "PGDATE_PART", "TIMESTAMPADD"}
_UNIT_SLOT = (exp.DateAdd, exp.DateDiff, exp.TimestampDiff)
_STATEMENTS = (exp.Create, exp.Drop, exp.Delete, exp.Insert, exp.Update, exp.Merge, exp.Alter, exp.Command,
               exp.Grant, exp.Use, exp.Set, exp.Transaction, exp.Commit, exp.Rollback)


class _Expr(BaseModel):
    kind: str
    dataset: str
    name: str
    dialect: str
    text: str
    superseded: bool = False

    def label(self, model: str) -> str:
        what = {"field": f"Field '{self.dataset}.{self.name}'", "dataset_metric": f"Metric '{self.dataset}.{self.name}'"}
        return f"{what.get(self.kind, f'Metric {self.name!r}')} in model '{model}' ({self.dialect})"


def _fold(name: Any) -> str | None:
    return name.casefold() if isinstance(name, str) and name else None


def _duplicates(names: list[str]) -> list[str]:
    """Names declared more than once, compared case-insensitively, in first-seen spelling."""
    first: dict[str, str] = {}
    counts = Counter(n.casefold() for n in names)
    for n in names:
        first.setdefault(n.casefold(), n)
    return [first[k] for k, c in counts.items() if c > 1]


def _names(items: list[dict[str, Any]] | None) -> list[str]:
    return [i["name"] for i in items or [] if isinstance(i.get("name"), str)]


def _unique_names(data: dict[str, Any], model: str) -> list[str]:
    out = [f"[Unique] Duplicate dataset name '{d}' in model '{model}'" for d in _duplicates(_names(data["datasets"]))]
    for dataset in data["datasets"]:
        fields, metrics = _names(dataset.get("fields")), _names(dataset.get("metrics"))
        where = f"dataset '{dataset['name']}'"
        out += [f"[Unique] Duplicate field name '{d}' in {where}" for d in _duplicates(fields)]
        out += [f"[Unique] Duplicate metric name '{d}' in {where}" for d in _duplicates(metrics)]
        shared = {f.casefold(): f for f in fields}
        out += [f"[Unique] Name '{shared[m.casefold()]}' in {where} is used by both a field and a dataset-scoped "
                "metric; a dataset's fields and metrics share one namespace"
                for m in dict.fromkeys(metrics) if m.casefold() in shared]
    out += [f"[Unique] Duplicate metric name '{d}' in model '{model}'" for d in _duplicates(_names(data.get("metrics")))]
    out += [f"[Unique] Duplicate relationship name '{d}' in model '{model}'"
            for d in _duplicates(_names(data.get("relationships")))]
    return out


def _field_names(dataset: dict[str, Any] | None) -> set[str]:
    return {n.casefold() for n in _names((dataset or {}).get("fields"))}


def _undeclared(used: list[str], declared: set[str]) -> list[str]:
    return list(dict.fromkeys(n for n in used if n.casefold() not in declared))


def _missing(prefix: str, where: str, names: list[str], owner: str) -> str:
    listed = ", ".join(repr(n) for n in sorted(names))
    return (f"[Reference] {prefix}: {where} references undeclared {'field' if len(names) == 1 else 'fields'} "
            f"{listed}. {where} must be field names of {owner}.")


def _dataset_keys(dataset: dict[str, Any], model: str) -> list[str]:
    out: list[str] = []
    declared = _field_names(dataset)
    prefix = f"Dataset '{dataset['name']}' in model '{model}'"
    owner = f"dataset '{dataset['name']}'"
    for label, key in [("primary_key", dataset.get("primary_key")),
                       *[(f"unique_keys {k}", k) for k in dataset.get("unique_keys") or []]]:
        if key:
            out += [f"[Unique] {prefix}: {label} names field '{d}' more than once" for d in _duplicates(key)]
    if missing := _undeclared(dataset.get("primary_key") or [], declared):
        out.append(_missing(prefix, "primary_key", missing, owner))
    if missing := _undeclared([n for k in dataset.get("unique_keys") or [] for n in k], declared):
        out.append(_missing(prefix, "unique_keys", missing, owner))
    return out


def _relationship_references(rel: dict[str, Any], *, by_name: dict[str, dict[str, Any]], model: str) -> list[str]:
    out: list[str] = []
    prefix = f"Relationship '{rel['name']}' in model '{model}'"
    for end in ("from", "to"):
        target = by_name.get(rel[end])
        if target is None:
            out.append(f"[Reference] {prefix} references unknown dataset '{rel[end]}'")
        elif missing := _undeclared(rel[f"{end}_columns"], _field_names(target)):
            out.append(_missing(prefix, f"{end}_columns", missing, f"the {end} dataset '{rel[end]}'"))
    if len(rel["from_columns"]) != len(rel["to_columns"]):
        out.append(f"[Arity] {prefix}: from_columns ({len(rel['from_columns'])}) and to_columns "
                   f"({len(rel['to_columns'])}) must have the same number of columns")
    return out


def _keys_and_references(data: dict[str, Any], model: str) -> list[str]:
    out: list[str] = []
    by_name = {d["name"]: d for d in data["datasets"]}
    for dataset in data["datasets"]:
        out += _dataset_keys(dataset, model=model)
    for rel in data.get("relationships") or []:
        out += _relationship_references(rel, by_name=by_name, model=model)
    return out


def _expressions(member: dict[str, Any], kind: str, dataset: str) -> Iterator[_Expr]:
    expression = member["expression"]
    sql = expression.get("sql")
    if sql is not None:
        yield _Expr(kind=kind, dataset=dataset, name=member["name"], dialect=PORTABLE_DIALECT, text=sql)
    for entry in expression.get("dialects") or []:
        yield _Expr(kind=kind, dataset=dataset, name=member["name"], dialect=entry["dialect"],
                    text=entry["expression"], superseded=entry["dialect"] == PORTABLE_DIALECT and sql is not None)


def _all_expressions(data: dict[str, Any]) -> Iterator[_Expr]:
    for dataset in data["datasets"]:
        for f in dataset.get("fields") or []:
            yield from _expressions(f, "field", dataset["name"])
        for m in dataset.get("metrics") or []:
            yield from _expressions(m, "dataset_metric", dataset["name"])
    for m in data.get("metrics") or []:
        yield from _expressions(m, "model_metric", "")


def parse_ossie_sql(text: str, dialect: str) -> tuple[exp.Expr | None, str | None]:
    """``text`` parsed as one expression of Ossie ``dialect``: ``(tree, None)``, or ``(None, why)``;
    ``(None, None)`` when the dialect is not one upstream syntax-checks."""
    if dialect not in SQLGLOT_DIALECTS:
        return None, None
    reader = SQLGLOT_DIALECTS[dialect]
    try:
        statements = [s for s in sqlglot.parse(text, read=reader) if s is not None]
    except (ParseError, TokenError, RecursionError):
        statements = None
    if statements is not None:
        if not statements:
            return None, "expression is empty"
        if len(statements) > 1:
            return None, f"expected a single expression but found {len(statements)} statements"
        tree = statements[0]
        if not isinstance(tree, _STATEMENTS):
            return tree, None
    try:
        wrapped = sqlglot.parse(f"SELECT {text}", read=reader)
    except (ParseError, TokenError) as e:
        return None, str(e).split("\n")[0]
    except RecursionError:
        return None, "expression is too deeply nested to parse"
    real = [s for s in wrapped if s is not None]
    if len(real) == 1 and isinstance(real[0], exp.Select) and len(real[0].expressions) == 1 \
            and not real[0].args.get("from_"):
        return real[0].expressions[0], None
    return None, "expected an expression, found a statement"


def _is_date_part_name(name: str) -> bool:
    parts = Dialect.DATE_PART_MAPPING
    return name.upper() in parts or name.upper() in parts.values()


def _is_date_part(column: exp.Column, *, default_reader: bool) -> bool:
    parent = column.parent
    if column.table or not _is_date_part_name(column.name):
        return False
    if isinstance(parent, _UNIT_SLOT) and default_reader:
        unit = parent.args.get("unit")
        consumed = isinstance(unit, exp.Var) and _is_date_part_name(unit.name)
        return column.arg_key == "this" and unit is not None and not consumed
    if isinstance(parent, exp.Anonymous) and parent.name.upper() in _DATE_PART_FUNCTIONS:
        return bool(parent.expressions) and parent.expressions[0] is column
    return False


def columns(tree: exp.Expr, dialect: str) -> Iterator[exp.Column]:
    """The column references of a parsed expression: no ``*`` and no date-function unit."""
    default_reader = SQLGLOT_DIALECTS.get(dialect) is None
    for column in tree.find_all(exp.Column):
        if not isinstance(column.this, exp.Star) and not _is_date_part(column, default_reader=default_reader):
            yield column
    if not default_reader:
        return
    for node in tree.find_all(*_UNIT_SLOT):
        first, unit = node.this, node.args.get("unit")
        if (isinstance(first, exp.Column) and not first.table and _is_date_part_name(first.name)
                and isinstance(unit, exp.Var) and not _is_date_part_name(unit.name) and unit.name.isidentifier()):
            yield exp.column(unit.name)


#: sqlglot's dedicated ``AGG`` node (from 30.19; ``AGG`` is a generic ``exp.Anonymous`` call before).
_AGG_NODE: type[exp.Expr] | None = getattr(exp, "Agg", None)


def is_agg_call(node: exp.Expr) -> bool:
    """Whether ``node`` is an ``AGG(…)`` call, in either sqlglot shape."""
    if _AGG_NODE is not None and isinstance(node, _AGG_NODE):
        return True
    return isinstance(node, exp.Anonymous) and node.name.upper() == "AGG"


def agg_arguments(call: exp.Expr) -> list[exp.Expr]:
    """The arguments of an ``AGG(…)`` call, in either sqlglot shape."""
    if isinstance(call, exp.Anonymous):
        return list(call.expressions)
    return [call.this] if call.this is not None else []


def agg_calls(tree: exp.Expr) -> Iterator[tuple[exp.Func, list[exp.Expr]]]:
    """Every ``AGG(…)`` call of ``tree`` with its arguments."""
    for node in tree.find_all(exp.Func):
        if is_agg_call(node):
            yield node, agg_arguments(node)


def agg_text(args: list[exp.Expr]) -> str:
    return f"AGG({', '.join(a.sql() for a in args)})"


def _single(args: list[exp.Expr]) -> exp.Expr | None:
    return args[0] if len(args) == 1 else None


def _agg_problem(args: list[exp.Expr], *, fields: dict[str, set[str]], dataset_metrics: dict[str, set[str]],
                 model_metrics: dict[str, str]) -> str | None:
    expected = "AGG takes a single dataset.metric reference to a dataset-scoped metric"
    arg = _single(args)
    if not isinstance(arg, exp.Column) or isinstance(arg.this, exp.Star) or arg.args.get("db"):
        return f"{agg_text(args)}: {expected}"
    name, table = arg.name.casefold(), arg.table.casefold()
    if not arg.table:
        if name in model_metrics:
            return f"AGG({arg.name}) names the model-scoped metric '{model_metrics[name]}'; {expected}"
        return f"AGG({arg.name}) is not qualified; {expected}"
    if table not in fields:
        return f"AGG({arg.table}.{arg.name}) references unknown dataset '{arg.table}'"
    if name in dataset_metrics.get(table, set()):
        return None
    if name in fields[table]:
        return f"AGG({arg.table}.{arg.name}) names a field, and fields are never measures; {expected}"
    return f"AGG({arg.table}.{arg.name}): '{arg.name}' is not a dataset-scoped metric of dataset '{arg.table}'"


def _agg_call_problem(call: exp.Expr, args: list[exp.Expr], *, fields: dict[str, set[str]],
                      dataset_metrics: dict[str, set[str]], model_metrics: dict[str, str]) -> str | None:
    message = _agg_problem(args, fields=fields, dataset_metrics=dataset_metrics, model_metrics=model_metrics)
    if message is None and isinstance(call.parent, exp.Filter):
        return f"{agg_text(args)} has a FILTER (WHERE ...) clause; AGG does not take one"
    return message


def source_measure(args: list[exp.Expr]) -> exp.Column | None:
    """The ``source.measure`` column of an ``AGG`` call's arguments when they are exactly that, else ``None``."""
    arg = _single(args)
    if (not isinstance(arg, exp.Column) or isinstance(arg.this, exp.Star) or arg.args.get("db")
            or arg.table.casefold() != _SOURCE or not arg.name):
        return None
    return arg


#: Why SLayer cannot evaluate ``AGG(source.measure)`` (Ossie Metric Scoping, rule 9).
SOURCE_MEASURE_REASON = ("evaluates a measure of its source with AGG(source.<measure>), which SLayer cannot evaluate yet: "
                         "SLayer's sources expose no measure columns")


def reads_source_measure(text: str | None) -> bool:
    """Whether portable ``text`` calls ``AGG(source.measure)``."""
    tree, _ = parse_ossie_sql(text, PORTABLE_DIALECT) if text else (None, None)
    return tree is not None and any(source_measure(args) is not None for _, args in agg_calls(tree))


def _source_agg_problem(call: exp.Expr, args: list[exp.Expr], *, field_names: set[str]) -> str | None:
    """Why an ``AGG`` call of a dataset-scoped metric is not ``AGG(source.measure)`` without ``FILTER`` (Ossie
    Metric Scoping, rule 7)."""
    if source_measure(args) is None:
        return f"{agg_text(args)}: AGG in a dataset-scoped metric takes a single source.measure reference"
    if _SOURCE in field_names:
        return (f"{agg_text(args)}: the dataset declares a field named 'source', which takes precedence over the "
                "source qualifier, so this dataset's metrics cannot refer to source measures")
    if isinstance(call.parent, exp.Filter):
        return f"{agg_text(args)} has a FILTER (WHERE ...) clause; AGG does not take one"
    return None


def _expression_agg_errors(e: _Expr, *, model: str, fields: dict[str, set[str]],
                           dataset_metrics: dict[str, set[str]], model_metrics: dict[str, str]) -> list[str]:
    if e.dialect != PORTABLE_DIALECT or e.superseded:
        return []
    tree, _ = parse_ossie_sql(e.text, e.dialect)
    if tree is None:
        return []
    calls = list(agg_calls(tree))
    if calls and e.kind == "field":
        return [f"[Scope] {e.label(model)} calls AGG in a field expression. This version allows AGG only in metrics, "
                "as AGG(dataset.metric) in a model-scoped metric and AGG(source.measure) in a dataset-scoped metric."]
    out: list[str] = []
    for call, args in calls:
        if e.kind == "dataset_metric":
            message = _source_agg_problem(call, args, field_names=fields[e.dataset.casefold()])
            out += [f"[Scope] {e.label(model)}: {message}"] if message else []
            continue
        message = _agg_call_problem(call, args, fields=fields, dataset_metrics=dataset_metrics,
                                    model_metrics=model_metrics)
        out += [f"[Reference] {e.label(model)}: {message}"] if message else []
    return out


def _agg_rules(data: dict[str, Any], model: str) -> list[str]:
    fields: dict[str, set[str]] = {}
    dataset_metrics: dict[str, set[str]] = {}
    for d in data["datasets"]:
        fields.setdefault(d["name"].casefold(), set()).update(_field_names(d))
        dataset_metrics.setdefault(d["name"].casefold(), set()).update(n.casefold() for n in _names(d.get("metrics")))
    model_metrics = {n.casefold(): n for n in _names(data.get("metrics"))}
    out: list[str] = []
    for e in _all_expressions(data):
        out += _expression_agg_errors(e, model=model, fields=fields, dataset_metrics=dataset_metrics,
                                      model_metrics=model_metrics)
    return out


def _scoped_references(tree: exp.Expr, *, dialect: str, names: set[str]) -> tuple[set[str], set[str], bool]:
    """A dataset-scoped metric's qualifiers, undeclared fields and whether it reads ``source.column``, outside AGG."""
    inside_agg = {id(c) for call, _ in agg_calls(tree) for c in call.find_all(exp.Column)} \
        if dialect == PORTABLE_DIALECT else set()
    stars = (c for c in tree.find_all(exp.Column) if isinstance(c.this, exp.Star) and c.table)
    qualifiers: set[str] = set()
    undeclared: set[str] = set()
    uses_source = False
    for col in (*columns(tree, dialect), *stars):
        if id(col) in inside_agg:
            continue
        parts = [p.name for p in col.parts]
        if len(parts) == 2 and parts[0].casefold() == _SOURCE:
            uses_source = True
        elif len(parts) > 1 and parts[0]:
            qualifiers.add(parts[0])
        elif parts and parts[0].casefold() not in names:
            undeclared.add(parts[0])
    return qualifiers, undeclared, uses_source


def _metric_scoping(data: dict[str, Any], model: str) -> list[str]:
    out: list[str] = []
    declared = {d["name"]: _field_names(d) for d in data["datasets"]}
    for e in _all_expressions(data):
        if e.kind != "dataset_metric" or e.superseded:
            continue
        tree, _ = parse_ossie_sql(e.text, e.dialect)
        if tree is None:
            continue
        names = declared[e.dataset]
        qualifiers, undeclared, uses_source = _scoped_references(tree, dialect=e.dialect, names=names)
        prefix = f"[Scope] Dataset-scoped metric '{e.dataset}.{e.name}' in model '{model}' ({e.dialect})"
        if qualifiers:
            out.append(f"{prefix} uses qualified reference(s) {', '.join(repr(q) for q in sorted(qualifiers))}. A "
                       "dataset-scoped metric references the dataset's fields by unqualified name and its source "
                       "columns as source.column; a metric reading another dataset belongs in the model metrics.")
        if undeclared:
            out.append(f"{prefix} references undeclared field(s) {', '.join(repr(n) for n in sorted(undeclared))}.")
        if uses_source and _SOURCE in names:
            out.append(f"{prefix} uses source.column, but dataset '{e.dataset}' declares a field named 'source', "
                       "which takes precedence over the source qualifier.")
    return out


def portable_construct_problem(tree: exp.Expr) -> str | None:
    """Why a parsed ``OSSIE_SQL_2026`` expression uses a construct sqlglot reads but the language lacks, if it does."""
    if tree.find(exp.Interval) is not None:
        return f"INTERVAL is not part of {PORTABLE_DIALECT}"
    if any(node.args.get("query") is None and not node.args.get("expressions") for node in tree.find_all(exp.In)):
        return "IN list must contain at least one value"
    if tree.find(exp.Alias) is not None:
        return "expected an expression, found an alias"
    if any(node.args.get("this") is None and not node.args.get("expressions") for node in tree.find_all(exp.Distinct)):
        return "DISTINCT requires an argument"
    return None


def portable_problem(text: str) -> str | None:
    """Why ``text`` is not a valid ``OSSIE_SQL_2026`` expression, if it is not."""
    tree, problem = parse_ossie_sql(text, PORTABLE_DIALECT)
    return problem if tree is None else portable_construct_problem(tree)


def _sql_syntax(data: dict[str, Any], model: str) -> list[str]:
    out: list[str] = []
    for e in _all_expressions(data):
        if e.superseded:
            continue
        tree, problem = parse_ossie_sql(e.text, e.dialect)
        if tree is not None and e.dialect == PORTABLE_DIALECT:
            problem = portable_construct_problem(tree)
        if problem is not None:
            out.append(f"[SQL] {e.label(model)}: {problem}")
    return out


#: Aggregates of the expression language sqlglot's default reader parses as an unknown function.
_AGGREGATES_UNKNOWN_TO_SQLGLOT = frozenset({"APPROX_PERCENTILE"})
#: What sqlglot puts between a windowed call and its ``Window`` node.
_WINDOW_WRAPPERS = (exp.Filter, exp.WithinGroup, exp.IgnoreNulls, exp.RespectNulls)


def _bare_projection(tree: exp.Expr) -> bool:
    return (isinstance(tree, exp.Select) and len(tree.expressions) == 1 and not isinstance(tree.expressions[0], exp.Alias)
            and not any(value for key, value in tree.args.items() if key != "expressions"))


def _windowed_call(call: exp.Expr) -> bool:
    node = call
    while isinstance(node.parent, _WINDOW_WRAPPERS) and node.arg_key == "this":
        node = node.parent
    return isinstance(node.parent, exp.Window) and node.arg_key == "this"


def _aggregate_name(call: exp.AggFunc | exp.Anonymous) -> str | None:
    """The aggregate function ``call`` names; ``None`` for ``AGG`` or a call that is no aggregate."""
    if is_agg_call(call):
        return None
    if isinstance(call, exp.Anonymous):
        name = call.name.upper()
        return name if name in _AGGREGATES_UNKNOWN_TO_SQLGLOT else None
    return call.sql_name()


def unwindowed_aggregates(tree: exp.Expr) -> list[str]:
    """The aggregate functions ``tree`` calls without ``OVER``, each once in order; ``AGG`` and subqueries excluded."""
    names: list[str] = []
    for call in tree.find_all(exp.AggFunc, exp.Anonymous, bfs=False):
        name = _aggregate_name(call)
        if name is None:
            continue
        query = call.find_ancestor(exp.Select)
        if (query is not None and not (query is tree and _bare_projection(tree))) or _windowed_call(call):
            continue
        if name not in names:
            names.append(name)
    return names


def _field_aggregates(data: dict[str, Any], model: str) -> list[str]:
    out: list[str] = []
    for e in _all_expressions(data):
        if e.kind != "field" or e.dialect != PORTABLE_DIALECT or e.superseded:
            continue
        tree, _ = parse_ossie_sql(e.text, e.dialect)
        names = unwindowed_aggregates(tree) if tree is not None else []
        if names:
            out.append(f"[Scope] {e.label(model)} calls {', '.join(names)} without an OVER clause in a field "
                       "expression. A field expression is scalar, with no aggregations: add an OVER clause to make it "
                       "a window function, or define the aggregation as a metric.")
    return out


def semantic_errors(data: dict[str, Any]) -> list[str]:
    """Ossie's semantic errors of a schema-valid document (warnings excluded)."""
    model = data["name"]
    return [*_unique_names(data, model), *_keys_and_references(data, model), *_agg_rules(data, model),
            *_metric_scoping(data, model), *_field_aggregates(data, model), *_sql_syntax(data, model)]
