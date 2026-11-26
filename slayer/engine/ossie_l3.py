"""The strict Ossie Layer 3 binding (``conformance="ossie-l3"``, ``wide_table_semantics.md``): Ossie's query shape,
its §8 errors in their order, and its output names over native evaluation of every query it admits."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict

from slayer.core.enums import DataType, TypeKind
from slayer.core.errors import OssieErrorCode, OssieQueryError
from slayer.core.models import SlayerDataset, is_key_set_unique
from slayer.core.names import NAME_PATTERN, match_name, normalized_identifier
from slayer.core.ossie_conformance import FilterReading, LiteralComparison, is_iso_temporal, read_filter
from slayer.core.ossie_steps import ossie_from
from slayer.core.query import QueryDimension, QueryMetric, SlayerQuery, deprecated_inputs_of
from slayer.core.relationship_walker import unique_key_sets
from slayer.core.semantic_model import ModelScopedMetric
from slayer.core.syntax import DottedRef, Ref, bind_variables, parse_expr
from slayer.engine.population.inference import query_datasource
from slayer.engine.response_meta import ResponseAttributes

if TYPE_CHECKING:
    from slayer.engine.query_engine import SlayerQueryEngine, SlayerResponse

__all__ = ["execute_strict"]

Code = OssieErrorCode
_PARTS = ("dimensions", "metrics", "filter")
_TEMPORAL = (TypeKind.DATE, TypeKind.TIMESTAMP)
_LAYER_2 = "write the query in SLayer's own language (conformance='slayer'), which answers it with warnings"


def _invalid(why: str) -> OssieQueryError:
    return OssieQueryError(Code.E_INVALID_QUERY, why)


# --------------------------------------------------------------------------- #
# Row 1: the query as submitted
# --------------------------------------------------------------------------- #
class _Entry(BaseModel):
    """A dimension or metric entry: its reference as written and parsed, its alias and output name."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    written: str
    reference: Any
    alias: Optional[str] = None
    alias_name: Optional[str] = None
    alias_quoted: bool = False


class _Query(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    dimensions: List[_Entry]
    metrics: List[_Entry]
    filter: Optional[str] = None
    reading: FilterReading = FilterReading()


def _alias(value: Any) -> Tuple[str, bool]:
    """An alias's name and whether it is quoted; raises unless it is one Layer 1 identifier."""
    if isinstance(value, str) and NAME_PATTERN.fullmatch(value):
        return value, False
    if isinstance(value, str) and len(value) > 2 and value[0] == value[-1] == '"':
        inner = value[1:-1]
        if '"' not in inner.replace('""', ""):
            return inner.replace('""', '"'), True
    raise _invalid(f"the alias {value!r} is not an identifier; write a regular identifier or a double-quoted one")


def _reference(value: Any) -> Any:
    if isinstance(value, str):
        try:
            node = parse_expr(value)
        except ValueError:
            node = None
        if isinstance(node, (Ref, DottedRef)) and not node.evaluated:
            return node
    raise _invalid(f"{value!r} is not a reference; an entry names a field or metric as dataset.name or a bare metric")


def _entries(value: Any, *, part: str, key: str) -> List[_Entry]:
    if not isinstance(value, list):
        raise _invalid(f"{part} is a list of entries")
    out = []
    for item in value:
        if isinstance(item, str):
            out.append(_Entry(written=item, reference=_reference(item)))
            continue
        if not isinstance(item, dict) or key not in item or set(item) - {key, "alias"}:
            raise _invalid(f"a {part} entry is a reference string or an object with {key!r} and an optional 'alias'")
        name, quoted = _alias(item["alias"]) if "alias" in item else (None, False)
        out.append(_Entry(written=item[key] if isinstance(item[key], str) else repr(item[key]),
                          reference=_reference(item[key]), alias=item.get("alias"), alias_name=name,
                          alias_quoted=quoted))
    return out


def _shape(raw: Any, *, variables: Mapping[str, Any]) -> _Query:
    """Row 1 on the query as submitted, before any conversion; raises ``E_INVALID_QUERY``."""
    if not isinstance(raw, dict):
        raise _invalid("a query is one object with the parts dimensions, metrics and filter")
    unknown = [k for k in raw if k not in _PARTS]
    if unknown:
        raise _invalid(f"{unknown[0]!r} is not a part of an Ossie query (dimensions, metrics, filter)")
    nulls = [k for k, v in raw.items() if v is None]
    if nulls:
        raise _invalid(f"the part {nulls[0]!r} is null")
    query = _Query(dimensions=_entries(raw.get("dimensions", []), part="dimensions", key="field"),
                   metrics=_entries(raw.get("metrics", []), part="metrics", key="metric"))
    if "filter" in raw:
        text = raw["filter"]
        if not isinstance(text, str):
            raise _invalid("the filter is one expression string")
        query.filter = text
        query.reading = read_filter(_bound(text, variables), variables=variables)
        if query.reading.problem is not None:
            raise _invalid(query.reading.problem)
    return query


def _bound(text: str, variables: Mapping[str, Any]) -> str:
    if not variables:
        return text
    try:
        return bind_variables(text, variables, windows=True)
    except ValueError:
        return text


def _query_parts(query: SlayerQuery, *, stored: bool) -> Dict[str, Any]:
    """The parts a query object sets (a stored stage: those off their default, bookkeeping stripped), in the JSON
    binding's shape."""
    names = _set_fields(query, stored=stored) - {"version"}
    if stored:
        names -= _stored_bookkeeping(query)
    elif deprecated_inputs_of([query]):
        raise _invalid("the query uses a SLayer input form that is not part of an Ossie query")
    parts = {name: _part_value(query, name, stored=stored) for name in sorted(names)}
    return {name: value for name, value in parts.items() if value is not _OMITTED}


_OMITTED = object()
_ENTRY_KEYS = {"dimensions": "field", "metrics": "metric"}


def _stored_bookkeeping(query: SlayerQuery) -> set[str]:
    out = {"name"}
    if query.population_inferred:
        out |= {"population", "population_inferred"}
    if not query.variables:
        out.add("variables")
    return out


def _part_value(query: SlayerQuery, name: str, *, stored: bool) -> Any:
    value = getattr(query, name)
    if value is None:
        return None
    if name in _ENTRY_KEYS:
        return [_entry_parts(e, key=_ENTRY_KEYS[name], stored=stored) for e in value]
    if name == "filter":
        conditions = query.filter_conditions
        if not conditions:
            return _OMITTED
        return conditions[0] if len(conditions) == 1 else conditions
    return value


def _set_fields(model: BaseModel, *, stored: bool) -> set[str]:
    if not stored:
        return set(model.model_fields_set)
    return {n for n, f in type(model).model_fields.items()
            if getattr(model, n) != f.get_default(call_default_factory=True)}


def _entry_parts(entry: QueryDimension | QueryMetric, *, key: str, stored: bool) -> Any:
    names = _set_fields(entry, stored=stored) | {key}
    if names == {key}:
        return getattr(entry, key)
    return {name: getattr(entry, name) for name in sorted(names)}


# --------------------------------------------------------------------------- #
# The model view
# --------------------------------------------------------------------------- #
class _Step(BaseModel):
    source: str
    target: str
    name: str


class _Model(BaseModel):
    """The identified datasource read as an Ossie model: its datasets, Ossie steps and model-scoped metrics."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    data_source: str
    datasets: Dict[str, SlayerDataset]
    metrics: List[ModelScopedMetric]
    steps: List[_Step]

    def dataset(self, name: str, *, quoted: bool) -> Optional[SlayerDataset]:
        found = match_name(name=name, declared=list(self.datasets), quoted=quoted)
        return self.datasets[found] if found is not None else None

    def metric(self, name: str, *, quoted: bool) -> Optional[ModelScopedMetric]:
        found = match_name(name=name, declared=[m.name for m in self.metrics], quoted=quoted)
        return next((m for m in self.metrics if m.name == found), None)

    def paths(self, source: str, target: str, *, cap: int = 2) -> List[List[str]]:
        """Up to ``cap`` paths of Ossie steps ``source → target``, each as its relationships' names; every step taken
        can still reach ``target`` off the path so far, so the walk expands at most ``cap`` paths' steps."""
        if source == target:
            return [[]]
        out: List[List[str]] = []

        def walk(node: str, seen: set[str], names: List[str]) -> None:
            for step in self.steps:
                if len(out) >= cap or step.source != node or step.target in seen:
                    continue
                if step.target == target:
                    out.append([*names, step.name])
                elif self._reaches(step.target, target, avoiding=seen):
                    walk(step.target, seen | {step.target}, [*names, step.name])

        walk(source, {source}, [])
        return out

    def _reaches(self, start: str, target: str, *, avoiding: set[str]) -> bool:
        """Whether a path of Ossie steps leads ``start → target`` through none of ``avoiding``."""
        found, frontier = {start}, [start]
        while frontier:
            node = frontier.pop()
            for step in self.steps:
                if step.source != node or step.target in found or step.target in avoiding:
                    continue
                if step.target == target:
                    return True
                found.add(step.target)
                frontier.append(step.target)
        return False


def _steps(datasets: Mapping[str, SlayerDataset]) -> List[_Step]:
    def keyed(name: str, keys: Sequence[str]) -> bool:
        return is_key_set_unique(key_fields=list(keys), unique_key_sets=unique_key_sets(datasets[name]))

    out = []
    for name, dataset in datasets.items():
        for rel in dataset.relationships:
            if rel.to not in datasets or rel.to == name:
                continue
            many = ossie_from(declaring=name, to=rel.to, cardinality=rel.cardinality, declaring_keys=rel.from_columns,
                              to_keys=rel.to_columns, keyed=keyed, document_from=rel.document_from)
            if many is not None:
                one = rel.to if many == name else name
                out.append(_Step(source=many, target=one, name=rel.name or f"{name}→{rel.to}"))
    return out


async def _model(engine: "SlayerQueryEngine", data_source: str) -> _Model:
    loaded, _unloaded = await engine.storage.load_datasets(data_source=data_source)
    datasets = {d.name: d for d in loaded}
    return _Model(data_source=data_source, datasets=datasets, steps=_steps(datasets),
                  metrics=await engine.storage.list_model_scoped_metrics(data_source))


def _zoned_stages(query: _Query, model: _Model) -> None:
    """A query-backed dataset the query reads whose stored stage sets a time zone runs a SLayer query part."""
    heads, bare = _heads(query)
    read = [d.name for head, quoted in heads if (d := model.dataset(head, quoted=quoted)) is not None]
    for name, quoted in bare:
        metric = model.metric(name, quoted=quoted)
        if metric is not None:
            read += _homes(_Target(kind="model_metric", name=metric.name, model_metric=metric), model)
    for name in dict.fromkeys(read):
        if any(q.time_zone is not None for q in model.datasets[name].source_queries or ()):
            raise _invalid(f"'time_zone' is not a part of an Ossie query; the query-backed dataset {name!r} it reads "
                           f"sets one")


def _heads(query: _Query) -> Tuple[List[Tuple[str, bool]], List[Tuple[str, bool]]]:
    """The first segments of the query's dotted references and its bare names, each with whether it is quoted."""
    refs = [*(e.reference for e in (*query.dimensions, *query.metrics)), *query.reading.references]
    heads = [(r.parts[0], r.quoted_at(0)) for r in refs if isinstance(r, DottedRef)]
    bare = [(r.name, r.quoted) for r in refs if isinstance(r, Ref)]
    return heads, bare


async def _identify(engine: "SlayerQueryEngine", query: _Query, data_source: Optional[str]) -> str:
    """The query's datasource: the given one, else the one native resolution picks (its errors raised)."""
    if data_source is not None:
        return data_source
    heads, bare = _heads(query)
    found = await query_datasource(storage=engine.storage, anchors=heads, metrics=bare)
    if found is not None:
        return found
    first = (query.dimensions or query.metrics)[0]
    raise OssieQueryError(Code.E_NAME_NOT_FOUND, f"{first.written!r} names nothing in any model")


# --------------------------------------------------------------------------- #
# Rows 3–17
# --------------------------------------------------------------------------- #
class _Target(BaseModel):
    """What a reference resolves to: a field or metric of a dataset, or a model-scoped metric."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    kind: Literal["field", "metric", "model_metric"]
    dataset: Optional[str] = None
    name: str
    model_metric: Optional[ModelScopedMetric] = None
    datatype: Optional[DataType] = None


def _resolve(ref: Any, model: _Model) -> Optional[_Target]:
    if isinstance(ref, Ref):
        metric = model.metric(ref.name, quoted=ref.quoted)
        return _Target(kind="model_metric", name=metric.name, model_metric=metric) if metric is not None else None
    if len(ref.parts) != 2:
        return None
    dataset = model.dataset(ref.parts[0], quoted=ref.quoted_at(0))
    if dataset is None:
        return None
    names = [f.name for f in dataset.fields] + [m.name for m in dataset.metrics if m.name]
    found = match_name(name=ref.parts[1], declared=names, quoted=ref.quoted_at(1))
    if found is None:
        return None
    field = dataset.get_field(found)
    if field is not None:
        return _Target(kind="field", dataset=dataset.name, name=found, datatype=field.datatype)
    return _Target(kind="metric", dataset=dataset.name, name=found)


def _written(ref: Any) -> str:
    return ref.name if isinstance(ref, Ref) else ".".join(ref.parts)


class _Judged(BaseModel):
    """An admitted query's resolved entries and filter dataset."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    dimensions: List[_Target]
    metrics: List[_Target]
    filter_dataset: Optional[str] = None


def _names_and_kinds(query: _Query, model: _Model) -> _Judged:
    """Rows 3–8."""
    resolved = _resolved(query, model)
    judged = _Judged(dimensions=[resolved[id(e.reference)] for e in query.dimensions],
                     metrics=[resolved[id(e.reference)] for e in query.metrics])
    _kinds(query, judged)
    judged.filter_dataset = _filter_dataset(query, [resolved[id(r)] for r in query.reading.references])
    return judged


def _resolved(query: _Query, model: _Model) -> Dict[int, _Target]:
    """Every reference's target by the reference's ``id``; raises ``E_NAME_NOT_FOUND`` (row 3)."""
    out: Dict[int, _Target] = {}
    for ref in [*(e.reference for e in (*query.dimensions, *query.metrics)), *query.reading.references]:
        target = _resolve(ref, model)
        if target is None:
            raise OssieQueryError(Code.E_NAME_NOT_FOUND, f"{_written(ref)!r} names no field or metric of the model "
                                  f"{model.data_source!r}", suggestion="reference a field or metric as dataset.name, "
                                  "or a model-scoped metric by its bare name")
        out[id(ref)] = target
    return out


def _kinds(query: _Query, judged: _Judged) -> None:
    for entry, target in zip(query.dimensions, judged.dimensions):
        if target.kind != "field":
            raise OssieQueryError(Code.E_METRIC_IN_DIMENSIONS, f"the dimension {entry.written!r} names a metric",
                                  dimension=entry.written, suggestion="move it to metrics")
    for entry, target in zip(query.metrics, judged.metrics):
        if target.kind == "field":
            raise OssieQueryError(Code.E_FIELD_IN_METRICS, f"the metric {entry.written!r} names a field",
                                  metric=entry.written, suggestion="move it to dimensions, or name a metric over it")


def _filter_dataset(query: _Query, targets: List[_Target]) -> Optional[str]:
    """The one dataset the filter reads; raises rows 6–8."""
    if query.reading.aggregate or any(t.kind != "field" for t in targets):
        raise OssieQueryError(Code.E_AGGREGATE_IN_WHERE, f"the filter {query.filter!r} contains an aggregate or a "
                              "metric reference", filter=query.filter, suggestion=_LAYER_2)
    if query.reading.window:
        raise OssieQueryError(Code.E_WINDOW_IN_WHERE, f"the filter {query.filter!r} contains a window function",
                              filter=query.filter, suggestion=_LAYER_2)
    datasets = list(dict.fromkeys(t.dataset for t in targets if t.dataset))
    if len(datasets) > 1:
        raise OssieQueryError(Code.E_CROSS_DATASET_FILTER, f"the filter {query.filter!r} reads fields of "
                              f"{', '.join(datasets)}; a filter reads one dataset", filter=query.filter,
                              suggestion=_LAYER_2)
    return datasets[0] if datasets else None


def _temporal_literals(query: _Query, model: _Model) -> None:
    """A temporal field compared with a string literal takes ISO-8601 dates and timestamps only (row 1)."""
    for comparison in query.reading.comparisons:
        _literal_comparison(comparison, model)
    for ref in query.reading.references:
        target = _resolve(ref, model)
        if target is not None and target.datatype is DataType.ARRAY:
            raise _invalid(f"{_written(ref)!r} is a list field, which Ossie has no type for")


def _literal_comparison(comparison: LiteralComparison, model: _Model) -> None:
    target = _resolve(comparison.reference, model)
    dataset = model.datasets.get(target.dataset or "") if target is not None else None
    field = dataset.get_field(target.name) if dataset is not None and target is not None else None
    if field is None or field.datatype is None:
        return
    if field.datatype is DataType.ARRAY:
        raise _invalid(f"{_written(comparison.reference)!r} is a list field, which Ossie has no type for")
    bad = [t for t in comparison.texts if not is_iso_temporal(t)]
    if field.datatype.kind in _TEMPORAL and bad:
        raise _invalid(f"{bad[0]!r} is not an ISO-8601 date or timestamp; a period or relative time is SLayer syntax")


def _output_names(query: _Query, judged: _Judged) -> List[Tuple[str, bool]]:
    """Each entry's output name and whether it is quoted (§3.3): the alias, else its reference's last segment."""
    out = []
    for entry, target in zip([*query.dimensions, *query.metrics], [*judged.dimensions, *judged.metrics]):
        if entry.alias_name is not None:
            out.append((entry.alias_name, entry.alias_quoted))
        else:
            ref = entry.reference
            out.append((target.name, ref.quoted if isinstance(ref, Ref) else ref.quoted_at(len(ref.parts) - 1)))
    return out


def _collisions(query: _Query, names: List[Tuple[str, bool]]) -> None:
    """Row 9."""
    entries = [*query.dimensions, *query.metrics]
    seen: Dict[str, int] = {}
    for i, (name, quoted) in enumerate(names):
        key = normalized_identifier(name, quoted=quoted)
        if key in seen:
            first = entries[seen[key]].written
            raise OssieQueryError(Code.E_NAME_COLLISION, f"the entries {first!r} and {entries[i].written!r} both "
                                  f"have the output name {name!r}", suggestion="give one of them an alias")
        seen[key] = i


def _model_metric_problems(judged: _Judged, query: _Query, model: _Model) -> None:
    """Rows 10–12, each judged across every model-scoped metric of the query."""
    found: List[Tuple[str, Any]] = []
    for entry, target in zip(query.metrics, judged.metrics):
        if target.model_metric is not None:
            found += [(entry.written, p) for p in _metric_problems(target.model_metric, model, chain=())]
    for code in (Code.E_UNAGGREGATED_FINER_GRAIN_REFERENCE, Code.E_DEFERRED_WINDOWED_METRIC,
                 Code.E_AMBIGUOUS_MEASURE_GRAIN):
        hit = next(((w, p) for w, p in found if p.code is code), None)
        if hit is not None:
            raise OssieQueryError(code, f"the model-scoped metric {hit[0]!r} {hit[1].reason}", metric=hit[0],
                                  suggestion="define it as a dataset-scoped metric, or query it with "
                                             "conformance='slayer'")


def _metric_problems(metric: ModelScopedMetric, model: _Model, *, chain: Tuple[str, ...]) -> List[Any]:
    out = list(metric.analysis.problems)
    for name in metric.analysis.metrics:
        named = model.metric(name, quoted=False)
        if named is not None and named.name not in chain:
            out += _metric_problems(named, model, chain=(*chain, metric.name))
    return out


def _homes(target: _Target, model: _Model, *, chain: Tuple[str, ...] = ()) -> List[str]:
    """A metric's datasets: its components' home datasets."""
    if target.model_metric is None:
        return [target.dataset] if target.dataset else []
    metric = target.model_metric
    out: List[str] = []
    for home in metric.analysis.homes:
        dataset = model.dataset(home, quoted=False)
        if dataset is not None:
            out.append(dataset.name)
    for name in metric.analysis.metrics:
        named = model.metric(name, quoted=False)
        if named is not None and named.name not in chain:
            out += _homes(_Target(kind="model_metric", name=named.name, model_metric=named), model,
                          chain=(*chain, metric.name))
    return list(dict.fromkeys(out))


def _paths(query: _Query, judged: _Judged, model: _Model) -> None:
    """Rows 13–17."""
    if judged.metrics:
        homes = [_homes(m, model) for m in judged.metrics]
        _metric_reachability(query, judged, model, homes)
        _metric_ambiguity(query, judged, model, homes)
    else:
        _anchor_paths(query, judged, model)


#: The reachability rows in Ossie's order, each holding of the per-metric reachability of one dimension.
_REACH_ROWS: Tuple[Tuple[Code, Callable[[List[bool]], bool]], ...] = (
    (Code.E_NO_PATH, lambda ok: not any(ok)),
    (Code.E_PARTIAL_DIMENSION_REACHABILITY, lambda ok: any(ok) and not all(ok)),
)


def _reaches(model: _Model, homes: List[str], dataset: str) -> bool:
    return all(model.paths(h, dataset, cap=1) for h in homes)


def _metric_reachability(query: _Query, judged: _Judged, model: _Model, homes: List[List[str]]) -> None:
    for code, holds in _REACH_ROWS:
        for entry, target in zip(query.dimensions, judged.dimensions):
            ok = [_reaches(model, h, target.dataset or "") for h in homes]
            if holds(ok):
                metric = query.metrics[ok.index(False)].written
                raise _unreached(code, dimension=entry.written, dataset=target.dataset, metric=metric)
    if judged.filter_dataset is None:
        return
    for entry, metric_homes in zip(query.metrics, homes):
        if not _reaches(model, metric_homes, judged.filter_dataset):
            raise OssieQueryError(Code.E_UNSUPPORTED_FILTER_PATH, f"the metric {entry.written!r} has no path to "
                                  f"{judged.filter_dataset!r}, the dataset of the filter {query.filter!r}",
                                  filter=query.filter, metric=entry.written, suggestion=_LAYER_2)


def _metric_ambiguity(query: _Query, judged: _Judged, model: _Model, homes: List[List[str]]) -> None:
    for entry, target in zip(query.dimensions, judged.dimensions):
        for metric, metric_homes in zip(query.metrics, homes):
            _unambiguous(model, metric_homes, target.dataset or "", dimension=entry.written, metric=metric.written)
    if judged.filter_dataset is not None:
        for metric, metric_homes in zip(query.metrics, homes):
            _unambiguous(model, metric_homes, judged.filter_dataset, filter=query.filter, metric=metric.written)


def _anchor_paths(query: _Query, judged: _Judged, model: _Model) -> None:
    """A dimension-only query's anchor and its paths."""
    datasets = list(dict.fromkeys(t.dataset or "" for t in judged.dimensions))
    anchors = [a for a in datasets if all(model.paths(a, d, cap=1) for d in datasets)]
    if not anchors:
        raise OssieQueryError(Code.E_NO_ANCHOR, f"no dataset among {', '.join(datasets)} reaches every other "
                              "dimension's dataset", suggestion="add a metric whose dataset reaches every dimension")
    reaching = [a for a in anchors if judged.filter_dataset is None or model.paths(a, judged.filter_dataset, cap=1)]
    if not reaching:
        raise OssieQueryError(Code.E_UNSUPPORTED_FILTER_PATH, f"the anchor {anchors[0]!r} has no path to "
                              f"{judged.filter_dataset!r}, the dataset of the filter {query.filter!r}",
                              filter=query.filter, anchor=anchors[0], suggestion=_LAYER_2)
    if len(anchors) > 1:
        raise OssieQueryError(Code.E_AMBIGUOUS_PATH, f"the datasets {', '.join(anchors)} each qualify as the anchor "
                              "of the query", suggestion="add a metric to choose the rows")
    anchor = anchors[0]
    for entry, target in zip(query.dimensions, judged.dimensions):
        _unambiguous(model, [anchor], target.dataset or "", dimension=entry.written, anchor=anchor)
    if judged.filter_dataset is not None:
        _unambiguous(model, [anchor], judged.filter_dataset, filter=query.filter, anchor=anchor)


def _unreached(code: Code, *, dimension: str, dataset: Optional[str], metric: str) -> OssieQueryError:
    what = "no metric reaches" if code is Code.E_NO_PATH else "not every metric reaches"
    return OssieQueryError(code, f"{what} {dataset!r}, the dataset of the dimension {dimension!r}: {metric!r} has no "
                           "path of many-to-one relationships to it", dimension=dimension, metric=metric,
                           suggestion=_LAYER_2)


def _unambiguous(model: _Model, sources: Sequence[str], target: str, *, dimension: Optional[str] = None,
                 filter: Optional[str] = None, metric: Optional[str] = None,  # noqa: A002
                 anchor: Optional[str] = None) -> None:
    for source in sources:
        found = model.paths(source, target)
        if len(found) > 1:
            spelled = " and ".join(" → ".join(p) for p in found)
            what = f"the dimension {dimension!r}" if dimension is not None else f"the filter {filter!r}"
            raise OssieQueryError(Code.E_AMBIGUOUS_PATH, f"{source!r} reaches {target!r} ({what}) by more than one "
                                  f"path: {spelled}", dimension=dimension, filter=filter, metric=metric,
                                  anchor=anchor, suggestion=_LAYER_2)


def _labels(names: List[Tuple[str, bool]]) -> List[str]:
    """Column labels: the output names, a regular one normalized where two would share a label."""
    plain = [n for n, _ in names]
    return [normalized_identifier(n, quoted=q) if not q and plain.count(n) > 1 else n for n, q in names]


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
class _Input(BaseModel):
    """The query to judge, in the JSON binding's shape, with what a saved query run by name brings."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    raw: Any
    data_source: Optional[str] = None
    #: A saved query's judged stage, which the native run executes.
    stage: Optional[SlayerQuery] = None
    #: The saved dataset's variable defaults, under the run's variables.
    defaults: Dict[str, Any] = {}


async def _raw_input(engine: "SlayerQueryEngine", query: Any, *, refine: Any, data_source: Optional[str]) -> _Input:
    if isinstance(query, str):
        stages, pinned, defaults = await engine.saved_query_stages(query, refine=refine, data_source=data_source)
        if len(stages) != 1:
            raise _invalid(f"the saved query {query!r} has {len(stages)} stages; an Ossie query has one")
        return _Input(raw=_query_parts(stages[0], stored=True), data_source=pinned, stage=stages[0], defaults=defaults)
    if refine is not None:
        raise ValueError("refine applies only to a saved query run by name; put these clauses in the query object instead")
    if isinstance(query, SlayerQuery):
        return _Input(raw=_query_parts(query, stored=False), data_source=data_source)
    return _Input(raw=query, data_source=data_source)


def _native_query(query: _Query, model: _Model) -> Dict[str, Any]:
    """The admitted query in SLayer's language: a quoted alias, which it cannot spell, becomes a fresh name."""
    taken = {n.casefold() for n in [*model.datasets, *(m.name for m in model.metrics)]}
    taken |= {e.alias.casefold() for e in (*query.dimensions, *query.metrics) if e.alias and not e.alias_quoted}

    def entry(e: _Entry, key: str) -> Any:
        if e.alias is None:
            return e.written
        alias = e.alias
        if e.alias_quoted:
            n = 1
            while f"ossie_alias_{n}" in taken:
                n += 1
            alias = f"ossie_alias_{n}"
            taken.add(alias)
        return {key: e.written, "alias": alias}

    out: Dict[str, Any] = {"dimensions": [entry(e, "field") for e in query.dimensions],
                           "metrics": [entry(e, "metric") for e in query.metrics]}
    if query.filter is not None:
        out["filter"] = query.filter
    return {k: v for k, v in out.items() if v}


def _relabelled(response: "SlayerResponse", labels: List[str]) -> "SlayerResponse":
    native = list(response.columns)
    if len(native) != len(labels):
        raise RuntimeError(f"the strict binding expected the columns {labels}, the query returned {native}")
    mapping = dict(zip(native, labels))

    def rekeyed(meta: Mapping[str, Any]) -> Dict[str, Any]:
        return {mapping.get(k, k): v for k, v in meta.items()}

    attributes = ResponseAttributes(dimensions=rekeyed(response.attributes.dimensions),
                                    metrics=rekeyed(response.attributes.metrics))
    return response.model_copy(update={"data": [{mapping[k]: v for k, v in row.items()} for row in response.data],
                                       "columns": labels, "attributes": attributes})


async def execute_strict(
    engine: "SlayerQueryEngine", query: Any, *, variables: Optional[Dict[str, Any]], dry_run: bool, explain: bool,
    data_source: Optional[str], cache: bool, refine: Any,
    run: Callable[..., Any],
) -> "SlayerResponse":
    """Judge ``query`` by Ossie Layer 3 (§8, in order), then answer it natively under its output names."""
    submitted = await _raw_input(engine, query, refine=refine, data_source=data_source)
    bindings = {**submitted.defaults, **(variables or {})}
    shaped = _shape(submitted.raw, variables=bindings)
    if not shaped.dimensions and not shaped.metrics:
        raise OssieQueryError(Code.E_EMPTY_AGGREGATION_QUERY, "the query has neither dimensions nor metrics")
    model = await _model(engine, await _identify(engine, shaped, submitted.data_source))
    _zoned_stages(shaped, model)
    _temporal_literals(shaped, model)
    judged = _names_and_kinds(shaped, model)
    names = _output_names(shaped, judged)
    _collisions(shaped, names)
    _model_metric_problems(judged, shaped, model)
    _paths(shaped, judged, model)
    native = submitted.stage if submitted.stage is not None else _native_query(shaped, model)
    response = await run(native, bindings or None, dry_run=dry_run, explain=explain, data_source=model.data_source,
                         cache=cache, refine=None)
    return response if explain else _relabelled(response, _labels(names))
