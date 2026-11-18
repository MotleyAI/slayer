"""Stored extension fields inlined where they are used: each becomes a Mode-B expression, bound against the datasets of
its datasource. A stored query that cannot be converted raises ``UnconvertibleStoredQueryError``."""

from __future__ import annotations

import warnings
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, ConfigDict

from slayer.core.errors import AmbiguousReferenceError, UnconvertibleStoredQueryError
from slayer.core.models import SlayerDataset
from slayer.core.names import match_name
from slayer.core.query import (
    FieldRef,
    ComputedDimension,
    DatasetExtension,
    DatasetMetric,
    OrderItem,
    SlayerQuery,
    TimeDimension,
    computed_dimension_name,
    extension_field_text,
)
from slayer.core.syntax import (
    DottedRef,
    Ref,
    canonical_metric_text,
    dotted_quoting,
    map_refs,
    onto_path,
    parse_expr,
)
from slayer.core.warnings import ExtensionFieldInlinedWarning, SlayerExtensionFieldInlinedWarning
from slayer.engine.extensions import QueryScope, query_scopes
from slayer.sql.naming import expression_text_alias
from slayer.storage.migrations import migrating_document

Key = Tuple[str, str]


def holds_removed_forms(query: Any) -> bool:
    """Whether a migrated stored query carries an inline ``population`` or extension ``columns``."""
    if not isinstance(query, dict):
        return False
    source, extensions = query.get("population"), query.get("extensions")
    return isinstance(source, dict) or (
        isinstance(extensions, list) and any(isinstance(e, dict) and "columns" in e for e in extensions))


class _ExtensionField(BaseModel):
    """One extension field: its own expression, read from the dataset or stage it extends."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    owner: str
    name: str
    tree: Any
    derived: bool
    label: Optional[str] = None


def inline_extension_fields(
    stages: Sequence[dict], *, datasets_by_ds: Mapping[str, Mapping[str, SlayerDataset]], data_source: Optional[str],
    priority: Sequence[str], document: str,
) -> List[SlayerQuery]:
    """The raw stored ``stages`` (migrated, one query list) with every extension field inlined where it is used."""
    for stage in stages:
        if isinstance(stage.get("population"), dict):
            raise UnconvertibleStoredQueryError("an inline population")
    raw_columns = [_extension_fields(stage) for stage in stages]
    with migrating_document(document, stored=True):
        queries = [SlayerQuery.model_validate(_without_fields(stage)) for stage in stages]
    out: List[SlayerQuery] = []
    for i, query in enumerate(queries):
        if not raw_columns[i]:
            out.append(query)
            continue
        scopes = query_scopes(queries, datasets_by_ds=datasets_by_ds, data_source=data_source)[i]
        if not scopes:
            raise UnconvertibleStoredQueryError("extension fields over datasets the store does not hold")
        extensions = [e for e in stages[i].get("extensions") or [] if isinstance(e, dict)]
        runs = [_Stage(query=query, raw=raw_columns[i], scope=scope, document=document)
                for scope in _running_scopes(scopes, priority=priority)]
        converted = [run.converted(extensions=extensions) for run in runs]
        if any(c != converted[0] for c in converted[1:]):
            raise UnconvertibleStoredQueryError("extension fields that convert differently in each datasource "
                                                "holding their datasets")
        for owner, name in runs[0].fields:
            warnings.warn(SlayerExtensionFieldInlinedWarning(ExtensionFieldInlinedWarning(
                document=document, field=f"{owner}.{name}",
            )), stacklevel=2)
        out.append(converted[0])
    return out


def _running_scopes(scopes: Sequence[QueryScope], *, priority: Sequence[str]) -> List[QueryScope]:
    """The scope the query runs in, as bare-name resolution picks it; every one when no priority entry decides."""
    if len(scopes) == 1:
        return list(scopes)
    by_ds = {s.data_source: s for s in scopes}
    preferred = next((by_ds[ds] for ds in priority if ds in by_ds), None)
    return [preferred] if preferred is not None else list(scopes)


def _extension_fields(stage: dict) -> List[Tuple[dict, List[Any]]]:
    """Each extension of ``stage`` carrying ``fields``, with them."""
    extensions = stage.get("extensions")
    return [
        (e, list(e["columns"])) for e in extensions if isinstance(e, dict) and e.get("columns")
    ] if isinstance(extensions, list) else []


def _without_fields(stage: dict) -> dict:
    extensions = stage.get("extensions")
    if not isinstance(extensions, list):
        return stage
    stripped = [{k: v for k, v in e.items() if k != "columns"} if isinstance(e, dict) else e for e in extensions]
    return {**stage, "extensions": [e for e in stripped if not isinstance(e, dict) or set(e) - {"source_name"}]}


class _Stage:
    """One stored query object's conversion over its static scope."""

    def __init__(
        self, *, query: SlayerQuery, raw: List[Tuple[dict, List[Any]]], scope: QueryScope, document: str,
    ) -> None:
        self.query = query
        self.scope = scope
        self.document = document
        self.fields: Dict[Key, _ExtensionField] = {}
        for extension, columns in raw:
            owner = self._owner(extension.get("source_name"))
            for column in columns:
                parsed = self._field(column, owner=owner)
                self.fields[(owner, parsed.name)] = parsed
        self._expanded: Dict[Key, Any] = {}
        self._visiting: Set[Key] = set()

    def _owner(self, name: Any) -> str:
        if not isinstance(name, str):
            raise UnconvertibleStoredQueryError("an extension without source_name")
        found = self._dataset_named(name, quoted=False)
        if found is None:
            raise UnconvertibleStoredQueryError(f"extension fields of {name!r}, which the store does not hold")
        return found

    def _dataset_named(self, name: str, *, quoted: bool) -> Optional[str]:
        """The dataset or stage of the scope ``name`` reaches, matched as the engine matches a dataset name."""
        try:
            return match_name(name=name, declared=self.scope.datasets, quoted=quoted)
        except AmbiguousReferenceError as exc:
            raise UnconvertibleStoredQueryError(f"a reference {name!r} that names several datasets") from exc

    def _field(self, raw: Any, *, owner: str) -> _ExtensionField:
        if not isinstance(raw, dict) or not isinstance(raw.get("name"), str):
            raise UnconvertibleStoredQueryError(f"an unnamed extension field of {owner!r}")
        name = raw["name"]
        what = f"extension field {owner}.{name}"
        try:
            text = extension_field_text(raw, owner=owner, what=what)
        except ValueError as exc:
            raise UnconvertibleStoredQueryError(str(exc)) from exc
        dataset = self.scope.datasets[owner]
        stored = owner not in self.scope.siblings
        if raw.get("sql") is None:
            if stored and not _visible(dataset, name):
                raise UnconvertibleStoredQueryError(f"{what}, which names no visible field of {owner!r}")
            return _ExtensionField(owner=owner, name=name, tree=Ref(name=name), derived=False, label=raw.get("label"))
        if stored and dataset.get_field(name) is not None:
            raise UnconvertibleStoredQueryError(f"{what}, named like a field of {owner!r}")
        return _ExtensionField(owner=owner, name=name, tree=parse_expr(text), derived=True, label=raw.get("label"))

    def _check_relationship_keys(self, extension: dict) -> None:
        owner = self._owner(extension.get("source_name"))
        for relationship in (j for j in extension.get("relationships") or [] if isinstance(j, dict)):
            target = relationship.get("to")
            target_owner = (self._dataset_named(target, quoted=False) or target) if isinstance(target, str) else target
            if any((owner, pair[0]) in self.fields or (target_owner, pair[1]) in self.fields
                   for pair in relationship.get("join_pairs") or []):
                raise UnconvertibleStoredQueryError(f"an extension relationship of {owner!r} keyed on an extension field")

    # -- resolution ----------------------------------------------------------
    def _hit(self, ref: Any, *, root: Optional[SlayerDataset]) -> Optional[Tuple[Tuple[str, ...], Key]]:
        """The qualifying path and extension field a reference resolves to; ``None`` for anything else."""
        parts, quoted = _parts(ref), _quoting(ref)
        hops, leaf = tuple(parts[:-1]), parts[-1]
        start = self._start(hops, leaf=leaf, quoted=quoted, root=root)
        if start is None:
            return None
        prefix, dataset, rest = start
        resolved = self.scope.resolve(rest, root=dataset) if rest else ((), [])
        if resolved is None:
            return None
        path, chain = resolved
        owner = chain[-1].to if chain else dataset.name
        name = self._field_named(owner, leaf=leaf, quoted=quoted[-1])
        return ((*prefix, *path), (owner, name)) if name is not None else None

    def _field_named(self, owner: str, *, leaf: str, quoted: bool) -> Optional[str]:
        """The extension field of ``owner`` that ``leaf`` names, matched as the engine matches a field name."""
        own = [n for o, n in self.fields if o == owner]
        if not own:
            return None
        dataset = self.scope.datasets.get(owner)
        try:
            found = match_name(name=leaf, declared=[*own, *(c.name for c in dataset.fields)] if dataset else own,
                               quoted=quoted)
        except AmbiguousReferenceError as exc:
            raise UnconvertibleStoredQueryError(f"a reference {leaf!r} that matches several fields of "
                                                f"{owner!r}") from exc
        return found if found in own else None

    def _start(
        self, hops: Tuple[str, ...], *, leaf: str, quoted: Tuple[bool, ...], root: Optional[SlayerDataset],
    ) -> Optional[Tuple[Tuple[str, ...], SlayerDataset, Tuple[str, ...]]]:
        """The qualifying prefix, dataset and remaining hops a reference resolves from."""
        if root is not None:
            return (), root, hops
        head_name = self._dataset_named(hops[0], quoted=quoted[0]) if hops else None
        if head_name is not None:
            return (head_name,), self.scope.datasets[head_name], hops[1:]
        if not hops and any(k[1] == leaf or (not quoted[0] and k[1].casefold() == leaf.casefold())
                            for k in self.fields):
            raise UnconvertibleStoredQueryError(f"a reference to the extension field {leaf!r} without a "
                                                f"population to attribute it to")
        return None

    def expansion(self, key: Key) -> Any:
        """Field ``key``'s expression with every extension field it reads expanded, read from its owner."""
        if key in self._expanded:
            return self._expanded[key]
        field = self.fields[key]
        if not field.derived:
            return field.tree
        if key in self._visiting:
            raise UnconvertibleStoredQueryError(f"a dependency cycle among extension fields through {key[1]!r}")
        self._visiting.add(key)
        tree = self._inlined(field.tree, root=self.scope.datasets[field.owner])
        self._visiting.discard(key)
        self._expanded[key] = tree
        return tree

    def _inlined(self, tree: Any, *, root: Optional[SlayerDataset]) -> Any:
        def _replace(ref: Ref | DottedRef) -> Any:
            hit = self._hit(ref, root=root)
            return ref if hit is None else onto_path(self.expansion(hit[1]), hit[0])

        return map_refs(tree, _replace)

    def _text(self, text: str, *, root: Optional[SlayerDataset]) -> Optional[str]:
        """``text`` with its extension fields inlined; ``None`` when it reads none."""
        try:
            parsed = parse_expr(text)
        except ValueError:
            return None
        out = self._inlined(parsed, root=root)
        return None if out is parsed else canonical_metric_text(out)

    def _expression(self, hit: Tuple[Tuple[str, ...], Key]) -> str:
        return canonical_metric_text(onto_path(self.expansion(hit[1]), hit[0]))

    # -- positions -----------------------------------------------------------
    def converted(self, *, extensions: List[dict]) -> SlayerQuery:
        for extension in extensions:
            self._check_relationship_keys(extension)
        q, host = self.query, self.scope.host
        update: Dict[str, Any] = {
            "dimensions": [self._dimension(d, root=host) for d in q.dimensions] if q.dimensions else q.dimensions,
            "time_dimensions": [self._time_dimension(td, root=host) for td in q.time_dimensions]
            if q.time_dimensions else q.time_dimensions,
            "metrics": [self._metric(m, root=host) for m in q.metrics] if q.metrics else q.metrics,
            "filter": [self._text(f, root=host) or f for f in q.filter] if q.filter else q.filter,
            "order": [self._order(o, root=host) for o in q.order] if q.order else q.order,
            "extensions": [self._extension(e) for e in q.extensions] if q.extensions else q.extensions,
        }
        if q.main_time_dimension is not None:
            update["main_time_dimension"] = self._reference_or_text(q.main_time_dimension, root=host)
        for key in self.fields:
            self.expansion(key)
        data = {f: getattr(q, f) for f in q.model_fields_set | {"version"}} | {
            f: v for f, v in update.items() if v is not None or f in q.model_fields_set}
        with migrating_document(self.document, stored=True):
            return SlayerQuery.model_validate(data)

    def _reference_or_text(self, text: str, *, root: Optional[SlayerDataset]) -> str:
        parsed = _parsed(text)
        hit = self._hit(parsed, root=root) if isinstance(parsed, (Ref, DottedRef)) else None
        if hit is not None:
            return self._expression(hit)
        return self._text(text, root=root) or text

    def _dimension(self, dim: Any, *, root: Optional[SlayerDataset]) -> Any:
        if isinstance(dim, ComputedDimension):
            text = self._text(dim.expression, root=root)
            return dim if text is None else ComputedDimension(expression=text, name=dim.name)
        hit = self._hit(dim, root=root) if isinstance(dim, FieldRef) else None
        if hit is None:
            return dim
        return ComputedDimension(expression=self._expression(hit), name="__".join([*hit[0], hit[1][1]]))

    def _time_dimension(self, td: TimeDimension, *, root: Optional[SlayerDataset]) -> TimeDimension:
        parsed = _parsed(td.field)
        hit = self._hit(parsed, root=root) if isinstance(parsed, (Ref, DottedRef)) else None
        if hit is not None:
            text, name = self._expression(hit), td.name or "__".join([*hit[0], hit[1][1]])
            label = td.label or self.fields[hit[1]].label
        else:
            text = self._text(td.field, root=root)
            if text is None:
                return td
            name, label = td.name or computed_dimension_name(td.field), td.label
        return TimeDimension.model_validate({
            "field": text, "name": name, "granularity": td.granularity, "date_range": td.date_range, "label": label,
        })

    def _metric(self, metric: DatasetMetric, *, root: Optional[SlayerDataset]) -> DatasetMetric:
        text = self._text(metric.expression, root=root) if metric.expression else None
        if text is None:
            return metric
        name = metric.name or expression_text_alias(parse_expr(metric.expression)).replace(".", "__")
        return DatasetMetric.model_validate({**metric.model_dump(exclude_none=True), "expression": text, "name": name})

    def _order(self, item: OrderItem, *, root: Optional[SlayerDataset]) -> OrderItem:
        if item.raw_expression:
            text = self._text(item.raw_expression, root=root)
            return item if text is None else item.model_copy(update={"raw_expression": text})
        hit = self._hit(item.field, root=root)
        if hit is None:
            return item
        return OrderItem.model_validate({"field": self._expression(hit), "direction": item.direction,
                                         "nulls": item.nulls})

    def _extension(self, extension: DatasetExtension) -> DatasetExtension:
        if not extension.metrics:
            return extension
        root = self.scope.datasets.get(self._owner(extension.source_name))
        return extension.model_copy(update={"metrics": [self._metric(m, root=root) for m in extension.metrics]})


def _visible(dataset: SlayerDataset, name: str) -> bool:
    field = dataset.get_field(name)
    return field is not None and not field.hidden


def _parts(ref: Any) -> Tuple[str, ...]:
    return (ref.name,) if isinstance(ref, Ref) else tuple(ref.parts)


def _quoting(ref: Any) -> Tuple[bool, ...]:
    """Each segment's quoted flag of a ``Ref``, ``DottedRef`` or ``FieldRef``."""
    if isinstance(ref, FieldRef):
        return tuple(ref.quoted) if ref.quoted else (False,) * len(ref.parts)
    return dotted_quoting(ref)


def _parsed(text: str) -> Any:
    try:
        return parse_expr(text)
    except ValueError:
        return None
