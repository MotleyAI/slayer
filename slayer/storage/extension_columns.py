"""Stored extension columns inlined where they are used: each becomes a Mode-B expression, bound against the models of
its datasource. A stored query that cannot be converted raises ``UnconvertibleStoredQueryError``."""

from __future__ import annotations

import warnings
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, ConfigDict

from slayer.core.errors import AmbiguousReferenceError, UnconvertibleStoredQueryError
from slayer.core.models import SlayerModel
from slayer.core.names import match_name
from slayer.core.query import (
    ColumnRef,
    ComputedDimension,
    ModelExtension,
    ModelMeasure,
    OrderItem,
    SlayerQuery,
    TimeDimension,
    computed_dimension_name,
    extension_column_text,
)
from slayer.core.syntax import (
    DottedRef,
    Ref,
    canonical_measure_text,
    dotted_quoting,
    map_refs,
    onto_path,
    parse_expr,
)
from slayer.core.warnings import ExtensionColumnInlinedWarning, SlayerExtensionColumnInlinedWarning
from slayer.engine.extensions import QueryScope, query_scopes
from slayer.sql.naming import formula_text_alias
from slayer.storage.migrations import migrating_document

Key = Tuple[str, str]


def holds_removed_forms(query: Any) -> bool:
    """Whether a raw stored query carries an inline ``source_model`` or extension ``columns``."""
    if not isinstance(query, dict):
        return False
    source, extensions = query.get("source_model"), query.get("extensions")
    return isinstance(source, dict) or (
        isinstance(extensions, list) and any(isinstance(e, dict) and "columns" in e for e in extensions))


class _Column(BaseModel):
    """One extension column: its own expression, read from the model or stage it extends."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    owner: str
    name: str
    tree: Any
    derived: bool
    label: Optional[str] = None


def inline_extension_columns(
    stages: Sequence[dict], *, models_by_ds: Mapping[str, Mapping[str, SlayerModel]], data_source: Optional[str],
    priority: Sequence[str], document: str,
) -> List[SlayerQuery]:
    """The raw stored ``stages`` (migrated, one query list) with every extension column inlined where it is used."""
    for stage in stages:
        if isinstance(stage.get("source_model"), dict):
            raise UnconvertibleStoredQueryError("an inline source_model")
    raw_columns = [_extension_columns(stage) for stage in stages]
    with migrating_document(document, stored=True):
        queries = [SlayerQuery.model_validate(_without_columns(stage)) for stage in stages]
    out: List[SlayerQuery] = []
    for i, query in enumerate(queries):
        if not raw_columns[i]:
            out.append(query)
            continue
        scopes = query_scopes(queries, models_by_ds=models_by_ds, data_source=data_source)[i]
        if not scopes:
            raise UnconvertibleStoredQueryError("extension columns over models the store does not hold")
        extensions = [e for e in stages[i].get("extensions") or [] if isinstance(e, dict)]
        runs = [_Stage(query=query, raw=raw_columns[i], scope=scope, document=document)
                for scope in _running_scopes(scopes, priority=priority)]
        converted = [run.converted(extensions=extensions) for run in runs]
        if any(c != converted[0] for c in converted[1:]):
            raise UnconvertibleStoredQueryError("extension columns that convert differently in each datasource "
                                                "holding their models")
        for owner, name in runs[0].columns:
            warnings.warn(SlayerExtensionColumnInlinedWarning(ExtensionColumnInlinedWarning(
                document=document, column=f"{owner}.{name}",
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


def _extension_columns(stage: dict) -> List[Tuple[dict, List[Any]]]:
    """Each extension of ``stage`` carrying ``columns``, with them."""
    extensions = stage.get("extensions")
    return [
        (e, list(e["columns"])) for e in extensions if isinstance(e, dict) and e.get("columns")
    ] if isinstance(extensions, list) else []


def _without_columns(stage: dict) -> dict:
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
        self.columns: Dict[Key, _Column] = {}
        for extension, columns in raw:
            owner = self._owner(extension.get("source_name"))
            for column in columns:
                parsed = self._column(column, owner=owner)
                self.columns[(owner, parsed.name)] = parsed
        self._expanded: Dict[Key, Any] = {}
        self._visiting: Set[Key] = set()

    def _owner(self, name: Any) -> str:
        if not isinstance(name, str):
            raise UnconvertibleStoredQueryError("an extension without source_name")
        found = next((n for n in self.scope.models if n == name), None) or next(
            (n for n in self.scope.models if n.casefold() == name.casefold()), None)
        if found is None:
            raise UnconvertibleStoredQueryError(f"extension columns of {name!r}, which the store does not hold")
        return found

    def _column(self, raw: Any, *, owner: str) -> _Column:
        if not isinstance(raw, dict) or not isinstance(raw.get("name"), str):
            raise UnconvertibleStoredQueryError(f"an unnamed extension column of {owner!r}")
        name = raw["name"]
        what = f"extension column {owner}.{name}"
        try:
            text = extension_column_text(raw, owner=owner, what=what)
        except ValueError as exc:
            raise UnconvertibleStoredQueryError(str(exc)) from exc
        model = self.scope.models[owner]
        stored = owner not in self.scope.siblings
        if raw.get("sql") is None:
            if stored and not _visible(model, name):
                raise UnconvertibleStoredQueryError(f"{what}, which names no visible column of {owner!r}")
            return _Column(owner=owner, name=name, tree=Ref(name=name), derived=False, label=raw.get("label"))
        if stored and model.get_column(name) is not None:
            raise UnconvertibleStoredQueryError(f"{what}, named like a column of {owner!r}")
        return _Column(owner=owner, name=name, tree=parse_expr(text), derived=True, label=raw.get("label"))

    def _check_join_keys(self, extension: dict) -> None:
        owner = self._owner(extension.get("source_name"))
        for join in extension.get("joins") or []:
            target = join.get("target_model") if isinstance(join, dict) else None
            target_owner = next((n for n in self.scope.models if isinstance(target, str)
                                 and n.casefold() == target.casefold()), target)
            for pair in (join.get("join_pairs") or []) if isinstance(join, dict) else []:
                if (owner, pair[0]) in self.columns or (target_owner, pair[1]) in self.columns:
                    raise UnconvertibleStoredQueryError(f"an extension join of {owner!r} keyed on an extension column")

    # -- resolution ----------------------------------------------------------
    def _hit(self, ref: Any, *, root: Optional[SlayerModel]) -> Optional[Tuple[Tuple[str, ...], Key]]:
        """The qualifying path and extension column a reference resolves to; ``None`` for anything else."""
        parts, quoted = _parts(ref), _quoting(ref)
        hops, leaf = tuple(parts[:-1]), parts[-1]
        start = self._start(hops, leaf=leaf, quoted=quoted, root=root)
        if start is None:
            return None
        prefix, model, rest = start
        resolved = self.scope.resolve(rest, root=model) if rest else ((), [])
        if resolved is None:
            return None
        path, chain = resolved
        owner = chain[-1].target_model if chain else model.name
        name = self._column_named(owner, leaf=leaf, quoted=quoted[-1])
        return ((*prefix, *path), (owner, name)) if name is not None else None

    def _column_named(self, owner: str, *, leaf: str, quoted: bool) -> Optional[str]:
        """The extension column of ``owner`` that ``leaf`` names, matched as the engine matches a column name."""
        own = [n for o, n in self.columns if o == owner]
        if not own:
            return None
        model = self.scope.models.get(owner)
        try:
            found = match_name(name=leaf, declared=[*own, *(c.name for c in model.columns)] if model else own,
                               quoted=quoted)
        except AmbiguousReferenceError as exc:
            raise UnconvertibleStoredQueryError(f"a reference {leaf!r} that matches several columns of "
                                                f"{owner!r}") from exc
        return found if found in own else None

    def _start(
        self, hops: Tuple[str, ...], *, leaf: str, quoted: Tuple[bool, ...], root: Optional[SlayerModel],
    ) -> Optional[Tuple[Tuple[str, ...], SlayerModel, Tuple[str, ...]]]:
        """The qualifying prefix, model and remaining hops a reference resolves from."""
        if root is not None:
            return (), root, hops
        head_name = match_name(name=hops[0], declared=self.scope.models, quoted=quoted[0]) if hops else None
        if head_name is not None:
            return (head_name,), self.scope.models[head_name], hops[1:]
        if not hops and any(k[1] == leaf or (not quoted[0] and k[1].casefold() == leaf.casefold())
                            for k in self.columns):
            raise UnconvertibleStoredQueryError(f"a reference to the extension column {leaf!r} without a "
                                                f"population to attribute it to")
        return None

    def expansion(self, key: Key) -> Any:
        """Column ``key``'s expression with every extension column it reads expanded, read from its owner."""
        if key in self._expanded:
            return self._expanded[key]
        column = self.columns[key]
        if not column.derived:
            return column.tree
        if key in self._visiting:
            raise UnconvertibleStoredQueryError(f"a dependency cycle among extension columns through {key[1]!r}")
        self._visiting.add(key)
        tree = self._inlined(column.tree, root=self.scope.models[column.owner])
        self._visiting.discard(key)
        self._expanded[key] = tree
        return tree

    def _inlined(self, tree: Any, *, root: Optional[SlayerModel]) -> Any:
        def _replace(ref: Ref | DottedRef) -> Any:
            hit = self._hit(ref, root=root)
            return ref if hit is None else onto_path(self.expansion(hit[1]), hit[0])

        return map_refs(tree, _replace)

    def _text(self, text: str, *, root: Optional[SlayerModel]) -> Optional[str]:
        """``text`` with its extension columns inlined; ``None`` when it reads none."""
        try:
            parsed = parse_expr(text)
        except ValueError:
            return None
        out = self._inlined(parsed, root=root)
        return None if out is parsed else canonical_measure_text(out)

    def _expression(self, hit: Tuple[Tuple[str, ...], Key]) -> str:
        return canonical_measure_text(onto_path(self.expansion(hit[1]), hit[0]))

    # -- positions -----------------------------------------------------------
    def converted(self, *, extensions: List[dict]) -> SlayerQuery:
        for extension in extensions:
            self._check_join_keys(extension)
        q, host = self.query, self.scope.host
        update: Dict[str, Any] = {
            "dimensions": [self._dimension(d, root=host) for d in q.dimensions] if q.dimensions else q.dimensions,
            "time_dimensions": [self._time_dimension(td, root=host) for td in q.time_dimensions]
            if q.time_dimensions else q.time_dimensions,
            "measures": [self._measure(m, root=host) for m in q.measures] if q.measures else q.measures,
            "filters": [self._text(f, root=host) or f for f in q.filters] if q.filters else q.filters,
            "order": [self._order(o, root=host) for o in q.order] if q.order else q.order,
            "extensions": [self._extension(e) for e in q.extensions] if q.extensions else q.extensions,
        }
        if q.main_time_dimension is not None:
            update["main_time_dimension"] = self._reference_or_text(q.main_time_dimension, root=host)
        for key in self.columns:
            self.expansion(key)
        data = {f: getattr(q, f) for f in q.model_fields_set | {"version"}} | {
            f: v for f, v in update.items() if v is not None or f in q.model_fields_set}
        with migrating_document(self.document, stored=True):
            return SlayerQuery.model_validate(data)

    def _reference_or_text(self, text: str, *, root: Optional[SlayerModel]) -> str:
        parsed = _parsed(text)
        hit = self._hit(parsed, root=root) if isinstance(parsed, (Ref, DottedRef)) else None
        if hit is not None:
            return self._expression(hit)
        return self._text(text, root=root) or text

    def _dimension(self, dim: Any, *, root: Optional[SlayerModel]) -> Any:
        if isinstance(dim, ComputedDimension):
            text = self._text(dim.expression, root=root)
            return dim if text is None else ComputedDimension(expression=text, name=dim.name)
        hit = self._hit(dim, root=root) if isinstance(dim, ColumnRef) else None
        if hit is None:
            return dim
        return ComputedDimension(expression=self._expression(hit), name="__".join([*hit[0], hit[1][1]]))

    def _time_dimension(self, td: TimeDimension, *, root: Optional[SlayerModel]) -> TimeDimension:
        parsed = _parsed(td.dimension)
        hit = self._hit(parsed, root=root) if isinstance(parsed, (Ref, DottedRef)) else None
        if hit is not None:
            text, name = self._expression(hit), td.name or "__".join([*hit[0], hit[1][1]])
            label = td.label or self.columns[hit[1]].label
        else:
            text = self._text(td.dimension, root=root)
            if text is None:
                return td
            name, label = td.name or computed_dimension_name(td.dimension), td.label
        return TimeDimension.model_validate({
            "dimension": text, "name": name, "granularity": td.granularity, "date_range": td.date_range, "label": label,
        })

    def _measure(self, measure: ModelMeasure, *, root: Optional[SlayerModel]) -> ModelMeasure:
        text = self._text(measure.formula, root=root) if measure.formula else None
        if text is None:
            return measure
        name = measure.name or formula_text_alias(parse_expr(measure.formula)).replace(".", "__")
        return ModelMeasure.model_validate({**measure.model_dump(exclude_none=True), "formula": text, "name": name})

    def _order(self, item: OrderItem, *, root: Optional[SlayerModel]) -> OrderItem:
        if item.raw_formula:
            text = self._text(item.raw_formula, root=root)
            return item if text is None else item.model_copy(update={"raw_formula": text})
        hit = self._hit(item.column, root=root)
        if hit is None:
            return item
        return OrderItem.model_validate({"column": self._expression(hit), "direction": item.direction,
                                         "nulls": item.nulls})

    def _extension(self, extension: ModelExtension) -> ModelExtension:
        if not extension.measures:
            return extension
        root = self.scope.models.get(self._owner(extension.source_name))
        return extension.model_copy(update={"measures": [self._measure(m, root=root) for m in extension.measures]})


def _visible(model: SlayerModel, name: str) -> bool:
    column = model.get_column(name)
    return column is not None and not column.hidden


def _parts(ref: Any) -> Tuple[str, ...]:
    return (ref.name,) if isinstance(ref, Ref) else tuple(ref.parts)


def _quoting(ref: Any) -> Tuple[bool, ...]:
    """Each segment's quoted flag of a ``Ref``, ``DottedRef`` or ``ColumnRef``."""
    if isinstance(ref, ColumnRef):
        return tuple(ref.quoted) if ref.quoted else (False,) * len(ref.parts)
    return dotted_quoting(ref)


def _parsed(text: str) -> Any:
    try:
        return parse_expr(text)
    except ValueError:
        return None
