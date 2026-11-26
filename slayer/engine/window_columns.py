"""A query's row-level windows as windowed columns of the datasets they are anchored at (queries/window-functions):
each one a hidden windowed field of that dataset, computed through its relation door like a declared one and shared
with a declared field running the same SQL."""

from __future__ import annotations

import hashlib
from typing import Dict, Tuple

from sqlglot import exp
from sqlglot.expressions.core import Expression

from slayer.core.keys import (
    FieldKey,
    FieldSqlKey,
    ValueKey,
    WindowKey,
    reroot_value_key,
    value_type,
    walk_value_keys,
)
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.refs import key_display, register_window_display
from slayer.core.relationship_walker import dataset_field_element, dataset_field_type, terminal_dataset
from slayer.core.scope import DatasetScope
from slayer.engine.elaborate_env import check_row_window
from slayer.ir.bound import BoundExpr, BoundFilter, DeclaredMetric, OrderSpec
from slayer.ir.prebound import PreboundQuery
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import dialect_for_ds_type, get_dialect
from slayer.sql.dialects.base import SqlDialect
from slayer.sql.render.row_expr import render_row_expression
from slayer.sql.render.window import render_window

__all__ = ["WINDOW_FIELD_PREFIX", "lift_row_windows", "with_datasets", "with_window_fields"]

#: Name prefix of a query's windowed column (reserved ``__slayer_`` space).
WINDOW_FIELD_PREFIX = "__slayer_window_"


class _Lifter:
    """Turns each row-level window into a reference to a windowed column, extending the datasets it lands on."""

    def __init__(self, *, bundle: ResolvedSourceBundle, population: SlayerDataset) -> None:
        self.population = population
        self.by_name: Dict[str, SlayerDataset] = dict(bundle.datasets_by_name)
        self.by_name[population.name] = population
        self.extended: Dict[str, SlayerDataset] = {}
        self.dialect: SqlDialect = (dialect_for_ds_type(bundle.datasource_type) if bundle.datasource_type
                                    else get_dialect(bundle.dialect))
        self.memo: Dict[ValueKey, ValueKey] = {}

    def lift(self, key: ValueKey) -> ValueKey:
        if key not in self.memo:
            mapped = key.map_children(self.lift)
            if isinstance(mapped, WindowKey) and mapped.row_level:
                check_row_window(mapped)
                rewritten = self.dialect.rewrite_window_key(mapped)
                mapped = self._windowed_field(mapped) if rewritten is mapped else self.lift(rewritten)
            self.memo[key] = mapped
        return self.memo[key]

    def _dataset(self, name: str) -> SlayerDataset:
        return self.extended.get(name) or self.by_name[name]

    def _windowed_field(self, window: WindowKey) -> FieldSqlKey:
        check_row_window(window)
        route = window.anchor or ()
        anchor = terminal_dataset(root=self._dataset(self.population.name), path=route, datasets_by_name=self.by_name)
        assert anchor is not None  # the binder resolved every route
        anchor = self._dataset(anchor.name)
        local = reroot_value_key(window.model_copy(update={"anchor": ()}), target_path=route)
        labels = [key_display(o) if field_path_of(o) else f"{self.population.name}.{key_display(o)}"
                  for o in window.order]
        native = self._native(local, dataset=anchor, labels=labels)
        field = next((f for f in anchor.fields if f.windowed and f.definition_sql == native), None)
        if field is None:
            name = WINDOW_FIELD_PREFIX + hashlib.sha256(native.encode()).hexdigest()[:16]
            datatype = value_type(local, field_type=dataset_field_type(dataset=anchor, datasets_by_name=self.by_name))
            field = DatasetField.model_construct(name=name, hidden=True, datatype=datatype).with_native(
                native, windowed=True, states_nulls=True)
            anchor = anchor.model_copy(update={"fields": [*anchor.fields, field]})
            self.extended[anchor.name] = anchor
            self.by_name[anchor.name] = anchor
            register_window_display(name, key_display(window))
        return FieldSqlKey(path=route, dataset=anchor.name, field_name=field.name)

    def _native(self, window: WindowKey, *, dataset: SlayerDataset, labels: list[str]) -> str:
        """``window`` as the datasource's SQL over ``dataset``'s rows: a base field by its column, any other field by
        its dataset-qualified path (what a portable definition of the same text runs)."""

        def resolve(ref: FieldKey | FieldSqlKey) -> Expression:
            leaf = ref.leaf if isinstance(ref, FieldKey) else ref.field_name
            if not ref.path:
                field = dataset.get_field(leaf)
                if field is not None and field.is_physical_reader:
                    return exp.column(exp.to_identifier(field.physical_name, quoted=field.physical_quoted))
            *quals, last = (*(ref.path or (dataset.name,)), leaf)
            names = dict(zip(("table", "db", "catalog"), reversed(quals)))
            return exp.column(last, **names)

        field_type = dataset_field_type(dataset=dataset, datasets_by_name=self.by_name)
        field_element = dataset_field_element(dataset=dataset, datasets_by_name=self.by_name)

        def part(k: ValueKey) -> Expression:
            return render_row_expression(key=k, dialect=self.dialect, resolve_field=resolve, field_type=field_type,
                                         field_element=field_element)

        rendered = render_window(window, part=part, dialect=self.dialect, labels=labels)
        return rendered.sql(dialect=self.dialect.sqlglot_name)


def _filter(bf: BoundFilter, *, lift) -> BoundFilter:
    key = lift(bf.value_key)
    if key is bf.value_key:
        return bf
    return bf.model_copy(update={"value_key": key, "referenced_keys": _walk(key)})


def field_path_of(key: ValueKey) -> Tuple[str, ...]:
    """A field reference's path; ``()`` for any other key."""
    return key.path if isinstance(key, (FieldKey, FieldSqlKey)) else ()


def _walk(key: ValueKey) -> Tuple[ValueKey, ...]:
    return tuple(walk_value_keys(key))


def lift_row_windows(
    prebound: PreboundQuery, *, bundle: ResolvedSourceBundle, scope: DatasetScope,
) -> Tuple[PreboundQuery, ResolvedSourceBundle, DatasetScope]:
    """``prebound`` with each row-level window read as a windowed column of its anchor dataset, and the bundle and
    scope holding those datasets extended by them; unchanged when it holds none."""
    population = scope.population
    if population is None or not _holds_row_window(prebound):
        return prebound, bundle, scope
    lifter = _Lifter(bundle=bundle, population=population)
    lift = lifter.lift

    def bound(b: BoundExpr) -> BoundExpr:
        key = lift(b.value_key)
        return b if key is b.value_key else b.model_copy(update={"value_key": key})

    lifted = prebound.model_copy(update={
        "declared_metrics": [_declared(dm, bound=bound) for dm in prebound.declared_metrics],
        "bound_filters": [_filter(bf, lift=lift) for bf in prebound.bound_filters],
        "order_specs": [_order(sp, bound=bound) for sp in prebound.order_specs],
        "ranking_candidates": tuple(lift(k) for k in prebound.ranking_candidates),
    })
    if not lifter.extended:
        return lifted, bundle, scope
    return lifted, with_datasets(bundle, lifter.extended), _scope_with(scope, lifter.extended)


def _declared(dm: DeclaredMetric, *, bound) -> DeclaredMetric:
    new = bound(dm.bound)
    return dm if new is dm.bound else dm.model_copy(update={"bound": new})


def _order(sp: OrderSpec, *, bound) -> OrderSpec:
    new = bound(sp.bound)
    return sp if new is sp.bound else sp.model_copy(update={"bound": new})


def _holds_row_window(prebound: PreboundQuery) -> bool:
    roots = [*(dm.bound.value_key for dm in prebound.declared_metrics), *(bf.value_key for bf in prebound.bound_filters),
             *(sp.bound.value_key for sp in prebound.order_specs)]
    return any(isinstance(k, WindowKey) and k.row_level for r in roots for k in walk_value_keys(r))


def with_datasets(bundle: ResolvedSourceBundle, datasets: Dict[str, SlayerDataset]) -> ResolvedSourceBundle:
    """``bundle`` with each dataset named in ``datasets`` replaced by its value there."""
    population = bundle.population
    return bundle.model_copy(update={
        "population": datasets.get(population.name, population) if population is not None else None,
        "referenced_datasets": [datasets.get(d.name, d) for d in bundle.referenced_datasets],
    })


def with_window_fields(bundle: ResolvedSourceBundle, *, planned: ResolvedSourceBundle) -> ResolvedSourceBundle:
    """``bundle`` with each dataset ``planned`` extended by a query's windowed columns."""
    extended = {d.name: d for d in planned.datasets_by_name.values()
                if any(f.name.startswith(WINDOW_FIELD_PREFIX) for f in d.fields)}
    return with_datasets(bundle, extended) if extended else bundle


def _scope_with(scope: DatasetScope, datasets: Dict[str, SlayerDataset]) -> DatasetScope:
    population = scope.population
    if population is None or population.name not in datasets:
        return scope
    return scope.model_copy(update={"population": datasets[population.name]})
