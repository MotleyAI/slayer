"""Key → slot-metadata lifts (``type`` / ``format`` / ``description``).

Shared by the compiler and the re-rooting strategy; the seam types they
decorate live in ``slayer.ir.prebound``.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from decimal import Decimal
from types import MappingProxyType
from typing import Callable, Generator, Mapping, Optional, Tuple

from slayer.core.enums import DataType
from slayer.core.errors import SlayerError
from slayer.core.format import NumberFormat
from slayer.core.keys import (
    AggregateKey,
    CastKey,
    FieldElementFn,
    FieldKey,
    FieldTypeFn,
    FieldSqlKey,
    ListSetKey,
    LiteralKey,
    SqlFragmentKey,
    StarKey,
    TimeTruncKey,
    TransformKey,
    ValueKey,
    aggregation_source_type,
    list_element_type,
    list_valued,
    prepend_value_key,
    value_type,
    walk_value_keys,
)
from slayer.core.relationship_walker import (
    aggregation_owner,
    dataset_field,
    dataset_field_element,
    dataset_field_type,
    follow,
)
from slayer.core.models import (
    VALUE_PLACEHOLDER,
    SlayerDataset,
    aggregation_definition,
    builtin_semantics,
    rendered_expression,
)
from slayer.core.scope import DatasetScope
from slayer.core.refs import EXPRESSION_SOURCE_KINDS
from slayer.sql.dialects.reflection import is_exact_numeric_db_type
from slayer.sql.template_type import TemplateBinding, TypedFragment, template_result_type
from slayer.engine.param_binding import bind_aggregation_params
from slayer.engine.response_meta import _infer_aggregated_format
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import SqlDialect, dialect_for_ds_type, get_dialect
from slayer.sql.naming import expression_source_leaf

__all__ = [
    "PlaceholderTypes",
    "SlotTyping",
    "minted_placeholders",
    "dimension_key_format_description",
    "field_aggregate_type",
    "key_element_type",
    "metric_key_format_description",
    "metric_key_preserves_native_type",
    "scope_field_type",
]

_NO_PLACEHOLDERS: Mapping[ValueKey, ValueKey] = MappingProxyType({})
# A minted regroup placeholder's type in the plan that minted it, by leaf.
PlaceholderTypes = Mapping[str, Callable[[], Optional[DataType]]]
#: The placeholders minted by each enclosing plan still compiling, innermost last.
_MINTED: ContextVar[Tuple[PlaceholderTypes, ...]] = ContextVar("_MINTED", default=())


@contextmanager
def minted_placeholders(types: PlaceholderTypes) -> Generator[None]:
    """Make ``types`` (a live mapping) visible to every plan compiled inside the block."""
    token = _MINTED.set((*_MINTED.get(), types))
    try:
        yield
    finally:
        _MINTED.reset(token)


def _minted_type(leaf: str) -> Optional[Callable[[], Optional[DataType]]]:
    for types in reversed(_MINTED.get()):
        found = types.get(leaf)
        if found is not None:
            return found
    return None
# Leaves that read rows: a value without any is built from literals alone.
_ROW_READING = (FieldKey, FieldSqlKey, StarKey, SqlFragmentKey, AggregateKey, TransformKey)


class SlotTyping:
    """The one derivation of a key's type over a scope: field leaves through the bundle (a regroup placeholder by
    its original key), expression aggregations by their definition."""

    def __init__(
        self, *, scope: DatasetScope, bundle: ResolvedSourceBundle,
        placeholders: Mapping[ValueKey, ValueKey] = _NO_PLACEHOLDERS,
    ) -> None:
        self._base_type = scope_field_type(scope=scope, bundle=bundle)
        self._base_element = scope_field_element(scope=scope, bundle=bundle)
        self._placeholders = placeholders
        self._population = scope.population
        self._datasets = bundle.datasets_by_name
        self._dialect = bundle.dialect

    def with_placeholders(self, placeholders: Mapping[ValueKey, ValueKey]) -> "SlotTyping":
        """This typing, regroup placeholders resolving to their original keys."""
        typing = object.__new__(SlotTyping)
        typing.__dict__.update(self.__dict__, _placeholders={**self._placeholders, **placeholders})
        return typing

    def _original(self, key: object) -> Optional[ValueKey]:
        """A placeholder's original key, re-anchored at the path it is read through."""
        if not isinstance(key, FieldKey):
            return None
        found = self._placeholders.get(key)
        if found is None and key.path:
            local = self._placeholders.get(FieldKey(leaf=key.leaf))
            found = None if local is None else prepend_value_key(local, host_path=key.path)
        return found

    def field_type(self, key: object) -> Optional[DataType]:
        original = self._original(key)
        if original is not None:
            return self.value_type(original)
        minted = _minted_type(key.leaf) if isinstance(key, FieldKey) else None
        return minted() if minted is not None else self._base_type(key)

    def field_element(self, key: object) -> Optional[DataType]:
        original = self._original(key)
        if original is None:
            return self._base_element(key)
        return list_element_type(original, field_type=self.field_type, field_element=self.field_element)

    def value_type(self, key: object) -> Optional[DataType]:
        """``key``'s type (a cast's target, opaque ones included); ``None`` only for NULL or an unresolvable field."""
        if isinstance(key, CastKey):
            return key.data_type
        return value_type(key, field_type=self.field_type, aggregate_type=self.expression_type)

    def type_of(self, key: ValueKey) -> DataType:
        """``key``'s slot type: a NULL-only value is INT."""
        found = self.value_type(key)
        if found is not None:
            return found
        if not any(isinstance(k, _ROW_READING) for k in walk_value_keys(key)):
            return DataType.INTEGER
        if self._population is not None:
            # The walker's own refusal (an ambiguous or broken hop) names the cause.
            for k in walk_value_keys(key):
                if isinstance(k, FieldKey) and k.path:
                    follow(root=self._population, path=tuple(k.path), datasets_by_name=self._datasets)
        raise TypeError(f"no type derives for {key!r}: a field it reads does not resolve")

    def preserves_native_type(self, key: ValueKey, data_type: DataType) -> bool:
        """Whether a numeric ``key`` reads a field of an exact-numeric database type (a cast would erase it)."""
        if data_type not in (DataType.DECIMAL, DataType.FLOAT) or self._population is None:
            return False
        return any(
            is_exact_numeric_db_type(getattr(dataset_field(
                dataset=self._population, datasets_by_name=self._datasets, key=k), "db_type", None))
            for k in walk_value_keys(key) if isinstance(k, (FieldKey, FieldSqlKey))
        )

    def element_type_of(self, key: ValueKey, data_type: Optional[DataType]) -> Optional[DataType]:
        """A list's element type (OPAQUE when it cannot be told); ``None`` for a scalar."""
        if data_type is not DataType.ARRAY:
            return None
        if not list_valued(key, field_type=self.field_type):
            return DataType.OPAQUE
        found = list_element_type(key, field_type=self.field_type, field_element=self.field_element)
        return found or DataType.OPAQUE

    def expression_type(self, key: AggregateKey) -> Optional[DataType]:
        """An expression aggregation's declared ``datatype``, else its expression's type over the bound call."""
        owner = aggregation_owner(root=self._population, source=key.source, datasets_by_name=self._datasets)
        definition = aggregation_definition(owner=owner, agg=key.agg)
        if definition is not None and definition.datatype is not None:
            return definition.datatype
        expression = rendered_expression(agg=key.agg, definition=definition)
        if expression is None:
            return None
        bindings = {VALUE_PLACEHOLDER: self._binding(key.source)}
        bindings.update((name, self._binding(value)) for name, value in key.kwargs)
        return template_result_type(template=expression, bindings=bindings, dialect=self._dialect)

    def _binding(self, value: object) -> TemplateBinding:
        if isinstance(value, SqlFragmentKey):
            return TypedFragment(template=value.template, ref_types=tuple(self.value_type(r) for r in value.refs))
        if isinstance(value, LiteralKey):
            value = value.value
        if value is None or isinstance(value, (Decimal, bool, str)):
            return value
        if isinstance(value, StarKey):
            return None
        return self.value_type(value)


def field_aggregate_type(*, dataset: SlayerDataset, field_name: str, aggregation: str) -> Optional[DataType]:
    """The type of ``aggregation`` over ``dataset``'s field ``field_name`` (``"*"``: its rows), dialect-neutral;
    ``None`` when a parameter default does not bind within ``dataset`` alone (it reads a related dataset)."""
    definition = aggregation_definition(owner=dataset, agg=aggregation)
    builtin = builtin_semantics(definition)
    # As the planner: an expression aggregation's declared datatype wins; a built-in's type is its class's.
    if not builtin and definition is not None and definition.datatype is not None:
        return definition.datatype
    source = StarKey() if field_name == "*" else FieldKey(leaf=field_name)
    bundle = ResolvedSourceBundle(dialect="", population=dataset)
    kwargs: tuple = ()
    if not builtin:
        try:
            kwargs = bind_aggregation_params(
                agg=aggregation, source=source, kwargs=(), bundle=bundle, bind_quoted=_no_quoted)
        except SlayerError:
            return None
    key = AggregateKey(source=source, agg=aggregation, kwargs=kwargs, builtin=builtin)
    return SlotTyping(scope=DatasetScope(population=dataset), bundle=bundle).value_type(key)


def _no_quoted(text: str) -> object:
    raise AssertionError(f"no query-side argument to bind: {text!r}")


def _local_aggregate_source_name(key: ValueKey) -> Optional[str]:
    """The source field name of a LOCAL aggregate, or ``None``.

    Expression sources (DEV-1826) surface their derived leaf — never a real
    field, so PRESERVING type/format lookups miss and fall to the class
    default (plain numeric), per design. ``None`` for anything else that isn't
    a bare local aggregate — a non-aggregate key or a cross-dataset source
    (whose metadata is lifted by ``response_meta`` against the target dataset).
    """
    if not isinstance(key, AggregateKey):
        return None
    src = key.source
    if isinstance(src, StarKey):
        return "*"
    if isinstance(src, EXPRESSION_SOURCE_KINDS):
        return expression_source_leaf(src)
    if not isinstance(src, (FieldKey, FieldSqlKey)):
        return None
    if getattr(src, "path", ()):
        return None
    return getattr(src, "leaf", None) or getattr(src, "field_name", None)


def _dataset_types(*, dataset: SlayerDataset, bundle: Optional[ResolvedSourceBundle]) -> FieldTypeFn:
    return dataset_field_type(
        dataset=dataset, datasets_by_name=bundle.datasets_by_name if bundle is not None else {},
    )


def scope_field_element(*, scope: DatasetScope, bundle: ResolvedSourceBundle) -> FieldElementFn:
    if scope.population is None:
        return lambda _key: None
    return dataset_field_element(dataset=scope.population, datasets_by_name=bundle.datasets_by_name)


def key_element_type(key: ValueKey, *, scope: DatasetScope, bundle: ResolvedSourceBundle) -> Optional[DataType]:
    """The element type of a list-valued ``key`` (``UNKNOWN`` when it cannot be told); ``None`` for a scalar."""
    field_type = scope_field_type(scope=scope, bundle=bundle)
    if not list_valued(key, field_type=field_type):
        return None
    element = list_element_type(
        key, field_type=field_type, field_element=scope_field_element(scope=scope, bundle=bundle),
    )
    return element or DataType.OPAQUE


def scope_field_type(*, scope: DatasetScope, bundle: ResolvedSourceBundle) -> FieldTypeFn:
    if scope.population is None:
        return lambda _key: None
    return dataset_field_type(dataset=scope.population, datasets_by_name=bundle.datasets_by_name)


def bundle_dialect(bundle: ResolvedSourceBundle) -> SqlDialect:
    """The datasource's own dialect (``MariadbDialect`` for a MariaDB source), else the sqlglot name's."""
    own = dialect_for_ds_type(bundle.datasource_type)
    return own if own.sqlglot_name == bundle.dialect else get_dialect(bundle.dialect)


def scope_zoned_timestamp(*, scope: DatasetScope, bundle: ResolvedSourceBundle) -> Callable[[object], bool]:
    """Whether a dataset field leaf holds a zoned instant on the bundle's dialect."""
    dialect = bundle_dialect(bundle)
    if not dialect.zoned_timestamp_db_types or scope.population is None:
        return lambda _key: False
    dataset, datasets_by_name = scope.population, bundle.datasets_by_name
    return lambda key: dialect.is_zoned_db_type(
        getattr(dataset_field(dataset=dataset, datasets_by_name=datasets_by_name, key=key), "db_type", None))


def metric_key_preserves_native_type(*, dataset: SlayerDataset, key: ValueKey) -> bool:
    """Whether an aggregate's inferred type would erase exact DB precision."""
    name = _local_aggregate_source_name(key)
    if name is None:
        return False
    col = dataset.get_field(name)
    if col is None:
        return False
    return is_exact_numeric_db_type(col.db_type)


def metric_key_format_description(
    *, dataset: SlayerDataset, key: ValueKey, bundle: Optional[ResolvedSourceBundle] = None,
) -> Tuple[Optional[NumberFormat], Optional[str]]:
    """``format`` / ``description`` for a metric slot, from its bound key.

    ``count(*)`` has an inferred INTEGER format but no description — there is
    no source field to document it.
    """
    name = _local_aggregate_source_name(key)
    if name is None or not isinstance(key, AggregateKey):
        return None, None
    fmt = _infer_aggregated_format(
        dataset=dataset, metric_name=name, aggregation=key.agg,
        source_type=aggregation_source_type(key.source, field_type=_dataset_types(dataset=dataset, bundle=bundle)),
    )
    if name == "*":
        return fmt, None
    col = dataset.get_field(name)
    return fmt, (col.description if col is not None else None)


def dimension_key_format_description(
    *, dataset: SlayerDataset, key: ValueKey,
) -> Tuple[Optional[NumberFormat], Optional[str]]:
    """``(format, description)`` for a dimension slot: a LOCAL field's display contract; a JOINED one surfaces its
    own through ``response_meta``."""
    inner = key.source if isinstance(key, (TimeTruncKey, ListSetKey)) else key
    if not isinstance(inner, (FieldKey, FieldSqlKey)) or inner.path:
        return None, None
    col = dataset.get_field(inner.leaf if isinstance(inner, FieldKey) else inner.field_name)
    return (col.format, col.description) if col is not None else (None, None)
