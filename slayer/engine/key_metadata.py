"""Key → slot-metadata lifts (``type`` / ``format`` / ``description``).

Shared by the compiler and the re-rooting strategy; the seam types they
decorate live in ``slayer.ir.prebound``.
"""

from __future__ import annotations

from typing import Callable, Optional, Tuple

from slayer.core.enums import (
    BUILTIN_AGGREGATIONS,
    AggregationValueClass,
    DataType,
    classify_aggregation,
)
from slayer.core.format import NumberFormat
from slayer.core.keys import (
    CONDITIONAL_SCALARS,
    AggregateKey,
    FieldElementFn,
    FieldKey,
    FieldTypeFn,
    FieldSqlKey,
    CastKey,
    ListSetKey,
    ScalarCallKey,
    StarKey,
    TimeTruncKey,
    ValueKey,
    aggregation_source_type,
    list_element_type,
    list_valued,
    temporal_type,
    value_type,
)
from slayer.core.relationship_walker import dataset_field, dataset_field_element, dataset_field_type
from slayer.core.models import SlayerDataset
from slayer.core.scope import DatasetScope
from slayer.core.refs import EXPRESSION_SOURCE_KINDS
from slayer.sql.dialects.reflection import is_exact_numeric_db_type
from slayer.engine.response_meta import _infer_aggregated_format
from slayer.ir.prebound import walk_key_path
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import SqlDialect, dialect_for_ds_type, get_dialect
from slayer.sql.naming import expression_source_leaf

__all__ = [
    "aggregated_type",
    "aggregation_result_type",
    "dimension_key_metadata",
    "key_element_type",
    "metric_key_format_description",
    "metric_key_preserves_native_type",
    "metric_key_type",
    "scope_field_type",
]


# ---------------------------------------------------------------------------
# Key -> slot metadata
# ---------------------------------------------------------------------------

def aggregation_result_type(
    *, metric_name: Optional[str], aggregation: str, source_type: Optional[DataType],
) -> Tuple[Optional[DataType], Optional[DataType]]:
    """``(type, element type)`` via ``classify_aggregation``: COUNT → INT, the float classes → DOUBLE,
    PRESERVING → ``source_type`` (unknown for a custom aggregation over a boolean, which reads it raw), LIST → a list
    of ``source_type``."""
    cls = classify_aggregation(metric_name=metric_name, aggregation=aggregation, source_type=source_type)
    if cls is AggregationValueClass.COUNT:
        return DataType.INTEGER, None
    if cls is AggregationValueClass.LIST:
        return DataType.ARRAY, source_type or DataType.OPAQUE
    if cls is AggregationValueClass.PRESERVING:
        custom_over_boolean = source_type is DataType.BOOLEAN and aggregation not in BUILTIN_AGGREGATIONS
        return (None if custom_over_boolean else source_type), None
    return DataType.FLOAT, None


def aggregated_type(
    *,
    dataset: SlayerDataset,
    metric_name: Optional[str],
    aggregation: str,
) -> Optional[DataType]:
    """Type for an aggregate over ``dataset``'s field ``metric_name`` (``"*"`` for ``count(*)``)."""
    col = dataset.get_field(metric_name) if metric_name is not None else None
    return aggregation_result_type(
        metric_name=metric_name, aggregation=aggregation, source_type=col.datatype if col is not None else None,
    )[0]


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


def date_scalar_type(key: ValueKey, *, field_type: FieldTypeFn) -> Optional[DataType]:
    """INT for ``date_part``/``date_diff``, else the key's DATE/TIMESTAMP type (date functions,
    temporal conditionals, min/max/first/last of a temporal value); ``None`` otherwise."""
    if isinstance(key, ScalarCallKey) and key.name in ("date_part", "date_diff"):
        return DataType.INTEGER
    return temporal_type(key, field_type=field_type)


def _typed_by_value(key: ValueKey) -> bool:
    """A bare field or a value-returning conditional, typed by its ``value_type``."""
    return isinstance(key, (FieldKey, FieldSqlKey)) or (
        isinstance(key, ScalarCallKey) and key.name in CONDITIONAL_SCALARS)


def metric_key_type(
    *, dataset: SlayerDataset, key: ValueKey, bundle: Optional[ResolvedSourceBundle] = None,
) -> Optional[DataType]:
    """``type`` for a metric slot, from its bound key alone."""
    if isinstance(key, CastKey):
        return key.data_type
    if list_valued(key, field_type=_dataset_types(dataset=dataset, bundle=bundle)):
        return DataType.ARRAY
    if _typed_by_value(key):
        return value_type(key, field_type=_dataset_types(dataset=dataset, bundle=bundle))
    date_type = date_scalar_type(key, field_type=_dataset_types(dataset=dataset, bundle=bundle))
    if date_type is not None:
        return date_type
    name = _local_aggregate_source_name(key)
    if not isinstance(key, AggregateKey):
        return None
    if name is None:
        # A cross-dataset source's metadata is lifted by response_meta; a count is an integer over any source.
        counted = classify_aggregation(metric_name=None, aggregation=key.agg, source_type=None)
        return DataType.INTEGER if counted is AggregationValueClass.COUNT else None
    return aggregation_result_type(
        metric_name=name, aggregation=key.agg,
        source_type=aggregation_source_type(key.source, field_type=_dataset_types(dataset=dataset, bundle=bundle)),
    )[0]


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


def dimension_key_metadata(
    *, dataset: SlayerDataset, key: ValueKey, bundle: ResolvedSourceBundle,
) -> Tuple[Optional[DataType], Optional[NumberFormat], Optional[str]]:
    """``(type, format, description)`` for a dimension slot, from its key.

    A LOCAL dimension carries the source field's full display contract; a
    JOINED one carries only its type. That asymmetry is deliberate and
    pre-existing: joined refs surface format / description through
    ``response_meta``, which resolves them against the owning dataset.
    """
    if isinstance(key, CastKey):
        return key.data_type, None, None
    inner = key.source if isinstance(key, (TimeTruncKey, ListSetKey)) else key
    if not isinstance(inner, (FieldKey, FieldSqlKey)):
        date_type = date_scalar_type(key, field_type=_dataset_types(dataset=dataset, bundle=bundle))
        if date_type is not None:
            return date_type, None, None
    path = tuple(getattr(inner, "path", ()) or ())
    leaf = getattr(inner, "leaf", None) or getattr(inner, "field_name", None)
    if leaf is None:
        return None, None, None
    if not path:
        col = dataset.get_field(leaf)
        if col is None:
            return None, None, None
        return col.datatype, col.format, col.description
    terminal = walk_key_path(dataset=dataset, path=path, bundle=bundle)
    if terminal is None:
        return None, None, None
    col = terminal.get_field(leaf)
    return (col.datatype if col is not None else None), None, None
