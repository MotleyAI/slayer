"""Key → slot-metadata lifts (``type`` / ``format`` / ``description``).

Shared by the compiler and the re-rooting strategy; the seam types they
decorate live in ``slayer.ir.prebound``.
"""

from __future__ import annotations

from typing import Callable, Optional, Tuple

from slayer.core.enums import (
    AggregationValueClass,
    DataType,
    classify_aggregation,
)
from slayer.core.format import NumberFormat
from slayer.core.keys import (
    CONDITIONAL_SCALARS,
    AggregateKey,
    ColumnElementFn,
    ColumnKey,
    ColumnTypeFn,
    ColumnSqlKey,
    CastKey,
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
from slayer.core.join_walker import LIST_ELEMENT_COLUMN, model_column_element, model_column_lookup, model_column_type
from slayer.core.models import SlayerModel
from slayer.core.scope import ModelScope, StageSchema
from slayer.core.refs import EXPRESSION_SOURCE_KINDS
from slayer.sql.dialects.reflection import is_exact_numeric_db_type
from slayer.engine.response_meta import _infer_aggregated_format
from slayer.ir.prebound import walk_key_path
from slayer.sql.dialects import SqlDialect, dialect_for_ds_type, get_dialect
from slayer.sql.naming import expression_source_leaf

__all__ = [
    "aggregated_type",
    "aggregation_result_type",
    "dimension_key_metadata",
    "key_element_type",
    "measure_key_format_description",
    "measure_key_preserves_native_type",
    "measure_key_type",
    "scope_column_type",
    "stage_measure_type",
]


# ---------------------------------------------------------------------------
# Key -> slot metadata
# ---------------------------------------------------------------------------

def aggregation_result_type(
    *, measure_name: Optional[str], aggregation: str, source_type: Optional[DataType],
) -> Tuple[Optional[DataType], Optional[DataType]]:
    """``(type, element type)`` via ``classify_aggregation``: COUNT → INT, the float classes → DOUBLE,
    PRESERVING → ``source_type``, LIST → a list of ``source_type``."""
    cls = classify_aggregation(measure_name=measure_name, aggregation=aggregation, source_type=source_type)
    if cls is AggregationValueClass.COUNT:
        return DataType.INT, None
    if cls is AggregationValueClass.LIST:
        return DataType.ARRAY, source_type or DataType.UNKNOWN
    if cls is AggregationValueClass.PRESERVING:
        return source_type, None
    return DataType.DOUBLE, None


def aggregated_type(
    *,
    model: SlayerModel,
    measure_name: Optional[str],
    aggregation: str,
) -> Optional[DataType]:
    """Type for an aggregate over ``model``'s column ``measure_name`` (``"*"`` for ``count(*)``)."""
    col = model.get_column(measure_name) if measure_name is not None else None
    return aggregation_result_type(
        measure_name=measure_name, aggregation=aggregation, source_type=col.type if col is not None else None,
    )[0]


def _local_aggregate_source_name(key: ValueKey) -> Optional[str]:
    """The source column name of a LOCAL aggregate, or ``None``.

    Expression sources (DEV-1826) surface their derived leaf — never a real
    column, so PRESERVING type/format lookups miss and fall to the class
    default (plain numeric), per design. ``None`` for anything else that isn't
    a bare local aggregate — a non-aggregate key or a cross-model source
    (whose metadata is lifted by ``response_meta`` against the target model).
    """
    if not isinstance(key, AggregateKey):
        return None
    src = key.source
    if isinstance(src, StarKey):
        return "*"
    if isinstance(src, EXPRESSION_SOURCE_KINDS):
        return expression_source_leaf(src)
    if not isinstance(src, (ColumnKey, ColumnSqlKey)):
        return None
    if getattr(src, "path", ()):
        return None
    return getattr(src, "leaf", None) or getattr(src, "column_name", None)


def _model_types(*, model: SlayerModel, bundle) -> ColumnTypeFn:
    return model_column_type(
        model=model, models_by_name=bundle.models_by_name if bundle is not None else {},
    )


def _stage_element(schema: StageSchema, key: ValueKey) -> Optional[DataType]:
    """The element type for ``L.value`` over a stage's list column ``L``."""
    if not (isinstance(key, ColumnKey) and len(key.path) == 1 and key.leaf == LIST_ELEMENT_COLUMN):
        return None
    col = schema.get(key.path[0])
    return (col.element_type or DataType.UNKNOWN) if col is not None and col.type is DataType.ARRAY else None


def stage_column_type(schema: StageSchema) -> ColumnTypeFn:
    """Types of a stage's flat columns and of its lists' elements."""
    def column_type(key: ValueKey) -> Optional[DataType]:
        col = schema.get(key.leaf) if isinstance(key, ColumnKey) and not key.path else None
        return col.type if col is not None else _stage_element(schema, key)
    return column_type


def stage_column_element(schema: StageSchema) -> ColumnElementFn:
    def column_element(key: ValueKey) -> Optional[DataType]:
        col = schema.get(key.leaf) if isinstance(key, ColumnKey) and not key.path else None
        return col.element_type if col is not None else None
    return column_element


def scope_column_element(*, scope: ModelScope | StageSchema, bundle) -> ColumnElementFn:
    if isinstance(scope, StageSchema):
        return stage_column_element(scope)
    if scope.source_model is None:
        return lambda _key: None
    return model_column_element(model=scope.source_model, models_by_name=bundle.models_by_name)


def key_element_type(key: ValueKey, *, scope: ModelScope | StageSchema, bundle) -> Optional[DataType]:
    """The element type of a list-valued ``key`` (``UNKNOWN`` when it cannot be told); ``None`` for a scalar."""
    column_type = scope_column_type(scope=scope, bundle=bundle)
    if not list_valued(key, column_type=column_type):
        return None
    element = list_element_type(
        key, column_type=column_type, column_element=scope_column_element(scope=scope, bundle=bundle),
    )
    return element or DataType.UNKNOWN


def scope_column_type(*, scope: ModelScope | StageSchema, bundle) -> ColumnTypeFn:
    if isinstance(scope, StageSchema):
        return stage_column_type(scope)
    if scope.source_model is None:
        return lambda _key: None
    return model_column_type(model=scope.source_model, models_by_name=bundle.models_by_name)


def bundle_dialect(bundle) -> SqlDialect:
    """The datasource's own dialect (``MariadbDialect`` for a MariaDB source), else the sqlglot name's."""
    own = dialect_for_ds_type(bundle.datasource_type)
    return own if own.sqlglot_name == bundle.dialect else get_dialect(bundle.dialect)


def scope_zoned_timestamp(*, scope: ModelScope | StageSchema, bundle) -> Callable[[object], bool]:
    """Whether a model column leaf holds a zoned instant on the bundle's dialect (a stage column already
    reads naive)."""
    dialect = bundle_dialect(bundle)
    if not dialect.zoned_timestamp_db_types or isinstance(scope, StageSchema) or scope.source_model is None:
        return lambda _key: False
    lookup = model_column_lookup(model=scope.source_model, models_by_name=bundle.models_by_name)
    return lambda key: dialect.is_zoned_db_type(getattr(lookup(key), "db_type", None))


def date_scalar_type(key: ValueKey, *, column_type: ColumnTypeFn) -> Optional[DataType]:
    """INT for ``date_part``/``date_diff``, else the key's DATE/TIMESTAMP type (date functions,
    temporal conditionals, min/max/first/last of a temporal value); ``None`` otherwise."""
    if isinstance(key, ScalarCallKey) and key.name in ("date_part", "date_diff"):
        return DataType.INT
    return temporal_type(key, column_type=column_type)


def _typed_by_value(key: ValueKey) -> bool:
    """A bare column or a value-returning conditional, typed by its ``value_type``."""
    return isinstance(key, (ColumnKey, ColumnSqlKey)) or (
        isinstance(key, ScalarCallKey) and key.name in CONDITIONAL_SCALARS)


def stage_measure_type(key: ValueKey, *, schema: StageSchema) -> Optional[DataType]:
    """``type`` for a measure over a stage's flat columns."""
    column_type = stage_column_type(schema)
    if list_valued(key, column_type=column_type):
        return DataType.ARRAY
    if _typed_by_value(key):
        return value_type(key, column_type=column_type)
    date_type = date_scalar_type(key, column_type=column_type)
    if date_type is not None or not isinstance(key, AggregateKey):
        return date_type
    return aggregation_result_type(
        measure_name="*" if isinstance(key.source, StarKey) else None, aggregation=key.agg,
        source_type=aggregation_source_type(key.source, column_type=column_type),
    )[0]


def measure_key_type(
    *, model: SlayerModel, key: ValueKey, bundle=None,
) -> Optional[DataType]:
    """``type`` for a measure slot, from its bound key alone."""
    if isinstance(key, CastKey):
        return key.data_type
    if list_valued(key, column_type=_model_types(model=model, bundle=bundle)):
        return DataType.ARRAY
    if _typed_by_value(key):
        return value_type(key, column_type=_model_types(model=model, bundle=bundle))
    date_type = date_scalar_type(key, column_type=_model_types(model=model, bundle=bundle))
    if date_type is not None:
        return date_type
    name = _local_aggregate_source_name(key)
    if name is None or not isinstance(key, AggregateKey):
        return None
    return aggregation_result_type(
        measure_name=name, aggregation=key.agg,
        source_type=aggregation_source_type(key.source, column_type=_model_types(model=model, bundle=bundle)),
    )[0]


def measure_key_preserves_native_type(*, model: SlayerModel, key: ValueKey) -> bool:
    """Whether an aggregate's inferred type would erase exact DB precision."""
    name = _local_aggregate_source_name(key)
    if name is None:
        return False
    col = model.get_column(name)
    if col is None:
        return False
    return is_exact_numeric_db_type(col.db_type)


def measure_key_format_description(
    *, model: SlayerModel, key: ValueKey, bundle=None,
) -> Tuple[Optional[NumberFormat], Optional[str]]:
    """``format`` / ``description`` for a measure slot, from its bound key.

    ``count(*)`` has an inferred INTEGER format but no description — there is
    no source column to document it.
    """
    name = _local_aggregate_source_name(key)
    if name is None or not isinstance(key, AggregateKey):
        return None, None
    fmt = _infer_aggregated_format(
        model=model, measure_name=name, aggregation=key.agg,
        source_type=aggregation_source_type(key.source, column_type=_model_types(model=model, bundle=bundle)),
    )
    if name == "*":
        return fmt, None
    col = model.get_column(name)
    return fmt, (col.description if col is not None else None)


def dimension_key_metadata(
    *, model: SlayerModel, key: ValueKey, bundle,
) -> Tuple[Optional[DataType], Optional[NumberFormat], Optional[str]]:
    """``(type, format, description)`` for a dimension slot, from its key.

    A LOCAL dimension carries the source column's full display contract; a
    JOINED one carries only its type. That asymmetry is deliberate and
    pre-existing: joined refs surface format / description through
    ``response_meta``, which resolves them against the owning model.
    """
    if isinstance(key, CastKey):
        return key.data_type, None, None
    inner = key.source if isinstance(key, TimeTruncKey) else key
    if not isinstance(inner, (ColumnKey, ColumnSqlKey)):
        date_type = date_scalar_type(key, column_type=_model_types(model=model, bundle=bundle))
        if date_type is not None:
            return date_type, None, None
    path = tuple(getattr(inner, "path", ()) or ())
    leaf = getattr(inner, "leaf", None) or getattr(inner, "column_name", None)
    if leaf is None:
        return None, None, None
    if not path:
        col = model.get_column(leaf)
        if col is None:
            return None, None, None
        return col.type, col.format, col.description
    terminal = walk_key_path(model=model, path=path, bundle=bundle)
    if terminal is None:
        return None, None, None
    col = terminal.get_column(leaf)
    return (col.type if col is not None else None), None, None
