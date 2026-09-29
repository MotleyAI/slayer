"""Key → slot-metadata lifts (``type`` / ``format`` / ``description``).

Shared by the compiler and the re-rooting strategy; the seam types they
decorate live in ``slayer.ir.prebound``.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Optional, Tuple, TypeGuard, Union

from slayer.core.enums import (
    AggregationValueClass,
    DataType,
    classify_aggregation,
)
from slayer.core.format import NumberFormat
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnTypeFn,
    ColumnSqlKey,
    LiteralKey,
    ScalarCallKey,
    StarKey,
    TimeTruncKey,
    ValueKey,
    join_conditional_branch_types,
    temporal_type,
)
from slayer.core.join_walker import model_column_type
from slayer.core.models import SlayerModel
from slayer.core.scope import ModelScope, StageSchema
from slayer.core.refs import EXPRESSION_SOURCE_KINDS, expression_source_leaf
from slayer.engine.introspect_utils import is_exact_numeric_db_type
from slayer.engine.response_meta import _infer_aggregated_format
from slayer.ir.prebound import walk_key_path

__all__ = [
    "aggregated_type",
    "dimension_key_metadata",
    "measure_key_format_description",
    "measure_key_preserves_native_type",
    "measure_key_type",
    "scope_column_type",
    "stage_measure_type",
]


# ---------------------------------------------------------------------------
# Key -> slot metadata
# ---------------------------------------------------------------------------

def aggregated_type(
    *,
    model: SlayerModel,
    measure_name: Optional[str],
    aggregation: str,
) -> Optional[DataType]:
    """Type for an aggregated measure slot, via the shared
    ``classify_aggregation`` (DEV-1788), so it cannot drift from
    ``_infer_aggregated_format``:

    * ``COUNT`` (``count(*)`` / count-family) → ``INT``
    * ``FLOAT_SOURCE_UNITS`` / ``FLOAT_PLAIN`` (avg-family, stat, parametric) →
      ``DOUBLE``
    * ``PRESERVING`` (sum / min / max / first / last, and custom aggs) → inherit
      source column type (``None`` if absent).
    """
    cls = classify_aggregation(measure_name=measure_name, aggregation=aggregation)
    if cls is AggregationValueClass.COUNT:
        return DataType.INT
    if cls in (
        AggregationValueClass.FLOAT_SOURCE_UNITS,
        AggregationValueClass.FLOAT_PLAIN,
    ):
        return DataType.DOUBLE
    # PRESERVING — inherit source column type.
    if measure_name is None:
        return None
    col = model.get_column(measure_name)
    if col is not None and col.type is not None:
        return col.type
    return None


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


def _literal_data_type(value) -> Optional[DataType]:
    """The SLayer type of a scalar literal (``None`` for a NULL literal)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return DataType.BOOLEAN
    if isinstance(value, Decimal):
        return DataType.INT if value == value.to_integral_value() else DataType.DOUBLE
    if isinstance(value, str):
        return DataType.TEXT
    return None


def _is_iif(key: object) -> TypeGuard[ScalarCallKey]:
    return isinstance(key, ScalarCallKey) and key.name == "iif"


def _branch_type(*, model: SlayerModel, key) -> Optional[DataType]:
    """The type of one CASE/iif branch, from the model alone (``None`` when it
    cannot be determined — a joined column, an arithmetic expression — so it is
    treated as NULL-absorbing rather than an incompatibility)."""
    if key is None or isinstance(key, (Decimal, str, bool)):
        return _literal_data_type(key)
    if isinstance(key, LiteralKey):
        return _literal_data_type(key.value)
    if isinstance(key, ColumnKey) and not key.path:
        col = model.get_column(key.leaf)
        return col.type if col is not None else None
    return measure_key_type(model=model, key=key)


def _model_types(*, model: SlayerModel, bundle) -> ColumnTypeFn:
    return model_column_type(
        model=model, models_by_name=bundle.models_by_name if bundle is not None else {},
    )


def stage_column_type(schema: StageSchema) -> ColumnTypeFn:
    """Types of a stage's flat columns."""
    def column_type(key: ValueKey) -> Optional[DataType]:
        col = schema.get(key.leaf) if isinstance(key, ColumnKey) and not key.path else None
        return col.type if col is not None else None
    return column_type


def scope_column_type(*, scope: Union[ModelScope, StageSchema], bundle) -> ColumnTypeFn:
    if isinstance(scope, StageSchema):
        return stage_column_type(scope)
    if scope.source_model is None:
        return lambda _key: None
    return model_column_type(model=scope.source_model, models_by_name=bundle.models_by_name)


def date_scalar_type(key: ValueKey, *, column_type: ColumnTypeFn) -> Optional[DataType]:
    """INT for ``date_part``/``date_diff``, else the key's DATE/TIMESTAMP type (date functions,
    temporal conditionals, min/max/first/last of a temporal value); ``None`` otherwise."""
    if isinstance(key, ScalarCallKey) and key.name in ("date_part", "date_diff"):
        return DataType.INT
    return temporal_type(key, column_type=column_type)


def stage_measure_type(key: ValueKey, *, schema: StageSchema) -> Optional[DataType]:
    """``type`` for a measure over a stage's flat columns."""
    column_type = stage_column_type(schema)
    date_type = date_scalar_type(key, column_type=column_type)
    if date_type is not None or not isinstance(key, AggregateKey):
        return date_type
    source = key.source
    cls = classify_aggregation(
        measure_name="*" if isinstance(source, StarKey) else None, aggregation=key.agg,
    )
    if cls is AggregationValueClass.COUNT:
        return DataType.INT
    if cls is not AggregationValueClass.PRESERVING:
        return DataType.DOUBLE
    return column_type(source)


def measure_key_type(
    *, model: SlayerModel, key: ValueKey, bundle=None,
) -> Optional[DataType]:
    """``type`` for a measure slot, from its bound key alone."""
    date_type = date_scalar_type(key, column_type=_model_types(model=model, bundle=bundle))
    if date_type is not None:
        return date_type
    if _is_iif(key):
        # Postgres branch typing: the join over every THEN branch and the final
        # ELSE. Raises on an incompatible mix (DEV-1740).
        result: Optional[DataType] = None
        node = key
        branches = []
        while _is_iif(node):
            branches.append(node.args[1])
            node = node.args[2]
        branches.append(node)
        for branch in branches:
            result = join_conditional_branch_types(
                result, _branch_type(model=model, key=branch),
            )
        return result
    name = _local_aggregate_source_name(key)
    if name is None or not isinstance(key, AggregateKey):
        return None
    return aggregated_type(
        model=model, measure_name=name, aggregation=key.agg,
    )


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
    *, model: SlayerModel, key: ValueKey,
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
    if not isinstance(key, (ColumnKey, ColumnSqlKey, TimeTruncKey)):
        date_type = date_scalar_type(key, column_type=_model_types(model=model, bundle=bundle))
        if date_type is not None:
            return date_type, None, None
    inner = key.column if isinstance(key, TimeTruncKey) else key
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
