"""Response metadata (``attributes`` + ``expected_columns``) from the typed plan.

Engine-import-free, so ``query_engine`` re-exports without a cycle.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from pydantic import BaseModel, Field as PydanticField

from slayer.core.enums import AggregationValueClass, DataType, classify_aggregation
from slayer.core.errors import AmbiguousJoinPathError
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.join_walker import model_column_type, resolve_hop
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    Phase,
    StarKey,
    TimeTruncKey,
    aggregation_source_type,
)
from slayer.core.models import Column, SlayerModel
from slayer.core.refs import EXPRESSION_SOURCE_KINDS, expression_source_leaf
from slayer.ir.planned import PlannedQuery, ValueSlot
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import get_dialect
from slayer.sql.naming import next_slot_result_key


class FieldMetadata(BaseModel):
    label: Optional[str] = None
    format: Optional[NumberFormat] = None


class ResponseAttributes(BaseModel):
    dimensions: Dict[str, FieldMetadata] = PydanticField(default_factory=dict)
    measures: Dict[str, FieldMetadata] = PydanticField(default_factory=dict)

    def get(self, column: str) -> Optional[FieldMetadata]:
        return self.dimensions.get(column) or self.measures.get(column)


def _infer_aggregated_format(
    model: SlayerModel,
    measure_name: str,
    aggregation: str,
    source_type: Optional[DataType] = None,
) -> Optional[NumberFormat]:
    """Display NumberFormat for an aggregated measure; ``source_type`` defaults to the column's type."""
    source_col = model.get_column(measure_name)
    source_type = source_type or (source_col.type if source_col is not None else None)
    cls = classify_aggregation(measure_name=measure_name, aggregation=aggregation, source_type=source_type)
    if cls is AggregationValueClass.COUNT:
        return NumberFormat(type=NumberFormatType.INTEGER)
    if cls is AggregationValueClass.FLOAT_PLAIN:
        return NumberFormat(type=NumberFormatType.FLOAT)
    if source_col and source_col.format:
        return source_col.format
    if cls is AggregationValueClass.FLOAT_SOURCE_UNITS:
        boolean_share = source_type is DataType.BOOLEAN and aggregation == "avg"
        return NumberFormat(type=NumberFormatType.PERCENT if boolean_share else NumberFormatType.FLOAT)
    return None


def expected_columns_from_sql(*, sql: str, dialect: str) -> List[str]:
    """The outer SELECT's result-key columns (aliases), read from the rendered SQL."""
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    return list(parsed.named_selects)


def _projected_slot_keys(*, root_planned: PlannedQuery) -> List[Tuple[ValueSlot, str]]:
    """``(slot, result key)`` per non-hidden projection occurrence, in projection order."""
    slots_by_id = {
        s.id: s
        for s in (
            list(root_planned.row_slots)
            + list(root_planned.aggregate_slots)
            + list(root_planned.combined_expression_slots)
        )
    }
    alias_index: Dict[str, int] = {}
    out: List[Tuple[ValueSlot, str]] = []
    for sid in root_planned.projection:
        slot = slots_by_id.get(sid)
        if slot is None or slot.hidden:
            continue
        out.append((slot, next_slot_result_key(
            slot=slot, alias_index=alias_index, source_relation=root_planned.source_relation,
        )))
    return out


def projection_result_keys(*, root_planned: PlannedQuery) -> List[str]:
    """Canonical result keys of the projection, in order; plan-derived, so independent of identifier fitting."""
    return [rk for _, rk in _projected_slot_keys(root_planned=root_planned)]


def _model_for_path(
    *, bundle: ResolvedSourceBundle, path: Tuple[str, ...]
) -> Optional[SlayerModel]:
    """The model a dotted join ``path`` lands on; empty path → host source model.

    Resolves each token through the shared bidirectional walker so reverse hops
    and edge-name tokens land on the right terminal model, never by reading the
    last token as a model name."""
    current = bundle.source_model
    if not path or current is None:
        return current
    models_by_name = bundle.models_by_name
    models_by_name.setdefault(current.name, current)
    for hop in path:
        try:
            edge = resolve_hop(
                current=current, token=hop, models_by_name=models_by_name,
            )
        except AmbiguousJoinPathError:
            return bundle.source_model
        nxt = models_by_name.get(edge.target_model) if edge is not None else None
        if nxt is None:
            return bundle.source_model
        current = nxt
    return current


def _column_for_row_slot(
    *, slot: ValueSlot, bundle: ResolvedSourceBundle
) -> Optional[Column]:
    """The source ``Column`` backing a ROW slot, for label / format lookup."""
    key = slot.key
    if isinstance(key, TimeTruncKey):
        key = key.column
    if isinstance(key, ColumnKey):
        model = _model_for_path(bundle=bundle, path=key.path)
        leaf = key.leaf
    elif isinstance(key, ColumnSqlKey):
        model = bundle.get_referenced_model(key.model) or bundle.source_model
        leaf = key.column_name
    else:
        return None
    if model is None:
        return None
    return model.get_column(leaf)


def _owning_model_for_agg_source(*, src, bundle: ResolvedSourceBundle):
    """The model that owns an aggregate's source column."""
    if isinstance(src, ColumnSqlKey):
        return bundle.get_referenced_model(src.model) or bundle.source_model
    return _model_for_path(bundle=bundle, path=getattr(src, "path", ()))


def _measure_format(
    *, slot: ValueSlot, bundle: ResolvedSourceBundle
) -> Optional[NumberFormat]:
    """Number format for a measure slot; non-aggregate slots default to FLOAT."""
    key = slot.key
    if isinstance(key, AggregateKey):
        src = key.source
        if isinstance(src, StarKey):
            measure_name: Optional[str] = "*"
        elif isinstance(src, EXPRESSION_SOURCE_KINDS):
            # An expression source classifies by its aggregation's
            # value class alone (the derived leaf is never a real column, so
            # PRESERVING inherits nothing — plain numeric by default).
            measure_name = expression_source_leaf(src)
        else:
            measure_name = getattr(src, "leaf", None) or getattr(
                src, "column_name", None
            )
        model = _owning_model_for_agg_source(src=src, bundle=bundle)
        if measure_name is None or model is None:
            return NumberFormat(type=NumberFormatType.FLOAT)
        root = bundle.source_model
        column_type = model_column_type(model=root, models_by_name=bundle.models_by_name) if root else None
        return _infer_aggregated_format(
            model=model, measure_name=measure_name, aggregation=key.agg,
            source_type=aggregation_source_type(src, column_type=column_type) if column_type else None,
        )
    return NumberFormat(type=NumberFormatType.FLOAT)


def _measure_label(
    *, slot: ValueSlot, bundle: ResolvedSourceBundle
) -> Optional[str]:
    """Label for a measure slot; inherits the source column's label when the slot has none."""
    if slot.label:
        return slot.label
    key = slot.key
    if isinstance(key, AggregateKey):
        src = key.source
        if isinstance(src, (ColumnKey, ColumnSqlKey)):
            model = _owning_model_for_agg_source(src=src, bundle=bundle)
            leaf = getattr(src, "leaf", None) or getattr(
                src, "column_name", None,
            )
            if model is not None and leaf is not None:
                col = model.get_column(leaf)
                if col is not None:
                    return col.label
    return None


def build_response_metadata(  # NOSONAR(S3776) — flat per-slot metadata classification (dimension vs measure, TimeTruncKey, label/format lookup) over one candidate-slot loop; complexity is inherent to the projection-to-metadata mapping and pre-dates this change. Splitting the loop body out would scatter the shared public_keys / source_relation state without improving readability.
    *,
    root_planned: PlannedQuery,
    bundle: ResolvedSourceBundle,
    sql: str,
    dialect: str,
) -> Tuple[ResponseAttributes, List[str]]:
    """Build ``(attributes, expected_columns)``; only keys in the rendered projection surface."""
    # Canonical projection keys from the plan; the emitted SQL may carry
    # length-fitted / alias-mangled names.
    slot_keys = _projected_slot_keys(root_planned=root_planned)
    plan_aliases = [rk for _, rk in slot_keys]

    expected_columns = expected_columns_from_sql(sql=sql, dialect=dialect)
    # Decode emitted projection names back to canonical dotted form so matching
    # below operates in the plan's result-key space.
    if expected_columns:
        expected_columns = list(
            get_dialect(dialect).decode_result_keys(
                [dict.fromkeys(expected_columns)], aliases=plan_aliases,
            )[0]
        )
    public_keys = set(expected_columns)

    dim_meta: Dict[str, FieldMetadata] = {}
    measure_meta: Dict[str, FieldMetadata] = {}

    # A combined regroup attach substitutes each consumed aggregate for a
    # reserved-leaf placeholder; map it back so its label/format resolve.
    placeholder_original: Dict[Any, Any] = {
        sub.placeholder: sub.original_key
        for attach in root_planned.regroup_attach_plans
        if attach.attach_phase == "combined"
        for sub in attach.substitutions
    }

    for slot, rk in slot_keys:
        if rk not in public_keys:
            continue
        # A combined regroup attach is a ROW-phase placeholder but is a measure.
        original = placeholder_original.get(slot.key)
        is_combined_placeholder = original is not None
        if slot.phase == Phase.ROW and not is_combined_placeholder:
            col = _column_for_row_slot(slot=slot, bundle=bundle)
            label = slot.label or (col.label if col else None)
            if isinstance(slot.key, TimeTruncKey):
                # Time dimensions carry a label only.
                if label:
                    dim_meta[rk] = FieldMetadata(label=label)
                continue
            fmt = col.format if col else None
            if label or fmt:
                dim_meta[rk] = FieldMetadata(label=label, format=fmt)
        else:
            measure_slot = (
                slot.model_copy(update={"key": original})
                if is_combined_placeholder else slot
            )
            fmt = _measure_format(slot=measure_slot, bundle=bundle)
            label = _measure_label(slot=measure_slot, bundle=bundle)
            if label or fmt:
                measure_meta[rk] = FieldMetadata(label=label, format=fmt)

    return ResponseAttributes(dimensions=dim_meta, measures=measure_meta), expected_columns
