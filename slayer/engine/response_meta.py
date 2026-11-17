"""Response metadata (``attributes`` + ``expected_columns``) from the typed plan.

Engine-import-free, so ``query_engine`` re-exports without a cycle.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field as PydanticField
from sqlglot.expressions.core import Expr
from sqlglot.expressions.query import Query

from slayer.core.enums import AggregationValueClass, DataType, classify_aggregation
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.relationship_walker import dataset_field_type, resolved_terminal
from slayer.core.keys import (
    AggregateKey,
    FieldKey,
    FieldSqlKey,
    Phase,
    StarKey,
    TimeTruncKey,
    aggregation_source_type,
)
from slayer.core.models import DatasetField, SlayerDataset
from slayer.core.refs import EXPRESSION_SOURCE_KINDS
from slayer.ir.planned import PlannedQuery, ValueSlot
from slayer.ir.source_bundle import ResolvedSourceBundle
from slayer.sql.naming import expression_source_leaf, next_slot_result_key


class FieldMetadata(BaseModel):
    label: Optional[str] = None
    format: Optional[NumberFormat] = None


class ResponseAttributes(BaseModel):
    dimensions: Dict[str, FieldMetadata] = PydanticField(default_factory=dict)
    metrics: Dict[str, FieldMetadata] = PydanticField(default_factory=dict)

    def get(self, column: str) -> Optional[FieldMetadata]:
        return self.dimensions.get(column) or self.metrics.get(column)


def _infer_aggregated_format(
    dataset: SlayerDataset,
    metric_name: str,
    aggregation: str,
    source_type: Optional[DataType] = None,
) -> Optional[NumberFormat]:
    """Display NumberFormat for an aggregated metric; ``source_type`` defaults to the field's type."""
    source_col = dataset.get_field(metric_name)
    source_type = source_type or (source_col.datatype if source_col is not None else None)
    cls = classify_aggregation(metric_name=metric_name, aggregation=aggregation, source_type=source_type)
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


def _dataset_for_path(
    *, bundle: ResolvedSourceBundle, path: Tuple[str, ...]
) -> Optional[SlayerDataset]:
    """The dataset a bound (possibly composed) relationship ``path`` lands on; the host source dataset
    for an empty or unresolvable path."""
    current = bundle.population
    if not path or current is None:
        return current
    return resolved_terminal(root=current, path=path, datasets_by_name=bundle.datasets_by_name) or current


def _field_for_row_slot(
    *, slot: ValueSlot, bundle: ResolvedSourceBundle
) -> Optional[DatasetField]:
    """The source ``DatasetField`` backing a ROW slot, for label / format lookup."""
    key = slot.key
    if isinstance(key, TimeTruncKey):
        key = key.source
    if isinstance(key, FieldKey):
        model = _dataset_for_path(bundle=bundle, path=key.path)
        leaf = key.leaf
    elif isinstance(key, FieldSqlKey):
        model = bundle.get_referenced_dataset(key.dataset) or bundle.population
        leaf = key.field_name
    else:
        return None
    if model is None:
        return None
    return model.get_field(leaf)


def _owning_dataset_for_agg_source(*, src, bundle: ResolvedSourceBundle):
    """The dataset that owns an aggregate's source field."""
    if isinstance(src, FieldSqlKey):
        return bundle.get_referenced_dataset(src.dataset) or bundle.population
    return _dataset_for_path(bundle=bundle, path=getattr(src, "path", ()))


def _metric_format(
    *, slot: ValueSlot, bundle: ResolvedSourceBundle
) -> Optional[NumberFormat]:
    """Number format for a metric slot; non-aggregate slots default to FLOAT."""
    key = slot.key
    if isinstance(key, AggregateKey):
        src = key.source
        if isinstance(src, StarKey):
            measure_name: Optional[str] = "*"
        elif isinstance(src, EXPRESSION_SOURCE_KINDS):
            # An expression source classifies by its aggregation's
            # value class alone (the derived leaf is never a real field, so
            # PRESERVING inherits nothing — plain numeric by default).
            measure_name = expression_source_leaf(src)
        else:
            measure_name = getattr(src, "leaf", None) or getattr(
                src, "field_name", None
            )
        model = _owning_dataset_for_agg_source(src=src, bundle=bundle)
        if measure_name is None or model is None:
            return NumberFormat(type=NumberFormatType.FLOAT)
        root = bundle.population
        column_type = dataset_field_type(dataset=root, datasets_by_name=bundle.datasets_by_name) if root else None
        return _infer_aggregated_format(
            dataset=model, metric_name=measure_name, aggregation=key.agg,
            source_type=aggregation_source_type(src, field_type=column_type) if column_type else None,
        )
    return NumberFormat(type=NumberFormatType.FLOAT)


def _metric_label(
    *, slot: ValueSlot, bundle: ResolvedSourceBundle
) -> Optional[str]:
    """Label for a metric slot; inherits the source field's label when the slot has none."""
    if slot.label:
        return slot.label
    key = slot.key
    if isinstance(key, AggregateKey):
        src = key.source
        if isinstance(src, (FieldKey, FieldSqlKey)):
            model = _owning_dataset_for_agg_source(src=src, bundle=bundle)
            leaf = getattr(src, "leaf", None) or getattr(
                src, "field_name", None,
            )
            if model is not None and leaf is not None:
                col = model.get_field(leaf)
                if col is not None:
                    return col.label
    return None


def build_response_metadata(  # NOSONAR(S3776) — flat per-slot metadata classification (dimension vs metric, TimeTruncKey, label/format lookup) over one candidate-slot loop; complexity is inherent to the projection-to-metadata mapping and pre-dates this change. Splitting the loop body out would scatter the shared public_keys / source_relation state without improving readability.
    *,
    root_planned: PlannedQuery,
    bundle: ResolvedSourceBundle,
    statement: Expr,
) -> Tuple[ResponseAttributes, List[str]]:
    """Build ``(attributes, expected_columns)``; only keys in ``statement``'s projection surface.

    The statement's outer names are canonical: fitting and mangling happen at its render.
    """
    assert isinstance(statement, Query)  # a generated statement is always a query
    slot_keys = _projected_slot_keys(root_planned=root_planned)
    expected_columns = list(statement.named_selects)
    public_keys = set(expected_columns)

    dim_meta: Dict[str, FieldMetadata] = {}
    metric_meta: Dict[str, FieldMetadata] = {}

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
        # A combined regroup attach is a ROW-phase placeholder but is a metric.
        original = placeholder_original.get(slot.key)
        is_combined_placeholder = original is not None
        if slot.phase == Phase.ROW and not is_combined_placeholder:
            col = _field_for_row_slot(slot=slot, bundle=bundle)
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
            fmt = _metric_format(slot=measure_slot, bundle=bundle)
            label = _metric_label(slot=measure_slot, bundle=bundle)
            if label or fmt:
                metric_meta[rk] = FieldMetadata(label=label, format=fmt)

    return ResponseAttributes(dimensions=dim_meta, metrics=metric_meta), expected_columns
