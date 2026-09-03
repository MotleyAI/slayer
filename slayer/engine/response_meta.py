"""Response metadata (``attributes`` + ``expected_columns``) from the typed plan.

Engine-import-free (so ``query_engine`` re-exports without a cycle); result keys
mirror ``_full_alias_for_slot`` in ``slayer/sql/generator.py``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from pydantic import BaseModel, Field as PydanticField

from slayer.core.enums import AggregationValueClass, classify_aggregation
from slayer.core.format import NumberFormat, NumberFormatType
from slayer.core.keys import (
    AggregateKey,
    ColumnKey,
    ColumnSqlKey,
    Phase,
    StarKey,
    TimeTruncKey,
    column_leaf,
    column_path,
)
from slayer.core.models import Column, SlayerModel
from slayer.core.refs import EXPRESSION_SOURCE_KINDS, expression_source_leaf
from slayer.engine.planned import PlannedQuery, ValueSlot
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import get_dialect
from slayer.sql.naming import result_key, result_key_from_alias


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
) -> Optional[NumberFormat]:
    """Display NumberFormat for an aggregated measure, via ``classify_aggregation``."""
    cls = classify_aggregation(measure_name=measure_name, aggregation=aggregation)
    if cls is AggregationValueClass.COUNT:
        return NumberFormat(type=NumberFormatType.INTEGER)
    if cls is AggregationValueClass.FLOAT_PLAIN:
        return NumberFormat(type=NumberFormatType.FLOAT)

    source_col = model.get_column(measure_name)
    if source_col and source_col.format:
        return source_col.format
    if cls is AggregationValueClass.FLOAT_SOURCE_UNITS:
        return NumberFormat(type=NumberFormatType.FLOAT)
    return None


def expected_columns_from_sql(*, sql: str, dialect: str) -> List[str]:
    """The outer SELECT's result-key columns (aliases), read from the rendered SQL."""
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    return list(parsed.named_selects)


def projection_result_keys(*, root_planned: PlannedQuery) -> List[str]:
    """Canonical result keys for the projected, non-hidden slots.

    Plan-derived, so independent of the emitted SQL (length-fitting / alias-mangling).
    """
    source_relation = root_planned.source_relation
    projection_ids = set(root_planned.projection)
    return [
        rk
        for slot in (
            list(root_planned.row_slots)
            + list(root_planned.aggregate_slots)
            + list(root_planned.combined_expression_slots)
        )
        if not slot.hidden and slot.id in projection_ids
        for rk in _slot_result_keys(slot=slot, source_relation=source_relation)
    ]


def _model_for_path(
    *, bundle: ResolvedSourceBundle, path: Tuple[str, ...]
) -> Optional[SlayerModel]:
    """The model a dotted join ``path`` lands on; empty path → host source model."""
    if not path:
        return bundle.source_model
    return bundle.get_referenced_model(path[-1]) or bundle.source_model


def _slot_result_keys(*, slot: ValueSlot, source_relation: str) -> List[str]:
    """Public result-key alias(es) for ``slot``; joined ROW slots emit the full dotted path."""
    key = slot.key
    if slot.phase == Phase.ROW:
        if isinstance(key, ColumnKey) and key.path:
            return [result_key(
                source_relation=source_relation, path=key.path, leaf=key.leaf,
            )]
        if isinstance(key, ColumnSqlKey) and key.path:
            return [result_key(
                source_relation=source_relation,
                path=key.path,
                leaf=key.column_name,
            )]
        if isinstance(key, TimeTruncKey) and column_path(key.column):
            return [result_key(
                source_relation=source_relation,
                path=column_path(key.column),
                leaf=column_leaf(key.column),
            )]
    aliases = slot.public_aliases or [slot.declared_name]
    return [
        result_key_from_alias(source_relation=source_relation, alias=a)
        for a in aliases
    ]


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
            # DEV-1826: an expression source classifies by its aggregation's
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
        return _infer_aggregated_format(
            model=model, measure_name=measure_name, aggregation=key.agg
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
    source_relation = root_planned.source_relation
    projection_ids = set(root_planned.projection)
    candidate_slots = (
        list(root_planned.row_slots)
        + list(root_planned.aggregate_slots)
        + list(root_planned.combined_expression_slots)
    )
    # Canonical projection keys from the plan; the emitted SQL may carry
    # length-fitted / alias-mangled names.
    plan_aliases = projection_result_keys(root_planned=root_planned)

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

    for slot in candidate_slots:
        if slot.hidden or slot.id not in projection_ids:
            continue
        # A combined regroup attach is a ROW-phase placeholder but is a measure.
        original = placeholder_original.get(slot.key)
        is_combined_placeholder = original is not None
        measure_slot = (
            slot.model_copy(update={"key": original})
            if is_combined_placeholder else slot
        )
        is_dim = slot.phase == Phase.ROW and not is_combined_placeholder
        for rk in _slot_result_keys(slot=slot, source_relation=source_relation):
            if rk not in public_keys:
                continue
            if is_dim:
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
                fmt = _measure_format(slot=measure_slot, bundle=bundle)
                label = _measure_label(slot=measure_slot, bundle=bundle)
                if label or fmt:
                    measure_meta[rk] = FieldMetadata(label=label, format=fmt)

    return ResponseAttributes(dimensions=dim_meta, measures=measure_meta), expected_columns
