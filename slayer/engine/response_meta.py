"""DEV-1450 stage 7b.15d — response metadata from the typed plan.

This module builds ``SlayerResponse.attributes`` and ``expected_columns``
from the root ``PlannedQuery`` plus the final rendered SQL.

* ``expected_columns`` comes from the final SQL's ``named_selects`` — the
  literal result-key columns the rows come back keyed by. Deriving them from
  the SQL (rather than re-walking slots) is bulletproof: it is exactly the
  outer SELECT projection the generator emitted.
* ``attributes`` (``ResponseAttributes.dimensions`` / ``.measures``) come from
  the root ``PlannedQuery``'s public ``ValueSlot``s, mirroring the
  ``_full_alias_for_slot`` result-key derivation in ``slayer/sql/generator.py``
  so the keys line up with the rendered projection.

``FieldMetadata`` / ``ResponseAttributes`` / ``_infer_aggregated_format`` live
here (not in ``query_engine``) so this module imports nothing from the engine —
``query_engine`` re-exports them, keeping the dependency one-directional and
the public import path (``from slayer.engine.query_engine import FieldMetadata``)
unchanged.
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
from slayer.engine.planned import PlannedQuery, ValueSlot
from slayer.engine.source_bundle import ResolvedSourceBundle
from slayer.sql.dialects import get_dialect
from slayer.sql.naming import result_key, result_key_from_alias


# ---------------------------------------------------------------------------
# Response metadata types (moved here from query_engine for import hygiene).
# ---------------------------------------------------------------------------


class FieldMetadata(BaseModel):
    """Metadata for a single field in the query response."""

    label: Optional[str] = None
    format: Optional[NumberFormat] = None


class ResponseAttributes(BaseModel):
    """Field metadata for a query response, split by type."""

    dimensions: Dict[str, FieldMetadata] = PydanticField(default_factory=dict)
    measures: Dict[str, FieldMetadata] = PydanticField(default_factory=dict)

    def get(self, column: str) -> Optional[FieldMetadata]:
        """Look up metadata for a column across both dicts."""
        return self.dimensions.get(column) or self.measures.get(column)


def _infer_aggregated_format(
    model: SlayerModel,
    measure_name: str,
    aggregation: str,
) -> Optional[NumberFormat]:
    """Infer the display NumberFormat for an aggregated measure via the shared
    ``classify_aggregation`` (DEV-1788), so it cannot drift from
    ``aggregated_type``:

    - COUNT (``*:count`` / count-family): INTEGER
    - FLOAT_PLAIN (corr / var / covar): plain FLOAT
    - FLOAT_SOURCE_UNITS (avg-family / percentile / stddev): inherit source
      format, else FLOAT (the result is fractional even absent source units)
    - PRESERVING (sum / min / max / first / last, and custom aggs): inherit
      source format, else None
    """
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


# ---------------------------------------------------------------------------
# expected_columns / attributes from the typed plan
# ---------------------------------------------------------------------------


def expected_columns_from_sql(*, sql: str, dialect: str) -> List[str]:
    """The outer SELECT's result-key columns, read from the rendered SQL.

    ``named_selects`` returns each projected column's alias (``orders.status``,
    ``orders.revenue_sum``, ...) — the exact keys execution returns rows under.
    """
    parsed = sqlglot.parse_one(sql, dialect=dialect)
    return list(parsed.named_selects)


def projection_result_keys(*, root_planned: PlannedQuery) -> List[str]:
    """Canonical result keys for the projected, non-hidden slots.

    Plan-derived, so independent of what the emitted SQL carries — DEV-1756
    length-fitting and dialect alias-mangling both change the emitted alias but
    not this. The read side re-runs the pure fit over these to rebuild the
    ``emitted -> canonical`` map, restoring keys that length-fitting makes
    unrecoverable from the emitted form alone.
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
    """The model a dotted join ``path`` lands on (best-effort).

    Empty path → the host source model. Otherwise the last path segment is
    the join target model name; resolve it from the bundle's referenced
    models, falling back to the host when absent.
    """
    if not path:
        return bundle.source_model
    return bundle.get_referenced_model(path[-1]) or bundle.source_model


def _slot_result_keys(*, slot: ValueSlot, source_relation: str) -> List[str]:
    """The public result-key alias(es) for ``slot``.

    Mirrors ``SQLGenerator._full_alias_for_slot`` via the SAME naming builders
    (``slayer.sql.naming.result_key`` / ``result_key_from_alias``) so the SQL
    alias and the response result key cannot drift. Joined ROW slots — base
    ``ColumnKey``, derived ``ColumnSqlKey`` (DEV-1713 D3 / DEV-1495 bug 1), and
    ``TimeTruncKey`` over either — emit the full dotted path
    (``orders.customers.region``); everything else uses the slot's public
    alias(es) — multiple for a C13 multi-name interned slot.
    """
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
    """The model that owns an aggregate's source column.

    A ``ColumnSqlKey`` (derived column) carries its owning model name in
    ``src.model`` — resolve through that (DEV-1450 #4a/#4b), mirroring
    ``_column_for_row_slot``. A ``ColumnKey`` / ``StarKey`` is resolved by
    walking ``src.path`` from the host.
    """
    if isinstance(src, ColumnSqlKey):
        return bundle.get_referenced_model(src.model) or bundle.source_model
    return _model_for_path(bundle=bundle, path=getattr(src, "path", ()))


def _measure_format(
    *, slot: ValueSlot, bundle: ResolvedSourceBundle
) -> Optional[NumberFormat]:
    """Number format for a measure slot.

    Aggregate slots inherit via ``_infer_aggregated_format`` (INTEGER for
    count(-distinct) / star, plain FLOAT for corr / var / covar, source format
    for the avg-family / percentile / stddev and for sum / min / max).
    Transform / arithmetic / scalar-call slots default to FLOAT.
    """
    key = slot.key
    if isinstance(key, AggregateKey):
        src = key.source
        if isinstance(src, StarKey):
            measure_name: Optional[str] = "*"
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
    """Label for a measure slot.

    A query measure (``labeled_rev:sum``) inherits its source column's label
    when the measure spec carried none — mirroring the legacy enrichment that
    propagated ``Column.label`` onto the aggregated field. Star aggregates and
    transform / arithmetic slots have no single source column, so they fall
    back to the slot's own label (usually ``None``).
    """
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
    """Build ``(attributes, expected_columns)`` for one executed query.

    ``expected_columns`` is read from the rendered SQL (bulletproof);
    ``attributes`` maps each public result key to its ``FieldMetadata``,
    classified dimension (ROW-phase slots) vs measure (everything else).
    Only keys that actually appear in the rendered projection are surfaced —
    a guard against any divergence between this derivation and the generator.
    """
    source_relation = root_planned.source_relation
    projection_ids = set(root_planned.projection)
    candidate_slots = (
        list(root_planned.row_slots)
        + list(root_planned.aggregate_slots)
        + list(root_planned.combined_expression_slots)
    )
    # DEV-1756: the canonical projection result keys, derived from the plan
    # (independent of the emitted SQL, which may carry length-fitted and/or
    # alias-mangled names). The read side rebuilds the ``emitted -> canonical``
    # map by re-running the pure fit over these.
    plan_aliases = projection_result_keys(root_planned=root_planned)

    expected_columns = expected_columns_from_sql(sql=sql, dialect=dialect)
    # DEV-1716 / DEV-1756: the rendered SQL carries emitted projection names —
    # alias-mangled (``orders___status``) and/or length-fitted; decode them back
    # to the canonical dotted form so ``expected_columns`` and the attribute
    # matching below operate in the same space as the plan's slot result keys.
    if expected_columns:
        expected_columns = list(
            get_dialect(dialect).decode_result_keys(
                [dict.fromkeys(expected_columns)], aliases=plan_aliases,
            )[0]
        )
    public_keys = set(expected_columns)

    dim_meta: Dict[str, FieldMetadata] = {}
    measure_meta: Dict[str, FieldMetadata] = {}

    # DEV-1836 — a combined regroup attach substitutes each consumed aggregate
    # for a reserved-leaf placeholder; map it back to the ORIGINAL aggregate key
    # so its measure label/format resolve against the aggregated column.
    placeholder_original: Dict[Any, Any] = {
        sub.placeholder: sub.original_key
        for attach in root_planned.regroup_attach_plans
        if attach.attach_phase == "combined"
        for sub in attach.substitutions
    }

    for slot in candidate_slots:
        if slot.hidden or slot.id not in projection_ids:
            continue
        # A combined regroup attach (a partitioned or cross-model MEASURE) is
        # substituted to a reserved-leaf ``ColumnKey`` placeholder, which is
        # ROW-phase — but it is a measure, not a dimension (DEV-1836).
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
                # Label falls back to the model Column's label when the query
                # ColumnRef carried none (legacy ``dim_ref.label or
                # dim_def.label``).
                col = _column_for_row_slot(slot=slot, bundle=bundle)
                label = slot.label or (col.label if col else None)
                if isinstance(slot.key, TimeTruncKey):
                    # Time dimensions carry a label only (legacy parity).
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
