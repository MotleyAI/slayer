"""DEV-1450 stage 7b.15d — response metadata from the typed plan.

The legacy engine derived ``SlayerResponse.attributes`` and
``expected_columns`` from an ``EnrichedQuery``. The typed pipeline has no
``EnrichedQuery``; this module rebuilds the same two artefacts from the root
``PlannedQuery`` plus the final rendered SQL.

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

from typing import Dict, List, Optional, Tuple

import sqlglot
from pydantic import BaseModel, Field as PydanticField

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
    """Infer NumberFormat for an aggregated measure based on aggregation type and source measure format.

    Rules:
    - count, count_distinct: always INTEGER
    - avg, weighted_avg, median: always FLOAT
    - sum, min, max, first, last: inherit from source measure
    - *:count (measure_name="*"): INTEGER
    """
    if measure_name == "*":
        return NumberFormat(type=NumberFormatType.INTEGER)

    if aggregation in ("count", "count_distinct"):
        return NumberFormat(type=NumberFormatType.INTEGER)

    if aggregation in ("avg", "weighted_avg", "median"):
        return NumberFormat(type=NumberFormatType.FLOAT)

    # sum, min, max, first, last: inherit from source column's format
    source_col = model.get_column(measure_name)
    if source_col and source_col.format:
        return source_col.format

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

    Mirrors ``SQLGenerator._full_alias_for_slot``: joined ROW slots emit the
    full dotted path (``orders.customers.region``); everything else uses the
    slot's public alias(es) — multiple for a C13 multi-name interned slot —
    prefixed by the stage's source relation.
    """
    key = slot.key
    if slot.phase == Phase.ROW:
        if isinstance(key, ColumnKey) and key.path:
            return [f"{source_relation}." + ".".join(key.path) + f".{key.leaf}"]
        if isinstance(key, TimeTruncKey) and column_path(key.column):
            return [
                f"{source_relation}."
                + ".".join(column_path(key.column))
                + f".{column_leaf(key.column)}"
            ]
    aliases = slot.public_aliases or [slot.declared_name]
    return [f"{source_relation}.{a}" for a in aliases]


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
    count(-distinct) / star, FLOAT for avg-family, source-column format for
    sum/min/max). Transform / arithmetic / scalar-call slots default to FLOAT,
    matching the legacy ``EnrichedQuery`` expression/transform handling.
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


def build_response_metadata(
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
    expected_columns = expected_columns_from_sql(sql=sql, dialect=dialect)
    public_keys = set(expected_columns)
    source_relation = root_planned.source_relation

    dim_meta: Dict[str, FieldMetadata] = {}
    measure_meta: Dict[str, FieldMetadata] = {}

    projection_ids = set(root_planned.projection)
    candidate_slots = (
        list(root_planned.row_slots)
        + list(root_planned.aggregate_slots)
        + list(root_planned.combined_expression_slots)
    )
    for slot in candidate_slots:
        if slot.hidden or slot.id not in projection_ids:
            continue
        is_dim = slot.phase == Phase.ROW
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
                fmt = _measure_format(slot=slot, bundle=bundle)
                label = _measure_label(slot=slot, bundle=bundle)
                if label or fmt:
                    measure_meta[rk] = FieldMetadata(label=label, format=fmt)

    return ResponseAttributes(dimensions=dim_meta, measures=measure_meta), expected_columns
