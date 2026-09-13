"""DEV-1871 raise-site ledger (design D4).

One row per raise in the planner modules; the parity test enforces that every
raise matches exactly one row with byte-identical literal message parts, so a
guard can relocate (module updated in lockstep) but its exception type and
message cannot drift unnoticed. ``…`` marks an interpolated segment.

Columns: module · function anchor · exception type · collapsed message ·
category (bind | checker | compiler | internal | control) · family ·
user-facing · final owner · sites (raises sharing the row) · deferral.
"""

from __future__ import annotations

from typing import Literal, Tuple

from pydantic import BaseModel, ConfigDict

Category = Literal["bind", "checker", "compiler", "internal", "control"]
Owner = Literal["bind", "checker", "compiler"]


class LedgerRow(BaseModel):
    model_config = ConfigDict(frozen=True)

    module: str
    function: str
    exc: str
    message: str
    category: Category
    family: str
    user: bool
    owner: Owner
    sites: int = 1
    deferral: bool = False


def _row(module, function, exc, message, category, family, user, owner,
         sites=1, deferral=False) -> LedgerRow:
    return LedgerRow(
        module=module, function=function, exc=exc, message=message,
        category=category, family=family, user=user, owner=owner,
        sites=sites, deferral=deferral,
    )


_SP = "compile/stages.py"
_PL = "compile/projection.py"
_EE = "elaborate_env.py"  # the checker (DEV-1871 G9+): guards relocate here from the compilers
_BI = "bind_inputs.py"  # the query-level bind pass (DEV-1871 G16): the bind block left the compiler
_EL = "elaborate.py"  # the one elaboration pass (entry API)
_CI = "compile/__init__.py"  # the compile entry API

ROWS: Tuple[LedgerRow, ...] = (
    _row(_EL, "elaborate_query", "ValueError",
         "elaborate_query needs query= or an explicit scope=.",
         "internal", "internal", False, "checker"),
    _row(_EL, "elaborate_query", "ValueError",
         "elaborate_query needs query= or prebound=.",
         "internal", "internal", False, "checker"),
    _row(_CI, "compile_query", "ValueError",
         "compile_query needs an environment produced by elaborate_query (its compile inputs are unset).",
         "internal", "internal", False, "compiler"),
    _row(_EE, "check_dimension_temporal_axis", "NotImplementedError",
         "A time-ordered transform '…' inside a computed dimension evaluates at a grain that does not contain its time axis '…'; a producer bucketed by time joined back on the coarser grain would duplicate result rows. Include the time key in the aggregate's partition_by= so the transform accumulates within its own grain.",
         "checker", "time-axis", True, "checker"),
    _row(_EE, "check_windowed_key_supported", "ValueError",
         "Aggregation parameter 'window' is only supported for sum and avg, not '…'.",
         "checker", "local-partitioned", True, "checker"),
    _row(_EE, "check_windowed_key_supported", "ValueError",
         "Window duration must be a compact duration string like '90d', got …. Use syntax like '1y2m3w5d6h7min8s'.",
         "checker", "local-partitioned", True, "checker"),
    _row(_EE, "check_windowed_time_dimension", "ValueError",
         "Windowed measure could not resolve its time dimension. Add a single time_dimensions entry, or set main_time_dimension to select among multiple time dimensions.",
         "checker", "time-axis", True, "checker"),
    _row(_EE, "check_partitioned_measures", "NotImplementedError",
         "partition_by on a cross-model first/last aggregation is not yet supported (DEV-1868); the aggregate must be local to the query's source.",
         "checker", "local-partitioned", True, "checker", deferral=True),
    _row(_EE, "check_partitioned_measures", "NotImplementedError",
         "A cross-model partition_by aggregate nested inside a transform is not yet supported (DEV-1868); the partitioned aggregate must be local to the query's source.",
         "checker", "local-partitioned", True, "checker", deferral=True),
    _row(_EE, "check_raw_rows_filter_measure_ref", "DistinctDimensionValuesError",
         "distinct_dimension_values=False rejects measure references, but filter … contains one. …",
         "checker", "positions", True, "checker"),
    _row(_EE, "check_raw_rows_order_measure_ref", "DistinctDimensionValuesError",
         "distinct_dimension_values=False rejects measure references, but order item … contains one. …",
         "checker", "positions", True, "checker"),
    _row(_EE, "check_raw_rows_order_measure_ref", "DistinctDimensionValuesError",
         "distinct_dimension_values=False rejects measure references, but order item … resolves to a saved measure on …. …",
         "checker", "positions", True, "checker"),
    _row(_EE, "check_raw_rows_order_measure_ref", "DistinctDimensionValuesError",
         "distinct_dimension_values=False rejects measure references, but order item … resolves to a saved measure. …",
         "checker", "positions", True, "checker"),
    _row(_EE, "check_time_dimension_date_range", "ValueError",
         "TimeDimension … has a date_range with a null bound (…); a null bound cannot be expressed as a range. Use a one-sided filter (e.g. '>=' / '<=') instead.",
         "checker", "time-axis", True, "checker"),
    _row(_EE, "check_time_transforms_resolved", "ValueError",
         "Transform '…' requires an unambiguous time dimension. Add a single time_dimensions entry, or set main_time_dimension to select among multiple time dimensions.",
         "checker", "time-axis", True, "checker"),
    _row(_EE, "check_partition_key_resolves", "ValueError",
         "…: partition_by column '…' is ambiguous — it is a time dimension at multiple granularities. Partition by a single query dimension instead.",
         "checker", "local-partitioned", True, "checker"),
    _row(_EE, "check_partition_key_resolves", "ValueError",
         "…: partition_by column '…' is not a query dimension. Add it to dimensions/time_dimensions, or choose one of: ….",
         "checker", "local-partitioned", True, "checker"),
    _row(_SP, "_assert_attach_covers_producer_grain", "ValueError",
         "Regroup attach join keys do not match the producer's grouping grain; the join must cover the complete grain or it changes cardinality (DEV-1824).",
         "internal", "internal", False, "compiler"),
    _row(_EE, "check_partition_key_attributable", "ValueError",
         "…: partition_by column '…' …; every partition key must be attributable from the aggregate's root — declare join cardinality or a covering unique key on the target.",
         "checker", "local-partitioned", True, "checker"),
    _row(_EE, "check_cross_model_inputs_safe", "ValueError",
         "Cross-model aggregate … reads an input across an unproven join hop to … from …; ….",
         "checker", "cross-model", True, "checker"),
    _row(_EE, "check_cross_model_inputs_safe", "ValueError",
         "Cross-model aggregate … ranks/reads by …, which is not attributable from … (crosses a fanning join); ….",
         "checker", "cross-model", True, "checker"),
    _row(_EE, "check_local_producer_inputs_safe", "ValueError",
         "Aggregate … ranks/reads by …, which crosses an unproven join hop to … from …; ….",
         "checker", "local-partitioned", True, "checker"),
    _row(_EE, "check_local_producer_inputs_safe", "ValueError",
         "Aggregate … reads an input across an unproven join hop to … from …; ….",
         "checker", "local-partitioned", True, "checker"),
    _row(_SP, "_forward_hops", "_PushBlocked",
         "unreachable from the aggregate's root (no join edge from … to …)",
         "control", "positions", False, "compiler"),
    _row(_SP, "_reverse_hops", "_PushBlocked",
         "unreachable from the aggregate's root (join path … does not resolve from …)",
         "control", "positions", False, "compiler"),
    _row(_SP, "_walk", "_PushBlocked",
         "mixes a root-anchored predicate with a cross-path predicate under OR/NOT, which a semi-join cannot preserve",
         "control", "positions", False, "compiler"),
    _row(_SP, "_resolve_ref_anchor", "_PushBlocked", "",
         "control", "positions", False, "compiler"),
    _row(_SP, "_conjunct_push_plan", "_PushBlocked",
         "its cross-path references span multiple join branches from …, so no single semi-join tree covers them",
         "control", "positions", False, "compiler"),
    _row(_EE, "check_cross_model_source_resolves", "ValueError",
         "Cross-model aggregate source path … does not resolve to a model from ….",
         "checker", "cross-model", True, "checker"),
    _row(_EE, "check_cross_model_partition_keys_attributable", "ValueError",
         "Cross-model aggregate … declares partition_by=…, which …; every explicit partition key must be attributable from … — declare join cardinality or a covering unique key on the target.",
         "checker", "cross-model", True, "checker"),
    _row(_EE, "check_windowed_cross_model_time_axis", "ValueError",
         "Windowed cross-model aggregate … has no active time dimension; add a single time_dimensions entry.",
         "checker", "cross-model", True, "checker"),
    _row(_EE, "check_windowed_cross_model_time_axis", "ValueError",
         "Windowed cross-model aggregate … needs the query's active time dimension ('…') attributable from …, but it crosses a fanning join; declare join cardinality or a covering unique key on the target.",
         "checker", "cross-model", True, "checker"),
    _row(_EE, "check_association_windowed_ranked", "SlayerError",
         "Aggregate … needs distinct-entity association over an unattributable dimension, which is unsupported in combination with window=/first/last; drop the window/first-last or attribute the dimension.",
         "checker", "association", True, "checker"),
    _row(_EE, "check_association_root_unique_key", "SlayerError",
         "Aggregate … needs distinct-entity association, but its root model … declares no primary or unique key to deduplicate entities by; declare a primary or unique key on ….",
         "checker", "association", True, "checker"),
    _row(_EE, "check_association_column_param", "SlayerError",
         "Aggregate … needs distinct-entity association over an unattributable dimension, which is unsupported with a column-reference parameter (e.g. weighted_avg(weight=…)); the per-entity pick carries only the aggregate's own value. Attribute the dimension or drop the column parameter.",
         "checker", "association", True, "checker"),
    _row(_EE, "check_reaggregation_no_window", "SlayerError",
         "Re-aggregation … cannot carry window= on its outer aggregation; apply the window inside the operand or consume the re-aggregated value through a transform.",
         "checker", "reaggregation", True, "checker"),
    _row(_EE, "check_reaggregation_no_column_param", "SlayerError",
         "Re-aggregation … carries a column-reference parameter on its outer aggregation (e.g. weighted_avg(weight=…)), which is unsupported: the outer aggregate consumes only the operand's per-cell values. Drop the column parameter or use a numeric literal.",
         "checker", "reaggregation", True, "checker"),
    _row(_EE, "check_reaggregation_partition_key_is_query_dim", "ValueError",
         "Re-aggregation … declares partition_by=…, which is not a query dimension; every explicit partition key must be a query dimension — add it to dimensions/time_dimensions.",
         "checker", "reaggregation", True, "checker"),
    _row(_EE, "check_reaggregation_dims_attributable", "ValueError",
         "Re-aggregation … cannot attribute dimension(s) … to the operand dataset under to_many_handling='error'; add them to the inner partition_by= so the operand is grained by them, or choose 'broadcast'/'associate'.",
         "checker", "reaggregation", True, "checker"),
    _row(_SP, "_assert_total_routing", "ValueError",
         "Aggregate … in a … received no routing disposition (inline, producer substitution, or explicit rejection) — the planner cannot compile this shape.",
         "internal", "internal", False, "compiler"),
    _row(_EE, "check_reserved_regroup_prefix", "ValueError",
         "Column(s) … use the reserved '__regroup__' prefix, which collides with the regroup primitive's placeholders. Rename them.",
         "checker", "regroup-roots", True, "checker"),
    _row(_EE, "check_raw_rows_no_aggregate_slots", "DistinctDimensionValuesError",
         "distinct_dimension_values=False rejects measure references, but this query references the aggregation … in its filters or order. Either remove the measure reference, or set distinct_dimension_values=True (the default) to keep the auto-aggregating behaviour.",
         "checker", "positions", True, "checker"),
    _row(_EE, "check_order_target_has_slot", "PositionTypingError",
         "ORDER BY expression is not supported: … has no materialisable slot. Order by an aggregate, a transform, a composite arithmetic / scalar expression, a dimension, or declare the expression as a measure and order by its name.",
         "checker", "positions", True, "checker"),
    _row(_EE, "check_opaque_grouping_dim", "ValueError",
         'Column \'…\' cannot be used as a dimension: its type does not support the GROUP BY / DISTINCT this query requires. Define a derived column that extracts a comparable value instead, e.g. sql="payload->>\'status\'" with type TEXT.',
         "checker", "regroup-roots", True, "checker"),
    _row(_EE, "check_computed_dim_name_collision", "ValueError",
         "Computed dimension name … collides with an existing column or measure on model …. Choose a different name.",
         "checker", "names", True, "checker"),
    _row(_EE, "check_computed_dim_name_collision", "ValueError",
         "Computed dimension name … collides with a query measure of the same name. Choose a different name.",
         "checker", "names", True, "checker"),
    _row(_EE, "check_computed_dimension", "NotImplementedError",
         "A transform inside computed dimension … must wrap an explicitly-grained aggregate — declare partition_by= on the aggregate it transforms (DEV-1868).",
         "checker", "regroup-roots", True, "checker", deferral=True),
    _row(_EE, "check_computed_dimension", "DistinctDimensionValuesError",
         "Computed dimension … references an aggregate, so it cannot be used with distinct_dimension_values=False (raw rows). Remove the flag (the default aggregates) or drop the aggregate from the dimension.",
         "checker", "positions", True, "checker"),
    _row(_EE, "check_computed_dimension", "ValueError",
         "The aggregate inside computed dimension … must declare the grain it aggregates over with partition_by=, e.g. 'CASE WHEN amount:sum(partition_by=city) > 5000 THEN 1 ELSE 0 END'. Without partition_by the group key is a function of the query's own dimensions and adds no grouping.",
         "checker", "regroup-roots", True, "checker"),
    _row(_EE, "check_stage_flatten_collision", "ValueError", "",
         "checker", "names", True, "checker"),
    _row(_EE, "check_measure_dedupe_collision", "ValueError",
         "Measures … and … both derive the result key … but compute different values; rename one (set 'name') to disambiguate.",
         "checker", "names", True, "checker"),
    _row(_EE, "check_measure_dedupe_collision", "ValueError",
         "Measures … and … merge into one result column … but declare different label/type; rename one (set 'name') to disambiguate.",
         "checker", "names", True, "checker"),
    _row(_SP, "_topo_sort", "ValueError",
         "Duplicate stage names in source_queries DAG: …",
         "compiler", "stages", True, "compiler"),
    _row(_SP, "_topo_sort", "ValueError",
         "Cycle detected in source_queries DAG involving stages: …",
         "compiler", "stages", True, "compiler"),

    _row(_EE, "validate_model_filter", "ValueError",
         "Model filter … references measure …. Model filters can only reference table columns (WHERE). Use query-level filters for measure conditions.",
         "checker", "model-filters", True, "checker"),
    _row(_EE, "validate_model_filter", "ValueError",
         "Model filter … references column … whose SQL contains a window function. Factor it into a multi-stage source_queries model or use a rank-family transform at query time.",
         "checker", "model-filters", True, "checker"),
    _row(_BI, "_resolve_main_time_dimension", "AmbiguousReferenceError", "",
         "bind", "time-axis", True, "bind"),
    _row(_BI, "_resolve_main_time_dimension", "UnknownReferenceError", "",
         "bind", "time-axis", True, "bind"),
    _row(_EE, "type_position_conjunct", "PositionTypingError",
         "This … expression is valid as neither a field nor a measure. Field typing failed: it references …, available only after aggregation. Measure typing failed: it references row-level …, not available at the query grain (not among the query dimensions). Split the top-level AND conjuncts so each resolves in one typing, or add the row-level reference to the query dimensions.",
         "checker", "positions", True, "checker"),
    _row(_EE, "type_position_conjunct", "PositionTypingError",
         "This … expression references …, so it is not a field, and measure typing is unavailable because the query has no measure position (distinct_dimension_values=False).",
         "checker", "positions", True, "checker"),
    _row(_EE, "check_measure_name_collision", "MeasureNameCollidesWithColumnError", "",
         "checker", "names", True, "checker"),
    _row(_EE, "check_canonical_alias_shadows_column", "CanonicalAliasShadowsColumnError", "",
         "checker", "names", True, "checker"),
    _row(_EE, "check_duplicate_measure_name", "DuplicateMeasureNameError", "",
         "checker", "names", True, "checker"),
    _row(_PL, "_iter_slot_deps", "TypeError",
         "_iter_slot_deps has no case for …: classify the kind explicitly (slot-worthy, inlined composite, or never slottable).",
         "internal", "internal", False, "compiler"),
)
