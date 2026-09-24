"""Raise-site ledger.

One row per raise in the scanned modules; the parity test enforces that every
raise matches exactly one row with byte-identical literal message parts, so a
guard can relocate (module updated in lockstep) but its exception type and
message cannot drift unnoticed. ``…`` marks an interpolated segment (or a
keyword passed as a variable). A checker row's message is its
``_format_error_message`` rendering minus the ``ClassName: `` prefix.

Columns: module (slayer-root-relative) · function anchor · exception type ·
collapsed message · category (bind | checker | compiler | internal | control) ·
family · user-facing · final owner · sites (raises sharing the row) · deferral.
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


_SP = "engine/compile/stages.py"
_PL = "engine/compile/projection.py"
_EE = "engine/elaborate_env.py"  # the checker: guards relocate here from the compilers
_BI = "engine/bind_inputs.py"  # the query-level bind pass: the bind block left the compiler
_CI = "engine/compile/__init__.py"  # the compile entry API
_WD = "core/window_duration.py"  # the checker's one window= raise path

AT = "\n  at "
SUGGEST = "\n  suggestion: "

ROWS: Tuple[LedgerRow, ...] = (
    _row(module=_CI, function="compile_query", exc="ValueError",
         message="compile_query needs an environment produced by elaborate_query (its compile inputs are unset).",
         category="internal", family="internal", user=False, owner="compiler"),
    _row(module=_EE, function="check_dimension_temporal_axis", exc="TimeAxisError",
         message="A time-ordered transform evaluates at a grain that does not contain its time axis '…'; a producer bucketed by time joined back on the coarser grain would duplicate result rows."
         + AT + "transform …"
         + SUGGEST + "Include the time key in the aggregate's partition_by= so the transform accumulates within its own grain.",
         category="checker", family="time-axis", user=True, owner="checker"),
    _row(module=_WD, function="parse_window_duration", exc="WindowDurationError",
         message="Window duration must be a compact duration string like '90d', got …."
         + SUGGEST + "Use syntax like '1y2m3w5d6h7min8s'.",
         category="checker", family="local-partitioned", user=True, owner="checker"),
    _row(module=_WD, function="parse_window_duration", exc="WindowDurationError",
         message="Window duration cannot be empty.",
         category="checker", family="local-partitioned", user=True, owner="checker"),
    # Two sites: a gap before a part and an unparsed tail.
    _row(module=_WD, function="parse_window_duration", exc="WindowDurationError",
         message="Invalid window duration '…'."
         + SUGGEST + "Use syntax like '1y2m3w5d6h7min8s'.",
         category="checker", family="local-partitioned", user=True, owner="checker", sites=2),
    _row(module=_WD, function="parse_window_duration", exc="WindowDurationError",
         message="Window duration parts must be positive in '…'.",
         category="checker", family="local-partitioned", user=True, owner="checker"),
    _row(module=_EE, function="check_windowed_time_dimension", exc="TimeAxisError",
         message="Windowed measure could not resolve its time dimension."
         + SUGGEST + "Add a single time_dimensions entry, or set main_time_dimension to select among multiple time dimensions.",
         category="checker", family="time-axis", user=True, owner="checker"),
    _row(module=_EE, function="check_transform_inputs", exc="TransformInputError",
         message="The transform cannot consume a boolean-shaped predicate: its desugared arithmetic subtracts the shifted series, and subtraction over truth values is undefined."
         + AT + "transform …"
         + SUGGEST + "Shift the predicate itself with time_shift, or compare the shifted values instead.",
         category="checker", family="positions", user=True, owner="checker"),
    _row(module=_EE, function="check_transform_inputs", exc="TransformInputError",
         message="The transform cannot consume the row-level (non-aggregate) leaf '…', which refines the query grain: it would inflate the base grain to one row per (bucket, …-value)."
         + AT + "transform …"
         + SUGGEST + "Aggregate the leaf — e.g. …(…:sum) — project '…' as a query dimension, or compute it in an earlier stage of a multi-stage `source_queries` model.",
         category="checker", family="positions", user=True, owner="checker"),
    _row(module=_EE, function="check_raw_rows_filter_measure_ref", exc="DistinctDimensionValuesError",
         message="distinct_dimension_values=False rejects measure references, but the filter contains one."
         + AT + "filter …"
         + SUGGEST + "…",
         category="checker", family="positions", user=True, owner="checker"),
    _row(module=_EE, function="check_raw_rows_order_measure_ref", exc="DistinctDimensionValuesError",
         message="distinct_dimension_values=False rejects measure references, but the order item contains one."
         + AT + "order item …"
         + SUGGEST + "…",
         category="checker", family="positions", user=True, owner="checker"),
    _row(module=_EE, function="check_raw_rows_order_measure_ref", exc="DistinctDimensionValuesError",
         message="distinct_dimension_values=False rejects measure references, but the order item resolves to a saved measure on …."
         + AT + "order item …"
         + SUGGEST + "…",
         category="checker", family="positions", user=True, owner="checker"),
    _row(module=_EE, function="check_raw_rows_order_measure_ref", exc="DistinctDimensionValuesError",
         message="distinct_dimension_values=False rejects measure references, but the order item resolves to a saved measure."
         + AT + "order item …"
         + SUGGEST + "…",
         category="checker", family="positions", user=True, owner="checker"),
    _row(module=_EE, function="check_time_dimension_date_range", exc="TimeAxisError",
         message="The date_range has a null bound (…); a null bound cannot be expressed as a range."
         + AT + "time dimension …"
         + SUGGEST + "Use a one-sided filter (e.g. '>=' / '<=') instead.",
         category="checker", family="time-axis", user=True, owner="checker"),
    _row(module=_EE, function="check_time_dimension_column", exc="TimeDimensionColumnError",
         message="A time dimension must reference a temporal column (DATE / TIMESTAMP); got column type …."
         + AT + "time dimension …",
         category="checker", family="time-axis", user=True, owner="checker"),
    _row(module=_EE, function="check_time_dimension_column", exc="TimeDimensionColumnError",
         message="Cannot re-bucket to '…': the column is already bucketed at '…', which does not nest into '…'."
         + AT + "time dimension …"
         + SUGGEST + "Request the same or a nesting-coarser granularity, or bucket the raw column instead.",
         category="checker", family="time-axis", user=True, owner="checker"),
    _row(module=_EE, function="check_time_transforms_resolved", exc="TimeAxisError",
         message="The transform requires an unambiguous time dimension."
         + AT + "transform …"
         + SUGGEST + "Add a single time_dimensions entry, or set main_time_dimension to select among multiple time dimensions.",
         category="checker", family="time-axis", user=True, owner="checker"),
    _row(module=_EE, function="check_partition_key_resolves", exc="PartitionKeyError",
         message="The partition_by column '…' is ambiguous — it is a time dimension at multiple granularities."
         + AT + "…"
         + SUGGEST + "Partition by a single query dimension instead.",
         category="checker", family="local-partitioned", user=True, owner="checker"),
    _row(module=_EE, function="check_partition_key_resolves", exc="PartitionKeyError",
         message="The partition_by column '…' is not a query dimension."
         + AT + "…"
         + SUGGEST + "Add it to dimensions/time_dimensions, or choose one of: ….",
         category="checker", family="local-partitioned", user=True, owner="checker"),
    _row(module=_EE, function="check_transform_partition_keys_in_operand_grain", exc="PartitionKeyError",
         message="The partition_by column '…' is not a member of the transform's operand grain (…); a rank-family transform partitions its operand's cells."
         + AT + "transform …"
         + SUGGEST + "Add it to the inner aggregate's partition_by=, or partition by one of: ….",
         category="checker", family="local-partitioned", user=True, owner="checker"),
    _row(module=_SP, function="_assert_attach_covers_producer_grain", exc="ValueError",
         message="Regroup attach join keys do not match the producer's grouping grain; the join must cover the complete grain or it changes cardinality (DEV-1824).",
         category="internal", family="internal", user=False, owner="compiler"),
    _row(module=_EE, function="check_partition_key_attributable", exc="PartitionKeyError",
         message="The partition_by column '…' …; every partition key must be attributable from the aggregate's root."
         + AT + "…"
         + SUGGEST + "…",
         category="checker", family="local-partitioned", user=True, owner="checker"),
    _row(module=_EE, function="check_cross_model_inputs_safe", exc="UnsafeJoinInputError",
         message="The cross-model aggregate reads an input across an unproven join hop to … from …."
         + AT + "measure …"
         + SUGGEST + "…",
         category="checker", family="cross-model", user=True, owner="checker"),
    _row(module=_EE, function="check_cross_model_inputs_safe", exc="UnsafeJoinInputError",
         message="The cross-model aggregate ranks/reads by …, which is not attributable from … (…)."
         + AT + "measure …"
         + SUGGEST + "…",
         category="checker", family="cross-model", user=True, owner="checker"),
    # An input whose derived-column definition cannot be analysed is unsafe, not empty.
    _row(module=_EE, function="check_input_dependencies_analyzable", exc="UnanalyzableDependencyError",
         message="The aggregate names derived column …, whose definition no supported dialect can analyse for join dependencies; an unanalyzable dependency is unsafe."
         + AT + "measure …"
         + SUGGEST + "Fix the column's SQL, or remove it from the aggregate.",
         category="checker", family="cross-model", user=True, owner="checker"),
    # Its unnameable arm: the closure is unanalysable but no single column can be named.
    _row(module=_EE, function="check_input_dependencies_analyzable", exc="UnanalyzableDependencyError",
         message="The aggregate has an input dependency whose definition no supported dialect can analyse for join dependencies; an unanalyzable dependency is unsafe."
         + AT + "measure …"
         + SUGGEST + "Fix the input's SQL, or remove it from the aggregate.",
         category="checker", family="cross-model", user=True, owner="checker"),
    _row(module=_EE, function="check_local_producer_inputs_safe", exc="UnsafeJoinInputError",
         message="The aggregate ranks/reads by …, which crosses an unproven join hop to … from …."
         + AT + "measure …"
         + SUGGEST + "…",
         category="checker", family="local-partitioned", user=True, owner="checker"),
    _row(module=_EE, function="check_local_producer_inputs_safe", exc="UnsafeJoinInputError",
         message="The aggregate reads an input across an unproven join hop to … from …."
         + AT + "measure …"
         + SUGGEST + "…",
         category="checker", family="local-partitioned", user=True, owner="checker"),
    _row(module=_EE, function="check_local_producer_inputs_safe", exc="UnsafeJoinInputError",
         message="The aggregate reads its source across an unproven or fanning join hop to … from …: a column of … cannot be aggregated across a to-many target."
         + AT + "measure …"
         + SUGGEST + "Aggregate the target column directly (….<column>:<aggregation>), or declare a to-one cardinality or a covering unique key if the hop is to-one.",
         category="checker", family="local-partitioned", user=True, owner="checker"),
    _row(module=_SP, function="_forward_hops", exc="_PushBlocked",
         message="unreachable from the aggregate's root (no join edge from … to …)",
         category="control", family="positions", user=False, owner="compiler"),
    _row(module=_SP, function="_reverse_hops", exc="_PushBlocked",
         message="unreachable from the aggregate's root (join path … does not resolve from …)",
         category="control", family="positions", user=False, owner="compiler"),
    _row(module=_SP, function="_resolve_ref_anchor", exc="_PushBlocked", message="",
         category="control", family="positions", user=False, owner="compiler"),
    _row(module=_SP, function="_ref_sql_dependency_paths", exc="_PushBlocked",
         message="unanalyzable definition fragment on ….… — cannot determine the semi-join hops it crosses",
         category="control", family="positions", user=False, owner="compiler"),
    _row(module=_EE, function="check_cross_model_source_resolves", exc="UnsafeJoinInputError",
         message="Cross-model aggregate source path … does not resolve to a model from ….",
         category="checker", family="cross-model", user=True, owner="checker"),
    _row(module=_EE, function="check_cross_model_partition_keys_attributable", exc="PartitionKeyError",
         message="The cross-model aggregate declares partition_by=…, which …; every explicit partition key must be attributable from …."
         + AT + "measure …"
         + SUGGEST + "…",
         category="checker", family="cross-model", user=True, owner="checker"),
    _row(module=_EE, function="check_windowed_time_axis_attributable", exc="TimeAxisError",
         message="The windowed aggregate has no active time dimension."
         + AT + "measure …"
         + SUGGEST + "Add a single time_dimensions entry.",
         category="checker", family="cross-model", user=True, owner="checker"),
    _row(module=_EE, function="check_windowed_time_axis_attributable", exc="TimeAxisError",
         message="The windowed aggregate needs the query's active time dimension ('…') attributable from …, but it crosses a fanning join."
         + AT + "measure …"
         + SUGGEST + "…",
         category="checker", family="cross-model", user=True, owner="checker"),
    _row(module=_EE, function="check_association_windowed_ranked", exc="AssociationError",
         message="The aggregate needs distinct-entity association over an unattributable dimension, which is unsupported in combination with window=/first/last."
         + AT + "measure …"
         + SUGGEST + "Drop the window/first-last or attribute the dimension.",
         category="checker", family="association", user=True, owner="checker"),
    _row(module=_EE, function="check_association_root_unique_key", exc="AssociationError",
         message="The aggregate needs distinct-entity association, but its root model … declares no primary or unique key to deduplicate entities by."
         + AT + "measure …"
         + SUGGEST + "Declare a primary or unique key on ….",
         category="checker", family="association", user=True, owner="checker"),
    _row(module=_EE, function="check_parameter_determined", exc="ParameterGrainError",
         message="Parameter … is not determined by the operand grain (…); the aggregation reads one value per cell of that grain."
         + AT + "measure …"
         + SUGGEST + "Aggregate the parameter to that grain, or add its determining keys to the operand's partition_by=.",
         category="checker", family="parameter", user=True, owner="checker"),
    # A filter conjunct whose dependency closure cannot be analysed.
    _row(module=_EE, function="check_filter_dependencies_analyzable", exc="UnanalyzableDependencyError",
         message="The filter names derived column …, whose definition no supported dialect can analyse for join dependencies; an unanalyzable dependency is unsafe."
         + AT + "filter …"
         + SUGGEST + "Fix the column's SQL, or remove the filter.",
         category="checker", family="filters", user=True, owner="checker"),
    # Its unnameable arm: the closure is unanalysable but no single column can be named.
    _row(module=_EE, function="check_filter_dependencies_analyzable", exc="UnanalyzableDependencyError",
         message="The filter has a dependency whose definition no supported dialect can analyse for join dependencies; an unanalyzable dependency is unsafe."
         + AT + "filter …"
         + SUGGEST + "Fix the referenced column's SQL, or remove the filter.",
         category="checker", family="filters", user=True, owner="checker"),
    _row(module=_EE, function="check_reaggregation_no_window", exc="ReaggregationError",
         message="The re-aggregation cannot carry window= on its outer aggregation."
         + AT + "measure …"
         + SUGGEST + "Apply the window inside the operand or consume the re-aggregated value through a transform.",
         category="checker", family="reaggregation", user=True, owner="checker"),
    _row(module=_EE, function="check_reaggregation_partition_key_is_query_dim", exc="PartitionKeyError",
         message="The re-aggregation declares partition_by=…, which is not a query dimension; every explicit partition key must be a query dimension."
         + AT + "measure …"
         + SUGGEST + "Add it to dimensions/time_dimensions.",
         category="checker", family="reaggregation", user=True, owner="checker"),
    _row(module=_EE, function="check_reaggregation_dims_attributable", exc="ReaggregationError",
         message="The re-aggregation cannot attribute dimension(s) … to the operand dataset under to_many_handling='error'."
         + AT + "measure …"
         + SUGGEST + "Add them to the inner partition_by= so the operand is grained by them, or choose 'broadcast'/'associate'.",
         category="checker", family="reaggregation", user=True, owner="checker"),
    _row(module=_SP, function="_assert_total_routing", exc="ValueError",
         message="Aggregate … in a … received no routing disposition (inline, producer substitution, or explicit rejection) — the planner cannot compile this shape.",
         category="internal", family="internal", user=False, owner="compiler"),
    _row(module=_EE, function="check_reserved_regroup_prefix", exc="NameCollisionError",
         message="Column(s) … use the reserved '__regroup__' prefix, which collides with the regroup primitive's placeholders."
         + SUGGEST + "Rename them.",
         category="checker", family="regroup-roots", user=True, owner="checker"),
    _row(module=_EE, function="check_raw_rows_no_aggregate_slots", exc="DistinctDimensionValuesError",
         message="distinct_dimension_values=False rejects measure references, but this query references the aggregation … in its filters or order."
         + SUGGEST + "…",
         category="checker", family="positions", user=True, owner="checker"),
    _row(module=_EE, function="check_order_target_has_slot", exc="PositionTypingError",
         message="ORDER BY expression is not supported: … has no materialisable slot."
         + SUGGEST + "Order by an aggregate, a transform, a composite arithmetic / scalar expression, a dimension, or declare the expression as a measure and order by its name.",
         category="checker", family="positions", user=True, owner="checker"),
    _row(module=_EE, function="check_opaque_grouping_dim", exc="DimensionTypeError",
         message="The column cannot be used as a dimension: its type does not support the GROUP BY / DISTINCT this query requires."
         + AT + "dimension …"
         + SUGGEST + 'Define a derived column that extracts a comparable value instead, e.g. sql="payload->>\'status\'" with type TEXT.',
         category="checker", family="regroup-roots", user=True, owner="checker"),
    _row(module=_EE, function="check_computed_dim_name_collision", exc="NameCollisionError",
         message="The computed dimension name collides with an existing column or measure on model …."
         + AT + "dimension …"
         + SUGGEST + "Choose a different name.",
         category="checker", family="names", user=True, owner="checker"),
    _row(module=_EE, function="check_computed_dim_name_collision", exc="NameCollisionError",
         message="The computed dimension name collides with a query measure of the same name."
         + AT + "dimension …"
         + SUGGEST + "Choose a different name.",
         category="checker", family="names", user=True, owner="checker"),
    _row(module=_EE, function="check_computed_dimension", exc="ComputedDimensionError",
         message="The transform '…' inside the computed dimension must take an aggregate input — a transform acts on aggregates, e.g. …(sum(amount, partition_by=city))."
         + AT + "dimension …",
         category="checker", family="regroup-roots", user=True, owner="checker"),
    _row(module=_EE, function="check_computed_dimension", exc="ComputedDimensionError",
         message="The aggregate '…(…)' inside the transform in the computed dimension must declare partition_by= explicitly: the ungrained default (the query's own dimensions) would include the dimension being defined."
         + AT + "dimension …",
         category="checker", family="regroup-roots", user=True, owner="checker"),
    _row(module=_EE, function="check_computed_dimension", exc="DistinctDimensionValuesError",
         message="The computed dimension references an aggregate, so it cannot be used with distinct_dimension_values=False (raw rows)."
         + AT + "dimension …"
         + SUGGEST + "Remove the flag (the default aggregates) or drop the aggregate from the dimension.",
         category="checker", family="positions", user=True, owner="checker"),
    _row(module=_EE, function="check_computed_dimension", exc="ComputedDimensionError",
         message="The aggregate inside the computed dimension must declare the grain it aggregates over with partition_by=, e.g. 'CASE WHEN sum(amount, partition_by=city) > 5000 THEN 1 ELSE 0 END'. Without partition_by the group key is a function of the query's own dimensions and adds no grouping."
         + AT + "dimension …",
         category="checker", family="regroup-roots", user=True, owner="checker"),
    _row(module=_EE, function="check_stage_flatten_collision", exc="NameCollisionError",
         message="Stage column name collision on …: two projected columns flatten to the same downstream name."
         + SUGGEST + "Give one an explicit measure `name` to disambiguate.",
         category="checker", family="names", user=True, owner="checker"),
    _row(module=_EE, function="check_measure_dedupe_collision", exc="NameCollisionError",
         message="Measures … and … both derive the result key … but compute different values."
         + SUGGEST + "Rename one (set 'name') to disambiguate.",
         category="checker", family="names", user=True, owner="checker"),
    _row(module=_EE, function="check_measure_dedupe_collision", exc="NameCollisionError",
         message="Measures … and … merge into one result column … but declare different label/type."
         + SUGGEST + "Rename one (set 'name') to disambiguate.",
         category="checker", family="names", user=True, owner="checker"),
    _row(module=_SP, function="_topo_sort", exc="ValueError",
         message="Duplicate stage names in source_queries DAG: …",
         category="compiler", family="stages", user=True, owner="compiler"),
    _row(module=_SP, function="_topo_sort", exc="ValueError",
         message="Cycle detected in source_queries DAG involving stages: …",
         category="compiler", family="stages", user=True, owner="compiler"),

    _row(module=_EE, function="validate_model_filter", exc="ModelFilterError",
         message="The model filter references measure …. Model filters can only reference table columns (WHERE)."
         + AT + "model filter …"
         + SUGGEST + "Use query-level filters for measure conditions.",
         category="checker", family="model-filters", user=True, owner="checker"),
    _row(module=_EE, function="validate_model_filter", exc="ModelFilterError",
         message="The model filter references column … whose SQL contains a window function."
         + AT + "model filter …"
         + SUGGEST + "Factor it into a multi-stage source_queries model or use a rank-family transform at query time.",
         category="checker", family="model-filters", user=True, owner="checker"),
    # Two sites: leaf-collision disambiguation and same-column multi-granularity.
    _row(module=_BI, function="_resolve_main_time_dimension", exc="AmbiguousReferenceError", message="",
         category="bind", family="time-axis", user=True, owner="bind", sites=2),
    _row(module=_BI, function="_resolve_main_time_dimension", exc="UnknownReferenceError", message="",
         category="bind", family="time-axis", user=True, owner="bind"),
    _row(module=_BI, function="bind_query_inputs", exc="GranularityCallError",
         message="Order key …(…) has no matching projected time dimension. Project a time_dimension on … at … granularity (e.g. …(…) in dimensions) to order by its bucket.",
         category="bind", family="time-axis", user=True, owner="bind"),
    _row(module=_BI, function="_assert_equivalent_tds_agree", exc="GranularityCallError",
         message="Conflicting time dimensions on … at … granularity: equivalent columns must not differ in date range or label.",
         category="bind", family="time-axis", user=True, owner="bind"),
    _row(module=_EE, function="type_position_conjunct", exc="PositionTypingError",
         message="This … expression is valid as neither a field nor a measure. Field typing failed: it references …, available only after aggregation. Measure typing failed: it references row-level …, not available at the query grain (not among the query dimensions)."
         + SUGGEST + "Split the top-level AND conjuncts so each resolves in one typing, or add the row-level reference to the query dimensions.",
         category="checker", family="positions", user=True, owner="checker"),
    _row(module=_EE, function="type_position_conjunct", exc="PositionTypingError",
         message="This … expression references …, so it is not a field, and measure typing is unavailable because the query has no measure position (distinct_dimension_values=False).",
         category="checker", family="positions", user=True, owner="checker"),
    _row(module=_EE, function="check_measure_name_collision", exc="MeasureNameCollidesWithColumnError", message="",
         category="checker", family="names", user=True, owner="checker"),
    _row(module=_EE, function="check_canonical_alias_shadows_column", exc="CanonicalAliasShadowsColumnError", message="",
         category="checker", family="names", user=True, owner="checker"),
    _row(module=_EE, function="check_duplicate_measure_name", exc="DuplicateMeasureNameError", message="",
         category="checker", family="names", user=True, owner="checker"),
    _row(module=_PL, function="_iter_slot_deps", exc="TypeError",
         message="_iter_slot_deps has no case for …: classify the kind explicitly (slot-worthy, inlined composite, or never slottable).",
         category="internal", family="internal", user=False, owner="compiler"),
)
