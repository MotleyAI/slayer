# Raise-site ledger (D4)

Machine source of truth: `tests/_dev1871_raise_ledger.py`, enforced by
`tests/test_dev1871_raise_parity.py` (byte-identical literals, exact site
counts per module, lockstep module updates on relocation). `…` marks an
interpolated segment; an empty message anchors by function.

| module | function | exception | category | family | user | owner | sites | deferral | message |
|---|---|---|---|---|---|---|---|---|---|
| elaborate_env.py | check_dimension_temporal_axis | NotImplementedError | checker | time-axis | yes | checker | 1 |  | A time-ordered transform '…' inside a computed dimension evaluates at a grain that does not contain its tim... |
| elaborate_env.py | check_windowed_key_supported | ValueError | checker | local-partitioned | yes | checker | 1 |  | Aggregation parameter 'window' is only supported for sum and avg, not '…'. |
| elaborate_env.py | check_windowed_key_supported | ValueError | checker | local-partitioned | yes | checker | 1 |  | Window duration must be a compact duration string like '90d', got …. Use syntax like '1y2m3w5d6h7min8s'. |
| elaborate_env.py | check_windowed_time_dimension | ValueError | checker | time-axis | yes | checker | 1 |  | Windowed measure could not resolve its time dimension. Add a single time_dimensions entry, or set main_time... |
| elaborate_env.py | check_partitioned_measures | NotImplementedError | checker | local-partitioned | yes | checker | 1 | yes | partition_by on a cross-model first/last aggregation is not yet supported (DEV-1868); the aggregate must be... |
| elaborate_env.py | check_partitioned_measures | NotImplementedError | checker | local-partitioned | yes | checker | 1 | yes | A cross-model partition_by aggregate nested inside a transform is not yet supported (DEV-1868); the partiti... |
| compile/stages.py | _windowed_slot_id_set | RuntimeError | internal | internal | no | compiler | 1 |  | Windowed measure … was selected but has no projection slot; planner/projection drift (DEV-1714). |
| compile/stages.py | _reject_measure_refs_in_filters | DistinctDimensionValuesError | checker | positions | yes | checker | 1 |  | distinct_dimension_values=False rejects measure references, but filter … contains one. … |
| compile/stages.py | _reject_measure_refs_in_order | DistinctDimensionValuesError | checker | positions | yes | checker | 1 |  | distinct_dimension_values=False rejects measure references, but order item … contains one. … |
| compile/stages.py | _reject_measure_refs_in_order | DistinctDimensionValuesError | checker | positions | yes | checker | 1 |  | distinct_dimension_values=False rejects measure references, but order item … resolves to a saved measure on... |
| compile/stages.py | _reject_measure_refs_in_order | DistinctDimensionValuesError | checker | positions | yes | checker | 1 |  | distinct_dimension_values=False rejects measure references, but order item … resolves to a saved measure. … |
| elaborate_env.py | check_time_dimension_date_range | ValueError | checker | time-axis | yes | checker | 1 |  | TimeDimension … has a date_range with a null bound (…); a null bound cannot be expressed as a range. Use a ... |
| elaborate_env.py | check_time_transforms_resolved | ValueError | checker | time-axis | yes | checker | 1 |  | Transform '…' requires an unambiguous time dimension. Add a single time_dimensions entry, or set main_time_... |
| elaborate_env.py | check_partition_key_resolves | ValueError | checker | local-partitioned | yes | checker | 1 |  | …: partition_by column '…' is ambiguous — it is a time dimension at multiple granularities. Partition by a ... |
| elaborate_env.py | check_partition_key_resolves | ValueError | checker | local-partitioned | yes | checker | 1 |  | …: partition_by column '…' is not a query dimension. Add it to dimensions/time_dimensions, or choose one of... |
| compile/stages.py | _find_regroup_slot | ValueError | internal | internal | no | compiler | 1 |  | Regroup producer plan is missing the … slot for …; synthesis and planning disagree on its grain. |
| compile/stages.py | _regroup_answer_slot_id | ValueError | internal | internal | no | compiler | 1 |  | Regroup producer plan is missing the answer slot for …; synthesis and planning disagree on its grain. |
| compile/stages.py | _assert_attach_covers_producer_grain | ValueError | internal | internal | no | compiler | 1 |  | Regroup attach join keys do not match the producer's grouping grain; the join must cover the complete grain... |
| elaborate_env.py | check_partition_key_attributable | ValueError | checker | local-partitioned | yes | checker | 1 |  | …: partition_by column '…' …; every partition key must be attributable from the aggregate's root — declare ... |
| elaborate_env.py | check_cross_model_inputs_safe | ValueError | checker | cross-model | yes | checker | 1 |  | Cross-model aggregate … reads an input across an unproven join hop to … from …; …. |
| elaborate_env.py | check_cross_model_inputs_safe | ValueError | checker | cross-model | yes | checker | 1 |  | Cross-model aggregate … ranks/reads by …, which is not attributable from … (crosses a fanning join); …. |
| elaborate_env.py | check_local_producer_inputs_safe | ValueError | checker | local-partitioned | yes | checker | 1 |  | Aggregate … ranks/reads by …, which crosses an unproven join hop to … from …; …. |
| elaborate_env.py | check_local_producer_inputs_safe | ValueError | checker | local-partitioned | yes | checker | 1 |  | Aggregate … reads an input across an unproven join hop to … from …; …. |
| compile/stages.py | _trailing_window_kernel | RuntimeError | internal | internal | no | compiler | 1 |  | Windowed producer is missing its window duration or bucket slot; synthesis and planning disagree (DEV-1838). |
| compile/stages.py | _forward_hops | _PushBlocked | control | positions | no | compiler | 1 |  | unreachable from the aggregate's root (no join edge from … to …) |
| compile/stages.py | _reverse_hops | _PushBlocked | control | positions | no | compiler | 1 |  | unreachable from the aggregate's root (join path … does not resolve from …) |
| compile/stages.py | _walk | _PushBlocked | control | positions | no | compiler | 1 |  | mixes a root-anchored predicate with a cross-path predicate under OR/NOT, which a semi-join cannot preserve |
| compile/stages.py | _resolve_ref_anchor | _PushBlocked | control | positions | no | compiler | 1 |  |  |
| compile/stages.py | _conjunct_push_plan | _PushBlocked | control | positions | no | compiler | 1 |  | its cross-path references span multiple join branches from …, so no single semi-join tree covers them |
| elaborate_env.py | check_cross_model_source_resolves | ValueError | checker | cross-model | yes | checker | 1 |  | Cross-model aggregate source path … does not resolve to a model from …. |
| elaborate_env.py | check_cross_model_partition_keys_attributable | ValueError | checker | cross-model | yes | checker | 1 |  | Cross-model aggregate … declares partition_by=…, which …; every explicit partition key must be attributable... |
| elaborate_env.py | check_windowed_cross_model_time_axis | ValueError | checker | cross-model | yes | checker | 1 |  | Windowed cross-model aggregate … has no active time dimension; add a single time_dimensions entry. |
| elaborate_env.py | check_windowed_cross_model_time_axis | ValueError | checker | cross-model | yes | checker | 1 |  | Windowed cross-model aggregate … needs the query's active time dimension ('…') attributable from …, but it ... |
| elaborate_env.py | check_association_windowed_ranked | SlayerError | checker | association | yes | checker | 1 |  | Aggregate … needs distinct-entity association over an unattributable dimension, which is unsupported in com... |
| elaborate_env.py | check_association_root_unique_key | SlayerError | checker | association | yes | checker | 1 |  | Aggregate … needs distinct-entity association, but its root model … declares no primary or unique key to de... |
| elaborate_env.py | check_association_column_param | SlayerError | checker | association | yes | checker | 1 |  | Aggregate … needs distinct-entity association over an unattributable dimension, which is unsupported with a... |
| elaborate_env.py | check_reaggregation_no_window | SlayerError | checker | reaggregation | yes | checker | 1 |  | Re-aggregation … cannot carry window= on its outer aggregation; apply the window inside the operand or cons... |
| elaborate_env.py | check_reaggregation_no_column_param | SlayerError | checker | reaggregation | yes | checker | 1 |  | Re-aggregation … carries a column-reference parameter on its outer aggregation (e.g. weighted_avg(weight=…)... |
| elaborate_env.py | check_reaggregation_partition_key_is_query_dim | ValueError | checker | reaggregation | yes | checker | 1 |  | Re-aggregation … declares partition_by=…, which is not a query dimension; every explicit partition key must... |
| elaborate_env.py | check_reaggregation_dims_attributable | ValueError | checker | reaggregation | yes | checker | 1 |  | Re-aggregation … cannot attribute dimension(s) … to the operand dataset under to_many_handling='error'; add... |
| compile/stages.py | _assert_total_routing | ValueError | internal | internal | no | compiler | 1 |  | Aggregate … in a … received no routing disposition (inline, producer substitution, or explicit rejection) —... |
| compile/stages.py | _plan_regroups | ValueError | checker | regroup-roots | yes | checker | 1 |  | Column(s) … use the reserved '__regroup__' prefix, which collides with the regroup primitive's placeholders... |
| compile/stages.py | plan_query | DistinctDimensionValuesError | checker | positions | yes | checker | 1 |  | distinct_dimension_values=False rejects measure references, but this query references the aggregation … in ... |
| compile/stages.py | plan_query | RuntimeError | internal | internal | no | compiler | 1 |  | Cross-model aggregate slot … survived the regroup desugar (DEV-1838 D8); every cross-model aggregate must b... |
| compile/stages.py | plan_query | PositionTypingError | checker | positions | yes | checker | 1 |  | ORDER BY expression is not supported: … has no materialisable slot. Order by an aggregate, a transform, a c... |
| elaborate_env.py | check_opaque_grouping_dim | ValueError | checker | regroup-roots | yes | checker | 1 |  | Column '…' cannot be used as a dimension: its type does not support the GROUP BY / DISTINCT this query requ... |
| compile/stages.py | _reject_computed_dim_name_collision | ValueError | checker | names | yes | checker | 1 |  | Computed dimension name … collides with an existing column or measure on model …. Choose a different name. |
| compile/stages.py | _reject_computed_dim_name_collision | ValueError | checker | names | yes | checker | 1 |  | Computed dimension name … collides with a query measure of the same name. Choose a different name. |
| elaborate_env.py | check_computed_dimension | NotImplementedError | checker | regroup-roots | yes | checker | 1 | yes | A transform inside computed dimension … must wrap an explicitly-grained aggregate — declare partition_by= o... |
| elaborate_env.py | check_computed_dimension | DistinctDimensionValuesError | checker | positions | yes | checker | 1 |  | Computed dimension … references an aggregate, so it cannot be used with distinct_dimension_values=False (ra... |
| elaborate_env.py | check_computed_dimension | ValueError | checker | regroup-roots | yes | checker | 1 |  | The aggregate inside computed dimension … must declare the grain it aggregates over with partition_by=, e.g... |
| compile/stages.py | _guard_flatten | ValueError | checker | names | yes | checker | 1 |  |  |
| compile/stages.py | _declared_measures_from_query | ValueError | checker | names | yes | checker | 1 |  | Measures … and … both derive the result key … but compute different values; rename one (set 'name') to disa... |
| compile/stages.py | _declared_measures_from_query | ValueError | checker | names | yes | checker | 1 |  | Measures … and … merge into one result column … but declare different label/type; rename one (set 'name') t... |
| compile/stages.py | _topo_sort | ValueError | compiler | stages | yes | compiler | 1 |  | Duplicate stage names in source_queries DAG: … |
| compile/stages.py | _topo_sort | ValueError | compiler | stages | yes | compiler | 1 |  | Cycle detected in source_queries DAG involving stages: … |
| compile/stages.py | _emit_stage_schema | ValueError | checker | names | yes | checker | 1 |  |  |
| compile/stages.py | _validate_model_filter | ValueError | checker | model-filters | yes | checker | 1 |  | Model filter … references measure …. Model filters can only reference table columns (WHERE). Use query-leve... |
| compile/stages.py | _validate_model_filter | ValueError | checker | model-filters | yes | checker | 1 |  | Model filter … references column … whose SQL contains a window function. Factor it into a multi-stage sourc... |
| compile/stages.py | _build_date_range_filter | ValueError | internal | internal | no | compiler | 1 |  | date_range filter for TimeDimension … expected a column reference; got …. |
| compile/stages.py | _resolve_main_time_dimension | AmbiguousReferenceError | bind | time-axis | yes | bind | 1 |  |  |
| compile/stages.py | _resolve_main_time_dimension | UnknownReferenceError | bind | time-axis | yes | bind | 1 |  |  |
| compile/regroup.py | type_position_conjunct | PositionTypingError | checker | positions | yes | checker | 1 |  | This … expression is valid as neither a field nor a measure. Field typing failed: it references …, availabl... |
| compile/regroup.py | type_position_conjunct | PositionTypingError | checker | positions | yes | checker | 1 |  | This … expression references …, so it is not a field, and measure typing is unavailable because the query h... |
| compile/projection.py | _validate_alias_collisions | MeasureNameCollidesWithColumnError | checker | names | yes | checker | 1 |  |  |
| compile/projection.py | _validate_alias_collisions | CanonicalAliasShadowsColumnError | checker | names | yes | checker | 1 |  |  |
| compile/projection.py | intern | DuplicateMeasureNameError | checker | names | yes | checker | 1 |  |  |
| compile/projection.py | _merge_into_existing | DuplicateMeasureNameError | checker | names | yes | checker | 1 |  |  |
| compile/projection.py | desugar_change | ValueError | internal | internal | no | compiler | 1 |  | desugar_change expected op='change', got …. |
| compile/projection.py | desugar_change_pct | ValueError | internal | internal | no | compiler | 1 |  | desugar_change_pct expected op='change_pct', got …. |
| compile/projection.py | _iter_slot_deps | TypeError | internal | internal | no | compiler | 1 |  | _iter_slot_deps has no case for …: classify the kind explicitly (slot-worthy, inlined composite, or never s... |
