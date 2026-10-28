## Context

See proposal.md for motivation. Every column-only position today is a separate `isinstance(…, ColumnKey | ColumnSqlKey)` gate: `TimeTruncKey.column` is typed as a column, and `binding.py` refuses expressions in `_bind_granularity_call`, `_bind_partition_keys`, `_fold_positional_agg_args` (ranking key), `_bind_agg` (first/last expression source) and `_bind_agg_arg` (parameters); `ranked_planner.explicit_ranking_time_arg` and the ranked kernel's renderers (`generator.py` `_ranked_value_expr`, `_ranked_scope_expr`) assume a column. `TimeDimension.dimension` is a `ColumnRef`, read by its `full_name` / `name` / `model` in about fifteen places. The dependency closure (`reference_closure.key_closure`), structural key equality and checker typing are already generic over key trees. This change stacks on DEV-2101 (`parse_expr` in `slayer/core/syntax.py`, `CastKey`, migration steps) and is implemented after it merges.

## Goals / Non-Goals

**Goals:** remove every column-only gate so each position reasons through the existing generic machinery; one representation of a time dimension's dimension.

**Non-Goals:** deleting `ModelExtension.columns` or query-time Mode A (DEV-2021); new scalar functions (DEV-2120); a dataset axis over an expression; aggregate-bearing inline partition keys that are not a query dimension.

## Decisions

### D1. Positions

All six column positions take a row-level expression: the time dimension (with the functional `gran(expr)` form in `dimensions`, `time_dimensions` and `order`), a granularity call's argument anywhere, `partition_by=` keys (aggregations and the rank family), the `first` / `last` ranking key, the `first` / `last` value (mixed sources included: picked at the ranked row, never the transform dispatch, which stays for a wholly attached operand), and column-valued aggregation parameters. Each former gate asks only whether the bound value is row-level (phase ROW, no aggregate / transform / attached value); anything else is a typed error naming the position. Alternative rejected (user ruling): only the three positions the issue lists — DEV-2021 would then lose `weight=` / `first` / `last` extension-column capability.

### D2. One bucket key

`TimeTruncKey.column` becomes `source: ValueKey` (any row-level key), renamed with `la:deterministic-refactor`; `column_leaf` / `column_path` callers move to the key's references. No new key kind (system P13). `explicit_ranking_time_arg` is kept, renamed `explicit_ranking_key`, returning any explicit row-level ranking argument, because `stages.py` `_assert_local_producer_inputs_safe` and `_checked_ranking_time_key` enforce Axiom 2.4 on it by closure for local and target-rooted producers. `_ranked_value_expr` and `_ranked_scope_expr` render through the row-expression renderer, registering every joined path of the expression's closure; a mixed source gets its row-attach substitution first. Truncation (`_raw_time_col_expr_for_planned`, `value_expr.py`) renders any row-level source.

### D3. `TimeDimension.dimension` is Mode-B text

Inputs coerce to text: a string, the `column` alias, the legacy `{"name", "model"}` object (→ `model.name`), and `gran(expr)` in `dimensions` / `time_dimensions` (the coercion parses with `parse_expr` and keeps the call's argument text). Dumps and the JSON schema use a string. Consumers of `full_name` / `name` / `model` (`unrelated.py` first hops, `model_reads.py` dimension texts, `schema_drift.py`, `population.py`, `time_spine.names_spine` / `is_spine_query`, `_dedupe_time_dimensions`, `_strip_time_dimensions`, `_merge_time_dimensions`, `ir/variables.py`, `facade/translator.py`, `rank_direction_migration.py`, `legacy_time_literals.py`) read the parsed expression's references (the walk computed dimensions use); the construction-time identity is the dimension text, the bind-time backstop the bound bucket. Alternative rejected (user ruling): a `ColumnRef | str` union — two shapes for one meaning, every consumer branching.

### D4. Names

`TimeDimension.name: str | None`. Unnamed column: today's result key. Unnamed expression: `auto_name_from_expression(text)`, result key model-prefixed, granularity suffix iff another projected time dimension shares the bound source. Named: the result key, never suffixed. Names are unique across dimensions and time dimensions (construction error). `main_time_dimension` matches a name, then (column time dimensions) full / leaf name; `partition_by=` resolves a time dimension's name through the same alias map computed dimensions use.

### D5. Typing

The checker types the expression (`temporal_type`, `CastKey.data_type`); not DATE / TIMESTAMP → `TimeDimensionColumnError` with the observed type; undetermined → the same error with the `CAST` remedy. Binding records each time dimension's checked type in a source → type map that `whole_periods_only` snapping (`_snap_whole_periods`) and `date_range` bound shaping read, instead of re-inferring through the column-type callback.

### D6. Per-row consumers

`date_range` builds its `TimePointCmpKey` on the bound source (no column assert); frame bounds (`time_bounds.is_frame_bound`, `stages.py` `_frame_bound_columns`) match by key identity on the source; `window=` kernels, time-ordered transforms (`generator.py` shift / `consecutive_periods` / `_in_transform_grain`) and the `first` / `last` collapse use the bucket key unchanged. At a stage boundary an expression time dimension emits a `StageColumn` carrying its checked temporal type and bucket granularity and no single-column provenance (label, format, respelling, default-axis); downstream it is a one-leaf column (`stages.py` `_source_column`, `_value_anchor_path`, `_leaf_variants`, `_surviving_default` return their no-column answer for it).

### D7. Re-bucketing per bucketed leaf

`check_time_dimension_column`'s re-bucketing branch runs once per bucketed temporal leaf of the source (a column carrying `Column.granularity`, a stage column with an upstream bucket, a sibling stand-in), naming that leaf. Alternative rejected (user ruling): an expression carries no granularity — silent fake precision, and wrapping a column in `CAST` would bypass the rule.

### D8. Default ranking key

`resolve_ranking_time_key` step 1 admits the first `dimensions` entry that is row-level and checker-typed DATE / TIMESTAMP (column or computed dimension), never a granularity bucket; step 2 reads the first time dimension's `source`. User ruling over keeping step 1 column-only: that would move the result change onto DEV-2021's migration path. Release note.

### D9. Partition keys

`_bind_partition_keys` accepts any row-level key; a key structurally equal to a query computed dimension or time-dimension bucket is that dimension (`pk in classes.dim_keys`, `dim_alias_map`), attach-carrying ones included; other aggregate-bearing keys are a typed error with the computed-dimension remedy. A raw key equal to a time dimension's source maps to its bucket (`_td_by_source`), keeping `_td_ambiguous_sources` for sources at several granularities. Attributability and input safety by closure (Axiom 8).

### D10. Spine

`is_spine_query` / `names_spine` read parsed references. A time dimension whose source reads `time_spine.timestamp` other than as the bare column is a typed error (`check_spine_plain_use` family); any other expression time dimension is an ordinary dimension of P. Alternative rejected (user ruling): rejecting every expression time dimension in a spine query — the same query with a fact column is legal.

### D11. Remedies

`binding.py`'s partition-key remedy and `slayer/memories/help_content/02_workflow.md`'s "inline extension column" advice point at the expression form. `SlayerModel`'s query-backed-measures remedy (a `ModelExtension` measure) stays: DEV-2021 removes only extension columns.

### Approved harness edits (exact text, approved 2026-10-21)

H1 `architecture/semantics.arc42.md`, end of Axiom 13 (before its tags): "A column reference is the one-leaf row-level expression: every position that takes a column takes any row-level expression of its type, homed by Axiom 2.2."

H2 `architecture/semantics.arc42.md`, Axiom 15, after "joins it to-one;": "an expression is never an axis, so a spine dimension over one is a type error;"

Each lands with an `enforced: test:…` tag naming this change's test file for it.

## Risks / Trade-offs

- [The `TimeTruncKey` widening touches every bucket consumer] → deterministic rename first, then the full suite; golden baselines must stay byte-identical for every pre-existing shape.
- [D8 changes `first` / `last` results for queries with a temporal computed dimension and no explicit ranking key] → release note; tests pin the new precedence.
- [D3 breaks Python attribute access and the object dump] → release note; the object form stays accepted on input; stored data migrates.
- [Expression `date_range` predicates are not sargable] → accepted: the author chose the expression.
- [DEV-2101 lands first] → `pr-implement` starts only once DEV-2101 is merged into this branch; tests needing `CAST` fail until then.

## Migration Plan

A stored-only step (SlayerQuery, SlayerModel `source_queries`, Memory; version numbers after DEV-2101's) rewrites object-form time dimensions to dotted text, recursing like `rank_direction_migration.py`. Rollback: an older SLayer cannot read the text form, as for any forward-written store.
