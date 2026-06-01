# SQL generation

**Modules:** `slayer/sql/generator.py` (the planned-consuming path),
`slayer/engine/response_meta.py` (response metadata)

The generator renders a `PlannedQuery` (or a list of them) to a SQL string. It
preserves the result-key contract exactly (**P10**) and emits SQL via sqlglot
AST building, not string concatenation.

## Entry points

```mermaid
flowchart TB
    gps["generate_planned_stages(planned_list, bundle, dialect)"]
    gps -->|single stage| gfp["generate_from_planned(planned, bundle, dialect)"]
    gps -->|multi-stage| loop["render each stage → CTE; root = outer SELECT"]
    loop --> gfp
    gfp --> inst["SQLGenerator(dialect).generate_from_planned"]
    inst -->|cross-model| cm["_render_with_cross_model_plans"]
    inst -->|transforms| tl["WITH base, step CTEs, outer wrap"]
    inst -->|plain| base["single SELECT"]
```

- `generate_from_planned(planned_query, *, bundle, dialect)` — module-level
  entry that constructs an `SQLGenerator` and delegates to the instance method.
  Renders **one** stage.
- `generate_planned_stages(planned_queries, *, bundle, dialect)` — renders a
  multi-stage DAG to one SQL string. Each non-root stage becomes a CTE; the root
  is the outer SELECT.

## `generate_from_planned` (instance method)

Reads from typed `PlannedQuery` fields (`row_slots` / `aggregate_slots` /
`filters_by_phase` / `order` / `transform_layers`) and dispatches:

- `cross_model_aggregate_plans` non-empty → `_render_with_cross_model_plans`;
- `transform_layers` present → `WITH base AS (...)`, Kahn-batched step CTEs
  carrying the window functions, an outer wrap projecting in user-spec order;
  POST-phase filters that reference transform slots wrap as `SELECT * FROM (...)
  AS _filtered WHERE …`; `time_shift` / `consecutive_periods` emit dedicated
  self-join CTE pairs;
- otherwise → a single base SELECT with WHERE/HAVING, GROUP BY, ORDER BY, LIMIT.
  When the base CTE materialises any hidden aggregate (an aggregate referenced
  ONLY by ORDER BY or a filter, never declared as a measure), a conditional
  outer-trim wrapper projects exactly the public projection — same shape as the
  transform path's outer wrap, minus the step CTEs — so the hidden alias does
  not leak into the result columns (DEV-1501).

It builds its own `slot_id_by_key` map (the `PlannedQuery` doesn't carry the
registry), materializes hidden aux slots referenced as transform inputs /
partition keys / time keys / POST-filter operands, and renders.

### The synthetic-`EnrichedMeasure` adapter (deviation)

To render aggregations identically to legacy across all dialects, the new path
**reuses the legacy dialect helpers** (`_build_agg`, `_build_percentile`,
`_build_stat_agg`, `_wrap_cast_for_type`, `_resolve_sql`, `_build_date_trunc`).
It does so by synthesizing `EnrichedMeasure` objects from planned slots
(`_synthesize_enriched_measure_from_planned`) and feeding them to those helpers.

This is a real coupling: `generate_from_planned` consumes `PlannedQuery` at the
top but adapts back to `EnrichedMeasure` — a type DEV-1452 wants to delete — to
emit aggregate SQL. The plan said "rewrite `generator.py` to consume
`PlannedQuery`"; the implemented path is a hybrid. It is flagged in
[the deviations list](index.md#deviations-from-the-plan). The upside is that
dialect-specific behavior (SQLite UDFs, ClickHouse `quantile`, the MySQL
`median` `NotImplementedError`, etc.) is rendered by exactly one code path,
shared with legacy — so the two pipelines can't drift on dialect SQL while both
exist.

## Multi-stage chaining (`generate_planned_stages`)

Each non-root stage renders independently (against a per-stage bundle from
`_bundle_for_stage`) and is wrapped by `_stage_rename_wrapper` so its output
columns become the flat names downstream stages bound against
(`orders.customers.region` → `customers__region`). The wrapper derives those from
the *actual* rendered `named_selects` (robust to the cross-model renderer
emitting columns out of `public_projection` order) and asserts they match the
stage's `StageSchema` — a planner/generator divergence fails here rather than as
a confusing downstream bind miss. Stage CTEs are prepended before any CTEs the
root already emits (the root reads `FROM <stage>`).

`_bundle_for_stage` picks the host model the stage renders against from the
planner's `render_source_model` (the stage's own source / overlay /
synthetic-over-sibling), falling back to a synthetic model over the upstream CTE
for a `StageSchema` chain stage — so the generator's FROM/joins bind against
exactly what the binder used.

## Cross-model rendering

`_render_with_cross_model_plans` emits one `_cm_*` CTE per
`CrossModelAggregatePlan` joined back to the host base. When `plan.rerooted_plan`
is set, `_render_rerooted_cross_model_cte` renders the nested re-rooted plan
(FROM target + the target's joins) preserving host grain; otherwise the
forward-path CTE renders (FROM bare target, grouped at the forward dims).
`Column.filter` on the aggregated column renders as
`SUM(CASE WHEN <filter> THEN <col> END)`. See
[Cross-model aggregates](cross-model-aggregates.md).

## Mode-A filter inlining and join discovery (DEV-1494)

A column-level `Column.filter` on an aggregated measure becomes a CASE-WHEN
wrapper (`SUM(CASE WHEN <filter> THEN <col> END)`), and a `SlayerModel.filters`
entry becomes a WHERE term. Both are Mode-A SQL and share one renderer,
`_render_mode_a_predicate`, which inline-expands references to derived columns —
bare (`is_eu` → its `CASE WHEN customers.region …`) or dotted to a derived
column on a joined model (`loss_payment.has_flag` → its `sql`) — so the emitted
predicate is runnable and never references a non-physical `<alias>.<derived_col>`.
A predicate with only base refs takes the cheap qualify path
(`_qualify_mode_a_sql_filter` regex for model filters, `_qualify_column_filter_sql`
AST for column filters), byte-identical to before. On sqlglot parse failure the
predicate falls through to the qualify path unchanged.

Join discovery for these text filters (`_filter_join_paths`) is the **union** of
the join paths in the **un-inlined** predicate and those in the **inline-expanded**
predicate. Both are needed: the dbt placeholder-join idiom — a constant derived
column such as `has_flag sql="1"` whose only purpose is to force the (inner)
join — keeps its alias only in the un-inlined form (it inlines to the constant
`(1)`), while a derived ref's *crossed* joins (`is_eu` → `customers`;
`loss_payment.deep_flag` → `loss_payment__claim`) appear only after expansion.
Discovery for column filters in the base SELECT is restricted to **local**
aggregate sources (empty `AggregateKey.path`); a cross-model aggregate's filter
joins are discovered inside its `_cm_*` CTE instead — `_render_cross_model_cte`
collects the join paths of the target measure's `Column.filter` and the
target-model filters and adds them to the CTE's own FROM. Because each `_cm_*`
CTE is an isolated per-(target, grain) computation, adding the join resolves the
filter's refs without affecting sibling measures. Discovery is root-scope-only,
so a correlated ref inside an `EXISTS (...)` subquery does not pull an outer join.

## Result-key contract (P10)

The generator preserves the result keys byte-for-byte: `orders.revenue_sum`,
`orders._count` (the `*` dropped, the leading `_` kept), joined dimensions as the
full dotted path `orders.customers.regions.name`, and renamed measures as
`orders.<user_name>`. `_full_alias_for_slot` derives these from the slot's key /
public aliases. Two documented exceptions, both routed through the same
`canonical_agg_name` helper: cross-model parametric aggregates carry the kwarg
suffix legacy dropped, and hidden parametric `first`/`last` (DEV-1501) carry the
explicit time-arg suffix so distinct time-column specs get distinct
materialised aliases (`orders.revenue_last_created_at`,
`orders.revenue_last_updated_at`).

## Response metadata (`response_meta.py`)

The legacy engine derived `SlayerResponse.attributes` and `expected_columns` from
an `EnrichedQuery`. The typed pipeline has none, so `build_response_metadata`
rebuilds both from the root `PlannedQuery` plus the rendered SQL:

- **`expected_columns`** comes from the final SQL's `named_selects` — the literal
  result-key columns rows come back under. Reading them from the SQL (rather than
  re-deriving from slots) is bulletproof: it is exactly the outer SELECT the
  generator emitted.
- **`attributes`** (`ResponseAttributes.dimensions` / `.measures`) come from the
  root plan's public `ValueSlot`s, classified dimension (ROW phase) vs measure
  (everything else), with each public result key mapped to its
  `FieldMetadata(label, format)`. `_slot_result_keys` mirrors
  `_full_alias_for_slot` so the keys line up with the rendered projection; only
  keys actually present in the rendered SQL are surfaced (a guard against
  divergence). Aggregate formats come from `_infer_aggregated_format` (INTEGER
  for count/star, FLOAT for avg-family, source-column format for sum/min/max).

`FieldMetadata` / `ResponseAttributes` / `_infer_aggregated_format` live here (not
in `query_engine`) so the module imports nothing from the engine;
`query_engine` re-exports them, keeping the public import path unchanged.

## Design rationale

- **Why reuse legacy dialect helpers instead of reimplementing aggregation SQL?**
  Dialect coverage (SQLite UDFs, ClickHouse parametric quantiles, MySQL's
  unsupported-function `NotImplementedError`, the `log10`/`log2` literal
  preservation, JSON-extract rewriting) is large and well-tested. Sharing one
  emitter keeps the two pipelines from drifting on dialect SQL while both exist —
  at the cost of the `EnrichedMeasure` coupling, which DEV-1452 removes.
- **Why derive `expected_columns` from the SQL?** Because the SQL is the ground
  truth for what rows come back keyed by. Re-deriving from slots risks a subtle
  mismatch; reading `named_selects` cannot.
- **Why assert in `_stage_rename_wrapper`?** A leaked hidden column or a C13
  over-projection would otherwise surface as a downstream "column not found"
  deep in the next stage's binding. Asserting at the boundary turns a confusing
  failure into a precise one.
