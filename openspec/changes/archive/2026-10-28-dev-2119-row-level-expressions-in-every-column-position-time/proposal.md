## Why

Several query positions take only a column reference — a time dimension, a granularity call's argument, `partition_by=` keys, the `first` / `last` ranking key and value, column-valued aggregation parameters — so an agent must declare a query-time Mode-A extension column to bucket, partition, rank or weight by a computed value. DEV-2021 deletes `ModelExtension.columns`; this change makes Mode-B row-level expressions legal in every column position first, so that deletion loses no capability.

## What Changes

- Rule: wherever a query takes a column reference it takes a row-level Mode-B expression of the same type, homed by semantics Axiom 2.2; a column is the one-leaf case. No new key kind: the time bucket stays one key whose source is any row-level value.
- `TimeDimension.dimension` accepts an expression (`coalesce(shipped_at, created_at)`, `CAST(created_txt AS DATE)`), also through the functional `gran(expr)` form in `dimensions`, `time_dimensions` and `order`. The expression must type DATE / TIMESTAMP (unknown fails closed); `date_range`, `whole_periods_only`, `window=`, the time-ordered transforms and the `first` / `last` collapse read its per-row value. Re-bucketing is judged per bucketed leaf.
- `TimeDimension` gains an optional `name` (default: the sanitised expression text; a column keeps today's result key); `main_time_dimension` and `partition_by=` may name a time dimension by it.
- A granularity call takes any row-level argument in every position (`month(coalesce(a, b))` in a filter, dimension or order key).
- `partition_by=` keys (aggregations and the rank family) accept row-level expressions; a key structurally equal to a query computed dimension or time-dimension bucket is that dimension.
- `first` / `last` accept an expression ranking key and an expression value (mixed sources included, picked at the ranked row); the default ranking key's "first DATE / TIMESTAMP dimension" step admits temporal expression dimensions (a result change for queries with a temporal computed dimension and no explicit ranking key).
- Column-valued aggregation parameters (`weight=`, `other=`, custom-aggregation column parameters) accept row-level expressions.
- A time dimension whose expression reads `time_spine.timestamp` other than as the bare column is a type error: an expression is never an axis.
- Error remedies pointing at an inline `ModelExtension` column point at the expression form.
- **BREAKING** `TimeDimension.dimension` is Mode-B text: it serialises as a string (formerly a `{"name", "model"}` object) and the schema advertises a string; the object form is still accepted on input and stored documents are migrated. Python callers reading `td.dimension.name` / `.model` break.

## Capabilities

### New Capabilities
<!-- none -->

### Modified Capabilities
- `queries/time-dimensions`: the dimension is an expression (text form, functional `gran(expr)`, granularity error surface, result keys and `name`, `main_time_dimension` by name, functional order key); expression time dimensions behave as a column in every time position; per-leaf re-bucketing.
- `queries/time-points`: a granularity call takes any row-level argument.
- `queries/time-spine`: a spine dimension over an expression is a type error.
- `queries/partitioned-aggregates`: expression partition keys; a mixed source under `first` / `last` picks at the ranked row.
- `aggregations/functional-form`: `first` / `last` over an expression value and ranking key; the default ranking key precedence.
- `aggregations/expression-aggregation`: `first` / `last` over expression sources and expression-valued parameters are accepted.
- `models/stored-upgrade`: stored time dimensions migrate from the object form to text.

## Impact

- Code: `slayer/core/keys.py` (`TimeTruncKey.column` → `source`, any row-level key), `slayer/core/query.py` (`TimeDimension.dimension` text, `name`, functional-form coercion, dedupe / strip / merge), `slayer/core/time_spine.py`, `slayer/engine/binding.py`, `bind_inputs.py`, `elaborate_env.py`, `ranked_planner.py`, `population.py`, `unrelated.py`, `model_reads.py`, `schema_drift.py`, `compile/stages.py`, `compile/spine.py`, `slayer/sql/generator.py`, `slayer/sql/render/value_expr.py`, `slayer/sql/naming.py`, `slayer/core/time_bounds.py`, `slayer/ir/variables.py`, `slayer/facade/translator.py`, `slayer/storage/migrations.py` + a new migration module, `slayer/storage/rank_direction_migration.py`, `slayer/storage/legacy_time_literals.py`.
- Depends on DEV-2101 (grammar, `CastKey`, migration steps it adds); implemented after it merges.
- Architecture: `semantics.arc42.md` Axioms 13 and 15 (approved text in design.md).
- Docs and agent surfaces: `docs/concepts/queries.md`, `formulas.md`, `time.md`, MCP `Field` descriptions, `slayer/memories/help_content/*`.
