## Why

SLayer has no calendar of its own (DEV-2015, from the DEV-1998 comparison), so three things trail MetricFlow: measures from two fact tables cannot share one time axis (one fact's time dimension broadcasts the other's total and drops the other's months), empty periods never come back as rows, and there are no custom calendars (fiscal year, 15-minute buckets). A hand-made `calendar` model that each fact joins by date already gives exact, filled results — the semantics exist, the calendar and its wiring do not.

## What Changes

- **Built-in time spine.** Every datasource has a virtual model `time_spine` with one `time` column `timestamp` (logically every instant). A model's **axis** — its declared `default_time_dimension`, else its sole `time`/`date` column (propagated or sole temporal column for query-backed models and stages) — joins it to-one by equality.
- **Spine routing.** A dataset reaches the spine by its nearest axis on a to-one chain (equal-length ties fail loudly); a join path may begin or end at the spine but never cross it. Ordinary routing is unchanged.
- **Product population.** A query with a spine time dimension has population `time_spine × P`, P inferred as today from its other dimensions and non-spine field filters (or named by `source_model`; the unit when there is nothing to infer from). Every bucket in range comes back for every P combination; each fact's measure is attributed through its own axis with no broadcast; empty cells take the empty value (count 0, else NULL). Gap filling is exactly this — no `fill` flag, no fill-value parameter (`coalesce(agg, v)` sets a value).
- **Bounds.** A spine query must bound the spine from below (any DEV-1999 frame bound); unbounded above, it is bounded by the current bucket of its finest spine granularity (`this <granularity>`). Bounds decide which buckets exist (interval overlap), restrict fact rows through their axis, and stay frame bounds for windows and `time_shift`.
- **Fail closed:** an aggregation homed on the spine, the spine column as a plain dimension or in raw-rows mode, a filter on it that is not a bound, a reserved-name clash with a stored model named `time_spine`.
- **Custom granularities.** `DatasourceConfig.granularities` defines named granularities `{name, base, multiple, origin}` (bucket boundaries `origin + k·multiple·base`). They are full granularities: time dimensions (spine or fact column), the functional form `fiscal_year(col)`, `time_shift` units, calendar stepping, `Column.granularity`, nesting/re-bucketing, and (after DEV-1999) `whole_periods_only` snapping and relative tokens.
- **BREAKING (error timing):** an unknown single-column dimension call (`mnth(created_at)`) and a string `time_dimensions` entry with an unknown callee now fail at binding (where the datasource's granularities are known) rather than at query construction; the error lists built-in and datasource granularities.
- **Fix:** a cross-model trailing-window aggregate is evaluated at every population cell — today it is NULL at a cell whose own bucket has no home rows even when its trailing interval does.
- **Importer:** dbt `fill_nulls_with: v` becomes `coalesce(<agg>, v)`; `join_to_timespine` is accepted with an ingest-report note instead of failing.
- **Surfaces:** `time_spine` in model listings, `inspect`, and search; `granularities` on datasource create/edit (MCP, REST, CLI) and datasource inspect.

## Capabilities

### New Capabilities
- `queries/time-spine`: the spine model, axes and wiring, spine routing, the product population, bounds and bucket existence, attribution and empty values, fail-closed shapes, reserved name, RLS/cache behaviour, discovery.
- `queries/custom-granularities`: definition, validation and persistence of datasource granularities, bucketing, stepping, nesting, and every site accepting a granularity.

### Modified Capabilities
- `queries/population`: spine dimensions factor out of inference; nothing-to-infer exemption for spine-only queries; reporting a product population.
- `queries/time-dimensions`: functional form and string entries accept datasource granularities, resolved at binding; unknown-callee errors move to binding.
- `models/column-granularity`: `Column.granularity` accepts a datasource granularity name.
- `queries/transforms`: `time_shift`, `change` / `change_pct` and `consecutive_periods` step by custom granularities.
- `aggregations/trailing-window`: a windowed aggregate is evaluated at every population cell, including cells whose own bucket has no home rows.

## Impact

- `slayer/core`: open granularity type (built-in or datasource reference), `CustomGranularity` model, `DatasourceConfig.granularities`, effective default time dimension, functional-form recognition moved out of construction.
- `slayer/engine`: bundle builds the spine and its edges and resolves granularities; spine routing; product population inference; checker errors; bound extraction; windowed-producer endpoints.
- `slayer/ir`: planned representation of the spine factor and product population.
- `slayer/sql`: integer-sequence dialect hook (all Tier-1 dialects), bucket-series + distinct-P emission, custom-grain bucketing via `date_diff` / `date_add`, windowed-producer fix.
- `slayer/storage`: persists datasource granularities (optional field, no migration).
- Surfaces (`slayer/mcp`, `slayer/api`, `slayer/cli`, `slayer/inspect`, `slayer/search`), `slayer/dbt` importer, agent help content.
- `architecture/semantics.arc42.md`: new Axiom 15 (time spine), Axiom 12 cross-reference.
- Docs: `docs/concepts/time.md` (from DEV-1999), `queries.md`, `models.md`, datasource configuration.
- Depends on DEV-1999 (time points, frame bounds, engine clock) being merged first.
