## Why

SLayer 2.0 makes Mode B compliant with Ossie's portable expression language (`core-spec/expression_language.md`) wherever Ossie defines a meaning. After the SQL grammar landed, the remaining gaps are breaking: date-part functions mean buckets instead of extraction, names resolve case-sensitively, `AGG` is not reserved, `=` inside aggregate calls is read as a keyword argument, `first`/`last` follow each database's NULL order, and Python-style spellings are still first-class.

## What Changes

- **BREAKING** `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute` and `second` mean integer extraction (`date_part`). A bucket is spelled only `date_trunc('<unit>', x)`, including `week_sunday` and custom granularities, which lose their call form. Typed errors carrying the `date_trunc` remedy appear wherever a bucket is required, and when an extraction is compared with a date-shaped literal. Stored bucket calls are converted on load.
- **BREAKING** The SQL API's `MONTH(col)` and its siblings mean extraction. `EXTRACT` and `date_part` become projectable in SELECT.
- **BREAKING** Unquoted identifiers and function names resolve case-insensitively; quoted names match exactly. A save that introduces a case-insensitive collision is refused, and stored collisions are grandfathered with a warning. Fresh queries whose result names collide case-insensitively are refused (DEV-1731).
- **BREAKING** `agg` is a reserved function name. Calls are refused, new custom aggregations named `agg` are refused, and stored ones are renamed to `agg_aggregation` on load.
- **BREAKING** Inside an aggregation or transform call, `name = value` is a keyword argument only when `name` is a declared parameter of the callee; otherwise it is a comparison.
- **BREAKING** `first` / `last` never pick a row whose ranking key is NULL, on every dialect. A cell whose keys are all NULL yields NULL. The rank family is unchanged.
- `==`, `is None` / `is not None`, `[...]` after `IN`, bare `None` and the method-call form `path.agg(src)` are deprecated until 3.0. They are converted on fresh input with a `FutureWarning` plus a structured warning, stored as their canonical twin on save and on load, undocumented, and tagged for the 3.0 tripwire. The colon spellings are removed by DEV-1920, not here.
- Two architecture edits: engine P8 (a deprecated spelling is stored as its canonical twin) and storage P3.2 (only a new library *function* yields to a model's own definition).

## Capabilities

### New Capabilities

- `queries/identifier-case`: case-insensitive resolution of unquoted names and exact matching of quoted names in every namespace, ambiguity, save-time collision refusal and grandfathering, result-name collisions, and ingestion of physical columns that differ only in case.

### Modified Capabilities

- `queries/date-functions`: the eight names become extraction; `date_trunc` is the only bucket spelling; granularity names have no call form; the date-shaped-literal error.
- `queries/time-points`: the bucket row value is `date_trunc`; `whole_periods_only` with only extraction dimensions fails with the remedy.
- `queries/time-dimensions`: the functional form is `date_trunc`; `time_dimensions` string entries must be a `date_trunc` call.
- `queries/time-spine`, `queries/transforms`: the `date_trunc` remedy where a bucket is required; re-spelled.
- `queries/custom-granularities`: custom granularities are `date_trunc` units with no call form.
- `queries/expression-grammar`: legacy spellings are deprecated; keyword argument vs comparison.
- `queries/expression-keywords`: CASE lowering emits canonical spellings.
- `aggregations/functional-form`: the method-call form is deprecated; NULL ranking keys of `first`/`last` are never picked.
- `aggregations/trailing-window`: windowed `first`/`last` follow the same NULL-key rule.
- `models/aggregation-names`: `agg` is reserved.
- `models/stored-upgrade`: stored bucket calls, legacy spellings and `agg` aggregations are upgraded on load; library growth distinguishes functions from other reserved names.
- `facade/projection`, `facade/time-filter-translation`: extraction projections; re-spelled.
- `compatibility/removal-tripwire`: the deprecated Mode-B spellings are marked for removal in 3.0.
- Re-spelled with no rule change (bucket calls and legacy spellings shown as current syntax): `queries/mixed-type-coercion`, `queries/result-order`, `queries/partitioned-aggregates`, `queries/dotted-dimension-routing`, `queries/saved-query-refinement`, `queries/variables`, `queries/positions`, `queries/cross-model-aggregates`, `queries/semantics`, `queries/computed-dimensions`, `aggregations/expression-aggregation`, `aggregations/formula-templates`, `models/fk-ingestion`, `models/model-access`, `mcp/tool-surface`, `mcp/response-row-cap`, `demo/jaffle-shop`, `usage-reporting/collection`.

## Impact

- Code:
  - `slayer/core/syntax.py`: the eight names become extraction aliases; a dedicated `date_trunc` node; neutral `=` arguments; per-segment quoting; legacy-span recording and the deprecation sink.
  - `slayer/core/query.py`: the functional time-dimension rewrite becomes `date_trunc`-only.
  - `slayer/engine/binding.py`, `bind_inputs.py`, `elaborate_env.py`: bucket-required remedies, `=` classification, case-insensitive lookups through one name matcher.
  - `slayer/core/models.py`: save-time collision checks; the `agg` reservation.
  - `slayer/sql/generator.py` and `slayer/sql/render/ranked.py`: the `first`/`last` NULL-key shape.
  - `slayer/facade/translator.py`: extraction projections.
  - `slayer/storage/`: a new migration step (SlayerModel 18→19, SlayerQuery 9→10, Memory 7→8) and the custom-granularity load pipeline.
  - Ingestion: case-only-distinct column names.
  - `tests/test_library_growth.py` and `tests/test_law_removal_tripwire.py`: pins.
- Architecture: `architecture/python.engine.arc42.md` P8 and `architecture/python.storage.arc42.md` P3.2, worded as approved.
- Docs: `docs/concepts/{time,formulas,queries,references}.md`, `docs/examples/04_time/time.md`, `docs/reference/mcp.md`, the pg-facade and Flight SQL pages, the MCP `help.*` memories and the tool `Field` descriptions.
- Coordination: DEV-2102, DEV-2021, DEV-1607 and DEV-1920 also bump stored versions, and whichever merges second renumbers. DEV-2231 and DEV-1920 also edit `compatibility/removal-tripwire`.
