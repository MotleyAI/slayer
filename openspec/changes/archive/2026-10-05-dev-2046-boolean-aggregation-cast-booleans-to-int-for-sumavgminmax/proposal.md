## Why

Aggregating a boolean is broken: `sum(has_fraudulent_dispute)` emits `CAST(SUM(b) AS BOOLEAN)` —
silently `True` instead of the count on DuckDB, an error on Postgres / SQL Server / BigQuery /
Snowflake. The cause is structural: result typing has four homes that disagree (the facade's
`_agg_output_type` already assumes INT), aggregate emission over a raw value is hand-built at
several render sites, and "is this value boolean" is answered once, best-effort, in binding.
Predicates (`amount > 15`, `status in (…)`) are booleans too, yet the parse gate rejects them as
aggregation sources and `IN` / time-point re-aggregation sources leak a raw pydantic error
(DEV-1970, folded in; its `BETWEEN` half is moot — Mode B has no `BETWEEN`, whose stale claims
in `consecutive_periods`' spec, docs and errors are corrected here).

## What Changes

- One boolean-type authority over every value-key kind; a boolean is its integer wherever a
  number is needed: the input of every numeric aggregation (`sum`, `avg`, `min`, `max`, `median`,
  `percentile`, `weighted_avg`, `stddev_*`, `var_*`, `corr`, `covar_*`, and the association /
  ranked picks), an arithmetic operand, a numeric scalar argument, a branch of a conditional
  mixed with numbers, and a comparison operand against a number. `min` / `max` cast back to
  BOOLEAN within the aggregate expression; the count family, `first` / `last` and custom
  aggregations receive the boolean unchanged.
- One aggregation classifier taking the source type: boolean `sum` → INT / INTEGER format,
  boolean `avg` → DOUBLE / PERCENT format (an explicit column format wins), boolean `min` / `max`
  → BOOLEAN. Engine slot typing, display format and the facade's `INFORMATION_SCHEMA.METRICS`
  types all read it (the facade's own inference is deleted; custom aggregations now report the
  source type there, as the engine does).
- The BOOLEAN default aggregation set becomes the numeric set; boolean expression sources are
  numeric (only text and temporal expressions stay rejected for numeric-only aggregations).
- Comparisons (time-point comparisons included), boolean connectives and `IN` are legal
  aggregation sources at row level and in re-aggregation (`sum(amount > 15)`,
  `avg(status in ('a', 'b'))`, `sum(count(x) in (1, 2))`, `sum(max(ordered_at) >= '2025-02')`).
- Aggregate expression sources reach the aggregate builders as AST, never re-parsed SQL text
  (`sum(True)` no longer renders as a column named `TRUE`); derived columns and parameters stay
  with DEV-1972.
- An aggregate over all-NULL inputs takes the empty value (0 for the count family, NULL
  otherwise) in every location — semantics Axiom 4.
- SQL Server: a predicate in a value position renders as a BIT value
  (`CAST(CASE WHEN p THEN 1 WHEN NOT p THEN 0 END AS BIT)`), fixing projected boolean measures
  (`sum(amount) > 50`) as well as predicate aggregation inputs.

## Capabilities

### New Capabilities
- `aggregations/boolean-inputs`: how a boolean-valued aggregation source (column, expression,
  predicate, stage column, attached value) is aggregated — input lowering, result type and
  format, NULL semantics, HAVING, re-aggregation, and facade agreement.
- `sql/predicate-values`: a predicate in a value position renders as a value on every dialect.

### Modified Capabilities
- `aggregations/expression-aggregation`: "Row-level expressions can be aggregated" admits
  comparisons, boolean connectives and `IN`; "Gate and type semantics for
  expressions" treats boolean expressions as numeric.
- `queries/transforms`: "Composite-input consecutive_periods" and "consecutive_periods
  predicate typing contract" drop the nonexistent `BETWEEN` predicate; a boolean-shaped node in a
  numeric position is its integer instead of a `ValueError`.
- `models/column-filters`: "Parameters are not masked by the source's filter" states its result
  through the derived-column equivalence, since `weighted_avg` now skips a NULL value's weight.

## Impact

- `slayer/core/keys.py` (boolean-type authority), `slayer/core/enums.py` (classifier, BOOLEAN
  defaults), `slayer/engine/key_metadata.py`, `slayer/engine/response_meta.py`,
  `slayer/engine/binding.py`, `slayer/engine/syntax.py` (parse gate), `slayer/facade/catalog.py`.
- `slayer/sql/render/aggregates.py` (emission helper), `slayer/sql/render/value_expr.py`,
  `slayer/sql/generator.py` (built-in builder, association pick, HAVING),
  `slayer/sql/dialects/tsql.py`.
- `AggregateKey` source union gains `InKey` / `TimePointCmpKey`; `EXPRESSION_SOURCE_KINDS`
  (`slayer/core/refs.py`) gains `InKey`.
- Docs: `docs/concepts/models.md`, `docs/concepts/formulas.md`.
- `tests/test_dev1847_gate.py` row-level comparison rejection flips to acceptance;
  `tests/golden/dev1846_sql_baseline.json` error strings lose `BETWEEN`, and
  `reject/cp_boolean_numeric` becomes an emitted-SQL golden.
- Existing rejection tests flip to acceptance: `tests/test_expression_aggregations.py`
  (`sum(True)`, `sum(like(…))`, `sum(iif(…, True, False))`), `tests/test_dev1846_composite_transforms.py`
  (boolean arithmetic / scalar argument), `tests/test_dev1854_null_test_predicates.py`
  (null test plus 1). `tests/test_dev1744_naming_allocator.py`'s no-leaf fixture becomes a
  nested aggregate; `tests/test_agg_render_spec.py` pins 12 fields; golden
  `ts/series_in_pred::tsql` is re-blessed (BIT value).
- `architecture/semantics.arc42.md` Axiom 4 states the all-NULL rule (approved 2026-10-05).
