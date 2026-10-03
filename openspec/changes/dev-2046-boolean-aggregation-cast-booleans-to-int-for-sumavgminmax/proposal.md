## Why

Aggregating a boolean is broken: `sum(has_fraudulent_dispute)` emits `CAST(SUM(b) AS BOOLEAN)` —
silently `True` instead of the count on DuckDB, an error on Postgres / SQL Server / BigQuery /
Snowflake. The cause is structural: result typing has four homes that disagree (the facade's
`_agg_output_type` already assumes INT), aggregate emission over a raw value is hand-built at
several render sites, and "is this value boolean" is answered once, best-effort, in binding.
Predicates (`amount > 15`, `status in (…)`) are booleans too, yet the parse gate rejects them as
aggregation sources and `IN` / `BETWEEN` re-aggregation sources leak a raw pydantic error
(DEV-1970, folded in).

## What Changes

- One boolean-type authority over every value-key kind; a boolean aggregation input is lowered to
  an integer inside `sum` / `avg` / `min` / `max` (and the association producer's value pick),
  `min` / `max` cast back to BOOLEAN within the aggregate expression; the count family,
  `first` / `last` and custom aggregations receive the boolean unchanged.
- One aggregation classifier taking the source type: boolean `sum` → INT / INTEGER format,
  boolean `avg` → DOUBLE / PERCENT format (an explicit column format wins), boolean `min` / `max`
  → BOOLEAN. Engine slot typing, display format and the facade's `INFORMATION_SCHEMA.METRICS`
  types all read it (the facade's own inference is deleted; custom aggregations now report the
  source type there, as the engine does).
- `avg` joins the BOOLEAN default aggregation set; boolean expression sources accept every
  aggregation in that set.
- Comparisons, boolean connectives, `IN` and `BETWEEN` are legal aggregation sources at row level
  and in re-aggregation (`sum(amount > 15)`, `avg(status in ('a', 'b'))`,
  `sum(count(x) between 1 and 3)`).
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
  comparisons, boolean connectives, `IN` and `BETWEEN`; "Gate and type semantics for
  expressions" accepts boolean expressions for the BOOLEAN default aggregation set.

## Impact

- `slayer/core/keys.py` (boolean-type authority), `slayer/core/enums.py` (classifier, BOOLEAN
  defaults), `slayer/engine/key_metadata.py`, `slayer/engine/response_meta.py`,
  `slayer/engine/binding.py`, `slayer/engine/syntax.py` (parse gate), `slayer/facade/catalog.py`.
- `slayer/sql/render/aggregates.py` (emission helper), `slayer/sql/render/value_expr.py`,
  `slayer/sql/generator.py` (built-in builder, association pick, HAVING),
  `slayer/sql/dialects/tsql.py`.
- `AggregateKey` source union gains `InKey` / `BetweenKey`.
- Docs: `docs/concepts/models.md`, `docs/concepts/formulas.md`.
- `tests/test_dev1847_gate.py` row-level comparison rejection flips to acceptance.
