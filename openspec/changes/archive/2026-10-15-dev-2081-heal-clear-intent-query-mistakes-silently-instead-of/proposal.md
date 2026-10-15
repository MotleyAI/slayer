## Why

Agents spend turns on queries whose meaning is obvious but whose spelling is not canonical — a
measure named `change`, `isodow(order_date)`, `count(distinct x)`, `order_items.count(*)`, an
expression in the object form of an order column, `partition_by=[month(order_date)]` over a monthly
time dimension, a refinement restating a saved measure by name. Each rejection costs a turn and
often pushes the agent off the single-query form. These inputs have exactly one reading, so they
should run as their canonical form.

## What Changes

- Query and model measures may be named after a transform (`change`, `rank`, …); importers stop
  skipping such measures.
- Date-part aliases (`isodow`, `day_of_week`, `doy`, `dayofyear`, `day_of_year`, `isoyear`,
  `iso_year`, `isoweek`, `iso_week`, `weekofyear`, `dayofmonth`, `day_of_month`) heal to
  `date_part` as a one-argument function and as a `date_part` part; `extract(<part> from x)` is the
  SQL spelling of `date_part('<part>', x)`. `dow`, `dayofweek` and `weekday` stay errors that name
  both weekday conventions; the alias names and `extract` become reserved.
- An unresolved call fails with an "Unknown function" error instead of "Unknown aggregation".
- `count(distinct x)` heals to `count_distinct(x)`; other `DISTINCT` uses get a targeted error.
- The method-call spelling `<path>.<agg>(<source>[, literals])` is an aggregation spelling equal to
  `<agg>(<path>.<source>, …)`.
- An order column object `{"name": "<expression>"}` heals to the string form; the query schema
  advertises the order column's string form and hides the internal `raw_formula`.
- A `partition_by` element `gran(col)` binds to the query's time dimension at that granularity.
- A refinement measure whose formula is the bare name of a saved entry keeps that entry.
- **BREAKING** (error text only): unknown aggregation names report "Unknown function".

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/date-functions`: date-part aliases, `extract`, ambiguous weekday names, extended reserved names.
- `aggregations/functional-form`: `count(distinct x)`, the method-call spelling, the unknown-function error.
- `queries/measure-naming`: measures may be named after transforms.
- `queries/positions`: an order column object may carry an expression.
- `mcp/tool-surface`: the order column's string form is advertised.
- `mcp/query-tool`: the invalid-object scenarios no longer rely on the transform-name reservation.
- `queries/partitioned-aggregates`: a granularity call is a partition key when it is a query time dimension.
- `queries/saved-query-refinement`: bare-name restatement of a saved measure.

## Impact

- `slayer/engine/syntax.py` (date-part aliases, `extract` and `count(distinct …)` rewriting,
  method-call aggregations), `slayer/engine/binding.py` / `bind_inputs.py` (unknown-function error,
  granularity partition keys), `slayer/core/enums.py` / `keys.py` (alias table, reserved names),
  `slayer/core/models.py` (transform-name reservation removed), `slayer/core/query.py` (order column
  coercion and schema, refinement merge).
- Importers: `slayer/cube`, `slayer/dbt`, `slayer/osi` import transform-named measures.
- MCP / REST input schema for `order`.
- Docs: `docs/concepts/references.md`, `docs/concepts/queries.md`, the aggregation concept page,
  help topic `03_aggregations.md`.
