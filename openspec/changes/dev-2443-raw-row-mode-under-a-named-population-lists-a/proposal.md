## Why

Raw-row mode (`distinct_dimension_values: false`) promises one row per population row and requires every dimension
to be provably determined (semantics Axiom 12.5). Only population inference enforces that: a named, pinned or stage
population accepts a dimension reached across a fanning or unproven hop and lists each population row once per
related row, with no warning. On the Appendix B seed, `{"population": "customers", "distinct_dimension_values": false,
"dimensions": ["orders.amount"]}` returns 5 rows for 4 customers.

## What Changes

- **BREAKING**: In raw-row mode the population — named, inferred, pinned or a query stage — must provably determine
  every dimension and time dimension; otherwise the query fails with `DistinctDimensionValuesError` naming the
  dimension and the first fanning or unproven hop, in every `to_many_handling` mode. Refused today-accepted inputs:
  plain, computed and time dimensions across a fanning hop; list children (`D.L.value`); unproven hops (no declared cardinality, no covering unique key); a
  population's derived field whose definition crosses such a hop; a stage population's dimension across a fanning
  inherited edge.
- A dimension whose dependency closure cannot be analysed fails closed with `UnanalyzableDependencyError`.
- **BREAKING** (facades): the Postgres-wire and Flight catalogs stop advertising, and `SELECT *` stops expanding,
  joined dimensions reached across a fanning or unproven hop; a plain `SELECT` of one is refused, and a
  grouped one still works.
- Unchanged: determined dimensions (one row per population row), whole list columns, filters across fanning hops,
  grouped mode under the same population, and inferred raw-row queries (still refused at inference with
  `PopulationInferenceError`).
- Docs: the raw-rows rules in `docs/concepts/queries.md`, the query help memory, a Part 1 entry in
  `docs/upgrading/2.0.md`, and `docs/interfaces/flight-sql.md`.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/semantics`: adds the requirement that raw-row dimensions are determined by the population.
- `facade/projection`: fanning joined dimensions are not advertised.

## Impact

- `slayer/engine/elaborate_env.py` (new checker rule), `slayer/engine/elaborate.py` (call site, after the Axiom 2.2
  home pass), `slayer/engine/population/home.py` (closure crossing walked from the population),
  `slayer/core/errors.py` (`DistinctDimensionValuesError` docstring), `tests/_dev1871_raise_ledger.py`,
  `slayer/facade/catalog.py`, `catalog_sql.py`, `info_schema.py` and `translator.py` (advertised dimensions).
- Gates the 2.0 release (DEV-2220, umbrella DEV-2372).
