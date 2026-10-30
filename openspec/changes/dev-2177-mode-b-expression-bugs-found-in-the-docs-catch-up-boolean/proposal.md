## Why

Writing the docs for the SQL grammar turned up three Mode-B expression bugs, each reproduced on DuckDB:

- A boolean aggregation parameter (`weight=`, `other=`) is not read as 0/1, so the database rejects it.
- An unnamed `FILTER (WHERE …)` aggregate is named by a different rule depending on whether its condition holds a variable.
- A CASE branch typed only by `||` slips past the plan-time type check.

Probing found two more instances of the same causes:

- When FILTER stays on the key (`first` / `last`, or `count(*)` with a joined-model condition), the filtered aggregate shares its unfiltered twin's names. It then collides with the twin, or silently returns the twin's value.
- The branch check misses joined-column and arithmetic branches, a CASE inside an aggregate, and `coalesce` / `ifnull` / `greatest` / `least`.

## What Changes

- Every input of a numeric built-in aggregation reads a boolean as its integer: the value, and also each parameter (`weight=`, `other=`, and the placeholders of a model-level override that reuses a built-in name). Custom aggregations keep receiving raw booleans for `{value}` and every placeholder.
- An unnamed filtered aggregate is named from its formula text, by one rule whether or not its condition holds a variable.
- A filtered aggregate never shares a name or a value with its unfiltered twin, internally or publicly.
- Every value-returning conditional (`CASE` / `iif`, `coalesce`, `ifnull`, `greatest`, `least`) joins its branch types at plan time, in every position and on every dialect. A provable mismatch is rejected with a typed error, which covers a `||` / `concat` branch, a joined-column branch, an arithmetic branch and a conditional nested in an aggregate. **BREAKING** on SQLite only: a mixed-type conditional that SQLite used to return as mixed values now fails at plan time, as it already does on Postgres and DuckDB.
- Measure and dimension types that are inferred, rather than declared, are derived from the expression after the plan-time coercions.
- `architecture/system.arc42.md` §3 gains principle 17, "One typing", targeted at DEV-2180.

## Capabilities

### New Capabilities

### Modified Capabilities
- `aggregations/boolean-inputs`: numeric aggregations lower boolean parameters as well as the value; custom aggregations receive raw booleans for every input.
- `aggregations/functional-form`: the FILTER clause has one text-naming rule, and a filtered aggregate never shares a name or a value with its unfiltered twin.
- `queries/mixed-type-coercion`: value-returning conditionals share one type, checked at plan time.

## Impact

- `slayer/sql/generator.py`: `ResolvedAggKwarg` gets a typed input, and every construction site and `_resolve_agg_param` change with it.
- `slayer/sql/naming.py`: `canonical_aggregate_alias` takes the filter into account, and the text-path alias derivation moves here.
- `slayer/engine/bind_inputs.py`: formula naming, and type inference after coercion.
- `slayer/engine/elaborate_env.py`: the new conditional-branch check.
- `slayer/engine/key_metadata.py`: the parallel branch typing is removed.
- `slayer/core/keys.py`: `value_type` returns the joined type of value-returning scalars.
- Docs: the aggregation docs, the FILTER docs and `docs/concepts/formulas.md`.
