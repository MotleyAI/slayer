## Why

A stage's dotted dimension (`customers.name`) is read downstream by its flat name (`customers__name`), and from a
model-scoped root that joins the stage as `s.customers__name` — which already works. Writing the dotted spelling
instead (`s.customers.name`, or `customers.name` in a stage scope) fails with misleading errors: "not a valid join
chain", a wrong "Did you mean 'customers.name'?", a `CircularJoinPathError`, advice to aggregate with `max(...)`, an
"illegal scope" error that never names the column, and messages naming the minted stage identity
(`__slayer_stage_s`). Agents cannot recover from these.

## What Changes

- The flat `__` name stays the one canonical spelling of a flattened column; the dotted spelling does not become legal.
- A dotted reference whose join walk fails, and whose remaining tokens joined with `__` name a column of a model on
  the walked prefix, fails as an unknown reference naming that model (as the user spells it), listing its columns,
  and suggesting the flat spelling.
- A broken chain with no such column names the model the failing hop leaves and lists its columns.
- In a downstream stage scope, the illegal-scope error for a dotted reference names the exact flat column it spells.
- An unrelated-dataset refusal whose short form spells a population column suggests that column first.
- Reference-resolution errors name models as the user spells them, never by a minted stage identity.
- The pinned strict-xfail `test_an_inline_source_is_local_to_its_own_stage[inline-root]` passes, spelled
  `s.customers__name`; stored-root variants execute on SQLite and DuckDB.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/dotted-dimension-routing`: broken chains defer to a flat-column hint and name the model they stop at; new
  requirements for the flat-spelling refusal and for user spellings in reference errors.

## Impact

- `slayer/engine/binding.py` (walk-failure doors, stage-scope dotted refusal, error spellings),
  `slayer/core/scope.py` (a non-recording flat-name probe), `slayer/engine/elaborate.py` /
  `slayer/engine/elaborate_env.py` (unrelated-read suggestion).
- Error types and texts only for references that already fail; no successful query changes.
- `docs/concepts/queries.md` (sibling-stage paragraph).
