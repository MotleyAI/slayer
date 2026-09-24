## Why

`DerivedColumnCircularError` and `DerivedColumnFanningError` carry a `kind`
(`"sql"` | `"filter"`) that is not a semantic distinction — `Column.filter` is a
pure value mask, lowering to the same masked SQL as a `CASE WHEN` in `sql` — and
the circular error duplicates `root_model` in a separate `model` field. Because
the engine save door rebuilds the error from the outer column surface, both
copies disagree with the truth in transitive shapes: a probe shows
`orders.outer = customers.bad * 2` (with `bad`'s `filter` revisiting) reported as
column `bad` **on model `orders`** with a **sql** reference. Remove the fields
that can disagree rather than threading state to keep them in sync.

## What Changes

- **BREAKING (Python API)**: `DerivedColumnFanningError` and
  `DerivedColumnCircularError` no longer accept or expose `kind`; their messages
  identify the offender by column, model and reference (no `sql`/`filter` label).
- **BREAKING (Python API)**: `DerivedColumnCircularError` no longer accepts
  `model`; `.model` is a read-only alias of `root_model`, so the engine door
  attributes a revisit inside a referenced derived column to that column's
  declaring model.
- `DerivedColumnFanningError.reference` becomes required and carries the full
  user spelling (a leading declaring-model qualifier kept), like the circular error.
- The save-time unproven-hop warning drops the `kind` label and is emitted once
  per column and hop (a column whose `sql` and `filter` cross the same unproven
  hop warns once, not twice).

## Capabilities

### New Capabilities

### Modified Capabilities
- `models/column-definitions`: the circular error names the column, its declaring
  model and the reference (no kind); the engine door attributes a revisit reached
  through another derived column to that inner column and its declaring model; the
  unproven-hop warning is once per column and hop; "filter-kind" wording dropped.

## Impact

- `slayer/core/errors.py` — `DerivedColumnFanningError`, `DerivedColumnCircularError`.
- `slayer/engine/column_dependency.py` — `_arity_reference_sources`,
  `_iter_arity_refs`, `_check_reference_arity`, `_unproven_arity_message`.
- `slayer/engine/query_engine.py` — `_validate_mode_a_join_paths`.
- Tests: `test_dev1952_derived_revisit.py`, `test_dev1930_save_time_arity.py`,
  `test_dev1853_traversal_execution.py` (kind assertions → reference assertions);
  new `test_dev1955_kind_free_vocabulary.py`.
- No REST/MCP/CLI/client consumer reads `.kind` / `.model` off these errors
  (both map through the generic `ValueError` → 400 path); no docs quote the messages.
