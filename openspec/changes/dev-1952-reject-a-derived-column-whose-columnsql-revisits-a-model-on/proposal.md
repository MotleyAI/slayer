## Why

A derived `Column.sql` / `Column.filter` whose join path revisits a model already
on it (`regions.customers.spend` declared on `customers`) is not refused today:
the model saves silently and a query over the column emits the reference as an
opaque token (`0.0` on SQLite, a raw SQL error elsewhere, the literal string in
raw-row mode) where the query-typed spelling of the same path gets the typed
circular-join refusal. It slips both DEV-1930 gates because the shared join
walker reports a revisit exactly like an unknown hop (`None`), so the save-time
classifier skips it and the expansion door treats it as an opaque physical
reference that crosses nothing.

## What Changes

- The shared join walker (`core/join_walker.py::walk`) raises a typed
  `CircularJoinPathError` on a revisit, symmetric with its existing
  `AmbiguousJoinPathError`; `None` now means only an unknown or unloaded hop.
  Every best-effort consumer keeps today's answer by catching it; the two
  consumers that classify a user-typed definition fail closed.
- **Save time**: a derived column whose `sql` / `filter` path revisits a model is
  rejected with `DerivedColumnCircularError` (a `CircularJoinPathError`) naming
  the column, the kind, the full reference, the revisited model, the hop and the
  remedy — identically from the storage save door and the engine's save door.
  A revisit wins over an earlier fanning hop on the same path; an unloaded,
  unresolvable or ambiguous hop still skips (the revisit is provable once the
  path's models are loaded).
- **Query-time backstop**: a stored circular definition (saved without
  validation) is refused in every position — aggregate input from any root,
  dimension, filter, raw rows, through a derived chain, the filter-kind column —
  with the circular error naming the reference and the revisited model, never a
  value.
- The binder's query-typed circular refusal adopts the same error class; its
  message keeps the "Circular join" / "revisits model" wording.
- Definition-default resolution (owner-first, root-fallback) treats a circular
  frame probe as a clean miss, so default-parameter behaviour is unchanged.
- Normative harness: the approved Axiom 1 clause in
  `architecture/semantics.arc42.md`; one sentence in `docs/concepts/models.md`.

## Capabilities

### New Capabilities

<!-- None. -->

### Modified Capabilities

- `models/column-definitions`: the to-one-targets requirement gains "and never
  revisits a model already on its path" (save-time rejection with the circular
  error); a new requirement in the same capability specifies the query-time
  refusal of a stored circular definition and its parity with the query-typed
  spelling.

## Impact

- **Code**: `slayer/core/errors.py` (two classes); `slayer/core/join_walker.py`
  (the raise); `slayer/engine/join_safety.py`, `slayer/engine/compile/stages.py`,
  `slayer/sql/column_expansion.py`, `slayer/engine/reference_closure.py` (catch
  or propagate per site); `slayer/engine/column_dependency.py` and
  `slayer/engine/query_engine.py::_validate_mode_a_join_paths` (save-time
  conversion); `slayer/engine/binding.py` (class swap).
- **Normative harness**: `architecture/semantics.arc42.md` Axiom 1 (approved
  clause + enforcement tag).
- **Docs**: `docs/concepts/models.md` derived-columns paragraph, one sentence.
- **Tests**: new `tests/test_dev1952_derived_revisit.py`; with consent,
  `tests/test_dev1853_join_walker.py::test_revisit_is_guarded` flips from
  `is None` to the typed raise and `tests/test_errors_hierarchy.py` lists the two
  classes. No raise-ledger row: no raising module is in the DEV-1871 scan.
- **Behaviour**: saving a model whose derived column revisits a model now fails
  where it previously succeeded; querying a stored one fails with a typed error
  where it previously emitted dialect-dependent garbage. Fixture saves under
  `_validate=False` are unaffected.
