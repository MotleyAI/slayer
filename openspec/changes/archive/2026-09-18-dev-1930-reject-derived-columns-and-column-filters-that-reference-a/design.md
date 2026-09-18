## Context

See proposal.md — Why. The save-time seam is
`slayer/engine/column_dependency.py`, wired from `StorageBackend.save_model`
(`_validate=True`). It already resolves every `Column.sql` / `Column.filter`
reference to its target model and raises on a broken path. The arity proof lives
in `slayer/engine/join_safety.py::provably_to_one`; join orientation (declared
either side, inverted as needed) is `slayer/core/join_walker.py`. The query-time
backstop (DEV-1832) is `elaborate_env.check_local_producer_inputs_safe`.

## Goals / Non-Goals

**Goals:**
- One save-time predicate that splits a reference's hop path three ways
  (to-one / provably fanning / unproven) and acts on each.
- Reuse the existing reference-extraction and prefetch; no second datasource
  round-trip.

**Non-Goals:**
- Detection-report-driven fanning at save time (only *declared* cardinality is
  read on the save path; detection reports are a query/CLI concern).
- Any change to the query-time backstop or to query-level filter semantics
  (DEV-1909's existential reading is untouched).
- A repo-wide save-time invariant across peer saves (see Decisions →
  best-effort scope).

## Decisions

- **`provably_fans` reads only declared cardinality, proof-first.**
  `provably_fans(edge, target) = not provably_to_one(edge, target) and
  edge.cardinality in {ONE_TO_MANY, MANY_TO_MANY}`. Checking `provably_to_one`
  first means a hop whose target-side columns cover a unique key is to-one even
  if contradictorily declared `one_to_many` (proof beats declaration). A
  reverse-PK-covered hop (reverse orientation covers the source's unique key)
  is therefore *unproven*, not fanning — sound, because absence of a unique key
  on the forward target permits but does not prove a fan. Alternative
  (reverse-PK ⇒ fanning) was rejected: not sound, and it would reject common
  FK→PK shapes and break existing fixtures.

- **Three-way action.** provably fanning → raise `DerivedColumnFanningError`;
  unproven → `warnings.warn(UserWarning)`; to-one / unloaded / unresolvable /
  ambiguous → nothing. Warnings use the existing save-time channel
  (`warnings.warn`, as in `_warn_unnamed_parallel_edges`).

- **Best-effort scope = the model being saved.** Arity is classified for
  `model.columns` only, not all reachable models. Rationale: arity is a
  per-definition property; classifying every peer on every save would re-warn
  datasource-wide and could fail an unrelated save on a peer's declaration.
  Consistent with the module's stated design ("early-failure UX layer; the
  compile-time/query-time guard is authoritative"). A later peer save that
  turns a stored column fanning is not re-rejected at save time; the query-time
  backstop refuses it. Cycle detection keeps its all-reachable roots (a cycle
  is a global property).

- **Warning de-duplication.** `_fragment_refs` yields every reference site, so
  `mb.a + mb.b` crosses `→ mb` twice. Collect unique `(column, kind, hop)`
  triples and emit one warning each; the first *fanning* hop raises immediately.

- **One entry, one prefetch.** Rename `validate_no_column_cycles` →
  `validate_derived_columns`: prefetch `reachable` once, run the arity pass over
  `model.columns`, then the existing cycle DFS. Update the single caller
  (`storage/base.py::save_model`) and `tests/test_column_dependency.py`.

- **Error class.** `DerivedColumnFanningError(SlayerError, ValueError)` in
  `core/errors.py`, carrying `column`, `model`, `hop`, `kind`. `ValueError`
  subclass so REST 400 / `except ValueError` callers catch it, matching
  `ColumnCycleError`.

- **arc42.** One approved clause on Axiom 1 of `architecture/semantics.arc42.md`
  (normative harness — edit only with per-change approval, already granted),
  tagged `[enforced: test:tests/test_dev1930_save_time_arity.py]`.

- **Spec home.** New `models/column-definitions` capability, not an extension of
  `models/save-validation` (which is narrowly raw-SQL trial execution). No
  `index.yaml` change — it sits under the existing `models` cross-cutting spec.

## Risks / Trade-offs

- [A peer save changes join topology and makes a stored column fanning without
  re-validation] → the query-time input-safety gate (DEV-1832) refuses it when
  aggregated; documented as best-effort, matching the module and the issue.
- [Warning noise on default-save fixtures with unproven-hop derived columns] →
  harmless (no `filterwarnings=error` in pytest config); the warning contract is
  pinned by a dedicated `pytest.warns` test.
- [`column_dependency.py` is not in the DEV-1871 raise-ledger scan] → confirmed;
  the new save-time raise needs no ledger row, like the existing
  `ColumnCycleError`.

## Migration Plan

Additive save-time gate; no data migration. Migration write-back
(`_validate=False`) already bypasses the whole validator, so legacy models load
unchanged. Rollback = revert the change.
