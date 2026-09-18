## Why

A derived column of model `ma` is a function of `ma`'s row only across provably
to-one join hops (Axiom 1). A definition like `Column.sql: "mb.b"` across a
fanning hop is a *set* per row — not a column of `ma` at all; what the author
wanted was an aggregation (`mb.b:sum`) or a filter by association. DEV-1832
lands the query-time backstop that fails such an aggregation closed. This change
moves the rejection to the *definition* where it belongs — at save time, where
the fan-out is provable — so the ill-formed model is caught early rather than
only when a query happens to aggregate the column.

## What Changes

- Save-time classification of every derived `Column.sql` / `Column.filter`
  reference's hop path from its declaring model, three ways:
  - **provably to-one** → accepted, unchanged.
  - **provably fanning** (the hop is *not* provably to-one *and* is declared
    `one_to_many` / `many_to_many` in its traversal orientation) → **rejected**
    at save time with a typed `DerivedColumnFanningError` naming the column, the
    hop, and the cross-model remedy.
  - **unproven** (resolvable, target loaded, but neither provably to-one nor
    provably fanning — includes an undeclared hop and the reverse-of-a-PK-covered
    hop) → accepted with a save-time `UserWarning`; the DEV-1832 query-time
    input-safety gate remains the fail-closed backstop when the column is
    aggregated.
  - unloaded / unresolvable / ambiguous target → skipped (best-effort).
- The same rule applies to a `Column.filter` reference.
- Scope is the model being saved; save-time rejection is a best-effort
  early-failure layer, with the query-time gate authoritative for a topology
  change introduced by a later peer save.

## Capabilities

### New Capabilities

- `models/column-definitions`: well-formedness of a derived column's definition
  (`Column.sql` / `Column.filter`) at save time — a derived column may reference
  only provably to-one targets; a provably fanning target is rejected, an
  unproven target is accepted with a warning and the query-time backstop.

### Modified Capabilities

<!-- None: the query-time backstop (queries/semantics Axiom 2.8) and query-level
     filter semantics are unchanged; this change only adds a save-time gate. -->

## Impact

- **Code**: `slayer/engine/join_safety.py` (new `provably_fans` predicate);
  `slayer/engine/column_dependency.py` (new save-time arity pass; rename
  `validate_no_column_cycles` → `validate_derived_columns`); `slayer/core/errors.py`
  (new `DerivedColumnFanningError`); `slayer/storage/base.py` (one caller rename).
- **Normative harness**: `architecture/semantics.arc42.md` Axiom 1 gains one
  clause (approved wording).
- **Docs**: `docs/concepts/models.md` derived-columns section, one sentence.
- **Behaviour**: saving a model whose derived column crosses a *declared*
  to-many hop now fails where it previously succeeded (the column was only
  refused at query time when aggregated). Intentional-fixture saves that use
  `_validate=False` are unaffected.
