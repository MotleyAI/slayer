## Why

DEV-1995 reported orphan child rows landing in a parent group whose dimension is
NULL. Resolving it fixed the governing rule: an aggregate is a **virtual model**
keyed by its grain, its rows computed from its source by the same joins as any
query, whatever consumes it, and read in an expression as a field of that model
joined one-to-one on the grain, NULL being a grain value like any other. Under
that rule the reported value is correct (an orphan's parent-derived dimension is
NULL in the virtual model), and the one behaviour that breaks it is the DEV-1910
association presence guard, which drops a home entity depending on the consumer's
spelling: the same associated aggregate yields a different NULL cell when the
query is rooted at the other end of the association.

## What Changes

- Remove the association presence guard: an associated home entity whose join path
  reaches no related row carries NULL for that dimension and sits in the NULL cell,
  however the query is rooted — matching the virtual model and the home-rooted twin.
- **BREAKING** (values): under `to_many_handling: "associate"`, a NULL cell of a
  dimension reached back through the population root now includes home entities
  with no related row (orders-rooted `customers.spend:sum` by `status` on the
  null-status seed: 100 → 155, equal to the customers-rooted spelling).
- Pin the virtual-model reading of attached aggregates (orphan and dangling rows
  count in the NULL grain cell, as in the materialised aggregate).
- Axiom 6 states the virtual-model rule; Axiom 2.3 cross-references it.

## Capabilities

### New Capabilities

### Modified Capabilities
- `queries/semantics`: adds the requirement that aggregates behave as virtual models.
- `queries/attribution-modes`: the distinct-entity association requirement drops the
  presence rule and pins spelling-invariant NULL cells.

## Impact

- Code: `slayer/engine/compile/stages.py` (`_association_present_keys`, association
  arm plumbing), `slayer/ir/planned.py` (`AssociationProducerKernel.present_keys`),
  `slayer/sql/generator.py` (level-1 presence guard).
- Tests: new `tests/test_dev1995_virtual_model.py`; DEV-1910 guard tests and
  fixtures rewritten; association goldens re-blessed (guard removed).
- Architecture: `architecture/semantics.arc42.md` Axioms 6 and 2.3.
- Docs: `docs/concepts/queries.md` association paragraph.
