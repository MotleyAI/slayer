## Context

A cross-model aggregate compiles as a producer rooted at its source and attaches to
the result on its complete grain with null-safe equality. The association producer
additionally guards the population root's join columns `NOT NULL` in its level-1
query when a dimension is read back through the population root (DEV-1910 D3). That
guard is the only place an aggregate's row set depends on its consumer. See
proposal.md for the motivation.

## Goals / Non-Goals

**Goals:**
- Make every aggregate satisfy Axiom 6's virtual-model rule, pinned by executed parity
  against the same aggregate materialised as a query-backed model.

**Non-Goals:**
- Changing null-safe grain matching, filter routing, broadcast, or declared-join
  semantics (a declared model join keeps plain `=`: it is a relationship, not a grain
  lookup).

## Decisions

### D1. The issue's reported value is correct
The interview first explored guarding every producer so orphans leave the NULL cell
(40 instead of 47). Rejected by the user's ruling: an aggregate is a field of a
grain-keyed virtual model built by the same null-extending joins as any query, so an
orphan's parent-derived dimension is NULL there — exactly what materialising the
aggregate yields ({North 100, South 20, NULL 47}). Distinguishing "no parent" from
"NULL value" would make the aggregate depend on its consumer.

### D2. Delete the presence guard, do not generalise it
`_association_present_keys`, `AssociationProducerKernel.present_keys` and the
generator's level-1 guard are removed outright. The orders-rooted association then
equals its customers-rooted twin (NULL-status cell 155 on the null-status seed).
Alternatives — keeping the guard for "population-root" dimensions, or extending it to
all producers — both keep a consumer-dependent row set.

### D3. Axiom text
Axiom 6 gains the virtual-model clause (`[enforced: test:tests/test_dev1995_virtual_model.py]`);
Axiom 2.3 references it for broadcast. Axiom 7 is unchanged.

### D4. Parity oracle
Parity tests compare executed cells with the rows of the same aggregate saved via
`create_model_from_query`, matched on grain value null-safely, and additionally assert
the NULL cell's hand-computed value explicitly. A declared join to the saved model is
not the oracle: its plain `=` never matches a NULL grain value.

## Risks / Trade-offs

- [Associated NULL cells grow for orders-rooted queries] → documented as BREAKING in
  the proposal; users who can drop the NULL cell filter the dimension to non-NULL,
  which removes the cell rather than restoring its former value.
- [Hidden consumer-dependence elsewhere] → parity is tested in measure, filter-only,
  order-only and explicit-`partition_by=` positions, single-hop, multi-hop, composite
  and filtered shapes.
