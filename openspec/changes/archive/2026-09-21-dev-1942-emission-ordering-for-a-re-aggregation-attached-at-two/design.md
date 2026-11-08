## Context

See proposal.md › Why. Two established mechanisms frame the fix. Planner: root
discovery walks consumer keys (`walk_consumer_keys`, opaque below an attach-owning
aggregate's inputs, still descending its partition keys) and `_plan_regroups`
pre-substitutes every re-aggregation root with a reserved-leaf placeholder so its
constituents belong to its own carrier; the same placeholder key attached at both phases
is already a dual role the stager places in `_base` (BASE). Renderer: `assemble_with_chain`
orders one flat `WITH` from caller-declared `depends_on` (depth-first, insertion tiebreak);
a producer renders as its own statement, is re-parsed, its `WITH` hoisted into the
consumer's chain with hardcoded `_base`/`base` names uniquified; a producer already
rendered in the generation reuses its CTE, and a row attach whose identity equals a
combined attach dedups onto the combined CTE (so `_base` declares it). Arc42: sql P6
(dependencies declared), P11 (one flat WITH), engine P4 (interning is dedup), P6 (one
stage per value); semantics Axioms 6, 13, 14.

## Goals / Non-Goals

**Goals:**
- Substitution and discovery share one traversal law, so a root nested where discovery
  does not look is never substituted away.
- Every ordering constraint in an emitted `WITH` is a declared edge at every hoisting
  depth and in the multi-stage pipeline; a forward reference is a harness failure on
  every dialect, not a DuckDB runtime error.
- Re-aggregation producers (root, constituent, carrier) are first-class population
  citizens: disposition inherited, diagnostics reported.

**Non-Goals:**
- Retiring the pre-substitution itself (DEV-1903's single discovery walk).
- Removing the producer text round-trip in the split (sql P1 tension, pre-existing).
- Interning the standalone and constituent attaches of one root into one plan object:
  they differ by public alias; render-level dedup is the established mechanism (D6
  naming kept). The observable — one CTE — is what is pinned.

## Decisions

1. **Consumer-scoped substitution (A1)** — `substitute_consumer_keys` beside
   `walk_consumer_keys`: pre-order replace at a match; below an attach-owning aggregate
   the source/args/kwargs are opaque and only `partition_keys` are traversed; otherwise
   `map_children`. `_substitute_prebound` and `substitute_in_bound_filter` take a
   `substitute` callable (default deep). Only the re-aggregation pre-substitution passes
   the consumer-scoped one; the redundant-partition strip and the final placeholder
   rewrite stay deep (the latter must reach inside row-attach roots). Alternatives:
   placeholder-aware discovery predicates (core predicates learn the compiler's
   placeholder mechanism, instance patch) — rejected; dropping pre-substitution
   (audits every deep discovery walk) — DEV-1903 scope.
2. **Statement-scoped dependency registry (B1, Codex 1–2)** — a stack of per-statement
   `Dict[name, List[name]]`; one generator method wraps every `assemble_with_chain` call
   and merges each entry's `depends_on` into the top entry (append-only, order kept);
   `_render_producer_split` and the stage loop push/pop around each statement render.
   `_split_statement_ctes` returns `List[CteEntry]` whose deps come from that statement's
   entry, re-keyed through the rename map `_uniquify_producer_base_ctes` now returns;
   a statement whose split meets one CTE name in two `WITH` nodes fails closed. Reuse
   edges keep their allocator-unique consumer keys. Alternative: a generation-wide
   registry keyed by name — conflates the hardcoded `_base` of different producers;
   a positional chain over each hoisted list — carries the row path's reuse-edge hack
   and over-declares. Rejected for exactness.
3. **Both preparers emit nodes** — `_prepare_combined_regroup_attaches` returns `Node`s:
   hoisted nodes with their own deps, the consumer with `[*hoisted, *reuse]`; the two
   `cm_regroup_ctes` sites splice; `_prepare_regroup_attaches` gives hoisted nodes their
   own deps instead of the consumer's reuse deps.
4. **Multi-stage pipeline assembled by declared edges (Codex 3)** — the root statement is
   split too (no base rename; it is the outermost); one `assemble_with_chain` orders
   stage hoists, stage relations and root CTEs. A stage relation declares its hoisted
   names plus the sibling stage it reads (the plan-time DAG edge); root entries declare
   the stage relations. Depth-first with insertion tiebreak reproduces today's order,
   so output is byte-identical.
5. **Scope-aware forward-reference validator (Codex 4)** — in `scope_check`, on the
   same `traverse_scope` walk: flag a table source only when it is unresolved (no
   earlier CTE / derived alias binds it), carries no catalog/schema, and case-folds to a
   CTE declared later in the nearest enclosing `WITH` that defines the name. Hooked into
   `maybe_validate_scopes` (suite-wide). It is a backstop check, not the order source —
   consistent with the DEV-1746 ruling.
6. **Population disposition and diagnostics on re-aggregation attaches (C, Codex 5)** —
   the constituent loop passes `population_filters`; both synthesis paths and the carrier
   attach set `dropped_filter_warnings` from the disposition and
   `population_semi_join_measures` to the consuming public measure names (root: its
   public alias; constituent: the public names of the row-attach roots consuming it;
   carrier: the enclosing list). Verified gap: today a standalone re-aggregation silently
   drops an out-of-scope conjunct (plain producers warn) — Axiom 14.
7. **Guard deletion** — `check_reaggregation_not_standalone_and_mixed` + call + ledger
   row; ValueError, so `guards.baseline` is untouched.
8. **arc42** — `sql.arc42.md` P6 `[review]` → `[enforced: test:tests/test_dev1942_cte_dependencies.py]`
   (approved).

## Risks / Trade-offs

- [Declared deps reorder CTEs in existing goldens] → re-bless, listed in tasks.md;
  execution tests prove equivalence.
- [Registry re-key on the `_base` rename misses a reference] → the forward-reference
  validator fails the suite on the first mis-ordered statement.
- [Semi-join entry names for nested carriers change from an internal alias to the public
  measure] → no existing test pins the old name (checked); DEV-1909 rule restored.
- [Two attaches of one root are distinct plan objects] → render dedup by attach identity
  yields one CTE; pinned as the observable.
