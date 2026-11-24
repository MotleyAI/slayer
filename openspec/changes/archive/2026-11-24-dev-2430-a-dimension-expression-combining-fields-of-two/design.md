## Context

See proposal.md › Why. Probe at `eef97186f` (SQLite and DuckDB, Appendix B seed, population `customers`, siblings `orders` / `returns`):

| Position | `orders.amount * returns.amount` | parent + child |
|---|---|---|
| dimension | accepted, `associated` | accepted |
| dimension, raw rows | accepted | accepted |
| time dimension | refused | refused (false positive) |
| order key | accepted (host-grain `min`/`max` over pairs) | accepted |
| field filter | accepted by association (specified) | accepted |
| aggregation source | refused | accepted |
| computed dimension, constituent grained on `returns.return_id` | accepted | — |

Only `aggregate_home` (Axiom 2.4, anchor first) and the time-dimension check `row_expression_crossing` (hops measured from the leaves' longest common prefix, not a home search) judge Axiom 2.2; dimensions and order keys judge nothing.

Principles that apply: semantics Axioms 1, 2.1–2.3, 2.8, 3.1, 4, 13, 14; engine P9 (algebra type errors raise in the checker, `elaborate_env`, as `QueryTypeError`, mirrored in the raise ledger) and P10 (dependencies are closures). `queries` is cross-cutting (`architecture/index.yaml`); no new spec attachment.

## Goals / Non-Goals

**Goals:**
- One Axiom 2.2 row-level home resolver, used by every value position, sharing candidate generation and determination with `aggregate_home`.
- The refusal raised once, in the checker.

**Non-Goals:**
- Axiom 12.5 (raw rows under a named population with a single dimension the population does not determine): DEV-2443. This change enforces only Axiom 2.2 in raw-row mode.
- Field filters: their association reading is unchanged (decision 1).
- Host-grain wrap code: unchanged (decision 9).
- No axiom for pairing sibling rows (decision 10).

## Decisions

1. **Field filters are exempt; value positions are refused** (user). Options: refuse value positions only / refuse filters too. A filter's existential over the referenced branches' product creates no cells and multiplies nothing, and `queries/semantics` › *Filters restrict by association or fail loudly* specifies it; refusing it would regress specified behaviour. Axiom 2.2 gains the carve-out (decision 11).
2. **Time dimensions use the shared resolver** (default). A legal parent-plus-child time dimension becomes accepted, restoring Axiom 13 parity; keeping the common-prefix check had no merit beyond scope.
3. **Raw rows under a named population with one fanning dimension → DEV-2443** (user), outside this change.
4. **Align the conflicting time-dimension artifacts with Axiom 2.2** (user): `tests/test_dev2119_expression_time_dimensions.py::test_leaf_across_a_fanning_hop_fails_closed` becomes executes-and-associates (the sibling refusal moves to the new test file); the `queries/time-dimensions` scenario *Expression leaf across a fanning hop fails closed* uses sibling leaves, with a new parent-plus-child scenario; the `check_time_dimension_home` row of `tests/_dev1871_raise_ledger.py` follows the new message.
5. **One resolver, structural fix** (default). `slayer/engine/population/home.py` gains the row-level home resolver; `row_expression_crossing` and `check_time_dimension_home` retire; one checker function in `elaborate_env` raises the refusal; `bind_inputs` calls it for dimensions, time dimensions and row-level order keys.
6. **Unproven hops determine nothing** (default): Axiom 1 / 2.2 say "provably to-one", as time dimensions and aggregations already judge.
7. **The product-hop error is reported first** (default): it is the more specific error (`queries/unrelated-datasets`).
8. **`UnsafeRelationshipInputError` naming two operands and their hops** (default): the issue's acceptance.
9. **Host-grain wrap rephrased in spec text only** (user, ruling 2026-11-23): always `min` (asc) / `max` (desc), duplicate-insensitive, consistent with Axiom 4; legal in every mode; no code or value change.
10. **Refuse; no axiom for pairing sibling rows; gates 2.0** (user, ruling 2026-11-23).
11. **arc42 edits** (user, approved wording), `architecture/semantics.arc42.md`:
    - Axiom 2.2: "determines them all, the expression is illegal, except as a field filter, which restricts by association (Axiom 14)." replacing "determines them all, the expression is illegal."
    - Axiom 2 status tags: add `[enforced: test:tests/test_dev2430_row_expression_home.py]` after `[enforced: test:tests/test_dev2388_own_anchor.py]`.
12. **Share candidates and determination, not the selection order** (codex). Aggregations try the source anchor first (Axiom 2.4); row-level expressions take the deepest determining candidate (Axiom 2.2). The shared core yields the candidate set and the determines-every-operand test; each caller orders the candidates.
13. **A leaf's candidate is its declared dataset, or every dataset its fanning definition reads** (codex; user, 2026-11-23). A leaf is an operand at the dataset it is declared on (Axiom 2.1); a derived leaf whose definition reads across a provably fanning hop is an operand at each dataset that definition reads a field on, its own included, so a definition no dataset determines is refused, and alone it resolves per `to_many_handling` as `queries/attribution-modes` › *Derived fanning dimension associates by default* specifies, and inside a combination it counts there; a definition reading across an unproven hop is refused. Tests: a derived leaf across an unproven hop, alone and inside a larger expression.
14. **Raw-row scope stated** (codex). The raw-row scenario covers only the sibling refusal; Axiom 12.5 is DEV-2443 (Non-Goals).
15. **Constituents reuse the existing grain expansion** (codex). In dimension position every constituent is explicitly grained (Axiom 9 residue); its members come from the existing constituent-grain expansion, which covers transforms and windowed inners. Test: a transform constituent.
16. **Product-hop precedence tests** (codex): dimension, time dimension, order key, and through a derived field.
17. **Order keys judged before wrap synthesis** (codex). The check runs on the bound row-level key before the host-grain `min`/`max` aggregate is built; tests: a sibling key refused, a parent-plus-child key still wrapped by direction.
18. **Time-dimension delta replaces the old scenario** (codex): already in decision 4; the scenario keeps its name (OpenSpec MODIFIED cannot rename a scenario) with sibling content.
19. **A homed expression is a column of its home** (user, 2026-11-23). In a value position, an expression whose home lies below another operand's dataset is evaluated as `CASE WHEN <home key> IS NOT NULL THEN … END`, so a population row with no home row reads NULL, exactly as a column of the home; this changes values of dimensions accepted today where the expression is non-NULL without a home row (breaking, gates 2.0).

## Risks / Trade-offs

- [Existing tests pin a dimension, order key or computed dimension whose operands no dataset determines] → per the flow, each is raised for approval to align, never silently changed.
- [Unproven-hop combinations accepted today as dimensions become refused] → consistent with time dimensions and aggregations; the error names the hop.
- [Stage hosts] → the resolver runs from each stage's host; covered by the query-list scenario.

## Migration Plan

Breaking; listed in `docs/upgrading/2.0.md` with the remedy. No stored-data migration: a stored query carrying such an expression fails with the typed error wherever it is planned.
