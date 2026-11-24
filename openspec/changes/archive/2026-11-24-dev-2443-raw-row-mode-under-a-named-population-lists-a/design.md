## Context

Raw-row determination (semantics Axiom 12.5) is checked only by population inference:
`dimension_anchor(..., raw_rows=True)` chooses the anchor with `associate=False`
(`slayer/engine/population/inference.py`). A named population (`_named_population_of` in
`slayer/engine/query_engine.py`), a pinned anchor (a saved stage's `population_inferred` name overrides the inferred
anchor and drops its error) and a query-stage population never reach that probe, so binding joins the population to
whatever the dimensions reach.

Binding already has the closure machinery: `row_expression_crossing` (`slayer/engine/population/home.py`) walks each
row leaf's dependency closure (engine P10) with `key_closure` + `safe_reachable`, but from the expression's own anchor
(Axiom 2.2), which is why `orders.ordered_at` from `customers` passes the time-dimension home check today.

Applicable principles: semantics Axioms 1, 2.2, 3.2, 12.5; engine P5 (a stage is bound as the dataset its schema
defines), P9 (every user-facing algebra type error raises in the checker `elaborate_env` as a `QueryTypeError`), P10
(dependency closures; unanalysable is unsafe). No arc42 or `.c4` edit: Axiom 12.5 already states the rule, and its
enforcing test file `tests/test_dev2212_raw_rows.py` receives the new tests. The spec is the existing, already-mapped
`queries/semantics`.

## Goals / Non-Goals

**Goals:**
- One bind-time rule for every population kind, so no population path can bypass Axiom 12.5.

**Non-Goals:**
- Filters (the Grain guarantee already keeps a fanning filter from multiplying raw rows).
- Expressions no dataset determines (Axiom 2.2) — their existing error stands; sibling-product dimensions are DEV-2430.
- Grouped mode (`distinct_dimension_values: true`).

## Decisions

1. **One bind-time rule for all populations, not an extra check on the named branch** (default). The symptom is one
   instance of the class "Axiom 12.5 enforced on one population path only"; the binder sees every population kind
   (named, inferred, pinned, stage) through `scope.population`, so a check there closes the class. Inference keeps
   its own `associate=False` anchor choice, so inferred queries still fail first with `PopulationInferenceError`; the
   bind rule is their backstop.
2. **Determination walked from the population, through dependency closures** (default). Each row leaf of every
   dimension and time dimension is closed under derived-field definitions (P10) and every closure path must be
   `safe_reachable` from the population itself — not from the expression's anchor. The first unsafe hop is named.
   The crossing helper in `population/home.py` walks from the population.
3. **Unproven hops are refused** (default). Axiom 12.5 says "provably determined"; inferred raw rows
   already refuse them, so named and inferred agree.
4. **Error type `DistinctDimensionValuesError`** (default). The condition exists only in raw-row mode — the same
   dimension is legal grouped — and raw-row refusals already use this type. Its docstring widens to cover it. The
   check function lives in `elaborate_env.py` (P9), called from `elaborate.py` for dimensions and time dimensions, after the Axiom 2.2 home check.
5. **List children, time dimensions, derived-field closures and stage populations included** (default) — the same bug
   class, cut off by the same rule.
6. **Filters out of scope** (default) — see Non-Goals.
7. **Precedence: the Axiom 2.2 homeless-expression error first** (codex, user). Codex proposed running the raw-row
   check first so the error is always `DistinctDimensionValuesError`; taken with the opposite precedence: an
   expression no dataset determines is illegal in every mode, so the Axiom 2.2 home check's
   `UnsafeRelationshipInputError` (`check_row_expression_home`) stays first and the raw-row rule applies only to
   homed entries. A test pins the precedence.
8. **Raise-site ledger row** (codex). The new `raise` in `elaborate_env.py` gets its row in
   `tests/_dev1871_raise_ledger.py` so `test_dev1871_raise_parity.py` stays green.
9. **Unanalysable closure fails closed with `UnanalyzableDependencyError`** (codex). When `key_closure` returns
   `None` there is no hop to name; per P10 the entry is unsafe, raised by
   `check_entry_dependencies_analyzable`, located at the entry.
10. **Inferred-precedence regression** (codex). Tests pin that inferred counterparts (computed, time dimension, list
    child) still raise `PopulationInferenceError` before binding.
11. **Ownership-boundary control** (codex). A derived field local to the population stays accepted in raw rows,
    guarding against a mis-rooted closure.
12. **The SQL facades advertise only determined joined dimensions** (user). A facade `SELECT <column> FROM <table>` is
    raw rows with that table as population, so `facade/projection`'s "one row per joined record" for a fanning
    dimension contradicts Axiom 12.5. The catalog decides advertisability with the engine's own to-one test
    (`provably_to_one`) and lists only determined dimensions in `INFORMATION_SCHEMA.DIMENSIONS` and `SELECT *`. Every
    dimension stays resolvable, so grouped queries over a fanning column still bind. The pg catalog-cache fingerprint
    includes advertisability (codex). Rejected: using the determining dataset as population, which silently changes
    the rows `FROM` names.

## Risks / Trade-offs

- [Breaking: raw-row queries accepted today over fanning or unproven hops now fail] → a Part 1 entry in
  `docs/upgrading/2.0.md` with the remedy (name the finer dataset as `population`, declare the hop's cardinality, or
  drop the flag); the change gates 2.0.
- [Stage inherited edges or list-child hops not classified by `safe_reachable` as expected] → scenarios pin both; if
  the bundle lacks a stage's edges, surface it before implementing rather than narrowing the scenario.
