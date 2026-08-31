## 1. TDD test suite (lands before implementation)

- [x] 1.1 Safety-predicate unit tests: structural proof (solo PK, composite full vs partial coverage — F6), declared m:1/1:1, inverted mirrored-INNER reverse edges, unknown → unsafe, no synthesized traversal (F1); verify all fail (module absent) for the right reason
- [x] 1.2 DEV-1689 stamping tests: aggregated backing query → PK stamped and N:1 provable; dimension-only distinct → stamped; `distinct_dimension_values=False` → NOT stamped (F2 negative); verify against `create_model_from_query`
- [x] 1.3 Cube importer mapping tests: full relationship table incl. aliases, unknown string → None + conversion warning (F10)
- [x] 1.4 Validation-pressure tests: unproven join flagged with remedies; detection-contradicted declaration flagged (F5)
- [x] 1.5 Executed-value tests (SQLite + DuckDB) for target-rooted producers: safe-dim exactness, unsafe-dim broadcast, unproven-arity broadcast, NULL grain keys, keyless/CROSS JOIN, filtered, order-only, hidden
- [x] 1.6 Broadcast metadata + strict tests: per-measure warning shape, per-dim reasons, dedup across roles (F8), strict errors for broadcast and dropped filter, strict passes when all-attributable, explicit-partition broadcast never warns
- [x] 1.7 Producer filter-inheritance tests reproducing the `classify_host_filter` decision-table cases as behavior: safe ROW conjunct applied re-rooted; unsafe/unreachable dropped + warned; AGGREGATE-phase outer-WHERE unchanged; the 1:N inherited-filter double-count shape now warns instead of fanning (class-c pin)
- [x] 1.8 Unsafe-input hard-error tests: source expr / args / kwargs / `Column.filter` across unproven hops (class-d pins); unsafe explicit `partition_by` error incl. cross-model (F4) and local-with-joined-key
- [x] 1.9 Matrix-cell flip tests: the four `(band|bare|rank|mixed) × cm` cells execute with plan-structure assertions (nested attach inside target-rooted producer, D8 key check — F3); cross-model source in computed dimension; intermediate-hop dim executes-and-broadcasts
- [x] 1.10 Cross-model `_rk_`/windowed/partitioned executed-value tests at safe grains; windowed requires attributable TD error case
- [x] 1.11 Total-routing invariant tests: an unroutable shape raises the explicit planner error, never silent drop (F9)
- [x] 1.12 Warning-collector traversal tests: same filter dropped by several producers → one warning; same measure broadcast in several roles → one warning (F7)
- [x] 1.13 Fixture audit: declare target PKs where N:1 was intended across join-declaring fixtures; record the class-(c) candidates found; verify prior goldens still pass before implementation starts

> Progress note (see `HANDOVER.md`): Commits 0–4 implemented; all 168
> `test_dev1836_*` pass (sqlite+duckdb). The `cross_model_planner.py` deletion
> (3.3), the D7 invariant + `classify_isolation` retirement (7.x), and the
> golden re-bless / `_cm_` shape-suite reconciliation (8.1) remain — the last
> needs the author's go-ahead to modify existing tests.

## 2. Commit 0 — safety primitive + metadata enablement (no query-behavior change)

- [x] 2.1 New engine module with `provably_to_one` / `safe_reachable` per design D1; tests 1.1 green; full non-integration suite untouched
- [x] 2.2 DEV-1689 conditional stamping in `create_model_from_query` (D5); tests 1.2 green
- [x] 2.3 Cube `relationship` → `Join.cardinality` mapping (D8); tests 1.3 green
- [x] 2.4 `validate_models` + import-report flags (D1/D5 validation pressure); tests 1.4 green

## 3. Commit 1 — `_cm_` families onto target-rooted producers

- [x] 3.1 Producer synthesis for cross-model aggregates (re-rooted prebound, grain = safe dims, filter inheritance per D3); discovery generalizes past the cross-model exclusion; tests 1.5/1.7 green
- [x] 3.2 Warnings moved onto `RegroupAttachPlan` + recursive collector (D6); tests 1.12 green
- [ ] 3.3 Delete `cross_model_planner.py`, `CrossModelAggregatePlan`, `_render_cross_model_cte`/`_render_rerooted_cross_model_cte` consumer arms; enumerate class-(b) golden divergences for batch approval; executed values pinned unchanged — DEFERRED (still handles local filtered-local/ranked-host; see HANDOVER)

## 4. Commit 2 — safe-grain flip + strict + metadata

- [x] 4.1 Arity-checked S(A) for implicit grains with broadcast + `kind="broadcast"` response warnings + Python-warning emission (D2/D6); tests 1.6 green
- [x] 4.2 `SlayerQuery.strict` flag through REST/MCP (D9); strict tests green
- [x] 4.3 Unsafe-input and unsafe-explicit-partition hard errors (D2); tests 1.8 green; class-(c)/(d) enumerations recorded for approval

## 5. Commit 3 — remaining cross-model families

- [x] 5.1 Cross-model `_rk_` (RANKED_TARGET) onto target-rooted producers with the ranked kernel; tests 1.10 green
- [x] 5.2 Cross-model partitioned + windowed producers (D5 synthesized bucket; attributable-TD check); tests 1.10 green (`_narrow_shared_grain_to_partition` deletion rides with 3.3)

## 6. Commit 4 — guard lifts

- [x] 6.1 Lift cross-model-source-in-computed-dim + producer-needs-cross-model-CTE + row-attach × cross-model via D4 nested-attach validation relaxation; matrix tests 1.9 flip to passing
- [x] 6.2 Intermediate-hop dim executes-and-broadcasts (test green; the dead `_render_cross_model_cte` raise removal rides with 3.3)
- [x] 6.3 Re-point/retire remaining cross-model guard messages; guard-residue test green

## 7. Commit 5 — retire classify_isolation

- [ ] 7.1 Post-discovery total-routing invariant (D7); test 1.11 green (passes via existing backstop; dedicated invariant not yet added)
- [ ] 7.2 Delete `classify_isolation` + `IsolationKind` dispatch (keep `may_inline_crossing_inputs` seam); no remaining references (grep clean); full suite green

## 8. Wrap-up

- [ ] 8.1 Full non-integration suite + integration SQLite/DuckDB green; golden batches approved and re-blessed; perf corpus re-recorded with regressions reported — ~238 D10 test reconciliations pending (needs consent)
- [ ] 8.2 Docs: `docs/architecture/composable-attach.md` (stage-3 section + filter/broadcast semantics), `docs/concepts/queries.md` (strict + broadcast), `docs/concepts/formulas.md` (cross-model in dims/window), `docs/concepts/models.md` (join cardinality + validation), `.claude/skills/slayer-query.md`; zensical nav check
- [ ] 8.3 Lint clean (`ruff check slayer/ tests/`) ✓; DEV-1689 closed on merge; archive the change post-merge
