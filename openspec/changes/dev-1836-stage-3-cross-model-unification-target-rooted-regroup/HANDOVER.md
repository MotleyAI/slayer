# DEV-1836 — implementation handover (resume state)

Branch: `egor/dev-1836-stage-3-cross-model-unification-target-rooted-regroup`.
Resume via `/spec` Step 1 (Linear DEV-1836 + this change folder). This doc is
deleted only when the WHOLE task is done.

## Status at a glance (implementation + reconciliation + wrap-up COMPLETE)

- **Implementation complete (Commits 0–5).** All 116 assertion-test
  reconciliations done; full non-integration suite green (14709);
  integration SQLite/DuckDB green (metabase_e2e errors are env-only: no
  live Metabase).
- **D7 total-routing invariant landed** (task 7.1) with a dedicated
  blinded-discovery failing-without-fix test. 7.2 (classify_isolation
  retirement) deferred to DEV-1838 per decision B1 (comment already on
  DEV-1838; do NOT file a new issue).
- **Third real regression found + fixed: Q9 target-join-filter shape.** The
  producer lacked design D3's host-hop/sibling reroot rules — a host dim /
  sibling filter with a provably to-one path from the root broadcast/dropped
  instead of staying exact. Fixed in `stage_planner.py`:
  `_attributable_from_root(host_name=)` + `_reroot_from_root` (leaf-wise:
  prefix-strip / direct-sibling-unchanged / prepend-host-hop), wired into the
  grain loop, `_cross_model_inherited_filters`, the window-TD check, and
  `_assert_partition_key_attributable`. Pinned by the Q9 integration test,
  the un-xfailed `test_rerooted_local_filter_remapped_to_source`, and
  `test_rerooted_cte_includes_reachable_and_sibling_filters`.
- **Docs done (8.2):** composable-attach stage-3 section; queries.md
  (strict param, warnings row, cross-model semantics, stale DEV-1445
  workaround removed — both filter forms now work); formulas.md (cross-model
  window/partition); models.md (cardinality now load-bearing + join-safety
  audit); rest-api/mcp `strict` rows; slayer-query skill. No new pages →
  zensical nav untouched.
- **OpenSpec `validate --strict` green.** Ruff green.
- **Perf A/B audit re-recorded** (`tests/perf/compare/out/`): no perf flags;
  value mismatches are oracle-attributed pypi bugs + ledgered class-(c)
  flips. NOTE: first run predated the Q9 fix — re-run with `--retime` and
  append a DEV-1836 section to `tests/perf/compare/RESULTS.md` (mirror the
  DEV-1835 section) if not yet done, then commit RESULTS.md.

Commit trail (newest first): `92bf54d1` D7 invariant · `7f3c4c26` 52-test
reconcile · `18611c24` handover · `37212376` dev1769 drop+warn · `b75d437f`
bug#2 · `4fa148fd` 59 tests + bug#1.

## Remaining work

1. **Step 7 gate (NOW):** everything is committed locally through `6126db04`;
   ask the author to push + PR (PR description includes
   `openspec show <change> --diff`). Hard stop after PR creation; then
   `/process-reviews` loop; archive via Step 8 pre-merge on the author's
   go-ahead. Delete this HANDOVER.md when the whole task is done.
2. Codex pre-PR review done (session `kpx7lai8`): finding 1 (TimeTruncKey
   never rerooted — `walk_value_keys` has no TimeTruncKey arm) and finding 2
   (direct-sibling rule could bind the ROOT's own join instance, not the
   host's) both fixed in `6126db04` with failing-first coverage
   (`tests/test_dev1836_reverse_hop_reroot.py`); finding 3 (D7 filter/order
   blinded coverage) added. Sibling rule now prefers the via-host path
   (instance-exact) and falls back to the root's own join (legacy star-schema
   parity) only when via-host is unprovable.
3. Flag to the author: 2 stale non-strict xfail markers XPASS in integration
   (`test_notebooks` — DEV-1713 / DEV-1715 reasons) — out of scope here.

## Key semantics (for reviewers / future stages)

- Cross-model aggregate → target-rooted regroup producer
  (`regroup_attach_plans`, `producer_root_model == target`); host slots carry
  reserved-leaf `__regroup__N__` placeholders (special-case
  `REGROUP_LEAF_PREFIX` when classifying slots by phase).
- Grain: shared-join-key reroot first (no join needed), then D3 rules under
  the D1 safety predicate (prefix-strip / direct-sibling / prepend-host-hop,
  every hop provably to-one), else broadcast + warning (hard error for an
  explicit partition key). Filters: same rules per ROW conjunct — inherit
  re-rooted or drop + warn. Aggregate-phase predicate over the cross-model
  value → outer-SELECT WHERE (uniform row restriction).
- `strict=true` turns broadcast / dropped-filter into errors.
- Host-rooted routes (crossing `Column.filter` inputs, host-grain wraps,
  filtered-local) still ride `cross_model_planner` / `classify_isolation` —
  retirement is DEV-1838.
- Reconciliation patterns for old→new test shapes: see this file's history at
  `18611c24` (patterns 1–10) if more legacy suites surface.

## Oracles

- `tests/test_dev1836_*.py` (169) · equivalence: `tests/_dev1836_equiv_driver.py`
  (122 golden cases: 113 preserved / 8 both-raise / 1 newly-works / 0 regressions)
- Divergence ledger: `divergences.md` (incl. the D3 scope-narrowing note)
- Q9 value pin: `tests/integration/test_integration.py::test_cross_model_measure_with_target_join_filters`
