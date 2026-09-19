## 1. Tests first (spec-tests stage)

- [ ] 1.1 `tests/test_dev1935_boolean_lowering.py`: executed-value oracles on SQLite + DuckDB for every
      new scenario in `specs/queries/semantics` and `specs/queries/cross-model-aggregates` (460/475/370/55/
      320/420, six raw rows, partitioned gold 190 / silver 230 / bronze 40, DEV-1840 gold 160 / silver 230 /
      bronze 40, association ok 270 / new 250 and ok 270 / new 100, materialised-branch cells {1,3,5,7,9,10},
      two-spellings gold 60 / silver 80), each parametrised over the three `to_many_handling` modes where the
      spec says "any mode"; assert the `semi_join_pushed` entries, no dropped-filter warning, no Python
      warning; verify each fails on current main (raise or defect value) and passes after implementation
- [ ] 1.2 Same file: structural pins — grouping merges on a shared branch and keeps first-appearance order
      (generate twice, compare SQL); `null_extended` false on every hop of every DEV-1840/1909 fixture push;
      the EXISTS never appears as an operand of OR; the spine shape only for a non-rejecting predicate;
      declared-INNER descendant under a nullable parent (forward and reverse) on a fixture variant with an
      event-less region; the population's dropped arm asserted impossible
- [ ] 1.3 Same file: unit tests of the null-rejection analysis — comparison/IN/BETWEEN → UNKNOWN,
      `is null`/`is not null` both operand orders, `is <non-null literal>`, scalar call → DEPENDS, NOT over
      each value, AND/OR tables, TimeTrunc and Between date ranges, StarKey, root-local Mode-A column with
      cross deps (bare-column fragment → null-valued; function fragment → DEPENDS), host-side ref on the
      reverse chain
- [ ] 1.4 `tests/test_dev1935_golden_sql.py` + `tests/golden/dev1935_sql_baseline.json`: spine-shape and
      product-shape goldens per Tier-1 dialect (mixed OR, multi-branch, cross-branch atom, partially
      materialised host base, `is null`); byte-identity tripwire over the existing dev1840/dev1909/dev1900/
      dev1747 baselines with `excluded/mixed_or` in ALLOWED_DELTAS and its reason recorded
- [ ] 1.5 Modify the consented tests (design decision 11) so they assert the new behaviour, and re-point
      the three dedup files to `semi_join_pushed` entry dedup; remove the ledger row; run
      `poetry run pytest -m "not integration"` and record the intended-red set

## 2. Planner (spec-implement stage)

- [ ] 2.1 `slayer/ir/planned.py`: `SemiJoinHop.null_extended: bool = False`; verify existing goldens
      unchanged
- [ ] 2.2 `stages.py`: canonical hop tokens in `_forward_hops` / `_reverse_hops` / `_remap_ref_path`
      (edge name, else target model); verify the two-spellings test and the golden corpus
- [ ] 2.3 `stages.py`: delete `_reject_mixed_or_not` and the single-first-hop block in
      `_conjunct_push_plan`; verify the DEV-1840 disposition tests flip to pushed
- [ ] 2.4 `stages.py`: union-find grouping in `_semi_join_groups_from_pushes` with first-appearance
      order; verify the grouping and determinism pins
- [ ] 2.5 `stages.py`: the null-rejection analysis (design decision 5) setting `null_extended` per hop
      after grouping; verify 1.3 and `null_extended` false on every existing fixture push
- [ ] 2.6 `stages.py`: `PopulationFilters` two-way with the asserted dropped arm; delete the residue
      check, `drop_excluded`, `dropped_warnings`; per-branch materialisation in `host_split` (reduced push
      for a partially materialised conjunct); verify the DEV-1909 residue tests and the materialised-branch
      scenario
- [ ] 2.7 `slayer/engine/elaborate_env.py`: delete `check_population_filter_in_pushdown_scope`; verify
      the ledger and `tests/test_dev1871_raise_parity.py` pass
- [ ] 2.8 `stages.py` association arm: confirm no change needed beyond 2.3; verify the association
      scenarios (ok 270 / new 250, ok 270 / new 100)

## 3. Renderer

- [ ] 3.1 `slayer/sql/generator.py::_build_semi_join_exists`: CROSS-joined further first-level hops with
      WHERE correlations; per-hop INNER/LEFT for deeper hops; verify byte-identity of every existing golden
- [ ] 3.2 Same builder: the spine shape (`FROM (SELECT 1 AS one) AS <alias>`, correlations in ON,
      allocator-reserved alias, scope-closure validator updated); verify the dev1935 goldens on every
      Tier-1 dialect and `assert_scope_closed`
- [ ] 3.3 Refs on a materialised branch render against the outer alias; verify the materialised-branch
      scenario SQL joins `orders` once, outside the EXISTS

## 4. Docs, harness, gates

- [ ] 4.1 `docs/concepts/queries.md` (one sentence: product rule, `is null` idiom; drop the OR/NOT
      exclusion) and `docs/database-support.md` (one sentence: MySQL 8.0.20+, BigQuery); verify with
      `grep -n "OR\`/\`NOT" docs/` finding nothing stale
- [ ] 4.2 `architecture/semantics.arc42.md` tags (Axiom 3, Axiom 14, Law 5, Law 6) — present the exact
      diff and apply only on explicit approval; verify `poetry run python tools/arch_check.py`
- [ ] 4.3 Full gates: `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`,
      `poetry run basedpyright` (no new errors), `poetry run python tools/arch_check.py`,
      `npx -y likec4@1.47.0 validate architecture`; then the CI integration invocation
