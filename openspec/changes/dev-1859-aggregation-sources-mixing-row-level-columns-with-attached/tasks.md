## 1. Fixtures and failing tests — first pass (done)

- [x] 1.1 Extend `tests/_dev1847_fixtures.py` ADDITIVELY: `quantity` and
      `unit_price` columns on `sales`, the same 15 rows widened in both the
      sqlite and duckdb seeds; hand-computed dev1859 oracle constants.
- [x] 1.2 `tests/test_dev1859_row_mixed_exec.py` (SQLite + DuckDB): base oracle by
      region; ungrained inner; coalesce-NULL pin; row-filter inheritance;
      count/count_distinct; cross-model inner; filter/order positions;
      grain-self-contained dimension; cardinality invariant; no placeholder leak;
      `assert_scope_closed`.
- [x] 1.3 Outer-modifier tests: outer `partition_by=`, outer `window='90d'`,
      `wavg(..., weight=qty)`, a custom aggregation, `corr(mixed, qty)`.
- [x] 1.4 `tests/test_dev1859_plan_structure.py`: mixed root not a re-aggregation
      root; constituents row-phase; outer slot stays an aggregate; no producer
      GROUP BY contains the placeholder or the row leaf; `first(mixed)` error.
- [x] 1.5 `tests/test_dev1859_transform_row_leaf.py`: rejection matrix over the
      derived non-shift set; projected-grain-key legality; shift family
      unchanged; plan-time raise; ledger pins.
- [x] 1.6 CONSENTED re-points: `tests/test_dev1847_gate.py::
      test_mixed_row_and_attached_rejected` and `tests/test_expression_aggregations.py
      ::TestExpressionErrors::test_mixed_row_and_attached_source_rejected` flipped.
- [x] 1.7 `rank(<CASE expr>)` with the same CASE projected as a computed dimension
      executes at the query grain (design decision 7).

## 2. Leg A — parse gate, classifier, row-grain branch (done, re-cut in §5)

- [x] 2.1 Delete the mixed-source branch in `syntax.py:_validated_agg_source`.
- [x] 2.2 Source-only classifier beside `operand_aggregates`; mixed roots excluded
      from re-aggregation discovery.
- [x] 2.3 Mixed block in `_plan_regroups`; inherited filters threaded.
- [x] 2.4 Pure-root scoping of the re-aggregation checkers; outer modifiers wired.
- [x] 2.5 first/last first-arg dispatch narrowed (design decision 6).

## 3. Leg B — the non-shift transform type rule (done, walker re-cut in §5)

- [x] 3.1 `check_non_shift_transform_row_leaf` in `elaborate_env.py`, fired from
      `bind_query_inputs` with the projected grain keys.
- [x] 3.2 Ledger row in `tests/_dev1871_raise_ledger.py`.
- [x] 3.3 Computed-dimension leaves: whole-key slot lookup in the generator's
      transform-layer readiness; step render reads the slot alias.

## 4. Second-pass failing tests (spec-tests stage) — done

- [x] 4.1 Classifier unit tests (`tests/test_dev1859_classifier.py`):
      `attached_inputs`, `is_reaggregation_key` (pure), `is_row_attach_root`,
      `attached_operand_keys`, `walk_consumer_keys`. Keys constructed directly;
      module reds via collection ImportError until the §5.1 API lands.
- [x] 4.2 Determination unit tests (`tests/test_dev1859_determination.py`):
      direct `grain_determines` — recursive arm (to-one/aggregate-valued partition
      keys) red until §6.1; conservative negatives + column-arm control are stable
      green pins.
- [x] 4.3 Plan-structure pins (extend `tests/test_dev1859_plan_structure.py`):
      mixed + parameter = two ROW attaches / no combined attach (red — the param
      escapes as a combined consumer today); banded computed dimension → one attach
      (red — 2 today, inner not yet nested); dedup shared source/parameter and
      across two measures = one producer (green guards).
- [x] 4.4 Leg C executed suite (`tests/test_dev1859_attached_param_exec.py`,
      SQLite + DuckDB): associate headline + c4 NULL cell; ordinary kernel (global,
      distinguishable); mixed + parameter; ungrained × three kernels via `wsum`
      (added locally in `tests/_dev1859_fixtures.py`); literal source; default-mode
      twin (executes + warns — exact broadcast oracle pinned at implement per
      decision 14) and error-mode refusal; cross-model parameter on a local root +
      fanning-hop residue; filter/order positions; cardinality / placeholder /
      scope-closed. Oracles are raw-row reductions in `_dev1859_fixtures.py`.
- [x] 4.5 Windowed constituent `sum(qty * sum(revenue, window='90d'))` on dev1846
      (added to `tests/test_dev1859_row_mixed_exec.py`) — GREEN guard: leg A already
      compiles it; oracle derived from the plain windowed measure (§5.3 preserves).
- [x] 4.6 Generator scope-render pins (`tests/test_dev1859_scope_render.py`):
      cross-model constituent producer and a derived-of-derived-across-a-join
      constituent each emit the join ONCE — green guards locking §5.4's one-renderer.
- [x] 4.7 Leg B walker unit test (`tests/test_dev1859_walker.py`, top-level import
      of the §5.5 `_first_row_leaf` — reds via collection): partition/time keys are
      not row leaves, aggregates opaque, exempt composites legal. Behaviour guards
      (aggregate input, all-projected composite) added to the transform file (green).
- [x] 4.8 CONSENTED re-point (2026-09-15):
      `tests/test_dev1892_residue.py::TestAttachedParameterOnRowLevelSource` →
      executed in associate/default, refusal in error mode (red). NOTE: the ledger
      row removal was left with §6.3 (checker deletion) so raise-parity stays green
      during the red phase — removing it now would red the bookkeeping harness, not
      the feature.
- [x] 4.9 Golden: `tests/test_dev1859_golden_sql.py` + `tests/golden/
      dev1859_sql_baseline.json` in the DEV-1892 pattern — mixed cases generate,
      param cases record raises; `test_lifted_cases_generate_not_raise` is the
      feature-missing tripwire (blessed in §6).

## 5. Structural re-cut (spec-implement)

- [x] 5.1 `core/keys.py`: `attached_inputs`, pure `is_reaggregation_key`,
      `is_row_attach_root`, `attached_operand_keys` (rename), `walk_consumer_keys`;
      delete `is_mixed_source_key`; update every consumer (`bind_inputs.py`,
      `stages.py`).
- [x] 5.2 Opacity: `ir/bound.py` combined-consumer walk (+ order / filter
      variants), `dimension_regroup_roots`, `dimension_partitioned_aggregates`;
      `stages.py::_bare_combined_roots`, the `_local_broadcasts` scan; one
      `_discover_roots(prebound, predicate)`; the regroup block on an explicit
      disposition, list-subtraction steps deleted. Plus: a LOCAL row-attach root
      whose `partition_by` equals the query grain aggregates inline.
- [x] 5.3 `_answers_need_nested_regroups` at the carrier, regroup producer,
      re-aggregation outer producer, dimension wrap and association producer;
      grain-key and windowed clauses untouched.
- [ ] 5.4 Generator: `_render_expression_source_sql(scope=…)` via
      `scope.resolve`; private resolver + dead guard deleted; `attached_columns`
      removed from the five signatures; `scope` required and passed at every
      caller incl. `_composite_agg_builder`, `_filter_agg_builder`, the time-shift
      leaf path (`shifted_scope`).
- [x] 5.5 `_first_row_leaf(key, *, exempt)` shared by `_check_shift_family_key`
      and `check_non_shift_transform_row_leaf`.
- [ ] 5.6 Verify: 4.1, 4.3, 4.5, 4.6, 4.7 green; full non-integration suite green.

## 6. Leg C (spec-implement)

- [x] 6.1 `join_safety.py::grain_determines`: recursive aggregate arm; an
      ungrained parameter is always determined (types at the query grain); the
      association site types grained parameters against the ENTITY grain; the
      re-aggregation site normalises an ungrained parameter to the outer grain.
- [x] 6.2 `_synthesize_association_producer`: nested discovery per 5.3; attached
      inputs stripped from the input-safety hop check; after compile, map each
      `PickedParam.key` through the plan's substitutions.
- [x] 6.3 Delete `check_attached_param_requires_attached_source`,
      `_attached_param_on_row_source`, both call sites, the ledger row.
- [x] 6.4 Default-mode twin (decision 14, B chosen 2026-09-15): the host-rooted
      parameter inside a target-rooted broadcast producer is a typed error
      (`check_attached_inputs_attributable`, raised before the producer compiles;
      error mode's dimension refusal wins); the executed twin is a strict xfail
      pointing at DEV-1906 (issue + worktree cut); a target-side parameter
      executes as the control. Consented re-points: the two default-mode pins,
      `test_dev1841_surfaces::test_association_slayer_error_maps_to_400`.
- [x] 6.5 Verify: 4.2, 4.4, 4.8 green; DEV-1892 / DEV-1841 / DEV-1847 suites green.
- [x] 6.6 Found on the way: a quoted four-segment dotted stage column re-parses on
      BigQuery into a `Dot` under the qualifier slots, which
      `unmangle_dotted_table_refs` skipped (pre-existing: DEV-1847 re-aggregation
      over a cross-model partition key leaked the same way) — the repair now
      flattens the `Dot` chain.

## 7. Specs, goldens, docs, harness, gates

- [x] 7.1 Spec deltas reconciled: expression-aggregation MODIFIED carries the
      broadcast-refusal clause + the "rejected" scenario; partitioned-aggregates
      default-mode-twin scenario flipped to the typed error; `openspec validate
      … --strict` valid. The MCP `server.py` "mixing row-level columns" clause is
      already absent (removed pre-branch) — nothing to delete.
- [x] 7.2 Blessed `tests/golden/dev1859_sql_baseline.json` (param cases generate;
      manifest emptied); all 21 golden suites byte-stable.
- [ ] 7.3 Flip `architecture/semantics.arc42.md` axiom-6 `[target: DEV-1859]` →
      `[enforced: test:tests/test_dev1859_row_mixed_exec.py]` — present the exact
      one-line diff for approval first; `poetry run python tools/arch_check.py`.
- [x] 7.4 Docs: `docs/concepts/queries.md` associate paragraph (attached
      parameter the entity grain determines + the broadcast caveat);
      `docs/concepts/formulas.md` parameter-form sentence.
- [x] 7.5 Gates: full non-integration suite (18024 green); SQLite/DuckDB
      integration green; ruff clean; conventions gate CLEAR; `tools/arch_check.py`
      OK; basedpyright baseline shrank by 1 (not grown); `openspec validate …
      --strict` valid; Codex pass on the working tree — no material findings.
- [ ] 7.6 PR against main if #394 has merged, else against the 1892 branch and
      retarget when it lands; merge only, never rebase.
