## 1. Fixtures and failing tests (spec-tests stage)

- [ ] 1.1 Build `tests/_dev1847_fixtures.py` with a denormalized city/region/product
      dataset (duplicate city names across regions, nullable city, all-NULL value
      pockets) and a normalized customer→region fixture for chain determination;
      verify by a fixture smoke test asserting the hand-computed oracle values.
- [ ] 1.2 Write executed-value tests (SQLite + DuckDB) for the oracle
      (`avg(sum(amount, partition_by=[city, region]))` by region), the broadcast
      twin (`partition_by=city`, warning payload asserted), the associate twin,
      error mode, and the degenerate-identity warning; all failing.
- [ ] 1.3 Write tests for the composite sparse carrier, chain-determined outer key,
      expression-grain non-transitivity, explicit outer `partition_by=` (broadcast
      composition + unattributable explicit key per mode), and the outer operator
      family (`count`, `count_distinct`, `percentile`, custom, `min`/`max`); all failing.
- [ ] 1.4 Write null/empty/keyless pin tests (null grain cell, all-NULL values,
      `partition_by=[]` identity), depth-3 nesting, shared-producer interning
      across depths, shape B (band-partitioned measure + nested-in-dimension),
      cross-model inner, and transform-over-re-aggregated-value tests; all failing.
- [ ] 1.5 Write parser/binder gate tests: fully-attached source accepted; row-mixing,
      nested-transform, outer `window=`/ranked, outer `filter=` rejected with typed
      errors; `first(sum(...))`/`last(sum(...))` still dispatch as transforms; all failing.
- [ ] 1.6 Write the equivalence-sweep test vs the manual multi-stage `source_queries`
      encoding and the cardinality/no-placeholder-leak/`assert_scope_closed`
      invariant tests for the new shapes; all failing.

## 2. Parse and binding

- [ ] 2.1 Narrow `syntax.py:_validated_agg_source` to reject only transforms and
      mixed row/attached sources (typed errors with remedies); verify by the 1.5
      gate tests passing at the parse/bind layer.
- [ ] 2.2 Add binding-time re-aggregation discovery on typed keys (operand subtree
      resolves entirely to attached values → re-aggregation; classify constituents
      and the union grain); verify by plan-shape unit tests.

## 3. Planning

- [ ] 3.1 Type the operand dataset: union-grain computation, default-grain (query
      dimensions) constituents, attributability = grain membership + seeded
      determination walk from entity-key grain fields; verify by 1.3 tests.
- [ ] 3.2 Route unattributable outer dimensions through the DEV-1841 disposition
      classification (broadcast/associate/error, warning payloads); verify by the
      1.2 mode-twin tests.
- [ ] 3.3 Lift `_validate_nested_producer_plan`: remove the depth-1 and
      strict-subset arms, admit recursion by the general complete-grain rule;
      remove `_reraise_nested_attach` and plan shape B by row-attaching the
      dimension producer inside the outer producer; verify by 1.4 tests.
- [ ] 3.4 Add the degenerate-re-aggregation typed warning (operand grain, outer
      grain, remedy; once per semantic event); verify by warning-dedup tests.

## 4. SQL generation

- [ ] 4.1 Extend the two-level kernel: level-1 carrier (population distinct
      union-grain cells + null-safe constituent LEFT joins), level-2 outer
      aggregate grouped by the outer grain; determination join and association
      join variants; verify by golden SQL and the executed oracle tests.
- [ ] 4.2 Wire the outer producer through interning, the CTE-hoist, and the
      combined/row attach rules at arbitrary depth; verify by depth-3, flat-WITH,
      and shared-producer tests.

## 5. Guard ratchet and architecture bundle

- [ ] 5.1 Remove the three DEV-1847 entries from `tests/_law_harness.py`
      `DEFERRAL_SITES` and lower `architecture/index.yaml` `guards.baseline` to 5;
      verify by `test_law_guard_ratchet` passing.
- [ ] 5.2 Flip the axiom-6 second-order clause in `architecture/semantics.arc42.md`
      to `[enforced: test:...]` naming the oracle test; verify by
      `poetry run python tools/arch_check.py`.
- [ ] 5.3 Run the full enforcement bundle (`poetry run lint-imports`,
      `tools/arch_check.py`, `npx -y likec4@1.47.0 validate architecture`,
      `poetry run basedpyright`) and fix regressions.

## 6. Docs, goldens, and closure

- [ ] 6.1 Bless golden SQL baselines for the new shapes and record any divergences
      per the ledger protocol; verify by the golden suites.
- [ ] 6.2 Update `docs/concepts/formulas.md`, the aggregation example page, and
      `.claude/skills/slayer-query.md` with the re-aggregation surface (one
      concise sentence each plus a runnable example); verify pages are in
      `zensical.toml` nav.
- [ ] 6.3 Run the full non-integration suite plus the SQLite/DuckDB integration
      files touched; verify green, then `openspec validate
      dev-1847-aggregate-over-attached-lift-the-nested-attach-shape --strict`.

## 7. Carry-over from the spec-tests stage (must close before the PR)

Recorded at spec-tests so they survive a session reset; each is blocking.

- [ ] 7.1 Update the existing `tests/test_expression_aggregations.py::test_nested_aggregation_rejected`:
      `sum(sum(amount))` becomes a legal degenerate re-aggregation when the gate
      narrows (task 2.1), so re-point its operand to a still-rejected shape
      (nested transform, or mixed row+attached). Sign-off granted at spec-tests
      (Egor) — apply in stage 3, no further ask.
- [ ] 7.2 Add the three failing tests deferred from stage 2 (needing more fixture
      surface): a cross-model inner aggregate, expression-grain non-transitivity
      (a time-bucket / computed-dim inner grain determines only itself), and a
      computed-dimension consumer of a re-aggregated value.
- 7.3 RESOLVED (Egor, spec-tests): the outer `window=` half is covered and stays;
      an outer measure-local `filter=` has no DSL surface to express it, so the
      rejection clause is unreachable and gets NO test — ship one only if a
      `filter=` surface is ever introduced. The four pre-existing combined-consumer
      scenarios restated in the partitioned-aggregates MODIFIED requirement stay
      covered by the existing DEV-1763/1838 suites (no new tests).
