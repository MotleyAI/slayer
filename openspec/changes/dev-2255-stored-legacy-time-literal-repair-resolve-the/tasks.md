## 1. Tests (pr-tests stage; all fail before implementation)

- [ ] 1.1 `tests/_stored_upgrade_fixtures.py`: seed a `regions` table and a raw `regions` model (DATE `opened_on`, TEXT
      `name`), a `customers → regions` join, and a variant whose `orders ↔ customers` join is declared on `customers`
      only; verify the existing stored-upgrade tests still pass on the new fixtures
- [ ] 1.2 Stored-upgrade tests, YAML and SQLite backends: short-form two-hop route (`regions.opened_on`), reverse edge
      (`customers.signed_up_at` over a join declared on `customers`), extension-declared DATE column on routed
      `regions` — each loads with no legacy fragment and returns the expected total; verify they fail on the current code
- [ ] 1.3 Verbatim tests: a routed TEXT column, an ambiguous short-form route (two routes, neither the sole
      fan-out-free one), and an unloadable model in the datasource not blocking the repair of a reference that does not
      touch it; verify the routed-TEXT and ambiguous cases pass verbatim and the unloadable case fails today
- [ ] 1.4 Quoting test: a case-sensitive model and column reached through double-quoted hop and leaf segments is
      repaired; verify it fails today
- [ ] 1.5 Memory tests: a v2 memory whose query filters a short-form routed DATE column is repaired; with the same model
      in two datasources and an explicit priority, the repair follows the priority datasource; verify both fail today
- [ ] 1.6 Raw-cache hygiene test: loading every model concurrently leaves the raw stored documents and the written-back
      bytes identical across two load orders; verify it runs
- [ ] 1.7 `core.syntax` literal-comparison reader tests: spans and parts for `=`, `==`, `!=`, `<>`, `<`, `<=`, `>`,
      `>=` in both operand orders, parenthesised comparisons, all-literal vs mixed `IN` / `NOT IN`, quoted segments,
      a literal that is a function argument (not reported), unparseable text (empty); splicing each span back yields
      the exact text; verify they fail (reader absent)
- [ ] 1.8 `resolve_path` / `QueryScope.resolve` quoting tests: a quoted hop matches only its exact spelling; callers
      passing no quoting resolve as before; verify they fail (parameter absent)

## 2. `core.syntax` reader

- [ ] 2.1 Record each string-literal token's offset outside node identity (as `Placeholder.start`); verify the full
      `core.syntax` test suite passes unchanged
- [ ] 2.2 Add the public literal-comparison span reader (Pydantic item type), reading through the shared `_read`, empty
      on unparseable text; verify 1.7 passes

## 3. Shared resolver quoting

- [ ] 3.1 Thread optional per-hop quoting through `resolve_path` and `QueryScope.resolve` (root-qualifier drop and
      walk); verify 1.8 and the drift / memory-resolver / `model_reads` suites pass

## 4. Storage

- [ ] 4.1 Factor the dict-upgrade steps of `_model_from_stored` into one method (deep copy of the raw document first)
      used by the load; verify the stored-upgrade suites and the 0.10.2 / 1.8.2 corpus tests pass unchanged
- [ ] 4.2 Typed views: build each datasource's models through 4.1 without refinement, write-back or filter repair,
      validate into `SlayerModel`, skip failures, memoise per load pass; verify 1.6 passes
- [ ] 4.3 Rewrite the repair as a static reader: sites from 2.2, stage list validated into `SlayerQuery`,
      `query_scopes` over the views of the one datasource (model's own; memory's by identity resolution with
      priority), per-site `scope.resolve` with quoting, DATE / TIMESTAMP only, span splice; delete
      `_stored_column_type`, `_query_host` and `_extended`; verify 1.2–1.5 pass
- [ ] 4.4 Reduce `storage/legacy_time_literals.py` to normalisation, date-range repair and the span splice (no sqlglot);
      verify the existing filter-literal and date-range tests pass

## 5. Finish

- [ ] 5.1 Full non-integration suite, `ruff check slayer/ tests/`, `basedpyright` (no new errors vs baseline) and the
      enforcement bundle (`la-arch-check`, LikeC4 validate) green
- [ ] 5.2 `openspec validate dev-2255-stored-legacy-time-literal-repair-resolve-the --strict` green
