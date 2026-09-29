## 1. Tests (pr-tests)

- [ ] 1.1 Add `tests/_reagg_outer_grain_fixtures.py` (customers / account_snapshots, SQLite + DuckDB seeding, query helpers; no issue numbers in names) and a smoke test re-deriving every design.md oracle from the raw rows; verify the smoke test passes on HEAD
- [ ] 1.2 Add `tests/test_reagg_outer_grain_attribution.py` covering every spec scenario with the design.md oracles on SQLite and DuckDB (sum / max / last / windowed inners, plain to-many dim, month-only, associate, default-mode and error-mode pins incl. the stamped outer grain excluding the broadcast dim, explicit case 1 / case 2 / case 3 incl. the sales fixture, positions, count empty value, cardinality invariance, expression-determined band, keyless, depth-3, computed dimension, transform input, aggregate parameter, fanning constituent key still erroring, one outer answer slot, no `__regroup__` leak); verify each fails on HEAD for the intended reason (IndexError / PartitionKeyError / old broadcast) and pins pass
- [ ] 1.3 Register the new `PartitionKeyError` raise site in `tests/_dev1871_raise_ledger.py` so raise-parity covers it; verify the parity test fails until the checker function exists
- [ ] 1.4 Codex-review the tests against this plan and resolve findings

## 2. Implementation (pr-implement)

- [ ] 2.1 Merge origin/main first (DEV-2006 touches the carrier and `_producer_nesting_rule`); verify the full unit suite state is unchanged apart from the new tests
- [ ] 2.2 Add the checker function in `slayer/engine/elaborate_env.py` taking resolved facts (alias, undetermined explicit key displays, operand grain display, mode) and raising `PartitionKeyError` with the remedy (D4); verify raise-parity passes
- [ ] 2.3 In `_synthesize_reaggregation_producer`: route undetermined explicit outer keys to associate in `"associate"` and to the checker raise otherwise; default members unchanged (D1); verify the case-2 / case-3 tests pass
- [ ] 2.4 Build the one canonical stamped `outer_agg` after mode resolution and use it for `aggs`, `public_alias_by_agg`, `explicit_types` (D2); verify the time-bucket, to-many, associate, keyless and expression-grain tests pass
- [ ] 2.5 Replace `outer_plan.aggregate_slots[0]` with the keyed `_regroup_answer_slot_id` lookup on the stamped key (D5); verify the plan-structure test passes
- [ ] 2.6 Skip `assert_partition_key_attributable` for `is_reaggregation_key(key)` in `_validate_partition_keys` (D3); verify case-1 and the constituent-key-still-errors tests pass
- [ ] 2.7 Apply the approved arc42 edits to `architecture/semantics.arc42.md` (Axiom 8 `host` → `home`; Axiom 7 enforced tag) exactly as in design.md D7; verify `la-arch-check` (pinned uvx form) passes
- [ ] 2.8 Append the D6 sentence to the re-aggregation section of `docs/concepts/formulas.md`; verify no other doc states the old outer-`partition_by=` rule (grep)
- [ ] 2.9 Run the full unit suite, the integration suite with CI settings, `ruff check slayer/ tests/`, basedpyright (no new errors vs baseline) and the conventions gate; any existing test pinning the old explicit-key behaviour → STOP and ask
