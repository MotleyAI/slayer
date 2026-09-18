# Tasks

## 1. Tests first (fail-without-fix)

- [x] 1.1 New suite `tests/test_dev1911_fanning_partition_key.py` on the DEV-1900 fixture
      graph (`tests/_dev1900_fixtures.py`), dual-engine (sqlite + duckdb).
- [x] 1.2 Repro A — `regions / [bad_pop] / pop:sum(partition_by=bad_pop)` raises naming
      `region_events` under `broadcast`, `error`, **and** `associate`.
- [x] 1.3 Chained derived — `pop:sum(partition_by=bad_pop2)` raises in all three modes.
- [x] 1.4 Transform position — `rank(pop:sum, partition_by=bad_pop)` raises in all modes.
- [x] 1.5 Re-aggregation inner — `avg(sum(pop, partition_by=bad_pop))` raises in all modes
      (a currently-silent-wrong shape).
- [x] 1.6 Computed-dimension position — a band over `pop:sum(partition_by=bad_pop)` raises
      in all modes.
- [x] 1.7 Windowed position — `pop:sum(window=..., partition_by=bad_pop)` raises in all
      modes.
- [x] 1.8 Filter position (`pop:sum(partition_by=bad_pop) > X`) and ORDER BY position raise
      in all modes.
- [x] 1.9 Cross-model aggregate with a fanning derived HOST partition key raises in all
      modes.
- [x] 1.10 Unanalysable derived key — `partition_by=unparseable` fails closed with a
      message that does not falsely name a hop.
- [x] 1.11 Regressions stay green: safe local derived `derived_pop` (non-fanning)
      partition key still executes; cross-model `partition_by=status` still associates
      (kernel) / broadcasts / errors unchanged.
- [x] 1.12 Diagnostic: both the path-less and the path-bearing
      (`partition_by=customers.regions.bad_pop`) spellings name `region_events`; an
      unreachable partition_by path fails closed upstream (`UnresolvableDimensionJoinError`)
      without falsely naming a hop; an ambiguous path fails closed deterministically
      upstream (`AmbiguousJoinPathError`).
- [x] 1.13 Normative-clause test enforcing the new Axiom 8 clause — `bad_pop` fails closed
      in every mode AND the `status` counterexample still associates.
- [x] 1.14 Codex-review the tests against this plan.

## 2. Implementation

- [x] 2.1 In `assert_partition_key_attributable` (`slayer/engine/join_safety.py`), replace
      the `if not hp: return` early-return with the closure-attributability check from the
      host; raise via `check_partition_key_attributable` with the closure-aware
      `key_broadcast_reason`.
- [x] 2.2 Switch the existing path-bearing branch's reason to `key_broadcast_reason` so it
      names the fanning hop.
- [x] 2.3 Record the StageSchema not-applicable reasoning at the call site (comment) and
      add a staged smoke test if a partition-by-in-stage shape is constructible.
- [x] 2.4 Make every test in section 1 pass.

## 3. Normative harness (explicit approval required)

- [ ] 3.1 Apply the approved Axiom 8 diff in `architecture/semantics.arc42.md` referencing
      the new test id.

## 4. Gates

- [ ] 4.1 Full non-integration suite green (`poetry run pytest -m "not integration"`).
- [ ] 4.2 Integration suite green (CI invocation).
- [ ] 4.3 `poetry run ruff check slayer/ tests/` clean.
- [ ] 4.4 `poetry run python tools/arch_check.py` green.
- [ ] 4.5 `poetry run basedpyright` — no new errors vs baseline.
- [ ] 4.6 Codex full-diff review as the last local gate before push.
- [ ] 4.7 Docs: no user-facing doc change expected (a silent-wrong query now raises);
      confirm during implement and update `docs/` only if a page states the old behavior.
