# Proposal: Algebra law harness + guard-list monotonicity gate

## Why

The query-semantics laws (`architecture/semantics.arc42.md` §3) are enforced only indirectly: a violation surfaces as "a golden changed", not "law N broke". This change turns the laws into CI — direct generative/named tests per law — and adds a monotonicity gate over fail-closed guard sites so the deferral list can only shrink.

## What Changes

- New seeded-deterministic law harness (`tests/_law_harness.py` + four `tests/test_law_*.py` files): split-invariance, grain union, broadcast coherence, lowering soundness — executed on SQLite (full) + DuckDB (spot slice).
- New guard ratchet `tests/test_law_guard_ratchet.py` absorbing and deleting `tests/test_dev1838_sweep.py`: AST scan of `slayer/sql` + `slayer/engine`; coexistence arms stay banned; deferral sites enumerated exactly, each message required to carry a live `DEV-\d+` ref; expressiveness regex allowlist retained; deferral count pinned to a new `guards: {baseline: N}` ratchet in `architecture/index.yaml`.
- Production seam: `SQLGenerator(force_unfused: bool = False)` — a synthetic fusion blocker so fused-vs-unfused parity is testable on identical input. No default-path behaviour change.
- Deferral messages re-pointed to live issues (old refs all closed): cross-model partition_by family + mixed-filter routing + AGGREGATE-phase composite + transform-as-dimension → DEV-1868; nested-attach shapes → DEV-1847; query-backed render → DEV-1878; time-axis arm loses its ref (typed residue). Affected recorded-raise golden keys re-blessed per-key.
- arc42 tag flips in `semantics.arc42.md` §3: laws 1, 2 → enforced; laws 3, 6 → per-clause enforced/review splits.

## Capabilities

### New Capabilities

None — the harness pins behaviour already specified in `openspec/specs/queries/semantics` and the laws in `semantics.arc42.md`; no new user-facing behaviour.

### Modified Capabilities

None — no spec-level behaviour changes (`skip_specs: true`): tests, a meta-gate, a test-only emission seam, and error-message ref edits.

## Impact

- `tests/`: 5 new law files + 1 shared harness module; `test_dev1838_sweep.py` deleted (absorbed).
- `slayer/sql/generator.py`: `force_unfused` seam; two message-ref edits. `slayer/engine/stage_planner.py`, `slayer/engine/regroup_planner.py`: message-ref edits only.
- `architecture/index.yaml`: `guards` baseline. `architecture/semantics.arc42.md`: tag flips.
- `tests/golden/`: per-key re-bless of recorded raises touched by message edits (dev1740_regroup / dev1824 / dev1839 baselines).
- Runtime: ~15–20s added to the unit suite (all constants in `tests/_law_harness.py`).
