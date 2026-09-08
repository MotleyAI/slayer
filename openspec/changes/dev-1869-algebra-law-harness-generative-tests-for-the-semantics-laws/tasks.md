# Tasks: Algebra law harness + guard ratchet

## 1. Shared harness

- [ ] 1.1 `tests/_law_harness.py`: `LawShape`, covering-core + seeded `sample_shapes()` (LAW_SEED=1869, N_SHAPES=40, N_DUCKDB=8 slice), collection-time coverage assert, keyed-rows helper (dup detection, NULL-safe approx), law-naming assert helper, expected-raise registry, engine fixture exposing `(dialect, db_path)`; execution-count arithmetic in a comment. Verify: module imports; coverage assert passes; shape ids stable across two collections.

## 2. Law tests (executed, SQLite full + DuckDB slice)

- [ ] 2.1 `tests/test_law_split_invariance.py`: Q vs Q∖{m} per measure — group sets and shared-measure cells identical; guarded variants raise the enumerated message (strict). Verify: file green; a deliberate local oracle perturbation fails naming the law.
- [ ] 2.2 `tests/test_law_grain_union.py`: population half (measure-bearing ≡ dims-only group set) + direct half (mixed-grain formula constant within union-grain cells at finer query grain) + per-operand broadcast constancy within full semantic grain (window ⇒ + time bucket); `cm == 350` filterless-only. Verify: file green on both backends.
- [ ] 2.3 `tests/test_law_broadcast_coherence.py`: 24 seeded (a,b,op) triples, formula vs client-side combine (SQL NULL propagation) at two grains; three-grain chain-composition case. Verify: file green on both backends.
- [ ] 2.4 `tests/test_law_lowering_soundness.py` (assoc-inline half): SLayer `customers.tier = 'gold'` filter vs raw qualified inline-WHERE JOIN oracle on the same DB file, region + month-grain variants, keyed via `month_key`. Verify: green on both dialects.

## 3. Fusion seam + parity

- [ ] 3.1 `SQLGenerator(force_unfused: bool = False)` — immutable constructor flag; synthetic `"forced unfused (test seam)"` blocker in the no-transform branch only. Verify: full non-integration suite + goldens untouched with the flag off.
- [ ] 3.2 Fusion-parity tests in `tests/test_law_lowering_soundness.py`: fused (no WITH) vs forced-unfused (wrap) renders of one planned query, executed raw, keyed compare; double-render stability per mode; docstring scopes the claim to the only fusion decision point. Verify: green on both dialects.

## 4. Guard ratchet

- [ ] 4.1 `tests/test_law_guard_ratchet.py`: sql+engine AST scan; coexistence empty; deferral class (`DEV-\d+` required + exact `DEFERRAL_SITES` + count == `guards.baseline`); expressiveness allowlist (reclassified deferrals out, engine residue arms in); empty-message raises red. Absorb needles/lineage from `tests/test_dev1838_sweep.py`, then delete it. Verify: gate green; injecting a scratch unenumerated raise turns it red (acceptance criterion).
- [ ] 4.2 Message re-points: 389/399/405/353/2496/3648 → DEV-1868; 1080/1092/3685 → DEV-1847; 5472 → DEV-1878; 287 drops `(DEV-1839)`. Add `guards: {baseline: 10}` + only-ever-lower comment to `architecture/index.yaml`. Verify: ratchet green.
- [ ] 4.3 Re-bless recorded-raise golden keys touched by 4.2 via per-module `ALLOWED_DELTAS` + `SLAYER_UPDATE_GOLDEN`; manifests emptied after. Verify: golden files green, manifests empty.

## 5. arc42 + wrap-up

- [ ] 5.1 Tag flips in `architecture/semantics.arc42.md` §3 (laws 1, 2 enforced; 3, 6 per-clause splits per design D7). Verify: `poetry run python tools/arch_check.py` green.
- [ ] 5.2 Full gate: `poetry run pytest -m "not integration"`, `poetry run ruff check slayer/ tests/`, enforcement bundle (`lint-imports`, arch_check, likec4 validate, basedpyright vs baseline). Verify: all green; harness wall-clock within budget (~20s).
