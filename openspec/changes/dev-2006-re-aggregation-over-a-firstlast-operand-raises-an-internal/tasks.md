## 0. Gate

- [x] 0.1 Before any implementation task (§2 onward): `git fetch` + merge `origin/main` once it contains BOTH DEV-1976 and DEV-1994 (verify with `git log origin/main --oneline --grep=1976` and `--grep=1994` showing their merge commits); re-read their archived specs for any drift against design.md, and re-run this change's tests to confirm they still fail only for this change's reasons

## 1. Tests (pr-tests stage — all fail before implementation)

- [x] 1.1 Create `tests/_dev2006_fixtures.py`: the snapshot fixture from the spec delta (`account_snapshots` with `recorded_at` + `customers`, SQLite and DuckDB seeds via `tests/_engine_helpers.seeded_exec_engine`), every oracle hand-computed in the module docstring; verify both seeds import and load
- [x] 1.2 Create `tests/test_dev2006_kernel_operands.py` (unit suite, `exec_engine` param `["sqlite", "duckdb"]`): one executed test per spec scenario — semi-additive sum; implicit / explicit (`recorded_at`) ranking; avg / count / sum(first); `last - first`; two `last`s with different ranking keys in one operand; ranked + plain mix; month-bucketed operand (explicit + implicit); windowed operands incl. two durations, each equal to the single-stage windowed measure; every consumer position; cross-model sum + count (C = 0 per DEV-1994); broadcast / associate / error modes; verify each fails on HEAD with the internal `RuntimeError` / rendered-schema `ValueError` (not a fixture bug)
- [x] 1.3 Plan-structure tests (plan level, no DB): for the semi-additive sum the carrier carries a nested attach with a ranked kernel and no ranked aggregate slot of its own; distinct ranking keys / windows get distinct kernel producers; a structural twin interns to ONE producer; `assert_scope_closed` passes
- [x] 1.4 Regression pins: cross-model associate-mode `first`/`last` still raises the typed `AssociationError`; shifted own-grain vs non-own-grain kernels and cross-model ranked / windowed kernels unchanged (existing suites named in 4.3 cover the rest)
- [x] 1.5 Codex-review the test suite against the spec delta and design.md; resolve findings with the user

## 2. Kernel decision and nesting invariant (D1–D3)

- [ ] 2.1 Add the one typed kernel decision (design D2: association → trailing-window → ranked; explicit inputs incl. the shifted site's own-grain / host-local / kernel-root conditions) and route the three kernel sites (local regroup, cross-model target-rooted, shifted own-grain) through it, deciding BEFORE compile; verify the existing ranked / windowed / shifted / cross-model suites pass unchanged
- [ ] 2.2 Add `ProducerContext.kernel_answer`, pass it from each kernel site into `compile_synthesized`, and make `_producer_nesting_rule` nest every kernel-requiring root at the producer grain other than the kernel answer; delete the two carve-outs; verify 1.2 and 1.3 pass

## 3. Plan-time invariant (D4)

- [ ] 3.1 Carry `kernel_answer` on `_Routed`; in `_emit_planned` assert no kernel-requiring aggregate key (aggregate slots and combined-expression slots, walked) other than it; keep the generator guard unchanged; verify with a unit test that a hand-built violating plan trips the assertion and the full suite stays green

## 4. Docs, architecture, gates

- [ ] 4.1 `docs/concepts/formulas.md`: one sentence after the re-aggregation paragraph — a `first`/`last` or windowed aggregate is a legal re-aggregation operand (e.g. last balance per account summed per customer); page already in `zensical.toml` nav
- [ ] 4.2 `architecture/sql.arc42.md` P10: add `[enforced: test:tests/test_dev2006_kernel_operands.py]` after `[review]` (approved exact edit); verify `uvx --no-build --from living-architecture==0.2.0 la-arch-check` passes
- [ ] 4.3 Full unit suite `poetry run pytest -m "not integration" -n auto` (goldens byte-identical — any diff is a STOP, design D5), the CI integration invocation, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no baseline growth), `openspec validate dev-2006-re-aggregation-over-a-firstlast-operand-raises-an-internal --strict` — all green
