## 1. Tests (pr-tests)

Write tests from the specs of this change, on SQLite and DuckDB over the dev2119 fixtures (`tests/_dev2119_fixtures.py`), in `tests/test_dev2136_default_ranking_key.py`; dataset variants (no `orders` default, `orders` default `shipped_at`, `customers` default `signup_ts`) are built in the test module.

- [x] 1.1 Local scenarios — coarser partition (US 60 / EU 50, `region` listed before `e`), time dimension's source (`shipped_at` bucket skipped for its source: US 60 / EU 50), no model default (US 40 / EU 50), nested (US 45 / EU 35), windowed `window='90d'` (US February 60), shifted (US 2025 = 40); verify each fails on `main` for the stated reason
- [x] 1.2 Stage scenario — first stage US 60 by `e`, second stage US 4 by its own `created_at`; verify it fails on `main`
- [x] 1.3 Cross-model scenarios — `customers.updated_at` time dimension (North 100 / South 200), computed `coalesce(customers.updated_at, customers.signup_ts)` (North 300 / South 200), ordered skip of `orders.created_at` for `customers.updated_at` with default `signup_ts` (North 100 / South 200), fallback to default `signup_ts` (North 300 / South NULL; passes on `main` as a regression guard)
- [x] 1.4 Fanning candidate — `accounts` by `[accounts.id, orders.created_at]`, `last(accounts.spend, partition_by=accounts.id)` raises `UnsafeRelationshipInputError` naming `created_at` and `orders`; verify it fails on `main` with the resolution error instead
- [ ] 1.5 Existing default-ranking and ranking-safety suites (`test_dev2119_ranked_expressions.py`, `test_dev1945_ranking_key_safety.py`, `test_dev1868_ranked_cm_exec.py`, `test_dev1836_ranked_windowed.py`, `test_dev1914_ranked_windowed_association.py`, `test_ranked_grain_determinism.py`) still pass unchanged; any that contradicts the plan is raised for approval before alignment

## 2. Planning input (pr-implement)

- [ ] 2.1 `PreboundQuery` carries the ordered ranking candidates (dimension keys then time-dimension keys, projection order, every declared one); binding sets them; `_rewrite_regrouped_prebound` copies them; verify 1.2
- [ ] 2.2 `_regroup_producer_prebound` takes the candidates as a required argument; local regroup, shifted and wrap producers pass the host prebound's; verify 1.1
- [ ] 2.3 Cross-model producer: factor the safe-grain attributability judgement into one helper shared by the grain loop and the candidate list; home-route and re-root each candidate, skipping an undetermined one in order; the pre-check, the kernel and the producer prebound read that list; verify 1.3, 1.4
- [ ] 2.4 Kernels resolve only from the producer prebound's candidates; delete `_producer_row_keys`, `_ranking_row_keys`, `ranking_candidates` and `ranked_planner.ordered_row_keys`; verify LSP finds no references and 1.1–1.5 pass

## 3. Docs and gates

- [ ] 3.1 `docs/upgrading/2.0.md`: one entry (bold headline + one sentence) beside the other `first` / `last` entries; verify `tests/test_doc_examples.py` and the docs guard pass
- [ ] 3.2 `openspec validate dev-2136-first-last-default-ranking-key-read-the-querys-dimensions-in --strict`, `ruff check slayer/ tests/`, `la-typecheck` (no baseline growth) and `la-arch-check` pass
