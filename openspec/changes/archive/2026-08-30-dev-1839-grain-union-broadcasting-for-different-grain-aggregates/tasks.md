# Tasks

## 1. Tests first (TDD — land before implementation)

- [x] 1.1 Fixture data where union-grain vs wrong-grain evaluation differ visibly (regions × cities × months with hand-computed oracles for every scenario); verify by importing the fixtures in a smoke test
- [x] 1.2 Executed-value tests (SQLite + DuckDB) for every `queries/computed-dimensions` scenario: mixed-grain rank, keyless share-of-total, subset grain (`rank(a:sum(partition_by=[region, city]) - a:sum(partition_by=region))`), nested transform at own grain (`rank(cumsum(a:sum(partition_by=[region, ordered_at])) - b:sum(partition_by=city))`), explicit transform partition over union rows, dimension+measure dual role; each with `assert_scope_closed`; verify they fail with the current guard message
- [x] 1.3 Cardinality + plan-structure test (D8): with/without the mixed-grain dimension returns identical shared columns; exactly one union-grain row attach whose producer carries the expected strict-subset combined attach(es); verify it fails today
- [x] 1.4 Guard tests: mixed windowed-grain and mixed first/last-grain → exact DEV-1835 message; transform kwarg outside the union → clean reference error; cross-model inner source unchanged; bare-aggregate self-containment unchanged; D9 temporal-axis containment (mixed AND single-grain forms — the single-grain form today returns duplicated rows, so its test fails by exhibiting the defect); verify each raises or fails as stated
- [x] 1.5 Locking tests for D6's actual single-grain non-regression surface: transform root over multiple same-grain plain aggregates (e.g. `rank(amount:sum(partition_by=region) + ok_amount:sum(partition_by=region))`, new oracle) — CASE-wrapped windowed/first-last dimension shapes stay pinned by the DEV-1824 suites; verify green before implementation
- [x] 1.6 Tests for `queries/partitioned-aggregates` scenarios — locking (green today): plain+partitioned mix, transform-as-measure over mixed grains, mixed-grain filter; TDD (broken today, leaked-placeholder error): mixed-grain measure arithmetic and same-grain partitioned arithmetic (D10); verify the locking set is green and the TDD set fails with the placeholder-leak message
- [x] 1.7 Flip `test_transform_over_mixed_grain_aggregates_deferred` to a positive lift assertion (sanctioned by the issue's acceptance); verify it fails before implementation
- [x] 1.8 Golden SQL test for the acceptance shape incl. SQL Server + BigQuery emission; DEV-1837 compatibility matrix gains the mixed-grain dimension family row (supported cells execute; windowed/ranked-mix cells strict-xfail at DEV-1835); verify matrix collects

## 2. Planner

- [x] 2.1 `regroup_root_grain` returns the union of all grained inner aggregates (D1); verify via planner unit tests
- [x] 2.2 Producer-planning control `enable_producer_regroups` (D3): regroup discovery inside producers with host-rooted isolation still disabled; verify no golden divergence for single-grain shapes
- [x] 2.3 Recursive root classification inside producers with own-grain exclusion (D2): inline at own grain, nested combined attach for strict-subset bare aggregates and transform roots; verify scenarios 1.2 pass at plan level
- [x] 2.4 Remove the mixed-grain guard; add the D6 mixed windowed/first-last guard (>1 distinct grain first); add the D9 temporal-axis containment guard; verify 1.4/1.5
- [x] 2.5 Structural validation of admitted nested producer plans (D4); verify targeted unit tests for each rejected shape
- [x] 2.6 D10 — classify all-placeholder regroup-substituted composites to the combined stage and render over attached values; plain+partitioned mixes keep their current path; verify 1.6 TDD set + goldens

## 3. Renderer

- [x] 3.1 Hoistable-producer render context (D5): combined attaches legal when the internal WITH hoists; `_render_producer_split` uses it; the CTE-body guard keys on genuinely non-hoistable contexts only; verify golden + SQL Server emission tests
- [x] 3.2 Grain-coverage assert at every nesting level; verify structure test 1.3

## 4. Full-suite verification

- [x] 4.1 Full non-integration suite green; goldens byte-identical except individually approved divergences; `ruff check slayer/ tests/` clean

## 5. Docs and issue hygiene

- [x] 5.1 `docs/architecture/composable-attach.md`: "Grain-union broadcasting" section (recursive node-grain rule) + roadmap update; verify page renders and nav intact
- [x] 5.2 User-facing docs (`docs/concepts/formulas.md` / `queries.md`) where mixed-grain semantics surface (incl. the composite-measure lift and the temporal-axis rule); verify by grep for stale "one grain per transform" wording
- [x] 5.3 Linear: DEV-1835 scope line-item (windowed union incl. D5 time bucket; delete guard + tests); DEV-1839 blocked-by note corrected; verify by reading back the issues — both already present in the issue descriptions (set at spec time)
