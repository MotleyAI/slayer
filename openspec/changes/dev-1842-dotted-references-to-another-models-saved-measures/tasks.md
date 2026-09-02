# Tasks — DEV-1842 dotted saved-measure references

TDD ordering: sections 1–2 land the full failing test surface (Step 4 of the
/spec flow); sections 3–7 implement until green; section 8 closes docs.

## 1. Tests first — new behavior (failing for the right reason)

- [ ] 1.1 Dual-engine equality suite (SQLite + DuckDB, `_dev1836_fixtures` pattern): dotted `customers.aov` vs hand-expanded prefixed formula — identical SQL bytes and executed values for simple, composite, partitioned (`partition_by=` target-local and `[]`), transform (`cumsum`), transform-wrapping-dotted, mixed local+dotted arithmetic, computed-dimension source; verify all fail today with UnknownReferenceError
- [ ] 1.2 Re-anchoring completeness tests: nested-join source (`regions.pop:sum` in a customers measure), self-qualified refs (no double prefix), `Column.filter` owner-local (`gold_spend`) and join-crossing (`vip_spend`) — equality vs hand-expanded; verify failing
- [ ] 1.3 Recursion tests: nested saved measure on target (`aov_big = "aov * 2"`), host measure containing dotted ref, depth-limit error, cross-model cycle error naming the (model, measure) chain; verify failing
- [ ] 1.4 Broadcast/strict inheritance tests: dotted form emits identical warnings/metadata to hand-expanded; `strict=true` errors identically; verify failing
- [ ] 1.5 Eligibility & error-contract tests: `customers.aov:sum`, plain dimension, unselected filter, ORDER BY formula, `partition_by=customers.aov`, StageSchema scope, raw-row targeted error, neither-column-nor-measure (both namespaces + suggestions); alias-precedence regressions (alias vs column collision, bare and dotted); selected `customers.aov` addressable in filters and ORDER BY; verify failing where behavior changes
- [ ] 1.6 Round-trip rejection test: `customers.order_total = "orders.amount:sum"` referenced from orders errors naming measure + revisited model; verify failing
- [ ] 1.7 Naming/metadata tests: implicit key `orders.customers.aov`, explicit `name` override, saved-type inheritance; verify failing
- [ ] 1.8 Query-backed fail-closed tests: direct `measures` + `source_queries` rejected at validation; `ModelExtension` measures over query-backed base work bare + dotted; a same-name `ModelExtension` measure is rejected by the DEV-1443 duplicate-name guard (no silent shadow — user decision, corrects D1); verify failing
- [ ] 1.9 Keys structural suite: generic visitor strip + prepend over every `ValueKey` variant — structural + hash equality, duplicate `SqlExprKey.referenced_join_paths`, exact-prefix strip, `TimeTruncKey`, `None`-vs-empty partition sets, `column_filter_key` invariance, fail-closed `TypeError`; strip cases pass today (pin), prepend cases fail (no `prepend_value_key` yet)

## 2. Tests first — migration of the pass's unit suites (user-approved)

- [ ] 2.1 Rewrite `tests/test_measure_expansion.py` + `tests/test_model_measure_expansion.py` scenarios (eligibility matrix per recursive edge, cycles, depth, shadowing, parse caching where observable) against binder entry points; verify the rewritten suites fail only where they exercise the not-yet-built binder path and the deleted-pass imports are gone

## 3. Re-anchoring authority (behavior-neutral)

- [ ] 3.1 Refactor `reroot_value_key` into the generic path-map visitor + strip wrapper; verify 1.9 strip cases and the full existing unit suite pass (goldens byte-identical)
- [ ] 3.2 Add `prepend_value_key` wrapper; verify 1.9 prepend cases pass

## 4. Binder resolution authority

- [ ] 4.1 Introduce the bind context (eligibility + (model, measure) chain), threaded from the two eligible call sites; consumed at `_bind_agg` source/args/kwargs, `partition_by`, transform scalar args; verify per-edge tests from 1.5/2.1
- [ ] 4.2 Bare-name fall-through in `_resolve_ref` (alias → column → measure, last-wins lookup); delete `slayer/engine/measure_expansion.py` and its call sites; verify migrated suites (2.1), `tests/test_named_measures.py`, and the full non-integration suite pass with goldens unchanged
- [ ] 4.3 Dotted fall-through in `_resolve_dotted` (dotted alias lookup → join walk → column → measure: target-anchored bind + prepend); verify 1.1–1.4 pass
- [ ] 4.4 Round-trip post-prepend validation; verify 1.6 passes
- [ ] 4.5 Error contract (D5 messages incl. both-namespace suggestions); verify 1.5 passes

## 5. Naming, metadata, guards

- [ ] 5.1 Dotted-aware `_bare_saved_measure_name` / `_saved_model_measure_type` via one shared resolver; verify 1.7 passes
- [ ] 5.2 Extend the raw-row measure detector's `DottedRef` case through the join graph; verify the raw-row test in 1.5 passes
- [ ] 5.3 Query-backed + `measures` validation in `slayer/core/models.py`; verify 1.8 passes

## 6. Full-suite gate

- [ ] 6.1 Run the full non-integration suite (`poetry run pytest -m "not integration"`) and fix every failure; verify green
- [ ] 6.2 Run `poetry run ruff check slayer/ tests/`; verify clean

## 7. OpenSpec hygiene

- [ ] 7.1 Re-run `openspec validate dev-1842-dotted-references-to-another-models-saved-measures --strict`; verify green

## 8. Docs

- [ ] 8.1 Update `docs/concepts/formulas.md`, `docs/concepts/models.md`, `docs/concepts/references.md` (dotted saved-measure reuse, resolution order, eligibility, query-backed constraint); verify each page renders and is reachable from `zensical.toml` nav
- [ ] 8.2 Update `.claude/skills/slayer-query.md` + `.claude/skills/slayer-models.md`; verify examples match implemented behavior
