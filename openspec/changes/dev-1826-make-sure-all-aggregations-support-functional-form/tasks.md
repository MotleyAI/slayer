## 1. Tests first (TDD — land failing for the right reason)

- [ ] 1.1 Parser equivalence suite (`tests/test_functional_aggregations.py`): functional vs colon `AggCall` identity parametrized dynamically over `BUILTIN_AGGREGATIONS` + aliases + case variants; star forms (`count(*)`, `count(customers.*)`); parametric/kwargs (`percentile`, `window=`, `partition_by=`, `last(balance, updated_at)`, `weighted_avg`, `corr`); `first`/`last` arbitration (incl. `last(revenue:sum)` and `last(sum(revenue))` staying transforms); unknown-name deferral; error cases (zero args, kwargs-only unknown call, `count(distinct x)` syntax error). Verify: suite fails only on missing feature, not setup.
- [ ] 1.2 Position-parity suite: SQL + result-key equivalence per position — query measures, WHERE/HAVING filters, order (placeholder round-trip + serialization), model measures saved AND hand-authored-YAML-loaded, `ModelExtension`, inline `source_model`, `source_queries` stages, transforms/arithmetic containing functional aggs, cross-spelling rename/filter-form matching, construction-time filters with custom functional aggs, raw-row rejection paths.
- [ ] 1.3 Binder validation suite: custom aggs functionally (binding-level fixtures incl. joined-model reachability); `bogus(*)` / `*:bogus` standard unknown-agg error (global name validation before gates); `avg(*)` parity error; scalar-hint in unknown-agg message; scalar-colliding custom agg name rejected at validation.
- [ ] 1.4 Expression-aggregation suite: SQL expectations for arithmetic/scalar-call/constant/derived-SQL-column operands; every position; percentile/custom aggs over expressions; naming (derived key, formatting-insensitivity, hash cap determinism, rename override); stage-scope naming; errors (cross-model ref, filtered-column operand, nested agg/transform); gates skipped (whitelist doesn't block) + confidently-non-numeric rejection.
- [ ] 1.5 Retirement suite: no `NormalizationWarning` for functional input on execute or save; save preserves author spelling; rework existing FUNC_STYLE_AGG slack tests; importer suites (cube/dbt/OSI) untouched and green.
- [ ] 1.6 Entity-ref suite: `sum(orders.revenue)` resolves like `orders.revenue:sum` in memories resolution and `recommend_root_model`; expression text rejected.
- [ ] 1.7 Add functional-form cases to Tier-1 integration fixtures (marked `integration`). Verify: collected but skipped without DBs.

## 2. Parser

- [ ] 2.1 Token-aware star pre-pass replacing first-arg `*` / `path.*` with collision-proof placeholders (extend `syntax.py:652` colon preprocessing); verify 1.1 star cases pass, multiplication untouched.
- [ ] 2.2 New `_convert_call` dispatch (design §Decisions 1–2): builtin-agg branch with name healing, first/last arbitration, unknown-name `AggCall` deferral, updated `UnknownFunctionError` text; verify 1.1 passes.
- [ ] 2.3 Accept aggregation-free scalar expressions as `AggCall` sources (parse side); verify expression parse cases in 1.4.

## 3. Binder / keys / naming

- [ ] 3.1 Split `_validate_agg_eligibility`: global name heal+validate always, per-column gates owner-conditional; scalar-allowlist hint in unknown-agg error; verify 1.3.
- [ ] 3.2 Reject scalar-colliding custom aggregation names in `core/models.py` (mirror transform rejection); verify 1.3.
- [ ] 3.3 Expression-source variant on `AggregateKey` (`keys.py`): bound row-phase tree, canonical serialization, hash/equality; additive to existing serialization; verify 1.4 key-identity cases + full suite serialization round-trips.
- [ ] 3.4 Binder desugar for expressions: in-scope ref resolution (model scope + `StageSchema`), boundaries (cross-model, filtered operands, nesting), gates-skip + type inference; verify 1.4 binding cases.
- [ ] 3.5 Naming: sanitizer (operator words, punctuation, digit-lead, 40/32+BLAKE2-8 cap) wired into `canonical_aggregate_alias` / stage `flat_name`; verify 1.4 naming cases.

## 4. SQL generation

- [ ] 4.1 Render `AGG(<row-level expr>)` for expression sources across dispatch kinds (simple/distinct/percentile/dialect-hook/formula `{value}`); verify 1.4 SQL expectations on SQLite + generator unit dialects.

## 5. Retirement and rerouting

- [ ] 5.1 Delete `FUNC_STYLE_AGG` rule, helpers, `func_style_agg_to_colon`, and warning emission from `normalization.py`; drop the `stage_planner.py:667` call; verify 1.5.
- [ ] 5.2 Switch `schema_drift.py:602` to `core.formula._rewrite_funcstyle_aggregations`; verify schema-drift tests green.
- [ ] 5.3 Order coercion: placeholder + original-text `raw_formula` for call-style entries, no legacy-rewriter import in `core/query.py`; audit `core/query.py:1079` construction filter check and `stage_planner.py:2511` alias fallback for functional text; verify 1.2 order/filter cases.
- [ ] 5.4 Entity-ref helper (parse-based, `AggCall`-over-column only) used by `memories/resolver.py` (incl. `:599` site) and `query_engine.py:2100`; verify 1.6.

## 6. Full-suite gate

- [ ] 6.1 `poetry run pytest -m "not integration"` fully green; fix all failures.
- [ ] 6.2 `poetry run ruff check slayer/ tests/` clean.

## 7. Docs

- [ ] 7.1 Equivalence section (mapping table, naming identity) in `docs/concepts/references.md` + `formulas.md`, pointer in `queries.md`; expression-aggregation docs (capability, boundaries, naming, advisory gates) in `formulas.md` + `docs/examples/07_aggregations/`; verify examples parse via doc-test conventions where present.
- [ ] 7.2 Rewrite `docs/architecture/slack-normalization.md` (drop FUNC_STYLE_AGG + rationale) and `parsing.md` (new dispatch order); grep repo docs for `FUNC_STYLE_AGG` leftovers.
- [ ] 7.3 Update `.claude/skills/slayer-query.md` and `slayer-models.md` (replace dead `(amount - cost):sum` with `sum(amount - cost)`); update `slayer/memories/help_content/03_aggregations.md`.
- [ ] 7.4 Append `DECISIONS.md` entry (functional first-class, parity contract, expression aggs same-model, advisory gates, scalar-name rejection, colon retirement planned separately); verify nav (`zensical.toml`) needs no change.

## 8. Follow-up issues (Linear)

- [x] 8.1 Create: docs flip to functional-primary (DEV-1830); legacy `core/formula.py` consolidation (DEV-1831); cross-model expression aggregation (DEV-1832). Verified: all three linked from DEV-1826.
