## 1. Tracker records (pr-plan)

- [x] 1.1 DEV-2101 body: point to this change and list the scope added in planning (casts with `TRY_CAST`, `FILTER` row semantics, placeholder binding, DEV-1831 absorption, split into DEV-2119 / DEV-2120); verify by reading the issue
- [x] 1.2 Comments (not body edits): DEV-2021 (re-scope: remove query-time Mode A for every caller after DEV-2102; `ModelExtension.columns` deleted outright, replaced by Mode-B expressions via DEV-2119 / DEV-2120; inline-model removal waits on DEV-1768 stages), DEV-1768 (EXTEND column items die with DEV-2021; `MODEL … SOURCE (raw SQL)` waits on it; EXTEND / MODEL items stay Mode A until then), DEV-2044 (Mode-B half absorbed here, Mode-A half stays), DEV-1831 (absorbed here), DEV-1320 (absorbed by DEV-2120), DEV-2034 (Storyline must port `storyline/api/adapters/models.py` from `parse_formula` / `FieldSpec` to `parse_expr` + `walk_parsed_refs`), DEV-2043 (eventual replacement for Storyline-side formula validation), DEV-1370 (superseded: one parser, no `FieldSpec`); verify each comment exists

## 2. Failing tests (pr-tests)

- [x] 2.1 Grammar tests for every `queries/expression-grammar` scenario, plus the ambiguity table: `f(x=1)` kwarg vs `=` in scalar calls, `x::t` vs `x:sum`, `a::int::text`, predicates inside transform arguments, `"x"` identifier, `CASE` only before `WHEN`, `date '…'` vs a column named `date`, `**` cases `2**3**2`, `-2**2`, `(-2)**2`, `2**-2`; verify they fail on the current parser
- [x] 2.2 SQL-form-in-every-position matrix: each form of the proposal as measure, dimension, filter and order target, executed on SQLite and DuckDB, emission-checked on every Tier-1 dialect; verify failing
- [x] 2.3 `queries/casts` scenarios, including the Postgres 15 / 16 / unknown-version emissions (fake server profile), SQLite `'12abc'`, MySQL emission, and a concrete type a dialect cannot express; verify failing
- [x] 2.4 `aggregations/functional-form` FILTER scenarios (count, sum ≡ masked sum, weighted_avg numerator and denominator, `last` newest passing row, `corr` / `covar_*` pairs, custom aggregation, rejected aggregate inside `c`, with `partition_by` and `window`); verify failing
- [x] 2.5 `queries/variables` scenarios incl. injection rejections, numeric-string binding, hostile string, template-derived names for measures and computed dimensions with different runtime values, `date_range` bounds (`{start}`, `{end}`, one-sided, missing, non-string); verify failing
- [x] 2.6 `queries/date-functions` scenarios (interval literal forms, multi-unit rejection, typed literals, clock keywords and cache bypass); verify failing
- [x] 2.7 `queries/positions` NULL-placement scenarios on every Tier-1 dialect (SQL Server and Postgres in their integration suites), incl. hidden targets, bracketed aliases, refinement and stage rewrites; verify failing
- [x] 2.8 `models/stored-upgrade` scenarios: tokenizer migration cases (embedded `"` and `'`, backslashes, control escapes, Unicode, adjacent literals, malformed text, `|`), Mode-A untouched, unversioned client query untouched, 0.10.2 corpus still green; verify failing
- [x] 2.9 Importer dependency check over `ParsedExpr`: transitive dangling reference and cyclic saved measures for dbt and Cube; for OSI, the emitted formula is validated by the new grammar; verify failing where behaviour is new, green where it is preserved
- [x] 2.10 Regression: `sum(a) ** 2` executes (today `NotImplementedError`); `%` sign per dialect; verify failing
- [x] 2.11 Parser benchmark over the longest formulas in the test suite and docs vs the old parser (recorded, not gating); verify it runs
- [x] 2.12 Retarget, with the user's approval already given, the tests asserting on deleted helpers to equivalent `parse_expr` / `ParsedExpr` assertions: `tests/test_dev1833_keyword_lexing.py`, `tests/test_dev1450fix_group2_correctness.py`, `tests/test_dev1740_case_rewrite.py`, `tests/test_dev1743_validators.py` (pre-pass helpers `_rewrite_case_when`, `_rewrite_sql_like`, `_preprocess_colons`, `_normalize_sql_filter_operators`), and `tests/test_formula.py`, `tests/test_functional_remedies.py`, `tests/test_dev1576_heals.py`, `tests/test_dev1737_scalar_specs.py`, `tests/test_rank_direction.py` (`parse_formula` / `FieldSpec`); flip tests asserting `"abc"` is a string to the identifier reading; ruling 2026-10-21: `apply_variables_to_query` keeps returning substituted text, rendered from the bound tree with SQL quoting (`'O''Brien'`, `('a', 'b')`) — `tests/test_variables_planner.py` expectations switch to SQL quoting and the `escape="python"` tests in `tests/test_mode_a_variable_substitution.py` are deleted; list each changed test in the commit message
- [x] 2.13 Codex review of the tests against this change (when `reviewers.codex` is on)

## 3. Architecture move

- [ ] 3.1 Move `slayer/engine/syntax.py` → `slayer/core/syntax.py` with `la:deterministic-refactor`; move `ALL_TRANSFORMS` / `TIME_TRANSFORMS` / `TIMELESS_TRANSFORMS` to `slayer/core/enums.py`; verify `la-typecheck`, `dr-mock-lint` and the full unit suite
- [ ] 3.2 Apply the approved harness edits H1–H6 exactly as in the "Approved harness edits" section below, then `la-arch-diagrams` (H7); verify `la-arch-check` and `npx -y likec4@1.47.0 validate architecture`

## 4. Grammar

- [ ] 4.1 Add `pyparsing` to `pyproject.toml` / `poetry.lock`; verify `poetry install -E all`
- [ ] 4.2 Implement the grammar (design D2) producing the existing `ParsedExpr` nodes plus `Cast` and the `FILTER` predicate; delete `ast.parse`, every pre-pass and `parse_filter_expr`; repoint every caller (`core/query.py`, `engine/bind_inputs.py`, `binding.py`, `population.py`, `model_reads.py`, `schema_drift.py`, `unrelated.py`, `query_engine.py`, `memories/resolver.py`); verify 2.1 / 2.2 parse-level tests and the SQL golden baselines byte-identical
- [ ] 4.3 Quoted identifiers end to end and the single-quote hint in the unresolved-reference error; verify the expression-grammar identifier scenarios
- [ ] 4.4 Rejections with hints (`|`, `//`, `TRIM(BOTH…)`, two-argument scalar `min` / `max`, `TIME '…'`, `SIMILAR TO`, `REGEXP` / `RLIKE`, raw `OVER`) and syntax-error positions; verify their scenarios

## 5. Keys, binding, rendering

- [ ] 5.1 `CastKey` (design D3): traversal protocol, binding, checker typing, render via `exp.Cast` with the author's type and the dialect-expressibility check; verify the CAST scenarios
- [ ] 5.2 `TRY_CAST` lowering per dialect, SQLite `slayer_try_cast` registration, MySQL guard, Postgres server profile (`server_profile_sql`, `parse_server_profile`) and render-time profile plumbing; verify 2.3
- [ ] 5.3 `FILTER (WHERE c)` binding (row-level check) and lowering by masking every row-level input incl. the `first` / `last` ranking key; verify 2.4
- [ ] 5.4 `BETWEEN`, `IS [NOT] DISTINCT FROM`, `IS [NOT] TRUE|FALSE`, `ILIKE`, `||`, `POSITION`, `SUBSTRING FROM FOR`, interval literals, typed literals, clock keywords; `**` → `pow`; verify 2.2, 2.6, 2.10
- [ ] 5.5 `OrderItem.nulls` plumbing (design D7) and the multi-term `build_ordered` with the T-SQL emulation over the resolved expression; verify 2.7
- [ ] 5.6 The binder rejects a `round` `ndigits` that is not an integer literal (`round(x, 2.5)`, `round(x, col)`) with an error naming `round` (ruling 2026-10-21: the check moves from the deleted importer parser); verify the `tests/test_dev1576_heals.py` arity cases

## 6. Variables

- [ ] 6.1 Offset-preserving placeholder masking, the three grammar slots, binding into the tree at the existing substitution points, names from the written text, `date_range` literal-only substitution; delete the Mode-B `escape="python"` path and `ComputedDimension.substituted`'s text splice; verify 2.5

## 7. Migration

- [ ] 7.1 Stored-only steps SlayerModel 15 → 16, SlayerQuery 6 → 7, Memory 4 → 5 (design D6), ordered before the legacy load-path repairs; verify 2.8 and the 0.10.2 corpus

## 8. One parser (DEV-1831)

- [ ] 8.1 Port the dbt, Cube and OSI importers to `parse_expr` with the shared namespace-aware dependency walk; delete `parse_formula`, `FieldSpec`, `_rewrite_funcstyle_aggregations`, `_preprocess_agg_refs`, `_expand_named_measures`; verify 2.9 and the importer fixture suites
- [ ] 8.2 Check the Mode-B text the facade translator and the importers emit against the new grammar (single quotes, no backslash escapes, no `|`); fix emitters; verify the facade and importer suites

## 9. Docs and agent surfaces

- [ ] 9.1 `docs/concepts/formulas.md` (Parsing Internals, operators incl. `**` / `%`, FILTER, the CASE section), `references.md` (two-mode table, string and identifier rules, casts and `TRY_CAST`, interval and typed literals), `queries.md` (filters operator table, Variables / "Trusted input", OrderItem `nulls`), `time.md` if touched; `zensical.toml` only if a page is added; verify `grep` finds no "Python AST" / "Python-AST" claims and no double-quoted string examples in Mode-B docs
- [ ] 9.2 MCP `Field` descriptions in `slayer/core/query.py` / `slayer/mcp/server.py` and `slayer/memories/help_content/*` (strings single-quoted, SQL forms, `nulls`); verify `tests/test_mcp_surface_budget.py` and the documented-idioms tests
- [ ] 9.3 Release-notes draft (untracked, never staged): double-quoted strings, backslash escapes, `|` / `//`, `{var}` literal-only, `parse_formula` removal, `CAST` / `TRY_CAST`, Postgres < 16 `TRY_CAST`

## 10. Gates

- [ ] 10.1 `poetry run pytest -m "not integration"`, the CI integration invocation, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline), `la-arch-check`; verify all green

## Approved harness edits (exact text, approved 2026-10-20)

H1 `architecture/model.c4`: add `syntax = node 'Syntax'` under `core` after `models = node 'Models'`; remove `syntax = node 'Syntax'` from `engine`; remove `core.query -> engine.syntax #legacy`; add `core.query -> core.syntax` after `core.query -> core.models`; `engine.binding -> engine.syntax` → `engine.binding -> core.syntax`; `engine.bind_inputs -> engine.syntax` → `engine.bind_inputs -> core.syntax`.

H2 `architecture/index.yaml`: `legacy_arrows: {baseline: 8}` → `legacy_arrows: {baseline: 7}`.

H3 `architecture/system.arc42.md` principle 2: the door list `(`core.query`/`core.models` → `engine.syntax`, `sql.*`, `storage.migrations`)` → `(`core.query`/`core.models` → `sql.*`, `storage.migrations`)`.

H4 `architecture/system.arc42.md` principle 10 becomes:
"10. **Two expression layers**: Mode A free SQL (`Column.sql`, model `filters`) vs Mode B, a SQL-compatible grammar (formulas, query fields) — one canonical function library, extended never forked. [review] Every Mode B construct means the same on every dialect. [target: DEV-2120]"

H5 `architecture/core.arc42.md`: §1 door list as in H3; §2 "Pydantic domain models alongside." → "Pydantic domain models alongside; the Mode B grammar: `syntax.py`."

H6 `architecture/engine.arc42.md`: §2 "`normalization.py` → `syntax.py` (parse)" → "`normalization.py` → `core/syntax.py` (parse)"; principle 9 becomes "**The term-interface boundary**: `binding`, `bind_inputs`, `elaborate`, `elaborate_env` (THE checker), `compile` and `plan` are declared children with only the modeled arrows among them — the compiler consults the checker, never `core.syntax` or binding; every user-facing algebra type error raises in the checker, as a `QueryTypeError`." (status tags unchanged).

H7: regenerate the embedded mermaid with `la-arch-diagrams`.
