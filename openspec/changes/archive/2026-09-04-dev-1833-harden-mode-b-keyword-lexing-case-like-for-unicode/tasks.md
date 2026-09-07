# Tasks

## 1. Regression tests first (spec-tests stage)

- [x] 1.1 Create `tests/test_dev1833_keyword_lexing.py` with the CASE identifier-safety
  matrix from the spec scenarios: bare `case` (also `case + 1`, `iif(case, 1, 2)`),
  `customers.case`, `customers . case`, `é_case`, `écase`, `变量`, decomposed `écase`,
  `℘case`, `caſe`, `case + CASE WHEN x THEN 1 END`,
  `CASE WHEN case THEN 1 WHEN other THEN 2 END`, `customers.end` in a THEN value,
  `CASE (case) WHEN 1 THEN 2 END`, and the `CASE case WHEN 1 THEN 2 END` loud-error
  pin. Verify: every regression test FAILS on the pre-fix code.
- [x] 1.2 Add CASE still-works coverage: searched/simple/nested lowering, SQL operator
  normalization in WHEN conditions, malformed-with-WHEN errors (missing THEN / END)
  preserved. Verify: these pass before AND after the fix.
- [x] 1.3 Add the LIKE matrix: escaped-quote pattern, ` like ` inside a string literal
  untouched, `x lıke 'p%'` / `x liKe 'p%'` not rewritten, `x LiKe 'p%'` rewritten,
  double-quoted RHS still errors, LIKE/NOT LIKE/dotted/scalar-call/Unicode LHS still
  rewrite. Verify: regression cases fail pre-fix; still-works cases pass pre-fix.
- [x] 1.4 Add the `_filter_refs_dsl` parity suite (in the schema-drift test module):
  colon aggs, funcstyle aggs, custom agg names, dotted paths, expression agg sources,
  `*:count` exclusion of `*`, LIKE-containing filter, unparseable → `[]`; assert set
  equality (order authorized to change). Verify: suite passes against the LEGACY
  implementation before migration (parity baseline).

## 2. CASE hardening (slayer/engine/syntax.py)

- [x] 2.1 Change `_CASE_TOKEN_RE` id alternative to
  `[^\W\d]\w*(?:\s*\.\s*[^\W\d]\w*)*`. Verify: task 1.1 Unicode + dotted cases pass.
- [x] 2.2 Add the keyword-guard helper (ASCII-exact + raw-text adjacency check per
  design decision 2) and use it at every keyword comparison in `_rewrite_case_when` /
  `_rw_value` / `_rw_case`. Verify: `caſe`, decomposed, `℘case` cases pass.
- [x] 2.3 Add `_case_has_when` lookahead gate and require it before entering `_rw_case`
  from `_rw_value`; document the two accepted mislex edges + parenthesize escape hatch
  in a concise comment. Verify: bare-`case` family and coexistence cases pass; 1.2
  still green.

## 3. LIKE + operator-keyword hardening (slayer/engine/syntax.py)

- [x] 3.1 Rework `_SQL_LIKE_RE`: ASCII character-class keywords (drop IGNORECASE),
  escape-aware RHS group. Verify: 1.3 spoof + escaped-quote cases pass.
- [x] 3.2 Add literal-span skip to `_rewrite_sql_like`. Verify: inside-literal case passes.
- [x] 3.3 Apply ASCII keyword classes to `_OVER_RE` and the keyword rewrites in
  `_normalize_sql_filter_operators`. Verify: existing operator-normalization and OVER
  rejection tests stay green.

## 4. parse_filter retirement

- [x] 4.1 Extract the shared ref-walk helper from `_measure_formula_refs` and
  reimplement `_filter_refs_dsl` on `parse_filter_expr`. Verify: 1.4 parity suite green
  on the new implementation; schema-drift tests green.
- [x] 4.2 Migrate/delete the ~52 legacy `parse_filter` test call sites
  (43 `tests/test_formula.py`, 5 `tests/test_sql_generator.py`,
  3 `tests/test_dev1576_heals.py`, 1 `tests/facade/test_translator.py`) per the
  DEV-1452 Stage C pattern. Verify: no `parse_filter` reference remains under `tests/`;
  suite green.
- [x] 4.3 Delete `parse_filter` and iterate orphan deletion to fixpoint (candidate list
  in design decision 6; audit `AggRef` / `_SUBQUERY_IN_FILTER_RE` first). Verify:
  `grep -rn "parse_filter\b\|__like__\|__notlike__\|_preprocess_like" slayer/ tests/`
  returns nothing; suite green.
- [x] 4.4 Move trimmed `ParsedFilter` (`sql`, `columns`) to
  `slayer/sql/sql_predicate.py`; update imports. Verify: suite + ruff green.
- [x] 4.5 Stale-comment scrub per design decision 8 (syntax.py DEV-1452/mirror claims,
  formula.py docstrings, docs/architecture `parse_filter` mentions). Verify:
  `grep -rn "parse_filter\|_preprocess_like" slayer/ docs/` shows no stale claims.

## 5. Docs + gates

- [x] 5.1 Add one sentence to `docs/concepts/formulas.md` (CASE section): keyword-named
  identifiers are safe; CASE only lowers when WHEN follows. Verify: sentence present,
  page already in `zensical.toml` nav.
- [x] 5.2 Full gates: `poetry run pytest -m "not integration"` green,
  `poetry run ruff check slayer/ tests/` clean,
  `openspec validate dev-1833-harden-mode-b-keyword-lexing-case-like-for-unicode --strict` green.
