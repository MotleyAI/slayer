# Proposal: Harden Mode-B keyword lexing (CASE / LIKE) for Unicode identifiers

## Why

The Mode-B pre-`ast.parse` textual preprocessors in `slayer/engine/syntax.py` key off SQL
keywords with ASCII-oriented regex patterns, so a legal identifier can be misread as a
keyword: a column named `case`, a qualified `customers.case`, or a Unicode-prefixed
`écase` all fail to parse today, and `customers.end` inside a THEN value silently
corrupts the rewritten expression. The sibling LIKE rewriter corrupts expressions whose
string literals contain ` like ` and patterns with escaped quotes. Same bug class as the
`__slayer_` boundary fix in DEV-1743 / PR #334.

Additionally (interview-approved scope extension): the legacy `formula.parse_filter` is a
second, near-dead Mode-B filter parser — its only production caller is
`schema_drift._filter_refs_dsl`, and its `__like__`/`__notlike__` machinery exists solely
to feed itself. Retiring it leaves exactly one Mode-B filter parser and one LIKE rewriter.

## What Changes

- `_CASE_TOKEN_RE` lexes complete identifiers: Unicode-aware start, dotted paths (with
  optional whitespace around dots) as single tokens.
- Keyword recognition requires ASCII tokens (blocks `'caſe'.upper() == 'CASE'` spoofs)
  and rejects tokens adjacent to identifier-material characters the regex `\w` class
  misses (combining marks, `Other_ID_Start` symbols like `℘`).
- `_rw_case` is entered only when a depth-0 `WHEN` follows the `CASE` token; otherwise
  `case` flows through as an ordinary identifier.
- `_SQL_LIKE_RE`: keyword matched as explicit ASCII character classes (drops
  `re.IGNORECASE`, which folds `lıke`/`liKe` into `like`); escape-aware pattern
  literal; matches starting inside string literals are skipped. Same ASCII-classes
  treatment for `_OVER_RE` and the keyword rewrites in `_normalize_sql_filter_operators`.
- **BREAKING (internal only)**: legacy `formula.parse_filter` and its private subtree
  (`_preprocess_like`, `_preprocess_sql_operators`, the `__like__`/`__notlike__` helpers,
  the Mode-B filter→SQL lowering functions) are deleted; `schema_drift._filter_refs_dsl`
  moves onto the typed `parse_filter_expr`; trimmed `ParsedFilter` (`sql` + `columns`)
  moves to `slayer/sql/sql_predicate.py`. No public API changes.
- Error-surface change: a bare `CASE`-named reference no longer raises "Malformed CASE";
  a `CASE` with no `WHEN` at all degrades to the generic invalid-expression error.

## Capabilities

### New Capabilities

- `queries/expression-keywords`: SQL keyword affordances inside Mode-B expressions —
  CASE WHEN lowering to `iif`, LIKE/NOT LIKE rewriting to the `like()` scalar — and the
  identifier-safety rules guaranteeing that identifiers named after, containing, or
  qualified by SQL keywords are never captured by that recognition.

### Modified Capabilities

(none — the `parse_filter` retirement preserves the drift-refs contract; no existing
spec's requirements change)

## Impact

- `slayer/engine/syntax.py` — tokenizer, CASE gate, LIKE/OVER/operator keyword regexes.
- `slayer/core/formula.py` — `parse_filter` subtree deleted (~several hundred LOC);
  `parse_formula`, `_preprocess_agg_refs`, `_rewrite_funcstyle_aggregations`, constants retained.
- `slayer/engine/schema_drift.py` — `_filter_refs_dsl` on the typed parser; reference
  order becomes expression order (verified: sole caller aggregates into a set).
- `slayer/sql/sql_predicate.py` — receives the trimmed `ParsedFilter`.
- Tests: new regression file; ~52 legacy `parse_filter` call sites across 4 files
  migrated or deleted (DEV-1452 Stage C pattern, user-consented).
- Docs: one sentence in `docs/concepts/formulas.md`.
