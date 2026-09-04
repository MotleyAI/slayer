# Design

## Context

See proposal.md — Why. The rewriter layer at stake runs on the raw Mode-B string before
`ast.parse`: `_rewrite_case_when` (all expressions, `slayer/engine/syntax.py:420`) and
`_rewrite_sql_like` + `_normalize_sql_filter_operators` (filters only). Verified current
failures: bare `case` / `customers.case` / `écase` raise "Malformed CASE"; `customers.end`
in a THEN value yields `iif(a, customers., None) ELSE 0 END` (silent corruption);
`_rewrite_sql_like` rewrites inside string literals and truncates escaped-quote patterns.
The legacy `formula.parse_filter` has one production caller
(`schema_drift._filter_refs_dsl`, already `try/except → []`); its `.sql` output is
production-dead since DEV-1450. DEV-1826 makes both aggregation spellings parse natively
in the typed parser (verified: `sum(revenue) > 100`, `revenue:countd > 5`, `*:count > 3`
all yield `AggCall`), so the migration needs no funcstyle pre-rewrite.

## Goals / Non-Goals

Goals: keyword-safe lexing per the spec; exactly one Mode-B filter parser and one LIKE
rewriter in the codebase. Non-goals: Unicode support in `_COLON_AGG_RE` /
`_SCAN_TOKEN_RE` (ASCII-start colon-agg identifiers — separate issue if wanted);
double-quoted LIKE patterns; migrating converters off `parse_formula`.

## Decisions

1. **Tokenizer** (`_CASE_TOKEN_RE` id alternative):
   `[^\W\d]\w*(?:\s*\.\s*[^\W\d]\w*)*` — Unicode-aware start; dotted paths (whitespace
   tolerated around dots, matching Python attribute syntax) lex as ONE token, so a
   qualified keyword can never equal a keyword. Alternative rejected: `tokenize` module
   (the text is not valid Python at this stage — SQL keywords, colon aggs).
2. **Keyword guard** (helper applied at every comparison site): a token is a keyword iff
   it `isascii()` and uppercases to the keyword AND neither adjacent raw-text character
   is identifier material beyond `\w` — i.e. not `isalnum()`/`_`/`isidentifier()` and not
   Unicode category `M*` (combining marks). Covers `caſe`, decomposed `écase`, `℘case`,
   trailing-mark spoofs without a full Unicode-identifier lexer (Codex finding, folded).
3. **WHEN-lookahead gate** (`_case_has_when`): scan after CASE at relative depth 0
   (parens + brackets); FIRST structural keyword decides — `WHEN` → conditional, any of
   `THEN`/`ELSE`/`END`/`CASE` → identifier; unmatched `)` or end → identifier.
   Alternative rejected (interview Q1, upheld against a Codex objection): backtracking
   parse that would also accept a bare keyword-named simple-CASE operand
   (`CASE case WHEN 1 …`) — real grammar ambiguity, disproportionate complexity; the
   parenthesized operand `CASE (case) WHEN …` works under the gate and is documented.
4. **ASCII keyword classes instead of `re.IGNORECASE`** for `_SQL_LIKE_RE` (`like`,
   `not`), `_OVER_RE`, and the keyword rewrites in `_normalize_sql_filter_operators`:
   IGNORECASE folds `lıke` (dotless ı) and `liKe` (KELVIN) into `like` (verified).
   `[lL][iI][kK][eE]`-style classes keep every ASCII casing and nothing else.
5. **LIKE literal-span guard**: precompute string-literal spans with
   `_PY_STRING_LITERAL_RE`; a match starting inside a span is returned unchanged. RHS
   group becomes the escape-aware `'(?:\\.|[^'\\])*'`. Single-quote-only by decision
   (interview Q3): pg-facade SQL double quotes denote identifiers.
6. **`parse_filter` retirement**: `_filter_refs_dsl` reimplemented as
   `parse_filter_expr` + the node-walk already used by `_measure_formula_refs`
   (shared helper extracted; DEV-1826 expression-source descent included). Reference
   order changes from measures-first to expression order — authorized: the sole caller
   (`_filter_refs_on_base`) aggregates into a set. Then delete `parse_filter` and every
   private helper reaching zero references, iterating to fixpoint:
   `_preprocess_like`, `_LIKE_RE`, `_preprocess_sql_operators`, `_preprocess_concat`,
   `_filter_node_to_sql`, `_call_to_sql`, `_compare_to_sql`, `_binop_to_sql`,
   `_flatten_lshift_chain`, `_LIKE_INTERNAL_NAMES` + the `__like__` branch of
   `_classify_call_name`; audit `AggRef` / `_SUBQUERY_IN_FILTER_RE` for other users
   before deleting. Retained: `parse_formula` (converters), `_preprocess_agg_refs`,
   `_rewrite_funcstyle_aggregations`, `has_window_function`, shared constants.
7. **`ParsedFilter` moves to `slayer/sql/sql_predicate.py`** trimmed to `sql` +
   `columns` — its only remaining producer is `parse_sql_predicate`; `agg_refs` /
   `synthesized_aliases` / `is_having` / `is_post_filter` have zero remaining readers.
8. **Stale-comment scrub**: syntax.py's "module DEV-1452 deletes" claim (DEV-1452
   completed and deliberately retained formula.py) and the `_preprocess_like` mirror
   claims; `parse_filter` mentions in docs/architecture.

## Risks / Trade-offs

- [Keyword-named bare simple-CASE operand and nested-CASE-as-operand mislex] → loud
  parse/plan error, never silent corruption; parenthesize escape hatch documented in
  code comment; spec scenario pins the behavior.
- [Legacy test semantics lost in migration] → DEV-1452 Stage C pattern: audit each of
  the ~52 call sites; delete only what typed-parser tests already cover; migrate the
  rest onto `parse_filter_expr`.
- [Drift-refs parity regression] → dedicated parity suite (colon/funcstyle/custom aggs,
  dotted paths, expression sources, `*` exclusion, failure → `[]`) written before the
  migration lands.

## Migration Plan

Single PR, sequenced commits: (1) hardening + new regression tests, (2)
`_filter_refs_dsl` migration + parity tests, (3) deletions + test migration + comment
scrub. Rollback = revert; no storage or API surface touched.
