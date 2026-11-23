## Context

See proposal.md - Why. The gate `is_old_grammar` (`slayer/storage/sql_string_migration.py`) runs two regex approximations of 1.9.1's pre-passes (colon refs, `*` args) then `ast.parse`. 1.9.1's reader (`git show v1.9.1:slayer/engine/syntax.py`) ran, before `ast.parse`: `_rewrite_extract`, `_rewrite_count_distinct`, `_rewrite_case_when` (its WHEN conditions through `_normalize_sql_filter_operators`), `_preprocess_colons`, `_preprocess_star_args`; query filters (`parse_filter_expr`, called only for `SlayerQuery.filters`) first ran `_normalize_sql_filter_operators` (`_rewrite_sql_like`, `NULL`→`None`, keyword lowercasing, `<>`→`!=`, `||`→`|`, `_rewrite_comparison_equals`). Mode-B `{var}` placeholders were substituted with Python literals before parsing, and `{? ?}` blocks were refused on Mode-B surfaces (`v1.9.1:slayer/core/query.py`).

SlayerQuery v6, SlayerModel v15 and Memory v4 — the gate's source versions — are exactly 1.9.1's `CURRENT_VERSIONS`; earlier migrations bring older documents up to them first, so 1.9.1's grammar is the grammar every gated document claims.

Principles: `python.storage` §3 (versioned persistence; 3.1 a load never writes — unaffected). System §3.8 (one reader of Mode B): the oracle reads the superseded 1.x grammar, not Mode B — the same carve-out today's gate and the tokenize allowlist in `tests/test_dev2172_one_mode_b_reader.py` rely on; the new module imports no `tokenize`, so the allowlist is unchanged. No new import edge (storage → core exists); no harness edit. Spec `models` is cross-cutting and already touches `python.storage`.

## Goals / Non-Goals

**Goals:**
- One oracle for "1.9.1 accepted this text on this surface", replacing the hand approximation.

**Non-Goals:**
- Replaying 1.9.1's semantic `_convert` checks (unknown functions, chained comparisons, unsupported nodes, reserved names).
- Changing `sql_quote_strings` (probed: it already tokenizes `<>`, `IS NULL`, `LIKE`, `CASE`, `{var}`, `extract` correctly).
- Docs (`docs/concepts/datasets.md:635` already states the upgrade correctly).

## Decisions

1. **Oracle = frozen verbatim port of the 1.9.1 pre-parse chain** in a new `slayer/storage/legacy_grammar.py`, exposing `is_old_grammar(text, *, filter: bool)`: the 1.9.1 passes in 1.9.1's order, then `ast.parse(mode="eval")`; a pass's `ValueError` or a `SyntaxError` means not old grammar. The 1.9.1 `SCALAR_FUNCTIONS` / `ALL_TRANSFORMS` names the `=`-vs-kwarg scanner reads are frozen as literals (recorded from 1.9.1). Alternative — extending the hand approximation — is the drift that caused this bug. (default)
2. **Syntax-level acceptance**: text 1.9.1 refused only in `_convert` counts as held and is string-upgraded with no warning; it never returned rows, so no working query's meaning can change. Alternative B — also porting `_convert` with 1.9.1's function / aggregation / transform registries — buys only a warning on already-broken text at the cost of a second, much larger frozen parser. (user, Codex finding 1)
3. **The 1.9.1 oracle covers every version up to the gate's source version** (see Context). (default)
4. **Spaced colon refs are dropped**: the current `\s*:\s*` tolerance matched no release (checked every tag v0.9.0–v1.9.1). (default)
5. **Surface kind comes from the walker**: the text walker's callback carries whether the field is the query's `filter` key; `main_time_dimension`, order keys and expressions, dimensions, measures and time-dimension columns are expression surfaces. Parsing the field-path string was rejected as fragile. (default; codex)
6. **`{var}` probe** as 1.9.1's `_VAR_PATTERN` reads it: 1.9.1 rendered string values unquoted, so a valid `{name}` could be a number or a name (an `extract` part, a path segment, a colon-aggregation operand); the text is old grammar if it parses with every `{name}` as the literal `0` or with every `{name}` as a bare identifier. Operator-valued placeholders (`amount {op} 5`) are not covered. `{{` / `}}` become braces, any other `{…}` and any `{?` / `?}` delimiter mean not old grammar. (default; codex; CodeRabbit)
7. **Verification corpus with provenance**: expected verdicts recorded once from real `motley-slayer==1.9.1` (`parse_filter_expr` / `parse_expr`) in an isolated venv, committed with the 1.9.1 version and recording command; it includes semantic-rejection texts with both verdicts (1.9.1 refused, gate accepts). A live differential test in CI is impossible — 1.9.1 installs as the same `slayer` package. (default; codex)
8. **LIKE scenario uses a single-quoted pattern** plus a separate double-quoted literal; `LIKE "…"` is in the refused set, matching 1.9.1's `_SQL_LIKE_RE`. (codex)
9. **Load tests assert both stages**: the exact pre-qualification text from the migration, then loaded qualified text, rows and zero grammar warnings, on YAML and SQLite, for `"ok"` and `'ok'`. (codex)
10. **No docs change.** (default)

## Risks / Trade-offs

- [The frozen port diverges from 1.9.1 by a transcription slip] → the recorded 1.9.1 corpus covers every spelling and pass, including error paths.
- [Semantically invalid 1.x text loses its warning] → accepted (decision 2): it returns no rows either way.
- [~500 lines of frozen legacy code] → isolated in one module, no callers but the gate; removed with the stored-upgrade chain.
