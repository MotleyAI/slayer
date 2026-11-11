## Context

See proposal.md for motivation. Current state on `main`:

- `core/syntax.py` accepts `x:agg(args)` through the `_COLON_AGG` production, building the same `AggCall` as the functional call. `canonical_measure_text` renders a ref/star-source `AggCall` in colon form (`_canonical_agg_text`).
- Result keys of single-aggregate roots come from the bound `AggregateKey` (no text). Every other root (formulas, `{var}` templates, FILTER, unnamed computed dimensions) gets its key from `formula_text_alias(canonical_measure_text(...))` → `AGG_REF_RE` / `auto_name_from_expression`, which sha256-suffixes text over 48 characters. Those keys therefore depend on the colon canonical text byte-for-byte, and persisted `ComputedDimension.name` auto-names are compared against `computed_dimension_name(expression)` to decide explicit vs auto.
- `core/query.py` normalises colon order keys to `<base>_<agg>` (`_colon_spelled_name`, stored as `{column: {name: amount_sum}, raw_formula: "amount:sum"}`), keeps unparseable colon order text for binding (`":" in v`), and reads `col:gran` through a fallback in `granularity_call_argument`.
- `split_entity_agg_ref` string-splits colon entity refs (`split_agg_suffix` / `AGG_REF_RE`); the memory resolver builds colon tokens from parsed functional text and feeds them back through it.
- Facade `catalog.py` / `translator.py` build colon `measure_formula` text (`*:count`, `col:agg`, `f"{prefix}.{formula}"`) and key `metrics_by_formula` / `metric_item_by_formula` on it. `live_schema/broken_dependents.py` probes with `*:count`.
- Stored-only migrations: `storage/migrations.py` registry (current SlayerModel 18, SlayerQuery 9, Memory 7); shared walkers in `storage/rank_direction_migration.py`; `granularity_rename` runs at the storage boundary before version steps.
- Principles: system §3.8 (one reader of Mode B, `tests/test_dev2172_one_mode_b_reader.py`), §3.11 (fail at save), core P5 (typed errors), storage P3 (versioned persistence). No arc42 or model edit is needed.

## Goals / Non-Goals

**Goals:**
- One colon recogniser, inside `core.syntax`, serving both the refusal and the stored-text rewrite, with one twin renderer.
- Result keys byte-identical, derived from the parsed node.
- Facade producers and consumers keyed on one canonical functional text, so they cannot diverge.

**Non-Goals:**
- Changing how stored provenance is detected (`stored_only` = the dict carries a `version`); this applies to every migration and is out of scope.
- DEV-2215's date-part flip and its `<unit>(x)` → `date_trunc` migration, including widening `date_trunc` to custom granularities.
- The other legacy spellings (`==`, `is None`, `[...]` after `IN`, bare `None`, method-call form); they stay under DEV-2215.

## Decisions

1. **Twin rule (user).** A colon span `<source>:<name>[(<args>)]` becomes `date_trunc('<name>', <source>)` when `<name>` is a built-in granularity (`GRANULARITY_NAMES`), else `<name>(<source>[, <args>])`, source and args as written. Exact on `main`: granularity names are reserved against custom aggregations, and `date_trunc(unit, x)` means `<unit>(x)` in every position. Alternative: uniform `name(x, args)` — rejected after confirming no shadowing is possible; it would widen the DEV-2215 ordering constraint to every bucket span and split the refusal hint from the migration output.
2. **One twin function for refusal and migration (default).** The same renderer produces the error hint (every position) and the migrated text, so they cannot disagree.
3. **Entity references refuse colon (user).** `split_entity_agg_ref` reads through the parser only; `split_agg_suffix`, `strip_agg_suffix` and `AGG_REF_RE` are deleted. Alternative: keep colon entity refs — rejected: a second colon reader outside `core.syntax`.
4. **Test disposition (user).** Colon≡functional parity halves deleted and replaced by one refusal suite; colon-vehicle tests switched to functional input with assertions kept; incidental colon input (~6,600 lines, ~55 f-string templates) rewritten mechanically by the new span rewriter; colon output pins updated; stored fixtures kept as migration tests. Every deletion is listed in tasks.md.
5. **Refusal in the parser (default).** The colon production no longer builds an `AggCall`; it raises the typed parse error carrying the parsed span (source text, name, args text). Every Mode-B surface inherits it, including model and saved-query saves (§3.11).
6. **Span rewrite is an accepting, location-capturing mode of the full grammar (codex).** Not a text scan of the colon production: the whole text is parsed with the colon production in a recording mode that captures each span's start/end; edits are applied innermost-first, so string literals, quoted identifiers, `::` casts and comments are never matched and every byte outside a span is kept. No new tokenizer (§3.8).
7. **Colon refusal survives the `_parsed_or_none` classifiers (codex).** Order-key and dimension classifiers that swallow parse errors re-raise the colon error, so it never degrades into a `ColumnRef` validation error.
8. **Storage step (default).** `storage/colon_spelling_migration.py`, `stored_only`, registered at SlayerModel 18 / SlayerQuery 9 / Memory 7 — one bump to 19 / 10 / 8 — reusing the shared walkers. Mode-A surfaces untouched.
9. **Order items re-coerced (default; codex).** The shared walker only rewrites `raw_formula`; this step has its own order rewrite that, when the text changes, rebuilds `column` with the fresh-input classification, so the item loads exactly as the functional key written fresh (`_funcstyle_pending` / `_expr_pending` + `raw_formula`).
10. **Keys from the parsed node via a legacy naming seed (default; codex).** `canonical_measure_text` renders functional text (identity, error text, refinement keys). A `legacy_result_name_seed(node)` — the same renderer with the legacy aggregation spelling — is the only input to text-path naming (`formula_text_alias`, `computed_dimension_name`). The seed is never parsed or shown. Golden key tests freeze it (quoted/dotted refs, args/kwargs, FILTER, casts, placeholders, nested aggregates, names either side of the 48-character hash boundary, computed and time dimensions). Alternative: a separate frozen renderer — rejected: two printers drifting apart, with the same freeze obtainable from golden tests.
11. **Computed-dimension auto-names keep their meaning (codex).** The explicit-vs-auto check compares a stored name against the seed, which is spelling-independent, so a migrated expression keeps its persisted auto-name as auto.
12. **Facade keyed on canonical functional text (default).** `_agg_formula` → `functional_agg_text`; row count `count(*)`; column × aggregation `agg(col)`; join-path prefixing through one parse-and-re-root helper in `core.syntax` (re-roots the `AggCall` source, renders canonical text — `count(customers.*)`, never `customers.count(*)`); `*:count` checks become a `StarSource` check on the parsed node.
13. **Memory resolver yields structured tokens (default).** `(source, agg)` values instead of colon text; nothing re-splits them.
14. **SQL-mode filters keep rejecting colon (default; codex).** A local regex in `sql/sql_predicate.py`, applied after the existing string-literal masking, requiring a complete aggregation suffix.
15. **Kept on purpose (default).** `sql_string_migration._COLON_REF` (old-grammar gate for documents before v15) and `granularity_rename`'s `x:g` handling (runs at the storage boundary before version steps, so a renamed custom granularity is then rewritten by this step).
16. **One docs sentence (default).** In `docs/concepts/formulas.md`: the colon spelling was removed in 2.0 and stored text is upgraded on load.
17. **Explicitly versioned client payloads are migrated (rejected codex finding).** Codex asked to stop inferring stored provenance from `version`. Rejected: that is the existing contract of every stored-only step, and specs only promise that a client payload *without* a version is not rewritten.

## Risks / Trade-offs

- [Ordering with DEV-2215] A custom-granularity span migrates to `<gran>(x)`, a spelling DEV-2215 removes. → This step MUST run before DEV-2215's `<unit>(x)` → `date_trunc` step; whichever slice merges second keeps that order, renumbering either registration (both unreleased). Recorded as a comment on DEV-2215.
- [Version collisions] DEV-2215, DEV-2102 and DEV-1607 also bump stored versions. → Whichever merges second renumbers.
- [Key drift] A future change to the canonical renderer could alter the seed and every hashed key. → Golden key tests fail on any seed change.
- [Mechanical rewrite] Respelling ~6,600 test lines could change a test's meaning. → The rewriter is the tested migration function; every respelled file must pass unchanged assertions.
- [Error-message change] The facade's "Aggregate … is not available" error now shows functional text. → The only client-visible facade change.

## Migration Plan

Load-time, versioned, stored-only; no manual step. Rollback: the previous release cannot read a document re-saved by this one if its text uses spellings it lacks — standard for a 2.0 bump.
