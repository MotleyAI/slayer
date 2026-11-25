## Context

See proposal.md — Why. The one canonical renderer (`_render` in `slayer/core/syntax.py`) spells each path segment with
`_identifier_text`, which quotes only names that are not Python identifiers and ignores `Ref.quoted` /
`DottedRef.quoted_parts`. `_render` already carries a `legacy` flag selecting the pre-2.0 aggregation, operator and
direction spellings used only for the result-name seed (`legacy_result_name_seed`).

Consumers that re-read canonical text (and so inherit the loss today): `extension_field_text` in `core/query.py` (the
issue's path), `storage/extension_fields.py` `_text` / `_expression` (query-time extension inlining),
`engine/model_scoped.py` `text`, `core/formula.py` `arithmetic`, `syntax.metric_text_at_path` (used by
`facade/catalog.py` and `facade/translator.py`). Consumers that use the text as identity or as a name: `core/query.py`
`_parsed_identity` (time-dimension dedupe, refinement merge identity and conflict check) and `_canonical_expression_text`
(time-dimension name identity), `sql/naming.py` `expression_text_alias`. All other uses are error messages, which
benefit from the faithful text.

Probes (on this branch, pre-change): `lower("REGIONS".lbl)` in a stored extension column is inlined; with a
monkeypatched keep-quotes renderer it is refused, `"Regions".lbl` still inlines, and the full unit suite passes
(59193 passed), so no existing test pins the dropped quotes. Bare `null`, `true`, `false`, `not` fail or read as
literals/operators as a standalone reference or a path head (`null.x` fails), but parse as plain segments after a dot
(`x.null`, `x.true`, `x.not`, `x.null.y`).

Applicable principles: `system.arc42.md` §3 P8 (one reader of Mode B — rendering and the read-back check stay in
`core.syntax`, nothing else re-tokenises); `python.core.arc42.md` P1 (a key answers one question — identity and
spelling stay separate questions). No arc42 or LikeC4 change; no new import edges.

## Goals / Non-Goals

**Goals:**
- One renderer whose canonical text always parses back to the same reading, segments and quoting included.
- Identity and naming behaviour byte-identical to today.

**Non-Goals:**
- Callee quoting (`"lower"(x)`): call nodes do not record it, and Ossie function names are case-insensitive.
- The respell in `engine/binding.py` `_frame_spelling` that clears tail quoting in an ambiguous-frame remedy: the remedy
  is issued only for references that resolved, where the unquoted same spelling resolves identically (exact spelling
  wins).
- Identifier matching itself (`queries/identifier-case`) is unchanged.

## Decisions

1. **Fix in the one renderer, not per call site.** Every re-reading consumer goes through `_render`, so the whole bug
   class closes in one place. (default — structural fix)
2. **Quote built segments whose bare spelling would not read back.** Options: include / leave out. Ossie's
   `core-spec/expression_language.md` (upstream main 7b8ed65) requires identifiers to "follow ANSI SQL naming", under
   which a reserved word is an identifier only when quoted; quoting a built name is safe because built names are
   declared spellings. (default — decided by the Ossie spec)
3. **The read-back check is derived from the grammar, position-aware.** A head segment (a `Ref`, or a `DottedRef`'s
   first part) is checked by parsing its bare spelling alone; a later segment through a harmless prefix (`x.<name>`);
   either must yield that unquoted name. Cached per (name, position). No hand-kept keyword list, so it cannot drift from
   the grammar; `*` segments and `AGG(...)` wrapping are rendered as today. Over-quoting a later segment would be
   harmless but is not the minimal canonical form. (codex)
4. **Pre-binding identity stays quote-blind.** Options: A — quote-only variants distinct (identity follows the faithful
   text); B — one expression (quote-blind identity); C — equal iff Ossie-normalized forms equal. B keeps DEV-2215's
   deliberate quote-blind dimension dedupe (`test_a_refinement_dedupes_a_quoted_and_a_plain_reference`) and node
   equality (`quoted` is "not part of identity"), and changes nothing outside the issue; none of A–C matches Ossie's
   normalized table while SLayer's resolver does not normalize that way, and Ossie leaves quoted-vs-unquoted comparison
   "DB specific". (user)
5. **Rendering takes a quoting policy independent of `legacy`.** Three forms: faithful canonical text (canonical
   spelling, quotes kept — `canonical_metric_text`); quote-blind identity (canonical spelling, today's quoting —
   byte-identical to today's `canonical_metric_text`, used by `_parsed_identity` and `_canonical_expression_text`);
   legacy naming seed (unchanged). The identity form cannot reuse the legacy seed, whose aggregation and operator
   spellings differ. (codex)
6. **Result names unchanged.** `legacy_result_name_seed` keeps today's quoting; `expression_text_alias` takes its
   `<source>_<agg>` base from the quote-blind form, so `count("customers".id)` keeps today's alias. (default — no
   regression)
7. **Spec placement.** An ADDED requirement in `queries/expression-grammar` beside "Canonical expression text keeps its
   grouping", not a rename of it; the stored-extension scenario sits there too. (default)
8. **Rejected (codex): "quote-blind identity is written back as the refined metric".** `_union`
   (`core/query.py` ~2151) uses `canon(...)` only in its conflict comparison and appends the original entries;
   `_drop_restatements` never rewrites `metric`. Nothing is written back.

## Risks / Trade-offs

- [A consumer uses canonical text as identity or a name and now sees quotes] → the Context audit lists every caller;
  the identity and naming sites switch to the quote-blind form, and regression tests pin today's identity and names for
  quote-only variants.
- [The read-back probe costs a parse per rendered segment] → cache by (name, position); bare identifiers that are not
  keywords dominate and hit the cache.
- [A quoted duplicate that would fail to bind can still be merged away by the quote-blind dedupe] → accepted under
  decision 4; it needs the same reference written twice.
