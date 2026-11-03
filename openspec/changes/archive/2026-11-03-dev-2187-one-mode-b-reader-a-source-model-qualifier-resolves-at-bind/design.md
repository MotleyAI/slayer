## Context

See proposal.md › Why. Facts the approach rests on (verified by probe and reading on 2026-11-02):

- Binding already drops a root qualifier for model roots — `binding._resolve_dotted` (:889),
  `_canonical_if_routed` (:345), `_resolve_dotted_star` (:1090), `bind_inputs._joined_column_type`
  (:838), `_resolve_saved_measure_ref` (:944), `column_dependency._hop_path` (:255),
  `model_reads` (:216), Mode-A `sql/column_expansion.py` (:253, :289) — each comparing `host.name`.
  A stage host's `name` is its minted identity and its `spelling` the user's name
  (`ir/source_bundle.model_from_stage_schema`); `unrelated.unrelated_targets` alone checks both.
  A `StageSchema` scope never drops a qualifier (`_stage_list_scope`), so `s1.tot * 2` fails today.
- Self-joins are rejected at model validation (`SlayerModel._reject_self_joins`), so the only hop that
  can share the root's name is an edge *named* like its own model.
- `bind_variables` substitutes `{name}` in code regions and string literals (skipping `{{`/`}}` and
  digit-only braces) and never in quoted identifiers, which the parser rejects; `parse_expr` parses
  placeholders natively.
- Measure result names come from the bound key (`sql/naming.py`), so they are already root-relative
  once binding resolves the qualifier; a dimension's public name is `bound.routed_dotted or full`
  (`bind_inputs.py:1101`), which falls back to the typed text when `_canonical_if_routed` returns `None`.
- The parser keeps no source offsets except on `Placeholder` (`syntax._placeholder_node`).
- Refinement merge (`core.query.refine_query`) runs on the stored query before any engine stage.

## Goals / Non-Goals

**Goals:**
- Every reading of Mode-B text outside `core.syntax` goes through a `core.syntax` reader.
- One root-qualifier rule, applied wherever a path head is compared with the root, keyed on the name
  the user writes.

**Non-Goals:**
- Root-relative auto-names for computed dimensions and expression time dimensions: they keep the
  written text (`orders.orders_amount_2`), as today.
- Mode A: `_VAR_PATTERN`, `substitute_variables`, `render_probe_text` on Mode-A surfaces, and
  `date_range` bound substitution (time points, not Mode B) are unchanged.
- `_WHOLE_CALL_RE` as the error-message fallback for text the parser rejected.

## Decisions

1. Root qualifier resolves at bind time; the text strip is deleted; the root wins; result keys are
   unchanged; no slack warning — engine P7 retires the slack rule by promotion. Rejected: keep the
   pre-pass region-aware with a warning (still a text rewrite of a previous stage's input; leaves a
   second text path for merge keys). (user, DEV-2185 Q2=A)
2. Fix the whole regex-over-Mode-B class and guard it with a behavioural law. Rejected: only the prefix
   strip (leaves known wrong-result instances and no guard). (user, DEV-2185 Q3=A)
3. A `{x}` inside a string literal is a variable by design (spec `queries/variables`); the brief's
   "`'{x}'` is not a variable demand" is corrected to: reported demand equals `bind_variables`'
   lookups. No wrong-result instance exists (regex and binding disagree only on `"{x}"`, which the
   parser refuses), so this part is a refactor guarded by a parity law. (user)
4. The remaining text slicers — functional time dimensions (`granularity_call_argument`,
   `_reject_call_placeholders`, the `_WHOLE_CALL_RE` legacy-spelling test), order coercion
   (`_order_formula_candidate`, `_coerce_order_column`), the misplaced-measure bare-token test
   (`normalization.py:98`) and `main_time_dimension` matching — read the parse tree, via call-argument
   source spans. Rejected: a separate issue with a `target:` tag on §3.18 (a principle left knowingly
   false). (user, Option A)
5. `architecture/system.arc42.md` §3.9 — insert after "(`customers.regions ≡ customers.hr` for an edge
   named `hr`);": "a leading **root qualifier** — the root's own name, read before any hop — drops out
   (`r.x ≡ x` from root `r`);" and add `[enforced: test:tests/test_dev2187_root_qualifier.py]`.
   (user, approved wording)
6. `architecture/semantics.arc42.md` Axiom 2.4 — "Only definition defaults cancel — a revisiting query
   path is refused (its root qualifier is no revisit, system §3.9), a revisiting `Column.sql`
   likewise." (user, approved wording)
7. `architecture/system.arc42.md` §3.18 — add `[enforced: test:tests/test_dev2187_one_reader_law.py]`.
   (user, approved wording)
8. The rule compares `host.spelling` (a `StageSchema`: `display_name`), never `host.name`, so every
   root kind — model, extension (its base), inline model, user stage, spliced query-backed model —
   takes the name its author writes. Minted identities carry the reserved `__slayer_` prefix no user
   can write. (default)
9. ColumnRef surfaces (dimensions, column time dimensions, order columns, `main_time_dimension`)
   resolve at bind time like text surfaces; no structural pre-rewrite survives. Alternative — keep the
   ColumnRef half of the strip at entry — leaves two mechanisms and a P7 slack rewrite. (default,
   follows from 1)
10. Construction-time time-dimension dedupe and refinement time-dimension merge compare
    `canonical_measure_text` of the root-relative parse (unparseable → raw text); spacing variants of
    one expression therefore collapse at construction too, where binding already deduplicated them.
    (user, the brief's parse-tree decision)
11. Computed-dimension and expression time-dimension auto-names keep the written qualifier, as today.
    (user, "result keys exactly as today")
12. Mode-A `column_expansion` uses the same root helper. (default, consistency)
13. Spine and splice detection read the same surfaces as today (spine: dimensions, time dimensions,
    filters; splice: measures, filters, dimensions). (default)
14. `_canonical_if_routed` returns the canonical path whenever the typed spelling differs from it, a
    dropped root qualifier included — otherwise `bound.routed_dotted or full` would key a qualified
    dimension under its typed text (`orders.orders.amount`). (codex)
15. Call-argument source text comes from located parse elements recording each callee's and
    argument's start/end offsets — never from unparsing normalised nodes (the parser keeps no spans
    today). (codex)
16. The root qualifier is dropped before every namespace lookup, the query alias map included, so a
    root-qualified reference to a dotted alias (`orders.customers.aov`) finds `customers.aov`, as the
    deleted strip made it. (codex)
17. Refinement measure identity and restatement are root-relative parsed meanings
    (`sum(amount)` ≡ `sum(orders.amount)`; `orders.revenue` restates `revenue`). Rejected: raw-text
    identity as today (qualified ≠ bare in one merge rule, inconsistent with dimensions). (user, codex
    finding, Option A)
18. Tests that call `strip_source_model_prefix` directly — `TestStripSourceModelPrefix`
    (`tests/test_models.py:1735-2000`), `tests/test_sql_generator.py:4569-4600`,
    `tests/test_dev1922_source_model_union.py:305`, `tests/test_dev1866_inference.py:60`,
    `tests/test_dev1733_order_only_transform_composite.py:263`, and the
    `tests/test_dev1713_naming.py:567` docstring — are ported to behavioural equivalence (the qualified
    spelling yields the bare spelling's SQL and keys; a non-root qualifier is untouched); assertions on
    the strip's own output go with it. (user)

## Risks / Trade-offs

- [A consumer reading a path head before or after binding misses the rule] → the qualified ≡ bare law
  in `tests/test_dev2187_root_qualifier.py` runs every surface for every root kind against the bare
  spelling (SQL, keys, warnings), so a missed consumer fails it.
- [Order-coercion rework changes a legacy spelling's sentinel or `raw_formula`] → parity pins against
  today's output for colon, call-style, expression and column entries, literals with `:` and `(`.
- [Spans change parse results] → spans are additive offsets on located elements; the full existing
  syntax suite must stay green unchanged.
- [Root-relative merge turns a silent duplicate into a conflict] → only refinements restating a saved
  entry under another spelling with a differing explicit field; spec scenario pins the collapse.
