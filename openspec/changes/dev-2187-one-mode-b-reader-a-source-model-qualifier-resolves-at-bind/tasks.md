## 1. Tests (pr-tests stage)

- [x] 1.1 `tests/test_dev2187_root_qualifier.py`: qualified ≡ bare (identical SQL, rows, result keys, warnings) on every surface — dimensions, column and expression time dimensions, measures and aggregation arguments, filters, order (column and expression), `main_time_dimension`, computed dimensions — for model, extension, inline-model, user-stage (`s1.tot * 2`, `sum(s1.tot)`, `count(s1.*)`, `s1.L.value`) and query-backed roots; precedence over an edge named like the root and over a dotted alias (`orders.customers.aov`); downstream-stage keys of a qualified dimension; computed-dimension auto-name keeps its written text. Verify: the stage-root and literal cases fail on the unchanged code.
- [x] 1.2 Same file: construction dedupe (`month(created_at)` ≡ `month(orders.created_at)`; literals never conflated) and refinement merge (qualified dimension, time dimension and measure formula collapse; `orders.revenue` restates; literal formulas stay distinct). Verify: failing on unchanged code where behaviour changes (measures).
- [ ] 1.3 `tests/test_dev2187_one_reader_law.py`: literal inertness over every Mode-B surface — `'orders.x'`, `"orders.x"`, `'time_spine.x'`, a literal naming a query-backed model leave SQL, population, spine status and splices unchanged; variable-demand parity (`'{x}'`, `{x}` demanded; `'{{x}}'`, `'{3}'` not; `"{x}"` refused) across query surfaces, saved measure formulas, inspection and the splice footprint. Verify: the prefix/spine/splice cases fail on unchanged code.
- [x] 1.4 Same file: parity pins with today's output for order coercion (colon, call-style, expression, column entries, literals holding `:` / `(`), functional time dimensions (nested calls, quoted `)` and `,`, `date_trunc('unit', x)`, kwargs, `--` comments) and the misplaced-measure bare-token check. Verify: green on unchanged code (pins).
- [x] 1.5 Port the direct strip tests (design Decision 18) to behavioural equivalence; delete assertions on the strip's own output. Verify: ported tests pass on unchanged code and stay green after the strip is deleted.
- [ ] 1.6 Codex-review the tests against this change. Verify: findings resolved or logged.

## 2. core.syntax readers

- [x] 2.1 Add `drop_root_qualifier`, `root_relative`, `referenced_paths` and `variable_names` (sharing `_check_variable`'s region scan; `bind_variables` uses it). Verify: unit tests in 1.x for these readers pass.
- [x] 2.2 Add located parse elements recording callee and argument start/end offsets, and the call-argument helper returning `(callee, argument source text)`. Verify: full existing syntax suite green unchanged; 1.4 span cases pass.

## 3. Root qualifier at bind time

- [x] 3.1 Route every root comparison through `drop_root_qualifier(parts, host.spelling)`: `binding._resolve_dotted` (before the alias-map lookup), `_canonical_if_routed` (returns the canonical path whenever the typed spelling differs), `_resolve_dotted_star`, `bind_inputs._joined_column_type`, `_resolve_saved_measure_ref`, `_named_td_matches`, `_host_local_default_td`, `column_dependency._hop_path`, `model_reads` (:216), `unrelated.unrelated_targets`, `sql/column_expansion.py` (:253, :289). Verify: 1.1 passes for model roots.
- [x] 3.2 `StageSchema` scope: `_resolve_dotted` / `_resolve_dotted_star` drop a leading `display_name` before the flat lookup and `_stage_list_scope`. Verify: 1.1 stage-root cases pass.
- [x] 3.3 Delete `strip_source_model_prefix`, `_strip_column_ref`, `_model_prefix_pattern`, `_strip_time_dimensions`, `_strip_order`, `_strip_measures`, `_canonical_text` and the calls at `query_engine.py:1158-1162`, `plan.py:450`, `stage_ordering.py:86-87`. Verify: no references remain (LSP find-references); 1.1 and 1.5 pass.

## 4. Comparison keys

- [x] 4.1 `_dedupe_time_dimensions` and `_merge_time_dimensions` key on `canonical_measure_text(root_relative(parse_expr(text), root))`, raw text when unparseable; refinement dimension identity via `drop_root_qualifier`; measure identity and `_drop_restatements` root-relative. Verify: 1.2 passes.

## 5. Regex readers replaced

- [ ] 5.1 `is_spine_query` (delete `mentions_spine` / `_SPINE_REF`) and `plan._spine_demands` read `referenced_paths` heads on today's surfaces. Verify: 1.3 spine and splice cases pass.
- [x] 5.2 Variable demand on Mode-B surfaces via `variable_names`: `extract_placeholder_names` (date-range bounds read separately), `ir/variables._measure_needs_pass`, `model_placeholder_names` and `extract_model_variables` for saved measure formulas, `_coerce_time_dimension_text`; delete `has_variable_syntax` / `_bare_variable_names` if no non-Mode-B caller remains. Verify: 1.3 parity cases pass.
- [ ] 5.3 Drop `render_probe_text` from `unrelated._parsed_heads`, `model_reads.query_reference_parts`, `population.py:130`, `query.call_callee`; replace `model_reads._node_parts` and `unrelated._dotted_heads` with `referenced_paths`. Verify: full unit suite green.
- [ ] 5.4 Functional time dimensions, order coercion, the misplaced-measure check and `main_time_dimension` matching read the parse tree (design Decision 4). Verify: 1.4 pins and 1.3 pass.

## 6. Architecture, docs, finish

- [x] 6.1 Apply the approved arc42 edits verbatim (design Decisions 5–7). Verify: `poetry run la-arch-check` green.
- [x] 6.2 One sentence in `docs/concepts/references.md` (DSL mode): "A reference may start with the query root's own name — a model, or a stage by its name in the query list (`sum(orders.amount)` from `orders` is `sum(amount)`); the root wins over a join spelled the same." Verify: `zensical.toml` nav unchanged (page already linked).
- [ ] 6.3 Full unit suite, CI-settings integration suite, `poetry run ruff check slayer/ tests/`, `poetry run basedpyright` (no new errors vs baseline). Verify: all green.
