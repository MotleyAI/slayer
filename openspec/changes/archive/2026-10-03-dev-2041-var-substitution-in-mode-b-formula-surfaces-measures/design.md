## Context

Mode-B text is parsed in three places: at query construction (dimension strings and order entries are classified by parsing them, `core/query.py` `_coerce_dimension_item` / `_order_formula_candidate`), during the one normalization pass, and at binding. Substitution is a textual pass that runs once, after normalization and before binding (`query_engine.py` ~L1118/L1136, `plan.py` `_prepare`). The parser has no notion of a placeholder, so any template parsed before that pass fails as `unsupported AST node Set`. Mode-A substitution covers only the direct source model and stage source models; joined models are DEV-1678.

Principles that apply: system §3 P10 (two expression layers, python regime for Mode-B) and P13 (kind dispatch fails closed); engine P1 (substitution rewrites raw text, never parsed output), P2 (parsing is pure syntax), P7 (one normalization pass); core P4 (typed errors); ir P1/P2 (pure, imports `core` only). No arc42 / `.c4` edit and no new import edge.

## Goals / Non-Goals

**Goals:** see the `queries/variables` spec. Design-level boundaries:
- one placeholder notion in the parser, so nothing that parses before substitution needs special-casing;
- name derivation reads the template; substitution itself stays a pure `ir` pass.

**Non-Goals:**
- Literal-only enforcement of variable values (DEV-2044). Formula surfaces substitute textually exactly like filters.
- Substitution on joined / cross-model lineages, for Mode-A surfaces and saved measures alike (DEV-1678).
- Substituting time-dimension granularities or columns (user ruling: variables supply literals).
- Substituting values into the parsed tree: it would break the documented contract that an unquoted `{var}` is raw text substitution.

## Decisions

### D1 — `Placeholder` parse node
A set literal holding exactly one bare name (`{k}`) parses to `Placeholder(name)`; any other set shape stays unsupported. It is admitted in every position a `Literal` is admitted: root, arithmetic and unary operands, comparison operands, the whole `in` / `not in` right-hand side, aggregation source / args / kwargs (`_AGG_SOURCE_KINDS` and `AggCall`'s typed fields), transform args / kwargs, scalar-function args, CASE / `iif` branches. `canonical_measure_text` renders it `{k}`. Every `ParsedExpr` walker and dispatcher handles it or fails closed. Binding a `Placeholder` raises `UnresolvedPlaceholderError(SlayerError)` (new, in `core/errors.py`). It is a binding input error, not an algebra type error, so it is not a `QueryTypeError`.
*Alternative rejected:* render `{var}` as `0` for construction-time classification and name from raw text. That means two parse inputs that must agree by hand, and naming that depends on spelling (`sum(amount) * {k}` → `sum_amount_k` vs `amount:sum * {k}` → `amount_sum_k`).

### D2 — Substituted surfaces and the rebuild
`ir.apply_variables_to_query` substitutes (python regime) `filters`, `measures[*].formula`, `ComputedDimension.expression`, `OrderItem.raw_formula`, and every `TimeDimension.date_range` bound. It returns `SlayerQuery.model_validate({f: getattr(q, f) for f in q.model_fields_set | {"version"}} | changed)`, passing typed objects through, the same pattern as `refine_query`. Construction validators therefore re-run on substituted text (date-range shape, `_dedupe_time_dimensions`), and omission semantics, `source_model` objects and migrated versions are preserved. `extract_placeholder_names(query)` walks the same surfaces. At construction, a `date_range` bound containing a placeholder skips the shape check. A `{…}`-shaped time-dimension granularity or column (dict form, or string form `{g}(col)` / `gran({col})`) raises a clear "variables supply literal values; they can't name a granularity / column" error. The `{? ?}` python-regime rejection is reworded from "filters" to "Mode-B expressions".

### D3 — Template carried as a private attribute; names derived at binding
`ModelMeasure` and `ComputedDimension` gain a pydantic `PrivateAttr` `_template: str | None`, set by `apply_variables_to_query` whenever substitution changes the entry's text. It isn't in the schema or in dumps; `model_copy` and the typed-object rebuild carry it. Binding (`bind_inputs`) derives everything name-related from `template or text`:
- **Measure:** an unnamed measure's public / declared name is `_canonical_alias_for_formula(template, parsed=parse_expr(template))` via the text path (no `bound`, since an aggregate-root key alias would embed the substituted value). It stays unnamed (`alias_name is None`, `name_is_explicit=False`), so the unnamed dedupe and collision check run on that public name. The substituted formula's own canonical alias is recorded as `canonical_alias` whenever it differs, so references through substituted text resolve.
- **Computed dimension:** `name_is_explicit = name != auto_name_from_expression(template or expression)`, both at `bind_inputs.py` ~L961 and in `_resolve_granularity_calls` (~L1224).

The order is unchanged: normalize (templates parse thanks to D1) → substitute → bind. Each query is substituted exactly once per pipeline: the root and named siblings in `query_engine`, spliced stages in `plan._prepare`.
*Alternatives rejected:* (a) pin `ModelMeasure.name`: that makes the entry explicit, skipping dedupe and changing alias wiring (Codex). (b) A side map from entry to template threaded into binding: plumbing through `plan_stages` / `_prepare`, and fragile when normalization moves entries.

### D4 — Saved `ModelMeasure.formula`
`substitute_model_sql_surfaces` also substitutes each `ModelMeasure.formula`, under the python regime, on the same models (direct source + stage sources). `_mode_a_surfaces` stays Mode-A only. Saved-measure placeholders are added to `model_placeholder_names` and `model_needs_substitution_pass` and to `extract_model_variables` (required unless defaulted; an optional block there is a substitution error, not "optional"). Saved measures carry explicit names, so no template naming is needed.

### D5 — Saving a query-backed model requires every variable
The save-time render runs exactly as execution does with no runtime variables. `dry_run_placeholders` and the `0` fill are deleted. A placeholder with no value from the model's `variables`, a stage's `variables` or a source model's defaults refuses the save with the undefined-variable error. Models that aren't query-backed are unaffected. Type probing of an already-stored query-backed model that lacks a default returns no types, as an undefaulted Mode-A template does today.

## Risks / Trade-offs

- [A path rebuilds measures or dimensions from dumps and loses `_template`] → names would derive from the substituted text. Mitigation: the executed name-stability tests run through the multi-stage, spliced-stage and query-backed paths; persisted queries store templates, not substituted text.
- [A new `ParsedExpr` kind missed by some dispatcher] → P13 fail-closed tails; a parse test per admitted position, and binding asserts `UnresolvedPlaceholderError`.
- [`{{k}}` in a filter now reports a placeholder error instead of a Set syntax error] → intended; the message names `{{` / `}}`.
