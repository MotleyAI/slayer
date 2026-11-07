## Why

Mode-B text is still re-tokenised outside `core.syntax` (system §3.18): the source-model prefix strip
regex-rewrites filters, measure formulas and order expressions, so `note = 'see orders.x'` silently
becomes `note = 'see x'` and `"orders.x"` becomes `"x"`; the spine and splice detectors fire on a
literal that merely names `time_spine` or a query-backed model; variable demand is read by a Mode-A
regex; and functional time dimensions, order coercion and the misplaced-measure check slice text by
hand. The root-qualifier rule the strip approximated is itself copied at seven bind sites, all
comparing the internal model name, so a stage root's own name (`s1.tot * 2`) fails.

## What Changes

- A reference whose leading qualifier names the query's root — a model, an extension's base, an
  inline model, a stage by its name in its query list, a spliced query-backed model — resolves to the
  root at bind time (`orders.amount` ≡ `amount` from root `orders`), the root winning over a join or
  alias spelled the same; result keys are unchanged.
- **BREAKING (internal API)**: `SlayerQuery.strip_source_model_prefix` and its regex helpers are
  deleted; no text rewrite precedes binding.
- Text inside a string literal or a quoted identifier is never read as a reference: no rewrite, no
  spine query, no splice.
- The variables a Mode-B surface reports as needed are exactly those its binding looks up.
- Time-dimension dedupe and refinement merge identify dimensions, time dimensions and measures by
  their root-relative parsed meaning; a refinement restating a saved measure by a root-qualified name
  restates it.
- Every Mode-B reader outside `core.syntax` (spine/splice detection, variable demand, functional
  time dimensions, order coercion, the misplaced-measure check, `main_time_dimension` matching)
  reads the parse tree; `core.syntax` exposes call-argument source spans for the slicing cases.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `queries/dotted-dimension-routing`: "Non-routing resolution is unchanged" states the root-qualifier
  rule, its root set, and its precedence.
- `queries/expression-grammar`: quoted text is never a reference, in any consumer.
- `queries/variables`: reported variable demand equals binding's lookups.
- `queries/time-dimensions`: construction-time dedupe compares the root-relative parsed dimension.
- `queries/saved-query-refinement`: merge identities are root-relative parsed meanings, measures
  included.

## Impact

- `slayer/core/syntax.py` (new readers, call-argument spans), `slayer/core/query.py` (strip deleted;
  dedupe, refinement merge, variable demand, functional time dimensions, order coercion),
  `slayer/core/time_spine.py`.
- `slayer/engine/binding.py`, `bind_inputs.py`, `column_dependency.py`, `model_reads.py`,
  `unrelated.py`, `population.py`, `normalization.py`, `plan.py`, `stage_ordering.py`,
  `query_engine.py`; `slayer/ir/variables.py`; `slayer/inspect/model_render.py`;
  `slayer/sql/column_expansion.py` (same root helper, Mode A).
- `architecture/system.arc42.md` §3.9 / §3.18 and `architecture/semantics.arc42.md` Axiom 2.4
  (approved wording); `docs/concepts/references.md` (one sentence).
- Tests calling `strip_source_model_prefix` directly are ported to behavioural equivalence.
