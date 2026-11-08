# Proposal: Functional form for all aggregations

## Why

Aggregations are currently first-class only in colon syntax (`revenue:sum`); the
functional spelling `sum(revenue)` is tolerated "slack" that a regex layer rewrites
on some surfaces and rejects on others (`ModelExtension` measures, hand-authored
YAML, entity refs). DEV-1826 requires that every aggregation writable as
`col:agg(args)` also be writable as `agg(col, args)` — in every position, for every
column and aggregation, without exception — as the first step toward eventually
retiring colon syntax.

## What Changes

- `parse_expr` accepts functional aggregations natively, emitting the same `AggCall`
  node as colon syntax; coverage of all Mode-B positions holds by construction.
- Unknown function names with an aggregatable first argument defer to the binder
  (parity with `x:whatever`), so custom aggregations and case/alias healing
  (`SUM(x)`, `countD(x)`) work with no parser plumbing.
- Binder validates the aggregation name globally before per-column gates, so
  `*:bogus` / `bogus(*)` fail with the standard unknown-aggregation error.
- The `FUNC_STYLE_AGG` slack rule and its `NormalizationWarning` are removed;
  functional input is no longer rewritten or warned about, and saving a model
  preserves the author's spelling. **BREAKING** for consumers asserting on
  `FUNC_STYLE_AGG` warnings.
- The legacy importer pipeline (`core/formula.py`) keeps its internal rewriter;
  the three quiet `func_style_agg_to_colon` call sites (stage planner, schema
  drift, memories resolver) are deleted as dead once the parser is native; order
  coercion moves to the parser-native path via the existing placeholder +
  `raw_formula` machinery.
- Parity explicitly covers the positions added by DEV-1740/1824/1839: computed
  dimension expressions (with the same partition_by guards) and mixed-grain
  arithmetic.
- Entity-reference surfaces (memories/search resolver, `recommend_root_model`)
  accept functional refs like `sum(orders.revenue)`.
- New: same-model expression aggregation `sum(amount - cost)` via binder-level
  desugar reusing the DEV-1740 row-level value-key machinery, with result keys
  auto-named by the same rule as computed dimensions; cross-model expressions,
  filtered-column references, and per-column gates are explicitly bounded.
- Custom aggregation names colliding with scalar-allowlist functions are rejected
  at validation (mirroring the existing transform-name rejection). **BREAKING**
  only for stored models with such names (none expected).
- Docs: colon-primary presentation plus an authoritative equivalence section;
  architecture docs rewritten to describe the native parser branch.

## Capabilities

### New Capabilities

- `aggregations/functional-form`: functional spelling `agg(col, args)` as a
  first-class equivalent of colon syntax in every Mode-B position and
  entity-reference surface, including dispatch/ambiguity rules, name healing,
  result-key identity, and retirement of the slack rewrite.
- `aggregations/expression-aggregation`: aggregation over same-model scalar
  expressions (`sum(amount - cost)`), including grammar boundaries, binding
  desugar, deterministic naming, gate/type semantics, and error contracts for
  unsupported shapes (cross-model, filtered columns, nesting).

### Modified Capabilities

(none — the existing corpus capabilities (`queries/computed-dimensions`,
`queries/partitioned-aggregates`, `queries/cross-model-aggregates`,
`models/join-cardinality`) are spelling-agnostic; this change adds an equivalent
spelling and an additive expression capability without altering any of their
requirements)

## Impact

- Code: `slayer/engine/syntax.py`, `slayer/engine/binding.py`,
  `slayer/engine/normalization.py`, `slayer/engine/stage_planner.py`,
  `slayer/engine/schema_drift.py`, `slayer/core/query.py`, `slayer/core/keys.py`,
  `slayer/core/refs.py`, `slayer/core/models.py`, `slayer/sql/naming.py`,
  `slayer/sql/generator.py` (+ `render/`), `slayer/memories/resolver.py`,
  `slayer/engine/query_engine.py`.
- Docs: `docs/concepts/{references,formulas,queries}.md`,
  `docs/architecture/{slack-normalization,parsing}.md`,
  `docs/examples/07_aggregations/`, `.claude/skills/slayer-{query,models}.md`,
  `slayer/memories/help_content/03_aggregations.md`.
- Follow-up Linear issues: docs flip to functional-primary; legacy
  `core/formula.py` consolidation; cross-model expression aggregation.
