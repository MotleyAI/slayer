# Dotted references to another model's saved measures (`customers.aov`)

## Why

Saved measures (`ModelMeasure`) are referenceable by bare name only from a query rooted at their own model; a dotted reference to another model's saved measure (`customers.aov` from an `orders`-rooted query) fails at bind time with an error that lists only the target's columns and never mentions its measures. DEV-1836 made cross-model aggregates first-class (target-rooted producers, safe-grain broadcast, `strict`), so once a dotted saved-measure reference expands and re-anchors, its aggregates inherit that entire semantics for free — this change is pure name-resolution sugar, no new SQL machinery.

## What Changes

- `customers.aov` in a measure formula or computed-dimension expression resolves `aov` as a saved measure on `customers`, binds its formula against the target model, and structurally re-anchors the bound tree into host coordinates (the §5.4 re-rooting machinery run in the prepend direction). The result is bound-tree-identical to the hand-written `customers.`-prefixed formula.
- **One measure-resolution authority**: the pre-bind expansion pass (`slayer/engine/measure_expansion.py`) is deleted; all saved-measure resolution (bare local and dotted cross-model) moves into the binder, where models, joins, and the bundle already live. Bare-name behavior is unchanged (mechanism moves, semantics stay).
- **One re-anchoring authority**: `reroot_value_key` is refactored into a generic path-mapping visitor; strip (reroot) and prepend become thin wrappers that cannot drift. Reroot behavior stays byte-identical.
- Resolution order per position: alias map → column → saved measure; eligibility mirrors the local feature exactly (measure formulas and computed-dimension expressions only; everywhere else errors clearly).
- Round-trip expansions (a target measure crossing a join back toward a model already on the host→target chain) are rejected with a first-class error — parity with the hand-written spelling, which errors today.
- **BREAKING (fail-closed)**: a query-backed model (`source_queries`) declaring `measures` directly is rejected at validation time — those measures were silently dropped during virtual expansion and never worked. `ModelExtension`-supplied measures over a query-backed base keep working (the overlay is re-applied after expansion).
- Misleading errors fixed: a dotted leaf matching a saved measure in an ineligible position, an aggregation suffix on a saved measure (`customers.aov:sum`), and a leaf matching neither namespace all get precise messages naming both namespaces with suggestions.

## Capabilities

### New Capabilities

- `queries/saved-measures`: saved-measure referencing — bare-name resolution on the host model (existing behavior, first-time specified), dotted cross-model resolution with typed re-anchoring, position eligibility, resolution order, naming and metadata, recursion/cycles/depth, round-trip rejection, the query-backed constraint, and the error contract.

### Modified Capabilities

<!-- none: cross-model aggregate semantics are consumed, not changed -->

## Impact

- `slayer/engine/binding.py`: measure fall-through in `_resolve_ref` / `_resolve_dotted`, target-anchored bind, eligibility context, cycle/depth chain, alias precedence for dotted names.
- `slayer/engine/measure_expansion.py`: deleted; call sites in `slayer/engine/stage_planner.py` drop the pass and pass eligibility instead; naming/metadata helpers gain dotted awareness; raw-row measure detector extended through the join graph.
- `slayer/core/keys.py`: generic path-map visitor; `prepend_value_key` added; `reroot_value_key` becomes a wrapper.
- `slayer/core/models.py`: query-backed + `measures` validation.
- Tests: `tests/test_measure_expansion.py` + `tests/test_model_measure_expansion.py` migrated to binder entry points (approved); new dual-engine executed-value suites (SQLite + DuckDB); reroot/prepend structural suite.
- Docs: `docs/concepts/formulas.md`, `docs/concepts/models.md`, `docs/concepts/references.md`, `.claude/skills/slayer-query.md`, `.claude/skills/slayer-models.md`.
