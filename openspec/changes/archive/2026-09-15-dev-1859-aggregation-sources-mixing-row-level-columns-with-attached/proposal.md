# Proposal: row-grain aggregation sources — mixing row-level columns with attached values

## Why

The last unfinished leg of axiom 6 (`architecture/semantics.arc42.md`, tagged
`[target: DEV-1859]`): an aggregation source mixing a raw row-level column with an
attached (partitioned-aggregate) value — `sum(quantity * avg(unit_price,
partition_by=product))` — fails closed at parse, though the grain-union doctrine
already defines it: the union with a row leaf is row grain, so the outer aggregate
consumes base rows with the attached value broadcast per row (explicit, intended
per-row weighting — the deliberate counterpart of the row-count-weighting bug
DEV-1847 guards against). The same doctrine covers an attached value arriving as a
*parameter* of a row-level aggregation (deferred here from DEV-1892):
`customers.spend:weighted_avg(weight=sum(amount, partition_by=customers.regions.name))`.
Alongside, an execution-confirmed pre-existing miscompile: a non-shift transform
over a row-level leaf that refines the query grain (`cumsum(weight)`, `rank(qty)`)
silently inflates the base grain to one row per (bucket, leaf-value).

## What Changes

- **Leg A — the mixed source compiles at row grain.** The operand's grain-union
  determines the aggregation's input relation (DEV-1847's typing rule, second
  branch): the source alone decides — a source with any row leaf (or no attached
  constituent at all) is row grain; the input relation is the row-filtered base
  population with every attached input, in the source or in a parameter,
  row-attached (null-safe LEFT join on its complete grain — the computed-dimension
  mechanism); the outer aggregation evaluates inline at its consumer grain. One
  classifier over the aggregation's full input set replaces the source-only one, so
  a pure re-aggregation is pure by definition and a row-attach root owns its inputs;
  discovery is opaque below such a root's inputs (never below its partition keys),
  so no walk discovers those inputs as consumers of the enclosing level and the
  regroup pass acts on an explicit inline / own-producer disposition. The full
  expression-source surface applies by grain type — outer `partition_by=`/`window=`,
  windowed or cross-model constituents, parametric/custom aggregations,
  `count`/`count_distinct`; `first`/`last` over the expression stays rejected by
  the existing expression rule; no new rejections. The parse gate narrows: only
  nested transforms in sources stay rejected.
- **Leg B — typed rejection for grain-refining row leaves under non-shift
  transforms**, in THE checker, sharing one row-leaf walker with the shift-family
  checker; projected grain keys (plain or computed) stay legal. Closes the
  miscompile; remedy: aggregate the leaf.
- **Leg C — attached parameters on row-level sources.** Determination is recursive
  (an aggregate is determined iff each partition key is), an ungrained parameter
  types at the query's dimensions like an ungrained constituent, the association
  producer row-attaches the parameter into level 1 and DEV-1892's level-1 pick
  carries it to level 2, and DEV-1892's "attached parameter requires an attached
  source" rejection is removed wholesale. Covers the associate headline, the
  ordinary kernel, the mixed-plus-parameter shape, cross-model parameters on a
  local root, the literal-source form, and the default / error-mode twins.
- **One renderer for expression sources**: the aggregate's expression source
  renders once through the scope (which already resolves placeholders, expands
  derived columns and registers joins); the private renderer and its dead
  cross-model guard go.
- `architecture/semantics.arc42.md` axiom-6 tag flips `[target: DEV-1859]` →
  `[enforced: test:…]` (normative harness, separate per-change approval).
  Explicit non-changes: `guards.baseline` stays 1, `index.yaml` untouched, nothing
  to delete in `slayer/mcp/server.py`. Further structural cleanup (one discovery
  walk, one producer flag, one transform checker, typed first/last dispatch) is
  DEV-1903.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `queries/semantics`: adds the row-grain branch of axiom 6's second-order rule —
  a source mixing row leaves with attached values aggregates over the row-filtered
  base population with attached constituents broadcast per row, no warning — and
  the rule that an ungrained aggregate parameter types at the query's dimensions.
- `queries/partitioned-aggregates`: mixed sources join the legal surface with the
  full expression-source modifier set, discovery opacity below a root's inputs, one
  producer per distinct attached input; the attached-parameter form becomes legal
  on row-level sources under the recursive determination rule, in every kernel,
  position and mode.
- `aggregations/expression-aggregation`: the "Unsupported expression shapes"
  boundary narrows — only transforms nested in the aggregated expression remain
  rejected; the mixed row/attached scenario and DEV-1892's attached-parameter
  scenario flip to accepted.
- `queries/transforms`: new requirement — non-shift transforms reject
  grain-refining row-level leaves with a typed checker error; projected grain
  keys and the shift family's bare-leaf regime are exempt.

## Impact

- `slayer/core/keys.py`: `attached_inputs`, pure `is_reaggregation_key`,
  `is_row_attach_root`, `attached_operand_keys`, `walk_consumer_keys`;
  `is_mixed_source_key` deleted.
- `slayer/ir/bound.py`: the combined-consumer and dimension root walks go opaque
  below a root's inputs.
- `slayer/engine/compile/stages.py`: one root-discovery walk by predicate; the
  regroup block acts on an explicit disposition; `_answers_need_nested_regroups`
  at the five nesting sites; the association producer runs nested discovery and
  maps picked-parameter keys through its plan's substitutions; ungrained-parameter
  normalisation at the two determination sites.
- `slayer/engine/join_safety.py`: recursive `grain_determines` aggregate arm.
- `slayer/engine/elaborate_env.py` / `bind_inputs.py`: `_first_row_leaf` shared by
  both transform checkers; `check_attached_param_requires_attached_source` and its
  detector deleted.
- `slayer/sql/generator.py`: `_render_expression_source_sql` via `scope.resolve`;
  `attached_columns` removed from five signatures; scope required at every caller
  incl. the time-shift leaf path.
- `slayer/engine/syntax.py`: parse gate narrowed (leg A, retained).
- Tests: classifier / determination / walker unit tests; plan-structure pins for
  opacity, disposition and dedup; leg C executed suites on the DEV-1840 and
  DEV-1847 graphs (SQLite + DuckDB); windowed-constituent and scope-render cases;
  golden `dev1859_sql_baseline.json`; ledger row removed; three consented
  re-points (two from the first pass, `test_dev1892_residue.py::
  TestAttachedParameterOnRowLevelSource` from the second).
- Docs: `docs/concepts/formulas.md` boundary sentence (done) + parameter sentence;
  `docs/concepts/queries.md` associate sentence.
- Sequencing: DEV-1892 is merged into the branch (PR #394 open); this change
  archives after DEV-1892's archive lands on main; the PR opens against main if
  #394 has merged, else against the 1892 branch and retargets — merge only.
