## Context

Colon leaks into agent-visible output because four emitters (dbt, Cube, OSI importers; the root-model
recommendation path) each hand-assemble aggregation text (`f"{col}:{agg}"`). engine §3.2 makes the
*parser* spelling-insensitive; nothing makes *emission* single-spelling. The importers validate their
output with the legacy `core/formula.py::parse_formula`, which rewrites functional text to colon
internally and warns on every functional formula.

Applicable principles: engine §3.1 (typed pipeline) and §3.2 (every spelling collapses to one parse
node); semantics Axiom 2.7 (spelling-invariance — example only); system §3.10 (two expression layers,
untouched). No new capability; `architecture/index.yaml` unchanged; all imports stay on existing arrows.

## Goals / Non-Goals

**Goals:** no colon spelling in docs, help, MCP descriptions, error/warning remedies,
`recommend_root_model` paths, or importer-written formulas; `architecture/` and `openspec/specs/` free of
colon examples except the one legacy-acceptance requirement; comments follow D7.

**Non-Goals:** test inputs; internal colon machinery (parser colon preprocessing,
`canonical_measure_text` alias derivation, the SQL facade's internal `measure_formula`s — DEV-1956,
memory-resolver entity tokens); the colon deprecation warning (DEV-1920); retiring `core/formula.py`
(DEV-1831); `openspec/changes/archive/`.

## Decisions

**D1 — One functional renderer.** `functional_agg_text(*, source: str, suffix: str) -> str` in
`slayer/core/refs.py`, the inverse of `split_agg_suffix`: `("amount", "sum")` → `sum(amount)`,
`("*", "count")` → `count(*)`, `("orders.*", "count")` → `count(orders.*)`,
`("price", "percentile(p=0.9)")` → `percentile(price, p=0.9)`,
`("amount", "sum(window='30d')")` → `sum(amount, window='30d')`,
`("balance", "last(updated_at)")` → `last(balance, updated_at)`; an empty-paren suffix `sum()` →
`sum(amount)`. Every emitter routes through it (recommend path `query_engine.py` `_emit_recommend_path`;
dbt `converter.py` sum_boolean / percentile / mapped agg / filtered leaf; Cube `_STAR_COUNT`, windowed
agg, view-facade star-count and agg re-exports; OSI `expression.py` agg / star count / count_distinct /
count / median / percentile). Alternative rejected: per-site f-string fixes — four copies of arg-splicing
logic that must agree by hand.

**D2 — The legacy validator stays, patched.** `_rewrite_funcstyle_aggregations` drops its
"Auto-rewrote … use colon" warning and accepts `<ident-or-path>.*` as its first argument
(`count(customers.*)` → `customers.*:count`). Importers keep calling `parse_formula`; its internal colon
round trip is DEV-1831's to retire. Alternative rejected: port importer validation to `parse_expr` now —
that is all of DEV-1831 (rebuild dangling-reference checks, delete/rewrite legacy test files).

**D3 — Cube validation carries the column and closes over dependencies.** `_validate_offline` receives
the cube's `_MeasureInfo` map and indexes it by `emitted_name` (it is keyed by the Cube member name,
which differs from the emitted name after a namespace collision). (1) An `agg` measure whose
`underlying_col` was dropped is dropped with a `COMPLEX_MEASURE` report entry (previously silent).
(2) Dropped measure names leave `known`; `_formula_parses` re-runs to a fixpoint, so a calc measure
naming a dropped measure, directly or transitively, is dropped and reported (previously left dangling).
(3) The `named_measures` stand-in becomes `count(*)` (named measures expand before the funcstyle
rewrite). (4) `m.formula.split(":")` is deleted. Report messages quote the emitted (functional) formula.

**D4 — Functional remedies.** `errors.py` `DerivedColumnFanningError`: `(<aggregation>({reference}))`;
`elaborate_env.py` cross-hop remedy: `(<aggregation>({hop}.<column>))` (ledger row
`tests/_dev1871_raise_ledger.py` in lockstep); `elaborate_env.py` transform row-leaf remedy:
`{op}(sum({disp}))`; `param_binding.py` and `sql/generator.py`: `'{agg}(measure, {name}=column)'`;
`syntax.py` entity-ref error: "only the `agg(column)` form names an entity"; `models.py` `_NO_COLON`:
colons stay reserved "as the legacy aggregation separator", no `revenue:sum` example; `formula.py`
bare-measure errors: `(e.g., 'sum({name})')`; `mcp/server.py` `create_model` description: custom
aggregations used as `sum_sq(column)`.

**D5 — `recommend_root_model` always replies functionally**, whatever the input spelling, via D1;
argument-bearing suffixes keep their arguments (`percentile(amount, p=0.9)`, `last(amount, ordered_at)`).

**D6 — Specs.** MODIFIED deltas for every requirement carrying a colon example (25 capabilities, found by
a broad scan that includes custom aggregation names); scenario titles unchanged. In
`aggregations/functional-form` the colon-equivalence requirement is REMOVED and its guarantee ADDED back
as "The legacy colon spelling is accepted as an exact equivalent" with a single parametrized scenario
(OpenSpec refuses a MODIFIED block that drops scenarios, so RENAMED+MODIFIED cannot collapse it); parity
THENs elsewhere become
concrete outcomes; a new requirement states emission is functional-only. The capability's `## Purpose`
is edited directly in the main spec (a delta cannot carry it).

**D7 — Comments/docstrings.** Colon shown as an example of user syntax → functional. A comment describing
colon machinery that stays (`core/formula.py` internals, `canonical_measure_text`, `split_agg_suffix`,
facade `measure_formula`, resolver tokens) stays accurate and calls it the "legacy colon spelling".
Non-forward issue-number references in touched comments are removed; touched files are brought into full
compliance.

**D8 — Docs.** `docs/dbt/dbt_import.md` mapping row: `<agg>(col)` only. Final sweep of `docs/`, MCP tool
descriptions and help text.

**arc42 (approved verbatim during planning).**
E1 `architecture/semantics.arc42.md` Axiom 2.7:
`types: \`sum(customers.spend)\` and \`sum(customers.spend + 0)\` share one home.` /
`A single-column source is the one-leaf case of 2.2 — the model` (replacing the two lines that listed
`customers.spend:sum` first).
E2 `architecture/engine.arc42.md` §3.2: `in the parser; every aggregation spelling collapses to one node, so`
/ `everything downstream is spelling-insensitive by construction.`

## Risks / Trade-offs

- [The legacy validator and the engine parser can still disagree on importer output] → the guard test
  also parses every emitted formula with `parse_expr`; DEV-1831 removes the second parser.
- [Spec rewrite volume (≈100 requirements)] → generated mechanically token-by-token, reviewed for
  spelling-contrast prose; residual-colon scan over `openspec/specs/` after archive-apply.
- [Re-imported models change formula text] → both spellings execute identically and keep result keys;
  already-saved models are untouched (models persist verbatim, engine §3.8).
