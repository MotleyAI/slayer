## Why

A declared join edge may be spelled two ways in a path — its edge `name` or its target model's name — and
resolution stores whichever token was typed. Every downstream path comparison (interning, SQL join aliases,
the home rule, the DEV-1908 D9 determination route, grain determination) is therefore spelling-sensitive:
mixing spellings of one named edge joins its model twice, or broadcasts a grand-total aggregate across a
dimension it actually determines (a spelling-dependent value, with only a warning). The issue's two
prefix functions are two instances of this class; the fix is one canonical spelling set at resolution.

## What Changes

- A resolved join path is **canonical**: per hop, the edge's declared name, else its traversal-target model.
  Every door that turns typed tokens into a stored path (query binding incl. saved measures and auto-routing,
  Mode-A `Column.sql` / model-filter qualifier resolution, definition-default cancellation) emits it, so keys,
  join aliases and every path comparison are spelling-invariant by construction.
- **BREAKING (narrow)**: result keys follow the canonical path. A named edge referenced by its model name now
  answers under the edge-name key (`customers.regions.rname` → `orders.customers.hr.rname`,
  `customers.regions.pop:max` → `customers.hr.pop_max`). Unnamed edges, edge-name spellings and parallel
  named edges are unchanged.
- Two spellings of one dimension in one query behave exactly like the identical dimension requested twice.
- Stage-boundary slack rule `STALE_PATH_SPELLING`: a flat name referenced across a stage boundary (a downstream
  stage, a query-backed model's columns from a query or another model's `Column.sql`) that matches exactly
  one upstream column's non-canonical respelling binds to that column with a typed warning — stored queries
  written against the old spelling keep working, including after an edge is named later.

## Capabilities

### New Capabilities

### Modified Capabilities
- `models/join-traversal`: result keys carry the canonical path (was "as typed"); new requirement that paths
  resolve to one canonical spelling.
- `queries/dotted-dimension-routing`: byte-identical result keys only for already-canonical paths; new
  requirement for the stale-spelling stage-boundary slack rule.

## Impact

- `slayer/core/join_walker.py` (canonical path helper; `walk_cancelling`), `slayer/engine/binding.py`,
  `slayer/engine/bind_inputs.py` (saved-measure / routed naming), `slayer/sql/column_expansion.py`,
  `slayer/engine/reference_closure.py`, `slayer/core/scope.py` (`StageColumn` respellings + resolver),
  `slayer/engine/compile/stages.py` (schema emission), `slayer/engine/query_engine.py` (query-backed columns,
  warning plumbing, cache-hit warnings), `slayer/core/warnings.py` / `slayer/engine/normalization.py` (rule id).
- Architecture: `system.arc42.md` §3.9 (canonical paths) and `engine.arc42.md` §3.7 (stage-boundary slack),
  both approved.
- Existing tests pinning typed-spelling result keys (e.g. `tests/test_dev1853_named_edges.py`) and golden SQL
  baselines where a named edge was referenced by model name change.
- Docs: `docs/concepts/references.md`.
