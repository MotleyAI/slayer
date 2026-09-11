# Design — extract slayer/ir

## Context

See proposal.md — Why. Measured at head (main + DEV-1847 merged): the 9
grandfathered `sql → engine` edges resolve to 6 engine modules, and sql's
actual needs are narrow — `planned` types (`SlotId`, `ValueSlot`,
`MaskTyping`, `RankedGrainMember`, `BoundExpr`), `ResolvedSourceBundle` +
two pure bundle helpers, three `column_expansion` functions,
`walk_value_keys`, `regroup_producer_identity`, and the `timing` module.
Constraints: the ratchet rule (ignore-list entries only removed, baselines
only lowered), model-truth (LikeC4 relations = measured import edges at every
commit), the arc42/.c4 normative-harness rule (each edit individually
approved), and end-state-over-churn (no re-export shims).

## Goals / Non-Goals

**Goals:** kill all 9 `sql → engine` edges; `slayer/ir` = representation
only, importing `core` only; deterministic (rope-verified) moves; enforcement
bundle green.

**Non-Goals:** the DEV-1871 term language; the core-purity slice (the 5
remaining `core → engine/sql` edges); any rename or behaviour change —
relocation only, names byte-identical.

## Decisions

1. **`column_expansion` → `sql`, not `ir`** — it is Mode-A sqlglot analysis
   (imports `sql.reserved_keywords`); ir must stay below sql. Alternative
   (move `reserved_keywords` into ir) rejected: drags dialect-keyword
   machinery below sql and dilutes ir. Engine importing sql is the sanctioned
   direction.
2. **`timing` → `core`, not `ir`** — execution metadata, not representation;
   core hosts cross-cutting leaf domain types. Keeps ir pure for DEV-1871.
3. **`Grain` → `ir/grain.py`** — algebra vocabulary DEV-1871 builds on;
   consumers are engine-only today, so the move is 2 import sites now vs a
   second structural PR later.
4. **`walk_value_keys` → `core/keys.py`** — a 5-line generic walker over the
   `ValueKey` `children()` traversal protocol belongs next to the protocol,
   not a layer above it.
5. **`source_bundle` splits** (Codex HIGH): `ir/source_bundle.py` gets
   `ResolvedSourceBundle` + pure helpers; the async storage-backed builders
   move to `engine/bundle_builder.py`. Moving the module wholesale would put
   storage I/O into ir — "representation-only" by construction, not by
   TYPE_CHECKING loophole.
6. **No re-export shims; `planned.BoundExpr` re-export dies** — one real home
   per symbol (`ir/bound.py`); all importers repointed.
7. **Deterministic mechanics** — every Python move via `dr-refactor`
   (`move-module` / `move-symbol`), dry-run reviewed then `--apply`; gate per
   move: basedpyright zero unbaselined diagnostics (moved files' baseline
   entries path-rewritten, never wholesale-regenerated) + dr-mock-lint. Hand
   edits only for non-Python artifacts. `make-refactor-target-compliant`
   first where a symbol's blast radius is untyped.
8. **Red phase = the ratchet flip** — empty `slayer/ir` package + layers
   contract edit + baseline 14 → 5; `lint-imports` must fail listing exactly
   the 9 edges while pytest stays green. The moves turn it green.

## Risks / Trade-offs

- [rope misses a dynamic/string reference] → post-move repo-wide grep for old
  dotted paths (`engine.planned`, `engine.source_bundle`, `engine.timing`,
  `engine.variables`, `engine.column_expansion`, `core.grain`) over slayer/,
  tests/, docs/, .claude/; plus full-suite + collection of integration tests.
- [three tests encode old symbol ownership] → approved rewrites listed in
  proposal; all other test changes are mechanical rope repoints.
- [model-truth breaks mid-stack] → architecture edits land in the same commit
  as the final move set; arch_check runs at the end of the implement stage,
  not per-move.
- [`ir/__init__.py` accretes re-exports later] → ir.arc42.md principle:
  import from submodules; `__init__` stays a docstring.

## Migration Plan

Single PR; no data or API migration. Rollback = revert the PR. PR bases on
`egor/dev-1847-…` until DEV-1847 lands, then retargets `main`; merge commits
only.

## Open Questions

None.
