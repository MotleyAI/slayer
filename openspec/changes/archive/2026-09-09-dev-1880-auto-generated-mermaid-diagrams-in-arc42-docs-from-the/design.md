# Design: Auto-generated mermaid diagrams from the LikeC4 model

## Context

See proposal.md — Why. Current state: `tools/arch_check.py` parses `model/*.c4` with
two regexes (`_ELEMENT_RE`, `_RELATION_RE`) capturing only ids and bare edges;
`views.c4` holds `landscape` and `query_pipeline`; the arc42 docs reference those
views by name. `npx likec4 validate` owns full syntax; `arch_check` parses only the
constrained §5 authoring convention. Enforcement bundle is deliberately not in CI;
`pytest` is the gate everyone actually runs.

## Goals / Non-Goals

- Goals: diagrams render on plain GitHub (any mermaid version); byte-deterministic
  generation; freshness enforced by both `arch_check` and plain `pytest`; the fix is
  always exactly one command; the constrained convention stays fail-closed.
- Non-Goals: full LikeC4 view syntax (anything unsupported fails loudly telling the
  author to extend the convention); user-facing `docs/`; CI wiring; styling beyond
  shape + line-style semantics.

## Decisions

1. **Pure-Python generator, no Node** — `npx likec4 codegen mmd` was rejected: drags
   node/npx into the check path, loses `#legacy` (all edges dashed), emits `@{shape}`
   syntax needing mermaid ≥ 11.3. `likec4 validate` still owns full syntax in the
   enforcement bundle.
2. **One parser of the convention** — `arch_diagrams.py` becomes the single parser of
   the §5 convention; `arch_check` imports it via importlib (as tests already import
   `arch_check`) and deletes its duplicate regex parsing. Two parsers of enforced
   text would drift silently. Existing finding message texts are preserved.
   Alternative (side-by-side parsers, minimal diff) rejected for exactly that drift.
3. **Fail-closed grammar** — inside `specification` / `model` / `views` blocks every
   non-blank, non-comment line must match the explicit grammar (element, relation,
   `tag`, `#virtual`, `title`, `include`, braces) or it is a finding; scanning is
   quote- and comment-aware so `{`/`}` in titles or comments cannot corrupt nesting.
   Validated invariants: duplicate element/view ids, duplicate relations, unknown
   relation endpoints, undeclared kinds.
4. **View semantics mirror LikeC4 1.47.0 codegen** (verified empirically): `*` = all
   top-level elements; bare ids = those elements; edge set = model edges among
   `*`/bare-id elements ∪ predicate-matched (`x -> *`, `* -> x`) edges; element set =
   explicit elements ∪ predicate-edge endpoints; no neighbor-to-neighbor edges from
   predicates. Bare ids must be top-level (LikeC4 nests children — flat emission
   would silently diverge, so child includes fail loudly).
5. **Emission format** — `flowchart TD`; second fence line `  %% <view_id>: <title>`
   (mermaid comment) so title-only view edits still trip freshness; declaration-order
   nodes/edges, first-occurrence dedup; `id["Title"]` precise / `id("Title")` virtual
   kinds; `-->` live / `-.->` legacy; `"` in titles → `#quot;`; legend line
   `*Dashed arrows: legacy edges slated to die.*` after the fence iff the diagram has
   a legacy edge; no frontmatter (classic syntax renders on any GitHub mermaid).
6. **Markers, not whole-file generation** — humans place
   `<!-- likec4:<view_id> -->` … `<!-- /likec4:<view_id> -->` once (§2 of each doc);
   the generator rewrites strictly between markers, preserves every other byte, and
   writes with `newline=""` (LF, no translation) for byte-stable reruns.
7. **`diagrams:` lives in index.yaml** — the existing cross-walk file; schema
   validated fail-closed (repo-relative `architecture/*.arc42.md` keys, non-empty
   string lists, no duplicates). `core.arc42.md` gets the new `core_focus` view
   (core-incident star illustrates system principle 2; the full landscape is a
   44-edge hairball in mermaid auto-layout); `semantics.arc42.md` maps to nothing.
8. **`skip_specs: true`** — developer tooling + architecture docs, no product
   behaviour change (DEV-1869 law-harness precedent).

## Risks / Trade-offs

- [Convention parser diverges from LikeC4 semantics] → `likec4 validate` stays in the
  bundle; unsupported constructs fail loudly instead of guessing; empirical check
  against 1.47.0 codegen recorded in the issue.
- [Parser consolidation churns existing checks] → `test_arch_check.py` assertions and
  finding texts unchanged; only module wiring and fixtures move.
- [Byte-for-byte check is brittle across environments] → generation is pure text from
  repo files; LF pinned; no timestamps or environment input.
- [arch_check must not crash on malformed input] → parse errors become findings;
  remaining checks still run.

## Migration Plan

Single PR; regeneration is idempotent so re-running the command is always safe.
Rollback = revert (docs keep rendering their last-generated diagrams).
