# Design: one import law at every declared granularity

## Context

See proposal.md — Why. Current state: `tools/arch_check.py` enforces node-level
`model-truth` (AST-measured runtime edges == declared relations, TYPE_CHECKING-guarded
imports excluded, root `slayer/__init__.py` exempt); import-linter carries the module-level
doors (`layers`: 5 ignores, `forbidden`: 2 ignores; `exclude_type_checking_imports = true`;
lint-imports is green, so those 7 pairs are the only cross-layer module edges).
`tools/arch_diagrams.py` is the single parser of the constrained LikeC4 authoring subset
(§5) and the deterministic mermaid generator; `sql` already declares children
`render`/`dialects`; CI runs neither tool. DEV-1871 has NOT landed — there is no
`term-boundary` contract and no `contracts-from-model` glue to migrate (sequencing
deliberately inverted with user approval; the DEV-1871 agent has been notified).

## Goals / Non-Goals

**Goals:**

- One enforcement law (extended `model-truth`) whose precision follows declared C4
  granularity; import-linter fully retired; parity with the old ban-set proven.
- Locality: declaring a child never forces redeclaring its unrelated edges.
- DEV-1871 (and any future boundary) expressible purely by declaring children + arrows.

**Non-Goals:**

- No new runtime behaviour; no changes under `slayer/` itself.
- No retirement of any door (baseline only moves 7 ignores + 3 coarse arrows → 8 declared
  legacy arrows; the allowed import set does not grow).
- No `likec4`-CLI dependency inside arch_check/tests; no LikeC4 SVG export pipeline.
- No subgraph-free fallback views; no per-arrow ratchet listing in index.yaml.

## Decisions

### D1. The law: coverage semantics ("S2"), not finest-granularity equality ("S1")

**Attribution.** Each endpoint of a runtime import resolves to its *finest declared
element*: the declared child whose dotted module path is the longest prefix of the imported
(or importing) module's path, else the owning node. Undeclared modules ARE their node —
there is no residual pseudo-child. TYPE_CHECKING-guarded imports stay excluded (deliberate
agreement with retired import-linter behaviour, now stated in system.arc42.md §3). The root
`slayer/__init__.py` stays exempt.

**Measured set.** All (finest-src, finest-dst) pairs over runtime imports, dropping
self-pairs and ancestor↔descendant pairs (parent↔own-child edges are internal plumbing:
never declared, always allowed).

**`from package import name`.** The extractor keeps emitting both `package` and
`package.name` targets: importing a submodule genuinely executes the parent package's
`__init__`, so both edges are faithful. `package.name` attributes to a child only when its
prefix matches a validated on-disk child path; a re-exported attribute that merely shares a
child's name resolves identically to importing that submodule, which is acceptable fuzz.

**Coverage.** Arrow (x, y) covers measured edge (a, b) iff x ∈ {a} ∪ ancestors(a) and
y ∈ {b} ∪ ancestors(b). Specificity is the product order: (x₁,y₁) ≤ (x₂,y₂) iff x₁
is-or-descends-from x₂ AND y₁ is-or-descends-from y₂; strict if different. Never compare
depths numerically — one-sided refinements are incomparable.

**Findings.**
- *missing-arrow*: a measured edge covered by no declared arrow; the finding names a
  module→module witness import.
- *dead-arrow*: a declared arrow that is a most-specific (minimal under ≤) cover of no
  measured edge. An arrow all of whose covered edges are also covered by strictly more
  specific declared arrows is dead; incomparable covers of the same edge all stay live.
- *internal-arrow*: a declared arrow whose endpoints are in an ancestor/descendant (or self)
  relation. Those edges are never measured (internal plumbing), and such an arrow would
  asymmetrically cover sibling edges (e.g. `core -> core.models` would license
  `core.query -> core.models`), so it is rejected and excluded from coverage. CI runs
  arch_check alone, so this cannot lean on `likec4 validate` to reject it.

**Consequences.** With no children declared this reduces exactly to today's node-level
equality. Sibling child→child edges (same parent) can only be covered by an explicit arrow
(the would-be parent cover is a self-pair), so bans between declared siblings are the
default — which is what makes DEV-1871's term boundary expressible by declaration alone.
Cross-node child edges stay covered by parent-level arrows, so no accidental legalization
or breakage.

**Alternative rejected (S1):** exact set equality at finest granularity. Non-local —
declaring one child demands explicit arrows for every cross-node edge it touches, turning
each refinement into a model-wide rewrite.

### D2. Element identity is the FQN

Parser and checks key every element by its fully qualified dotted id (`core.query`); the
local declaration name is retained only as the render label. Identical leaf names under
different parents are legal. Relations reference non-top elements by FQN; top-level nodes
by bare id. `views.c4` and `model/*.c4` stay valid LikeC4 (`likec4 validate` remains the
syntax authority in the enforcement bundle); only the constrained §5 authoring *subset*
grows dotted endpoints.

### D3. index.yaml schema

- `children:` on non-virtual single-package nodes: a list of dotted paths *relative to the
  node's package* (`query`, `render`, later e.g. `render.joins`), each of which must exist
  on disk as `<package>/<path>.py` or a package directory, be named at most once, not
  collide with any other node's declared package/claim (an attribution ambiguity), and must
  agree with the model's nesting (extends `claims-exist`/`model-identity`). Longest-prefix
  attribution makes the law recursive-ready; today all entries are single-segment.
- `contracts:` block replaced by `legacy_arrows: {baseline: 8}`. `baseline-ratchet`
  retargets to: count of `#legacy` arrows in the model == baseline. The count is a loud,
  stateless backstop; monotonicity ("only ever lower") is enforced by review of the paired
  .c4 + index.yaml diff, exactly as with the retired `ignore_imports` regime — stated
  explicitly in §3 P5.
- `root_package: slayer` moves to index.yaml top level (arch_check's config source once
  `[tool.importlinter]` is gone).
- `view_depth: {<view-id>: N}` optional map, default 3 (D5). Malformed values (missing or
  negative baseline — 0 is the ratchet's goal state and legal; non-integer or non-positive
  depth, unknown view id, duplicate/unknown child paths) produce findings, never exceptions.
- `guards:` untouched.

### D4. Keep the Python parser; keep mermaid

`npx likec4 export json` as the parser was considered and rejected: arch_check must stay
hermetic and fast (new CI step, ~30 tmp-repo pytest fixtures), and fixtures would need full
LikeC4 validity. The regex parser already handles nesting; this change adds dotted
endpoints. LikeC4 SVG export and D2/Graphviz were rejected for diagrams: they break inline
GitHub rendering and the byte-for-byte `diagrams-fresh` regime. Mermaid `subgraph` blocks
render hierarchy natively and keep the pipeline deterministic.

### D5. Views: per-view relative depth, subgraph rendering

For each view, elements more than `view_depth − 1` levels below the view's shown top
elements collapse into their ancestor at the cutoff; edges roll up with dedupe, self-edges
dropped, a rolled-up edge dashed iff ALL contributing arrows are `#legacy`. Focus
predicates (`x -> *`, `* -> x`) match arrows by top-level ancestor of the endpoint;
pulled-in child endpoints bring their ancestor chain. Rendering: nodes with rendered
children become (recursively nested) `subgraph` blocks; child mermaid ids mangle `.` → `__`
with leaf-name labels; edges attach at whatever level each endpoint lives (mermaid
flowchart supports node↔subgraph edges); tasteful `classDef` styling, theme-safe (no
hard-coded fills that break dark mode); deterministic ordering throughout; legacy legend
line as today.

### D6. Model deltas (each .c4/arc42 edit individually user-approved at implement time)

- Children declared: `core.query`, `core.models`, `engine.syntax`, `sql.sql_predicate`,
  `sql.window_detect`, `storage.migrations`.
- Node arrows removed: `core -> engine/sql/storage` (all `#legacy`).
- Child arrows added — doors, `#legacy`: `core.query -> engine.syntax`,
  `core.models -> sql.dialects`, `core.models -> sql.sql_predicate`,
  `core.models -> sql.window_detect`, `core.query -> sql.window_detect`,
  `core.models -> storage.migrations`, `core.query -> storage.migrations`; plus
  `core.models -> core.query` `#legacy` (lazy circular function-level import, tagged legacy
  so it can retire without ever growing).
- Child arrows added — plain truth: `core.query -> core.models`,
  `sql.render -> sql.dialects`, `sql.sql_predicate -> sql.window_detect`.
- Legacy baseline: 8.

### D7. Parity harness domain

`tests/test_import_law_parity.py` freezes the old law in code (layer order
`engine > sql > ir > core`, lower may not import higher; `forbidden` core ↛ storage; the 7
door pairs as literals **matched at declared-child granularity** — a door licenses its
target's subtree, because the new law cannot distinguish modules inside a declared child;
exact-module door matching is unsatisfiable once a door target is a package, e.g.
`sql.dialects` — a Codex finding). Domain: **cross-node** module pairs among
{engine, sql, ir, core} plus core→storage — intra-node pairs were never governed by the old
law and are excluded (Codex finding: including them falsifies parity, e.g.
`sql.render → sql.sql_predicate`). Assertions: (1) banned-by-old ⇒ banned-by-new,
unconditionally and forever; (2) allowed-by-old ⇒ allowed-by-new for every door still
declared in the model — exact parity at migration, monotone-safe as doors retire. New-law
verdicts come from an exposed `license(src_module, dst_module) -> bool` helper in
arch_check.

### D8. Tag vocabulary and doc sweep

`enforced-tags` drops import-linter contract ids; valid ids are `arch_check:<check-id>` and
`test:<path>`. Retag sweep covers ALL arc42 files: system.arc42.md §3 P1/P2 and
ir.arc42.md P2 (`[enforced: layers]`/`[enforced: forbidden]` → `[enforced:
arch_check:model-truth]`), P5 reworded to the legacy-arrow ratchet, §4 bundle drops
lint-imports and notes the CI step, §5 documents dotted endpoints/children/depth/subgraphs;
core.arc42.md door wording; all mapped diagrams regenerated. Out-of-repo:
`~/.claude/skills/{living-architecture,arch-slice,spec-review,spec-plan}` and
deterministic-refactor docs swept for lint-imports references (user-consented; diffs shown).

## Risks / Trade-offs

- [Attribution fuzz: a re-exported attribute sharing a declared child's name measures as a
  child edge] → faithful enough (the submodule import happens anyway); documented in §5;
  test pins the behaviour.
- [Count ratchet is not self-enforcing monotonic (add arrow + bump baseline in one commit
  passes)] → identical to the retired regime; the paired diff is loud and .c4 edits require
  explicit user approval; §3 P5 states that review owns monotonicity.
- [Coverage semantics subtler than set equality] → exact definitions in §3/§5 and this doc;
  fixture tests for most-specific/incomparable/shadowed cases.
- [Mermaid subgraph layout may render poorly on some views] → styling pass at implement
  time; layout-only tweaks (direction hints, classDef) don't change the byte-deterministic
  contract, they just set it.
- [Deleting import-linter breaks any un-swept reference] → repo grep shows references only
  in pyproject/tools/tests/architecture (all in scope) plus openspec archive (historical,
  untouched); skills sweep covers the out-of-repo harness.

## Migration Plan

Single PR: extend tools + rewrite model/index + retag docs + regenerate diagrams + delete
import-linter + add CI step, with the parity harness green in the same commit range.
`poetry run python tools/arch_check.py` green locally and in CI proves the cutover; the
full non-integration suite gates as usual. Rollback = revert the PR (no data, no runtime
surface). Out-of-repo skill edits land alongside the merge.

## Open Questions

None.
