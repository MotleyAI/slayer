# Design: dev-1857-living-architecture-scaffold-likec4-arc42-import-linter

## Context

See proposal.md — Why. This is the one-time Init of the living-architecture layer per the
global `living-architecture` skill; DEV-1857 fixes the node tree, wedge depth, contract set,
and no-CI constraint. All numbers below were re-measured on 2026-09-03 with an AST scan
(runtime edges only; `TYPE_CHECKING`-guarded imports excluded) and matched the issue.

## Goals / Non-Goals

**Goals:**
- Enforcement bundle green on the scaffold commit: `poetry run lint-imports`,
  `poetry run python tools/arch_check.py`, pinned LikeC4 validation, basedpyright at baseline.
- Model truth machine-checked: the LikeC4 relation set must equal the measured runtime
  node-level edges at every commit (arch_check model-truth check).
- Monotonic ratchet: `ignore_imports` entries only ever removed; per-contract count ≤ the
  baseline recorded in `index.yaml`.

**Non-Goals:**
- No CI wiring (flow gates only: spec-review, arch-slice move gate).
- No code moves; the follow-up slices (core purity → `slayer/ir` extraction → sql internals)
  are separate later issues. Contracts/model must merely anticipate `ir` (it joins the
  `layers` contract when the package is created).
- No contract for `storage`'s outbound tangles (storage → engine/sql/memories/search) yet.
- No ADRs, no DECISIONS.md successor log.

## Decisions

### D1 — Node tree and claims (fixed by the issue)

| Node | Kind | Claims |
| -- | -- | -- |
| `core` | precise | `slayer.core` + loose module `slayer.async_utils` |
| `sql` | precise, children `render`, `dialects` | `slayer.sql` |
| `engine` | precise | `slayer.engine` |
| `storage` | precise | `slayer.storage` |
| `importers` | virtual | `slayer.dbt`, `slayer.cube`, `slayer.osi`, `slayer.ingest_report` |
| `search` | virtual | `slayer.search`, `slayer.embeddings` |
| `memories` | virtual | `slayer.memories` |
| `protocols` | virtual | `slayer.flight`, `slayer.pg_facade`, `slayer.facade` |
| `surfaces` | virtual | `slayer.api`, `slayer.mcp`, `slayer.cli`, `slayer.client`, `slayer.inspect`, `slayer.demo`, `slayer.__main__` |

"Top-level `slayer.*`" = immediate children of `slayer/`: package dirs containing
`__init__.py` plus loose `*.py` modules; `slayer/__init__.py` itself is the exempt root.
Non-Python files, `.pyi`, `__pycache__` ignored. A future unclaimed top-level module fails
arch_check by design.

### D2 — index.yaml schema (two deviations from the skill's example)

- Precise nodes use `package:` plus optional `claims:` for loose modules
  (`core: {package: slayer.core, claims: [slayer.async_utils]}`); buckets use `packages:`.
- Contract baselines live in a top-level `contracts:` section
  (`layers: {baseline: 15}`, `forbidden: {baseline: 2}`) rather than per node, because the
  `layers` contract spans three nodes and has no single owner. arch_check ratchets per
  contract.
- Spec mapping — all three top-level groups are cross-cutting:
  `queries: {touches: [core, engine, sql]}`, `models: {touches: [core, storage, engine]}`,
  `aggregations: {touches: [core, engine, sql]}`. Ownership attaches at the top-level spec
  group and inherits recursively over nested capability dirs.

### D3 — Grandfathered edge inventory (the baselines)

`layers` (`slayer.engine` | `slayer.sql` | `slayer.core`), 15 unique module→module edges:

```
slayer.core.query -> slayer.engine.syntax
slayer.core.formula -> slayer.sql.window_detect
slayer.core.models -> slayer.sql.dialects
slayer.core.models -> slayer.sql.sql_predicate
slayer.core.models -> slayer.sql.window_detect
slayer.core.query -> slayer.sql.window_detect
slayer.sql.client -> slayer.engine.timing
slayer.sql.generator -> slayer.engine.binding
slayer.sql.generator -> slayer.engine.column_expansion
slayer.sql.generator -> slayer.engine.planned
slayer.sql.generator -> slayer.engine.source_bundle
slayer.sql.generator -> slayer.engine.stage_planner
slayer.sql.render.order_terms -> slayer.engine.planned
slayer.sql.scope -> slayer.engine.column_expansion
slayer.sql.scope -> slayer.engine.source_bundle
```

`forbidden` (`slayer.core` ↛ `slayer.storage`), 2 edges:

```
slayer.core.models -> slayer.storage.migrations
slayer.core.query -> slayer.storage.migrations
```

Re-verify by running `lint-imports` with empty ignores first; the exact failing edges become
the `ignore_imports` lists. `exclude_type_checking_imports = true`,
`unmatched_ignore_imports_alerting = "error"` on both contracts.

### D4 — Measured node-level runtime edges (the LikeC4 relation set)

37 directed edges (2026-09-03); this exact set becomes the model's relations, and the
arch_check model-truth check keeps it equal to the AST measurement thereafter:

```
core -> engine #legacy          core -> sql #legacy            core -> storage #legacy
engine -> core                  engine -> memories             engine -> search
engine -> sql                   engine -> storage
importers -> core               importers -> engine            importers -> sql
memories -> core                memories -> engine             memories -> search
memories -> storage
protocols -> core               protocols -> engine            protocols -> storage
search -> core                  search -> engine               search -> memories
search -> storage
sql -> core                     sql -> engine #legacy
storage -> core                 storage -> engine              storage -> memories
storage -> search               storage -> sql
surfaces -> core                surfaces -> engine             surfaces -> importers
surfaces -> memories            surfaces -> protocols          surfaces -> search
surfaces -> sql                 surfaces -> storage
```

`#legacy` marks the four edges slated to die in the follow-up slices (target layering
engine → sql → [ir →] core; core imports nothing internal).

### D5 — Constrained .c4 authoring convention (enables text-level cross-check)

`likec4 validate` owns real syntax; arch_check parses only this convention, documented in
`system.arc42.md`:
- every node element declared as `<id> = <kind> '<title>'` (one per line);
- all relations at model top level, flat, one `<src> -> <dst>` per line (never inside
  element bodies, never `this`/`it`), `#legacy` tag on the same line where applicable.

### D6 — arch_check.py checks (stdlib `ast`/`tomllib`/`re` + pyyaml)

From `index.yaml` as the hub: (1) every claimed package/module exists on disk; (2) every
top-level `slayer.*` package/module claimed exactly once; (3) every `contract:` named exists
in `[tool.importlinter]` and every pyproject contract is known to index.yaml; (4) every
`arc42:` path exists; (5) bidirectional node↔element identity with `architecture/model/*.c4`
(every precise node id declared as an element; every declared element maps back to a node or
declared child; children declared in both); (6) every top-level dir under `openspec/specs/`
mapped exactly once (a node's `specs:` or `cross_cutting_specs:`) and containing ≥1
`spec.md`; every `touches:` node exists; (7) per contract: `ignore_imports` count ≤ recorded
baseline; (8) model truth: AST-measured runtime node-level edge set == the model's relation
set; (9) `[enforced: <id>]` tags in arc42 files are syntax-valid and ids naming an
importlinter contract or arch_check check exist (`test:`-prefixed ids taken on trust).
Non-zero exit with a readable finding list. Covered by `tests/test_arch_check.py`
(tmp-dir fixtures; negative cases: unclaimed module, duplicate claim, missing arc42, unknown
`touches:` node, baseline exceeded, missing/extra element, relation drift).

### D7 — basedpyright

Dev dep; `[tool.basedpyright]` in pyproject: `typeCheckingMode = "basic"`,
`include = ["slayer", "tests", "tools"]`, exclude generated `slayer/flight/_flight_sql_pb2.py`
(and sibling generated pb2 files if present). Baseline via
`poetry run basedpyright --writebaseline`, committed at `.basedpyright/baseline.json`.
Smoke-verify during implementation: inject a deliberate type error → red; remove → green.

### D8 — LikeC4 CLI pinning

Pin an exact version in the documented invocation, e.g. `npx -y likec4@<pinned> validate .`
run from `architecture/` (fall back to the lightest parsing command if `validate` is absent
in that version — decide once at implementation, then pin). Invocation + version recorded in
`system.arc42.md`, not CI.

### D9 — DECISIONS.md fold

Produce a categorized inventory (every entry → arc42-principle / already-in-openspec-corpus /
history-only), fold the still-true prescriptive structural rules into `system.arc42.md` or
`sql.arc42.md`, grep the repo for references to `DECISIONS.md` and update them, then delete —
only after explicit user confirmation at that moment. CLAUDE.md keeps its convention
one-liners and gains a single pointer line to `architecture/` + the enforcement bundle.

## Risks / Trade-offs

- [Model-truth check couples arch_check to the AST scan's edge definition] → the scan and the
  grandfather lists come from the same measurement code path; convention D5 keeps parsing
  trivial; `likec4 validate` still guards real syntax.
- [basedpyright baseline may be large in basic mode] → baseline absorbs it; the gate is
  "no NEW errors", and arch-slice tightens over time.
- [npx requires Node at gate time] → acceptable: gates run on dev machines; version pinned;
  no CI dependency introduced.
- [DECISIONS.md deletion could lose still-relevant rules] → mitigated by the D9 inventory +
  user confirmation; history recoverable from git.

## Migration Plan

Single PR; no deploy surface. Rollback = revert the PR (nothing under `slayer/` changes).
The `/spec` flow for this change skips the spec-tests stage (user decision;
`tests/test_arch_check.py` is written in spec-implement instead).

## Open Questions

None.
