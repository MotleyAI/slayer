## 1. Prerequisites

- [x] 1.1 Merge DEV-1607 (PR #604) into this branch and respell the deltas and design to its names (verify: `openspec validate dev-2219-stages-inherit-joins-on-pass-through-key --strict` passes)
- [ ] 1.2 If DEV-2277 merged first, respell the scenarios to its reference rules with the owner's approval of the exact edit (verify: validate passes)
- [x] 1.3 After PR #604 merges, merge `origin/main` before opening this change's PR, so its diff holds only this change (verify: `git diff --stat origin/main...HEAD` lists no DEV-1607 file)

## 2. Tests (pr-tests; unit, SQLite and DuckDB, no `integration` marker)

- [x] 2.1 Ordering and graph membership (`queries/multi-stage` › Stages are ordered…): the expression-only sibling under both name orders, routing never through an unnamed sibling, permutations (verify: fail before implementation)
- [x] 2.2 Inheritance (› Stages inherit edges on pass-through keys): every scenario, including reverse direction, extension relationships, transitivity, diamond dedup, filters, same-named stage, and the parity law against an explicit extension relationship (verify: fail before implementation)
- [x] 2.3 Cardinality (› Inherited edges re-derive…): one-to-one by grain, wider grain, raw-row stage (verify: fail before implementation)
- [x] 2.4 Conflicts (› Declared edges win…): identical applied once, same name wins, differently named same-keys edges stay parallel, parallel distinct edges need names (verify: fail before implementation)
- [x] 2.5 One scope (› A stage is read as the dataset…, `queries/dotted-dimension-routing`): edge-less stages unchanged for flat names, list elements, stale spellings, root qualifier, re-bucketing, population inference; the dotted-spelling error; routing through an inherited edge (verify: the new-behaviour cases fail before implementation)
- [x] 2.6 Saved query-backed models (`queries/query-backed-inline`): inheriting like a stage, declared absorbs inherited, inspect listing and its failure cases (cycle, invalid key, required variable) on MCP, REST and CLI (verify: fail before implementation)
- [x] 2.7 Existing tests that encode flat stages (the 12 probe failures and any whose values change from unrelated to joined): ask the owner, per file, to align each; never change one without approval (verify: owner's approval recorded)

## 3. Ordering and join-graph membership

- [x] 3.1 Extend `stage_sibling_reads` to return siblings named by any parsed reference in a stage's expressions, through the one Mode-B reader (verify: 2.1 ordering cases pass)
- [x] 3.2 Thread into each stage's join graph exactly the siblings 3.1 returns, replacing "every planned sibling" in `stage_bundle_with_siblings` (verify: 2.1 routing case passes)

## 4. Pass-through lineage and inheritance

- [x] 4.1 Add a typed pass-through origin to `StageField` (bound field key, resolved path with each hop's edge identity, field), set in schema emission only for untransformed public row-level columns (verify: unit test on emitted schemas for bucket, cast, computed, alias and dotted cases)
- [x] 4.2 Compute inherited edges in an engine module from a stage's origins and its candidate edges (stored, its extension relationships, upstream inherited), both directions, with the hop-target proxy, resolved target identities, origin identity for dedup, name and join type per the design, and cardinality per decision 8; carry them as typed data on `StageSchema` (verify: 2.2 and 2.3 pass)
- [x] 4.3 `dataset_from_stage_schema` merges inherited edges with a saved model's declared relationships per decision 16 and the same-name rule; a consuming extension's relationship of the same name replaces the inherited edge (verify: 2.4 passes)

## 5. One binding scope

- [x] 5.1 The planner's stage-population branch and `resolve_scope` always yield a model scope over the stage's dataset (verify: 2.5 passes)
- [x] 5.2 Delete the now-dead `StageSchema`-scope branches in `binding`, `bind_inputs`, `elaborate_env`, `extensions`, `key_metadata` and `compile/stages` (verify: `la-typecheck` no new errors; local test scope green)

## 6. Inspect

- [x] 6.1 Render a saved query-backed model's inherited edges in the `relationships` section and the summary, marked inherited with the source edge, derived from its dry-run expansion under default variables; on a planning error list none and carry the typed error's message (verify: 2.6 passes on MCP, REST, CLI)

## 7. Architecture, docs, gates

- [x] 7.1 Apply the approved `architecture/python.engine.arc42.md` P5 and §4 rationale wording verbatim (design decisions 5–6) (verify: `la-arch-check` green)
- [x] 7.2 Docs: inherited edges in `docs/concepts/queries.md` (multi-stage) and `docs/concepts/models.md` (query-backed models), the inspect output in `docs/reference/mcp.md`, the stage help memories, and a Part 1 entry in `docs/upgrading/2.0.md` for the unrelated → joined change (verify: every changed page reachable from `zensical.toml` nav)
- [x] 7.3 Enforcement bundle and lint: `la-arch-check`, LikeC4 validation, `la-typecheck`, `ruff check slayer/ tests/` (verify: all green)
