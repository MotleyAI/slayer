# SLayer — system architecture

## 1. Purpose & context

SLayer is a semantic layer for AI agents: agents describe measures, dimensions,
and filters; SLayer generates and executes the SQL. This file is the root of the
living-architecture layer — present-tense structure and principles, updated in
place. Behaviour lives in `openspec/specs/`; decision history lives in
`openspec/changes/archive/` and git (there is no ADR log).

## 2. Building blocks

See the `landscape` view in [views.c4](views.c4) (model in
[model/slayer.c4](model/slayer.c4)). Nine nodes: precise `core`, `sql`,
`engine`, `storage` around the query pipeline; virtual buckets `importers`,
`search`, `memories`, `protocols`, `surfaces` for the rest. Package claims,
contract baselines, and spec mapping are in [index.yaml](index.yaml).
Contributor-level pipeline docs: `docs/architecture/`.

## 3. Principles

All code, old and new, MUST obey these.

1. **Target layering**: `engine` → `sql` → `core`; a future `slayer/ir` slots in
   between `sql` and `core` when extracted. The 15 grandfathered edges are dying,
   never growing. [enforced: layers]
2. **`core` imports no other SLayer node.** The 3 remaining edge classes
   (`core → engine/sql/storage`) are grandfathered and slated to die.
   [enforced: layers] [enforced: forbidden]
3. **Every top-level `slayer.*` package/module belongs to exactly one node.**
   A new top-level package must be claimed in `index.yaml` in the same change.
   [enforced: arch_check:claims-exactly-once]
4. **Model truth**: the LikeC4 relation set equals the AST-measured runtime
   node-level import edges at every commit — the model describes the code as it
   IS, `#legacy` marks edges slated to die. [enforced: arch_check:model-truth]
5. **Ratchet**: `ignore_imports` entries are only ever removed; per-contract
   counts never exceed the `index.yaml` baselines. Wanting to add one means the
   architecture is changing — change model + contract + arc42 deliberately, with
   explicit user OK. [enforced: arch_check:baseline-ratchet]
6. **SQL is built as sqlglot AST**, never by string concatenation of fragments.
   [review]
7. **Async-first**: engine and storage methods are async; sync entry points
   bridge via `execute_sync` / `run_sync`. [review]
8. **Cardinality invariant**: adding a measure/field never changes result
   cardinality or other fields' values. [review]
9. **Dotted-canonical references**: dots denote join paths in queries and model
   SQL; the legacy `__` split-alias input form is a hard error; `__` survives
   only as an internal generated-SQL join alias (`__slayer_` prefix reserved).
   [enforced: test:tests/test_dev1743_resolution.py]
10. **Two expression layers**: Mode A free SQL (`Column.sql`, model `filters`)
    vs Mode B Python-AST DSL (formulas, query fields, scalar allowlist only) —
    one canonical `SCALAR_PASSTHROUGH` set, extended never forked. [review]
11. **Versioned persistence**: models/queries/datasource configs carry
    `version`; migrations run automatically on load. [review]
12. **Pydantic v2 for all models; never dataclasses.** [review]
13. **The ValueKey union grows reluctantly**: a construct that traverses like an
    existing key kind reuses it (reserved-name scalar, reserved-leaf
    placeholder) rather than adding a union member — hand-rolled visitors are
    fail-open on new kinds. [review]
14. **Ingestion is idempotent and additive-only**: user metadata is never
    overwritten by a re-ingest (`source_kind` refresh is the one documented
    exception). [review]
15. **Row-level security fails closed**: anything a session policy cannot
    confirm is rejected, never passed through unscoped. [review]

## 4. Enforcement

The enforcement bundle — run by the spec-review gate and the arch-slice move
gate (deliberately NOT wired into CI yet):

```bash
poetry run lint-imports
poetry run python tools/arch_check.py
npx -y likec4@1.47.0 validate architecture   # pinned; run from the repo root
poetry run basedpyright                      # gate = no new errors vs baseline
```

Each enforced principle carries an `enforced:` tag (square-bracketed, checked
by arch_check) whose id names an import-linter contract (`layers`,
`forbidden`), an arch_check check (`arch_check:<check-id>`), or a test
(`test:<pytest path>`, taken on trust). Unenforced principles are written
`[review]` — each one is a standing candidate for a fitness function.

## 5. Model authoring convention

`likec4 validate` owns syntax; `arch_check` additionally parses `model/*.c4`
under this constrained convention:

- every element declared as `<id> = <kind> '<title>'`, one per line (children
  nest inside the parent's `{ }` body);
- all relations flat at model top level, one `<src> -> <dst>` per line, never
  inside element bodies, never `this`/`it`; `#legacy` on the same line.

## 6. Rationale

The wedge is focused on the query pipeline because that is where boundary
violations accumulate (see `sql.arc42.md`); buckets stay coarse until real work
touches them. Structure-shaping decision trails: the DEV-1450 typed-pipeline
redesign and DEV-1742 consolidation (git history, `docs/architecture/`), and
the archived changes under `openspec/changes/archive/`.
