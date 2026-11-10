## Context

See proposal.md › Why. History that shapes the design:

- `1.x` was `main` until 1.8.1 (2026-11-07). PR #509 (2026-11-08) merged the 2.0 branch into `main`; its merge `24122f19` (`origin/main` into the 2.0 branch) resolved list-column conflicts toward the 2.0 side. `main`'s merge base with `1.x` did not move, so the loss entered without any `1.x` commit entering `main`.
- Item 1 (sql-mode `element_type`) was a `1.x` spec requirement (DEV-2196 design D18). Item 2 (`collect` on a sole primary key) existed only in `1.x` code (`slayer/core/enums.py`), added for its `collect(assignments.id)` join-back test; `1.x`'s own `models/identifier-columns` never listed it.
- An audit of the 52 `1.x` requirements whose body differs on `main`, plus the 4 absent by title, found no lost behaviour; 4 clauses were lost from the spec only, their code still passing the dropped `1.x` tests. Every other difference is wording, an extension, or a deliberate change citing an archived change or commit.
- `1.x` writes `SlayerModel` version 15; on `main` 15→16 is the Mode-B SQL string rewrite, then time-dimension text (16→17) and `distinct` (17→18). A 1.8.2 document passes through all three.
- `main` re-ingest re-types stale sql-mode columns (`retyped_sql_columns`) through `_retyped_list`, which also re-infers a stored `ARRAY`/`UNKNOWN` when the live list type is the bare Snowflake `ARRAY` and an element type is known.

Applicable principles: system P6 (Pydantic), P10 (one typing — `element_type` canonicalised in one place), P11 (fail at save time — `UNKNOWN` is a valid declared state, not a definition guaranteed to fail); core P5 (typed errors); storage P3 and 3.2 (versioned persistence; stored artefacts are not invalidated — the 1.8.2 corpus enforces it for that release). No arc42 or `.c4` edit: `scripts/` sits outside the `python` root, and the touched spec groups (`models`, `compatibility`) are already mapped as cross-cutting in `architecture/index.yaml`.

## Goals / Non-Goals

**Goals:** a 1.8.2 store's list-column surface loads and answers on `main` as on 1.8.2; the four spec-only losses are specified and tested again; a forward merge cannot drop a `1.x` requirement silently.

**Non-Goals:**
- Inferring an sql-mode element type at save time from the live result, or on re-ingest beyond the existing Snowflake untyped-array rule.
- A full-state spec guard (every `1.x` requirement on every PR).
- Corpus shapes outside the list-column surface, and corpus queries whose results 2.0 deliberately changed (e.g. `length` of a list with repeats, per dev-2209).
- Backporting the `models/identifier-columns` `collect` clause to `1.x`.
- A migration-guide entry: 1.8.x behaviour is kept.

## Decisions

1. **Item 1: restore `1.x`'s rule** — an undeclared sql-mode element type is `UNKNOWN` on load and save — over keeping `main`'s refusal plus a stored-only v18→19 migration: one rule for stored and fresh documents, no version bump, and the migration would only buy a different error message for a valid state. (user)
2. **Item 1 is a model-level canonicalising validator**, as on `1.x` (`SlayerModel._validate_list_element_types`): `Column` stops requiring `element_type` on `ARRAY` (it still rejects it off `ARRAY` and rejects `ARRAY` as an element type); the model refuses a table-backed `ARRAY` without one and sets an sql-mode one to `UNKNOWN`. Only the model knows its mode, and after validation no model column is `ARRAY` with no `element_type`. (default)
3. **No save-time live inference and no new re-ingest re-inference for sql-mode `UNKNOWN`** — scope extensions, offered and not taken. (default)
4. **An sql-mode `UNKNOWN` re-infers on Snowflake like a table-mode one** (Codex finding 1): the restored requirement drops `1.x`'s "nothing SHALL infer it" and says an `UNKNOWN` element type changes only as the Snowflake untyped-array re-inference allows — sql-mode parity with table mode, no ingestion change; `1.x` never ran sql-mode re-typing, which is why its text said nothing infers. Option "skip re-inference for sql-mode" rejected: less than table mode, with a carve-out for no user benefit. User-approved wording: "An `sql`-mode model's `ARRAY` column without a declared `element_type` SHALL have `element_type: UNKNOWN`; a declared `element_type` SHALL be kept by every re-ingest, and an `UNKNOWN` one SHALL change only as the Snowflake untyped-array rule allows." (user, approved wording)
5. **Item 2: forward-port `collect` into the sole-primary-key set and specify it** in `models/identifier-columns`, over keeping the clamp plus a migration: the migration cannot keep a stored `collect(id)` measure's 1.8.x results, and `collect(id)` is a set read like the already allowed `count_distinct(id)`. (user)
6. **The facade gains a `collect` metric per sole primary key** — it follows from the one shared set, as on `1.x`, and every scalar column already exposes one. (default)
7. **The spec guard is a delta guard** scoped to the `1.x` commits a pull request brings into its base, over a full-state guard: 2.0 rewrites shared requirements constantly, so a full-state ledger would fill with intentional divergence; the past is covered by the one-time audit. (user)
8. **Guard mechanics:** `scripts/forward_merge_specs.py` (`python -m scripts.forward_merge_specs --base <sha> --head <sha> [--one-x origin/1.x]`) reads spec files at a revision through `git show`; requirements match by title plus whitespace-normalised body under any capability path; the ledger `openspec/forward-merge-ledger.yaml` holds `{title, one_x_hash, reason}` entries (sha256 of the normalised `1.x` body), starts empty, requires a reason and fails stale entries; removals on `1.x` are not checked; a CI step in the `lint` job, `pull_request` only, with `fetch-depth: 0`, comparing `github.event.pull_request.base.sha` with the head SHA; unit tests on throwaway git repos in `tmp_path`. (default)
9. **Guard ancestry rules** (Codex findings 2–3): `git merge-base --all`, failing on several bases; compare only when the base's merge base is a strict ancestor of the head's, so equal bases and branches behind `1.x` pass, and the step can run on every PR base ungated. A squash-merged forward merge is still checked on its own PR head; a cherry-pick that bypasses the guard is caught at the next real forward merge, whose range covers it. (codex)
10. **The guard is specified** as the new capability `compatibility/forward-merge-specs`, after `compatibility/removal-tripwire`. (default)
11. **One-time audit** of the 52 differing and 4 missing `1.x` requirements: no behaviour lost; 4 spec-only losses. (user)
12. **Restore the four spec-only losses** — MODIFIED deltas putting each lost clause and scenario back into `main`'s current wording, keeping `main`'s extensions — and port the dropped `1.x` tests: `TestEverySurface` (`tests/test_library_name_shadow_warnings.py`), `test_re_typing_field_effects_converge` and `TestListColumnRetype` (`tests/test_drift_list_retype.py`), `test_a_rename_converges`. (user)
13. **`models/schema-drift`'s re-type sentence says "only those a list column allows"** instead of `main`'s "only `count`", which contradicts `models/list-columns` (a list allows `count` and `count_distinct`) — fixed while that requirement is modified. (default)
14. **A `v1_8_2` stored-store corpus, kept small**: list-column surface only, and only queries whose semantics 2.0 did not deliberately change, so `expected.json` holds 1.8.2's rows and the test asserts equality throughout. (user: "A; keep it small")
15. **Corpus formulas use the spellings 1.8.2 accepted**, including a double-quoted string inside a list function, so the corpus exercises the 15→16 Mode-B string migration. (default)
16. **Standalone and edit-path coverage for item 1** (Codex finding 4): tests for a standalone `Column(type=ARRAY)` (accepted), a table-backed model (refused), MCP `create_model` / `edit_model` (sql-mode canonicalised, table-backed refused) and a storage round trip keeping `UNKNOWN`. Every current ingestion `model_copy` path sets `element_type`. (codex)
17. **`docs/concepts/models.md:128`'s "On a list column it may name only `count`"** is fixed (`count` and `count_distinct`) in the same edit as the primary-key list. (default)
18. **No migration-guide entry** — 1.8.x behaviour is kept. (default)

## Risks / Trade-offs

- [A ported `1.x` test fails on `main`] → it is a real loss; stop and raise it with the user, never edit the test to pass.
- [1.8.2's writer cannot produce a listed corpus shape] → drop that shape from the generator with a note in tasks.md and raise it at pr-implement; never hand-forge a document as 1.8.2's.
- [The 1.8.2 generator needs PyPI and `uv`] → runs outside CI, like the 0.10.2 one; the committed corpus is what CI reads.
- [`fetch-depth: 0` slows the lint job] → one job; the history fetch is small next to the dependency install.
- [The delta guard misses a branch merge that overwrites `main` without moving its merge base with `1.x`, as #509 did] → accepted (decision 7); the one-time audit covers the past.

## Migration Plan

No stored-document version bump. A 1.8.x store loads without rewriting; a re-save writes `element_type: UNKNOWN` on sql-mode list columns that lacked it. Rollback: a store saved by this version still loads on 1.8.x (`element_type: UNKNOWN` is a valid 1.8.x value).
