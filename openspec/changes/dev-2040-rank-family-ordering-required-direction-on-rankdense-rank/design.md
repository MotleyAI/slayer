## Context

- Migrations are dict→dict steps run by each persisted class's `mode="before"` validator
  (`slayer/storage/migrations.py`, `migrate()`), so they run on every `model_validate`.
  `migrate()` treats a missing `version` as v1. Fresh agent and API payloads carry no
  `version`, so today every registry step also runs on fresh input.
- Stored `SlayerModel` documents load through `StorageBackend._migrate_and_refine_on_load`
  (`slayer/storage/base.py`), which writes a migrated document back. Memories load
  directly through `Memory.model_validate` in `yaml_storage._md_to_memory` and the
  `sqlite_storage` row readers. Stored queries live nested in `source_queries`, in an
  inline-query `source_model`, and in `Memory.query`. Each carries its own `version`.
- Two parsers read transform kwargs: `core/formula.py` `_parse_transform_kwargs`
  (importer validation only; its retirement is DEV-1831) and `engine/binding.py`
  `_bind_transform_params`.
- The window is emitted in `sql/generator.py` (`rank_order`, `_over`). The NULL position
  comes from `build_ordered`'s dialect default, which on T-SQL is NULLs first on `ASC`.
- `StorageBackend.save_model` does not parse measure formulas (DEV-2043 tracks
  save-time validation).
- Normative: `architecture/system.arc42.md` §3.11 (versioned persistence, migrations run
  on load), §3.10 (two expression layers: only Mode-B text is DSL), §3.2 (`core`
  imports no other node), §3.6 (SQL built as sqlglot AST); `storage.arc42.md` §3.1.

## Goals / Non-Goals

**Goals:**
- One `direction` rule that both parsers obey, so they cannot drift.
- Lazy migration that fills in only legacy documents and can never fill in a fresh
  payload.
- NULL semantics that do not depend on the dialect.

**Non-Goals:**
- Merging the two parsers (DEV-1831).
- Save-time formula validation (DEV-2043).
- Rewriting stored references to old auto-named rank result keys.
- Migrating `ntile` / `percent_rank` meaning, which no rewrite preserves exactly.

## Decisions

1. **Stored-only migration steps.**
   - What: `register_migration(entity, source_version, stored_only=True)`. In `migrate()`,
     note once whether the incoming dict carried an explicit `version`. A stored-only step
     runs only if it did. Otherwise the version still advances past it.
   - Why: a missing `version` cannot tell a v1 document from a fresh payload. An explicit
     `version` can, because every persisted document stamps one. It is the generalised form
     of the `strict` retirement precedent in `SlayerQuery._migrate_and_rewrite`.
   - Alternative rejected: rewriting only in the storage load path (the v10/v11
     precedent). It needs a hand-rolled walk of nested queries and two memory hooks, and
     any future direct `model_validate(stored_dict)` would silently miss it.
2. **Stamping legacy documents.**
   - `_migrate_and_refine_on_load` and the two memory load sites set `version: 1` on a
     stored dict that has none.
   - The `SlayerModel` v12→v13 step and the `Memory` v2→v3 step (both stored-only) stamp
     `version: 1` on each unversioned nested query dict before it validates: every
     `source_queries` entry, recursively through inline-query `source_model`s, and
     `Memory.query`.
   - The `SlayerQuery` v4→v5 step stamps its own unversioned inline `source_model` query
     the same way.
   - A payload that declares an explicit old version is legacy by its own declaration
     and is filled in (Codex review finding 1, rejected with this rationale).
3. **The rewrite is token-level, not AST.**
   - One stdlib-`tokenize` function in `storage` inserts `, direction='desc'` before the
     matching `)` of each `rank(` / `dense_rank(` NAME token that is not preceded by `.`
     and has no top-level `direction` keyword.
   - Why not AST: `ast.unparse` would drop colon syntax and the user's formatting.
   - Strings and comments are skipped by construction.
   - A `tokenize.TokenError` leaves the text byte-identical. Any formula the DSL accepts
     tokenises, so such a formula is already broken and still fails loudly when queried.
     Failing the migration instead would make the model unloadable for repair (Codex
     finding 6, rejected).
   - Applied to: `measures[].formula` for models; for queries, string or dict
     `measures` (`formula`), `filters`, `dimensions` (string or `expression`),
     `time_dimensions` (string or `dimension`), `order[].column`, `main_time_dimension`.
4. **One direction rule in core.**
   - A core function validates a rank-family call's `direction`: required for
     `rank` / `dense_rank`, forbidden for `ntile` / `percent_rank`, string-literal only.
     It normalises through the shared synonym table, which moves out of `core/query.py`
     and which `OrderItem` keeps using. It raises `TransformArgumentError`.
   - Both parsers call it. The binder stores `("direction", "asc"|"desc")` in
     `TransformKey.kwargs`, so asc and desc intern separately.
   - The binder's other transform-kwarg `ValueError`s move onto `TransformArgumentError`,
     which is backwards compatible since `QueryTypeError` subclasses `ValueError`.
5. **NULL→NULL emission.**
   - Shape:
     `CASE WHEN v IS NULL THEN NULL ELSE fn() OVER (PARTITION BY <pks>, CASE WHEN v IS NULL THEN 1 ELSE 0 END ORDER BY v <dir>) END`,
     built as a sqlglot AST.
   - The integer flag (not a boolean predicate) keeps `PARTITION BY` valid on T-SQL.
   - The NULL rows sit in their own window, so the NULL ordering inside the window no
     longer matters on any dialect.
   - The same shape is used for all four functions, even though `rank` / `dense_rank`
     alone would only need NULLs-last ordering.
6. **Naming.** The canonical formula text renders `direction` as its bare value, so the
   sanitiser yields `rank_a_sum_desc`. Other kwargs keep `name_value`.

## Risks / Trade-offs

- [Unnamed rank keys change; downstream references to old keys break] → Accepted by the
  user; release notes list it.
- [`ntile` / `percent_rank` results flip, and NULL inputs now yield NULL, with no
  migration] → Release notes. `rank(...) <= N` filters now drop NULL-inner rows.
- [Explicit-version payloads from API clients are treated as legacy] → Intended; covered
  by a scenario.
- [The CASE wrapper adds noise to golden SQL for non-nullable inners such as `count(*)`]
  → Accepted for one uniform shape; goldens are re-blessed.
- [Very old unversioned queries reachable through some path not covered by stamping]
  → They fail loudly with the typed error that names both spellings.

## Migration Plan

- `CURRENT_VERSIONS` goes to `SlayerModel` 13, `SlayerQuery` 5, `Memory` 3. All three new
  steps are stored-only.
- Model documents are written back on first load. Memories and nested queries are
  persisted with `direction` the next time they are saved.
- Rollback: older code reading a v13 / v5 document best-effort-loads it, but its parser
  rejects the unknown `direction` keyword. Downgrading after migration therefore needs the
  documents restored from backup.
