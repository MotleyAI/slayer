# Design

## Context

See proposal.md — Why. Census of today's `join_pairs` consumers (verified 2026-09-23):

- Already logical (compare against `Column.name`): `schema_drift._diff_sql_table_joins` (resolves `join_pairs[*][0]` through `Column.sql` with a local map), `osi/converter._missing_join_columns`, the pg facade (`translator._canonical_column_name` → literal compare in `_classify_against_parent_joins`), `join_safety.shared_join_key_reroot` (`src == key.leaf`), the back-join keys in `compile/stages.py` (`ColumnKey(leaf=src)`), ingestion (`Column(name=column_name, sql=col.name)`, only the `_count → count_col` rename, not applied to `join_pairs`).
- Physical (emit raw): `generator._build_from_and_joins` (ON clause), `generator._SemiJoinOps` / `_spine_projection` (semi-join spine), `query_engine._detect_one_join` (profiling SQL), `cube/converter._resolve_join_pairs`.
- Maybe-logical: `join_safety.provably_to_one` (lookup with physical fallback), `_hop_pins` via `_physical_grain_leaves`.
- Latent bug of the class: `compile/stages._association_arm` builds `entity_keys_root = [ColumnKey(leaf=col) for col in _unique_key_sets(root)[0]]` and `_check_attached_params_determined` its grain display from PHYSICAL key sets, so a renamed PK root breaks in associate mode.
- Five copies of the "bare identifier" notion: `join_safety._BARE_IDENT_RE`, `reference_closure._BARE_IDENT_RE` (default-expression parsing, out of scope), `schema_drift._column_is_base`, `type_refinement._column_is_base` (documented as a mirror), `cube/refs.is_bare_identifier`; `sql/column_expansion.is_trivial_base` is the different "self-name" notion.
- No golden fixture uses a renamed key column; existing goldens are expected byte-identical.
- Post-construction join mutation sites: `osi/converter._add_relationship` (append), MCP `edit_model` (rebuilds via `model_validate(model_dump())`), Cube/MCP/drift (removal only), `ir/source_bundle.apply_extension_overlay` (`model_copy(update=...)`, no validators).
- Load pipeline (`storage/base._migrate_and_refine_on_load`): per-model `migrations.migrate` → `_rewrite_legacy_join_aliases` (cross-model raw) → `_dedup_exact_inverse_joins` (cross-model raw via `_load_raw_model_dict`) → refinement → `model_validate` → write-back when a migration ran. `_is_exact_inverse_join` compares pair spellings literally.

Constraints: system P9 (canonical references), the new P16 (below); core P4 (typed errors, stable text); engine P8 (models persist verbatim — validation rejects, never rewrites at save) and P11/system P11 (versioned persistence, migrations on load); sql P9 (fail closed); semantics Axiom 1 (determination judged on provable to-one hops — the proof must see the same spelling on both sides).

## Goals / Non-Goals

**Goals:**
- One spelling in `join_pairs`; one definition of the physical spelling; join-graph reasoning with no normalisation map anywhere.
- The class closed at the type level where a validator can see the data, and fail-closed everywhere else.

**Non-Goals:**
- Joining on derived or filtered columns (rejected, not rendered).
- DEV-1954's path-token spelling (edge name vs model name) — a different layer.
- Ingestion's DB-side `_get_unique_key_sets` (physical by nature) and `reference_closure._BARE_IDENT_RE` (parses default-expression text).
- Consolidating `is_trivial_base` (self-name identity, a different predicate).

## Decisions

1. **Logical pin, strict (interview Q1, option A).** `join_pairs` = `Column.name`. `ModelJoin`'s field validator applies `_validate_column_name`. A module-level `_check_join_keys(*, model_name, columns, joins)` in `core/models.py` (next to `_check_column_measure_namespace`) enforces the source side: declared, `is_base`, `filter is None`; called by the `SlayerModel` after-validator and by `apply_extension_overlay` (Codex finding 2 — `model_copy` runs no validators). Target side: `query_engine._validate_join_keys` at save (peers via `_preload_join_targets`; an unloadable peer is skipped, as today) raising `JoinKeyError`; `join_safety.audit_join_safety` emits an error-severity `JoinSafetyFinding` for a key that is not a declared base column on its side, so `validate_models` reports it on old stores. Alternative (save-time only, option B) rejected: leaves the class open in memory.
2. **Base columns only (Q2, option 1).** `is_base_column_sql(sql)`: `None`, or a bare identifier (`core.refs.IDENTIFIER_RE`), or a double-quoted bare identifier; `physical_column_sql(sql, name)` returns the unquoted identifier else `name`. `Column.is_base` / `Column.physical_name` wrap them; `physical_name` raises `JoinKeyError` on a non-base column. A join key must also have `filter is None` (a value-masked key has no coherent ON meaning). Rendering an expression key rejected: it would need the Mode-A door inside an ON clause and cannot be profiled.
3. **Seam in `core` (Q3a).** `physical_join_pairs(*, edge: OrientedLike, source: SlayerModel, target: SlayerModel) -> list[tuple[str, str]]` in `core/join_walker.py`, raising `JoinKeyError` when an entry is not a declared base column on its side. Every node may import `core`; a helper under `sql/render/` would need a new `engine.compile -> sql.render` arrow (none exists), and `sql/naming.py` is scoped to aliases (sql P3).
4. **IR carries physical pairs (Q3b, option i).** `compile/stages._forward_hops` / `_reverse_hops` call the seam when registering `SemiJoinHop`s (both have the source and target models in hand); `SemiJoinHop.join_pairs` is documented PHYSICAL. The generator's base-join path (`_build_from_and_joins`, which re-walks `OrientedJoin`s) calls the seam with `source_model` for hop 0 and the previous hop's model after; `_SemiJoinOps` and `_spine_projection` stay raw. Quoting stays with `_to_ident` (mixed-case → quoted); profiling keeps `exp.column(c, quoted=True)` (always quoted). Alternative (IR logical, resolve parent models in the renderer) rejected: threads model lookups through the spine builder for no semantic gain.
5. **join_safety in logical space.** `_unique_key_sets` returns `Column.name`; `provably_to_one` passes `pair[1]` straight to `is_key_set_unique`; `_entity_seeded` compares grain leaves to key sets directly; `_hop_pins` compares `join_pairs` sources to grain leaves directly; `_physical_name`, `_physical_grain_leaves`, `_BARE_IDENT_RE` deleted. `compile/stages.py`'s entity keys and grain display become correct by construction. `_detect_one_join` maps each key through `Column.physical_name`; `_detection_skip_reason` keeps a non-base skip via `Column.is_base` (reachable only for a target-side key on an old store).
6. **Drift and refinement (D7 + Codex finding 4).** `schema_drift._column_is_base` and `type_refinement._column_is_base` delegate to `is_base_column_sql`; drift's `base_sql_by_name` map, its `(col.sql or col.name).strip()` lookups (two sites) and type refinement's raw `col.get("sql") or col.get("name")` lookups (two sites) go through `physical_column_sql`, so a double-quoted base column is checked against the live column it names instead of being skipped as derived.
7. **Persistence (D8 + Codex finding 1).** `CURRENT_VERSIONS["SlayerModel"] = 11`, a registered no-op v10→v11 converter (the rewrite needs peers). `canonical_join_pairs(pairs, *, source_columns, target_columns)` is a pure function over raw column dicts: an entry naming a declared column is kept; an entry equal to exactly one declared base column's unquoted `sql` whose `name` differs is rewritten; else verbatim. `_canonicalize_join_key_spellings` applies it to this document (source = own columns, target = the peer's raw columns via `_load_raw_model_dict`) BEFORE `_dedup_exact_inverse_joins`; `_stored_counterpart` applies it to the peer's joins (source = peer columns, target = this document's columns) before `_is_exact_inverse_join`, so a mirror pair spelled two ways collapses. Write-back unchanged. `docs/concepts/models.md` "currently 9" → 11.
8. **Importers (Q4 + Codex finding 3).** Cube: `_resolve_join_pairs` returns member names and `None` (drop + report warning) when an operand names a non-base member or no member. dbt: `resolve_joins_for_model` / the converter synthesise `Column(name=expr or name, hidden=True, type=DOUBLE)` for a foreign entity no dimension covers; a non-bare `expr` skips the join with a warning. OSI: `_add_relationship` runs `_check_join_keys` for both sides (it has both models) and skips with the existing warning on violation. Ingestion: `_logical_column_name(live_name)` shared by column building and `_generate_joins`; the dotted-`join_pairs` branch in `_introspect_columns` (no producer anywhere; unreachable under decision 1) deleted.
9. **Dead code.** `ir/planned.JoinRequirement` (no constructor in `slayer/`) and `tests/test_planned.py::TestJoinRequirement` deleted (consent given 2026-09-23).
10. **arc42 (Q5).** Approved 2026-09-23, to land verbatim in `architecture/system.arc42.md` §3 as item 16, in the same commit as its enforcing test:

    ```
    16. **Logical names, physical once**: every model-level column reference —
        query fields, `join_pairs`, unique-key sets — names a column by
        `Column.name`; the physical spelling (`Column.physical_name`: a base
        column's bare `sql`, else its name) is applied once, where a reference
        becomes SQL or meets the live schema, and is never the key two references
        are matched on.
        [enforced: test:tests/test_dev1902_join_key_spelling.py]
    ```

11. **Error class.** `JoinKeyError(SlayerError, ValueError)` in `core/errors.py` with the `_format_error_message` shape: first line `JoinKeyError: join <model> → <target> key '<column>' <reason>`, then the remedy line. Construction-time source-side failures are `ValueError`s from the validator (Pydantic wraps them), with the same text.
12. **Spec placement.** New capability `models/join-keys` (the `models` cross-cutting spec mapping in `architecture/index.yaml` already covers it); no MODIFIED deltas.

## Risks / Trade-offs

- [A stored model with an undeclared or expression key column stops loading] → the error names the column and the remedy; sibling models load (the bundle builder skips broken peers); the drift cascade already treated an undeclared local FK as join-invalidating.
- [Semi-join spine alias derives from the parent column name] → with physical pairs the alias uses the physical spelling; only renamed keys differ, no existing golden uses one; the new golden module pins the shape.
- [Type refinement starts refining double-quoted base columns] → correct behaviour (they name a live column); pinned by a scenario.
- [Case folding cannot be caught on SQLite/DuckDB] → golden (postgres dialect) pins `_to_ident` quoting; a unit test pins `_side_stats_sql`.
- [Migration touches every v10 document once] → same write-back path as v9→v10; canonicalisation is idempotent and a no-op for `name == sql` stores (every auto-ingested model).

## Migration Plan

Load-time, automatic (decision 7): v10 documents are canonicalised and written back at v11 on first load; no manual step. Rollback = revert; a v11 document loads under v10 code unchanged (Pydantic ignores the forward version, entries are already logical).
