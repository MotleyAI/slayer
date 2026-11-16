## Context

See proposal.md for the symptom. On `1.x`, `_association_arm` in `slayer/engine/compile/stages.py` takes the entity key from `unique_key_sets(root)[0]` and refuses a home with no key via `check_association_root_unique_key`. `main` replaced the refusal with a per-query row identity (DEV-2216 commits `cc19808d`, `09d0e80f`, `a649ad5d`, `10234016`) and #598 narrows the entity key to the primary key. The generator's hook points on `1.x` (`_producer_render_bundle`, the relation door ending in `_stamp_model`) match `main` before DEV-2216, and every import edge the change needs already exists in the model.

**Structural cause.** "Unique" was read as "identifies every row", which a nullable SQL `UNIQUE` column does not. One rule — entity key = primary key, else per-query row identity — gives every home a row-identifying key, so the NULL-loss class and the no-key refusal both disappear (`semantics.arc42.md` Axiom 8: associate aggregates over the distinct associated home rows).

## Goals / Non-Goals

**Goals:**
- Back-port the row identity and #598's primary-key-only entity key to `1.x`, aligned with `main`'s code, names and file layout.

**Non-Goals:**
- Associate-by-default, the broadcast fallback, error-mode-per-stage and stored-mode migration from DEV-2216.
- A cheaper standalone key when a unique column is provably non-NULL (models carry no nullability).

## Decisions

1. Back-port the per-query row identity rather than refusing homes without a primary key: refusing turns queries that are correct today into errors in a patch release. (default)
2. Homes with no key at all, refused on `1.x` today, associate through the row identity; restricting numbering to unique-only homes would be an arbitrary special case, and `main` does the same. (default)
3. The entity key is the primary key only; `unique` keeps proving join cardinality, which stays sound because NULL never matches in a join. (default, #598)
4. A declared primary key is trusted non-NULL. (default, #598)
5. The row identity is minted over the home's filtered rows before any join, as `__slayer_row_id` in the reserved `__slayer_` namespace (verified reserved on `1.x`). (default, `main`)
6. `SqlDialect.row_number()` with a `row_number_needs_order` flag (`ORDER BY NULL` on Snowflake, Oracle, Databricks, Spark) and a T-SQL override ordering by `(SELECT NULL)`. (default, `main`)
7. Parameter-determination checks read the numbered home (#598's `determines_param`; a local home reads the numbered root), so a weighted average uses each row's own weight. (default, #598)
8. `queries/list-children` is updated too: its "unique key" sentence would otherwise contradict the new rule (#598 leaves it stale on `main`). (default)
9. The eligibility requirement is REMOVED and re-ADDED under `main`'s name ("Association eligibility, input handling and row identity"), and the row-identity requirement takes `main`'s name: a MODIFIED delta cannot retitle the pinned refusal scenario. (default, `main`)
10. Test and fixture files follow `main`'s names (`tests/_dev2216_fixtures.py`, `tests/test_dev2216_keyless_association.py`); explicit `associate` only, since `1.x` defaults to `broadcast`. (default)
11. Existing tests pinning the refusal are aligned as on `main`: `test_dev1841_association_errors.py::test_root_without_unique_key_fails` asserts association; `test_dev1894_query_type_errors.py`'s "association" case triggers on `window=`; `_dev1871_raise_ledger.py` drops the `check_association_root_unique_key` row. (user)
12. Ship as 1.8.5 from a PR into `1.x`. (default)
13. Back-port the per-query row identity (option A of the issue). (user)
14. Port #598's regression matrix explicitly: nullable vs all-non-NULL × local vs cross-model × `sum`/`count`/`weighted_avg` on SQLite and DuckDB, asserting each row's own weight. (codex)
15. Cover a home with a primary key and a nullable unique column: not numbered, keyed on the PK, NULL-unique rows kept. (codex)
16. Rejected (codex): "the list-children wording contradicts key-free list-only association" — the requirement's first sentence keeps the list-only exception; only the other-hop sentence changes.

## Risks / Trade-offs

- [An associated home with only standalone unique columns now pays a row-number window] → accepted, as on `main` (#598 trade-off).
- [#598 is still open and may change in review] → `pr-implement` re-reads #598's final diff before implementing and follows it.
- [SQL goldens covering association on a unique-only home change] → re-bless with the diff shown, per the approved plan.
