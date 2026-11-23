## Context

See proposal.md (Why). No dataset-side model sets `validate_assignment`. Probes on `main` (2026-11-22, forcing it on every model in `slayer/core/models.py`, then on `NumberFormat`, `ExpressionObject`, `DialectEntry` too):

- **v1** (plain switch): 234 failed / 53,643 passed in 2h15m; the issue's "hang" is deep `RecursionError` whose broken traceback crashed pytest's formatter.
- **v2/v3** (writes made inside a model's own validators bypass validation; failing and >50 ms assignments logged): 4 failed in 14–20 min, no slow assignment. So nearly all failures are one class.

Failure classes:

- **A — validators write through assignment** (recursion): `DatasetField._sql_shorthand` (`self.expression, self.sql = …`), `NumberFormat`'s after-validator (`self.symbol = "$"`), `SlayerDataset._validate_list_element_types` (writes the child's `element_type`).
- **B — linked attributes edited one by one**, the half-edited object failing a cross-attribute check: `slayer/mcp/server.py:1106-1124` (`edit_dataset` source switch: `sql_table` / `sql` / `source_queries`, plus `fields = []`), the deprecated `join_pairs` setter (`from_columns` then `to_columns`).
- **C — tests assigning genuinely invalid values**: `tests/test_ossie_vocabulary_python_names.py:486` (`dataset.columns` case) and `:538` drop a field a relationship keys on.

Pydantic 2.13 on assignment (probed): a model wrap validator receives the instance (so the dict-only input conversion in `_part_input` and `SlayerDataset._name_nested_migrations` passes through), a before validator receives a fresh dict of the fields with the new value, after validators re-run; a failed after validator leaves the value assigned.

**Symptom → structural cause.** Construction is the only checkpoint; every later write is unchecked. Fix by construction, not per site: every dataset-side model validates writes (law test + principle), validators never write through assignment, linked attributes move through one atomic `assign`, and in-place container edits are caught at the one persistence boundary (system §3.11). Making them impossible in memory is DEV-2401.

**Applicable principles.** `python.core` §3.5 (assignment errors are construction's errors), §3.7 (the change stays in `core` and `storage`; the new module is part of node `core`, no new arrow), the new principle (decision 2); system §3.6, §3.11.

## Goals / Non-Goals

**Goals:**
- Every model reachable from `SlayerDataset` / `DatasourceConfig` is frozen or validates assignment atomically; a law test fails when a reachable model escapes.
- No validator writes through validated assignment.
- Every production edit of linked attributes goes through `assign`.
- The three save paths re-validate before any work.

**Non-Goals:**
- Validating `model_copy(update=…)` — internal copies are code, not input (DEV-1695 decision 1).
- Refusing in-place container edits in memory — DEV-2401.
- Query-side objects — DEV-1695.
- A 1.x backport.

## Decisions

1. **Scope of "can't bypass": assignment validation plus full re-validation on save (user).** Options: assignment only; plus save re-validation; immutable containers. Taken: the second; immutable containers split out as DEV-2401 (filed with its worktree on the user's instruction).
2. **arc42 principle (user, approved wording).** `architecture/python.core.arc42.md` §3 gains, numbered next after `main`'s highest at merge time (8 if this lands before DEV-1695, else 9):
   "8. **Every write is checked**: an attribute assigned on a dataset or query object passes the checks construction applies, and a refused assignment leaves the object unchanged. [enforced: test:tests/test_law_dataset_objects_validate_assignment.py] [target: DEV-1695]"
   Rejected: law test only (DEV-1695's choice for its assignment half) — the rule would live only in a test.
3. **Save-time re-validation covers datasources too (user).** Options: datasets only; datasets and datasources. Taken: both — one rule at one boundary.
4. **Atomic writes by isolated candidate (codex, user-approved).** `AtomicAssignment` in new `slayer/core/assignment.py`: a validated assignment, and `assign(**changes)`, run on a candidate — a shallow copy of the object with its list and dict containers copied — and commit the candidate's `__dict__`, fields-set and private state to the object only on success. `assign` writes every change to the candidate, then validates each changed attribute once against that final state. Rejected: DEV-1695 decision 10's snapshot/restore — a shallow snapshot shares lists that after validators edit in place (`_qualify_sql_sibling_aliases` replaces `self.fields` items), so restore is not exact. Built here, not after DEV-1695, so a 2.0 blocker does not wait on it; DEV-1695 reuses it (comment on DEV-1695). (default + codex)
5. **Validators are pure checks; normalizations happen on input (default).** `DatasetField`'s sql shorthand moves into its wrap validator: dict input only is normalized, then `_from_sql` is set on the returned instance (instance input — a prebuilt field or an assignment — passes through, so `field.sql = "amount"` after construction stays plain `sql`, as today) (codex). `NumberFormat`'s symbol default moves to a before validator. The dataset's list element-type default replaces the list item with a copy, like `_qualify_sql_sibling_aliases`, instead of writing the caller's child. Rejected: writing `self.__dict__` inside after validators — it hides the write from the checks.
6. **Linked attributes change through `assign` (default).** `edit_dataset` switches source in one call carrying all four attributes it changes (`sql_table`, `sql`, `source_queries`, and `fields=[]` on the switch to `source_queries`) (codex); the `join_pairs` setter assigns both key lists in one call. A task audits production code for other consecutive writes to attributes a cross-attribute check reads.
7. **Save-time check = rebuild from raw field values (codex).** A recursive extractor turns the object and every nested model into plain field values, runtime-only (`exclude=True`) fields included — `model_dump` drops them, and `model_validate` over nested instances does not re-check them under `revalidate_instances="never"`. The result is validated in stored-document mode (no deprecation warnings, as at load) and thrown away. It runs first in `save_dataset` (when `_validate`), `save_dataset_extensions` (codex) and `save_datasource`, before canonicalization, dialect conversion, peer loading or the backend write (codex).
8. **Model scope (default).** Everything reachable from `SlayerDataset` and `DatasourceConfig` annotations, stopping at query-side models (DEV-1695's law): the `models.py` dataset models plus `NumberFormat`, `ExpressionObject`, `DialectEntry`; frozen `CustomGranularity`, `RelationshipOrigin`, `EmptyList` qualify as they are. Mirrors DEV-1695 decision 12.
9. **Law test shape (default).** `tests/test_law_dataset_objects_validate_assignment.py` walks the reachable models; per model: frozen, or `validate_assignment` plus the mixin; from a minimal valid payload, a wrong-typed assignment raises and leaves the object equal to before. A minimal-payload table fails when a reachable model has no entry. A test-only model whose early validator edits a list and a later validator refuses pins the candidate commit (codex).
10. **Errors (default).** A refused assignment raises Pydantic's `ValidationError`, as construction does; typed errors raised by validators surface as at construction.
11. **Spec placement (default).** `compatibility/ossie-vocabulary` (beside "Datasets refuse unknown keys" and DEV-1695's "Query objects validate assignments") and `models/save-validation`; no new capability.
12. **Codex findings folded (codex, user-approved):** candidate commit (decision 4); `save_dataset_extensions` (7); raw-value rebuild incl. a child's in-place list edit (7, spec); `_from_sql` on dict input only, with dict / prebuilt / post-construction tests (5); `edit_dataset` four-attribute switch with rollback test (6); check before any work and no backend write on refusal, datasource included (7, spec).

## Risks / Trade-offs

- [Callers assigning invalid intermediate states raise] → intended; `assign` is the migration path, called out in `docs/upgrading/2.0.md`.
- [Mutation sites the suite does not exercise] → the audit task (decision 6); each fixed site gets a test.
- [Candidate copy per assignment] → shallow copy of a few containers; the probes found no assignment over 50 ms, and production builds rather than mutates.
- [The save check refuses a dataset saved today] → only a dataset its own construction would refuse; load already refuses it, so it could never be read back.
- [DEV-1695 lands its own mixin first] → whichever merges second keeps one implementation, the candidate-commit form.
- [Existing tests pin invalid assignments] → the two class-C tests, aligned with the user's approval in pr-tests.
