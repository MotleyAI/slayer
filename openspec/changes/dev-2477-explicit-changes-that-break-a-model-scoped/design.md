## Context

See proposal.md — Why. Today three call sites judge model-scoped metric dependents, each with its own rule: the delete warning in `StorageBackend._delete_document_dataset`, `warn_broken_model_metrics` (only when the save reports and validates), and `_admit_metrics` at load (missing datasets only). DEV-2474 added a fourth mechanism for ontology mappings: `_guard_mappings` builds a `MappingScope` before a change, maps it to its state after, and refuses (or, under `database_driven()`, warns about) every mapping that gains a problem; `AccessFilteredStorage._unbroken` turns a break of hidden content into a hidden-content conflict. `MappingScope` (`slayer/ontology/scope.py`) already holds one datasource's documents with their datasets.

Applicable principles: system §3.5 (async storage), §3.6 (Pydantic, never dataclasses), §3.11 (fail at save time: a change that would leave a stored definition failing every query is refused; the query-time and load-time checks stay as fail-closed backstops); storage §3.2 (access is a view: a refusal touching hidden content is translated in the access-filtered view, and code downstream takes no caller context), §3.3.1 (a load never writes: the unloadable disposition is computed on load, never persisted). No arc42 or LikeC4 edit is needed: the shared scope moves into `slayer.core`, and `ontology → core` and `storage → core` already exist.

**Symptom → structural cause.** The symptom is a dataset delete that only warns about the metric it breaks. The cause is that cross-document dependents are judged ad hoc at each write site, with differing rules and one mechanism per dependent kind. The fix makes the class impossible: one before/after datasource state, one guard run on every durable dataset and document write, one refusal error and one warning for every dependent kind, and one reference resolver shared by the guard, the database-driven warning and load.

## Goals / Non-Goals

**Goals:** the deltas under `specs/`, through one guard that DEV-2491 extends with peer dataset definitions.

**Non-Goals:**
- Peer dataset definitions broken by a save (derived fields, filters, relationship paths of another dataset): DEV-2491.
- Judging model-scoped metrics newly added by an imported or whole-text document save: such a document is never judged by the authoring rules; a metric absent before a change cannot be broken by it.
- Relationship removal: a model-scoped metric's references do not include relationships.
- Making guard and write one atomic step across concurrent writers (Codex finding 2/3, rejected; see Risks).

## Decisions

1. **Sequence after DEV-2474 and generalise its guard**; no second marker, guard or error. **(user)**
2. **A metric left unresolved is unloadable** — dropped from the index and recorded as a metric load failure with the resolver's reasons, transitively — rather than loading with a load warning. Options: (a) load unevaluable with a warning (today's dataset-delete outcome; keeps it in `inspect`); (b) unloadable. Taken: (b), one meaning for "references what is not there", matching quarantined-dataset readers and DEV-2474's unresolved mappings, and the issue's wording. **(user)**
3. **Existing tests aligned**: the six document-edit tests that delete a dataset `revenue_per_customer` reads first remove that metric (`test_deleting_a_document_dataset_drops_inbound_relationships` also drops its `pytest.warns`); `test_renaming_a_referenced_field_warns_about_dependents` becomes a refusal test (`…_is_refused_naming_dependents`); `test_dev2474_storage.py` follows the renamed error and warning mechanically. **(user)**
4. **Peer dataset definitions are DEV-2491**, filed with its own worktree, stacked on this branch. **(user)**
5. **One state, one guard, one resolver, one error, one warning, one marker.**
   - `MappingScope` becomes `DatasourceScope` in `slayer.core` (next to `ModelScopedMetric`), each document holding its datasets and its model-scoped metrics; `with_dataset`, `with_document` (now with metrics), `with_ontology` stay.
   - `_guard_mappings` becomes `_guard_dependents`: one before-state, one after-transform, then the mapping check and the metric check over the pair.
   - The generic parts of `slayer/storage/ontology_documents.py` (`database_driven()`, reporting) move to `slayer/storage/dependents.py`; `OntologyMappingBreakError` → `StoredDependentBreakError`, `BrokenMapping` → `BrokenDependent` (kind `mapping` or `metric`), `OntologyMappingUnresolvedWarning` → `StoredDependentUnresolvedWarning`.
   - One resolver in `slayer/storage/model_metric_rules.py` returns a metric's problems over a scope, keyed by reference (never raising); the guard, `_admit_metrics` and the warning use it, and `check_model_metric` builds its typed refusals on it.
   Renames and moves go through `la:deterministic-refactor`. **(default)**
6. **"Breaks" = a new problem**, compared per reference by metric name, as DEV-2474 compares mapping problems; a reference already unresolved never blocks, but a further break of a partly unresolved metric is refused. Transitive readers are broken with their chain. **(default; codex)**
7. **Drift cleanup is database-driven** (DEV-2474 precedent: `apply_drift_deletes`, `apply_untyped_findings` and ingest already run inside `database_driven()`); the database already broke the metric. **(default)**
8. **No compatibility shims for the renames**: DEV-2474 is unreleased (`v2.0.0` predates it) and its names are used only inside storage. **(default)**
9. **A database-driven write moves every affected core metric into `SLAYER`** — the transitive closure of the broken metrics, generalising today's dataset-delete move to field and dataset-metric removal — so the stored document stays Ossie-valid and a database-driven write always applies (without it, a cleanup dropping a dataset metric a core metric `AGG()`s fails Ossie validity). **(default; codex)**
10. **REST deletes answer a break or hidden-content refusal with 400**, as saves already answer the same error; this also fixes DEV-2474's 500. **(default)**
11. **A refused move writes nothing**: MCP `edit_dataset` with `new_data_source` and the engine's move of a query-backed dataset run the deletion guard before writing the new datasource. **(default)**
12. **Newly added dangling metrics in text saves are not judged** (non-goal above). **(default)**
13. **`save_dataset_extensions` is guarded** like `save_dataset`, over the effective dataset before and after. **(codex)**
14. **The guard does not depend on `_validate`**: it runs on every durable dataset and document write; `database_driven()` alone decides refuse versus warn, and `_validate` stays object validation only. **(codex)**
15. **Dataset-metric references (`AGG(d.m)`) get explicit scenarios** for removal and rename, explicit and database-driven. **(codex)**
16. **Check-then-write race not closed here** — rejected (codex): the guard runs before the document's atomic patch, as DEV-2474's does; a concurrent write adding a reader can slip between. The outcome fails closed (the metric loads as unloadable with a diagnostic, never a wrong result), and closing it needs datasource-wide locking across documents and mappings, beyond this issue. **rejected (codex)**
17. **Datasource moves not made atomic** under concurrency — rejected (codex) for the same reason; the preflight covers the non-concurrent refusal. **rejected (codex)**

## Risks / Trade-offs

- [Concurrent writes slip between guard and write] → Fails closed: the metric is unloadable with a diagnostic naming the reference (decision 16).
- [Refusals make metrics block dataset edits] → The refusal names every metric and reference; removing or editing the metric first is always allowed.
- [A guard on every dataset save adds load cost] → The metric half short-circuits when the datasource holds no model-scoped metrics; the mapping half already short-circuits without mappings.
- [Already-dangling metrics stored by earlier versions turn unloadable] → They failed every query already; load reports them with the missing reference, and re-adding the reference or editing the metric restores them.

## Migration Plan

No stored-format change. Metrics that dangle in existing documents load as unloadable instead of with a warning; nothing is rewritten on load.
