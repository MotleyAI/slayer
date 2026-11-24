## Context

See proposal.md › Why. Current state on the 2.0 line:

- **Moved modules.** About 35 moved modules raise `ImportError` through `raise_moved` (`slayer/core/deprecation.py`). The removal tripwire (`tests/test_law_removal_tripwire.py`) pins exactly one `raise_moved` site per moved module.
- **Name twins.** The Ossie-vocabulary twins (`name_twins` in `slayer/core/models.py`, the `StorageBackend` hook twins in `slayer/storage/base.py`) already keep most 1.x names working, with `DeprecatedInputFutureWarning`.
- **Write-back on load.** `StorageBackend._migrate_and_refine_on_load` / `_migrated_dataset` (`slayer/storage/base.py` ~1678–1765) migrate, repair and convert a stored dataset, then write it back with `save_dataset(..., _validate=False)`. Memories (~2214) and datasources take their own paths. The library-granularity settle (~1450–1510) renames and persists dependents before the datasource. The peer-relationship repair (~1568) saves a peer. `slayer/cli.py` `storage migrate-types` mirrors the dataset path rather than reusing it.
- **Drift predicate.** Drift uses `DatasetField.is_physical_reader` (`is_base and filter is None`, `slayer/core/models.py:489`) in its missing-column loop (`slayer/live_schema/schema_drift.py:462`), its re-type loop (~510) and its cascade (~1208).
- **Storage axiom.** `architecture/python.storage.arc42.md` 3.1, "A load never writes; an upgraded document is persisted only by a save", is a `[target]`.

### The integrator's call set (measured on 2026-11-24)

What the integrator calls, and how each call fares on `main`:

| Area | Call shape the integrator uses | `main` today |
|---|---|---|
| model files | `SlayerModel.model_validate(yaml.safe_load(v15))`, then reads `.columns/.measures/.joins/.get_column()`, `col.type/.sql/.filter/.primary_key/.unique/.allowed_aggregations/.element_type`, `m.formula`, `j.target_model/.join_pairs/.cardinality/.join_type`, `target.awaits_columns`, `model.aggregations`, `default_time_dimension`, `effective_default_time_dimension`, `model_dump(mode="json", exclude_none=True)`; recursive `model_fields` / `model_config["extra"]` walk for unknown keys | works with warnings; `model_fields`, dump shape, `DataType.value` and `col.sql` have 2.0 values (heads-up, not code) |
| versions | `CURRENT_VERSIONS["SlayerModel"]` as the only accepted file version | 28 (the integrator's files are 15): upgrade path |
| formulas | `from slayer.core.formula import parse_formula, AggregatedMeasureRef, ArithmeticField, MixedArithmeticField, TransformField`; `parse_formula(formula, extra_agg_names=frozenset, named_measures={name: formula})`, catching `(ValueError, RecursionError)`; reads `.inner`, `.agg_refs.values()`, `.sub_transforms`, `ref.measure_name`, `ref.aggregation_name` | module raises |
| filters | `from slayer.engine.syntax import AggCall, Arith, ArrayLit, BoolOp, Cmp, DottedRef, Literal, Placeholder, Ref, ScalarCall, StarSource, TransformCall, TupleLit, UnaryOp, parse_filter_expr`; walks `type(node).model_fields` recursively; refuses unknown node classes; reads `Ref.name`, `DottedRef.parts` | module raises; `parse_filter_expr` gone; new node classes appear (`NamedArg` for `partition_by=`) |
| refs | `from slayer.sql.column_expansion import resolve_ref_target`; `resolve_ref_target(qualifiers=tuple, source_model=, models_by_name=dict)`; reads `.list_of`, `.name`, `.measures`, `m.formula` | module raises; keywords renamed |
| helpers | `slayer.core.query.has_variable_syntax(str)`; `slayer.core.models.is_base_column_sql(sql)`, `physical_column_sql(sql=, name=)`, `is_identifier(column=, columns=)`, `join_key_error(model=, target=, key=, side=, columns=)` | first two gone; `is_identifier` keywords renamed; the rest work |
| enums | `DataType.ARRAY/BOOLEAN/DATE/TIMESTAMP/INT/DOUBLE/TEXT`, `TimeGranularity`, `DEFAULT_AGGREGATIONS_BY_TYPE.get(col.type)`, `PRIMARY_KEY_AGGREGATIONS`, `normalize_aggregation_name` | `INT/DOUBLE/TEXT/TIMESTAMP` gone |
| drift | `from slayer.engine.schema_drift import EditModelDelete, LiveTable, WholeModelDelete, compute_datasource_drops, data_type_bucket, diff_sql_table_model, retype_findings`; `diff_sql_table_model(model=, live_table=LiveTable(columns={name: DataType}) or None, available_models_in_ds=set)`; `retype_findings(model=, live_table=)`; `compute_datasource_drops(models=, sql_table_diffs={name: diff}, sql_diffs={})`; reads `drop.model_name`, `isinstance(drop, WholeModelDelete/EditModelDelete)`, `drop.remove.columns/.measures/.aggregations/.join_edges[i].model_dump(mode="json")` (keys `target_model`, `join_pairs`, `name`), `drop.remove_filters`, `drop.reasons[i].target/.reason`, `retype.column/.stored_type/.live_type/.remedy/.remedy_kind` | module raises; keywords and attributes renamed; filtered-field drift bug |
| render | `class X(StorageBackend)` overriding `get_model`, `_list_all_model_identities`, `get_datasource`, `list_datasources`, `get_datasource_priority`, `_save_model_impl`, `_delete_model_row`, `update_column_sampled`, and the datasource / memory / embedding hooks (writes raise); `DatasourceConfig(name=, type=)`; `SlayerQueryEngine(storage, policy=None, clock=callable)`; `engine.execute_sync(query, dry_run=True)` under `warnings.simplefilter("ignore")`; `response.sql`, `response.columns`; `dialect_for_ds_type(t).emit_alias(key)`; `import slayer.sql.dialects`, `slayer.sql.scope` | works (hook twins warn at class definition) |
| queries | `ColumnRef(name=)`; `TimeDimension(dimension=ColumnRef(...), granularity=str)`; `OrderItem(column=ColumnRef(...), direction="asc"/"desc")`; `SlayerQuery(source_model=, measures=[str], dimensions=[ColumnRef], time_dimensions=[...], filters=[str], order=[...], limit=, to_many_handling="associate")`; values query `SlayerQuery(source_model=, distinct_dimension_values=True, dimensions=[...] or time_dimensions=[...])`; catches pydantic `ValidationError` | everything except `TimeDimension`/`OrderItem` with a `ColumnRef` works with warnings; those two fail at construction |

Expected 1.8.4 values for the tests come from running the same calls against `motley-slayer==1.8.4`, e.g. `uv run --no-project --with "motley-slayer[all]==1.8.4" python -I <probe>`, and are written into the tests as literals.

## Goals / Non-Goals

**Goals:**
- One upgrade service, used by ordinary loads, `slayer storage upgrade` and the pure entry point.
- Every kept 1.x form is a thin conversion onto the 2.0 implementation, with no second implementation of any behaviour.

**Non-Goals:**
- 1.x value representations: `DataType` value strings, `Column.sql` for bare identifiers, dump shapes and hashes, class names, new syntax node classes in 2.0 trees, drift message texts, ARRAY `count_distinct`, joined-aggregate association.
- Deprecating the other moved modules.
- Restoring colon spellings.
- The integrator's test-only touchpoints (`slayer.sql.client`, `_column_agg_refusal`).
- Moving YAML's pre-v4 layout move.
- Naming the integrator in the repository.

## Decisions

### Implementation shape
- **Partial moved-module shims.** Each of the four old modules defines a module `__getattr__`:
  - a listed name resolves to the 2.0 object (or its 1.x adapter) and warns through the shared deprecation helper;
  - any other name raises the moved-location `ImportError`;
  - importing the module itself warns nothing.

  The tripwire pins one feature per restored name, plus one moved-module fallback per module.
- **1.x formula adapter.** `slayer.core.formula.parse_formula` calls `slayer.core.syntax.parse_expr` and projects its tree onto 1.x result classes:
  - private pydantic models carrying `inner`, `agg_refs`, `sub_transforms`, `measure_name` and `aggregation_name`;
  - sibling names from `named_measures` are substituted by re-parsing their formulas, recursively, with a cycle raising `ValueError` naming the cycle, as 1.8.4 did;
  - `extra_agg_names` lets a bare custom-aggregation call count as an aggregation.

  `core.syntax` stays the one Mode-B reader (system P8) and the parser stays pure (engine P2).
- **`parse_filter_expr` projection.** It parses with `parse_expr` and projects to the 1.8.4 shape:
  - any node of the five 2.0-only classes is rewritten to the node 1.8.4 built for the same text;
  - text 1.8.4 refused raises `ValueError`.

  Tests replicate the integrator's recursive `model_fields` walk and check per-class field values.
- **DataType legacy names.** The legacy names resolve through a metaclass attribute lookup on `DataType`, not through enum aliases. They are therefore not members, and iteration, `__members__`, validation, schema and pickling are unchanged.
- **Old keywords and result attributes.** The old keywords go through the existing `name_twins` `arguments=` mechanism on the 2.0 drift functions, `is_identifier` and `resolve_ref_target`. Result attributes are deprecated properties on `EditDatasetDelete`, `RemoveSpec` and `RetypeFinding`. `join_edges` returns 1.x-shaped edge models, built from `relationship_edges`, that dump `target_model`, `join_pairs` and `name`.
- **Column references in queries.** `TimeDimension.field` / `OrderItem.field` accept a `QueryDimension` / `ColumnRef` object under the deprecated `dimension=` / `column=` spellings and convert it to its reference text, through the shared deprecated-input marker (core P8).

### The upgrade service
- **One service.** It is extracted from `_migrate_and_refine_on_load`, `_migrated_dataset`, the memory and datasource load paths and the settle. It takes a snapshot context: raw documents, peer documents, datasource configs or types, and an optional live-refinement provider. It returns upgraded documents plus diagnostics, and for each document whether the upgrade changes it.
  - Ordinary loads call it and return the upgraded objects without writing.
  - `slayer storage upgrade` calls it and writes changed documents.
  - The pure entry point calls it over a snapshot built from the documents passed in.
- **Settle.** The rename map is read once per top-level load from the stored datasources, and is a function of them alone. It is applied in memory to every dependent document a load reads. The upgrade command writes dependents before the datasource and leaves a renaming datasource unwritten while a dependent is refused, conflicted or unreadable, so an interrupted run converges on the next one.
- **Upgrade writes.** These go through the migration write path: wire-schema-valid and without client-save semantic validation, as the old write-back did. They are conditional on the stored raw form being unchanged since it was read: bytes for YAML, an in-transaction comparison for SQLite. A changed document is a reported conflict.
- **Load warning.** It is keyed on the service's "changes this document" result, not on the top-level version. It is emitted once per document per storage instance.
- **Entities covered.** Datasets, semantic-model documents, saved queries, memories and datasources. Datasource priority and embeddings are untouched.
- **Drift predicate.** A named "reads a physical column" predicate (`is_base`, filter or not) replaces `is_physical_reader` in every drift and cascade use. `is_physical_reader` stays for relationship keys and live typing, and its docstring drops "drift".
- **Law test.** `tests/test_law_load_never_writes.py` compares logical store contents (YAML document bytes; SQLite rows through a read-only snapshot) and spies the backend write hooks. The open-time layout and schema moves are tested separately as the licensed writes on open.

### Decisions log
1. The compat suite pins the integrator's current calls, kept working by deprecated forms on `main`, not a change-detecting partner contract. Options: 2.0 forms plus a heads-up; 1.8.4 forms plus exceptions; deprecated forms. (user)
2. Warnings accompany deprecations, never breaks. (user)
3. The migration process (`slayer storage upgrade` plus a pure entry point) is folded into this change. (user)
4. Timing: a 2.0-release-slice prerequisite, merged after the other in-flight 2.0 slices, so 2.0.0 is adoptable unchanged and the release's audits cover the new forms. Option rejected: after the release slice. (user, delegated)
5. The surface is the integrator's actual code at its 1.8.4 pin, not the issue's original list. Each form carries a clear warning with an explicit upgrade instruction. This is a one-off. (user)
6. No byte goldens and no "heads-up to the partner" failure message. Rendered SQL is checked by execution (DuckDB, Postgres) and emission (the cloud dialects). (default)
7. Moved modules: only the integrator's four, and only its names. Options: those names only; whole modules; all ~35. (user)
8. Back-port the call tests to 1.x, as a separate tracker issue with its own worktree. (user)
9. The breaking calls all get deprecated forms. (default)
10. Representation changes go in the migration guide, not code: keeping 1.x values would undo 2.0's Ossie-valid types. (default)
11. The integrator's stored v15 files are handled by the upgrade path, after which its version and unknown-key gates pass. (default)
12. Drift misses a filtered base field: fixed in scope. Options: in scope; xfail against an existing static-readers issue; narrow the fixture. (user)
13. The integrator's test-only touchpoints are excluded. Options: exclude both; include `slayer.sql.client`; include both. (user)
14. Spec placement uses existing cross-cutting groups, so `index.yaml` is unchanged. (default)
15. Loads never write, fulfilling storage arc42 3.1 and closing its target issue. Options: fold it in; leave it separate. (user)
16. "Load never writes" scope: document write-backs, repairs and the settle go; the pre-v4 layout move on open stays. Options: that; everything; write-backs only. (user)
17. Neutral naming, with provenance in the tracker only. Options: neutral; name the partner. (user)
18. arc42 `architecture/python.storage.arc42.md` 3.1 becomes: "`- **3.1** A load never writes; an upgraded document is persisted only by a save. [enforced: test:tests/test_law_load_never_writes.py]`" (user, approved wording)
19. A load warning points at `slayer storage upgrade`. (default)
20. Colon spellings stay removed, also through `parse_formula`. They raise the colon-spelling `ValueError`, which the integrator catches. The integrator's runtime input is migrated, functional text. (default)
21. `parse_filter_expr` projects 2.0-only node classes onto their 1.8.4 shape, and refuses text 1.8.4 refused. (default; refined by codex, see 25)
22. Drift uses a "reads a physical column" predicate; `is_physical_reader` stays for keys and live typing. (default; widened by codex, see 32)
23. `DataType` legacy names cover only the four broken members, mapped by the stored v15 type migration. (default)
24. Rejected (codex): accept colon spellings in `parse_formula`. The integrator's runtime input is migrated, functional text. Its colon text comes only from documents it authors at the current version, which 2.0 refuses at query time, so accepting it would validate models every query fails.
25. Filter-tree contract (codex): a recursive declared-field walk reaches only the fourteen 1.x classes carrying the 1.8.4 values; 1.x-refused text raises `ValueError`; nothing lossy is invented. Tests replicate the integrator's walk and check per-class fields.
26. One upgrade service over a snapshot context, shared by loads, the CLI and the pure entry point. (codex)
27. The settle's rename map is computed once per top-level load and applied in memory; the CLI persists dependents before datasources and holds a renaming datasource while a dependent is not stored, so it converges after an interruption. (codex, CodeRabbit)
28. Upgrade writes skip client-save semantic validation, as the old write-back did. Fixtures cover a legacy cycle and a fanning reference. (codex)
29. Upgrade writes are conditional on an unchanged stored form, and a changed one is a reported conflict. (codex)
30. The load warning is keyed on "the upgrade changes this document", including nested stages and repairs. (codex)
31. Entities are enumerated: datasets, semantic-model documents, saved queries, memories and datasources. Priority and embeddings are untouched. A test covers a corrupt document among valid ones. (codex)
32. Every drift and cascade use of `is_physical_reader` is audited onto the new predicate. Tests cover a filtered alias, a quoted physical name, two fields on one column, and metrics naming either. (codex)
33. The tripwire pins one feature per restored name plus the moved-module fallback. Tests: importing doesn't warn, each access warns once, an unlisted name raises. (codex)
34. `DataType` legacy names go through a metaclass lookup, with tests for identity, iteration, `__members__`, pydantic validation and schema, pickling, and one warning per lookup. (codex)
35. The law test compares logical contents and spies write hooks; the pre-v4 move is tested separately. (codex)
36. Deferred past 2.0: a durable, version-gated rename (the datasource records `renamed_from`; only dependents below the settled versions are renamed), and counting validator-level rewrites in the "changes this document" flag. Until then, a 1.x document arriving in an upgraded store, or a current document saved while its datasource is unrenamed, reads a library-colliding granularity name with the library's meaning; this needs a 1.x custom granularity named like a library function. (user)

## Risks / Trade-offs

- **[Risk]** A store below v8 now refines against its live datasource on every load until it is upgraded. → The load warning names `slayer storage upgrade`, and such stores are rare.
- **[Risk]** In-flight 2.0 slices may change behaviour the call suite pins. → This change merges after them. A later slice that trips the suite must keep the call working or say why in its migration-guide entry.
- **[Risk]** Projecting 2.0 trees onto 1.x shapes may drift as the grammar grows. → The adapter refuses anything it cannot project. The forms are removed in 3.0.
- **[Trade-off]** The integrator still has to change its own value-level code: `DataType` strings, `Column.sql`, dump shapes, snapshot hashes. That is documented as the one-off heads-up.

## Migration Plan

Users of a SLayer-owned store run `slayer storage upgrade` (optionally `--dry-run` first) once after moving to 2.0. Until then, loads work and warn. Owners of external files call the pure entry point, or run the command on a copy. Rollback is a plain downgrade: a 2.0 load no longer rewrites a 1.x store, so the store stays readable by 1.x until someone upgrades or saves.
