## 1. Failing tests (pr-tests)

- [x] 1.1 Capture the no-result-change goldens at DEV-2318's head, before any class changes: rows, values, result keys and column order for single- and multi-stage queries, named and unnamed metric entries, reference and expression dimensions (aliased and not), time dimensions, order items by reference and expression, refinements, and string, list and empty filters. Write them to `tests/fixtures/ossie_shapes/golden.json`. The golden test passes on the current code.
- [ ] 1.2 Write one test per scenario of `specs/compatibility/ossie-vocabulary/spec.md` in `tests/test_ossie_shapes_*.py`, porting the shape-change cases of `egor/dev-1607-attempt-2`'s `tests/test_ossie_vocabulary_*.py` where they fit. Include the named boundary cases (design decision 26):
  - `filter` single/list/empty dumps, including after a refinement;
  - deprecation dedup across several MCP upserts;
  - unknown keys on prebuilt nested Python objects;
  - nested unknown keys in a current-version stored document;
  - datatype conversion without `db_type`;
  - a reordered composite relationship (same edge, author's order kept).

  Every new test fails on the current code for the stated reason.
- [ ] 1.3 Add a test that walks every dataset-side model class (dataset, field, metric, relationship, aggregation, aggregation parameter) with a stray key. Fresh input is refused naming the path; stored input is dropped with one `unknown_stored_key` payload. It fails now.
- [ ] 1.4 Extend the curated twin tests and the type-checker probe with:
  - `ColumnRef` and `ComputedDimension` → `QueryDimension`, with their 1.x keywords;
  - `DatasetRelationship.join_pairs`, read and write;
  - `DatasetExtension.source_name`;
  - `QueryMetric.formula`.

  Remove the twins DEV-2318 aimed at main-only names (`ColumnRef` → `FieldRef`). Both fail now.
- [ ] 1.5 Respell existing tests mechanically to the final shapes with a throwaway script that is not committed. The substitution table:
  - metric `{expression, name}` → `{metric, alias}`;
  - `FieldRef {name, dataset}` → dotted `field` text;
  - `ComputedDimension` → `{field, alias}`;
  - `TimeDimension.name` → `alias`;
  - an order item's field object → text;
  - `source_name` → `dataset`;
  - `join_pairs` → `from_columns` + `to_columns`;
  - datatype values → Ossie names;
  - `RelationshipEdgeRef` keys.

  Leave untouched the 1.x back-compat tests, stored documents of older versions and the upgrade fixtures `v0_10_2` / `v1_8_2`. Rewrite the `new` side of `tests/fixtures/vocabulary_corpus/*.json` to the final shapes (design decision 20). Bring any test whose logic, not just its spelling, contradicts the plan to the owner before changing it.

## 2. Core shapes

- [ ] 2.1 Split the metric classes in `slayer/core/models.py` and `slayer/core/query.py`. A shared base carries the substitution state. `QueryMetric {metric, alias, label, description, datatype, meta}` has `extra="forbid"`. `DatasetMetric.name` is required, with the existing error text. Verify the query-entry scenario tests and the goldens pass.
- [ ] 2.2 Merge `FieldRef` / `ComputedDimension` into `QueryDimension {field, alias, label}` with `extra="forbid"` and bare-reference string dumps. `FieldRef` stays internal (`QueryDimension.ref`). Move every `isinstance` consumer to it. Verify auto-naming, collision and refinement-identity goldens.
- [ ] 2.3 Make `OrderItem.field` text with a derived `ref`, rename `TimeDimension.name` → `alias` and `DatasetExtension.source_name` → `dataset` (the latter via `la:deterministic-refactor`). Verify the time dimension, order and extension scenario tests.
- [ ] 2.4 Make `filter` `str | list[str] | None` on `SlayerQuery`, `QueryRefinement` and REST `QueryRequest`, with conjunct accessors. `[]` and `null` normalize to none. A refinement appends. Dumps keep the shape written. Move every consumer (stored repair, engine, inspect) to the conjunct accessor. Verify the filter scenario tests.
- [ ] 2.5 Replace `DatasetRelationship.join_pairs` with `from_columns` / `to_columns` and their validation (equal non-zero length, typed errors, conflict with `join_pairs`), plus the derived `column_pairs`. Retarget internal carriers of relationship key pairs. Keep `RelationshipSignature` order-free (design decision 27). Move `RelationshipEdgeRef` to `from_columns` / `to_columns`. Verify the relationship scenario tests.
- [ ] 2.6 Datatype values: introduce the wire value set (Ossie's ten names plus `Array`) for `datatype` and `element_type`, the one wire→kind table, and the kind accessor. Move every engine read of the kind to it. Move the exporter's `db_type` rules to one place shared by conversion, ingestion and drift. Verify the datatype scenario tests and that the engine behaviour test matches the old kinds.
- [ ] 2.7 Unknown keys: one shared mechanism on every dataset-side model class. Convert 1.x keys first. Then, when fresh, refuse with a typed error naming key, path and dataset; when stored, drop the key and record it, emitting one `unknown_stored_key` payload per document through the storage-load → response/save path. Verify task 1.3's test and the unknown-key scenarios.

## 3. Conversions, versions and twins

- [ ] 3.1 In `slayer/core/vocabulary.py`, retarget DEV-2318's 1.x conversions to the final shapes and add:
  - `{formula, name}` and `ModelMeasure` / `DatasetMetric` objects in query metrics;
  - `{name, model}` and `{expression, name}` dimension entries and their objects (design decision 15);
  - time dimension `dimension` / `column`;
  - order item `column`;
  - `source_name`;
  - `join_pairs`, including 1.x `JoinEdgeRef` entries in MCP tools;
  - datatype values with the `db_type` split (design decisions 4, 23).

  Main-only forms are refused. Verify the fresh-conversion scenarios, one deprecation per form per document.
- [ ] 3.2 Wrap each MCP tool request in one conversion document (design decision 22). Verify the MCP dedup test.
- [ ] 3.3 Make DEV-2318's version steps (`slayer/storage/ossie_vocabulary_migration.py`: SlayerModel 25→26, SlayerQuery 15→16, Memory 11→12) write the final shapes and datatype values, including nested queries and the `meta["osi_datatype"]` stash (design decision 18). No new version numbers. Verify the stored-migration scenarios and the `v0_10_2` / `v1_8_2` upgrade fixtures.
- [ ] 3.4 Make the `SLAYER` payload v2 carry the final shapes, with v1 still read (fingerprint over its content as written). The Ossie importer keeps datatypes verbatim and the exporter copies them. Relationships map one-to-one to `from_columns` / `to_columns`. Verify the export-payload scenarios.
- [ ] 3.5 Add the curated twins of task 1.4 in DEV-2318's typed form, then move internal uses through `reportDeprecated` + `dr-refactor retarget`. Verify the twin tests, the type-checker probe and `la-typecheck` (no new errors).

## 4. Surfaces

- [ ] 4.1 REST, the CLI and the Python client accept and emit the new shapes. Verify the surface scenario tests.
- [ ] 4.2 MCP:
  - the `query` tool schema, descriptions and examples advertise `{metric, alias}`, `{field, alias}`, `filter` as a string, time dimension `alias`, order `field` text and extension `dataset`;
  - `edit_dataset` relationships and `remove.relationship_edges` use `from_columns` / `to_columns`;
  - 1.x arguments convert inside the tools.

  Verify the MCP tests.
- [ ] 4.3 `inspect`, search rendering, drift and retype findings, ingestion, and the dbt and Cube converters emit the new shapes and datatype names (`Array<String>`). Verify their tests.

## 5. Docs, spec corpus and harness

- [ ] 5.1 Update `docs/concepts/queries.md`, `models.md`, `time.md`, `docs/reference/mcp.md`, `schema-drift.md`, the dbt and Cube pages and `docs/examples`, re-executing each edited notebook. The `docs/upgrading/2.0.md` vocabulary section lists the 1.x→2.0 forms and the curated twins. Verify every query and dataset example in `docs/` validates.
- [ ] 5.2 Respell the literal identifiers in `openspec/specs/` (`join_pairs`, datatype values in document examples, `{expression, name}` / `{name, dataset}` entries) by a scripted direct edit (design decision 19). Show the owner the substitution table and the diff before committing.
- [ ] 5.3 Apply the approved `architecture/semantics.arc42.md` §4 P3 edit verbatim (design decision 9). Verify `la-arch-check` passes.

## 6. Gates

- [ ] 6.1 Run the local test scope, ruff, `la-typecheck` (no new errors, baseline untouched), `la-arch-check` and `openspec validate dev-1607-ossie-shapes-on-top-of-the-vocabulary-rename --strict`. All pass.
- [ ] 6.2 The PR description states that stores written by `main` between DEV-2318 and this change must be recreated, and that DEV-2318 archives first.
