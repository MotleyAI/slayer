## Context

- See proposal.md for the motivation. This is the shape half of DEV-1607. DEV-2318 (PR #603, change `dev-2318-ossie-vocabulary-names-only-dataset-field`) renamed every name and left these shapes alone. The branch starts at DEV-2318's head (`02fb8f6b`), 0 commits behind `origin/main`.
- DEV-2318's branch is merged in as its review fixes land, then `origin/main` once it merges. Never rebase. DEV-2318 archives `compatibility/ossie-vocabulary` before it merges, and this change's MODIFIED deltas apply after that. A probe confirmed `openspec validate --strict` accepts the deltas against the unarchived capability.
- Ossie's query spec (`apache/ossie` `core-spec/wide_table_semantics.md`):
  - §3.2: a query has `dimensions`, `metrics` and `filter`.
  - §3.7: an entry is a reference string, `{field, alias}` or `{metric, alias}`, and `filter` is one expression string.
  - §3.3: the output name is the alias, else the last part of the reference.
  - §3.5: unsupported keys are malformed and an engine must reject them.
- Facts on the branch today:
  - `MetricsField = list[DatasetMetric]`, so one class serves stored metrics and query entries and carries the substitution state.
  - `DimensionsField = list[FieldRef | ComputedDimension]`. `OrderItem.field` is a `FieldRef` object, dumped `{name, dataset}`. `TimeDimension.name` exists only on `main`; v1.8.5 had `{dimension, granularity, label}`.
  - `DatasetRelationship.join_pairs: list[list[str]]`. `RelationshipEdgeRef` carries `join_pairs`, and its identity (`RelationshipSignature`) is a frozenset of pairs.
  - `SlayerQuery.filter: list[str] | None`.
  - `SlayerDataset` and most nested dataset models ignore unknown keys. `DatasetExtension`, `OrderItem` and `ComputedDimension` forbid them.
  - `DataType` values are `TEXT … ARRAY`, matching sqlglot's names. Legacy lowercase aliases are mapped in `core/enums.py`. The Ossie importer maps Ossie's names lossily (`slayer/osi/models.py::OSSIE_DATATYPES`) and stashes the original in `meta["osi_datatype"]`. The exporter re-derives `Decimal` / `DateTimeTz` from `db_type`.
  - Version steps (DEV-2318): SlayerModel 25→26, SlayerQuery 15→16, Memory 11→12 (`slayer/storage/ossie_vocabulary_migration.py`). The `SLAYER` payload is v2.
  - Storage loads run in stored-document context (`stored_document_boundary`, `migrating_document(…, stored=True)`).
  - Conversion state is shared only inside an enclosing `converted_document` context (`slayer/core/vocabulary.py`).
  - MCP `edit_dataset` validates each upserted part on its own (`slayer/mcp/server.py`).

**Principles that apply:**
- **system:**
  - P6 (Pydantic, no dataclasses).
  - P10 (one typing: the wire→kind table is the one derivation).
  - P11 (fail at save time: unknown keys are refused at save).
- **python.core:**
  - P5 (typed errors: unknown key, key-list length, `SpellingConflictError`).
  - P6 (warnings surfaced twice: deprecations, `unknown_stored_key`).
  - P7 (`core` imports no other node).
- **python.engine:**
  - P7 (one slack pass).
  - P8 (models persist verbatim: datatypes and filters are stored as written).
- **python.storage:** P3 (versioned persistence; P3.1 is `[target: DEV-1844]` and not yet binding).
- **python.importers:** P1 (importers preserve meaning: Ossie datatypes are imported verbatim).
- **python.protocols:** P1 (honest wire catalogs: pg facade and Flight types derive from the engine kind).
- **semantics §4 P3:** edited (decision 9).

None is strained.

**Structural shape:**
- one unknown-key mechanism (convert, then refuse when fresh, or drop and record when stored), applied by the shared dataset-side validation rather than per class;
- one wire→kind datatype table;
- one `column_pairs` accessor;
- one conversion site per 1.x form, in `slayer/core/vocabulary.py`;
- one conversion document per input document, MCP requests included.

**Spec attachment:** MODIFIED and ADDED requirements in `compatibility/ossie-vocabulary`, under the cross-cutting `compatibility` group. Its `touches:` list already covers every affected node, so `index.yaml` and the `.c4` model are unchanged.

## Goals / Non-Goals

**Goals:**
- Every outward-facing key of the query format in Ossie's naming, and every Ossie query (§3.7) accepted as written.
- Ossie's relationship key lists and datatype names, stored verbatim.
- Every 1.x spelling converting with one deprecation per form per document until 3.0, and silently in stored documents.
- No query's rows, values or result keys changing.

**Non-Goals:**
- Output names (alias, else the last part of the reference): DEV-2279.
- Dataset keys SLayer doesn't read yet (`source`, dataset-level `primary_key` / `unique_keys`, `nonportable_extensions`, `ai_context`, `is_time`, arbitrary names) and exact semantics for `Decimal`, `Time` and `DateTimeTz`: DEV-2244. Until then those keys are refused like any other unknown key, and the three types keep their old engine kinds.
- Query-side unknown keys outside the new entry classes (`TimeDimension` and its peers): DEV-1695.
- Back-compat for spellings only `main` accepted before 2.0 (DEV-2318's `{expression, name}` metric entries, `{name, dataset}` references, `TimeDimension.name`), and for stores written by `main` between DEV-2318 and this change.
- Renaming internal IR and SQL join-condition pairs (the SQL `JOIN` carve-out).

## Decisions

1. **Main-only spellings get no back-compat. Only 1.x spellings convert, with a deprecation.** `main` is never released before 2.0. (user)
2. **No new version numbers.** DEV-2318's 1.x→2.0 steps (SlayerModel 26, SlayerQuery 16, Memory 12) write the final shapes and datatype values. The `SLAYER` payload's final form is v2, and only v1 is read.
   - This supersedes the issue body's "payload version 3, versions 1 and 2 read" and its "steps after DEV-2318's numbers".
   - Alternative: steps 27/17/13 and payload v3. Rejected: back-compat for unreleased formats.
   - (default, delegated by user)
3. **Datatype values are Ossie's ten names plus `Array`, stored verbatim.** The engine keeps its kinds through one wire→kind table, and 1.x values convert.
   - Alternatives:
     - A: normalize to SLayer's values. Rejected: needs a P8 edit, and loses `Decimal`/`Float`, `DateTimeTz`/`DateTime`, `Time`/`Opaque`.
     - B: keep a remembered Ossie name next to SLayer's value. Rejected: two wire spellings for one value.
   - (user)
4. **Splitting a 1.x `DOUBLE` and `TIMESTAMP`:** `Decimal` / `DateTimeTz` when the field's `db_type` says so, by the exporter's existing rules moved to one place; else `Float` / `DateTime`. `Time` stays opaque in the engine. (default)
5. **Order items take `field` as text,** folding the reference object like dimension entries do. The issue's "the old `model` key folds into the `field` text" and the old plan cover it. (default)
6. **Time dimensions name their result `alias`.** `name` exists only on `main` (v1.8.5 had none), so it is replaced outright. (user)
7. **Outward-facing surfaces, the query format above all, follow Ossie's naming in full.** Checked against `core-spec/wide_table_semantics.md` §3.2/3.3/3.7: after this change every Ossie query key matches. The remaining gaps are tracked in DEV-2279 (output names) and DEV-2244 (dataset keys). (user)
8. **Extensions name their target `dataset`.** Ossie's `source` means a physical source, so `source_name` clashed with it. The 1.x `source_name` converts with a deprecation. (user)
9. **arc42 edit to `architecture/semantics.arc42.md` §4 P3** (user, approved wording):
   ```diff
   -   query fields, `join_pairs`, unique-key sets — names a column by
   +   query fields, `from_columns` / `to_columns`, unique-key sets — names a column by
   ```
10. **`filter` is `str | list[str]`.** Internally it is an ordered list of conjuncts. A refinement appends the conjuncts the saved query lacks. Omitted, `null` and `[]` all mean none. (issue)
11. **`DatasetMetric` and `QueryMetric` share a base carrying the substitution state.** `FieldRef` and `ComputedDimension` become `QueryDimension`, and `FieldRef` stays only as the internal parsed reference. Auto-naming, collision checks and refinement identity are pinned by golden tests. (issue)
12. **`from_columns` / `to_columns`:** equal, non-zero length; order kept in storage, dumps, exports and SQL; mixing with `join_pairs` refused. (issue)
13. **Unknown keys:** refused on fresh datasets after 1.x keys convert; dropped with one warning per stored document; nested parts included. Ossie keys not yet read are refused like any other, as Ossie §3.5 requires. (issue; default for the order and the channel)
14. **`DatasetMetric.name` becomes required,** keeping the existing "must have a name" error text. Datasets already refuse unnamed metrics, and the shared query use is gone. (default)
15. **In a 1.x dimension entry, `name` is the field unless `expression` is present, in which case it is the alias.** The same rule governs the `ColumnRef` and `ComputedDimension` twins. (default)
16. **`RelationshipEdgeRef` carries `to`, `name`, `from_columns` and `to_columns`.** It is outward-facing (MCP `remove.relationship_edges`, drift). (default, under decision 7)
17. **The internal key-pair accessor is `column_pairs`,** after Ossie's `*_columns`. Internal carriers of relationship key pairs follow it, and IR and SQL join-condition pairs keep their names. (default)
18. **The importer's `meta["osi_datatype"]` stash is retired.** A stored stash migrates into `datatype` when it maps to the stored type, and is then dropped. (default, follows decision 3)
19. **The spec attaches as MODIFIED + ADDED requirements in `compatibility/ossie-vocabulary`.** Literal identifiers elsewhere in the spec corpus (`join_pairs`, datatype values in document examples, `{expression, name}` entries) are respelled by a direct scripted edit, as in DEV-2318 decision 18. The owner sees the diff before the commit. (default)
20. **DEV-2318's corpus fixtures** (`tests/fixtures/vocabulary_corpus/*.json`) have their `new` side rewritten to the final shapes by a throwaway script. (default; approved with the plan)
21. **`filter` is dumped in the shape written; `[]` and `null` normalize to no filter and are omitted.**
    - Rejected (codex): canonical dumping by conjunct count, which rewrites the author's spelling against engine P8 and isn't the issue's wording.
    - Folded (codex): the undefined `[]` case.
    - (user)
22. **One conversion document per MCP tool request,** so that `edit_dataset`'s separately validated parts share one deprecation set. (codex)
23. **Without a usable `db_type`** (dataset and query metrics, `element_type`, a field with none), a 1.x `DOUBLE`/`number` becomes `Float` and `TIMESTAMP`/`time` becomes `DateTime`. (codex)
24. **Every covered nested dataset-side model handles unknown keys at construction,** depending on whether it is fresh or stored: refused with the key's path when fresh, dropped and recorded when stored. A nested Python object can't lose a stray key before the dataset sees it. (codex)
25. **Stored unknown keys travel as a structured `unknown_stored_key` payload** naming the dataset and every dropped key path, one per document. It goes through the same storage-load → response/save path as `spine_relationship_dropped`. A dropped key is not kept on write-back. (codex)
26. **Named boundary tests:**
    - `filter` single/list/empty dumps, including after a refinement;
    - deprecation dedup across several MCP upserts;
    - unknown keys on prebuilt nested Python objects;
    - nested unknown keys in a current-version stored document;
    - datatype conversion without `db_type`;
    - a reordered composite relationship.

    (codex)
27. **Rejected (codex): ordered relationship identity.** `from_columns: [a, b] / to_columns: [x, y]` and `[b, a] / [y, x]` join on the same condition, so they are one relationship. The set of pairs keeps each pairing. "Order kept" means dumps, storage, export and SQL `ON` order keep the author's order. A test pins both. (user)
28. **Python twins** follow DEV-2318's curated list and typed form:
    - class names `ColumnRef` and `ComputedDimension` → `QueryDimension`, with their 1.x constructor keywords;
    - attributes `DatasetRelationship.join_pairs` (get and set), `DatasetExtension.source_name`, `QueryMetric.formula`.

    Main-only names get none. (default, per the carried-over curated-list ruling)
29. **`DataType` holds the wire values; the engine's kinds move to an internal `TypeKind`.**
    - `DataType` members: `STRING`, `INTEGER`, `DECIMAL`, `FLOAT`, `BOOLEAN`, `DATE`, `TIME`, `DATETIME`, `DATETIME_TZ`, `OPAQUE`, `ARRAY`, valued by Ossie's names. Pre-2.0 member names get no twins.
    - `TypeKind` keeps today's eight kinds and is reached only through the wire→kind table (decision 3). It goes once DEV-2244 gives `Decimal`, `Time` and `DateTimeTz` their own semantics.
    - Tests respell `TEXT→STRING`, `INT→INTEGER`, `DOUBLE→FLOAT`, `TIMESTAMP→DATETIME`, `UNKNOWN→OPAQUE`.
    - Alternatives: a new public enum beside `DataType`, or a string `Literal`. Rejected: the enum named for the field would not hold its values.
    - (user)

## Risks / Trade-offs

- **A result key drifts through the entry-class merge.** → Golden rows, values, keys and column order are captured at DEV-2318's head before the classes change, and compared for the 1.x and final spellings.
- **A dev store written by `main` between the two merges loses relationships** (a v26 `join_pairs` is an unknown key in a current-version document). → Recreate such stores. This goes in the PR description; there are no users outside the team.
- **Datatype respelling touches about 690 test sites and every type display.** → A mechanical script, gated by the local test scope. Engine behaviour is pinned by the "behaviour follows the old kind" scenario.
- **The unknown-key mechanism misses a nested model,** and a stray key vanishes silently. → One shared mechanism on every dataset-side class, and a test that walks every nested model class with a stray key.
- **DEV-2318's review fixes conflict with this branch's edits** to its migration step and conversions. → Merge its branch as fixes land, and resolve conflicts towards the final shapes.
- **Archive order.** → DEV-2318 archives first; this change's MODIFIED deltas then apply to the archived capability.

## Migration Plan

- Stored documents from 1.x releases upgrade on load through DEV-2318's version steps, now writing the final shapes and datatype values.
- Fresh 1.x spellings keep working until 3.0, with deprecations, and the removal tripwire covers their `removal="3.0"` markers.
- Stores written by `main` between DEV-2318 and this change are recreated.
- Rollback: a store opened by 2.0 is not readable by 1.x (DEV-2318 decision 17), so rolling back means restoring a backup.
