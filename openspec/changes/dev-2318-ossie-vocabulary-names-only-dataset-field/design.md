## Context

- See proposal.md for the motivation. The tracker issue DEV-2318 holds the word table, the fixed names, the carve-outs and the attached generated rename list (AST sweep of `main` @ 73e8ebb5). That list is regenerated from the current `main` before the rename runs.
- This is the names-only half of DEV-1607. DEV-1607's earlier branch (`egor/dev-1607-rename-columns-to-fields-to-be-consistent-with`) has reusable pieces:
  - the `reportDeprecated` groundwork;
  - the blast-radius typing;
  - `rename-map.yaml`;
  - the twin helpers in `slayer/core/vocabulary.py`;
  - the version-step traversal for nested queries;
  - its tests under `tests/test_ossie_vocabulary_*.py`.

  Shape changes in those pieces are not ported.
- Stored versions on `main`: SlayerModel 25, SlayerQuery 15, Memory 11, Embedding 1 (`slayer/storage/migrations.py`).
- `slayer.core.deprecation.deprecated_input(old, new, *, removal)` emits one `FutureWarning` and one structured `deprecated_input` payload. It is in the removal tripwire's `HELPERS`. `raise_moved(old, new, *, removal)` is the moved-module `ImportError` stub. `slayer/core/formula.py` is already such a stub, pointing at `slayer.core.syntax`.
- The embeddings sidecar (`slayer/storage/sidecar_embedding_store.py`, used by both YAML and SQLite storage) has no `version` column. It rebuilds rows as fresh `Embedding` documents, so a version step never runs on them.
- Help topics are memories whose IDs come from their file names (`slayer/memories/help_seed.py`). Seeding deletes only the IDs in `RETIRED_HELP_IDS`.
- Usage reporting: schema 2 (`slayer/usage_reporting/payload.py`).
  - Spool files are named by schema version.
  - The sender claims every pending file of this or an older schema and sends one merged current-schema report.
  - `read_spooled` folds an undefined usage key into its surface's `other`.
  - The disclosure notice is printed again after a schema bump.
- The Ossie export `SLAYER` payload is version 1 (`slayer/osi/slayer_extension.py`). It is `extra="forbid"`, and its fingerprint hashes the dumped payload content.
- The Python client reads `attributes.measures` (`slayer/client/slayer_client.py`).
- `living-architecture` is pinned at 0.6.0 in `pyproject.toml`, while the installed tools are 0.11.0. `dr-refactor rename-map`, `--with`, `--keep`, `--docs` and `retarget` are available.
- No LikeC4 element maps a renamed module by name. The only elements named are `core.models`, `core.query`, `core.syntax` and package-level nodes.

**Principles that apply:**
- `system` P1 (every top-level package claimed once) and P2 (model truth): the renamed modules stay inside their packages.
- `python.core` P5 (typed errors: `SpellingConflictError`), P6 (warnings surfaced twice: `deprecated_input`) and P7 (`core` imports no other node: `slayer/core/vocabulary.py` imports only `core`).
- `python.engine` P7 (one slack pass, typed warnings) and P8 (a deprecated spelling is stored as its canonical twin).
- `python.storage` P3 (versioned persistence). P3.1 ("a load never writes") is `[target: DEV-1844]` and not yet binding.
- `python.usage_reporting` P2–P4: tokens stay class-only and reports stay content-free.

None is strained.

**Spec attachment:** the new capability `compatibility/ossie-vocabulary` sits under the cross-cutting `compatibility` group. Its `touches:` list in `architecture/index.yaml` gains `python.ir`, `python.search`, `python.memories`, `python.protocols` and `python.usage_reporting`. The modified capabilities, `models/stored-upgrade` (under `models`) and `usage-reporting/collection` (owned by `python.usage_reporting`), are already mapped.

## Goals / Non-Goals

**Goals:**
- Every SLayer name in its final Ossie form, from one `rename-map` run plus a respelling pass, with no old word left outside the carve-outs.
- Every old spelling of a document, query, surface or listed Python name converts with a warning until 3.0, and every stored document converts silently, for good.
- No query's rows, values or result keys change.

**Non-Goals:**
- Everything in the issue's "Left to DEV-1607" list: `join_pairs` → `from_columns` / `to_columns`, `QueryMetric` / `QueryDimension`, `{metric, alias}` entries, `filter` as a string, refusing unknown keys. Also out are DEV-1607's own export-payload changes, which become payload version 3.
- Ossie datatype values (accepting Ossie type names, `time` vs `Time`). This is a value change, not a name, and stays with DEV-1607.
- Prose nouns in `openspec/specs/` and `architecture/`, and spec capability ids and directory names.
- The storage layout (the YAML `models/` folder, the SQLite schema; DEV-2243) and words inside embedding text.
- Twins for Python names outside the curated list, which is backfilled when a gap is found.
- A new client talking to a pre-2.0 REST server.

## Decisions

1. **Python twin scope is a curated, explicitly listed public surface.** It covers:
   - the documented API (`docs/reference/`, and the classes and fields `docs/` shows);
   - the public classes of `core.models`, `core.query`, `core.errors`, `core.warnings` and `core.recommend`, with their renamed fields;
   - the methods of `StorageBackend`, `SlayerQueryEngine` and `SlayerClient`;
   - every renamed name Storyline imports or calls.

   The list lives in the twin test module and in `docs/upgrading/2.0.md`. Everything else is renamed outright, and the list is backfilled when a gap is found. Alternative: twin every renamed public name (about 500). Rejected: about 400 twins for internals that 2.0 already treats as unstable, since moved modules raise. (user)
2. **The pairwise corpus is captured as committed per-file pair fixtures.** After phase 1's respelling, a throwaway extractor (not committed) walks every test file and fixture that phase 1 changed, in `main`'s version and in the respelled version, and pairs the differing document literals by position. It classifies each pair's kind from its call context (constructor or `model_validate`, REST route, MCP tool, fixture folder). It writes `tests/fixtures/vocabulary_corpus/<file>.json` holding `pairs: [{kind, old, new}]` and `skipped: [{line, reason}]`, where `skipped` lists the literals that are not fully literal or can't be classified. One parametrized test per file checks that each `old` converts to exactly its `new`, with its deprecations. Upgrade fixtures and stored documents with an older `version` are excluded; they are the stored corpus.

   Alternatives: commit `main`'s old test files and pair them at test time (rejected: every later edit to a test breaks its pairing); read git objects at test time (rejected: CI checks out at depth 1). (user)
3. **The Ossie export `SLAYER` payload goes to version 2 now.** A version-1 payload is verified against its own fingerprint over its content as written, before conversion. It then converts silently and is read for good. DEV-1607's shape change becomes version 3. Alternative: keep version 1 and accept both spellings. Rejected: the version would no longer identify the format. (user)
4. **arc42 edit to `architecture/python.engine.arc42.md` Principle 5** (user, approved wording):
   ```diff
   -   (`ModelScope`: dots walk joins; `StageSchema`: flat names, plus a list
   +   (`DatasetScope`: dots walk joins; `StageSchema`: flat names, plus a list
   ```
5. **arc42 edit to `architecture/python.engine.arc42.md` Principle 8** (user, approved wording):
   ```diff
   -8. **Models persist verbatim**: `save_model` stores the author's spelling
   +8. **Models persist verbatim**: `save_dataset` stores the author's spelling
        unchanged, a deprecated spelling excepted, which is stored as its canonical
        twin; normalization applies to queries at execute time only. The one
        exception: a saved query stores each stage's inferred population as its
   -   `source_model`. [review]
   +   `population`. [review]
   ```
6. **arc42 edit to `architecture/semantics.arc42.md`** (user, approved wording):
   ```diff
   @@ Axiom 2
   -     `Column.sql` likewise.
   +     `DatasetField.sql` likewise.
   @@ §4 Principle 2
   -2. **Two expression layers**: Mode A free SQL (`Column.sql`, model `filters`)
   +2. **Two expression layers**: Mode A free SQL (`DatasetField.sql`, model `filters`)
   @@ §4 Principle 3
        query fields, `join_pairs`, unique-key sets — names a column by
   -   `Column.name`; the physical spelling (`Column.physical_name`: a base
   +   `DatasetField.name`; the physical spelling (`DatasetField.physical_name`: a base
   ```
7. **The usage report goes to schema 3,** with `dataset_count`, the new tool tokens and the new route templates. The disclosure notice prints once more, and the receiving endpoint must accept schema 3 when this ships. Alternative: keep schema 2 with `model_count` on the wire. Rejected: an old word would stay on the wire for good, and schema 2 would mix two token vocabularies. (user)
8. **No `.c4` edit is needed,** because no LikeC4 element maps a renamed module by name. (default)
9. **Renamed modules get `raise_moved` stubs,** following `main`'s moved-module convention, which the issue's "moved-module stub" refers to. `slayer/core/formula.py` and `slayer/engine/join_safety.py` are already stubs and keep their paths, so their entries in the rename list are dropped; the `engine/join_safety.py` stub is repointed to `slayer.engine.cardinality.relationship_safety`. (default, user)
10. **Query `filters` → `filter`** applies to `SlayerQuery`, `QueryRefinement` and REST `QueryRequest`. The IR's internal `filters` and a dataset's `filters` are unchanged. (default)
11. **Old MCP tool arguments, `inspect` kinds and sections, and help references convert inside the new tools, with a warning.** Carried over from DEV-1607 decision 5. (default)
12. **Both spellings of one concept in one object raise `SpellingConflictError`.** (default)
13. **What counts as stored, and write-back,** are as on `main` (DEV-1607 decisions 10 and 11). A document is stored when it carries a `version` key or is nested in a stored document. Migrated stored documents are written back as today, per entity: datasets are written back on load, and queries and memories convert on every load. (default)
14. **`attributes.measures` is renamed outright.** A serialized response carries no `measures` key, and Python keeps a deprecated, never-serialized `ResponseAttributes.measures` property until 3.0 (DEV-1607 decision 7, as amended 2026-11-15). (user, carried over)
15. **Twin form** (DEV-1607 decisions 29 and 30):
    - each method twin is a declared `def` with the new member's signature, and each attribute twin is a `@property` with a getter and a setter;
    - both are `@deprecated("…", category=None)` and warn through the one tagged site in `slayer/core/vocabulary.py`;
    - an old class name is a `TYPE_CHECKING`-only `@deprecated` subclass declaration, plus a runtime module `__getattr__`;
    - a 1.x backend's overrides of old hook names stay runtime-only, because basedpyright does not report overriding a deprecated method;
    - a curated class also accepts its old field names as input keys (constructor keywords or dict keys), converted as fresh input with a deprecation, so Storyline's `JoinFilterRuleset(column=…, joins=…)`, `JoinFilterRule(join_path=…)` and `ColumnFilterRuleset(column=…)` keep working (user).

    (default, probe)
16. **The `reportDeprecated` groundwork is in this PR** (DEV-2294, closed as a duplicate):
    - `typing-extensions` becomes a direct dependency;
    - `[tool.basedpyright]` sets `reportDeprecated = "error"`;
    - the sites the rule flags today are fixed in code, never baselined or suppressed;
    - back-compat tests that use an old name on purpose mark that line `# pyright: ignore[reportDeprecated]`;
    - the baseline is never rewritten.

    (default)
17. **A 2.0 store is not readable by 1.x.** `models/stored-upgrade` drops the v1.4.1 field-set promise, and `TestPreviousReleaseFieldSets` and `tests/_v1_4_1_field_sets.py` are deleted (DEV-1607 decision 41). (user, carried over)
18. **Literal identifiers in the spec corpus are respelled by a direct scripted edit, not by deltas** (DEV-1607 decision 20). The owner sees the substitution table and the diff before the commit. Capability ids and directory names are unchanged. (user, carried over)
19. **The `compatibility` group's `touches:` list is extended** with `python.ir`, `python.search`, `python.memories`, `python.protocols` and `python.usage_reporting`. (default)
20. **Ossie datatype values stay with DEV-1607,** and this is noted on DEV-1607. (default)
21. **The `living-architecture` pin moves to 0.11.0,** the installed tools' release, which carries `rename-map` and `retarget`. (default)
22. **CodeRabbit:** the PR is far over its 150-file limit, so at review I ask once whether it counts as off. (default)
23. **Deprecations are one per distinct old form per document,** deduplicated across nested parts and repeats, so two forms give two payloads. This is the reading of the issue's "one warning per document" that agrees with `compatibility/removal-tripwire` (one payload names one form) and with DEV-1607's `deprecations()`. Alternative: one combined payload per document. Rejected: it changes the payload's shape and the tripwire requirement. (codex)
24. **Embedding kinds are rewritten in the sidecar** in one idempotent transaction when it opens, because the sidecar stores no `version` and rebuilds rows as fresh documents. The Embedding 1→2 step stays for versioned embedding documents. Nothing is re-embedded, and no content hash changes. (codex)
25. **`help.models` and `help.joins` are retired** through `RETIRED_HELP_IDS`, which removes them and their embeddings on reseed. Old references resolve to the new topics with a warning. (codex)
26. **Spooled schema-2 usage batches migrate into schema 3:** the renamed keys and tokens map to their new keys before any unknown key folds into `other`. They are sent as schema 3. This replaces the first draft's "sent as schema 2", which the sender's merge can't do. Tests mix v1, v2 and v3 spool files. (codex)
27. **The Python client reads `attributes.metrics`,** with contract tests for a new client against a new server and against an old route. A new client against a pre-2.0 server is not supported. (codex)
28. **Rejected (codex): "query `filters` must stay `filters`".** The issue renames the key ("query `source_model` / `measures` / `filters` (still a list)"); only "filter as a string" is DEV-1607's.
29. **Rejected (codex): "write-back on load violates storage P3.1".** P3.1 is `[target: DEV-1844]`, and the plan keeps today's per-entity behaviour (decision 13). (user)

## Risks / Trade-offs

- **A result key drifts through the rename.** → Golden rows, values and keys are captured on `main`'s code before phase 1, and compared for both spellings afterwards.
- **`rename-map` rewrites a legacy fixture's keys,** for example a pre-2.0 document validated through a migrating validator. → Upgrade fixtures and older-version stored documents go under `--keep`, and the applied diff is reviewed for them before the gate.
- **`rename-map` runs out of memory on a map this size.** → Split the map into batches and run each batch through the full dry-run → review → apply procedure.
- **A site `dr-refactor` misses.** → A `reportDeprecated` finding outside the `--keep` paths, or an old word the final sweep finds, is a living-architecture bug. It is fixed in that repo and moved here with `retarget` entries, never by hand.
- **A stored document is taken for fresh input, or the reverse.** → The stored-document context is carried through nested validation, and tests cover unversioned nested queries.
- **The diff is far over CodeRabbit's 150-file limit.** → Codex, Sonar and CI are the gates (decision 22).
- **Hard-coded MCP tool names, and deep imports from moved modules, break on upgrade.** → The `docs/upgrading/2.0.md` section lists every old and new tool name and module path.
- **The usage-report receiver rejects schema 3.** → Coordinate the receiver before this ships; this is noted in the PR description.

## Migration Plan

Stored documents upgrade on load through the four version steps, and the embeddings sidecar's row kinds are rewritten when it opens. Fresh old spellings keep working until 3.0, and the removal tripwire fails the 3.0 release while any `removal="3.0"` marker remains. Rollback: a store opened by this release is not readable by 1.x (decision 17), so rolling back means restoring a backup of the store.
