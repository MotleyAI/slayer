## Context

See proposal.md for motivation. Current state (2.0):

- **Two stored shapes.**
  - Standalone datasets are `SlayerDataset` dumps: YAML `models/<ds>/<name>.yaml`, SQLite `models(data_source, name, data)`.
  - Document datasets live inside Ossie documents: YAML `semantic_models/<ds>/<name>.yaml`, SQLite `semantic_models(data_source, name, text)`.
  - The standalone shape has no `source` string, per-field `primary_key` / `unique` flags, no composite unique keys, and `ai_context` / foreign extensions only as `meta.osi_*` stashes. Unknown keys are dropped on load.
- **Two Ossie mappings.**
  - `slayer/storage/document_mapping.py::_Writer` writes document datasets and patches the stored text in place with ruamel.
  - `slayer/osi/exporter.py::_Exporter` renders standalone datasets for export.
  - They disagree: the exporter drops composite unique keys, an explicit `is_time` and target-dialect entries.
- **The writer.**
  - It writes `source = sql_table` raw.
  - `core_source` reads a stored `source` back as `sql_table` verbatim, so the table-name rule applies: an unquoted upper-case part is read exactly, whereas Ossie folds it.
- **A leak.** A load adds an in-memory dialect entry to a field with no runnable form. An edit of that field's other attributes then writes the entry into its `SLAYER` entry.
- **Names.**
  - Dataset, field and relationship names refuse `.` and `:`; metric names must match `[A-Za-z_]\w*`. A document holding any other name fails to load as a whole.
  - File names are the raw names.
  - A document name must equal its file stem.
- **The upstream base.** apache/ossie PR 827 (open) defines multi-file models. The vendored corpus is pinned at `d4dca4e9` (22 Nov), 30 commits behind upstream `main`, and DEV-2483 is re-vendoring `main`.

Element principles that apply:
- **system:** 4, 5, 6, 10, 11.
- **storage:** 1, 2, 3, 3.1, 3.2.
- **core:** 5, 6, 8, 9.
- **importers:** 1.

The new capability `models/stored-format` sits under the existing cross-cutting `models` group. No `model.c4` or `index.yaml` change is intended; if `la-arch-check` needs one, stop and present the exact edit.

## Goals / Non-Goals

**Goals:**
- One stored shape for a dataset (an Ossie dataset inside an Ossie model) and one storage writer.
- A move between models moves a reference, never converts.
- Every built-in store's models resolve and validate as Ossie, and the format is frozen by a committed corpus.

**Non-Goals:**
- **Evaluating the constructs**, all DEV-2464: exact decimals, composite-key proofs, choosing a dialect entry at query time, quoted names in references, `ai_context` in search. Time of day is DEV-2460.
- **Accepting old SLayer spellings as input** (DEV-2464).
- **Importing a split-model folder, or exporting one.**
- **Formats that aren't Ossie:** datasource configs, memories, priority and embeddings.
- **DEV-2452's remaining rulings:** expression text, construct rulings, the scope of the promise, the version marker.
- **Custom backends and the public upgrade entry point**, which keep `SlayerDataset` dicts.
- **Vendoring PR 827's head now.**

## Decisions

Each entry keeps its tag: `(user)` the owner decided, `(default)` taken because it had an obvious default, `(codex)` a Codex plan-review finding folded in.

1. **The layout follows PR 827.** Per datasource, model documents and dataset documents live in `semantic_models/<ds>/` and are recognised by content. Every stored dataset is Ossie, one storage writer (`document_mapping`) remains, and the standalone shape is retired for the built-in stores. (default; the brief "base on PR 827", issue items 1–5 and DEV-2452's acceptance; any other layout needs a second migration or regresses 2.0 documents)
2. **The datasource model is a semantic model like any other.** It is named exactly like its datasource, holds every dataset no named document holds, and is listed, inspected, edited and deleted through every semantic-model surface. A same-named 2.0 document is adopted. It is created on its first dataset and deleted with its last (2.0 precedent: a document is deleted with its last dataset). Rejected: a storage-only container with a reserved name. That would put a SLayer-only placeholder name into files other tools read and hide a stored model from SLayer's own listing. (user)
3. **A document's `source` is read by Ossie's identifier rule, and writers quote every part SLayer's table-name rule reads exactly,** so stored text names the same table under both rules. Rejected: recording the reader fix on DEV-2464, which would leave a valid Ossie document reading another table in SLayer than in any other Ossie tool. (user)
4. **Export follows one rule for every dataset:** Ossie keys as stored, plus the `osi-export` rendering wherever `SLAYER` changes what core means. This revises DEV-2243 decision 11's verbatim copy. Rejected: keeping 2.0's split by kind, which would special-case the datasource model and keep exporting filtered document datasets unfiltered. (user)
5. **A move between models moves a `dataset_refs` entry;** the dataset file is untouched. (default)
6. **File names** use a readable stem or its percent-encoding, plus a hash suffix for length, reserved device names and case clashes. They are recorded in `iri` and kept while the name stays. (default; PR 827 leaves file names to the splitter)
7. **2.0 document files keep their names;** new files use `.semantic_model.yaml` / `.dataset.yaml`. (default)
8. **SQLite has one `documents(data_source, file_name, text)` table** of texts kept as written, under YAML's file names. (default)
9. **SLayer's versioning is the `SLAYER` payload `version`;** the v3 shape is listed in the spec and frozen. (default; 2.0 precedent, DEV-2452 item 2)
10. **Names are stored as written.**
    - A name SLayer can't reference yet loads inert with a warning, following the 2.0 pattern for entities SLayer can't evaluate.
    - Authoring such a name waits for DEV-2464.
    - Model names may be any string, because file names are encoded. (default)
11. **Query-backed datasets keep 2.0's core `source`** (the dataset name); their queries stay in `SLAYER`. (default; 2.0 precedent)
12. **The migration runs in memory on load** and is persisted only by a save or `slayer storage upgrade`. (default; storage principle 3.1)
13. **Custom backends and the public upgrade entry point keep `SlayerDataset` dicts.** (default; no regression)
14. **The upstream oracle for PR 827's own resolver waits for its merge and a re-vendor,** as a strict xfail citing DEV-2484. Vendoring PR 827's head now would pull 30 unrelated upstream commits into this change while DEV-2483 re-vendors `main`. (default, amended by 16)
15. **The translated in-memory dialect entry is never persisted.** (default; a bug against item 6)
16. **Every stored model is checked against the vendored core schema and upstream `validate.py`,** after resolution, and this runs now. Each dataset document's `dataset` is checked against `$defs/Dataset`. (codex)
17. **Writes are serialised per datasource.** YAML takes one lock per datasource folder for every write; SQLite runs every write in one `BEGIN IMMEDIATE` transaction. (codex)
18. **Multi-file YAML writes are journaled.**
    - A load reads a pending journal as if the operation had completed, in memory only.
    - The next write or the upgrade completes it.
    - A dataset document is written before its reference is added, and its reference is removed before the file is deleted.
    - An unlisted dataset document produces a warning. (codex)
19. **The upgrade compare-and-swaps every participating artifact per datasource;** a conflict leaves the legacy form intact. (codex)
20. **PR 827 resolution errors isolate their model** (`models/document-isolation`). A dataset document listed by two models follows the 2.0 collision quarantine. (codex)
21. **The `source` test matrix covers** multi-part names, quoted dots, embedded quotes, spaces, reserved words and mixed parts, through every step. (codex)
22. **Rejected (codex): editing the prose of `architecture/python.storage.arc42.md`.** Its purpose text stays accurate and no principle changes; arc42 stays normative and minimal.
23. **The change carries deltas** for `semantic-model-documents`, `stored-upgrade`, `table-names` and `importers/osi-export`. (codex)

## Risks / Trade-offs

- [PR 827 changes before it merges (renamed keys, other file rules)] → the shapes are implemented natively in one module. A change would need a migration before this format is declared final, so watch DEV-2484 and re-check before DEV-2452 declares the format final.
- [The source-reader fix changes which table some 2.0 documents read] → it affects only an unquoted mixed-case part on a case-folding database. The migration guide states it, and ingested and SLayer-written sources are quoted, so they are unaffected.
- [Multi-file writes and crashes] → the journal plus write ordering. Interruption tests after every write step.
- [Many existing tests pin the 2.0 standalone layout] → they are aligned to the plan, with approval, in pr-tests.
- [A folder lock serialises writes within a datasource] → writes are rare and short; reads take no lock.

## Migration Plan

- `slayer storage upgrade` converts a 2.0 store per datasource. Until then a store reads unchanged, with upgrade warnings.
- A new `docs/upgrading/` page covers:
  - the layout;
  - the datasource model appearing in `semantic-models list`;
  - the `source` case reading change;
  - the one-rule export change for documents carrying SLayer row filters.
- Rollback: an upgraded store needs this version or later. A 2.0 install can't read the new layout, which is the nature of the format migration.
