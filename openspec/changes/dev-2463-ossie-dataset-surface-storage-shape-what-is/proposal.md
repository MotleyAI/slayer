## Why

SLayer stores a dataset in two shapes: datasets of a native Ossie semantic-model document as Ossie, and every other ("standalone") dataset in SLayer's own shape. The standalone shape holds none of Ossie's dataset constructs: `source` as a string, dataset-level `primary_key` / `unique_keys`, `nonportable_extensions`, `ai_context`, `dimension.is_time`. Converting between the two shapes loses them. Users migrate their stores once after 2.0 (DEV-2451, DEV-2452), so the stored format must become Ossie, and final, before then. Ossie's multi-file models (apache/ossie PR 827) give the layout: a model document plus one dataset document per dataset, in one folder.

## What Changes

- **BREAKING (stored format)**: every dataset the YAML and SQLite stores hold is an Ossie dataset. Each datasource's folder (`semantic_models/<data_source>/`; SQLite table `documents`) holds Ossie model documents (flat, or PR 827 split model documents) and PR 827 dataset documents.
- The **datasource model**: the model named like its datasource holds every dataset not in a named document. It is a semantic model like any other, listed, inspected, edited and deleted through the semantic-model surfaces. `create_dataset` without `semantic_model` adds to it, and moving a dataset between models moves a `dataset_refs` entry.
- Each construct of the issue is stored as Ossie defines it: the `source` string in Ossie spelling, dataset-level keys, `nonportable_extensions` at every level, `ai_context`, `is_time`, every dialect entry, names as written, exact types and `label`. SLayer-only content stays in `SLAYER` entries whose payload shape is frozen.
- **BREAKING (query results)**: a document's table-form `source` is read by Ossie's identifier rule. An unquoted part folds per dialect, so a 2.0 document with an unquoted mixed-case part reads the folded table on a case-folding database.
- File names are derived from names and recorded in `iri`. Multi-file writes are locked per datasource folder and journaled.
- `slayer storage upgrade` migrates 2.0 standalone datasets into the layout. Loads read the 2.0 forms in memory and never write.
- `export-osi` follows one rule for every dataset: Ossie keys as stored, and the `osi-export` rendering wherever a `SLAYER` entry changes what core means.
- A name SLayer cannot reference yet loads inert with a warning, and its text round-trips.

## Capabilities

### New Capabilities
- `models/stored-format`: the on-disk layout of models and datasets (folders, document kinds, the datasource model, file names, locking and journaling, resolution errors), what is stored for each Ossie construct, and the format-freeze corpus.

### Modified Capabilities
- `models/semantic-model-documents`: documents live in the datasource folder and may be split, a dataset without a named document belongs to the datasource model, the version counter location, and names stored as written.
- `models/stored-upgrade`: `slayer storage upgrade` migrates standalone datasets into the layout, and loads read 2.0 forms in memory.
- `models/table-names`: a document `source` reads by Ossie's identifier rule, and writers spell `source` so that both rules name the same table.
- `importers/osi-export`: one export rule for every dataset of every model.

## Impact

- **Code**:
  - `slayer/storage/`: the YAML and SQLite backends, `base.py` owner routing, `document_mapping.py`, `semantic_models.py`, upgrade, and a new layout/file-name module.
  - `slayer/osi/exporter.py`.
  - `slayer/sql/ossie_source.py`.
  - The name validators in `slayer/core/models.py`.
  - The semantic-model surfaces (MCP, REST, CLI, client) for the datasource model.
- **Stores**: a 2.0 store reads unchanged until it is upgraded. `slayer storage upgrade` writes the final format.
- **Unchanged**: custom backends and the public upgrade entry point (`SlayerDataset` dicts).
- **Docs**: storage configuration, semantic models, Ossie export, and a new upgrading page.
