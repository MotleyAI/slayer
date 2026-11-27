# Upgrading to the Ossie stored format

From this release, the YAML and SQLite stores keep every dataset as an [Ossie](https://github.com/apache/ossie)
dataset inside an Ossie semantic model. This is the stored format later releases keep, so a store needs this one
migration.

## What changes on disk

Each datasource's datasets, relationships and models are Ossie documents in one folder, laid out as Ossie
multi-file models (see [Semantic Models](../concepts/semantic-models.md#the-stored-format)):

```
semantic_models/shop/
  shop.semantic_model.yaml     # the datasource model: lists one dataset document per dataset
  orders.dataset.yaml
  customers.dataset.yaml
  sales.yaml                   # a 2.0 document keeps its file name
```

SQLite holds the same texts in its `documents` table. Datasource configs, the datasource priority, memories and
embeddings keep their storage.

Every Ossie construct is stored as Ossie defines it: `source` as a string, dataset-level `primary_key` and
`unique_keys` (composite keys included), `nonportable_extensions` and `ai_context` at every level, `dimension.is_time`,
every `expression.dialects` entry, exact datatypes (`Decimal`, `Time`, `DateTimeTz`), and names as written. SLayer-only
content stays in each entity's `SLAYER` entry, whose payload is now version 4 (a version-3 payload still reads).

## Upgrading a store

```bash
slayer storage upgrade --storage ./slayer_data
```

Per datasource, the upgrade writes each 2.0 standalone dataset as a dataset document of the datasource model, then
removes its `models/<data_source>/<name>.yaml` file or `models` row. On the way, a dataset's `meta` entries
`osi_ai_context`, `osi_nonportable_extensions`, `osi_is_time` and `osi_unique_keys` become its core `ai_context`,
`nonportable_extensions`, `dimension.is_time` and `unique_keys` (an entry core rejects stays in its `SLAYER` entry, with a warning), and its `sql_table` becomes a `source` (each part
SLayer reads exactly, such as a mixed-case table name, quoted). A 2.0 document already named like its datasource
becomes the datasource model and gains `dataset_refs`; nothing else of its text changes.

The upgrade compares each file or row it writes over with what it read; if one changed meanwhile, the datasource is left
as it was, the conflict is reported and the command exits 1, and a later run upgrades it. A dataset the upgrade refuses
stays as it was beside its upgraded siblings. An interrupted upgrade converges when run again.

Until you upgrade, a store reads as before: each 2.0 standalone dataset loads as a dataset of its datasource model,
with a `stored_upgrade_needed` warning, and a load writes nothing. Saving such a dataset stores it in the new format.

## What you may notice

- **The datasource model is listed.** `semantic-models list`, `list_semantic_models` and `datasets_summary` show a model
  named like each datasource, holding the datasets no other model holds. Edit it like any model, for example to add
  model-scoped metrics over those datasets; it is created by its first dataset and deleted with its last.
- **A document `source` reads by Ossie's identifier rule.** An unquoted part of a table name folds as the database folds
  it. A 2.0 document with `source: public.Orders` on Postgres now reads the table `orders`; write `public."Orders"` to
  read `Orders`. Sources SLayer wrote or ingested are quoted and unaffected.
- **One export rule.** `slayer export-osi` exports every dataset's stored Ossie keys as stored. A document dataset
  carrying a SLayer row filter now exports as a query `source` returning only the rows where the filter holds, where 2.0
  copied its document verbatim.
- **Names are stored as written.** A dataset, field, metric or relationship whose name SLayer cannot reference yet
  (one holding `.` or `:`) loads inert with a warning instead of making its document unloadable.

## Rolling back

An upgraded store needs this release or later; a 2.0 install does not read the new layout. Keep a copy of the store
before upgrading if you may need to roll back.
