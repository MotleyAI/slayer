# Importing Ossie (OSI) Documents

SLayer imports [Ossie / Open Semantic Interchange](https://github.com/apache/ossie) (OSI) semantic-model documents natively: each valid document is stored as written, as a [semantic model](../concepts/semantic-models.md) of the datasource, and its datasets, relationships and metrics are queryable at once. Nothing is converted.

## Quick Start

```bash
slayer import-osi ./osi_configs --datasource my_postgres --storage ./slayer_data
```

This reads every `.yaml`/`.yml`/`.json` file in the path (a single file or a directory). The datasource must be configured, but it need not be reachable: importing a current-shape document never connects to the database. Each document is stored under the document's `name`; importing a document of a name that already exists replaces it.

The command prints each stored document, then a report, then a `stored / refused` tally. It exits non-zero when any file was refused; the other files are still stored.

From Python:

```python
from slayer.osi.importer import import_osi

result = await import_osi(storage=storage, path="./osi_configs", data_source="my_postgres", engine=engine)
result.stored           # names of the stored documents
print(result.render_report())
```

## Document Shapes

**Current documents.** A flat document declaring `version: 0.3.0.dev0` (`version`, `name`, `datasets`, and optionally `description`, `ai_context`, `relationships`, `metrics`, `nonportable_extensions`) is checked for Ossie validity and stored with its text unchanged, comments included.

**Earlier flat documents.** A flat document declaring `1.0`, `0.1.0`, `0.1.1` or `0.2.0.dev0` is upgraded as Ossie's migration guidance prescribes (its `version` set to `0.3.0.dev0`, root-level `dialects` and `vendors` removed), then validated and stored. The report lists each change. Any other version is refused.

**Legacy wrapper documents.** A document whose models sit under `semantic_model` (an array or a single object) is split into one flat document per model. A legacy dataset exposed every source column, so the importer introspects each one and declares every source column no field declares as a field, typed from the database; this upgrade alone needs a reachable datasource. A model-level metric's bare reference that exactly one dataset declares is qualified (`SUM(amount)` becomes `SUM(orders.amount)`); an ambiguous one is stored as written and reported as not evaluable. The report lists each split, added field and rewritten metric.

**Legacy `custom_extensions`.** Entries under `custom_extensions`, whose `data` is a JSON string (SLayer 1.8 exports), are read at every level as `nonportable_extensions` entries with the decoded object, after the entity's own entries. The report holds one entry per document.

## Refused Documents

A document that is not valid Ossie stores nothing and is refused with every reason, each naming its entity:

- YAML or JSON text repeating a mapping key at any depth (each repeat named with its line; repeated `<<` merge directives are allowed), or holding a scalar YAML cannot build, such as `2024-02-30` (these are then the only reasons reported);
- a root mixing the flat and wrapper shapes, or holding neither (an Ossie ontology or mapping document);
- an empty `semantic_model`, or a wrapper declaring `0.3.0.dev0` (whose documents are flat);
- a key Ossie does not define, or an unknown version;
- a duplicate name, compared as Ossie compares identifiers;
- a key or relationship naming an undeclared field, mismatched relationship key lengths, or a relationship naming a dataset outside the document;
- a metric reference that breaks Ossie's metric scoping;
- an expression with neither `sql` nor a `dialects` entry;
- an extension whose `data` is not a JSON object (a list, a scalar, or an object holding a date, a timestamp, `.inf`/`.nan` or a non-string key at any depth);
- a dataset or model-scoped metric name already used by another document or a standalone dataset of the datasource ([one namespace per datasource](../concepts/semantic-models.md#one-namespace-per-datasource)).

## The Import Report

Each report entry names its document and entity:

| Kind | Meaning |
|---|---|
| `upgraded` | an earlier version upgraded to `0.3.0.dev0` |
| `legacy_extensions` | legacy `custom_extensions` read as `nonportable_extensions` |
| `split` | a legacy wrapper model, or a document of a merged export, stored as its own document |
| `field_added` | a legacy dataset's source column declared as a field |
| `qualified` | a legacy model-level metric's bare reference qualified |
| `not_evaluable` | an entity SLayer [cannot evaluate yet](../concepts/semantic-models.md#what-loads-but-cannot-be-evaluated-yet); it is stored and fails only when a query reads it |
| `replaced` | an existing document of the same name replaced |
| `moved_metric` | a 1.x-imported metric anchored on a dataset is now the document's model-scoped metric |
| `unexpressed` | a field of a replaced 1.x dataset that the document no longer declares; its settings were not carried |
| `standalone` | a dataset of a merged export stored as a standalone dataset |

Whether SLayer can evaluate an entity is never checked at import: upstream's TPC-DS example, for instance, is stored whole, with its window-function metrics reported as `not_evaluable`.

## Re-importing a 1.x Import

SLayer 1.x converted each document into standalone datasets and attached each model-level metric to an anchor dataset. Importing the same document again replaces those datasets with the native document:

- each anchored metric becomes the document's model-scoped metric, queried by bare name (`revenue_per_customer`, no longer `orders.revenue_per_customer`), and the report lists the move;
- SLayer-only settings added to the replaced datasets (formats, hidden flags, row filters, access tags, custom aggregations, extra metrics) are carried into `SLAYER` entries of the document;
- dataset names are kept, so saved queries, stages and memories referring to them keep resolving.

Until re-imported, 1.x-converted datasets keep loading unchanged.

## Re-importing an Export

A document written by [`slayer export-osi`](osi_export.md) for a datasource holding several documents and standalone datasets records where each came from. Importing it into an empty datasource splits it back into the same documents and standalone datasets.

## After the Import

The stored document behaves as described in [Semantic models](../concepts/semantic-models.md): its datasets expose exactly their declared fields, a field without a `datatype` is typed from the live column at query time, `ai_context` and other vendors' extensions show in `inspect` and search, and dataset tools edit the document in place. A field reading a column the source lacks is [schema drift](../concepts/schema-drift.md), reported by `slayer validate-datasets`.
