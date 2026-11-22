# Exporting to OSI (Ossie)

SLayer writes one datasource as one current-spec [Ossie](https://github.com/apache/ossie) document (`version: 0.3.0.dev0`, validating against the upstream schema): every stored [semantic model](../concepts/semantic-models.md) and every standalone dataset of the datasource. It's the reverse of [importing Ossie documents](osi_import.md). Everything Ossie can express is exported, and everything else is reported, never silently dropped.

## Quick Start

```bash
slayer export-osi --datasource my_postgres --out shop.osi.yaml
slayer export-osi --datasource my_postgres --datasets orders customers --out shop.osi.json --no-extensions
```

The file is JSON when its name ends in `.json` and YAML otherwise. `--datasets` exports only the named datasets. The command prints an export report. Export reads the stored datasets through the query engine and sends no query to the database for table-backed or sql-mode datasets.

From Python:

```python
from slayer.osi.exporter import export_osi

result = await export_osi(engine=engine, datasource="my_postgres")
result.document         # the Ossie document as a dict
result.text             # the document as YAML text
print(result.render_report())
```

## One Document per Datasource

**A single semantic model exports verbatim.** When the datasource holds exactly one semantic model and no standalone dataset, and no `--datasets` subset is given, the output is that document's stored text unchanged, comments included (converted to JSON only for a `.json` file). With `--no-extensions` it is the same text with only its `SLAYER` entries removed; other vendors' entries and every other line are kept.

**Otherwise the export is merged.** The document holds every dataset, relationship and model-scoped metric of the datasource: semantic-model entities as stored, standalone datasets exported by the rules below. Its `name` and `description` come from the datasource. A document-level `SLAYER` entry records each semantic model's `name`, `description`, `ai_context`, extensions and member names, so importing the export into an empty datasource restores each semantic model and each standalone dataset. Datasets are ordered by name, and the rest deterministically, so exporting unchanged content twice gives identical documents.

`slayer ossie-roundtrip <in> <out>` imports one document into scratch storage and exports it again, without touching your storage; a valid document comes back unchanged.

## Standalone Datasets

| SLayer | Ossie |
|---|---|
| the datasource | the document's `name` and `description` (or the semantic model the datasets were imported from, when they all share one) |
| a table-backed dataset | a dataset whose `source` is its table |
| a sql-mode dataset | a dataset whose `source` is its SQL |
| a dataset with row `filters` on its own fields | a query `source` selecting the rows where every filter holds |
| a query-backed dataset | a query `source` holding its generated SQL, with its variables at their defaults |
| a field reading only its own dataset | a field whose expression reads the source's physical columns (other fields of the dataset expanded) |
| a field with a `filter` | a field `CASE WHEN <filter> THEN <value> END`, masked in every position |
| `primary_key`, `unique`, imported unique keys | `primary_key`, `unique_keys` |
| a relationship | a relationship from the many side to the one side (a one-to-many relationship is reversed) |
| a metric whose aggregates all read one dataset | a metric on that dataset |
| a metric whose aggregates read different datasets | a model-scoped metric with `dataset.field` references, and `AGG(dataset.metric)` for each metric it uses |
| `label`, `description`, imported `ai_context` | `label`, `description`, `ai_context` |

Field types map to `datatype` as `String`, `Integer`, `Float` (`Decimal` for an exact-numeric database type), `Boolean`, `Date`, and `DateTime` or `DateTimeTz`. `dimension.is_time` is written only where it differs from Ossie's default for that datatype.

Each aggregate in a metric is evaluated over the rows of the one dataset it reads, which is Ossie's component semantics. So `sum(customers.discount)` on `orders` becomes `SUM(discount)` on `customers` and covers every customer, including customers with no orders. In a dataset-scoped metric, references to other metrics are expanded (`revenue / n` becomes `SUM(amount) / COUNT(*)`); in a model-scoped one, each becomes `AGG` of the referenced metric's exported dataset metric (`revenue / count_distinct(customers.id)` becomes `AGG(orders.revenue) / COUNT(DISTINCT customers.id)`). A stored model-scoped metric exports unchanged; one Ossie does not define (it reads another model-scoped metric) travels only in the `SLAYER` entry. Percentiles and medians use the standard `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)` on every database.

## Expressions

Each form of a field's expression exports as itself: its portable `sql` as `sql`, each `dialects` entry with its text, and an entry for the datasource's dialect (or a field `sql` not yet converted) under that dialect's token (`POSTGRES`, `DUCKDB`, `TSQL`, …). A path to another field of the dataset is replaced by that field's definition; the result stays portable `sql` only when every replaced part is portable, else it becomes the datasource's entry. Datasource SQL is never written as portable `sql`. A source's expression is written in the datasource's dialect. Metric expressions are always standard portable SQL, except an approximate distinct count, which has no portable form and is written only in the datasource's dialect.

## The SLAYER Extension

Each exported dataset, field, metric and relationship carries one `nonportable_extensions` entry with `vendor_name: SLAYER`. Its `data` is a versioned payload object (version 3, a field's definition under `expression`; versions 1 and 2 still import) of what Ossie has no place for:

- join types, names, cardinalities and the dataset that declares each relationship
- metric expressions, home datasets, labels and types
- field types, formats, granularities, hidden flags, allowed aggregations and filters
- dataset sources (table or SQL plus filters, or saved queries plus variables), access tags, custom aggregations, and SLayer-only fields and metrics

SLayer-only fields read joined datasets. A comparison with an instant (`ts <= '2024-04-10'`, a date being its midnight) exports as written. SLayer-only metrics are transforms (`change_pct`, `cumsum`, …), comparisons with a period or relative token (`ts in '2025-Q1'`, `ts >= 'last month'`), `first`/`last`, windowed or partitioned aggregates, aggregates reading several datasets, and metrics whose route to another dataset is ambiguous. Each payload records a fingerprint of the Ossie content it was written with. On every load SLayer applies the payload only where the Ossie content is unedited; where a third party edited it, the Ossie content wins and `inspect` shows a load warning on the entity. Other vendors' extensions recorded on imported entities and documents are written back unchanged, with or without `--no-extensions`; one whose `data` is not a JSON object is reported and omitted. A YAML export quotes every string a YAML 1.1 or 1.2 loader would read as a number, boolean, null or date.

## Without Extensions

`--no-extensions` writes plain Ossie. Every entity that exists only in the extension is reported as not exported. Exported entities that a plain Ossie reader cannot restore exactly, such as a one-to-one, one-to-many or inner-join relationship, or a metric exported away from its dataset, are reported as lossy. A dataset with access tags is not exported, because re-importing it without its tags would make it public.

## Export Report

Every report entry names its entity, a reason and a workaround. Severity `dropped` means the entity is not exported: a many-to-many relationship, a relationship whose cardinality its keys cannot prove, a dataset filtered on a joined dataset, or anything needing a dataset outside `--datasets`. Severity `info` marks content carried only in the extension (category `extension_only`) or exported lossily (category `lossy`). Connection details, memories and sampled values are never exported.
