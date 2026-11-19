# Importing OSI (Open Semantic Interchange) Configs

SLayer can import [Ossie / Open Semantic Interchange](https://github.com/apache/ossie) (OSI) semantic-model documents and convert them into SLayer datasets. OSI is a vendor-neutral YAML/JSON standard for datasets, relationships, and metrics.

## Quick Start

```bash
slayer import-osi ./osi_configs --datasource my_postgres --storage ./slayer_data
```

This reads every `.yaml`/`.yml`/`.json` file in the path (a single file or a directory), converts each OSI dataset into a SLayer dataset, and saves the datasets to storage. A **reachable datasource is required**: column types come from live introspection of `--datasource`, and OSI's semantic metadata is overlaid on top.

## Document Shapes

A current-spec document holds one semantic model at its root (`version`, `name`, `datasets`, and optionally `description`, `ai_context`, `relationships`, `metrics`, `nonportable_extensions`). An older document wrapping its models in `semantic_model` (an array or a single object) still imports, each wrapped model as its own semantic model. A file that mixes both shapes, holds neither (such as an Ossie ontology or ontology-mapping document), has an empty `semantic_model`, or wraps its models while declaring `0.3.0.dev0` (whose documents are flat only) is skipped with a warning; the other files still import.

Spec versions `1.0`, `0.1.0`, `0.1.1`, `0.2.0.dev0` and `0.3.0.dev0` parse silently; an unknown version is warned about but still parsed.

## What Gets Converted

| OSI construct | SLayer target |
|---|---|
| `datasets[]` | one `SlayerDataset` each (name = dataset name) |
| dataset `source` (`db.schema.table`) | `sql_table` + `schema`; a query source → `sql` mode |
| dataset `fields[]` | the dataset's fields (see [Fields](#fields)) |
| dataset `primary_key` | `DatasetField.primary_key = true` |
| `relationships[]` (`from` → `to`) | a LEFT `DatasetRelationship` on the `from` dataset, [`cardinality`](../concepts/datasets.md#relationship-cardinality) `many_to_one` (`from` = many, `to` = one) |
| `datasets[].metrics[]` | a `DatasetMetric` on that dataset |
| `metrics[]` | a `DatasetMetric` on an anchor dataset (see [Metrics](#metrics)) |
| `ai_context` (instructions + synonyms) | entity `description` + `meta["osi_ai_context"]` |
| `unique_keys` / `nonportable_extensions` | dataset/field `meta`; a single-field unique key also sets `DatasetField.unique` |

A `SLAYER` non-portable extension written by [`slayer export-osi`](osi_export.md) restores the SLayer structure it carries (join types and directions, metric expressions, field filters, dataset sources, SLayer-only fields and metrics) wherever the document is unedited since export, and otherwise the Ossie content is imported and the report names the entity.

An extension entry whose `data` is not a JSON object (a list, a scalar, or an object holding a date, a timestamp, `.inf`/`.nan` or a non-string key) is dropped and reported; the rest of the entity imports. The legacy key `custom_extensions`, whose `data` is a JSON string (documents exported by SLayer 1.8), is still read at every level, with one informational report entry per document suggesting a re-export.

### Fields

A current-spec dataset exposes **exactly its declared fields**: a field renaming a source column (`order_id: o_orderkey`) hides `o_orderkey`, and source columns no field declares are not on the dataset. Field expressions may still read any source column, and every column they read must exist in the live source. A dataset with no fields imports with no visible fields and a warning. Older wrapped documents keep every introspected column and overlay their fields on top. When a dataset's query source outputs a label more than once (`SELECT o.id, c.id, …`), every field reading that label is dropped with a report as ambiguous. A field expression reads source columns whatever fields the dataset declares (`status: UPPER(status)` upper-cases the source `status`).

A field's declared `datatype` is kept as written, whatever the database reports, and a disagreement adds an informational report entry; a field without one takes the type the database reports (`Float` when it reports none). SLayer's datatypes are Ossie's names, so a metric's declared `datatype` is kept as written too.

A field is a time dimension when `dimension.is_time` is `true`, or when `is_time` is unset and its `datatype` is `Date`, `Time`, `DateTime` or `DateTimeTz` (an explicit `is_time: false` wins). The role never changes the field's type: it is recorded as `meta["osi_is_time"]`, and only a `Date`, `DateTime` or `DateTimeTz` time-dimension field can become the dataset's `default_time_dimension`.

### Keys and Relationships

In a current-spec document, every name in `primary_key`, `unique_keys`, `from_columns` and `to_columns` must be a declared field of its dataset; anything else is reported and not applied. A relationship keyed on a computed field is reported as not representable, and a relationship resolves its datasets only within its own document. A valid `primary_key` replaces the introspected one; without one, a field renaming an introspected key column keeps the key flag. An empty `primary_key` or `unique_keys` entry is reported and ignored (an empty `primary_key` leaves the dataset without a key).

### Metrics

An OSI metric holds a raw SQL aggregation, which SLayer converts into an expression:

- `SUM(amount)` → `sum(amount)`, `COUNT(*)` → `count(*)`, `COUNT(DISTINCT id)` → `count_distinct(id)`
- arithmetic, constants and allowlisted scalar functions pass through: `SUM(a) / NULLIF(COUNT(*), 0)` → `sum(a) / nullif(count(*), 0)`
- `PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY x)` → `percentile(x, p=0.9)` (`0.5` → `median(x)`); `PERCENTILE_DISC`, and a percentile the target dialect cannot compute, is reported as unconverted
- `STDDEV_SAMP`, `STDDEV_POP`, `VAR_SAMP`, `VAR_POP(x)` → `stddev_samp(x)` etc., `CORR`, `COVAR_SAMP`, `COVAR_POP(x, y)` → `corr(x, other=y)` etc., and `APPROX_COUNT_DISTINCT(x)` → `count_distinct_approx(x)`
- an aggregate operand is written inline when the expression language expresses it — `SUM(quantity * amount)` → `sum(quantity * amount)`, `CASE`/`IFF`, comparisons, `AND`/`OR`/`NOT`, `IS NULL` — and otherwise becomes a hidden helper field on its single owning dataset, typed from the database
- `agg(x) FILTER (WHERE p)` → `agg(CASE WHEN p THEN x END)`; a date string compared in `p` means what Ossie says, so `created_at <= '2024-04-10'` stops at its midnight (`COUNT(*)` counts `CASE WHEN p THEN 1 END`); the predicate must be row-level and read only the aggregate's dataset, and groups are never removed
- `ARRAY_AGG(DISTINCT x)` → `collect(x)`, `ARRAY_CONTAINS(a, v)` and `v = ANY(a)` → `contains(a, v)`, `ARRAY_LENGTH(a)` and `CARDINALITY(a)` → `length(a)` (in a field, stored as SLayer's own SQL for the datasource), with SLayer's NULL conventions: `collect` drops NULL inputs and gives `[]` for none, `contains` is NULL for a NULL list or value and FALSE when the value is absent, and `length` counts non-NULL elements
- a string literal keeps its `'` (written `''`) and backslash characters

`SUM`/`AVG`/`MIN`/`MAX` over `DISTINCT`, any aggregate but `COUNT(x)` over a list field (`MAX(tags)`, `COUNT(DISTINCT tags)`), window functions, and a `CASE` outside any aggregate are reported as unconverted.

A **dataset-scoped** metric references its own dataset's fields unqualified (`SUM(amount)`). A **model-scoped** metric references fields as `dataset.field` (`SUM(orders.amount) / COUNT(DISTINCT customers.id)`) and is attached to an **anchor** dataset: among the datasets that reach every referenced dataset over the relationships, preferably one that never crosses a relationship in its fan-out direction (usually the fact table), then the fewest total hops, a referenced dataset, and the smallest name; an operand reading several datasets must reach them all over to-one relationships. A dataset-scoped metric reads a column of its own source directly as `source.column` (`SUM(source.quantity)`, `source` matched case-insensitively): it reads a field passing that column through unchanged, else a hidden helper field, and is reported as unconverted when a field named `source` exists, the column is missing or ambiguous, or a field holding its name does not read it unchanged; older wrapped documents and model-scoped metrics do not read `source.column`. Unquoted names match declared fields case-insensitively and quoted names exactly; a dataset whose field and dataset-metric names repeat, in any case, is refused, since Ossie requires them unique. Older wrapped documents may also reference columns unqualified when exactly one dataset has them. A metric's `datatype` is kept in `meta["osi_datatype"]` and never changes the result type.

## Expression Forms

A field's expression is stored as written: its portable `sql` and every `dialects` entry. On the datasource, the entry for its own dialect (`POSTGRES`, `DUCKDB`, `TSQL`, …) runs, else the portable `sql`, else an `OSSIE_SQL_2026` entry; an entry for the datasource's dialect is stored verbatim, apart from its unquoted names, which every expression and dataset `source` stores as the datasource's database folds them (`OrderAmount` → `orderamount` on Postgres, Redshift, Snowflake and Oracle; a column qualified by another dataset's name is kept as written).

When no form runs — no entry for the datasource's dialect, and no portable `sql` or `OSSIE_SQL_2026` entry SLayer reads (SLayer does not read `IFF` or `ZEROIFNULL` yet, for example) — the importer translates one candidate into an entry for the datasource's dialect, with an informational report entry naming the dialect it read. It takes the first that exists in this order: the `--dialect` override; the top-level `sql`; an `OSSIE_SQL_2026` entry; an `ANSI_SQL` entry; any other SQL dialect SLayer can read; within one step the first entry wins. `OSSIE_SQL_2026` and `ANSI_SQL` are read with Ossie's portable function set (`IFF`, `ZEROIFNULL`, `DATEADD`, `STARTSWITH`, `||`, …). An expression with only non-SQL dialects (`MDX`, `DAX`, `TABLEAU`, …) is reported, never raised.

A metric runs only its portable form: its entry for the datasource's dialect is dropped with a report when it also has `sql`, and translated otherwise; entries for other dialects are kept.

## Clean-Fail Report

Anything that cannot be expressed exactly is reported (never silently dropped): each entry lists the entity, a reason, and any workaround. Examples are an unreadable or unparseable expression, a field reading a column the source lacks, a relationship with mismatched key lengths or an undeclared key, a dataset whose source cannot be introspected, an illegal name (containing `.`/`:`), a metric whose datasets no relationship path connects, and an orphan `COUNT(*)` (a column-less model-scoped metric with no unique fact table, so its grain is ambiguous). The CLI prints a grouped report and a `datasets / unconverted / dropped` tally at the end.
