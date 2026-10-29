# Importing OSI (Open Semantic Interchange) Configs

SLayer can import [Ossie / Open Semantic Interchange](https://github.com/open-semantic-interchange/OSI) (OSI) semantic-model documents and convert them into SLayer models. OSI is a vendor-neutral YAML/JSON standard for datasets, relationships, and metrics.

## Quick Start

```bash
slayer import-osi ./osi_configs --datasource my_postgres --storage ./slayer_data
```

This reads every `.yaml`/`.yml`/`.json` file in the path (a single file or a directory), converts each OSI dataset into a SLayer model, and saves the models to storage. A **reachable datasource is required**: column types come from live introspection of `--datasource`, and OSI's semantic metadata is overlaid on top.

## Document Shapes

A current-spec document holds one semantic model at its root (`version`, `name`, `datasets`, and optionally `description`, `ai_context`, `relationships`, `metrics`, `custom_extensions`). An older document wrapping its models in `semantic_model` (an array or a single object) still imports, each wrapped model as its own semantic model. A file that mixes both shapes, holds neither, or has an empty `semantic_model` is skipped with a warning; the other files still import.

Spec versions `1.0`, `0.1.0`, `0.1.1` and `0.2.0.dev0` parse silently; an unknown version is warned about but still parsed.

## What Gets Converted

| OSI construct | SLayer target |
|---|---|
| `datasets[]` | one `SlayerModel` each (name = dataset name) |
| dataset `source` (`db.schema.table`) | `sql_table` + `schema`; a query source → `sql` mode |
| dataset `fields[]` | the model's columns (see [Fields](#fields)) |
| dataset `primary_key` | `Column.primary_key = true` |
| `relationships[]` (`from` → `to`) | a LEFT `ModelJoin` on the `from` model, [`cardinality`](../concepts/models.md#join-cardinality) `many_to_one` (`from` = many, `to` = one) |
| `datasets[].metrics[]` | a `ModelMeasure` on that dataset's model |
| `metrics[]` | a `ModelMeasure` on an anchor model (see [Metrics](#metrics)) |
| `ai_context` (instructions + synonyms) | entity `description` + `meta["osi_ai_context"]` |
| `unique_keys` / `custom_extensions` | model/column `meta` |

### Fields

A current-spec model exposes **exactly its declared fields**: a field renaming a source column (`order_id: o_orderkey`) hides `o_orderkey`, and source columns no field declares are not on the model. Field expressions may still read any source column, and every column they read must exist in the live source. A dataset with no fields imports with no visible columns and a warning. Older wrapped documents keep every introspected column and overlay their fields on top. A field reading a source column whose name another field holds, or its own name (`status: UPPER(status)`), is dropped with a report (an older wrapped document keeps the source column).

A column's type is the type the database reports; a field's declared `datatype` is used only when the database cannot report one, then the type of a source column the field shares its name with (DOUBLE when none exists), and a disagreement adds an informational report entry. This deviates from the spec's consumer guidance on purpose, since `Column.type` drives SQL casts against the real database. The declared value is kept in `meta["osi_datatype"]`.

| `datatype` | SLayer type |
|---|---|
| `String` | TEXT |
| `Integer` | INT |
| `Decimal`, `Float` | DOUBLE |
| `Boolean` | BOOLEAN |
| `Date` | DATE |
| `DateTime`, `DateTimeTz` | TIMESTAMP |
| `Time`, `Opaque` | UNKNOWN |

A field is a time dimension when `dimension.is_time` is `true`, or when `is_time` is unset and its `datatype` is `Date`, `Time`, `DateTime` or `DateTimeTz` (an explicit `is_time: false` wins). The role never changes the column's type: it is recorded as `meta["osi_is_time"]`, and only a DATE or TIMESTAMP time-dimension field can become the model's `default_time_dimension`.

### Keys and Relationships

In a current-spec document, every name in `primary_key`, `unique_keys`, `from_columns` and `to_columns` must be a declared field of its dataset; anything else is reported and not applied. A relationship keyed on a computed field is reported as not representable, and a relationship resolves its datasets only within its own document. A valid `primary_key` replaces the introspected one; without one, a field renaming an introspected key column keeps the key flag. An empty `primary_key` or `unique_keys` entry is reported and ignored (an empty `primary_key` leaves the model without a key).

### Metrics

An OSI metric holds a raw SQL aggregation, which SLayer converts into a formula:

- `SUM(amount)` → `sum(amount)`, `COUNT(*)` → `count(*)`, `COUNT(DISTINCT id)` → `count_distinct(id)`
- arithmetic, constants and allowlisted scalar functions pass through: `SUM(a) / NULLIF(COUNT(*), 0)` → `sum(a) / nullif(count(*), 0)`
- `PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY x)` → `percentile(x, p=0.9)` (`0.5` → `median(x)`)
- an aggregate operand is written inline when the formula language expresses it — `SUM(quantity * amount)` → `sum(quantity * amount)`, `CASE`/`IFF`, comparisons, `AND`/`OR`/`NOT`, `IS NULL` — and otherwise becomes a hidden helper column on its single owning dataset, typed from the database
- `agg(x) FILTER (WHERE p)` → `agg(CASE WHEN p THEN x END)` (`COUNT(*)` counts `CASE WHEN p THEN 1 END`); the predicate must be row-level and read only the aggregate's dataset, and groups are never removed
- `ARRAY_AGG(DISTINCT x)` → `collect(x)`, `ARRAY_CONTAINS(a, v)` and `v = ANY(a)` → `contains(a, v)`, `ARRAY_LENGTH(a)` and `CARDINALITY(a)` → `length(a)` (in a field, stored as SLayer's own SQL for the datasource), with SLayer's NULL conventions: `collect` drops NULL inputs and gives `[]` for none, `contains` is NULL for a NULL list or value and FALSE when the value is absent, and `length` counts non-NULL elements
- a string literal keeps its `'` (written `''`) and backslash characters

`SUM`/`AVG`/`MIN`/`MAX` over `DISTINCT`, any aggregate but `COUNT(x)` over a list field (`MAX(tags)`, `COUNT(DISTINCT tags)`), window functions, and a `CASE` outside any aggregate are reported as unconverted.

A **dataset-scoped** metric references its own dataset's fields unqualified (`SUM(amount)`). A **model-scoped** metric references fields as `dataset.field` (`SUM(orders.amount) / COUNT(DISTINCT customers.id)`) and is attached to an **anchor** model: among the models that reach every referenced dataset over the relationship joins, preferably one that never crosses a join in its fan-out direction (usually the fact table), then the fewest total hops, a referenced dataset, and the smallest name; an operand reading several datasets must reach them all over to-one joins. Unquoted names match declared fields case-insensitively, quoted names exactly, and a name matching several fields is reported as ambiguous. Older wrapped documents may also reference columns unqualified when exactly one dataset has them. A metric's `datatype` is kept in `meta["osi_datatype"]` and never changes the result type.

## Dialect Selection

Each expression is read from exactly one candidate, the first available in this order: the `--dialect` override; the top-level `sql`; an `OSSIE_SQL_2026` entry; an `ANSI_SQL` entry; an entry in the datasource's own dialect (`POSTGRES`, `DUCKDB`, `TSQL`, …); any other SQL dialect SLayer can read (with an informational report entry). Within one step the first entry wins. An expression with only non-SQL dialects (`MDX`, `DAX`, `TABLEAU`, …) is reported, never raised.

`OSSIE_SQL_2026` and `ANSI_SQL` are read with Ossie's portable function set (`IFF`, `ZEROIFNULL`, `DATEADD`, `STARTSWITH`, `||`, …), and every stored column SQL is rendered in the datasource's dialect (for example `||` becomes `CONCAT` on MySQL). An expression already in the datasource's own dialect is stored verbatim.

## Clean-Fail Report

Anything that cannot be expressed exactly is reported (never silently dropped): each entry lists the entity, a reason, and any workaround. Examples are an unreadable or unparseable expression, a field reading a column the source lacks, a relationship with mismatched key lengths or an undeclared key, a dataset whose source cannot be introspected, an illegal name (containing `.`/`:`), a metric whose datasets no join path connects, and an orphan `COUNT(*)` (a column-less model-scoped metric with no unique fact table, so its grain is ambiguous). The CLI prints a grouped report and a `models / unconverted / dropped` tally at the end.
