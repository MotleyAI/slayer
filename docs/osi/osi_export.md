# Exporting to OSI (Ossie)

SLayer can write one datasource's models as a current-spec [Ossie](https://github.com/apache/ossie) document (`version: 0.3.0.dev0`, validating against the upstream schema). It's the reverse of [importing OSI configs](osi_import.md), with the same clean-fail contract: everything Ossie can express is exported, and everything else is reported, never silently dropped.

## Quick Start

```bash
slayer export-osi --datasource my_postgres --out shop.osi.yaml
slayer export-osi --datasource my_postgres --models orders customers --out shop.osi.json --no-extensions
```

The file is JSON when its name ends in `.json` and YAML otherwise. `--models` exports only the named models. The command prints an export report. Export reads the stored models through the query engine and sends no query to the database for table-backed or sql-mode models.

From Python:

```python
from slayer.osi.exporter import export_osi

result = await export_osi(engine=engine, datasource="my_postgres")
result.document         # the Ossie document as a dict
print(result.render_report())
```

## What Gets Exported

| SLayer | Ossie |
|---|---|
| the datasource | the document's `name` and `description` (or the semantic model the models were imported from, when they all share one) |
| a table-backed model | a dataset whose `source` is its table |
| a sql-mode model | a dataset whose `source` is its SQL |
| a model with row `filters` on its own columns | a query `source` selecting the rows where every filter holds |
| a query-backed model | a query `source` holding its generated SQL, with its variables at their defaults |
| a column reading only its own model | a field whose expression reads the source's physical columns (other columns of the model expanded) |
| a column with a `filter` | a field `CASE WHEN <filter> THEN <value> END`, masked in every position |
| `primary_key`, `unique`, imported unique keys | `primary_key`, `unique_keys` |
| a join | a relationship from the many side to the one side (a one-to-many join is reversed) |
| a measure whose aggregates all read one model | a metric on that model's dataset |
| a measure whose aggregates read different models | a model-scoped metric with `dataset.field` references |
| `label`, `description`, imported `ai_context` | `label`, `description`, `ai_context` |

Column types map to `datatype` as `String`, `Integer`, `Float` (`Decimal` for an exact-numeric database type), `Boolean`, `Date`, and `DateTime` or `DateTimeTz`. `dimension.is_time` is written only where it differs from Ossie's default for that datatype.

Each aggregate in a metric is evaluated over the rows of the one dataset it reads, which is Ossie's component semantics. So `sum(customers.discount)` on `orders` becomes `SUM(discount)` on `customers` and covers every customer, including customers with no orders. References to other measures are expanded (`revenue / n` becomes `SUM(amount) / COUNT(*)`). Percentiles and medians use the standard `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)` on every database.

## Expressions

A field's or source's expression is written as portable `sql` (`OSSIE_SQL_2026`) only when reading it back reproduces the datasource's own SQL exactly. Otherwise it is written as a single `dialects` entry in the datasource's dialect (`POSTGRES`, `DUCKDB`, `TSQL`, …), holding the SQL verbatim. Metric expressions are always standard portable SQL, except an approximate distinct count, which has no portable form and is written only in the datasource's dialect.

## The SLAYER Extension

Each exported dataset, field, metric and relationship carries one custom extension with `vendor_name: SLAYER`. Its `data` is a versioned JSON payload of what Ossie has no place for:

- join types, names, cardinalities and the model that declares each join
- measure formulas, home models, labels and types
- column types, formats, granularities, hidden flags, allowed aggregations and filters
- model sources (table or SQL plus filters, or saved queries plus variables), access tags, custom aggregations, and SLayer-only columns and measures

SLayer-only columns read joined models. SLayer-only measures are transforms (`change_pct`, `cumsum`, …), `first`/`last`, windowed or partitioned aggregates, aggregates reading several models, and measures whose route to another model is ambiguous. Each payload records a fingerprint of the Ossie content it was written with. `slayer import-osi` restores the SLayer structure only where the document is unedited; where a third party edited it, the Ossie content wins and the import report says so. Other vendors' extensions recorded on imported entities are written back unchanged.

## Without Extensions

`--no-extensions` writes plain Ossie. Every entity that exists only in the extension is reported as not exported. Exported entities that a plain Ossie reader cannot restore exactly, such as a one-to-one, one-to-many or inner join, or a measure exported away from its model, are reported as lossy. A model with access tags is not exported, because re-importing it without its tags would make it public.

## Export Report

Every report entry names its entity, a reason and a workaround. Severity `dropped` means the entity is not exported: a many-to-many join, a join whose cardinality its keys cannot prove, a model filtered on a joined model, or anything needing a model outside `--models`. Severity `info` marks content carried only in the extension (category `extension_only`) or exported lossily (category `lossy`). Connection details, memories and sampled values are never exported.
