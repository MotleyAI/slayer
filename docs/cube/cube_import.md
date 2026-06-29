# Importing Cube definitions

SLayer can import [Cube](https://cube.dev) (Cube.js / Cube.dev) YAML data models —
cubes and views — and convert them to SLayer models. The conversion is **fully
offline**: data types come from Cube's declared dimension / measure types, so no
database connection is required. Everything that can't be mapped cleanly is
captured in a structured JSON report rather than silently dropped.

## Quick start

```bash
slayer import-cube ./cube_project --datasource my_postgres --storage ./slayer_data
```

This recursively reads every `.yml`/`.yaml` file under the path, extracts
`cubes:` and `views:`, writes SLayer model files to the storage directory, and
writes `cube_import_report.json` next to it.

`--datasource` is just the SLayer datasource name to file the models under — it
does not need to exist or be reachable. After importing, run `slayer ingest`
against a live connection to profile sample values and refine numeric types.

## What gets converted

### Cubes → models

Each cube becomes one `SlayerModel` anchored on its `sql_table` (or `sql`).

| Cube | SLayer |
|------|--------|
| `name` | `name` |
| `sql_table` / `sql` | `sql_table` / `sql` (with `{CUBE}`/`{member}` refs translated) |
| `description` | `description` |
| `public: false` | `hidden: true` |
| `meta` (incl. `ai_context`) | `meta` |
| `title` | `meta.cube_title` |

### Measures → columns + measures

Cube bakes the aggregation into each measure; SLayer separates the row-level
expression (a `Column`) from the named aggregation (a `ModelMeasure`).

```yaml
# Cube
measures:
  - { name: total_revenue, type: sum, sql: "{CUBE}.amount" }
# SLayer
columns:
  - { name: amount, type: DOUBLE }
measures:
  - { name: total_revenue, formula: "amount:sum" }
```

- `count` with no `sql` → `*:count`; `count_distinct_approx` → `count_distinct`.
- Conditional `filters:` become a `CASE WHEN` on the column's `filter`. Two
  measures over the same expression but different filters get distinct columns.
- A finite trailing `rolling_window` becomes a windowed aggregation
  (`amount:sum(window='30d')`).
- Calculated measures (`type: number/string/time/boolean`) referencing other
  measures become a `ModelMeasure` formula (`{revenue} / {count}` → `revenue / count`).
- `format` maps to `NumberFormat` (`percent`, `currency`, `number`).

### Dimensions → columns

`string`→`TEXT`, `number`→`DOUBLE`, `boolean`→`BOOLEAN`, `time`→`TIMESTAMP`.
`primary_key: true` carries over. A `case:` dimension becomes a `CASE WHEN`
column.

### Joins

A join's ON clause (`{CUBE}.customer_id = {customers.id}`) becomes
`join_pairs`; member references resolve to their physical columns. Composite
(`AND`-joined) keys are supported. All joins emit as `LEFT`.

### Segments → boolean columns

Each segment becomes a boolean column carrying the predicate, so it stays
filterable (`completed = true`) and group-able.

### Views → facade models

A Cube view (which owns no table) becomes a thin model anchored on its
`join_path` root cube: included dimensions become derived columns
(`sql: "customers.name"`), included measures become local or cross-model
`ModelMeasure`s (`customers.ltv:sum`), `prefix: true` prepends the cube name,
and `default_filters` become model filters.

### `extends`

Cube inheritance is **flattened** at import time — a child inherits the parent's
members (child wins on conflicts), and abstract bases (`public: false`) are
emitted as hidden models.

## What does not map (reported)

These are recorded in `cube_import_report.json` and, where useful, preserved
under `meta.cube_unmapped.<feature>`:

- Caching / infra: `pre_aggregations`, `refresh_key`, `calendar`, `sql_alias`.
- Presentation: `hierarchies`, `drill_members`, folders, dimension `links`/`order`.
- Security: `access_policy`.
- No SLayer equivalent: `geo` dimensions, `sub_query` dimensions, custom
  `granularities`, per-cube `data_source`.
- Non-equality / non-column join ON clauses (the join is dropped).
- Files or members using Jinja templating (`{{ }}` / `{% %}`) — skipped, since
  conversion is offline and does not render templates.

### Tesseract features (deferred)

Features that require the Tesseract SQL planner — `switch` dimensions,
`number_agg` measures, `case` measures, and the measure `filter` grain control —
have no clean SLayer mapping yet and are reported as `deferred_stage2`. The
cube's other members still convert.

## The report

`CubeConversionResult` carries the emitted `models` and a `CubeConversionReport`
of categorized issues (each with a category, severity, the owning cube/view/member,
a message, and the raw Cube fragment when useful). The CLI always writes it to
`cube_import_report.json` (override with `--report PATH`) and prints a summary
grouped by severity.

## CLI reference

```text
slayer import-cube <cube_project_path> [options]

Arguments:
  cube_project_path     Path to the Cube project (or its model directory)

Options:
  --datasource NAME     SLayer datasource name for the imported models (required)
  --storage PATH        Storage directory / .db file (default: platform path)
  --report PATH         JSON report path (default: <storage>/cube_import_report.json)
  --include-hidden      Also print hidden (public: false) models in the summary
```
