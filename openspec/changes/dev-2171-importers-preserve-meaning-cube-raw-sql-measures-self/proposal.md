## Why

The Cube, dbt and OSI importers copy the source's physical column names into SLayer SQL as bare names, and SLayer resolves a bare name to a logical column first. Imported measures and dimensions therefore read a different column than the source meant (a Cube `SUM(CASE WHEN {CUBE}.status = 'paid' …)` returns 1 where Cube gives 2), self-named transforms fail at save, and list columns are typed `DOUBLE`, so `validate_models` proposes deleting working measures. Principle 20 (importers preserve meaning) is the release bar for the DEV-2101 / DEV-2137 stack.

## What Changes

- A shared physical-reference resolver rewrites every physical column reference an importer emits to a logical column reading that physical column; a reference whose name a different column holds drops its construct with a report (reading it is DEV-2175). Used by Cube (dimensions, CASE predicates, measure SQL and filters, segments, calc-measure operands, join keys), dbt (dimension, measure and entity `expr`s) and OSI (field expressions).
- A self-named transform (`status: UPPER(status)`) is dropped with a report instead of failing the whole model's save (importing it is DEV-2175); a filtered column never takes a physical column's name.
- Cube calculated measures containing raw SQL aggregates are translated by the shared aggregate translator (moved from the OSI importer): bare operands aggregate their column, other operands become hidden verbatim Mode-A columns, `FILTER` becomes the helper's filter; untranslatable measures are dropped with a report.
- Cube `{<own cube>.member}` and `{CUBE.member}` resolve like `{member}`; Cube view facades carry the hidden columns their SQL reads.
- Import-time measure validation looks inside aggregations: an aggregation reading no model column or join path drops its measure with a report.
- Cube and dbt type columns from the database when the datasource is reachable, then from declared types (dbt `data_type` for every type, lists included; Cube dimension types), then from the aggregation; an untyped `min` / `max` source is dropped with a report; an offline import reports that types were not checked. Aggregations SLayer refuses over a list are dropped with a report.
- OSI: list aggregation inputs other than `COUNT` are refused; `ARRAY_AGG(DISTINCT x)`, `ARRAY_CONTAINS` / `= ANY`, `ARRAY_LENGTH` / `CARDINALITY` map to `collect`, `contains`, `length` (in metric formulas, and rendered through SLayer's dialect list renderer in field SQL); string literals may contain `'` and backslash.
- Both import CLIs resolve `--datasource` and connect best-effort, independent of other flags.
- Docs: `docs/cube/cube_import.md`, `docs/dbt/dbt_import.md`, `docs/osi/osi_import.md`.

## Capabilities

### New Capabilities
- `importers/meaning-preservation`: rules every importer obeys — physical references resolve to the logical column reading them, self-named transforms, filtered helpers, identifier matching, typing precedence and refusals, offline reporting.
- `importers/cube-import`: Cube-specific translation — reference forms, calculated measures with raw SQL aggregates, same-cube dotted references, join keys, FILTER_PARAMS bodies, view facades.
- `importers/dbt-import`: dbt-specific translation — `data_type` mapping (lists included) and the measure-source columns.

### Modified Capabilities
- `importers/osi-import`: aggregate operands (list aggregation refusals, array-function mappings, quoted string literals) and field expressions (list functions rendered for the target).

## Impact

- Code: new package `slayer/importers/` (resolver, aggregate translator, typing helper) claimed in `architecture/model.c4`; `slayer/cube/*`, `slayer/dbt/*`, `slayer/osi/*`; `slayer/core/syntax.py` (`measure_dependency_problems` gains column and join-path context); `slayer/cli.py` (datasource resolution for `import-cube` / `import-dbt`).
- Behaviour: imported models change shape (`<measure>_col` filtered helpers, list types); some previously imported constructs are now dropped with a report (collisions and self-named transforms, untyped `min` / `max` offline, list aggregations SLayer refuses, untranslatable Cube calc measures).
- arc42: principle 20 flips to `[enforced: test:tests/test_dev2171_importers_preserve_meaning.py]`.
- Closes DEV-1363.
