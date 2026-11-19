## Why

In a field's `sql`, a bare `x` reads the field `x` whenever one exists, so once a derived field holds a source column's name, nothing in a dataset can read that column. Cube, dbt and Ossie definitions such as `status: UPPER(status)` therefore fail with a cycle, and DEV-2171 had to drop every construct reading a column another field has taken. Ossie, which SLayer 2.0 must accept with its meaning, defines fields by an `expression` object in which a bare name always reads the source column.

## What Changes

- **BREAKING** A field has one definition key, `expression`: Ossie's `{sql, dialects}` object, or a string shorthand for `{sql: …}`. Field `sql` is a deprecated spelling, converted on load and on fresh input, removed in 3.0.
- **BREAKING** In a field's `expression`, its `filter` and a dataset's `filters`, a bare name or `source.x` reads the dataset's source column, and a dotted path from the dataset (`orders.status`, `customers.region`) reads a field. Stored definitions are converted so their results do not change.
- The entry for the datasource's own dialect runs when present; portable `sql` is the fallback and is read by Mode B under Ossie's rules. Entries for other dialects are kept and never run. A field with no form it can run is refused at save.
- **BREAKING** In a metric's `expression`, `source.x` reads a source column unless the dataset declares a field named `source`. Stored definitions that used `source` as a path are migrated, or become unloadable.
- A metric's `expression` takes the same object; only its portable `sql` is evaluated, and an entry for the datasource's dialect is refused at save.
- The Cube, dbt and Ossie importers read a physical column whose name a field has taken with the source's meaning: DEV-2171's collision drops and self-named drops are lifted. Ossie expression objects are stored verbatim.
- Ingestion writes `expression`; the Ossie exporter writes stored expressions as they are and inlines field references. The Ossie `SLAYER` payload moves to version 3, which carries a field's definition under `expression`; versions 1 and 2 still import.
- `architecture/semantics.arc42.md` §4 P1–P3 state the row-definition name rule.

## Capabilities

### New Capabilities
- `models/field-expressions`: the expression object and its string shorthand, which form runs on a datasource, the name rule inside definitions, metric expressions, refusals, and the conversion of the deprecated field `sql`.

### Modified Capabilities
- `models/column-definitions`: derived definitions are expressions; the one-expression rule applies to native dialect entries.
- `models/column-filters`: a filter wraps the field's running expression, and its bare names read source columns.
- `models/model-filters`: bare names in dataset filters read source columns.
- `models/join-keys`: the physical spelling comes from the physical-reader predicate.
- `models/stored-upgrade`: stored field `sql` and `source.` paths are converted.
- `importers/meaning-preservation`: physical references read the physical column with no drop; self-named transforms import; one helper per physical column.
- `importers/cube-import`, `importers/dbt-import`: masked and self-named constructs import with the source's values.
- `importers/osi-import`: expression objects stored verbatim; metric dialect entries and `source.x`.
- `importers/osi-export`: stored expressions exported as they are, field references inlined.
- `compatibility/removal-tripwire`: field `sql` joins the covered deprecated forms.
- `compatibility/ossie-vocabulary`: a field's definition is `expression` (its `sql` no longer a kept key); the `SLAYER` payload moves to version 3.

## Impact

- **Code:** `slayer/core/models.py` (the field model, expression object, conversion), `slayer/core/syntax` binding of source columns, `slayer/sql/column_expansion.py` and `slayer/engine/column_dependency.py` (one name rule, one dependency graph), `slayer/engine/binding.py`, `slayer/storage/migrations.py` and the load pipeline in `slayer/storage/base.py`, `slayer/importers/physical.py`, `slayer/cube/`, `slayer/dbt/`, `slayer/osi/` (converter, dialects, exporter), `slayer/live_schema/` (ingestion, drift, type refinement), MCP and REST field descriptions.
- **Stored data:** one `SlayerModel` version step after DEV-1607's; documents whose datasource config is absent keep the deprecated `sql` until it is known.
- **Tests:** about 2,840 test field definitions respelled; 33 strict xfails start passing; six existing tests aligned with the owner's approval.
- **Docs:** concepts (models, formulas, references), the Cube, dbt and Ossie pages, `docs/upgrading/2.0.md`, help memories.
- **Depends on:** DEV-2318 (vocabulary names, on `main`) and DEV-1607 (Ossie shapes, merged into this branch, not yet on `main`); DEV-2295 and DEV-2292 (living-architecture `dr-refactor`) for the keyword respelling of the suite.
- **Hands off:** delimited Ossie names to DEV-2244 item 7.
