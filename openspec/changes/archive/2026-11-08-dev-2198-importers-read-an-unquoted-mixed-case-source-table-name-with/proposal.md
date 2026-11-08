## Why

Importers copy the source tool's SQL into SLayer's Mode A text verbatim. The source tool's database folds an
unquoted identifier, while SLayer reads an unquoted identifier holding an upper-case letter as an exact,
case-preserved name: on Postgres a Cube `sql_table: Sales.OrderItems` or `{CUBE}.OrderAmount` means
`sales.orderitems` / `orderamount` to Cube but `"Sales"."OrderItems"` / `"OrderAmount"` to SLayer — another table or
column, or none (system §3.21). The same import boundary also stores live table names by string concatenation, so a
name holding a space or a dot cannot be queried after ingest, and resolves dbt `source()` to its group and table
names instead of the declared relation.

## What Changes

- Each importer (Cube, dbt, OSI) folds every unquoted identifier of the source's physical SQL — table names, source
  queries, column / member / field expressions, join SQL, FILTER_PARAMS bodies — once, at the start of conversion,
  with the datasource's identifier rule: on a folding dialect (Postgres, Redshift, Snowflake, Oracle) an unquoted
  identifier holding an upper-case letter is stored lowercased; quoted identifiers, logical references
  (placeholders, Jinja, OSI metric and cross-dataset references), literals and comments stay byte-identical.
- dbt relation names (`model:` without a `.sql` file, `ref()` to a table, `source()`, hidden regular models) follow
  dbt's quoting policy: the adapter default for the datasource dialect, overlaid by `dbt_project.yml` `quoting:`;
  per-model and per-source overrides are reported, not honoured. `source(g, t)` names the relation its `sources:`
  block declares.
- A Cube or dbt import whose datasource dialect is unknown and whose physical SQL holds an identifier whose meaning
  depends on it is refused before any model is produced, naming those identifiers.
- A physical SQL fragment the importer cannot parse that may hold such an identifier drops its construct with a
  report.
- One renderer writes every stored `sql_table` (ingestion, importers): a part that is not a plain identifier is
  quoted. Every reader of a stored `sql_table` parses it with the one table-name parser, never by splitting on dots.

## Capabilities

### New Capabilities
- `models/table-names`: how a stored `sql_table` names its table, and that every writer stores a name reading back as
  its live table.

### Modified Capabilities
- `importers/meaning-preservation`: imported physical SQL reads with the source's identifier folding; an import whose
  identifier case cannot be determined is refused; an unparseable fragment that may need folding is dropped; the
  verbatim, identifier-matching and table-name clauses restated accordingly.
- `importers/dbt-import`: relation names follow dbt's quoting policy; `source()` names the declared relation.
- `importers/osi-import`: target-native expressions are kept verbatim apart from the identifier fold; a dataset
  source's table name is read with the datasource dialect.
- `importers/cube-import`: a calculated measure's non-bare operand is Cube's SQL verbatim apart from the identifier
  fold.

## Impact

- Code: `slayer/sql/render/parse.py` (fold, offset-mapped prequote), `slayer/sql/naming.py` (`denoted_name`,
  `render_table_name`), `slayer/importers/{physical,column_types}.py`, `slayer/cube/{converter,filter_params}.py`,
  `slayer/dbt/{converter,parser,models,sql_resolver}.py`, `slayer/osi/{converter,source,dialects}.py`,
  `slayer/engine/{schema_scope,ingestion,schema_drift}.py`, `slayer/cli.py`.
- Behaviour: re-importing a mixed-case source on Postgres / Redshift / Snowflake / Oracle stores lowercased unquoted
  identifiers; models imported before this change are not migrated (re-import fixes them). Offline Cube / dbt imports
  of mixed-case unquoted sources now fail with the identifiers listed.
- Architecture: no arc42 or model change.
- Closes DEV-1679.
- Docs: `docs/cube/cube_import.md`, `docs/dbt/dbt_import.md`, `docs/osi/osi_import.md`.
