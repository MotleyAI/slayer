## Context

See proposal.md (Why). Structural cause: importers read a source identifier by two rules that agree only by hand —
`PhysicalIndex` (`slayer/importers/physical.py`) matches an unquoted name by the dialect's folding, while the text it
stores is read by SLayer's case-preserving Mode A rule (`quote_mixed_case_identifiers`, `slayer/sql/naming.py`). The
same boundary writes `sql_table` by string concatenation (`SchemaRef.qualify`, `slayer/engine/schema_scope.py`) and
reads it back by splitting on dots (`split_sql_table`, `_parse_qualified_sql_table`, `schema_drift.py` `sql_table.split`).
Principles: system §3.21 (importers preserve meaning), §3.16 (physical spelling applied once), §3.6 (SQL built as AST),
§3.20 (sql-mode sources treated like table sources). No arc42 edit.

Each importer has one in-memory boundary between parse and conversion (`CubeProject`, `DbtProject`,
`list[OSIDocument]`), consumed by `convert()`.

## Goals / Non-Goals

**Goals:**
- One import-time fold puts every physical identifier in SLayer's spelling; SLayer's own Mode A reading is unchanged.
- One `sql_table` writer and one `sql_table` reader.

**Non-Goals:**
- Honouring per-model / per-source dbt `quoting` overrides (reported only).
- dbt `ref()` / `model:` relation identity (alias, custom schema, database) for models without a `.sql` file — only
  case and quoting go through the renderer.
- dbt `ref()` / `source()` arguments outside `\w+` (stay "unresolved Jinja", as today).
- Folding OSI legacy-document metric references.
- Migrating models already stored by an earlier import.
- Manifest `relation_name` as a source of relation names.

## Decisions

1. **Scope: every identifier of imported physical SQL**, not only table names — options: (A) all physical SQL; (B)
   table names only. A: column references and source SQL are the same §3.21 defect at the same boundary. (user)
2. **dbt quoting policy** — options: (A) adapter default + `dbt_project.yml` `quoting:`, per-node overrides reported;
   (B) as A with manifest `relation_name` winning; (C) adapter default only. A: one policy, no manifest dependency.
   (user)
3. **Fold DEV-1679 in: one `sql_table` renderer** for ingestion and importers — options: (A) fold in; (B) separate.
   A: same boundary, same renderer. (user)
4. **Unknown dialect: refuse the whole import** when an undecidable identifier exists — options: (A) drop the
   constructs with a report; (B) keep with an info note; (C) refuse. C: re-import replaces models, so partial imports
   are costly to recover from, and the cause is purely environmental. (user)
5. **`tests/test_importer_source_names.py::test_unquoted_mixed_case_table_types_the_table_queries_read` logic
   revised** to the per-dialect reading, with a Postgres decoy `sales.orderitems` seeded in the Postgres fixture only;
   DuckDB keeps today's assertion; SQLite stays skipped. (user)
6. **Fold mechanism: in-place edit of identifier spans**, text otherwise byte-identical — options: (A) in-place fold;
   (B) AST fold and re-render. A: keeps the OSI / Cube verbatim guarantees, no sqlglot round-trip risk. (user) The
   spans come from the parse tree (`exp.Identifier` meta, inclusive character offsets), not a raw token scan, so
   Snowflake path keys, function-call syntax and keywords are not identifiers to fold. (default)
7. **dbt `source()` names the declared relation**; `sources:` and `dbt_project.yml` are parsed — options: (A) in
   scope; (B) leave out. A. (user)
8. **The fold lowercases on every folding dialect**, Snowflake and Oracle included: a lowercase unquoted name is
   emitted unquoted and folded by the database, and matches SQLAlchemy's normalized live names and SLayer's ingested
   spelling. Postgres folds ASCII letters only (its UTF-8 downcasing rule); Snowflake / Oracle lowercase fully.
   (default)
9. **SLayer's reading is one helper** (`denoted_name`, `slayer/sql/naming.py`): quoted or holding an upper-case
   letter → exact; else the database's folding. `PhysicalIndex`, `LiveSource.column` and the OSI converter's
   field-column lookups match by it; `PhysicalIndex` keeps its ambiguity logic. `LiveSource.column`'s blanket
   case-insensitive fallback becomes the dialect's own matching. (default; OSI lookups: user)
10. **Folding is an SLayer-owned attribute of each dialect** (`slayer/sql/dialects/`, sql §3.2), not sqlglot's
    normalization strategy (codex): fold on Postgres,
    Redshift, Snowflake, Oracle; no-op on MySQL, ClickHouse, BigQuery, T-SQL, Trino, Presto, DuckDB, SQLite,
    Databricks, Spark — there quoting does not change which object a name resolves to. Redshift folds although sqlglot
    classes it case-insensitive: with `enable_case_sensitive_identifier` on it reads like Postgres, and lowercasing is
    correct under either setting. (codex)
11. **Every unquoted identifier is folded, whatever its role** (physical name, CTE, alias, type, function name):
    that is the database's own lexical rule, and folding declaration and uses alike keeps them consistent. Codex's
    proposed role matrix is rejected; its CTE / alias / ORDER BY alias / correlated-subquery cases become tests.
    (codex, partly rejected)
12. **dbt metric and input filters are not folded** (codex): they reference only Jinja objects; an `Entity()`'s
    physical part is its entity `expr`, folded where it is declared. (codex)
13. **Offset-mapped prequote** (codex): reserved words still parse via `_prequote`; a variant returns the inserted
    quote positions so spans map back to the original text, and a reserved word made parseable only by an inserted
    quote counts as unquoted. Replacements are applied right to left. (codex)
14. **Live names keep SQLAlchemy's quote flag** (codex): a reference matching one live column by its folded spelling
    and another by its exact spelling is ambiguous and goes down the existing ambiguity report path. (codex)
15. **OSI folds only physical identifiers of the selected candidate, before target rendering** (codex): a column
    qualified by another dataset's name is a logical reference (neither part folded); each candidate is parsed with its
    own read dialect and folded by the target's rule. (codex)
16. **dbt parser additions** (codex): `dbt_project.yml` `quoting:`, `sources:` blocks (duplicate declarations
    reported, first wins), package-qualified `ref()`; `resolve_refs` takes a relation callback that renders through
    `render_table_name` instead of interpolating strings. (codex)
17. **Typed refusal error** `UndecidableIdentifierCaseError`, caught in `_run_import_cube` / `_run_import_dbt`
    before anything is saved (codex). (codex)
18. **One `sql_table` reader** (codex): `split_sql_table`, `_parse_qualified_sql_table` and `schema_drift.py`'s dot
    splitting read through `parse_table`. (codex)
19. **Cube pipeline order** (codex): extract FILTER_PARAMS; fold each captured physical body with its `{var}`
    placeholders masked; fold each raw SQL surface with Cube references and FILTER_PARAMS sentinels masked; only then
    classify references and generate markers. Masks are equal-length quoted identifiers, never folded. (codex)
20. **An unparseable fragment that may need folding drops its construct** (codex): on a folding dialect, a fragment
    sqlglot cannot parse that holds an unquoted word with an upper-case letter (found by a token scan) drops its member,
    or its model for a source query / table name, with a report; without such a word, today's treatment. (codex, user)
21. **Pure fold / render / parse tests live in modules without a module-level `importorskip`**; the Postgres decoy
    acceptance runs for each importer (codex). (codex)
22. **dbt quoting flags are tested part by part** (database / schema / identifier) on Postgres and Snowflake (codex).
    Codex's claim that the defaults are wrong is rejected: dbt-adapters' `BaseRelation` quote policy is all-true and
    Postgres inherits it; dbt-snowflake and dbt-oracle are all-false; the defaults change meaning only on folding
    dialects. Manifest truth is rejected per decision 2. (codex, partly rejected)
23. **`ref()` / `model:` relation identity is out of scope** — options: (A) resolve only the declared `alias`; (B)
    leave out. B: the full fix needs dbt's `generate_schema_name` and target, i.e. the manifest, excluded by decision
    2; every model with a `.sql` file is inlined and never reaches the renderer. (user)
24. **A member without an `expr` (dbt) reads the physical column its name folds to**, keeping its name. (default)
25. **No report entry per fold; no migration of stored models** (re-import fixes them). (default)
26. **Placement**: `denoted_name` and `render_table_name` in `slayer/sql/naming.py`; `fold_source_identifiers` and
    the offset-mapped prequote in `slayer/sql/render/parse.py` (importers → sql and engine → sql arrows exist).
    (default)
27. **An undeclared `source()` is reported and keeps `g.t`**; the database part is emitted only when declared.
    (default)
28. **`render_table_name` quotes a part** that holds a space, dot or quote, starts with a digit, or is a reserved
    word; an exact mixed-case live name stays unquoted (SLayer reads it exactly); an explicit requested schema is an
    exact name. (default)

## Risks / Trade-offs

- [Re-imports of mixed-case sources on folding dialects change stored text] → intended; the old text read the wrong
  object. Documented in each importer page.
- [Offline camelCase imports now fail] → the error names every identifier and the remedy (configure the datasource;
  it need not be reachable).
- [sqlglot cannot parse some vendor SQL] → decision 20 drops only constructs that may need folding; others keep
  today's treatment.
- [Snowflake / Oracle live objects created with quoted lowercase names] → collide with folded names and are reported
  as ambiguous (decision 14), never matched silently.
