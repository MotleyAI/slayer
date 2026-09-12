# sql — SQL generation

## 1. Purpose & context

`slayer/sql` turns a `PlannedQuery` (built by `engine`) into dialect-correct SQL
text. Children: `render` (AST assembly: value keys, order terms, joins, CTE
assembly) and `dialects` (per-dialect emission strategies). It must not know how
plans are made — it consumes the shared representation in `slayer/ir`, never
`engine` internals.

## 2. Building blocks

The `sql_focus` view ([views.c4](views.c4)):

<!-- likec4:sql_focus -->
```mermaid
flowchart TD
  %% sql_focus: SQL generation in context
  subgraph core["Core domain models"]
    core__query["Query"]
    core__models["Models"]
  end
  subgraph sql["SQL generation"]
    sql__render["Render"]
    sql__dialects["Dialects"]
    sql__sql_predicate["SQL predicate"]
    sql__window_detect["Window detect"]
  end
  engine["Query engine"]
  ir["Intermediate representation"]
  storage["Storage backends"]
  importers("Importers")
  surfaces("User-facing surfaces")
  core__models -.-> sql__dialects
  core__models -.-> sql__sql_predicate
  core__models -.-> sql__window_detect
  core__query -.-> sql__window_detect
  sql__render --> sql__dialects
  sql__sql_predicate --> sql__window_detect
  engine --> sql
  importers --> sql
  sql --> core
  sql --> ir
  storage --> sql
  surfaces --> sql
  classDef leaf fill:none;
  class core__query,core__models,sql__render,sql__dialects,sql__sql_predicate,sql__window_detect,engine,ir,storage,importers,surfaces leaf;
```
*Dashed arrows: legacy edges slated to die.*
<!-- /likec4:sql_focus -->

Children: `render`
(value keys, aggregates, order terms, joins, node assembly) and `dialects`.

## 3. Principles

1. **AST end to end**: statements are built and composed as sqlglot AST; text
   round-trips of already-emitted SQL are forbidden (dotted aliases corrupt on
   re-parse). [review]
2. **Dialect quirks live only in `dialects/`** — one file per Tier-1 dialect,
   data-shaped Tier-2 table; everything not explicitly overridden goes through
   sqlglot transpilation. No `if dialect == …` outside `dialects/`. [review]
3. **One naming authority**: `slayer/sql/naming.py` owns every alias and
   result-key decision (dotted result keys, flat inner names, mangling,
   identifier-length fitting, CTE names). [review]
4. **One Mode-A door**: free SQL enters a scope only through
   `ScopeFrame.enter_predicate` / `enter_expression`; join discovery is a side
   effect of entering, never a separate pass. [review]
5. **One ValueKey renderer**: `render_value_key` / `render_scalar_call` /
   `render_arithmetic` are the sole render paths for typed keys. [review]
6. **CTE dependencies are declared**, never rediscovered by scanning rendered
   SQL. [review]
7. **Grain join-backs are null-safe**, built by the one builder in
   `render/joins.py`; an empty grain is an explicit CROSS JOIN, never `TRUE`.
   [review]
8. **One reserved-keyword set** (`slayer/sql/reserved_keywords.py`); extend that
   set only. [review]
9. **Fail closed**: reject with a typed error rather than emit invalid or
   silently-wrong SQL; the scope-closure validator (`SLAYER_VALIDATE_SCOPES`,
   on in the test harness) backstops emission. [review]
10. **One composition primitive**: every aggregate that needs its own rows —
    crossing a join, its own ordering, its own frame, its own grain — compiles
    as a producer (a plan-shaped CTE rooted where its rows live), attached
    back by a null-safe LEFT JOIN on its complete grain and substituted into
    expressions by structural identity, never text. [review]
11. **One flat WITH**: every statement renders through one pipeline
    (base → aggregate → combined → steps → post) with one allocator; a
    producer's internal WITH hoists to the top level — a WITH never nests
    inside a CTE definition; fusion of adjacent phases is an emission
    decision, never semantic. [review]
12. **Attach is cardinality-neutral**: attaching a producer never changes the
    host row count or any other column's value.
    [enforced: test:tests/test_dev1837_dimension_measure_matrix.py]

## 4. Rationale

Parallel render paths diverge silently: per-path renderers, per-position ORDER
BY resolvers, and regex-based join discovery each produce subtly different SQL
for the same plan, so the single-door / single-renderer / single-namer shape
makes that divergence structurally impossible. The layering
`engine` → `sql` → `ir` → `core` holds because `generator.py` consumes the
shared plan types from `slayer/ir`, never planner internals.
