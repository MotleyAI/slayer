## Why

Statement assembly still composes SQL **text**. Every `_generate_from_planned_impl` branch
returns `str`, so each downstream composer re-parses it: the outer wrap (`emit_outer_wrap`,
plus T-SQL's re-parse with a silent fallback), the POST-filter f-string, the producer hoist
(`as_ast` fallback), the multi-stage CTE splits, the flat-rename wrapper and WHERE/HAVING
assembly. This violates `sql.arc42.md` §3.1 and `system.arc42.md` §3.6, and it is silently
wrong on MySQL: the re-parse turns a windowed `corr` / `covar_samp`'s `VAR_SAMP` into
population `VARIANCE`.

## What Changes

- Internal render paths return `exp.Select`. One finishing step (render → identifier
  fitting → scope validation → over-limit assertion) runs once at the public text entry
  points `generate_from_planned` / `generate_planned_stages`. The `as_ast` flag, its
  fallback parse and the dead `_parse_cte_body` are removed.
- One public-projection outer-wrap builder serves the plain and transform-chain paths.
  The chain's `WITH` is hoisted onto the outer statement on every dialect.
  **BREAKING (internal)**: `SqlDialect.emit_outer_wrap`, `TsqlDialect.emit_outer_wrap` and
  `_offset_ordering_fallback` are removed; T-SQL pagination stays in `apply_pagination`.
- POST-phase filters become the `WHERE` of the chain's final select (AST `AND`). The
  `_filtered` derived table and `FILTERED_ALIAS` are removed.
- The multi-stage stage / root CTE splits, the producer attach and the stage rename wrap
  operate on AST. `build_flat_rename_wrapper` takes an `exp.Select`. The round-trip repair
  calls to `unmangle_dotted_table_refs` are removed.
- WHERE/HAVING conjuncts are combined as AST (`exp.and_`), not joined as text and
  re-parsed.
- The query-backed virtual-model wrap is built over the unrewritten stage AST and
  finished once.
- MySQL windowed `corr` / `covar_samp` emit `VAR_SAMP` (sample semantics).
- Law: a test-harness tripwire fails on any statement render inside the AST builders
  (`tests/test_law_no_statement_reparse.py`); `sql.arc42.md` §3.1 edit (approved).
- Out of scope: post-render text rewrites (identifier fitting, dot-mangling, ClickHouse
  `SETTINGS`) → DEV-1961; value-layer re-parses (`AggRenderSpec.sql`) → DEV-1972.

## Capabilities

### New Capabilities
- `sql/statement-assembly`: how a generated statement is composed and rendered: AST
  composition with a single render, one top-level `WITH` on every dialect, where
  POST-phase filters apply, and how the query-backed model wrap composes.

### Modified Capabilities
- `aggregations/trailing-window`: windowed sample statistics keep sample semantics on
  every dialect (MySQL `VAR_SAMP`).

## Impact

- Code: `slayer/sql/generator.py`, `slayer/sql/stage_wrapper.py`,
  `slayer/sql/dialects/base.py`, `slayer/sql/dialects/tsql.py`, `slayer/sql/naming.py`
  (`FILTERED_ALIAS`), `slayer/engine/query_engine.py` (query-backed model wrap).
- Goldens: transform-chain statements re-blessed structurally on every dialect (`WITH`
  hoisted, `_filtered` level gone). MySQL windowed `corr` / `covar_samp` change to
  `VAR_SAMP`. Everything else is byte-identical or differs only in whitespace or
  parenthesisation.
- Tests: `emit_outer_wrap` unit tests and the `_filtered` layer-boundary tests are
  re-pointed or deleted (owner-approved), with their intent kept.
- Architecture: `architecture/sql.arc42.md` §3.1 reworded, with `[target: DEV-1972]`,
  `[target: DEV-1961]` and a new `[enforced:]` tag (approved). No `.c4` or `index.yaml`
  change.
