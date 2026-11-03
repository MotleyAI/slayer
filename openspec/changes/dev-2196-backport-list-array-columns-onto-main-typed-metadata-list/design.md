## Context

See proposal.md › Why. The feature exists on the Mode-B grammar stack branch `origin/egor/dev-2101-mode-b-as-a-sql-compatible-grammar-custom-parser-sql-forms` (source of truth: `adc0d8a7`; no list commits after it). References:

- PR #518 (list columns): merge `fccd289d`, diff `fccd289d^1..fccd289d`; archived change `openspec/changes/archive/2026-10-29-dev-2137-*` on the branch.
- Later list-only fixes: `02a1fc10` (element aggregate filtered across another to-many hop; ClickHouse < 25.8 refusal), `649ecc2b` (a list child built once per parent), `4322c840` (untyped empty list), `b48cb26c` (stage element access; Trino `any_match`; ClickHouse cells), `2e3d3d67` (Postgres array `db_type`; MCP schema budget), `371d8714` (`element_type` in `edit_model`), `7d9c44fd` list part (Postgres canonical-array casts), the BigQuery exact-numeric `collect` from `31b9f531`/`7d9c44fd`, and `81184c79` (live-spec edits).
- PR #524 (library-name collisions, DEV-2170): merge `e22cc1fc`, diff `e22cc1fc^1..e22cc1fc`.

Constraints on `main` the branch does not have:

- Principle 17 "One database door" (`tests/test_law_database_door.py`): every live access goes through a client owned by a query engine; storage reaches the engine through `storage/engine_view.py`. The branch's `sa_engine.connect()` calls in `_inferred_elements` are not portable as written.
- No DEV-2120 library framework (`_library(...)`, `ScalarSpec.result`, `library_functions` / `refuse`, `CastKey`, `coerce_text_numbers`, `render_scalar_key`). List functions go into main's `SCALAR_SPECS` and render through `render_scalar_call` → `dialect.build_list_call`.
- Main's parser is `slayer/engine/syntax.py` (Python-AST). It already parses `L.value`, `count(L.*)`, list calls and `{v}`; it lowercases scalar names and refuses scalar keyword arguments at parse time (`_convert_call`), and refuses scalar names as method calls (`_method_call_refusal`).
- Main has no `BETWEEN`, `IS DISTINCT FROM` or typed `DATE '…'` literals.

## Goals / Non-Goals

**Goals:**
- Same user-visible list behaviour as the branch wherever it does not need stack-only machinery (specs in this change).
- Same symbol names, module layout, test-file names and spec text as the branch, so the DEV-2101 → main merge resolves mostly as same-content.
- Every stored artefact that loads on 1.6.0 loads unchanged.

**Non-Goals:**
- String `contains(str, substr)`; TEXT↔number coercion of columns (`coerce_text_numbers`, `CastKey`, TRY_CAST); element-by-element comparison of differently typed lists; `json_value` on Snowflake arrays (DEV-2120 / DEV-2169).
- DEV-2169's translate-or-refuse library framework and its extended refusal list.
- sql-mode result re-typing and element inference (`SqlResultTypes`, `_typed_result_lists`, `retyped_sql_columns`, `_retype_sql_models`).
- DEV-2172: result-value codec (`encode_json` / `encode_text`), selectable facade list columns, raw-rows per-record list projection, list order-key rule, `results/value-encoding`, facade metric exclusion.
- DEV-2171 importer list typing / OSI list-function mapping.
- Branch arc42 P10 rewording, 11.1 / 11.2, P17–22.
- Unrelated pieces of PR #518 (startup-signal race fix, `tests/_dev1871_raise_ledger.py`, comparison matrix Q27, the basedpyright baseline edit — this port adds no baseline entries).
- DEV-2170 pieces unrelated to name collisions: `agg(distinct)` migration, old-grammar gate, rollback-safe serialisation, element-level drift attribution, SQLite temporal-over-TEXT, `statement_reads`, interval / method-call parser ambiguity changes.
- Model, query, datasource and memory version bumps.

## Decisions

1. **`contains` is list-only on main.** A non-list first argument raises a plan-time `ListTypeError` pointing at `LIKE '%x%'`; `contains_any` / `contains_all`'s non-list suggestion names `LIKE` instead of `contains(text, substring)`. DEV-2120 adds the string overload. (user)
2. **Element typing A′.** Family rule (number = INT/DOUBLE, time = DATE/TIMESTAMP). Against numeric elements, a string literal that parses as a number is rewritten to a number literal (`contains`' value; elements of a `[...]` literal); a non-numeric string literal or any TEXT column/expression is a `ListTypeError`. No `CastKey` / TRY_CAST. Postgres canonical-array casts. Options weighed: A (refuse every TEXT; refuses a spelling the branch accepts), B (exact element type only; refuses `contains_any(prices, [1, 2])`). (user)
3. **Library-name collisions: port DEV-2170's mechanism as on the branch (A).** Any scalar or list function name may be a custom aggregation's name; it shadows the function inside its owning model with a structured warning on save, load and every resolving response; `AmbiguousAggregationError` on case; a formula-backed built-in (`collect` included) is the model's own; `storage/granularity_rename.py` renames reserved-named stored granularities on every load with every reference rewritten; transform and date-syntax names stay reserved. Option B (shadowing only for the three new names) rejected: two rules for one collision kind, and a hardcoded set the merge deletes. (user)
4. **Names and content align with the branch**, including `tests/test_dev2137_*.py`, `tests/_dev2137_fixtures.py`, `tests/_dev2120_fixtures.py` (list subset only), `tests/test_dev2169_postgres.py` and `tests/test_dev2120_library_matrix.py` (list subsets), overriding the no-issue-numbers-in-identifiers rule. (user)
5. **arc42 edits** (user, approved wording):
   - `semantics.arc42.md` Axiom 3, new sub-item: "**3.2 List children.** A list column `L` of dataset `D` is a keyless one-to-many child `D.L`: one row per non-NULL element, its value `D.L.value`, each row determining its `D` row. [enforced: test:tests/test_dev2137_list_children.py]"
   - `semantics.arc42.md` Axiom 4: "0 for the count family, the empty list for `collect`, NULL otherwise."
   - `system.arc42.md` P14: "overwritten by a re-ingest (documented exceptions: the `source_kind` refresh, and a non-list column re-typed to the list its live column is). [review]"
   - `python.engine.arc42.md` P5: "(`ModelScope`: dots walk joins; `StageSchema`: flat names, plus a list column's elements `L.value` / `L.*`) are distinct types, never a flag. [review]"
   - No `.c4` change.
6. **Specs come from the branch's live specs** (incl. `81184c79`), not only the archived change; DEV-2172 / DEV-2169-framework additions are dropped. (default)
7. **The library-growth pin test is included** (`tests/test_library_growth.py`) as the structural guard against the bug class "a new reserved name breaks stored stores". (default)
8. **DEV-2170 pieces unrelated to list or name collisions stay out** (see Non-Goals). (default)
9. **An sql-mode declared `ARRAY` is not drift; sql-mode re-typing stays out.** (default, per issue)
10. **No version bumps**, mirroring PR #518. (default)
11. **Shadowing needs main-specific parse deferral** (Codex #2): port the branch's node fields `ScalarCall.written` / `written_args` and `AggCall.library_error` into main's `engine/syntax.py`; scalar-only parse errors (keyword arguments, method-call refusal, arity) are deferred to binding, which resolves the shadow against the owning model (engine P2: the parser only parses). The branch's interval and method-call ambiguity parser changes are not ported. (codex)
12. **`ArrayLiteralKey` dispatch audit** (Codex #3): every `ValueKey` union, visitor, serialiser, rebuilder, dependency walker, projection path, scope union and renderer that PR #518 touched (`core/refs.py`, `engine/reference_closure.py`, `engine/compile/projection.py`, `sql/scope.py`, `sql/render/value_expr.py`, `sql/render/row_expr.py`, `_LEAF_KINDS`, …) handles the new kind or fails closed (system P13); the branch's generic traversal / substitution tests are ported. (codex)
13. **Snowflake element inference uses the engine-owned client** (Codex #4): it runs through the ingesting engine's `SqlClient` (the same door reflection uses); `tests/test_law_database_door.py` gains cases for Snowflake inference, re-type on re-ingest, list profiling and sql-mode cursor typing. (codex)
14. **P14 wording kept; re-typing's field effects pinned by tests** (Codex #5): an idempotent re-ingest asserts every kept field (description, label, meta, sql, hidden, keys) and every dropped one (`format`, granularity, non-`count` aggregations). Rewording P14 rejected: the approved branch text is kept for alignment. (codex; wording change rejected)
15. **List variables decide by position** (Codex #6): `_list_slot_ordinals` marks a placeholder in `contains_any` / `contains_all`'s list argument; no importer `list_valued` meta is involved. Codex's dependency claim rejected (false); its edge cases (`[]`, nested lists, a repeated placeholder, a list placeholder in a non-list function) are tested. (codex; premise rejected)
16. **Shadow-warning plumbing ported with per-surface tests** (Codex #7): the branch's `core/warnings.py` warning and query-engine plumbing; tested on save, load, query, dry run, EXPLAIN, cached response, REST and MCP. (codex)
17. **A′ ordering contract** (Codex #8): the branch's order is kept — `check_list_operands` runs on the bound keys, then `coerce_list_arguments` rewrites in `_map_bound_keys`. The checker's TEXT exception narrows to string literals that parse as numbers; the rewrite touches only those. `['1', 2]` stays a mixed literal, refused as on the branch. Edge cases tested: decimals, exponents, whitespace, non-finite spellings. (codex)
18. **sql-mode element contract** (Codex #9): sql-mode drift compares only the top-level list type; an sql-mode list column's `element_type` is the declared one, else `UNKNOWN`; nothing infers it. (codex)
19. **Refusal and storage-parity coverage** (Codex #10): plan-time refusal tests for all eight non-list dialects; YAML + SQLite parity for the granularity rename (repeated load, interrupted rename, `<name>_granularity` taken, cross-document rewrite). (codex)
20. **No DatasourceConfig version bump for the rename** (Codex #1 rejected): the branch's rename is driven by names on every load (`_with_library_granularities_renamed`), and its v2→v3 step is a no-op, so a current-version store is renamed without a bump. (rejected (codex))

## Risks / Trade-offs

- [Snowflake VARIANT may not compare FIXED with REAL numbers by value] → the mixed-numeric scenario runs in the live Snowflake suite; a mismatch is raised with the user in pr-implement, never silently narrowed.
- [ClickHouse < 25.8 cannot plan a correlated EXISTS beside ARRAY JOIN] → typed refusal naming 25.8; the live ClickHouse suite runs on ≥ 25.8.
- [Lifting the scalar-name save check lets new models shadow any function] → the shadow warning on every response; transform and date-syntax names stay reserved.
- [The granularity rename rewrites user documents] → name-driven, converging, dependants persisted first; YAML and SQLite parity tests.
- [Re-merge with DEV-2101] → divergences listed below, all resolving toward the branch:
  - `'abc'` against a number list: main raises `ListTypeError`; the branch TRY_CASTs to no match.
  - A TEXT column against a number list: main raises; the branch TRY_CASTs.
  - `contains(text, x)`: main raises pointing at `LIKE`; the branch has the substring overload.
  - sql-mode list columns: main keeps `element_type: UNKNOWN` unless declared; the branch infers.
  - Main's `engine/syntax.py` gains `ArrayLit` and the shadow node fields; the branch deletes that module.
  - Conflict hotspots: `core/keys.py`, `engine/elaborate_env.py`, `binding.py`, `bind_inputs.py`, `render/row_expr.py`, `value_expr.py`, `dialects/base.py` and every Tier-1 dialect, `core/format.py`, `pg_facade/types.py`, `flight/handlers.py`, `mcp/server.py`, duplicated test files.

## Migration Plan

No version bump; the only stored-document rewrite is the on-load granularity rename (name-driven, idempotent), whose new names 1.6.0 also loads. A model this release writes with an `ARRAY` column or an aggregation shadowing a function does not load on 1.6.0; rolling back means re-ingesting such models there.
