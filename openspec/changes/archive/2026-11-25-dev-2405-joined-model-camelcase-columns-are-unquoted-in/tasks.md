## 1. Tests (pr-tests stage)

Base branch is `1.x` (PR base `1.x`; `la-affected-tests --base origin/1.x`). The worktree venv is
installed (`poetry install -E all`). Neutral names only (design.md decision 7): `orders`
(`orderNumber`, `accountId`) → `accounts` (`extRef`, `signedUpAt`, `regionId`) → `regions`
(`regionName`). New file `tests/test_joined_column_quoting.py` (no issue key in file or test names);
emission helper `tests/_engine_helpers.py::_engine_generate` (dry run); AST assertions via sqlglot —
every `exp.Column` whose name is the camelCase leaf has `quoted=True` and the same qualifier in every
clause.

- [x] 1.1 Emission, Postgres + Snowflake: dimensions `orderNumber`, `accounts.extRef` + filter
  `accounts.extRef = 'a'`; and `accounts.extRef` grouped under `{"formula": "count(*)"}` with no filter.
  Verify: both fail on today's 1.x (projection / `GROUP BY` unquoted).
- [x] 1.2 Emission: two-hop `accounts.regions.regionName`, plus a variant whose two-hop join alias is
  fitted to the identifier limit or disambiguated against a colliding name; assert the column's
  qualifier equals the emitted `JOIN` alias. Verify: fails on today's 1.x.
- [x] 1.3 Emission: time dimension `month(accounts.signedUpAt)` in the base select, as a duration-window
  source, and as a collapsed window grain (the `_raw_time_col_expr_for_planned` callers at
  `generator.py` ~2367 / ~2382 / ~3263). Verify: each fails on today's 1.x.
- [x] 1.4 Emission: ranked CTE — a `first` / `last` measure partitioned by `accounts.extRef`, and one
  ranked by `accounts.signedUpAt`; assert quoting inside the ranked subquery (reaches
  `_ranked_scope_expr`). Verify: fails on today's 1.x; confirm the route by temporarily breakpointing
  or by the subquery shape.
- [x] 1.5 Emission: ordering by `accounts.extRef` — projected; grouped but not projected
  (materialised); and an ungrouped query neither projecting nor materialising it (reaches the
  hidden-`ORDER BY` split-reference branch, `generator.py` ~6289). Verify: every route that reads the
  source column fails on today's 1.x; note which, if any, already pass.
- [x] 1.6 Lowercase regression: the 1.1 queries with all-lowercase joined columns emit byte-identical
  SQL to today's 1.x (pin today's output as the expected string). Verify: passes on today's 1.x.
- [x] 1.7 Execution on Postgres via pytest-postgresql (follow an existing `postgresql_proc` test, e.g.
  `tests/test_dev2169_postgres.py`; sandboxed Postgres needs TCP only): quoted camelCase DDL, seed
  rows, run both 1.1 queries, compare to hand-computed rows. Not marked `integration`. Verify: fails
  on today's 1.x with an undefined-column error.
- [x] 1.8 Fail closed: a plain column reference whose leaf the terminal model does not declare, reaching
  emission in a projection and in a filter (drive `ScopeFrame.resolve` / the generator directly — the
  binder refuses it earlier) → `UnknownReferenceError`, no SQL. Verify: fails on today's 1.x (today it
  emits SQL or raises a bare `ValueError`).

## 2. Implementation (pr-implement stage)

- [x] 2.1 `slayer/sql/scope.py`: the `ColumnKey` branch, after the attach-registry lookup, raises
  `UnknownReferenceError` when `resolved_terminal` or the terminal's `get_column(leaf)` does not
  resolve; a semi-join `EXISTS` scope carries the group's node models, which the guard checks first,
  then the join graph. Verify: 1.8 passes; no existing test regresses (if a synthetic leaf trips it, STOP and ask).
- [x] 2.2 `slayer/sql/generator.py` `_build_base_select_for_planned`: resolve row `ColumnKey` slots and
  `TimeTruncKey` columns in render order through `host_scope` before `_build_from_and_joins`, in the
  collector's position; order-only slots register their join path prefixes without resolving; build the projection / `GROUP BY` from those
  expressions; delete `_collect_joined_paths_for_base` and the separate `regroup_env` lookup (the
  scope's `attached_columns` covers it). Verify: 1.1, 1.6 pass; goldens show a byte-identical `FROM`
  (any join reorder → STOP and ask).
- [x] 2.3 `_raw_time_col_expr_for_planned` takes a `ScopeFrame`; every caller passes its own scope
  (`src_scope` / `scope` in the window producers), else a frame built with `self._scope_frame(...,
  allocator=self._gen_allocator)` on the same root relation — never `_throwaway_frame`. Verify: 1.3
  passes.
- [x] 2.4 `_ranked_scope_expr` resolves `ColumnKey` through its `scope`; the host base order env's
  hidden-row branch resolves through a scope on the generation allocator. Verify: 1.4, 1.5 pass.
- [x] 2.5 Delete `_joined_or_local_dim_expr`; no `ColumnKey` render remains outside `ScopeFrame.resolve`
  in `slayer/sql/`. Verify: 1.2 and 1.7 pass; `grep` finds no caller.
- [x] 2.6 Local scope: `la-affected-tests --base origin/1.x` plus goldens / conformance cases pinning
  generator SQL. Verify: all pass.

## 3. Gates

- [x] 3.1 `poetry run ruff check slayer/ tests/`, `la-typecheck` (no new errors vs the baseline),
  `la-check-conventions`, `poetry run la-arch-check`,
  `openspec validate dev-2405-joined-model-camelcase-columns-are-unquoted-in --strict` all pass.
- [x] 3.2 After merge, post forward-port notes for `main` as a comment on DEV-2405: `main` reads a
  joined model through a `SELECT DISTINCT` subquery whose inner column read is also unquoted; carry
  over the tests and the one-renderer shape. Verify: the comment exists on DEV-2405.
