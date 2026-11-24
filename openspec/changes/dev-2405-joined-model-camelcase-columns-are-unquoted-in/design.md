## Context

See proposal.md — Why. On `1.x` the generator renders a plain column reference (`ColumnKey`) through
two code paths that must agree by hand:

- `ScopeFrame.resolve` / `_anchor` (`slayer/sql/scope.py`) — used by filters, aggregate arguments and
  computed dimensions; quotes mixed-case leaves via `quote_mixed_case_identifiers` and registers the
  joins it crosses.
- `SQLGenerator._joined_or_local_dim_expr` (`slayer/sql/generator.py`) — used by the base-select
  projection / `GROUP BY`, `_ranked_scope_expr`, `_raw_time_col_expr_for_planned` (and so every time
  dimension and window-producer time source) and the hidden-`ORDER BY` branch of the host base order
  env. Its host branch quotes (via `_resolve_sql` → `_to_ident`); its joined branch builds a bare
  `exp.to_identifier(leaf)` — the bug.

This violates `python.sql` §3.5 (One ValueKey renderer) and §3.4 (join discovery is a side effect of
entering a scope, never a separate pass — the base select discovers row-slot joins through the
separate typed collector `_collect_joined_paths_for_base`).

Applicable principles: `python.sql` §3.1 (AST end to end), §3.3 (one naming authority — join aliases
come from the generation allocator), §3.4, §3.5, §3.9 (fail closed); `system` §3.6 (SQL built as AST),
§3.16 (logical names, physical once — a `ColumnKey` leaf is its physical name by construction, since a
column whose `sql` differs from its name is a `ColumnSqlKey`). No arc42 or `.c4` edit is needed; the
spec delta lands in the `sql` group, owned by `python.sql`.

## Goals / Non-Goals

**Goals:**
- One renderer for plain column references, so the joined/host quoting split cannot recur.
- Base-select row-slot joins discovered by resolving, not by a separate collector.
- Fail closed on an unresolvable plain column in every clause.

**Non-Goals:**
- Changing the mixed-case quoting rule (quote-all, reserved-word handling).
- Derived columns (`ColumnSqlKey`) — they already expand through the scope.
- The `main` forward-port: `main` reads a joined model through a `SELECT DISTINCT` subquery whose
  inner column read is also unquoted; that shape gets its own change after this merges.
- The 1.x version bump (lands separately after merge, as for 1.9.0 / 1.9.1).
- Session-policy (RLS) ruleset identifiers — raw physical SQL strings, a different surface.
- A separate formula / `IN` issue that shares the reporter's repro script.

## Decisions

1. **Target `1.x` first.** The branch is re-pointed at `origin/1.x` (it carried no commits) and the PR
   targets `1.x`; `main` follows after merge. (user)
2. **Structural fix — one renderer.** Delete `_joined_or_local_dim_expr`; every `ColumnKey` render in
   the generator goes through `ScopeFrame.resolve`. Alternative: quote the leaf in the joined branch —
   rejected, it leaves two renderers that must agree by hand. (default)
3. **Quoting rule unchanged.** The scope's existing mixed-case rule; lowercase output stays
   byte-identical. Alternative: quote every identifier — rejected, inconsistent with host and filter
   paths and churns every golden. (default)
4. **Execution proof on Postgres** via pytest-postgresql (a unit test, no Docker): SQLite and DuckDB
   resolve identifiers case-insensitively, so only a case-folding engine reproduces the failure.
   Emission tests run on Postgres and Snowflake (folds to upper case). (default)
5. **Spec home:** a new requirement in the existing `sql/statement-assembly` capability; no new
   capability, so no attachment decision. (default)
6. **No version bump** in this PR. (default)
7. **Neutral names only.** The reporter's model names derive from a customer; tests and specs use
   `orders` / `accounts` / `regions` with camelCase columns (`orderNumber`, `extRef`, `signedUpAt`,
   `regionName`). (default)
8. **Resolve before the `FROM`; delete the collector.** Base-select row slots (`ColumnKey` and the
   column of a `TimeTruncKey`), in render order and then order slots, resolve through `host_scope`
   before `_build_from_and_joins`, in the position `_collect_joined_paths_for_base` holds today; their
   crossed paths register as a side effect, and the collector is deleted. The `FROM` must stay
   byte-identical (goldens pin it); if goldens show any join reordering, stop and ask the user. (codex)
9. **Fail closed in the canonical path.** The deleted helper was the only existence check for a joined
   plain column. `ScopeFrame`'s `ColumnKey` branch raises `UnknownReferenceError` when the terminal
   model (`resolved_terminal`) or its leaf (`get_column`) does not resolve — after the regroup
   attach-registry lookup, so attached placeholders are unaffected. Filters gain the same guard. If a
   legitimate synthetic leaf trips it, stop and ask the user. (codex)
10. **No fresh allocators.** A call site uses its caller's own `ScopeFrame` wherever one exists; the
    fallback frame is built on the active generation allocator (`self._gen_allocator`) with the same
    root relation — never `_throwaway_frame`, which mints a fresh allocator whose aliases can diverge
    from the `FROM` under identifier fitting or collisions. (codex)
11. **Window producers pass their scopes.** The duration-window source and collapsed window-grain
    paths already own `src_scope` / `scope`; they pass it into raw time-column resolution, and each
    gets a joined camelCase time test. (codex)
12. **Ranked-CTE test shape.** A `first` / `last` measure partitioned by the joined camelCase column,
    and separately ranked by a joined camelCase time column; assert quoting inside the ranked
    subquery. (codex)
13. **Hidden-`ORDER BY` test shape.** An ungrouped query ordering by a non-projected joined plain
    column with no materialised alias reaches the split-reference branch; projected and
    grouped/materialised ordering stay separate cases. (codex)
14. **Spec scoped to one relational scope.** The requirement covers references reading the same
    physical model column within one scope and excludes result aliases and materialised stage / CTE
    columns. (codex)

## Risks / Trade-offs

- [Pre-resolving in the collector's position registers path prefixes the collector did not add,
  possibly reordering `JOIN`s] → goldens are the check; any reorder stops for the user's call.
- [The new existence guard refuses a synthetic leaf (row identity, time spine, list element) that a
  filter or projection legitimately reads] → the full local scope plus CI surface it; stop and ask
  rather than carve an exception silently.
- [A fallback frame on the generation allocator could still diverge from the `FROM` alias] → the
  fitted / colliding two-hop alias scenario pins qualifier = `JOIN` alias.
