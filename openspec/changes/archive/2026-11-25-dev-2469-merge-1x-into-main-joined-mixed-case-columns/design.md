## Context

See proposal.md — Why. `origin/1.x` holds #653 (DEV-2405) as merge `fa1644481`, commits `a734911c2`, `411a88182`, `26d756bbf`, `4f369e79f`, `c3058b7fc`, `468ea6034`; its code diff is `git diff 159d4f894 fa1644481 -- slayer/ tests/`, its design `openspec/changes/archive/2026-11-25-dev-2405-…/design.md` on `origin/1.x`. DEV-2406's forward merge (#655) is on `main`; `git merge-tree` shows conflicts only in `slayer/sql/generator.py`, `slayer/sql/scope.py`, `tests/test_agg_render_spec.py`, `tests/test_dev1825_scope_attach.py`, `tests/test_sql_generator.py`; `openspec/specs/sql/statement-assembly/spec.md` auto-merges.

On `main` (probed at `9bffaca20`) the same two-renderer shape as `1.x` stands, in `main`'s vocabulary (`FieldKey`, `SlayerDataset`, `fields`, `relationships`, `population`): `ScopeFrame.resolve` (`slayer/sql/scope.py`) quotes mixed-case leaves via `quote_mixed_case_identifiers`, applies `read_column` (zoned typing) and registers crossed paths; `SQLGenerator._joined_or_local_dim_expr` builds a bare joined identifier, used by the base select, `_ranked_scope_expr`-equivalent composite paths, `_raw_time_col_expr_for_planned` (and so `_bucket_source_expr`, window producers) and `_host_base_order_ref`; `_collect_joined_paths_for_base` collects base-select paths in a separate pass; `_throwaway_frame` mints a fresh allocator. A joined dataset over an untrusted to-one hop is then rewritten by `slayer/sql/untrusted_projection.py` into `SELECT DISTINCT <keys>, <reads> FROM <subtree>`; the rewrite copies each column's identifier with its quoting (`_ident` keeps quoting when the carried name equals the column name), so the unquoted inner read is the generator's, not the rewrite's. Unknown fields already fail at bind time (`DatasetScope`), but the deleted helper's `ValueError` is the only emission-time existence check.

A `FieldKey` leaf is its physical name by construction: the binder emits `FieldSqlKey` for any field whose expression is not its bare self-name (`needs_expansion`, `slayer/engine/binding.py`), a quoted self-name included.

Applicable principles: `python.sql.arc42.md` §3.1 (AST end to end), §3.3 (one naming authority), §3.4 (join discovery is a side effect of entering a scope, never a separate pass), §3.5 (one renderer), §3.9 (fail closed), §3.11 (one allocator per statement); `system.arc42.md` §3 4 (SQL built as AST). No arc42 or `.c4` edit, no import edge change; the delta lands in the `sql` spec group owned by `python.sql`.

Structural cause: two plain-field renderers that must agree by hand, a separate join-path pass, and scopes on a private allocator. Fix: one renderer, discovery by resolving, the generation allocator everywhere.

## Goals / Non-Goals

**Goals:**
- `main` contains every `1.x` commit; one renderer for plain field references on `main`, so the joined/host quoting split and the filter/FROM alias split cannot recur.
- Every spec scenario has a test on `main` that fails without the fix where behaviour changes.

**Non-Goals:**
- Changing the mixed-case quoting rule, derived fields (`FieldSqlKey`), list-element or lateral reads.
- `untrusted_projection.py` changes; the `__slayer_` reserved-name namespace (save-time reservation stands).
- Docs, version bump, API / wire / storage change.

## Decisions

1. **Tests on `main` before the merge, then `git merge origin/1.x` in pr-implement** (message first line `Merge branch 'origin/1.x' into egor/dev-2469-merge-1x-into-main-joined-mixed-case-columns`), as DEV-2379: each new test fails without the fix. (default)
2. **Conflict rule: #653's behaviour, `main`'s layout and vocabulary** — `node_datasets` (1.x `node_models`), `_resolve_plain_row_fields` (1.x `_resolve_plain_row_columns`), `TimeTruncKey.source`, `FieldKey`. (default)
3. **The requirement is ADDED by this change's delta in `main`'s wording; `openspec/specs/**` resolve to the pre-merge HEAD at the merge.** A MODIFIED delta cannot validate before the merge, and keeping the auto-merged `1.x` text would make the archive add a duplicate. Title kept for traceability; ledger entry for the `1.x` body. (default)
4. **`untrusted_projection.py` unchanged**: it copies identifiers with their quoting, so fixing the generator fixes the projection; tests pin it. (default)
5. **Delete `_joined_or_local_dim_expr` and `_collect_joined_paths_for_base`; base-select row `FieldKey`s and the `FieldKey` source of each `TimeTruncKey` resolve through `host_scope` before `_build_from_and_joins`**, in the collector's position; `_raw_time_col_expr_for_planned` takes `scope` and every caller passes its own (window source, collapsed window grain, composite / ranked paths, `_bucket_source_expr`). The `FROM` stays byte-identical (goldens pin it); any join reordering stops for the user's call. (default, as 1.x decision 8)
6. **Order-only row `FieldKey` slots register their path prefixes only, as on 1.x**: a regroup placeholder ordered on is attached after the base select, so resolving it there trips the existence guard; an unknown order-only field still fails closed where it is read (`_host_base_order_ref` resolves through the scope). (codex, amended in pr-implement)
7. **Fail closed in the canonical path**: `ScopeFrame`'s `FieldKey` branch raises `UnknownReferenceError` when the terminal dataset or its leaf does not resolve — after the regroup attach-registry lookup; a semi-join node path is checked through `node_datasets` first, then the relationship graph. Ported though `main`'s binder already refuses unknowns: nothing else checks at emission once the helper is gone. A legitimate synthetic leaf tripping it stops for the user's call. (default)
8. **No fresh allocators**: `_throwaway_frame` mints through `self._gen_allocator or self._new_allocator()`; `_host_base_order_ref`'s fallback frame is built on the generation allocator; the semi-join render context passes the group's node datasets to `_scope_frame`. (default)
9. **Both relationship shapes tested** — untrusted (main's default, distinct projection) and trusted (flat join, 1.x's shape). (default)
10. **`TestFilterJoinAliasMatchesFrom` asserts on every read of `population`**, not `tree.args["where"]`: on an untrusted hop the predicate moves into the projection. (default)
11. **The filter-collision scenario is new in the spec** (a test only on 1.x); the tracker's acceptance lists it. (user)
12. **Lowercase goldens pin `main`'s emission before the fix**, both shapes; `main`'s result keys and shape differ from 1.x's strings. (default)
13. **Neutral names only**: `orders` / `accounts` / `regions`, `stores` / `regions` / `stores__regions` (table `tags`), camelCase `orderNumber`, `extRef`, `signedUpAt`, `regionName`, `displayName`. (default)
14. **No docs change, no version bump.** (default)
15. **Aggregate-argument and post-aggregation-filter scenario** (`max(accounts.signedUpAt)`, `count(distinct accounts.extRef)`, a filter on the count), both shapes, zoned read path included. (codex)
16. **Same-leaf nested projection scenario**: `displayName` on both `accounts` and `regions`, untrusted. (codex) Rejected (codex): logical ≠ physical name tests — a `FieldKey` leaf is the physical name by construction (Context); `__slayer_` namespace tests — save-time reservation is out of scope.
17. **Real semi-join `EXISTS` scenario**, plus a binding-bypassed unknown node leaf. (codex) Rejected (codex): list-element and lateral variants — not plain fields across relationships.
18. **OpenSpec / guard mechanics**: the archived DEV-2405 folder comes in unchanged; after the merge `git diff <pre-merge HEAD> -- openspec/specs` is empty, so no `main`-only requirement drops; this delta is the only place `main`'s requirement is added (once, at archive); the ledger entry uses the exact `1.x` hash the guard reports; the guard runs after the merge commit and again on the archived HEAD. (codex)
19. Rejected (codex): **keep `_throwaway_frame` on a fresh allocator** — the tracker's acceptance line describes the current state to fix; the isolation concern is a risk below.

## Risks / Trade-offs

- [`main`'s extra `_throwaway_frame` callers (list relations, semi-join values) move onto the shared allocator and renumber aliases (`_2` suffixes)] → goldens are the check; any renumbering stops for the user's call.
- [Resolving row slots in the collector's position registers prefixes in a different order, reordering `JOIN`s] → goldens; any reorder stops for the user's call.
- [The existence guard refuses a synthetic leaf (row identity, spine, list element, order-only placeholder)] → the local test scope plus CI surface it; stop and ask, never carve an exception silently.
- [Clean auto-merges carry 1.x vocabulary or 1.x test files with legacy names (`ColumnKey` no longer exists on `main`)] → `git diff origin/main --stat` audit; new test files resolve to their pr-tests versions.
- [Existing `main` tests pin the unquoted shape or the helper's `ValueError`] → found by the local test scope; aligned with per-file user approval.
